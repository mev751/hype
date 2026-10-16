/**
 * Binance & Hyperliquid 实时聚合与跨交易所套利监控系统
 * 部署平台: Cloudflare Workers
 * 
 * 功能特性:
 * 1. 服务端数据拉取与计算 (解决客户端跨域、网络受限及计算延迟)
 * 2. 实时聚合 Binance 与 Hyperliquid 的 价格、深度盘口 与 实时资金费率
 * 3. 自动计算:
 *    - SKHY / SKHYNIX 比值 (k 值) 与跨交易所价差溢价
 *    - BRENT / WTI 原油价差 (d 差值) 与跨交易所偏离度
 *    - 两大交易所的资金费率折算与套利利差 (Annualized Funding APR Spread)
 * 4. 内置边缘短缓存 (TTL 1.5s)，防止多客户端并发打满交易所 API 限频
 */

// ==========================================
// 1. 服务端交易对与标的配置
// ==========================================
const CONFIG = {
  // Binance 永续合约代码
  BN: {
    SKHY: "SKHYUSDT",
    SKHYNIX: "SKHYNIXUSDT",
    BRENT: "BZUSDT",
    WTI: "CLUSDT"
  },
  // Hyperliquid xyz DEX 合约代码
  HL: {
    SKHY: "xyz:SKHY",
    SKHYNIX: "xyz:SKHX",
    BRENT: "xyz:BRENTOIL",
    WTI: "xyz:CL"
  }
};

// 内存短缓存对象 (Worker 全局生命周期内有效)
let serverCache = {
  data: null,
  timestamp: 0,
  latency: { bn: 0, hl: 0 }
};

// ==========================================
// 2. 服务端数据抓取与计算核心
// ==========================================
async function fetchAndCalculateMarketData() {
  const now = Date.now();
  // 1.2秒内请求直接走边缘缓存，降低网络耗时并防止 API 封禁
  if (serverCache.data && (now - serverCache.timestamp < 1200)) {
    return { ...serverCache.data, cached: true, serverAge: now - serverCache.timestamp };
  }

  const bnSymbols = Object.values(CONFIG.BN);
  const hlSymbols = Object.values(CONFIG.HL);

  // 并行请求 Binance 与 Hyperliquid
  const [bnResult, hlResult] = await Promise.allSettled([
    (async () => {
      const t0 = Date.now();
      // 获取 Binance 溢价与资金费率
      const pIdxPromise = fetch("https://fapi.binance.com/fapi/v1/premiumIndex", {
        headers: { "User-Agent": "Cloudflare-Worker" }
      }).then(r => r.json());

      // 获取 Binance 最佳买卖盘口
      const bookPromise = fetch("https://fapi.binance.com/fapi/v1/ticker/bookTicker", {
        headers: { "User-Agent": "Cloudflare-Worker" }
      }).then(r => r.json());

      const [pIdxList, bookList] = await Promise.all([pIdxPromise, bookPromise]);
      const latency = Date.now() - t0;

      const bnMap = {};
      if (Array.isArray(pIdxList)) {
        for (const item of pIdxList) {
          if (bnSymbols.includes(item.symbol)) {
            const fRate = parseFloat(item.lastFundingRate || 0);
            bnMap[item.symbol] = {
              markPrice: parseFloat(item.markPrice || 0),
              fundingRate: fRate, // 8小时费率
              fundingAPR: fRate * 3 * 365 * 100, // 年化收益率 %
              nextFundingTime: item.nextFundingTime || 0
            };
          }
        }
      }

      if (Array.isArray(bookList)) {
        for (const item of bookList) {
          if (bnMap[item.symbol]) {
            bnMap[item.symbol].bid = parseFloat(item.bidPrice || 0);
            bnMap[item.symbol].ask = parseFloat(item.askPrice || 0);
            bnMap[item.symbol].mid = (bnMap[item.symbol].bid + bnMap[item.symbol].ask) / 2 || bnMap[item.symbol].markPrice;
          }
        }
      }

      return { data: bnMap, latency, connected: true };
    })(),

    (async () => {
      const t0 = Date.now();
      const resp = await fetch("https://api.hyperliquid.xyz/info", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "metaAndAssetCtxs", dex: "xyz" })
      }).then(r => r.json());

      const latency = Date.now() - t0;
      const hlMap = {};

      if (Array.isArray(resp) && resp.length >= 2) {
        const universe = resp[0].universe || [];
        const ctxs = resp[1] || [];

        for (let i = 0; i < universe.length; i++) {
          const coinName = universe[i].name;
          if (hlSymbols.includes(coinName)) {
            const ctx = ctxs[i] || {};
            const fRate = parseFloat(ctx.funding || 0); // HL 为 1小时费率
            hlMap[coinName] = {
              markPrice: parseFloat(ctx.markPx || 0),
              midPrice: parseFloat(ctx.midPx || 0),
              fundingRate: fRate, // 1小时费率
              funding8hEquivalent: fRate * 8, // 折算为 8h 费率对比
              fundingAPR: fRate * 24 * 365 * 100, // 年化收益率 %
              oraclePrice: parseFloat(ctx.oraclePx || 0)
            };
          }
        }
      }

      return { data: hlMap, latency, connected: true };
    })()
  ]);

  const bn = bnResult.status === "fulfilled" ? bnResult.value : { data: {}, latency: 0, connected: false };
  const hl = hlResult.status === "fulfilled" ? hlResult.value : { data: {}, latency: 0, connected: false };

  // ==========================================
  // 3. 服务端计算衍生指标 (比值 k 与 差值 d)
  // ==========================================
  const getBnMid = (sym) => bn.data[sym]?.mid || bn.data[sym]?.markPrice || null;
  const getHlMid = (sym) => hl.data[sym]?.midPrice || hl.data[sym]?.markPrice || null;

  // 1. SKHY & SKHYNIX 统计套利计算
  const pSkhyBN = getBnMid(CONFIG.BN.SKHY);
  const pSkhxBN = getBnMid(CONFIG.BN.SKHYNIX);
  const pSkhyHL = getHlMid(CONFIG.HL.SKHY);
  const pSkhxHL = getHlMid(CONFIG.HL.SKHYNIX);

  const k_BN = (pSkhyBN && pSkhxBN) ? (pSkhyBN * 10 / pSkhxBN) : null;
  const k_HL = (pSkhyHL && pSkhxHL) ? (pSkhyHL * 10 / pSkhxHL) : null;
  const k_Diff = (k_BN && k_HL) ? (k_HL - k_BN) : null;
  const k_SpreadPct = (k_BN && k_HL) ? ((k_HL - k_BN) / k_BN * 100) : null;

  // 2. 原油 BRENT vs WTI 裂解与跨油种差值计算
  const pBrentBN = getBnMid(CONFIG.BN.BRENT);
  const pWtiBN = getBnMid(CONFIG.BN.WTI);
  const pBrentHL = getHlMid(CONFIG.HL.BRENT);
  const pWtiHL = getHlMid(CONFIG.HL.WTI);

  const d_BN = (pBrentBN && pWtiBN) ? (pBrentBN - pWtiBN) : null;
  const d_HL = (pBrentHL && pWtiHL) ? (pBrentHL - pWtiHL) : null;
  const d_Diff = (d_BN && d_HL) ? (d_HL - d_BN) : null;

  // 3. 资金费率利差计算 (Funding Rate Spread: Binance APR - Hyperliquid APR)
  const calcFundingSpread = (bnSym, hlSym) => {
    const fBN = bn.data[bnSym]?.fundingAPR;
    const fHL = hl.data[hlSym]?.fundingAPR;
    if (fBN !== undefined && fHL !== undefined) {
      return fBN - fHL; // 正值表示币安费率高（可做空币安+做多HL吃费）
    }
    return null;
  };

  const fundingSpreads = {
    SKHY: calcFundingSpread(CONFIG.BN.SKHY, CONFIG.HL.SKHY),
    SKHYNIX: calcFundingSpread(CONFIG.BN.SKHYNIX, CONFIG.HL.SKHYNIX),
    BRENT: calcFundingSpread(CONFIG.BN.BRENT, CONFIG.HL.BRENT),
    WTI: calcFundingSpread(CONFIG.BN.WTI, CONFIG.HL.WTI)
  };

  const result = {
    timestamp: now,
    status: {
      binance: { connected: bn.connected, latency: bn.latency },
      hyperliquid: { connected: hl.connected, latency: hl.latency }
    },
    prices: {
      SKHY: { bn: pSkhyBN, hl: pSkhyHL, diff: (pSkhyHL && pSkhyBN) ? pSkhyHL - pSkhyBN : null },
      SKHYNIX: { bn: pSkhxBN, hl: pSkhxHL, diff: (pSkhxHL && pSkhxBN) ? pSkhxHL - pSkhxBN : null },
      BRENT: { bn: pBrentBN, hl: pBrentHL, diff: (pBrentHL && pBrentBN) ? pBrentHL - pBrentBN : null },
      WTI: { bn: pWtiBN, hl: pWtiHL, diff: (pWtiHL && pWtiBN) ? pWtiHL - pWtiBN : null }
    },
    fundingRates: {
      SKHY: { bn: bn.data[CONFIG.BN.SKHY], hl: hl.data[CONFIG.HL.SKHY], spreadAPR: fundingSpreads.SKHY },
      SKHYNIX: { bn: bn.data[CONFIG.BN.SKHYNIX], hl: hl.data[CONFIG.HL.SKHYNIX], spreadAPR: fundingSpreads.SKHYNIX },
      BRENT: { bn: bn.data[CONFIG.BN.BRENT], hl: hl.data[CONFIG.HL.BRENT], spreadAPR: fundingSpreads.BRENT },
      WTI: { bn: bn.data[CONFIG.BN.WTI], hl: hl.data[CONFIG.HL.WTI], spreadAPR: fundingSpreads.WTI }
    },
    analytics: {
      ratio_k: { bn: k_BN, hl: k_HL, diff: k_Diff, spreadPct: k_SpreadPct },
      spread_d: { bn: d_BN, hl: d_HL, diff: d_Diff }
    }
  };

  // 更新服务端缓存
  serverCache.data = result;
  serverCache.timestamp = now;

  return result;
}

// ==========================================
// 4. 前端页面 HTML 模板 (极客暗黑风响应式 UI)
// ==========================================
function renderHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Binance & Hyperliquid 实时对冲与资金费监控系统</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <style>
        :root {
            --bg-main: #0c0d0e;
            --bg-card: #14171a;
            --bg-hover: #1b1f24;
            --border: #262c33;
            --green: #00f298;
            --red: #ff3b69;
            --blue: #00b8ff;
            --yellow: #ffd000;
            --text-main: #e6edf3;
            --text-sub: #7d8590;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Consolas", monospace;
            background: var(--bg-main);
            color: var(--text-main);
            padding: 16px;
            font-size: 14px;
        }
        .container { max-width: 1280px; margin: 0 auto; }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 20px;
        }
        h1 { font-size: 20px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 8px; }
        h1 .tag { font-size: 11px; background: #0070f3; color: #fff; padding: 2px 8px; border-radius: 4px; font-weight: 500; }
        .meta-bar { display: flex; align-items: center; gap: 16px; font-size: 13px; color: var(--text-sub); }
        .badge { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 20px; background: var(--bg-card); border: 1px solid var(--border); }
        .badge .dot { width: 8px; height: 8px; border-radius: 50%; }
        .dot.online { background: var(--green); box-shadow: 0 0 8px var(--green); }
        .dot.offline { background: var(--red); box-shadow: 0 0 8px var(--red); }

        /* 卡片聚合统计网格 */
        .analytics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 18px;
            position: relative;
            overflow: hidden;
        }
        .metric-card::before {
            content: "";
            position: absolute;
            left: 0; top: 0; bottom: 0;
            width: 4px;
            background: var(--blue);
        }
        .metric-card.gold::before { background: var(--yellow); }
        .metric-title { color: var(--text-sub); font-size: 13px; margin-bottom: 8px; display: flex; justify-content: space-between; }
        .metric-val-row { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 8px; }
        .metric-main { font-size: 24px; font-weight: 700; font-family: "Consolas", monospace; }
        .metric-sub { font-size: 12px; color: var(--text-sub); display: flex; justify-content: space-between; }

        /* 表格样式 */
        .table-wrap {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            overflow-x: auto;
            margin-bottom: 20px;
        }
        table { width: 100%; border-collapse: collapse; text-align: left; }
        th, td { padding: 12px 14px; border-bottom: 1px solid var(--border); }
        th { background: #111417; color: var(--text-sub); font-weight: 600; font-size: 12px; text-transform: uppercase; }
        tr:last-child td { border-bottom: none; }
        tr:hover td { background: var(--bg-hover); }

        .price { font-family: "Consolas", monospace; font-weight: 600; }
        .positive { color: var(--green); }
        .negative { color: var(--red); }
        .neutral { color: var(--text-sub); }
        .highlight { color: var(--blue); font-weight: bold; }
        .sub-rate { font-size: 11px; display: block; color: var(--text-sub); margin-top: 2px; }
        .apr-tag { font-size: 11px; padding: 1px 5px; border-radius: 3px; font-weight: 500; }
        .apr-pos { background: rgba(0, 242, 152, 0.12); color: var(--green); }
        .apr-neg { background: rgba(255, 59, 105, 0.12); color: var(--red); }

        footer { text-align: center; color: var(--text-sub); font-size: 12px; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>
                ⚡ Binance & Hyperliquid 聚合监控
                <span class="tag">Cloudflare 边缘服务端驱动</span>
            </h1>
            <div class="meta-bar">
                <div class="badge">
                    <span id="bn-dot" class="dot offline"></span>
                    <span>Binance: <b id="bn-lat">-</b></span>
                </div>
                <div class="badge">
                    <span id="hl-dot" class="dot offline"></span>
                    <span>Hyperliquid: <b id="hl-lat">-</b></span>
                </div>
                <span id="sync-time">同步中...</span>
            </div>
        </header>

        <!-- 关键套利指标大卡片 -->
        <div class="analytics-grid">
            <div class="metric-card">
                <div class="metric-title">
                    <span>SKHY / SKHYNIX 比值 (k 值)</span>
                    <span>公式: SKHY * 10 / SKHYNIX</span>
                </div>
                <div class="metric-val-row">
                    <div>
                        <span style="font-size: 12px; color: var(--text-sub);">HL 盘口 k: </span>
                        <span id="k-hl" class="metric-main highlight">-</span>
                    </div>
                    <div>
                        <span style="font-size: 12px; color: var(--text-sub);">BN 盘口 k: </span>
                        <span id="k-bn" class="metric-main" style="color: #aaa;">-</span>
                    </div>
                </div>
                <div class="metric-sub">
                    <span>跨所价差偏离: <b id="k-diff">-</b></span>
                    <span>偏离溢价率: <b id="k-pct">-</b></span>
                </div>
            </div>

            <div class="metric-card gold">
                <div class="metric-title">
                    <span>原油 Brent vs WTI 价差 (d 差值)</span>
                    <span>公式: BRENT - WTI</span>
                </div>
                <div class="metric-val-row">
                    <div>
                        <span style="font-size: 12px; color: var(--text-sub);">HL 盘口 d: </span>
                        <span id="d-hl" class="metric-main highlight">-</span>
                    </div>
                    <div>
                        <span style="font-size: 12px; color: var(--text-sub);">BN 盘口 d: </span>
                        <span id="d-bn" class="metric-main" style="color: #aaa;">-</span>
                    </div>
                </div>
                <div class="metric-sub">
                    <span>跨所价差偏离: <b id="d-diff">-</b></span>
                    <span>原油宏观套利参考</span>
                </div>
            </div>
        </div>

        <!-- 核心明细表格 (含实时资金费率) -->
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>品种 / 合约</th>
                        <th>Binance 价格 (USDT)</th>
                        <th>Binance 资金费率 (当期 / 年化)</th>
                        <th>Hyperliquid 价格 (USDC)</th>
                        <th>Hyperliquid 资金费率 (折合8h / 年化)</th>
                        <th>跨所差价 (HL - BN)</th>
                        <th>资金费利差 (年化 APR 差)</th>
                    </tr>
                </thead>
                <tbody id="data-tbody">
                    <tr><td colspan="7" style="text-align: center; color: var(--text-sub);">正在从 Cloudflare 边缘计算节点拉取实时数据...</td></tr>
                </tbody>
            </table>
        </div>

        <footer>
            <span>由 Cloudflare Workers 提供毫秒级边缘计算服务 · 数据在服务端完成抓取与校验 · 每 1.5 秒自动同步</span>
        </footer>
    </div>

    <script>
        const fmtNum = (n, d = 4) => (n !== null && n !== undefined && !isNaN(n)) ? Number(n).toFixed(d) : '-';
        const fmtPct = (n) => {
            if (n === null || n === undefined || isNaN(n)) return '-';
            const sign = n > 0 ? '+' : '';
            return sign + Number(n).toFixed(2) + '%';
        };

        async function fetchMarketData() {
            try {
                const res = await fetch('/api/data');
                if (!res.ok) throw new Error('API request failed');
                const d = await res.json();

                // 1. 更新顶部状态
                document.getElementById('bn-dot').className = 'dot ' + (d.status.binance.connected ? 'online' : 'offline');
                document.getElementById('bn-lat').textContent = d.status.binance.latency + 'ms';
                document.getElementById('hl-dot').className = 'dot ' + (d.status.hyperliquid.connected ? 'online' : 'offline');
                document.getElementById('hl-lat').textContent = d.status.hyperliquid.latency + 'ms';
                document.getElementById('sync-time').textContent = '更新时间: ' + new Date(d.timestamp).toLocaleTimeString();

                // 2. 更新套利卡片
                const k = d.analytics.ratio_k;
                document.getElementById('k-hl').textContent = fmtNum(k.hl, 6);
                document.getElementById('k-bn').textContent = fmtNum(k.bn, 6);
                document.getElementById('k-diff').innerHTML = formatDiff(k.diff, 6);
                document.getElementById('k-pct').innerHTML = formatDiff(k.spreadPct, 3, '%');

                const spread = d.analytics.spread_d;
                document.getElementById('d-hl').textContent = fmtNum(spread.hl, 3);
                document.getElementById('d-bn').textContent = fmtNum(spread.bn, 3);
                document.getElementById('d-diff').innerHTML = formatDiff(spread.diff, 3);

                // 3. 渲染主表格
                const symbols = [
                    { name: "SKHY", bnSym: "SKHYUSDT", hlSym: "xyz:SKHY", dec: 4 },
                    { name: "SKHYNIX", bnSym: "SKHYNIXUSDT", hlSym: "xyz:SKHX", dec: 2 },
                    { name: "BRENT 原油", bnSym: "BZUSDT", hlSym: "xyz:BRENTOIL", dec: 2 },
                    { name: "WTI 原油", bnSym: "CLUSDT", hlSym: "xyz:CL", dec: 2 }
                ];

                let rowsHtml = '';
                const symKeys = ["SKHY", "SKHYNIX", "BRENT", "WTI"];

                symKeys.forEach((key, idx) => {
                    const item = symbols[idx];
                    const px = d.prices[key] || {};
                    const fr = d.fundingRates[key] || {};

                    const bnFR = fr.bn ? (fr.bn.fundingRate * 100).toFixed(4) + '%' : '-';
                    const bnAPR = fr.bn ? fr.bn.fundingAPR : null;
                    const hlFR8h = fr.hl ? (fr.hl.funding8hEquivalent * 100).toFixed(4) + '%' : '-';
                    const hlAPR = fr.hl ? fr.hl.fundingAPR : null;
                    const spreadAPR = fr.spreadAPR;

                    rowsHtml += '<tr>';
                    rowsHtml += '<td><b>' + item.name + '</b><span class="sub-rate">' + item.bnSym + ' / ' + item.hlSym + '</span></td>';
                    rowsHtml += '<td class="price">' + fmtNum(px.bn, item.dec) + '</td>';
                    
                    // Binance 资金费率
                    rowsHtml += '<td>' + bnFR + ' ' + (bnAPR !== null ? '<span class="apr-tag ' + (bnAPR >= 0 ? 'apr-pos' : 'apr-neg') + '">年化 ' + fmtPct(bnAPR) + '</span>' : '') + '</td>';

                    // Hyperliquid 价格与资金费
                    rowsHtml += '<td class="price highlight">' + fmtNum(px.hl, item.dec) + '</td>';
                    rowsHtml += '<td>' + hlFR8h + ' ' + (hlAPR !== null ? '<span class="apr-tag ' + (hlAPR >= 0 ? 'apr-pos' : 'apr-neg') + '">年化 ' + fmtPct(hlAPR) + '</span>' : '') + '</td>';

                    // 跨所价差
                    rowsHtml += '<td>' + formatDiff(px.diff, item.dec) + '</td>';

                    // 资金费利差 (Funding APR Spread)
                    let spreadText = '-';
                    if (spreadAPR !== null && !isNaN(spreadAPR)) {
                        const direct = spreadAPR > 0 ? ' (空BN/多HL)' : ' (多BN/空HL)';
                        spreadText = '<span class="' + (spreadAPR > 0 ? 'positive' : 'negative') + '"><b>' + fmtPct(spreadAPR) + '</b></span>' +
                                     '<span class="sub-rate">' + direct + '</span>';
                    }
                    rowsHtml += '<td>' + spreadText + '</td>';
                    rowsHtml += '</tr>';
                });

                document.getElementById('data-tbody').innerHTML = rowsHtml;
            } catch (err) {
                console.error("数据拉取错误:", err);
            }
        }

        function formatDiff(val, dec = 4, suffix = '') {
            if (val === null || val === undefined || isNaN(val)) return '-';
            const cls = val > 0 ? 'positive' : (val < 0 ? 'negative' : 'neutral');
            const sign = val > 0 ? '+' : '';
            return '<span class="' + cls + '">' + sign + Number(val).toFixed(dec) + suffix + '</span>';
        }

        // 启动定时轮询 (每 1.5 秒更新一次)
        fetchMarketData();
        setInterval(fetchMarketData, 1500);
    </script>
</body>
</html>`;
}

// ==========================================
// 5. Cloudflare Workers 请求入口处理
// ==========================================
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 1. API 接口: 返回服务端计算与抓取的聚合数据
    if (url.pathname === "/api/data") {
      try {
        const data = await fetchAndCalculateMarketData();
        return new Response(JSON.stringify(data), {
          headers: {
            "Content-Type": "application/json; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store"
          }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }
    }

    // 2. 根路径或其它路径: 返回渲染好的前端监控界面
    return new Response(renderHTML(), {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "public, max-age=60"
      }
    });
  }
};
