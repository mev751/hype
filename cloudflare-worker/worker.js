/**
 * Binance & Hyperliquid 实时聚合与跨交易所套利监控系统 (v2.0 智能双引擎版)
 * 部署平台: Cloudflare Workers
 * 
 * 核心架构优化:
 * 1. 彻底解决 Binance 对 Cloudflare 数据中心 IP 的 451/403 地区封锁限制:
 *    - 服务端 (Worker) 负责抓取 Hyperliquid (无任何 IP 限制，毫秒级响应) 并计算基准指标;
 *    - 前端内置 Binance 原生 WebSocket 实时双向流 (@bookTicker 盘口 + @markPrice@1s 资金费率秒级推送);
 *    - 无论 Worker 运行在哪个国家机房、无论用户在何地，币安与 HL 两端数据 100% 秒级连通！
 * 2. 实时聚合与折算资金费率:
 *    - Binance 8h 费率 -> 年化 APR %
 *    - Hyperliquid 1h 费率 -> 折合8h等效与年化 APR %
 *    - 实时计算跨所利差 (Funding APR Spread) 与对冲操作建议
 * 3. 实时计算套利指标:
 *    - SKHY / SKHYNIX 比值 (k 值) 盘口与跨所溢价
 *    - BRENT / WTI 原油价差 (d 差值)
 */

const CONFIG = {
  BN: {
    SKHY: "SKHYUSDT",
    SKHYNIX: "SKHYNIXUSDT",
    BRENT: "BZUSDT",
    WTI: "CLUSDT"
  },
  HL: {
    SKHY: "xyz:SKHY",
    SKHYNIX: "xyz:SKHX",
    BRENT: "xyz:BRENTOIL",
    WTI: "xyz:CL"
  }
};

let serverCache = {
  data: null,
  timestamp: 0
};

// ==========================================
// 1. 服务端 Hyperliquid 与 Binance 抓取
// ==========================================
async function fetchServerMarketData() {
  const now = Date.now();
  if (serverCache.data && (now - serverCache.timestamp < 1200)) {
    return { ...serverCache.data, cached: true };
  }

  const bnSymbols = Object.values(CONFIG.BN);
  const hlSymbols = Object.values(CONFIG.HL);

  // 1. 抓取 Hyperliquid 数据 (极度稳定，无 IP 地区审查)
  const hlPromise = (async () => {
    const t0 = Date.now();
    try {
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
            const fRate = parseFloat(ctx.funding || 0); // 1h 费率
            hlMap[coinName] = {
              markPrice: parseFloat(ctx.markPx || 0),
              midPrice: parseFloat(ctx.midPx || 0),
              fundingRate: fRate,
              funding8hEquivalent: fRate * 8,
              fundingAPR: fRate * 24 * 365 * 100, // 年化收益率 %
              oraclePrice: parseFloat(ctx.oraclePx || 0)
            };
          }
        }
      }
      return { data: hlMap, latency, connected: true, error: null };
    } catch (err) {
      return { data: {}, latency: Date.now() - t0, connected: false, error: err.message };
    }
  })();

  // 2. 服务端尝试抓取 Binance (带真实浏览器指纹防拦截)
  const bnPromise = (async () => {
    const t0 = Date.now();
    try {
      const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json"
      };

      const [pIdxResp, bookResp] = await Promise.all([
        fetch("https://fapi.binance.com/fapi/v1/premiumIndex", { headers }),
        fetch("https://fapi.binance.com/fapi/v1/ticker/bookTicker", { headers })
      ]);

      if (!pIdxResp.ok) {
        return { data: {}, latency: Date.now() - t0, connected: false, error: `HTTP ${pIdxResp.status} (受币安机房限制)` };
      }

      const pIdxList = await pIdxResp.json();
      const bookList = bookResp.ok ? await bookResp.json() : [];
      const latency = Date.now() - t0;
      const bnMap = {};

      if (Array.isArray(pIdxList)) {
        for (const item of pIdxList) {
          if (bnSymbols.includes(item.symbol)) {
            const fRate = parseFloat(item.lastFundingRate || 0);
            bnMap[item.symbol] = {
              markPrice: parseFloat(item.markPrice || 0),
              fundingRate: fRate,
              fundingAPR: fRate * 3 * 365 * 100,
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

      return { data: bnMap, latency, connected: Object.keys(bnMap).length > 0, error: null };
    } catch (err) {
      return { data: {}, latency: Date.now() - t0, connected: false, error: err.message };
    }
  })();

  const [hlRes, bnRes] = await Promise.all([hlPromise, bnPromise]);

  const result = {
    timestamp: now,
    status: {
      binance: { connected: bnRes.connected, latency: bnRes.latency, error: bnRes.error },
      hyperliquid: { connected: hlRes.connected, latency: hlRes.latency, error: hlRes.error }
    },
    binance: bnRes.data,
    hyperliquid: hlRes.data
  };

  serverCache.data = result;
  serverCache.timestamp = now;
  return result;
}

// ==========================================
// 2. 前端页面渲染 (极客暗黑风 UI + 客户端原生双引擎)
// ==========================================
function renderHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Binance & Hyperliquid 实时对冲与资金费监控</title>
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
        .container { max-width: 1300px; margin: 0 auto; }
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
        .badge { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 20px; background: var(--bg-card); border: 1px solid var(--border); }
        .badge .dot { width: 8px; height: 8px; border-radius: 50%; }
        .dot.online { background: var(--green); box-shadow: 0 0 8px var(--green); }
        .dot.offline { background: var(--red); box-shadow: 0 0 8px var(--red); }
        .dot.hybrid { background: var(--yellow); box-shadow: 0 0 8px var(--yellow); }

        .analytics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
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
        .apr-tag { font-size: 11px; padding: 2px 6px; border-radius: 3px; font-weight: 500; }
        .apr-pos { background: rgba(0, 242, 152, 0.12); color: var(--green); }
        .apr-neg { background: rgba(255, 59, 105, 0.12); color: var(--red); }

        .banner-tip {
            background: rgba(0, 184, 255, 0.08);
            border: 1px solid rgba(0, 184, 255, 0.3);
            border-radius: 6px;
            padding: 10px 14px;
            font-size: 12px;
            color: #b3e5fc;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        footer { text-align: center; color: var(--text-sub); font-size: 12px; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>
                ⚡ Binance & Hyperliquid 实时对冲监控
                <span class="tag">双引擎智能架构</span>
            </h1>
            <div class="meta-bar">
                <div class="badge">
                    <span id="bn-dot" class="dot offline"></span>
                    <span>Binance: <b id="bn-status-text">连接中...</b></span>
                </div>
                <div class="badge">
                    <span id="hl-dot" class="dot offline"></span>
                    <span>Hyperliquid: <b id="hl-status-text">连接中...</b></span>
                </div>
                <span id="sync-time">正在建立高频流...</span>
            </div>
        </header>

        <div class="banner-tip">
            <span>🛡️ <b>双引擎保障</b>：Hyperliquid 数据由 Cloudflare 边缘计算毫秒级直连抓取；Binance 数据自动融合客户端原生 WebSocket (@bookTicker + @markPrice@1s 秒级资金费率)，彻底解除云厂商 IP 地区限制！</span>
        </div>

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
                    <span>跨所差价 (HL-BN): <b id="k-diff">-</b></span>
                    <span>溢价率: <b id="k-pct">-</b></span>
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
                    <span>跨所差价 (HL-BN): <b id="d-diff">-</b></span>
                    <span>原油裂解/跨产地参考</span>
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
                        <th>Binance 资金费率 (8h / 年化)</th>
                        <th>Hyperliquid 价格 (USDC)</th>
                        <th>Hyperliquid 资金费率 (折合8h / 年化)</th>
                        <th>跨所差价 (HL - BN)</th>
                        <th>资金费利差 (年化 APR 差)</th>
                    </tr>
                </thead>
                <tbody id="data-tbody">
                    <tr><td colspan="7" style="text-align: center; color: var(--text-sub);">正在接收行情与费率流...</td></tr>
                </tbody>
            </table>
        </div>

        <footer>
            <span>由 Cloudflare Workers + Binance 原生数据流驱动 · 双向心跳保活 · 实时资金费率每秒自动计算</span>
        </footer>
    </div>

    <script>
        const BN_SYMBOLS = ["SKHYUSDT", "SKHYNIXUSDT", "BZUSDT", "CLUSDT"];
        const HL_MAP = {
            "SKHY": "xyz:SKHY",
            "SKHYNIX": "xyz:SKHX",
            "BRENT": "xyz:BRENTOIL",
            "WTI": "xyz:CL"
        };

        // 数据存储中心
        let bnData = {
            "SKHYUSDT": { mid: null, bid: null, ask: null, fundingRate: null, fundingAPR: null },
            "SKHYNIXUSDT": { mid: null, bid: null, ask: null, fundingRate: null, fundingAPR: null },
            "BZUSDT": { mid: null, bid: null, ask: null, fundingRate: null, fundingAPR: null },
            "CLUSDT": { mid: null, bid: null, ask: null, fundingRate: null, fundingAPR: null }
        };

        let hlData = {};
        let bnWsConnected = false;
        let hlServerConnected = false;

        // ==========================================
        // 1. 启动客户端 Binance WebSocket (秒级极速直连)
        // ==========================================
        function initBinanceWS() {
            // 同时订阅每个品种的盘口行情(@bookTicker)与每秒资金费率(@markPrice@1s)
            const streams = [];
            BN_SYMBOLS.forEach(s => {
                const lower = s.toLowerCase();
                streams.push(lower + "@bookTicker");
                streams.push(lower + "@markPrice@1s");
            });

            const wsUrl = "wss://fstream.binance.com/stream?streams=" + streams.join('/');
            const ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                bnWsConnected = true;
                updateStatusUI();
            };

            ws.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    const stream = msg.stream || "";
                    const d = msg.data;
                    if (!d || !d.s) return;

                    const sym = d.s;
                    if (!bnData[sym]) return;

                    // 1. 盘口数据
                    if (stream.includes("@bookTicker")) {
                        const bid = parseFloat(d.b);
                        const ask = parseFloat(d.a);
                        bnData[sym].bid = bid;
                        bnData[sym].ask = ask;
                        bnData[sym].mid = (bid + ask) / 2;
                    }

                    // 2. 标记价格与实时资金费率
                    if (stream.includes("@markPrice")) {
                        const fRate = parseFloat(d.r || 0); // 8h 费率
                        bnData[sym].fundingRate = fRate;
                        bnData[sym].fundingAPR = fRate * 3 * 365 * 100;
                        if (!bnData[sym].mid) {
                            bnData[sym].mid = parseFloat(d.p);
                        }
                    }

                    renderUI();
                } catch (err) {}
            };

            ws.onerror = () => {
                bnWsConnected = false;
                updateStatusUI();
            };

            ws.onclose = () => {
                bnWsConnected = false;
                updateStatusUI();
                setTimeout(initBinanceWS, 3000);
            };
        }

        // ==========================================
        // 2. 从 Cloudflare Worker 服务端轮询 Hyperliquid
        // ==========================================
        async function fetchHyperliquidFromServer() {
            try {
                const res = await fetch('/api/data');
                if (!res.ok) throw new Error("Worker HTTP " + res.status);
                const d = await res.json();

                if (d.hyperliquid) {
                    hlData = d.hyperliquid;
                    hlServerConnected = d.status.hyperliquid.connected;
                }

                // 如果客户端 WebSocket 暂时未就绪且服务端成功拿到 Binance 数据，补充作为备用
                if (!bnWsConnected && d.binance && Object.keys(d.binance).length > 0) {
                    for (const [sym, val] of Object.entries(d.binance)) {
                        if (val.mid && !bnData[sym].mid) {
                            bnData[sym].mid = val.mid;
                            bnData[sym].fundingRate = val.fundingRate;
                            bnData[sym].fundingAPR = val.fundingAPR;
                        }
                    }
                }

                updateStatusUI(d.status.hyperliquid.latency);
                renderUI();
            } catch (err) {
                hlServerConnected = false;
                updateStatusUI();
            }
        }

        function updateStatusUI(hlLatency = null) {
            // Binance 状态
            const bnDot = document.getElementById('bn-dot');
            const bnText = document.getElementById('bn-status-text');
            if (bnWsConnected) {
                bnDot.className = "dot online";
                bnText.textContent = "🟢 直连就绪 (极速流)";
            } else {
                bnDot.className = "dot offline";
                bnText.textContent = "🔴 重连中...";
            }

            // Hyperliquid 状态
            const hlDot = document.getElementById('hl-dot');
            const hlText = document.getElementById('hl-status-text');
            if (hlServerConnected) {
                hlDot.className = "dot online";
                hlText.textContent = "🟢 边缘连通 " + (hlLatency ? "(" + hlLatency + "ms)" : "");
            } else {
                hlDot.className = "dot offline";
                hlText.textContent = "🔴 服务端同步中";
            }

            document.getElementById('sync-time').textContent = "更新时间: " + new Date().toLocaleTimeString();
        }

        // ==========================================
        // 3. 页面综合计算与渲染
        // ==========================================
        const fmtNum = (n, d = 4) => (n !== null && n !== undefined && !isNaN(n)) ? Number(n).toFixed(d) : '-';
        const fmtPct = (n) => {
            if (n === null || n === undefined || isNaN(n)) return '-';
            const sign = n > 0 ? '+' : '';
            return sign + Number(n).toFixed(2) + '%';
        };

        function formatDiff(val, dec = 4, suffix = '') {
            if (val === null || val === undefined || isNaN(val)) return '-';
            const cls = val > 0 ? 'positive' : (val < 0 ? 'negative' : 'neutral');
            const sign = val > 0 ? '+' : '';
            return '<span class="' + cls + '">' + sign + Number(val).toFixed(dec) + suffix + '</span>';
        }

        function renderUI() {
            // 1. 获取两端价格
            const pSkhyBN = bnData["SKHYUSDT"].mid;
            const pSkhxBN = bnData["SKHYNIXUSDT"].mid;
            const pBrentBN = bnData["BZUSDT"].mid;
            const pWtiBN = bnData["CLUSDT"].mid;

            const pSkhyHL = hlData[HL_MAP.SKHY]?.midPrice || null;
            const pSkhxHL = hlData[HL_MAP.SKHYNIX]?.midPrice || null;
            const pBrentHL = hlData[HL_MAP.BRENT]?.midPrice || null;
            const pWtiHL = hlData[HL_MAP.WTI]?.midPrice || null;

            // 2. 计算 k 值与差值
            const k_BN = (pSkhyBN && pSkhxBN) ? (pSkhyBN * 10 / pSkhxBN) : null;
            const k_HL = (pSkhyHL && pSkhxHL) ? (pSkhyHL * 10 / pSkhxHL) : null;
            const k_Diff = (k_BN && k_HL) ? (k_HL - k_BN) : null;
            const k_Pct = (k_BN && k_HL) ? ((k_HL - k_BN) / k_BN * 100) : null;

            const d_BN = (pBrentBN && pWtiBN) ? (pBrentBN - pWtiBN) : null;
            const d_HL = (pBrentHL && pWtiHL) ? (pBrentHL - pWtiHL) : null;
            const d_Diff = (d_BN && d_HL) ? (d_HL - d_BN) : null;

            // 3. 更新大卡片
            document.getElementById('k-hl').textContent = fmtNum(k_HL, 6);
            document.getElementById('k-bn').textContent = fmtNum(k_BN, 6);
            document.getElementById('k-diff').innerHTML = formatDiff(k_Diff, 6);
            document.getElementById('k-pct').innerHTML = formatDiff(k_Pct, 3, '%');

            document.getElementById('d-hl').textContent = fmtNum(d_HL, 3);
            document.getElementById('d-bn').textContent = fmtNum(d_BN, 3);
            document.getElementById('d-diff').innerHTML = formatDiff(d_Diff, 3);

            // 4. 表格行渲染
            const rowConfigs = [
                { name: "SKHY", bnSym: "SKHYUSDT", hlSym: HL_MAP.SKHY, dec: 4 },
                { name: "SKHYNIX", bnSym: "SKHYNIXUSDT", hlSym: HL_MAP.SKHYNIX, dec: 2 },
                { name: "BRENT 原油", bnSym: "BZUSDT", hlSym: HL_MAP.BRENT, dec: 2 },
                { name: "WTI 原油", bnSym: "CLUSDT", hlSym: HL_MAP.WTI, dec: 2 }
            ];

            let html = '';
            rowConfigs.forEach(item => {
                const bn = bnData[item.bnSym] || {};
                const hl = hlData[item.hlSym] || {};

                const bnPrice = bn.mid;
                const hlPrice = hl.midPrice || null;
                const priceDiff = (bnPrice && hlPrice) ? (hlPrice - bnPrice) : null;

                const bnFR = bn.fundingRate !== null ? (bn.fundingRate * 100).toFixed(4) + '%' : '-';
                const bnAPR = bn.fundingAPR;
                const hlFR = hl.funding8hEquivalent !== undefined ? (hl.funding8hEquivalent * 100).toFixed(4) + '%' : '-';
                const hlAPR = hl.fundingAPR;

                let spreadText = '-';
                if (bnAPR !== null && hlAPR !== undefined && !isNaN(bnAPR) && !isNaN(hlAPR)) {
                    const spread = bnAPR - hlAPR;
                    const direct = spread > 0 ? ' (空BN/多HL)' : ' (多BN/空HL)';
                    spreadText = '<span class="' + (spread > 0 ? 'positive' : 'negative') + '"><b>' + fmtPct(spread) + '</b></span>' +
                                 '<span class="sub-rate">' + direct + '</span>';
                }

                html += '<tr>';
                html += '<td><b>' + item.name + '</b><span class="sub-rate">' + item.bnSym + ' / ' + item.hlSym + '</span></td>';
                html += '<td class="price">' + fmtNum(bnPrice, item.dec) + '</td>';
                html += '<td>' + bnFR + ' ' + (bnAPR !== null ? '<span class="apr-tag ' + (bnAPR >= 0 ? 'apr-pos' : 'apr-neg') + '">年化 ' + fmtPct(bnAPR) + '</span>' : '') + '</td>';
                html += '<td class="price highlight">' + fmtNum(hlPrice, item.dec) + '</td>';
                html += '<td>' + hlFR + ' ' + (hlAPR !== undefined ? '<span class="apr-tag ' + (hlAPR >= 0 ? 'apr-pos' : 'apr-neg') + '">年化 ' + fmtPct(hlAPR) + '</span>' : '') + '</td>';
                html += '<td>' + formatDiff(priceDiff, item.dec) + '</td>';
                html += '<td>' + spreadText + '</td>';
                html += '</tr>';
            });

            document.getElementById('data-tbody').innerHTML = html;
        }

        // 启动双引擎
        initBinanceWS();
        fetchHyperliquidFromServer();
        setInterval(fetchHyperliquidFromServer, 1500);
    </script>
</body>
</html>`;
}

// ==========================================
// 3. Worker 请求分发入口
// ==========================================
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/api/data") {
      try {
        const data = await fetchServerMarketData();
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

    return new Response(renderHTML(), {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "public, max-age=60"
      }
    });
  }
};
