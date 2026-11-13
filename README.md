# SKHY/SKHX 相对价值做空网格机器人

基于 Hyperliquid dex（`xyz`）的**相对价值网格策略**：同时做空 `xyz:SKHY`、做多 `xyz:SKHX`（即 SKHYNIX），在 K 值区间内来回收割波动利润。

- 标的 A：`xyz:SKHY`（程序内显示为 SKHY-USDC）
- 标的 B：`xyz:SKHX`（程序内显示为 SKHYNIX-USDC）

> ⚠️ 本程序涉及真实资金与杠杆交易。请先跑模拟模式，自行确认 SDK 版本、账户模式、杠杆设置与风险预算后再上实盘。

---

## 目录结构

| 文件 | 说明 |
|---|---|
| `HL_SKHY_STRAT_3.9.py` | 主程序（Python 版，含参数依据注释） |
| `cmd/hlstrat/` | 主程序（**Go 版**入口，`HL_SKHY_STRAT_3.9.py` 的重写） |
| `internal/hl/` | Go 版内核：EIP-712 签名下单、行情、网格状态机、对账、终端 UI |
| `hl_ratio_alert.py` | 比率监控 + Bark 手机报警（只读行情，不下单） |
| `.github/workflows/go-build.yml` | GitHub Actions：Go 交叉编译 + 签名向量测试 |
| `README.md` | 本文档 |

---

## 环境要求与安装

- Python 3.9+
- 依赖：

```bash
pip install hyperliquid-python-sdk eth_account websocket-client requests
```

---

## 策略核心逻辑

### K 值（价差信号）

```
K = SKHY买一价 × 10 / SKHX卖一价
```

当前参考（2026-11-08）：`128.93 × 10 / 872.88 ≈ 1.4775`

### 网格规则

- **建仓（做空方向）**：当 `K ≥ P1 + 已持层数 × STEP_X` 时，新开一层：做空 SKHY `Q_USDC` 名义 + 做多 SKHX `Q_USDC` 名义。
- **止盈**：当 `K ≤ P1 + (已持层数 − 2) × STEP_X` 时，平掉最上层：买回 SKHY + 卖出 SKHX，实现价差利润。
- 两腿名义按 `Q_USDC` 对齐，`HEDGE_IMBALANCE_TOLERANCE=15%` 内视为健康。

### 当前参数配置区

| 参数 | 值 | 含义 |
|---|---|---|
| `P1` | 1.450 | 建仓基准线（区间下沿） |
| `STEP_X` | 0.003 | 网格步长（贴住现价：跌 0.0035 即收割第一层） |
| `MAX_GRIDS` | 14 | 最大建仓层数（建仓线 1.450 ~ 1.489） |
| `Q_USDC` | 1500 | 每层每条腿目标名义价值（USDC） |
| `LEVERAGE` | 5 | 期望杠杆（启动时同步到交易所，两币上限 10x） |
| `SPLIT_N` | 1 | 单层拆单次数（首次启动可临时调大） |
| `SLICE_DELAY` | 0.75 | 拆单间隔（秒） |

**启动定位（K≈1.4775）**：目标堆 10 层（10×1500=15000U/腿，对名义 30000U），下一建仓线 **1.480**，第一止盈线 **1.474**（回调 0.0035 即收割，随后 1.471…1.447 每跌 0.003 收一层）。

**收益参考（taker 费率约 0.009%，growth mode 币种）**：
- 单格完整循环毛利 ≈ 3.05U，4 笔手续费 ≈ 0.54U，净 ≈ 2.5U（步长小 → 单利薄、次数多）
- K 从 1.4775 滑落至 1.447（全堆 10 层收割）毛利 ≈ 175U

---

## 启动方式

### 1. 模拟模式（无密钥，先验证）

```bash
python3 HL_SKHY_STRAT_3.9.py
```

不设置 `HL_SECRET_KEY` 时自动进入模拟模式：行情照常打、网格逻辑照常跑，但不下真实单，可确认对准逻辑与触发线是否符合预期。

### 2. 实盘模式

先注入环境变量（私钥不要写进源码）：

```bash
export HL_SECRET_KEY='0x你的私钥'
export HL_WALLET_ADDR='0x你的钱包地址'   # 可选，不填自动从私钥推导
export HL_STOP_LOSS_USDC=500            # 可选：浮动亏损≤-500U 时紧急全平；0 表示关闭
export HL_LOG_PATH=./strat.log          # 可选：日志路径，默认 hype_strategy.log

python3 HL_SKHY_STRAT_3.9.py
```

启动流程（自动）：

1. 读取链上仓位，校验两腿方向（SKHY 空、SKHX 多）、对冲比例与层数。
2. 计算目标层数 vs 当前层数；若有差额，终端显示**对准确认**——输入 `1` 回车执行，其他内容退出。
3. 对准完成后启动交易工作线程，开始网格循环。
4. 主界面每 0.3s 刷新一次：账户资产、盈亏拆解、盘口、网格触发线、底层日志。

### 3. 首次启动注意（一次性建仓深度）

当前 K≈1.4775，启动对准会**一次性建满 10 层**（买入约 17.2 币 SKHX）。0.02% 滑点限价恰好覆盖 SKHX 前 5 档累计约 21.2 币，**正常情况下整体可成交**；若遇到盘口深度瞬时不足导致部分成交，程序会因"链上对账不一致"触发停机保护。此时临时调整，跑通首轮后还原：

```python
PAIR_SLIPPAGE_PCT = 0.1   # 放宽 IOC 限价（默认 0.02）
# 或将
SPLIT_N = 4~6             # 拆单，每片 ~3~5 币，配合 SLICE_DELAY 分批成交
```

---

## Go 版本（`cmd/hlstrat`）

`HL_SKHY_STRAT_3.9.py` 的功能等价重写：策略参数、下单顺序、回滚与对账逻辑一一对应，环境变量也完全一致（`HL_SECRET_KEY` / `HL_WALLET_ADDR` / `HL_STOP_LOSS_USDC` / `HL_LOG_PATH`）。不设 `HL_SECRET_KEY` 即为模拟模式。

技术要点：

- **不依赖第三方交易 SDK**：REST（`/info`、`/exchange`）、EIP-712 签名（msgpack 动作哈希 + `Agent` 类型域）、secp256k1 全部自行实现，签名结果与 `hyperliquid-python-sdk` **逐字节一致**（`internal/hl/sign_test.go` 内置 Python 生成的固定向量，`go test ./...` 校验）。
- builder-deployed dex 的 asset id 按官方规则推导：`110000 + dex序号×10000 + 币种下标`（当前 `xyz:SKHX` = 110022、`xyz:SKHY` = 110099）。
- 行情订阅 `wss://api.hyperliquid.xyz/ws` 的 `l2Book`，断线退避重连；交易仍由**单协程串行执行**，与 v3.9 的"单工作线程"设计一致。

本地构建（Go 1.25+）：

```bash
go build -trimpath -o hlstrat ./cmd/hlstrat
./hlstrat                        # 模拟模式
HL_SECRET_KEY='0x...' ./hlstrat  # 实盘
go test ./...                    # 校验签名向量与数值格式
```

不装 Go 也可以直接用 GitHub Actions 产物：推送后到仓库 **Actions → go build** 下载，覆盖 Linux amd64/arm64 与 Windows amd64（如 `hlstrat-linux-amd64`），静态编译、免运行时依赖。

---

## 安全与保护机制

- **行情过期保护**：任一腿盘口超过 5 秒未更新即禁止交易。
- **成对下单**：先建/平 SKHX 再建/平 SKHY；失败时定向回滚，回滚失败尝试自动全平。
- **链上对账**：每次网格动作后回读链上仓位，方向/层数/对冲偏差异常立即停机（默认不全平，避免误平手工仓位）。
- **保证金缓冲**：可用保证金低于 `2×Q/LEVERAGE×1.5` 时锁定当前层数为上限，不再新增。
- **紧急停机**：浮动亏损达 `HL_STOP_LOSS_USDC` 或交易异常时自动全平/停机。
- **Ctrl+C 停止**：仅停交易线程，默认**不平仓**（`FLATTEN_ON_EXIT=False`；如需退出时全平改 `True`）。

---

## 常见停机原因排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `[⛔ 已停机] 链上仓位不一致: 单腿裸露` | 部分成交/回滚失败导致只剩一腿 | 人工核对，修复后再启动 |
| `[⛔ 已停机] 链上推导层数 X 与内部层数 Y 不一致` | 一次性建仓深度不足（部分成交） | 临时调大 `PAIR_SLIPPAGE_PCT` 或 `SPLIT_N` 重跑 |
| `[⚠️ 保证金缓冲不足] 锁定第 N 层` | 可用保证金不够新一层 | 加仓资金或减小 `Q_USDC`/`MAX_GRIDS` |
| `[错误] 启动仓位检查失败` | 链上有反方向或失衡仓位 | 人工处理后再启动 |

---

## 调参指引（不要随意改）

- 步长 `STEP_X`：参考近 14 天 K 值日均振幅（约 0.022）与目标区间宽度。0.003~0.007 均可；**步长小 = 单利薄、次数多**（0.003 在 0.009% 费率下仍盈利），低于 0.003 让价位小数粒度噪声占比上升。
- 每层 `Q_USDC`：受 SKHX 盘口厚度约束（卖一约 1.7 币/1.5kU），单笔 ≤1500 基本零滑点。
- 层数 `MAX_GRIDS`：`P1 + MAX_GRIDS×STEP_X` 应覆盖目标波段；当前 14 层覆盖 1.450~1.489。步长越小，相同区间需要的层数越多。
- 杠杆 `LEVERAGE`：只影响保证金占用率（2×Q×层数/杠杆），**不直接放大利润**；利润来自实际部署名义（Q×层数）。

---

## 比率报警脚本（`hl_ratio_alert.py`）

只订阅 `xyz:SKHY` / `xyz:SKHX` 的 l2Book，计算 `K = SKHY中价 × 10 / SKHX中价`，越过阈值时通过 Bark 推送 iOS 锁屏强提醒。只读行情，不下单、不碰私钥。

> ⚠️ 币种名必须带 `xyz:` 前缀。若写成 `SKHY` / `SKHYNIX`，Hyperliquid 不会返回错误，而是**直接断开连接**，表现为"连上就断、无限重连"。

| 参数 | 默认 | 含义 |
|---|---|---|
| `LOWER_THRESHOLD` | 1.445 | 低于此值报警（alarm 铃声） |
| `UPPER_THRESHOLD` | 1.477 | 高于此值报警（强提醒） |
| `COOLDOWN` | 600 | 同一状态重复提醒最短间隔（秒） |
| `STALE_SECONDS` | 30 | 行情超过该秒数未更新则不判断、不报警 |

Bark Key 不写进源码，用环境变量注入：

```bash
export BARK_KEY='你的Bark Key'
python3 hl_ratio_alert.py

python3 hl_ratio_alert.py --test-alert    # 只发一条测试通知，验证 Key
```

服务器上以 systemd 常驻，Key 放在 `/etc/skhy-alert.env`（不要提交进仓库）：

```bash
sudo systemctl status skhy-alert
sudo journalctl -u skhy-alert -f
```

---

## 风险提示

- 价差网格收益依赖 K 在区间内来回波动；**K 单边突破 1.489 上沿**时最多 14 层封顶，只能满仓扛回撤（上破 1.52 浮亏约 −480U，1.55 约 −820U，满载粗估）。
- **下破 1.447** 全堆收割清仓，K 需回到 ≥1.45 才重新进场；近 14 天约 10 天 K 在 1.40~1.45，若均值回归到 1.45 以下，本网格会空转（选此区间 = 押注 K 维持 1.45~1.49）。
- 两腿仅"按名义对齐"，并非完全对冲，绝对涨跌与资金费率仍会带来误差。
- 首次实盘务必小额/模拟先行；历史收益数据不代表未来表现。
