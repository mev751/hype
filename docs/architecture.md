# HFT 平台架构（Hyperliquid × Binance 跨所套利）

> 状态：设计草案（Phase 0/1）· 作者：mev751 · 最后更新：2026-11-13
>
> 关联：[ADR-0001 Rust 热路径与 FFI 边界](adr/0001-rust-hotpath-ffi.md)

---

## 1. 定位，以及刻意不做的事

**做什么**：Hyperliquid 与 Binance 之间的跨所相对价值/套利系统。标的以 SK 海力士系为主
（HL `xyz:SKHY` / `xyz:SKHX`，Binance 侧对应合约见 §3.1 待核实清单），后续可扩展到同一框架下的其它标的。

**不做什么**：

- 不做"抢队列的微秒级 HFT"。Hyperliquid 只提供 HTTPS+JSON 下单、无 FIX、无 colocation、前置 Cloudflare，
  端到端下限由公网 RTT 决定（10–30 ms）。任何微秒级优化对总时延的贡献 < 2%。
- 不做链上抢跑/MEV。结算在 Hyperliquid 自有 L1，我们只做中心化订单流的低延迟。
- 不在热路径使用 Python。现有 `HL_SKHY_STRAT_3.9.py` 保留为策略语义参考与回测对照。

**目标指标**（Phase 3 验收）：

| 指标 | 目标 | 说明 |
|---|---|---|
| 行情到达 → 决策完成 | p99 < 200 µs | 单策略，无分配热路径 |
| 决策 → 订单发出 | p99 < 150 µs | 含 msgpack/keccak/ECDSA，签名在独立协程 |
| 端到端下单（send→ack） | 受 RTT 支配，目标机房 p99 < 40 ms | 机房选址决定 |
| 行情缺口检测 | 100% 覆盖 | 缺口即禁止交易，不降级使用旧价 |
| 回放确定性 | 同输入逐笔复现 | 记录原始帧 + 单写者状态机 |
| 进程故障恢复 | 自动重启 + 仓位对账 < 30 s | systemd + 独立 control-plane |

---

## 2. 约束与事实（写进架构前提）

| 项 | Hyperliquid | Binance |
|---|---|---|
| 行情 | WS `l2Book`，**全量快照**（约 20 档），无增量 diff | WS 深度增量流（需要 order book 重建） |
| 下单 | `POST /exchange`，JSON + EIP-712 签名，`cloid` 幂等 | REST，`newClientOrderId` 幂等，`recvWindow` 依赖时钟同步 |
| 身份 | 私钥签名（每单都签） | API Key + HMAC |
| 限速 | 按地址权重 + 按 IP（约 1200 req/min） | 权重制，下单 10 weight/单 |
| 结算 | 自有 L1，出块约 0.1 s 量级 | 中心化撮合，即时回报 |
| 资金费 | 连续结算（约每小时） | 每 8 小时 |
| 特殊性 | builder-deployed dex 的 asset id = `110000 + dex序号×10000 + 下标` | 合约规格（tick/minNotional）按 symbol 查询 |

**结论**：两条腿的撮合特性差异很大——HL 侧是稀疏快照 + 链上结算，BN 侧是密集增量 + 中心化即时回报。
跨所套利的实际瓶颈在"腿间时间差"与"库存风险"，不在单一环节的纳秒。

---

## 3. 系统边界

### 3.1 跨所标的映射（**待核实，Phase 0 必须先确认**）

| 语义 | Hyperliquid | Binance | 状态 |
|---|---|---|---|
| SKHY（ADR / 美股） | `xyz:SKHY` | `SKHYUSDT`？ | **待核实**：Binance 是否有该合约、合约类型（U 本位/币本位）、上市时间 |
| SKHYNIX（韩股本尊） | `xyz:SKHX` | `SKHYNIXUSDT`？ | **待核实** |
| 布伦特原油 | `xyz:BRENTOIL` | `BZUSDT`？ | 待核实 |
| WTI | `xyz:CL` | `CLUSDT`？ | 待核实 |

> 若 Binance 无对应合约，则跨所腿改用其它 venue（或改用 ETF/指数代理），
> 框架层面通过 `venue.Venue` 接口隔离，不影响其余设计。

### 3.2 系统边界图

```
                     ┌────────────── 外部 ──────────────┐
   Hyperliquid WS ───┤                                  ├─── Binance WS
   Hyperliquid REST ─┤                                  ├─── Binance REST
                     └──────────────────────────────────┘
                                   │
   ┌───────────────────────────────▼─────────────────────────────────┐
   │  hftd（Phase 1 单进程，内存通道）                                  │
   │                                                                  │
   │  feed-gateway ──► book-cache ──► strategy ──► risk-gate ──► exec │
   │       │                              ▲            │             │
   │       └──► recorder（原始帧落盘）      └── fills ──┘             │
   │                                                                  │
   │  portfolio / reconciler（仓位视图 + 对账）                          │
   │  obs：每跳时间戳、指标、结构化日志                                  │
   └───────────────┬──────────────────────────────────────────────────┘
                   │ unix socket（控制面，独立进程）
   ┌───────────────▼──────────────┐
   │ controld：start/stop/flatten │  独立于策略进程，策略挂了也能全平
   │ kill-switch / 参数热加载      │
   └──────────────────────────────┘
```

---

## 4. 组件职责

| 组件 | 职责 | 不做 |
|---|---|---|
| `feed-gateway` | 每 venue 一条 WS 连接；归一化为 `venue.Snapshot`；心跳/缺口检测；原始帧交给 recorder | 不做策略判断 |
| `book-cache` | 双缓冲发布归一化盘口；读侧无锁 | 不落历史（历史在 recorder） |
| `recorder` | 原始 WS 帧 + 接收时间戳分段落盘（zstd），供回放/复盘 | 不参与实时决策 |
| `strategy` | 单写者状态机；消费 snapshot/fill/timer；产出 `OrderIntent` | 不直接调 venue API |
| `risk-gate` | 预交易校验：名义、净额、单量、速率、日亏、kill-switch 状态 | 不修改订单内容 |
| `exec` | 订单路由：幂等（cloid/clientOrderId）、超时、重试、ack/fill 跟踪、拒单分类 | 不做风控判断 |
| `portfolio` | 跨所统一仓位视图（含两腿方向、对冲偏差、未实现盈亏） | 不作为唯一真相（链上/交易所为准） |
| `reconciler` | WS 成交流 + 定时全量对账；不一致即触发停机 | 不自动改仓 |
| `controld` | 启停、全平、参数热加载、kill-switch 翻转 | 不参与撮合 |
| `obs` | 每跳 monotonic 时间戳、Prometheus 指标、结构化日志 | — |

---

## 5. 线程模型与数据流

**单写者原则**：策略状态只能由一条 goroutine 修改。行情、成交、定时器都通过同一个 channel 进入策略循环，
循环内不阻塞（不做网络 I/O）。这保证了：

1. 无锁、无竞态；
2. 相同输入 → 相同输出（回放可复现）；
3. 延迟可预测（不会因为锁竞争抖到毫秒级）。

```
WS reader goroutine ──(Snapshot, recvNS)──► snapshotCh ──┐
WS fills goroutine ──(Fill, recvNS)──────► fillCh ───────┼──► strategy loop（单 goroutine）
timer ticker ───────────────────────────► tickCh ───────┘        │
                                                                 OrderIntent
                                                                 ▼
                                                     risk gate（同步，无锁状态）
                                                                 │
                                                                 ▼
                                                 exec（异步：签名 + POST + ack 跟踪）
```

**通道语义**：`snapshotCh` 容量固定，满时**丢弃最旧**并记 `feed_drop_total`（宁可丢行情，不可阻塞 reader）；
成交通道不可丢（容量放大 + 阻塞 reader 是可接受代价）。

**executor 并发**：签名在 N 个 worker 上并行，但**发送按 nonce 顺序**（Hyperliquid L1 动作 nonce 单调）。
签名耗时 ~50 µs，单 worker 已足够到 20k 单/秒，Phase 1 用 2 个 worker + 有序发送。

---

## 6. 接口契约（骨架已落代码）

| 契约 | 文件 | 说明 |
|---|---|---|
| `venue.Venue` | `internal/hft/venue/venue.go` | 行情流、下单、撤单、仓位、费率；跨所统一 |
| `venue.Snapshot` | 同上 | 归一化盘口 + 序列号 + 交易所时间 + 接收时间 |
| `book.Cache` | `internal/hft/book/book.go` | 双缓冲，读侧 RLock |
| `strategy.Strategy` / `strategy.Context` | `internal/hft/strategy/strategy.go` | 单写者回调 + 只读上下文 |
| `risk.Gate` / `risk.KillSwitch` | `internal/hft/risk/risk.go` | 预交易校验；kill-switch 可跨进程 |
| `exec.Router` | `internal/hft/exec/exec.go` | 幂等提交、ack/fill 跟踪 |
| `portfolio.Snapshot` | `internal/hft/portfolio/portfolio.go` | 跨所仓位视图 |
| `obs.Stamp` / `obs.Recorder` | `internal/hft/obs/obs.go` | 每跳时间戳 + 原始帧记录 |
| `hotpath.Signer` | `internal/hft/hotpath/hotpath.go` | 纯 Go 实现 + Rust FFI 实现（构建标签切换） |

**接口稳定性约定**：`venue.Venue` 与 `strategy.Strategy` 是长期契约，变更需 ADR；
其余为内部实现，可自由重构。

---

## 7. Go / Rust 分工

| 层 | 语言 | 理由 |
|---|---|---|
| feed 连接、归一化、book cache | Go | 网络 I/O 为主，GC 压力可控 |
| 策略状态机、风控、仓位、对账 | Go | 逻辑复杂、需要可读性与快速迭代 |
| 控制面、指标、CLI | Go | 无延迟要求 |
| **纯计算热路径**：msgpack 编码、keccak、secp256k1 签名、Binance 签名/HMAC | **Rust**（cdylib，C ABI） | 无 GC、可精确控制分配；每单省 ~30–60 µs |
| 行情帧解析（Phase 4 再评估） | 可能 Rust | 只有实测证明 Go 解析成为瓶颈才迁移 |

**边界原则**：Rust 只做无状态纯函数（输入 bytes → 输出 bytes），不得持有状态、不得做 I/O。
Go 侧保留 `hotpath` 纯 Go 实现作为 fallback（构建标签 `pure`），Rust 不可用时可编译运行。

详见 [ADR-0001](adr/0001-rust-hotpath-ffi.md)。

---

## 8. 延迟预算与测量

| 阶段 | 当前估 | 优化手段 | 验收 |
|---|---|---|---|
| WS 帧到达 → 解析完成 | 50–200 µs | 预分配 buffer；struct 解码（禁 `map[string]any`）；单读协程 | `feed_parse_ns` p99 |
| 归一化 → 入队 | 5–20 µs | 复用对象池；避免复制深度数组 | `feed_enqueue_ns` |
| 策略决策 | 1–50 µs | 单写者 + 无分配 + 预计算阈值 | `decision_ns` p99 |
| msgpack+keccak+ECDSA | 40–80 µs（Go）/ 20–50 µs（Rust） | Rust 热路径 + 签名池 | `sign_ns` p99 |
| 发送（热 HTTP/2 连接） | 1–5 µs | 长连接、keepalive、禁用按请求建连 | `send_ns` |
| 服务端 ack | RTT（10–40 ms） | **机房选址** | `ack_rtt_ns` p99 |

**测量优先于优化**：Phase 0 交付 `cmd/latency-probe`，输出上述各段的 p50/p90/p99
以及 `api.hyperliquid.xyz` 在候选机房的 RTT，用真实数字决定：
1. 机房位置（us-east / 东京 / 法兰克福）；
2. Rust 热路径是否值得（若 RTT 占比 > 95%，则推迟）；
3. 是否需要多连接扇出。

---

## 9. 风险与故障处理

| 场景 | 检测 | 动作 |
|---|---|---|
| 行情缺口/断流 | 序列号跳变、`time` 字段倒退、心跳延迟 > 1 s | 该 venue 标记 stale，策略禁止开新仓；已有仓位仅允许减仓 |
| 下单超时 | `ack_rtt` 超阈值 | 先 `orderStatus`/`queryOrder` 查状态，**绝不直接重发**；确认未成交才重试（同一 cloid/clientOrderId） |
| 单腿成交、另一腿失败 | 成对下单后仓位不一致 | 定向回滚 → 失败则 `controld` 降杠杆/全平；进入 `halted` |
| 跨所库存失衡 | notional 偏差 > 阈值（默认 15%） | 停止加仓，仅允许对冲方向交易 |
| 浮动亏损超限 | 实时 PnL | kill-switch：独立进程全平 + 停止策略 |
| 限速触发 | HTTP 429/权重错误 | 令牌桶退避；连续 N 次则暂停该 venue |
| 时钟漂移（Binance `recvWindow`） | 本地/服务器时间差 | 启动时校时，漂移 > 1 s 拒绝交易 |
| 进程崩溃 | systemd 重启 | 重启后强制与交易所对账，不一致则 halt（不自动平仓） |

**kill-switch 设计**：`/run/hft/killswitch` 文件 + `controld` unix socket 双通道；
`exec` 在每次发送前检查本地缓存标志（每 100 ms 刷新），保证策略进程卡死时仍能阻断下单。

---

## 10. 观测与告警

**指标清单**（Prometheus，前缀 `hft_`）：

| 指标 | 类型 | 建议告警 |
|---|---|---|
| `hft_feed_gap_total{venue}` | counter | 5 min 内 > 0 → 告警 |
| `hft_feed_staleness_seconds{venue,symbol}` | gauge | > 2 s → 告警 |
| `hft_decision_ns` / `hft_sign_ns` / `hft_ack_rtt_ns` | histogram | p99 超预算 |
| `hft_order_total{venue,status}` | counter | reject 比例 > 5% |
| `hft_position_mismatch_usd` | gauge | > 50 USD → 告警 |
| `hft_killswitch_state` | gauge | = 1 → 告警 |
| `hft_strategy_pnl_usd` | gauge | 日内 < -阈值 |
| `hft_ws_reconnect_total{venue}` | counter | 10 min 内 ≥ 3 |

**日志**：结构化 JSON，字段固定 `ts_ns, level, component, venue, symbol, client_id, msg, kv`；
订单全生命周期以 `client_id` 串联（HL `cloid` / BN `newClientOrderId`）。

---

## 11. 部署拓扑

**Phase 1（当前骨架）**：单机、单进程 `hftd` + 独立 `controld`。

```
/usr/local/bin/hftd           # feed + strategy + exec + risk + reconciler
/usr/local/bin/controld       # 控制面（unix socket /run/hft/controld.sock）
/etc/hft/hft.toml             # 参数（热加载）
/etc/hft/env                  # 密钥（600 root），EnvironmentFile
/var/lib/hft/recordings/      # 原始帧分段（zstd），logrotate 管理
/var/log/hft/hftd.log         # journald 优先；文件仅作兜底
```

systemd 单元要求：

```
Restart=always
RestartSec=2
KillSignal=SIGTERM
TimeoutStopSec=15
EnvironmentFile=/etc/hft/env
LimitNOFILE=65536
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
ReadWritePaths=/var/lib/hft /var/log/hft /run/hft
```

**发布流程**：GitHub Release（`hftd-linux-amd64` + sha256）→ 校验 → 原子替换 → `systemctl restart` → 健康检查；
失败自动回滚上一版本（保留 `hftd.prev`）。灰度：先 1 个策略、小额度，观察 30 min 指标再全量。

**密钥**：仅环境变量/`EnvironmentFile`；仓库内禁止任何私钥；日志与报错禁止打印私钥或完整签名。

---

## 12. 数据与回放

- **记录格式**：分段文件，每段 1 GB 或 15 min；帧 = `venue_id(1B) | ts_recv_ns(8B) | len(4B) | payload`，zstd 压缩。
- **回放一致性**：回放以记录帧为唯一输入，时钟由帧时间戳驱动（虚拟时钟），策略内禁用 `time.Now()`（统一走 `ctx.Now()`）。
- **回放用途**：策略回归（同输入必须同成交序列）、事故复盘、参数扫描。
- **保留策略**：热数据 7 天，冷数据转对象存储；涉及交易的记录保留 180 天（审计）。

---

## 13. 路线图

| 阶段 | 内容 | 退出条件 |
|---|---|---|
| **Phase 0** | `cmd/latency-probe`：RTT/签名/解析/限速实测；确认 §3.1 合约规格 | 产出测量报告，确定机房与语言边界 |
| **Phase 1** | 骨架落地（本目录）：接口 + 空实现 + 回放/记录格式定义 | `go build ./...` 通过，接口评审 |
| **Phase 2** | 单所（HL）完整链路：feed → book → strategy → risk → exec → reconcile；网格策略迁移为第一个 `Strategy` | 纸面交易 24h 无缺口，指标齐全 |
| **Phase 3** | 接入 Binance 腿 + 跨所策略 + 库存管理 | 小额实盘 1 周，偏差 < 阈值 |
| **Phase 4** | Rust 热路径上线、多策略隔离、录制/回放自动化 | p99 达标，回放复现一致 |

---

## 14. 未决问题（需要拍板）

1. §3.1 的 Binance 合约是否真实存在？若不存在，跨所腿落在哪个 venue？
2. 目标机房：是否接受 us-east（离 HL 最近）而 Binance 稍远？
3. 资金与杠杆上限：跨所保证金如何分配（单所 5x？）与最大总名义？
4. 是否要求"同一进程多策略"（省 IPC）还是"一策略一进程"（隔离优先）？
5. Rust 工具链在 CI 中如何交付（`cargo build --release` + cbindgen，或预编译 artifact）？
6. 记录数据落盘带宽：按 20 档快照 × 2 venue 估算，是否需采样（只记录 top 5 档 + 深度摘要）？
