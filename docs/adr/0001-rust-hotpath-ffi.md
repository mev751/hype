# ADR-0001：Rust 热路径与 Go 的 FFI 边界

- 状态：提议（Accepted 待 Phase 0 测量后确认）
- 日期：2026-11-13
- 决策者：mev751

## 背景

系统主体用 Go 实现（延续现有 `internal/hl` 的签名/执行/对账代码），但每笔订单都需要
msgpack 编码 + keccak256 + secp256k1 签名。Go 现有实现每单约 40–80 µs，且分配较多。

Hyperliquid 端到端时延由公网 RTT 支配（10–30 ms），因此**微秒级优化在总时延中占比 < 2%**。
是否引入第二条工具链（Rust），需要明确收益与代价，避免为了"看起来像 HFT"而增加运维复杂度。

## 决策

1. **引入 Rust 作为可选热路径**，以 `cdylib` 暴露 C ABI，Go 通过 cgo 调用；
2. Rust 只实现**无状态纯函数**：
   - `hl_sign(snapshot_bytes, nonce, privkey) -> signature`
   - `hl_encode_order(order_fields) -> msgpack_bytes`
   - Binance HMAC 签名（Phase 3 需要时）
3. Go 侧保留**纯 Go 实现**作为 fallback，通过构建标签切换：

   | 构建标签 | 实现 |
   |---|---|
   | `pure`（默认，无 cgo） | `internal/hft/hotpath/pure.go` |
   | `rust`（需要预编译 .so） | `internal/hft/hotpath/rust.go`（cgo） |

4. 边界铁律：Rust 侧**不得**持有跨调用状态、不得做 I/O、不得抛 Go panic（错误用返回码 + 错误码枚举）。

## 备选方案与取舍

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| 全 Go | 单一工具链，CI 简单 | 签名有分配开销（可优化到 30 µs 量级） | Phase 1/2 采用 |
| **Go + Rust cdylib（本决策）** | 热路径无 GC，可控分配 | 双工具链、FFI 调试成本、部署需带 .so | Phase 4 启用 |
| 全 Rust | 延迟最优 | 策略/风控迭代慢、与现有 Go 代码割裂 | 不采用 |
| CGO + C | 无额外工具链 | 手写内存管理，ECC 库维护成本高 | 不采用 |

## 后果

**正向**
- 签名成本可降到 20–50 µs（目标再降 30%），且无 GC 抖动；
- 纯计算边界清晰，测试可用黄金向量跨语言比对（复用 `internal/hl/sign_test.go` 里 Python 生成的向量）。

**负向 / 需要承担**
- CI 需要额外 `cargo build --release`；产物需把 `.so` 与二进制一起发布（或静态链接 musl）；
- 部署单元必须校验 `.so` 存在且 ABI 版本匹配，否则拒绝启动（fail-fast，不静默回退）；
- cgo 会破坏 `CGO_ENABLED=0` 静态编译，需改为 `CGO_ENABLED=1` + 静态链接（musl）或随包分发 `.so`；
- 调试 cgo 崩溃更难（core dump、符号），需要固定 backtrace 流程。

## 触发条件（何时真正启用）

只有 Phase 0 测量同时满足以下两条才启用 Rust：

1. `sign_ns` p99 > 50 µs（Go 实现）；
2. 签名耗时占"决策→ack"总时延 > 5%。

否则推迟到 Phase 4，先把机房、连接复用、解析路径做好。

## ABI 版本管理

`rust/hl-codec` 导出 `hl_abi_version() -> u32`；Go 侧启动时校验，不匹配则拒绝启动并打印期望版本。
接口变更必须递增版本号并同步更新两侧。

## 测试策略

- **黄金向量**：Python SDK 生成的固定 `(action, nonce) → (hash, r, s, v)`，Go 与 Rust 实现都必须匹配（现有测试已覆盖 Go 侧）；
- **差分测试**：随机生成 10k 组 action，Go 实现与 Rust 实现结果逐字节比对（Phase 4）；
- **压力测试**：单核 1M 次签名，测 p50/p99 与内存分配次数。
