// Package hotpath 定义热路径上的签名抽象。
//
// 两种实现通过构建标签切换（见 docs/adr/0001-rust-hotpath-ffi.md）：
//
//	go build ./...            → 纯 Go 实现（pure.go）
//	go build -tags rust ./... → Rust cdylib（Phase 4，rust.go，尚未提交）
//
// 契约：实现必须是无状态纯函数；禁止持有跨调用状态、禁止 I/O。
package hotpath

// Signer 对已编码的 action 字节签名。
//
// actionBytes 必须是 msgpack 编码后的完整动作（字段顺序由协议决定）；
// nonce 为毫秒时间戳，必须单调递增。
// 返回 hyperliquid 线上签名格式的三个分量（0x 前缀十六进制）。
type Signer interface {
	SignAction(actionBytes []byte, nonce int64) (rHex, sHex string, v int, err error)
	// Backend 返回实现标识（"go" / "rust"），用于日志与指标标签
	Backend() string
}

// RequiredABIVersion Go 侧期望的 Rust ABI 版本；纯 Go 实现返回 0。
// 启动时若不匹配必须 fail-fast，禁止静默回退。
const RequiredABIVersion uint32 = 1
