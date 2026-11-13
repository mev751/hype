// Package binance 是 Binance 的 venue.Venue 实现。
//
// Phase 3 计划：
//   - 行情：WS 深度增量流（需实现本地 order book 重建 + 序列号校验；
//     与 Hyperliquid 的全量快照语义不同，接口层已用 Snapshot 统一）；
//   - 下单：REST + HMAC 签名，幂等键 newClientOrderId；
//   - 时钟：启动时校时，recvWindow 依赖本地时钟精度，漂移超限拒绝交易；
//   - 合约规格（tickSize/minNotional/stepSize）启动时拉取并缓存。
//
// 注意：SKHY/SKHYNIX 在 Binance 是否存在对应合约尚未核实（见架构文档 §3.1）。
package binance

import (
	"context"

	"github.com/mev751/hype/internal/hft/venue"
)

// Venue Binance 交易所适配
type Venue struct {
	APIKey    string
	APISecret string
}

// New 构造适配器
func New(apiKey, apiSecret string) *Venue {
	return &Venue{APIKey: apiKey, APISecret: apiSecret}
}

// Name venue 标识
func (v *Venue) Name() string { return "binance" }

// Stream 行情流（Phase 3 实现）
func (v *Venue) Stream(ctx context.Context, out chan<- venue.Snapshot) error {
	return venue.ErrNotImplemented
}

// FillStream 成交流（Phase 3 实现：USER_DATA 流）
func (v *Venue) FillStream(ctx context.Context, out chan<- venue.Fill) error {
	return venue.ErrNotImplemented
}

// Place 下单（Phase 3 实现）
func (v *Venue) Place(ctx context.Context, in venue.OrderIntent) (venue.Ack, error) {
	return venue.Ack{ClientID: in.ClientID, Status: venue.AckUnknown}, venue.ErrNotImplemented
}

// Cancel 撤单（Phase 3 实现）
func (v *Venue) Cancel(ctx context.Context, clientID string) (venue.Ack, error) {
	return venue.Ack{ClientID: clientID, Status: venue.AckUnknown}, venue.ErrNotImplemented
}

// Positions 仓位（Phase 3 实现）
func (v *Venue) Positions(ctx context.Context) ([]venue.Position, error) {
	return nil, venue.ErrNotImplemented
}

// Fees 费率（U 本位永续默认档位，实际值需按 VIP 等级查询）
func (v *Venue) Fees() venue.FeeSchedule {
	return venue.FeeSchedule{MakerBps: 2.0, TakerBps: 5.0}
}
