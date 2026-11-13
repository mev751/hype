// Package hyperliquid 是 Hyperliquid 的 venue.Venue 实现。
//
// Phase 2 计划：
//   - 行情复用现有 l2Book WS（internal/hl.MarketWS 的思路），但改为推送完整
//     venue.Snapshot（保留 20 档），而不是只取买卖一价；
//   - 下单/撤单复用 internal/hl 的 EIP-712 签名与 cloid 幂等逻辑；
//   - 成交通道改用 WS 订阅 userFills（当前网格是轮询 userFillsByTime，延迟 1s+）。
package hyperliquid

import (
	"context"

	"github.com/mev751/hype/internal/hft/venue"
	"github.com/mev751/hype/internal/hl"
)

// Venue Hyperliquid 交易所适配
type Venue struct {
	API *hl.API
}

// New 构造适配器
func New(api *hl.API) *Venue { return &Venue{API: api} }

// Name venue 标识
func (v *Venue) Name() string { return "hyperliquid" }

// Stream 行情流（Phase 2 实现）
func (v *Venue) Stream(ctx context.Context, out chan<- venue.Snapshot) error {
	return venue.ErrNotImplemented
}

// FillStream 成交流（Phase 2 实现）
func (v *Venue) FillStream(ctx context.Context, out chan<- venue.Fill) error {
	return venue.ErrNotImplemented
}

// Place 下单（Phase 2 实现）
func (v *Venue) Place(ctx context.Context, in venue.OrderIntent) (venue.Ack, error) {
	return venue.Ack{ClientID: in.ClientID, Status: venue.AckUnknown}, venue.ErrNotImplemented
}

// Cancel 撤单（Phase 2 实现）
func (v *Venue) Cancel(ctx context.Context, clientID string) (venue.Ack, error) {
	return venue.Ack{ClientID: clientID, Status: venue.AckUnknown}, venue.ErrNotImplemented
}

// Positions 仓位（Phase 2 实现）
func (v *Venue) Positions(ctx context.Context) ([]venue.Position, error) {
	return nil, venue.ErrNotImplemented
}

// Fees 费率：growth mode 档位，实际值需按账户查询（Phase 2 从 userFees 接口读取）
func (v *Venue) Fees() venue.FeeSchedule {
	return venue.FeeSchedule{MakerBps: 0.15, TakerBps: 0.9}
}
