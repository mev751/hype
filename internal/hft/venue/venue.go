// Package venue 定义跨交易所统一的行情/交易适配接口。
//
// 设计约束：
//   - 一次调用只做一件事，不隐含重试（重试与幂等由 exec 层负责）；
//   - 热路径上不得返回 map 等需要二次解析的结构；
//   - 所有实现必须支持 ctx 取消，取消即断开在途请求。
package venue

import (
	"context"
	"errors"
	"time"
)

// Side 买卖方向
type Side int8

const (
	SideUnknown Side = iota
	Buy
	Sell
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "BUY"
	case Sell:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

// Type 订单类型
type Type int8

const (
	// TypeLimit 限价（IOC 语义由 tif 决定）
	TypeLimit Type = iota
	// TypeIOC 立即成交否则取消
	TypeIOC
	// TypePostOnly 只做 maker，若会吃单则拒绝
	TypePostOnly
)

// Level 盘口档位
type Level struct {
	Px float64
	Sz float64
}

// Snapshot 归一化后的盘口快照。
//
// 注意：Hyperliquid 的 l2Book 是全量快照且不含序列号，Seq 恒为 0，
// 缺口检测依赖 ExchangeTS/RecvTS 的跳变；Binance 增量流需实现侧维护序列号。
type Snapshot struct {
	Venue      string
	Symbol     string
	Bids       []Level // 价格降序
	Asks       []Level // 价格升序
	ExchangeTS time.Time
	RecvTS     time.Time
	Seq        uint64
}

// Best 返回买卖一价与对应挂单量。
func (s Snapshot) Best() (bid, ask, bidSz, askSz float64, ok bool) {
	if len(s.Bids) == 0 || len(s.Asks) == 0 {
		return 0, 0, 0, 0, false
	}
	return s.Bids[0].Px, s.Asks[0].Px, s.Bids[0].Sz, s.Asks[0].Sz, true
}

// Mid 返回中间价（无盘口时为 0）。
func (s Snapshot) Mid() float64 {
	bid, ask, _, _, ok := s.Best()
	if !ok {
		return 0
	}
	return (bid + ask) / 2
}

// Position 单腿仓位（正=多头，负=空头）
type Position struct {
	Venue         string
	Symbol        string
	Qty           float64
	EntryPx       float64
	UnrealizedPnl float64
	MarginUsed    float64
}

// FeeSchedule 费率（bps = 万分之一）
type FeeSchedule struct {
	MakerBps float64
	TakerBps float64
}

// OrderIntent 订单意图：策略产出、exec 消费。
//
// ClientID 必须由调用方生成且全局唯一，作为幂等键
// （Hyperliquid: cloid；Binance: newClientOrderId）。
type OrderIntent struct {
	ClientID    string
	Venue       string
	Symbol      string
	Side        Side
	Type        Type
	Qty         float64
	Price       float64
	ReduceOnly  bool
	DecisionTSN int64 // 策略做出决策的 monotonic 纳秒时间戳
}

// Ack 下单回报
type Ack struct {
	ClientID     string
	VenueOrderID string
	Status       AckStatus
	FilledQty    float64
	AvgPx        float64
	SentTSN      int64
	AckTSN       int64
	Err          error
}

// AckStatus 下单回报状态
type AckStatus int8

const (
	// AckUnknown 未收到最终回报（需要查询确认）
	AckUnknown AckStatus = iota
	// AckFilled 已完全成交
	AckFilled
	// AckPartial 部分成交，剩余已撤
	AckPartial
	// AckResting 挂在盘口（IOC/PostOnly 不应出现）
	AckResting
	// AckRejected 被交易所拒绝
	AckRejected
)

func (a AckStatus) String() string {
	switch a {
	case AckFilled:
		return "filled"
	case AckPartial:
		return "partial"
	case AckResting:
		return "resting"
	case AckRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Fill 成交回报（费用与已实现盈亏由交易所给出）
type Fill struct {
	Venue        string
	Symbol       string
	ClientID     string
	VenueOrderID string
	Side         Side
	Qty          float64
	Px           float64
	Fee          float64
	ClosedPnl    float64
	TradeTSN     int64 // 交易所成交时间
	RecvTSN      int64 // 本地收到时间
}

// Venue 交易所适配接口。
type Venue interface {
	// Name 返回 venue 标识（"hyperliquid" / "binance"）
	Name() string
	// Stream 推送归一化行情；阻塞直到 ctx 取消或连接不可恢复
	Stream(ctx context.Context, out chan<- Snapshot) error
	// FillStream 推送成交回报
	FillStream(ctx context.Context, out chan<- Fill) error
	// Place 下单（幂等：同一 ClientID 重发不得造成重复成交）
	Place(ctx context.Context, in OrderIntent) (Ack, error)
	// Cancel 撤单
	Cancel(ctx context.Context, clientID string) (Ack, error)
	// Positions 读取当前仓位（全量，非增量）
	Positions(ctx context.Context) ([]Position, error)
	// Fees 返回当前费率档位
	Fees() FeeSchedule
}

// ErrNotImplemented 骨架期未实现
var ErrNotImplemented = errors.New("venue: 尚未实现")

// ErrStale 行情过期，禁止交易
var ErrStale = errors.New("venue: 行情过期或缺口")
