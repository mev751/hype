// Package portfolio 维护跨所统一仓位视图。
//
// 注意：本包只是"本地视图"，不是权威真相。权威来源是交易所的 clearinghouseState /
// 账户仓位接口；reconciler 定期用权威数据校正本视图，偏差超阈值即停机。
package portfolio

import (
	"math"
	"time"
)

// Leg 单腿仓位
type Leg struct {
	Venue         string
	Symbol        string
	Qty           float64 // 正=多头，负=空头
	EntryPx       float64
	Notional      float64 // 按标记价计算的绝对值名义
	UnrealizedPnl float64
}

// Snapshot 跨所仓位快照
type Snapshot struct {
	Legs           []Leg
	GrossNotional  float64 // 各腿名义绝对值之和
	NetNotional    float64 // 带符号求和
	RealizedPnl    float64
	UnrealizedPnl  float64
	Fees           float64
	Funding        float64
	HedgeImbalance float64 // 两腿名义偏差比例（0..1），单腿或无仓时为 0
	UpdatedAt      time.Time
	HasPosition    bool
}

// Build 由腿列表计算汇总字段。
//
// 对冲偏差定义为 |max(|n1|,|n2|) - min(|n1|,|n2|)| / max(...)，
// 仅当恰好两条腿时计算，其余情况为 0（由策略自行判断）。
func Build(legs []Leg) Snapshot {
	s := Snapshot{Legs: legs, UpdatedAt: time.Now()}
	var absSum float64
	for _, l := range legs {
		n := l.Notional
		if n == 0 {
			n = math.Abs(l.Qty * l.EntryPx)
		}
		absSum += math.Abs(n)
		s.NetNotional += l.Qty * l.EntryPx
		s.UnrealizedPnl += l.UnrealizedPnl
		if l.Qty != 0 {
			s.HasPosition = true
		}
	}
	s.GrossNotional = absSum

	if len(legs) == 2 {
		a, b := math.Abs(legs[0].Notional), math.Abs(legs[1].Notional)
		if a == 0 && b == 0 {
			a = math.Abs(legs[0].Qty * legs[0].EntryPx)
			b = math.Abs(legs[1].Qty * legs[1].EntryPx)
		}
		base := math.Max(a, b)
		if base > 0 {
			s.HedgeImbalance = math.Abs(a-b) / base
		}
	}
	return s
}

// Notional 返回指定腿的绝对名义（没有该腿时为 0）。
func (s Snapshot) Notional(v, symbol string) float64 {
	for _, l := range s.Legs {
		if l.Venue == v && l.Symbol == symbol {
			return math.Abs(l.Notional)
		}
	}
	return 0
}

// Direction 返回指定腿的方向：正=多、负=空、0=无仓。
func (s Snapshot) Direction(v, symbol string) float64 {
	for _, l := range s.Legs {
		if l.Venue == v && l.Symbol == symbol {
			return l.Qty
		}
	}
	return 0
}

// Eq 近似相等（用于对账比较）
func Eq(a, b, eps float64) bool {
	return math.Abs(a-b) <= eps
}
