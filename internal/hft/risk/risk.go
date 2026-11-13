// Package risk 实现预交易风控与 kill-switch。
//
// 设计要点：
//   - 校验在策略单写者线程内同步执行，因此 Gate 内部状态无需高频锁；
//   - kill-switch 状态可被外部 controld 翻转，Gate 每 100ms 从本地缓存刷新一次；
//   - 任何拒绝都必须带明确原因，写进日志与指标（禁止静默丢弃订单）。
package risk

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/mev751/hype/internal/hft/portfolio"
	"github.com/mev751/hype/internal/hft/venue"
)

// Limits 风控限额（全部为 0 表示该项不限制）
type Limits struct {
	MaxNotionalPerOrder float64 // 单笔名义上限（USDC）
	MaxGrossNotional    float64 // 总名义上限（两腿绝对值之和）
	MaxNetNotional      float64 // 净名义上限（带符号和）
	MaxOrdersPerSecond  float64 // 下单速率上限
	MaxHedgeImbalance   float64 // 两腿名义偏差上限（0.15 = 15%）
	MaxDailyLossUSDC    float64 // 日内亏损上限（正数，超过则拒绝新开仓）
}

// ErrRejected 风控拒绝
type ErrRejected struct {
	Reason string
}

func (e *ErrRejected) Error() string { return "risk: 拒绝下单: " + e.Reason }

// tokenBucket 简单令牌桶（单写者使用，但仍加锁以便外部手动调整）
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64) *tokenBucket {
	burst := rate
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

func (b *tokenBucket) take(now time.Time) bool {
	if b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.last.IsZero() {
		b.last = now
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(b.burst, b.tokens+elapsed*b.rate)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Gate 预交易风控闸门
type Gate struct {
	limits Limits

	mu       sync.Mutex
	tripped  bool
	reason   string
	dailyPnl float64

	bucket *tokenBucket
}

// NewGate 构造闸门
func NewGate(l Limits) *Gate {
	return &Gate{limits: l, bucket: newTokenBucket(l.MaxOrdersPerSecond)}
}

// Check 校验一笔订单意图；返回 *ErrRejected 表示被拒。
func (g *Gate) Check(p portfolio.Snapshot, in venue.OrderIntent) error {
	g.mu.Lock()
	tripped, reason, dailyPnl := g.tripped, g.reason, g.dailyPnl
	g.mu.Unlock()

	if tripped {
		return &ErrRejected{Reason: "kill-switch 已触发: " + reason}
	}
	if in.Qty <= 0 {
		return &ErrRejected{Reason: fmt.Sprintf("数量非法: %v", in.Qty)}
	}
	if in.Price <= 0 {
		return &ErrRejected{Reason: fmt.Sprintf("价格非法: %v", in.Price)}
	}

	notional := in.Qty * in.Price
	l := g.limits

	if l.MaxNotionalPerOrder > 0 && notional > l.MaxNotionalPerOrder {
		return &ErrRejected{Reason: fmt.Sprintf("单笔名义 %.2f > 上限 %.2f", notional, l.MaxNotionalPerOrder)}
	}
	if l.MaxGrossNotional > 0 && p.GrossNotional+notional > l.MaxGrossNotional {
		return &ErrRejected{Reason: fmt.Sprintf("总名义 %.2f + %.2f > 上限 %.2f", p.GrossNotional, notional, l.MaxGrossNotional)}
	}

	signed := notional
	if in.Side == venue.Sell {
		signed = -notional
	}
	if l.MaxNetNotional > 0 && math.Abs(p.NetNotional+signed) > l.MaxNetNotional {
		return &ErrRejected{Reason: fmt.Sprintf("净名义 %.2f 超出上限 %.2f", p.NetNotional+signed, l.MaxNetNotional)}
	}

	// 对冲失衡时只允许减少风险（reduce-only），禁止新开仓
	if l.MaxHedgeImbalance > 0 && p.HedgeImbalance > l.MaxHedgeImbalance && !in.ReduceOnly {
		return &ErrRejected{Reason: fmt.Sprintf("两腿失衡 %.1f%% > 上限 %.1f%%，仅允许减仓", p.HedgeImbalance*100, l.MaxHedgeImbalance*100)}
	}

	if l.MaxDailyLossUSDC > 0 && dailyPnl < -l.MaxDailyLossUSDC {
		return &ErrRejected{Reason: fmt.Sprintf("日内亏损 %.2f 超过上限 %.2f", dailyPnl, l.MaxDailyLossUSDC)}
	}

	if !g.bucket.take(time.Now()) {
		return &ErrRejected{Reason: fmt.Sprintf("下单速率超过 %.1f 笔/秒", l.MaxOrdersPerSecond)}
	}
	return nil
}

// Trip 触发 kill-switch（幂等）
func (g *Gate) Trip(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tripped {
		return
	}
	g.tripped = true
	g.reason = reason
}

// Reset 复位 kill-switch（人工干预后调用）
func (g *Gate) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tripped = false
	g.reason = ""
}

// State 返回当前 kill-switch 状态
func (g *Gate) State() (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tripped, g.reason
}

// SetDailyPnl 更新日内盈亏（由 portfolio/obs 侧驱动）
func (g *Gate) SetDailyPnl(v float64) {
	g.mu.Lock()
	g.dailyPnl = v
	g.mu.Unlock()
}
