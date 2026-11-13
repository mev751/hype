// Package strategy 定义单写者策略接口与注册表。
//
// 关键约束（与架构文档 §5 一致）：
//   - 所有回调由同一条 goroutine 串行调用，策略内部无需加锁；
//   - 策略不得直接访问 venue API，只能通过 Context.Emit 产出订单意图；
//   - 策略不得调用 time.Now()，必须使用 ctx.Now()/ctx.NowNano() 以保证回放一致。
package strategy

import (
	"fmt"
	"sort"
	"time"

	"github.com/mev751/hype/internal/hft/portfolio"
	"github.com/mev751/hype/internal/hft/venue"
)

// Context 策略可见的只读上下文 + 意图出口。
type Context interface {
	// Now 当前时间（回放时由帧时间驱动）
	Now() time.Time
	// NowNano monotonic 纳秒时间戳
	NowNano() int64
	// Book 读取指定标的的最新盘口
	Book(v, symbol string) (venue.Snapshot, bool)
	// Portfolio 读取跨所仓位快照
	Portfolio() portfolio.Snapshot
	// Emit 提交订单意图（经风控与执行层，非阻塞）
	Emit(intent venue.OrderIntent) error
	// Logf 结构化日志
	Logf(format string, args ...any)
}

// Strategy 策略生命周期回调（全部在单写者 goroutine 内执行）。
type Strategy interface {
	Name() string
	OnStart(ctx Context) error
	OnSnapshot(ctx Context, s venue.Snapshot) error
	OnFill(ctx Context, f venue.Fill) error
	OnTimer(ctx Context, now time.Time) error
	OnStop(ctx Context) error
}

// Factory 策略构造函数
type Factory func() Strategy

// Registry 策略注册表
type Registry struct {
	m map[string]Factory
}

// NewRegistry 构造空注册表
func NewRegistry() *Registry {
	return &Registry{m: map[string]Factory{}}
}

// Register 注册策略；重名直接 panic（启动期错误，不应静默覆盖）。
func (r *Registry) Register(name string, f Factory) {
	if name == "" || f == nil {
		panic("strategy: 注册参数非法")
	}
	if _, exists := r.m[name]; exists {
		panic(fmt.Sprintf("strategy: %q 已注册", name))
	}
	r.m[name] = f
}

// New 实例化策略
func (r *Registry) New(name string) (Strategy, bool) {
	f, ok := r.m[name]
	if !ok {
		return nil, false
	}
	return f(), true
}

// Names 已注册策略名（排序后返回，便于日志稳定）
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.m))
	for k := range r.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
