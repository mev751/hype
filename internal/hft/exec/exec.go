// Package exec 负责订单路由：幂等提交、超时、ack 跟踪。
//
// 铁律（来自线上事故教训）：
//   - 下单超时绝不直接重发，必须先按 ClientID 查询订单最终状态；
//   - 同一 ClientID 只能对应一笔订单，重复提交直接返回已有结果；
//   - 所有状态迁移都要写日志 + 指标，便于事后按 client_id 复盘。
package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mev751/hype/internal/hft/venue"
)

// ErrDuplicate 同一 ClientID 重复提交（幂等命中）
var ErrDuplicate = errors.New("exec: ClientID 已存在，返回既有结果")

// ErrUnknownOutcome 下单结果未知（超时/网络异常），必须先查询再决定是否重试
var ErrUnknownOutcome = errors.New("exec: 下单结果未知，需查询订单状态")

type entry struct {
	ack  venue.Ack
	done bool
}

// Router 订单路由器
type Router struct {
	v venue.Venue

	mu   sync.Mutex
	seen map[string]*entry

	onAck  func(venue.Ack)
	onFill func(venue.Fill)
}

// NewRouter 构造路由器
func NewRouter(v venue.Venue) *Router {
	return &Router{v: v, seen: make(map[string]*entry, 256)}
}

// OnAck 注册回报回调（在提交线程内同步调用，保持顺序）
func (r *Router) OnAck(f func(venue.Ack)) { r.onAck = f }

// OnFill 注册成交回调
func (r *Router) OnFill(f func(venue.Fill)) { r.onFill = f }

// Submit 提交订单。
//
// 语义：
//   - 首次提交：调用 venue.Place，记录结果；
//   - 重复提交：返回既有结果 + ErrDuplicate；
//   - Place 返回未知结果：记录为 AckUnknown 并返回 ErrUnknownOutcome，
//     调用方必须先查询（本层不自动重发）。
func (r *Router) Submit(ctx context.Context, in venue.OrderIntent) (venue.Ack, error) {
	if in.ClientID == "" {
		return venue.Ack{}, errors.New("exec: ClientID 不能为空")
	}

	r.mu.Lock()
	if e, ok := r.seen[in.ClientID]; ok {
		ack := e.ack
		r.mu.Unlock()
		return ack, ErrDuplicate
	}
	slot := &entry{}
	r.seen[in.ClientID] = slot
	r.mu.Unlock()

	ack, err := r.v.Place(ctx, in)

	r.mu.Lock()
	slot.ack = ack
	slot.done = ack.Status != venue.AckUnknown
	r.mu.Unlock()

	if err != nil {
		if ack.Status == venue.AckUnknown {
			return ack, fmt.Errorf("%w: %v", ErrUnknownOutcome, err)
		}
		return ack, err
	}

	if r.onAck != nil {
		r.onAck(ack)
	}
	return ack, nil
}

// HandleFill 处理成交回报（由 venue.FillStream 驱动）
func (r *Router) HandleFill(f venue.Fill) {
	if r.onFill != nil {
		r.onFill(f)
	}
}

// Pending 未确定结果的订单数（AckUnknown）
func (r *Router) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.seen {
		if !e.done {
			n++
		}
	}
	return n
}

// Lookup 查询某 ClientID 的已知结果
func (r *Router) Lookup(clientID string) (venue.Ack, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.seen[clientID]
	if !ok {
		return venue.Ack{}, false
	}
	return e.ack, true
}
