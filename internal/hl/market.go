package hl

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Quote 单个标的的盘口快照
type Quote struct {
	Bid float64
	Ask float64
	TS  time.Time
	Has bool
}

// PriceState 全局盘口状态
type PriceState struct {
	mu     sync.RWMutex
	quotes map[string]*Quote
	maxAge time.Duration
}

// NewPriceState 初始化两条腿的盘口容器。
func NewPriceState() *PriceState {
	return &PriceState{
		quotes: map[string]*Quote{
			SymSKHY: {},
			SymSKHX: {},
		},
		maxAge: 5 * time.Second,
	}
}

// SetMaxAge 设置行情有效期（支持热加载）。
func (p *PriceState) SetMaxAge(d time.Duration) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxAge = d
}

// Update 更新盘口（bid/ask 为买卖一价）。
func (p *PriceState) Update(sym string, bid, ask float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := p.quotes[sym]
	if q == nil {
		q = &Quote{}
		p.quotes[sym] = q
	}
	q.Bid = bid
	q.Ask = ask
	q.TS = time.Now()
	q.Has = true
}

// Snapshot 读取某标的的买卖一价。
func (p *PriceState) Snapshot(sym string) (bid, ask float64, ok bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	q := p.quotes[sym]
	if q == nil || !q.Has {
		return 0, 0, false
	}
	return q.Bid, q.Ask, true
}

// Fresh 返回可交易的最新行情：SKHY 买一 + SKHX 卖一，且均在有效期内。
func (p *PriceState) Fresh() (pSKHYBid, pSKHXAsk float64, ok bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	hy := p.quotes[SymSKHY]
	hx := p.quotes[SymSKHX]
	if hy == nil || hx == nil || !hy.Has || !hx.Has {
		return 0, 0, false
	}
	if hy.Bid <= 0 || hx.Ask <= 0 {
		return 0, 0, false
	}
	now := time.Now()
	if now.Sub(hy.TS) > p.maxAge || now.Sub(hx.TS) > p.maxAge {
		return 0, 0, false
	}
	return hy.Bid, hx.Ask, true
}

// MarketWS Hyperliquid l2Book 行情客户端（断线自动重连）
type MarketWS struct {
	prices    *PriceState
	onBook    func(coin string, bid, ask float64)
	connected atomic.Bool
	writeMu   sync.Mutex
}

// NewMarketWS 构造行情客户端。
func NewMarketWS(prices *PriceState, onBook func(coin string, bid, ask float64)) *MarketWS {
	return &MarketWS{prices: prices, onBook: onBook}
}

// Connected 当前是否连通。
func (w *MarketWS) Connected() bool { return w.connected.Load() }

// Run 持续重连直到 ctx 取消。
func (w *MarketWS) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		w.session(ctx)
		w.connected.Store(false)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (w *MarketWS) session(ctx context.Context) {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, "wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		logErrorf("Hyperliquid WebSocket 连接失败: %v", err)
		return
	}
	defer conn.Close()

	w.connected.Store(true)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for coin := range CoinToDisplay {
		msg := fmt.Sprintf(`{"method":"subscribe","subscription":{"type":"l2Book","coin":%q}}`, coin)
		w.writeMu.Lock()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(msg))
		w.writeMu.Unlock()
	}

	// 应用层心跳（与 Python 版一致：每 5 秒 ping）
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-subCtx.Done():
				return
			case <-ticker.C:
				w.writeMu.Lock()
				err := conn.WriteMessage(websocket.TextMessage, []byte(`{"method":"ping"}`))
				w.writeMu.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	go func() {
		<-subCtx.Done()
		_ = conn.Close()
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			w.connected.Store(false)
			logErrorf("Hyperliquid WebSocket 错误: %v", err)
			return
		}
		w.connected.Store(true)
		w.handleMessage(raw)
	}
}

func (w *MarketWS) handleMessage(raw []byte) {
	var msg struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		logErrorf("WebSocket 行情处理异常: %v", err)
		return
	}
	if msg.Channel != "l2Book" || len(msg.Data) == 0 {
		return
	}

	var book struct {
		Coin   string `json:"coin"`
		Levels [][]struct {
			Px string `json:"px"`
			Sz string `json:"sz"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(msg.Data, &book); err != nil {
		logErrorf("WebSocket 行情处理异常: %v", err)
		return
	}
	if len(book.Levels) < 2 || len(book.Levels[0]) == 0 || len(book.Levels[1]) == 0 {
		return
	}
	display, ok := CoinToDisplay[book.Coin]
	if !ok {
		return
	}
	bid := numToFloat(book.Levels[0][0].Px)
	ask := numToFloat(book.Levels[1][0].Px)
	w.prices.Update(display, bid, ask)
	if w.onBook != nil {
		w.onBook(book.Coin, bid, ask)
	}
}

// FetchInitialPrices 启动时用 REST l2Book 预热盘口。
func FetchInitialPrices(ctx context.Context, api *API, prices *PriceState) {
	for coin, display := range CoinToDisplay {
		res, err := api.Info(ctx, map[string]any{"type": "l2Book", "coin": coin}, 5*time.Second)
		if err != nil {
			LogError(nil, "初始行情获取失败", err)
			continue
		}
		levels := asSlice(asMap(res)["levels"])
		if len(levels) < 2 {
			continue
		}
		bids := asSlice(levels[0])
		asks := asSlice(levels[1])
		if len(bids) == 0 || len(asks) == 0 {
			continue
		}
		bid := getFloat(asMap(bids[0]), "px")
		ask := getFloat(asMap(asks[0]), "px")
		prices.Update(display, bid, ask)
	}
}
