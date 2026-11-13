package hl

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// Globals 全局账户/统计状态（对应 Python 版模块级全局变量）
type Globals struct {
	mu sync.Mutex

	botStartMS         int64
	initialEquity      float64
	currentEquity      float64
	hasEquity          bool
	withdrawableMargin float64
	realPosSKHY        float64
	realPosSKHX        float64
	floatingPnl        float64
	realizedPnl        float64
	fees               float64
	funding            float64
	entryCount         int
	tpCount            int
	trueProfit         float64
	processedFillIDs   map[string]struct{}
	tradeLogs          []string
}

// NewGlobals 初始化全局状态。
func NewGlobals(botStartMS int64) *Globals {
	return &Globals{
		botStartMS:       botStartMS,
		processedFillIDs: map[string]struct{}{},
	}
}

// Snapshot 供 UI 读取的一致快照。
type Snapshot struct {
	BotStartMS         int64
	CurrentEquity      float64
	HasEquity          bool
	WithdrawableMargin float64
	RealPosSKHY        float64
	RealPosSKHX        float64
	FloatingPnl        float64
	RealizedPnl        float64
	Fees               float64
	Funding            float64
	EntryCount         int
	TPCount            int
	TrueProfit         float64
	Logs               []string
}

// AddLog 写入日志环（保留最近 20 条）。
func (g *Globals) AddLog(text string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tradeLogs = append(g.tradeLogs, text)
	if len(g.tradeLogs) > 20 {
		g.tradeLogs = g.tradeLogs[len(g.tradeLogs)-20:]
	}
}

// Snapshot 读取状态快照。
func (g *Globals) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	logs := make([]string, len(g.tradeLogs))
	copy(logs, g.tradeLogs)
	return Snapshot{
		BotStartMS:         g.botStartMS,
		CurrentEquity:      g.currentEquity,
		HasEquity:          g.hasEquity,
		WithdrawableMargin: g.withdrawableMargin,
		RealPosSKHY:        g.realPosSKHY,
		RealPosSKHX:        g.realPosSKHX,
		FloatingPnl:        g.floatingPnl,
		RealizedPnl:        g.realizedPnl,
		Fees:               g.fees,
		Funding:            g.funding,
		EntryCount:         g.entryCount,
		TPCount:            g.tpCount,
		TrueProfit:         g.trueProfit,
		Logs:               logs,
	}
}

// BotStartMS 机器人启动时间（毫秒）
func (g *Globals) BotStartMS() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.botStartMS
}

// WithdrawableMargin 当前可用保证金。
func (g *Globals) WithdrawableMargin() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.withdrawableMargin
}

// FloatingPnl 当前浮动盈亏。
func (g *Globals) FloatingPnl() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.floatingPnl
}

// EntryCount 累计建仓次数。
func (g *Globals) EntryCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.entryCount
}

// TPCount 累计止盈次数。
func (g *Globals) TPCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tpCount
}

// IncEntry 建仓计数 +1。
func (g *Globals) IncEntry() {
	g.mu.Lock()
	g.entryCount++
	g.mu.Unlock()
}

// IncTP 止盈计数 +1。
func (g *Globals) IncTP() {
	g.mu.Lock()
	g.tpCount++
	g.mu.Unlock()
}

// AddTrueProfit 累加网格估算收益。
func (g *Globals) AddTrueProfit(v float64) {
	g.mu.Lock()
	g.trueProfit += v
	g.mu.Unlock()
}

// SetAccount 更新账户与真实仓位快照。
func (g *Globals) SetAccount(realHy, realHx, floating, equity, withdrawable float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.realPosSKHY = realHy
	g.realPosSKHX = realHx
	g.floatingPnl = floating
	g.currentEquity = equity
	g.withdrawableMargin = withdrawable
	if !g.hasEquity && equity > 0 {
		g.initialEquity = equity
		g.hasEquity = true
	}
}

// SetHistory 更新已实现盈亏与资金费。
func (g *Globals) SetHistory(realized, funding float64) {
	g.mu.Lock()
	g.realizedPnl = realized
	g.funding = funding
	g.mu.Unlock()
}

// AddFees 累加手续费（同一成交只计一次，由调用方保证）。
func (g *Globals) AddFees(v float64) {
	g.mu.Lock()
	g.fees += v
	g.mu.Unlock()
}

// MarkFill 记录成交 id，返回是否首次出现。
func (g *Globals) MarkFill(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.processedFillIDs[id]; exists {
		return false
	}
	g.processedFillIDs[id] = struct{}{}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// FetchAccountEquity 每 3 秒刷新账户净值与链上仓位。
func FetchAccountEquity(ctx context.Context, api *API, wallet string, g *Globals) {
	if wallet == "" {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		func() {
			spot, err := api.Info(ctx, map[string]any{"type": "spotClearinghouseState", "user": wallet}, 5*time.Second)
			if err != nil {
				LogError(g, "账户/仓位刷新失败", err)
				return
			}
			var spotUSDC float64
			for _, item := range asSlice(asMap(spot)["balances"]) {
				bm := asMap(item)
				if getStr(bm, "coin") == "USDC" {
					spotUSDC = getFloat(bm, "total")
					break
				}
			}

			xyz, err := api.Info(ctx, map[string]any{
				"type": "clearinghouseState", "user": wallet, "dex": "xyz",
			}, 5*time.Second)
			if err != nil {
				LogError(g, "账户/仓位刷新失败", err)
				return
			}
			xm := asMap(xyz)
			accountValue := getFloat(asMap(xm["marginSummary"]), "accountValue")
			withdrawable := math.Max(0, getFloat(xm, "withdrawable"))

			var rY, rX, uY, uX float64
			for _, item := range asSlice(xm["assetPositions"]) {
				p := asMap(asMap(item)["position"])
				if p == nil {
					continue
				}
				coin := strings.ToUpper(getStr(p, "coin"))
				szi := getFloat(p, "szi")
				upnl := getFloat(p, "unrealizedPnl")
				switch {
				case strings.Contains(coin, "SKHX") || strings.Contains(coin, "NIX"):
					rX, uX = szi, upnl
				case strings.Contains(coin, "SKHY"):
					rY, uY = szi, upnl
				}
			}
			g.SetAccount(rY, rX, uY+uX, accountValue+spotUSDC, withdrawable)
		}()
		if !sleepCtx(ctx, 3*time.Second) {
			return
		}
	}
}

// FetchTradeHistory 每 5 秒刷新成交与资金费统计。
func FetchTradeHistory(ctx context.Context, api *API, wallet string, g *Globals) {
	if wallet == "" {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		func() {
			start := g.BotStartMS()

			fills, err := api.Info(ctx, map[string]any{"type": "userFills", "user": wallet}, 5*time.Second)
			if err != nil {
				LogError(g, "成交/资金费统计失败", err)
			} else {
				var realized, newFees float64
				for _, item := range asSlice(fills) {
					f := asMap(item)
					if f == nil {
						continue
					}
					if getFloat(f, "time") < float64(start) {
						continue
					}
					coin := getStr(f, "coin")
					if !strings.Contains(coin, "SKHY") && !strings.Contains(coin, "SKHX") && !strings.Contains(coin, "NIX") {
						continue
					}
					realized += getFloat(f, "closedPnl")
					id := fillID(f)
					if g.MarkFill(id) {
						newFees += math.Abs(getFloat(f, "fee"))
					}
				}
				if newFees != 0 {
					g.AddFees(newFees)
				}
				g.SetHistory(realized, g.Snapshot().Funding)
			}

			// 资金费：None 与 xyz 两种查询结果按唯一键去重，避免重复累计
			var funding float64
			seen := map[string]struct{}{}
			for _, dex := range []string{"", "xyz"} {
				payload := map[string]any{
					"type":      "userFunding",
					"user":      wallet,
					"startTime": start,
				}
				if dex != "" {
					payload["dex"] = dex
				}
				res, err := api.Info(ctx, payload, 5*time.Second)
				if err != nil {
					continue
				}
				for _, item := range asSlice(res) {
					f := asMap(item)
					if f == nil {
						continue
					}
					delta := asMap(f["delta"])
					coin := getStr(f, "coin")
					if coin == "" {
						coin = getStr(delta, "coin")
					}
					if !strings.Contains(coin, "SKHY") && !strings.Contains(coin, "SKHX") && !strings.Contains(coin, "NIX") {
						continue
					}
					var usdc float64
					if _, ok := f["usdc"]; ok {
						usdc = getFloat(f, "usdc")
					} else {
						usdc = getFloat(delta, "usdc")
					}
					var fundingTime float64
					if _, ok := f["time"]; ok {
						fundingTime = getFloat(f, "time")
					} else {
						fundingTime = getFloat(delta, "time")
					}
					key := getStr(f, "tid")
					if key == "" {
						key = getStr(f, "hash")
					}
					if key == "" {
						key = fmt.Sprintf("%v_%s_%v", fundingTime, coin, usdc)
					}
					if _, dup := seen[key]; dup {
						continue
					}
					seen[key] = struct{}{}
					funding += usdc
				}
			}
			g.SetHistory(g.Snapshot().RealizedPnl, funding)
		}()
		if !sleepCtx(ctx, 5*time.Second) {
			return
		}
	}
}

func fillID(f map[string]any) string {
	if id := getStr(f, "tid"); id != "" {
		return id
	}
	if h := getStr(f, "hash"); h != "" {
		return h
	}
	return fmt.Sprintf("%v_%s_%s_%s_%v",
		getFloat(f, "time"), getStr(f, "coin"), getStr(f, "sz"), getStr(f, "px"), getFloat(f, "fee"))
}
