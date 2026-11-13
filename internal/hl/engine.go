package hl

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LayerEntry 每层建仓记录
type LayerEntry struct {
	K         float64
	Fee       float64
	EntryPxHy float64
	EntryPxHx float64
}

// Tick 行情信号
type Tick struct {
	K     float64
	PSKHY float64
	PSKHX float64
}

// FlattenRequest 保护性平仓请求
type FlattenRequest struct {
	PSKHY  float64
	PSKHX  float64
	Reason string
}

// OrderOutcome 单腿下单结果
type OrderOutcome struct {
	OK       bool
	Reason   string
	Cloid    string
	FilledSz float64
	AvgPx    float64
}

// PairResult 成对下单结果
type PairResult struct {
	OK            bool
	Reason        string
	RequestedHy   float64
	RequestedHx   float64
	FilledHy      float64
	FilledHx      float64
	AvgHy         float64
	AvgHx         float64
	Fees          float64
	ClosedPnl     float64
	FillsVerified bool
	FillDetails   []map[string]any
}

// ShortGridEngine 做空网格状态机（对应 Python ShortGridEngine）
type ShortGridEngine struct {
	mu       sync.Mutex
	qPerGrid float64
	splitN   int
	maxGrids int
	p1       float64
	x        float64

	positionsHeld  int
	posSKHY        float64
	posSKHX        float64
	decHY          int
	decHX          int
	layerEntryData map[int]LayerEntry

	liveTrading bool
	isAligned   bool
	halted      bool
	haltReason  string
	kSmoothed   *float64

	orderMu sync.Mutex

	api         *API
	globals     *Globals
	walletAddr  string
	key         *ecdsa.PrivateKey
	coinToAsset map[string]int
	assetToSz   map[int]int

	tickCh    chan Tick
	flattenCh chan FlattenRequest
	stopCh    chan struct{}
	stopOnce  sync.Once
	workerUp  atomic.Bool
	workerWG  sync.WaitGroup
}

// NewShortGridEngine 构造引擎；live 初始化失败时降级为模拟模式（与 Python 行为一致）。
func NewShortGridEngine(ctx context.Context, cfg Config, g *Globals, api *API,
	q float64, splitN, maxGrids int, p1, x float64) (*ShortGridEngine, error) {

	if splitN < 1 {
		return nil, fmt.Errorf("SPLIT_N 必须 >= 1")
	}
	if x <= 0 {
		return nil, fmt.Errorf("STEP_X 必须 > 0")
	}

	e := &ShortGridEngine{
		qPerGrid:       q,
		splitN:         splitN,
		maxGrids:       maxGrids,
		p1:             p1,
		x:              x,
		decHY:          4,
		decHX:          4,
		layerEntryData: map[int]LayerEntry{},
		api:            api,
		globals:        g,
		walletAddr:     cfg.WalletAddr,
		tickCh:         make(chan Tick, 1),
		flattenCh:      make(chan FlattenRequest, 1),
	}

	if cfg.SecretKey != "" && cfg.WalletAddr != "" {
		if err := e.initializeExchange(ctx, cfg.SecretKey); err != nil {
			LogError(g, "钱包与官方 SDK 初始化失败，已降级为模拟模式", err)
			e.liveTrading = false
			e.key = nil
		}
	}
	return e, nil
}

func (e *ShortGridEngine) initializeExchange(ctx context.Context, secretKey string) error {
	coinToAsset, assetToSz, err := BuildAssetMap(ctx, e.api)
	if err != nil {
		return err
	}
	key, err := LoadPrivateKey(secretKey)
	if err != nil {
		return err
	}

	hyAsset, ok := coinToAsset[CoinSKHY]
	if ok {
		if d, found := assetToSz[hyAsset]; found {
			e.decHY = d
		}
	}
	hxAsset, ok := coinToAsset[CoinSKHX]
	if ok {
		if d, found := assetToSz[hxAsset]; found {
			e.decHX = d
		}
	}

	if EnforceExchangeLeverage {
		for _, coin := range []string{CoinSKHY, CoinSKHX} {
			if err := e.updateLeverage(ctx, key, coinToAsset[coin], Leverage); err != nil {
				return fmt.Errorf("设置 %s 杠杆失败: %w", coin, err)
			}
		}
		logInfo("已将 %s / %s 设置为 %d 倍全仓", CoinSKHY, CoinSKHX, Leverage)
	}

	e.key = key
	e.coinToAsset = coinToAsset
	e.assetToSz = assetToSz
	e.liveTrading = true
	return nil
}

func (e *ShortGridEngine) updateLeverage(ctx context.Context, key *ecdsa.PrivateKey, asset int, leverage int) error {
	actionBytes := EncodeUpdateLeverageAction(asset, true, leverage)
	nonce := time.Now().UnixMilli()
	sig, err := SignL1Action(key, actionBytes, nonce)
	if err != nil {
		return err
	}
	action := updateLeverageJSON{Type: "updateLeverage", Asset: asset, IsCross: true, Leverage: leverage}
	res, err := e.api.Exchange(ctx, ExchangePayload{Action: action, Nonce: nonce, Signature: sig})
	if err != nil {
		return err
	}
	m := asMap(res)
	if getStr(m, "status") != "ok" {
		return fmt.Errorf("%v", res)
	}
	return nil
}

// ------------------------------
// 工作线程与行情入口
// ------------------------------

// StartWorker 启动串行交易工作协程。
func (e *ShortGridEngine) StartWorker() {
	if e.workerUp.Load() {
		return
	}
	e.mu.Lock()
	if e.stopCh != nil {
		e.mu.Unlock()
		return
	}
	e.stopCh = make(chan struct{})
	stop := e.stopCh
	e.mu.Unlock()

	e.workerUp.Store(true)
	e.workerWG.Add(1)
	go func() {
		defer e.workerWG.Done()
		defer e.workerUp.Store(false)
		e.workerLoop(stop)
	}()
}

// StopWorker 停止工作协程（最多等待 3 秒）。
func (e *ShortGridEngine) StopWorker() {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		ch := e.stopCh
		e.stopCh = nil
		e.mu.Unlock()
		if ch != nil {
			close(ch)
		}
	})
	done := make(chan struct{})
	go func() { e.workerWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

// OnTick 只更新信号，不直接交易（交易全部交给工作协程）。
func (e *ShortGridEngine) OnTick(k, pSKHY, pSKHX float64) {
	e.mu.Lock()
	if e.kSmoothed == nil {
		v := k
		e.kSmoothed = &v
	} else {
		v := EMAAlpha*k + (1.0-EMAAlpha)*(*e.kSmoothed)
		e.kSmoothed = &v
	}
	smoothed := *e.kSmoothed
	allowed := e.isAligned && !e.halted
	e.mu.Unlock()

	if !allowed {
		return
	}
	e.pushTick(Tick{K: smoothed, PSKHY: pSKHY, PSKHX: pSKHX})
}

func (e *ShortGridEngine) pushTick(t Tick) {
	select {
	case e.tickCh <- t:
		return
	default:
	}
	// 丢弃旧信号，只保留最新行情（与 Python pending_tick 语义一致）
	select {
	case <-e.tickCh:
	default:
	}
	select {
	case e.tickCh <- t:
	default:
	}
}

func (e *ShortGridEngine) workerLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case t := <-e.tickCh:
			e.safeProcessTick(t)
		case f := <-e.flattenCh:
			func() {
				defer func() {
					if r := recover(); r != nil {
						LogError(e.globals, "保护性平仓失败", fmt.Errorf("%v", r))
					}
				}()
				e.flattenAll(f.PSKHY, f.PSKHX, f.Reason)
			}()
		}
	}
}

func (e *ShortGridEngine) safeProcessTick(t Tick) {
	defer func() {
		if r := recover(); r != nil {
			LogError(e.globals, "交易工作线程异常", fmt.Errorf("%v", r))
			e.halt(fmt.Sprintf("交易工作线程异常: %v", r))
		}
	}()
	e.processTick(t.K, t.PSKHY, t.PSKHX)
}

// Halt 停机（幂等）。
func (e *ShortGridEngine) halt(reason string) {
	e.mu.Lock()
	already := e.halted
	if !already {
		e.halted = true
		e.haltReason = reason
	}
	e.mu.Unlock()
	if already {
		return
	}
	e.globals.AddLog("[⛔ 已停机] " + reason)
	logErrorf("交易停机: %s", reason)
}

// RequestEmergencyFlatten 由风控线程发起，真正下单仍由工作协程串行执行。
func (e *ShortGridEngine) RequestEmergencyFlatten(pSKHY, pSKHX float64, reason string) {
	e.halt(reason)
	req := FlattenRequest{PSKHY: pSKHY, PSKHX: pSKHX, Reason: reason}
	select {
	case e.flattenCh <- req:
		return
	default:
	}
	select {
	case <-e.flattenCh:
	default:
	}
	select {
	case e.flattenCh <- req:
	default:
	}
}

// ------------------------------
// 网格主逻辑
// ------------------------------

func (e *ShortGridEngine) processTick(k, pSKHY, pSKHX float64) {
	e.mu.Lock()
	halted := e.halted
	aligned := e.isAligned
	e.mu.Unlock()
	if halted || !aligned {
		return
	}

	actions := 0
	for actions < MaxActionsPerTick && !e.Halted() {
		held := e.PositionsHeld()
		maxGrids := e.MaxGrids()

		// 🟢 建仓
		if held < maxGrids && k >= e.p1+float64(held)*e.x {
			requiredMargin := (e.qPerGrid * 2.0 / Leverage) * MarginBufferMultiplier
			if e.LiveTrading() && e.globals.WithdrawableMargin() < requiredMargin {
				e.mu.Lock()
				e.maxGrids = held
				e.mu.Unlock()
				e.addLog(fmt.Sprintf("[⚠️ 保证金缓冲不足] 可用 %.2f < 缓冲要求 %.2f，锁定第 %d 层为上限",
					e.globals.WithdrawableMargin(), requiredMargin, held))
				break
			}

			szHy := RoundSize(e.qPerGrid/pSKHY, e.decHY)
			szHx := RoundSize(e.qPerGrid/pSKHX, e.decHX)
			if szHy <= 0 || szHx <= 0 {
				e.halt(fmt.Sprintf("计算出的下单数量无效: SKHY=%v, SKHX=%v", szHy, szHx))
				break
			}

			result := e.executePair("ENTRY", held+1, pSKHY, pSKHX, szHy, szHx, k)
			if !result.OK {
				e.handlePairFailure("建仓", result)
				break
			}

			e.mu.Lock()
			if result.FilledHy > 0 {
				e.posSKHY += result.FilledHy
			} else {
				e.posSKHY += szHy
			}
			if result.FilledHx > 0 {
				e.posSKHX += result.FilledHx
			} else {
				e.posSKHX += szHx
			}
			e.positionsHeld++
			level := e.positionsHeld
			actualK := e.vwapK(result, k)
			e.layerEntryData[level] = LayerEntry{
				K:         actualK,
				Fee:       result.Fees,
				EntryPxHy: result.AvgHy,
				EntryPxHx: result.AvgHx,
			}
			e.mu.Unlock()
			e.globals.IncEntry()

			e.addLog(fmt.Sprintf("[%s] 第%d格建仓成功! 真实k=%.4f (累计建仓%d次)",
				nowHM(), level, actualK, e.globals.EntryCount()))
			actions++
			if !e.reconcileAfterAction(e.PositionsHeld(), pSKHY, pSKHX) {
				break
			}
			continue
		}

		// 🔴 止盈
		if held > 0 && k <= e.p1+float64(held-2)*e.x {
			e.mu.Lock()
			var closeHy, closeHx float64
			if held == 1 {
				closeHy = RoundSize(e.posSKHY, e.decHY)
				closeHx = RoundSize(e.posSKHX, e.decHX)
			} else {
				closeHy = RoundSize(e.posSKHY/float64(held), e.decHY)
				closeHx = RoundSize(e.posSKHX/float64(held), e.decHX)
			}
			e.mu.Unlock()

			if closeHy <= 0 || closeHx <= 0 {
				e.halt(fmt.Sprintf("计算出的平仓数量无效: SKHY=%v, SKHX=%v", closeHy, closeHx))
				break
			}

			result := e.executePair("EXIT", held, pSKHY, pSKHX, closeHy, closeHx, k)
			if !result.OK {
				e.handlePairFailure("止盈", result)
				break
			}

			e.mu.Lock()
			entry, found := e.layerEntryData[held]
			if !found {
				entry = LayerEntry{K: e.p1 + float64(held-1)*e.x, EntryPxHy: 0, EntryPxHx: 0}
			}
			delete(e.layerEntryData, held)
			e.mu.Unlock()

			exitK := e.vwapK(result, k)
			cycleProfit := e.cycleProfit(entry, result, exitK)

			e.mu.Lock()
			if result.FilledHy > 0 {
				e.posSKHY = math.Max(0, e.posSKHY-result.FilledHy)
			} else {
				e.posSKHY = math.Max(0, e.posSKHY-closeHy)
			}
			if result.FilledHx > 0 {
				e.posSKHX = math.Max(0, e.posSKHX-result.FilledHx)
			} else {
				e.posSKHX = math.Max(0, e.posSKHX-closeHx)
			}
			e.positionsHeld--
			e.mu.Unlock()

			e.globals.IncTP()
			e.globals.AddTrueProfit(cycleProfit)

			lastFlag := ""
			if held == 1 {
				lastFlag = " <全平>"
			}
			e.addLog(fmt.Sprintf("[%s] 第%d格止盈成功! [%.4f->%.4f] 净收益: %+.2f (累计止盈%d次)%s",
				nowHM(), held, entry.K, exitK, cycleProfit, e.globals.TPCount(), lastFlag))
			actions++
			if !e.reconcileAfterAction(e.PositionsHeld(), pSKHY, pSKHX) {
				break
			}
			if e.PositionsHeld() == 0 {
				e.cleanupRemainingPositions(pSKHY, pSKHX)
			}
			continue
		}

		break
	}
}

func (e *ShortGridEngine) reconcileAfterAction(expectHeld int, pSKHY, pSKHX float64) bool {
	if !e.LiveTrading() {
		return true
	}
	posHy, posHx, ok := e.syncChainPositionsWithRetry()
	if !ok {
		e.halt("成交后无法读取链上仓位，不能确认风险状态")
		return false
	}
	healthy, reason := e.positionsAreHealthy(posHy, posHx, pSKHY, pSKHX)
	if !healthy {
		e.halt("链上仓位不一致: " + reason)
		if AutoFlattenOnReconcileError {
			e.flattenAll(pSKHY, pSKHX, "仓位不一致兜底")
		}
		return false
	}

	derived := e.deriveLayerCount(posHy, posHx, pSKHY, pSKHX)
	e.mu.Lock()
	e.posSKHY = math.Abs(posHy)
	e.posSKHX = math.Abs(posHx)
	e.mu.Unlock()

	if math.Abs(derived-float64(expectHeld)) > 0.75 {
		e.halt(fmt.Sprintf("链上推导层数 %.2f 与内部层数 %d 不一致", derived, expectHeld))
		return false
	}

	e.mu.Lock()
	e.positionsHeld = int(math.Round(derived))
	e.mu.Unlock()
	return true
}

func (e *ShortGridEngine) cleanupRemainingPositions(pSKHY, pSKHX float64) {
	if !e.LiveTrading() {
		return
	}
	posHy, posHx, ok := e.syncChainPositionsWithRetry()
	if !ok {
		return
	}
	if math.Abs(posHy) <= Epsilon(e.decHY) && math.Abs(posHx) <= Epsilon(e.decHX) {
		return
	}
	e.addLog("[兜底清理] 网格已归零但链上仍有仓位，执行 reduce-only 清理")
	e.flattenAll(pSKHY, pSKHX, "网格归零残仓")
}

func (e *ShortGridEngine) deriveLayerCount(posHy, posHx, pSKHY, pSKHX float64) float64 {
	nHy := math.Abs(posHy) * pSKHY
	nHx := math.Abs(posHx) * pSKHX
	if nHy <= 0 && nHx <= 0 {
		return 0
	}
	return ((nHy + nHx) / 2.0) / e.qPerGrid
}

func (e *ShortGridEngine) vwapK(result PairResult, fallback float64) float64 {
	if result.AvgHy > 0 && result.AvgHx > 0 {
		return calculateK(result.AvgHy, result.AvgHx)
	}
	return fallback
}

func (e *ShortGridEngine) cycleProfit(entry LayerEntry, result PairResult, exitK float64) float64 {
	if result.FillsVerified {
		return result.ClosedPnl - entry.Fee - result.Fees
	}
	if exitK == 0 {
		return -entry.Fee - result.Fees
	}
	return (e.qPerGrid * (entry.K - exitK) / exitK) - entry.Fee - result.Fees
}

// positionsAreHealthy 校验两腿方向与名义价值平衡。
func (e *ShortGridEngine) positionsAreHealthy(posHy, posHx, pSKHY, pSKHX float64) (bool, string) {
	epsY := Epsilon(e.decHY)
	epsX := Epsilon(e.decHX)
	hasHy := math.Abs(posHy) > epsY
	hasHx := math.Abs(posHx) > epsX

	if !hasHy && !hasHx {
		return true, "空仓"
	}
	if hasHy != hasHx {
		return false, fmt.Sprintf("单腿裸露: SKHY=%v, SKHX=%v", posHy, posHx)
	}
	if posHy > 0 {
		return false, fmt.Sprintf("SKHY 应为空头，实际 szi=%v", posHy)
	}
	if posHx < 0 {
		return false, fmt.Sprintf("SKHX 应为多头，实际 szi=%v", posHx)
	}

	nHy := math.Abs(posHy) * pSKHY
	nHx := math.Abs(posHx) * pSKHX
	base := math.Max(nHy, nHx)
	if base <= 0 {
		return false, "两腿名义价值无效"
	}
	imbalance := math.Abs(nHy-nHx) / base
	if imbalance > HedgeImbalanceTolerance {
		return false, fmt.Sprintf("两腿名义价值失衡 %.1f%%: %.2f vs %.2f", imbalance*100, nHy, nHx)
	}
	return true, fmt.Sprintf("对冲平衡偏差 %.1f%%", imbalance*100)
}

// ------------------------------
// 订单执行
// ------------------------------

func newCloid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("0x%032x", time.Now().UnixNano())
	}
	return "0x" + hex.EncodeToString(b[:])
}

func (e *ShortGridEngine) placeOrderWithRetry(coin string, isBuy bool, sz, px float64,
	reduceOnly bool, purpose string) OrderOutcome {

	if sz <= 0 {
		return OrderOutcome{OK: false, Reason: fmt.Sprintf("%s: 数量必须大于 0，实际为 %v", purpose, sz)}
	}

	cloid := newCloid()
	lastReason := "未执行"
	asset := e.coinToAsset[coin]

	for attempt := 1; attempt <= OrderRetries; attempt++ {
		wire := OrderWire{
			Asset:      asset,
			IsBuy:      isBuy,
			Px:         FloatToWire(px),
			Sz:         FloatToWire(sz),
			ReduceOnly: reduceOnly,
			Cloid:      cloid,
		}
		actionBytes := EncodeOrderAction([]OrderWire{wire}, "na")
		action := toOrderActionJSON([]OrderWire{wire}, "na")
		nonce := time.Now().UnixMilli()

		sig, err := SignL1Action(e.key, actionBytes, nonce)
		if err != nil {
			lastReason = err.Error()
			if status, ok := e.queryOrderStatusByCloid(cloid); ok && status == "filled" {
				return OrderOutcome{OK: true, Reason: "异常后 cloid 查询状态=filled", Cloid: cloid, FilledSz: sz, AvgPx: px}
			}
			if attempt < OrderRetries {
				time.Sleep(OrderRetrySleep)
			}
			continue
		}

		res, err := e.api.Exchange(context.Background(), ExchangePayload{Action: action, Nonce: nonce, Signature: sig})
		if err != nil {
			lastReason = err.Error()
			if status, ok := e.queryOrderStatusByCloid(cloid); ok && status == "filled" {
				return OrderOutcome{OK: true, Reason: "异常后 cloid 查询状态=filled", Cloid: cloid, FilledSz: sz, AvgPx: px}
			}
			if attempt < OrderRetries {
				time.Sleep(OrderRetrySleep)
			}
			continue
		}

		ok, reason := parseOrderResponse(res)
		if ok {
			filledSz, avgPx := extractFilledInfo(res, sz, px)
			return OrderOutcome{OK: true, Reason: reason, Cloid: cloid, FilledSz: filledSz, AvgPx: avgPx}
		}
		lastReason = reason

		lower := strings.ToLower(reason)
		if strings.Contains(lower, "cloid") || strings.Contains(lower, "duplicate") {
			if status, found := e.queryOrderStatusByCloid(cloid); found && status == "filled" {
				return OrderOutcome{OK: true, Reason: "cloid 查询状态=filled", Cloid: cloid, FilledSz: sz, AvgPx: px}
			}
		}
		return OrderOutcome{OK: false, Reason: reason, Cloid: cloid}
	}
	return OrderOutcome{OK: false, Reason: lastReason, Cloid: cloid}
}

func extractFilledInfo(res any, fallbackSz, fallbackPx float64) (float64, float64) {
	statuses := asSlice(asMap(asMap(asMap(res)["response"])["data"])["statuses"])
	if len(statuses) == 0 {
		return fallbackSz, fallbackPx
	}
	filled := asMap(asMap(statuses[0])["filled"])
	if filled == nil {
		return fallbackSz, fallbackPx
	}
	sz := getFloat(filled, "totalSz")
	px := getFloat(filled, "avgPx")
	if sz <= 0 {
		sz = fallbackSz
	}
	if px <= 0 {
		px = fallbackPx
	}
	return sz, px
}

func parseOrderResponse(res any) (bool, string) {
	m := asMap(res)
	if m == nil {
		return false, "返回值不是 dict"
	}
	if getStr(m, "status") != "ok" {
		return false, fmt.Sprintf("顶层状态异常: %q", getStr(m, "status"))
	}
	statuses := asSlice(asMap(asMap(m["response"])["data"])["statuses"])
	if len(statuses) == 0 {
		return false, "statuses 为空"
	}
	for _, item := range statuses {
		im := asMap(item)
		if im == nil {
			return false, "status 条目格式异常"
		}
		if errStr := getStr(im, "error"); errStr != "" {
			return false, "订单错误: " + errStr
		}
		if _, has := im["filled"]; has {
			return true, "filled"
		}
		if _, has := im["resting"]; has {
			return false, "IOC 订单意外进入 resting 状态"
		}
	}
	return false, fmt.Sprintf("未识别的订单状态: %v", statuses)
}

func (e *ShortGridEngine) queryOrderStatusByCloid(cloid string) (string, bool) {
	if !e.LiveTrading() || e.walletAddr == "" {
		return "", false
	}
	res, err := e.api.Info(context.Background(), map[string]any{
		"type": "orderStatus",
		"user": e.walletAddr,
		"oid":  cloid,
	}, 5*time.Second)
	if err != nil {
		return "", false
	}
	m := asMap(res)
	if m == nil || getStr(m, "status") != "order" {
		return "", false
	}
	order := asMap(m["order"])
	if order == nil {
		return "", false
	}
	status := getStr(order, "status")
	if status == "" {
		return "", false
	}
	return status, true
}

// ExecutePair 成对下单（对外）
func (e *ShortGridEngine) executePair(action string, level int, pSKHY, pSKHX,
	totalSzHy, totalSzHx, kVal float64) PairResult {
	e.orderMu.Lock()
	defer e.orderMu.Unlock()
	return e.executePairLocked(action, level, pSKHY, pSKHX, totalSzHy, totalSzHx, kVal)
}

func (e *ShortGridEngine) executePairLocked(action string, level int, pSKHY, pSKHX,
	totalSzHy, totalSzHx, kVal float64) PairResult {

	if action != "ENTRY" && action != "EXIT" {
		return PairResult{OK: false, Reason: "未知动作 " + action}
	}

	startMS := time.Now().UnixMilli()
	requestedHy := totalSzHy
	requestedHx := totalSzHx
	remainingHy := totalSzHy
	remainingHx := totalSzHx
	sliceHy := RoundSize(totalSzHy/float64(e.splitN), e.decHY)
	sliceHx := RoundSize(totalSzHx/float64(e.splitN), e.decHX)
	allFills := make([]map[string]any, 0, 4)

	for i := 0; i < e.splitN; i++ {
		isLast := i == e.splitN-1
		actualHy := sliceHy
		actualHx := sliceHx
		if isLast {
			actualHy = RoundSize(remainingHy, e.decHY)
			actualHx = RoundSize(remainingHx, e.decHX)
		}
		remainingHy = RoundSize(remainingHy-actualHy, e.decHY)
		remainingHx = RoundSize(remainingHx-actualHx, e.decHX)

		if actualHy <= 0 || actualHx <= 0 {
			return PairResult{OK: false, Reason: fmt.Sprintf("切片 %d 数量无效: hy=%v, hx=%v", i+1, actualHy, actualHx)}
		}

		if !e.LiveTrading() {
			allFills = append(allFills,
				map[string]any{"coin": CoinSKHY, "sz": fmt.Sprintf("%v", actualHy), "px": fmt.Sprintf("%v", pSKHY), "fee": "0", "closedPnl": "0"},
				map[string]any{"coin": CoinSKHX, "sz": fmt.Sprintf("%v", actualHx), "px": fmt.Sprintf("%v", pSKHX), "fee": "0", "closedPnl": "0"},
			)
			if !isLast {
				time.Sleep(SliceDelay)
			}
			continue
		}

		buyPxHx := MakeLimitPrice(pSKHX, true, PairSlippagePct)
		sellPxHx := MakeLimitPrice(pSKHX, false, PairSlippagePct)
		buyPxHy := MakeLimitPrice(pSKHY, true, PairSlippagePct)
		sellPxHy := MakeLimitPrice(pSKHY, false, PairSlippagePct)

		if action == "ENTRY" {
			// 先建立多头 SKHX，再建立空头 SKHY
			first := e.placeOrderWithRetry(CoinSKHX, true, actualHx, buyPxHx, false, "建仓-SKHX")
			if !first.OK {
				return PairResult{OK: false, Reason: fmt.Sprintf("切片 %d SKHX 建仓失败: %s", i+1, first.Reason)}
			}
			second := e.placeOrderWithRetry(CoinSKHY, false, actualHy, sellPxHy, false, "建仓-SKHY")
			if !second.OK {
				rollbackSz := first.FilledSz
				if rollbackSz <= 0 {
					rollbackSz = actualHx
				}
				rollback := e.placeOrderWithRetry(CoinSKHX, false, rollbackSz, sellPxHx, true, "建仓失败回滚-SKHX")
				reason := fmt.Sprintf("切片 %d SKHY 建仓失败: %s", i+1, second.Reason)
				if rollback.OK {
					reason += "；SKHX 已回滚"
				} else {
					reason += fmt.Sprintf("；SKHX 回滚失败: %s", rollback.Reason)
					if AutoFlattenOnError {
						e.emergencyFlatten(pSKHY, pSKHX, "成对建仓失败")
					}
				}
				return PairResult{OK: false, Reason: reason}
			}
		} else {
			// 先平多头 SKHX，再平空头 SKHY
			first := e.placeOrderWithRetry(CoinSKHX, false, actualHx, sellPxHx, true, "止盈-SKHX")
			if !first.OK {
				return PairResult{OK: false, Reason: fmt.Sprintf("切片 %d SKHX 平仓失败: %s", i+1, first.Reason)}
			}
			second := e.placeOrderWithRetry(CoinSKHY, true, actualHy, buyPxHy, true, "止盈-SKHY")
			if !second.OK {
				rollbackSz := first.FilledSz
				if rollbackSz <= 0 {
					rollbackSz = actualHx
				}
				rollback := e.placeOrderWithRetry(CoinSKHX, true, rollbackSz, buyPxHx, false, "止盈失败回滚-SKHX")
				reason := fmt.Sprintf("切片 %d SKHY 平仓失败: %s", i+1, second.Reason)
				if rollback.OK {
					reason += "；SKHX 已重新对冲"
				} else {
					reason += fmt.Sprintf("；SKHX 回滚失败: %s", rollback.Reason)
					if AutoFlattenOnError {
						e.emergencyFlatten(pSKHY, pSKHX, "成对止盈失败")
					}
				}
				return PairResult{OK: false, Reason: reason}
			}
		}

		if !isLast {
			time.Sleep(SliceDelay)
		}
	}

	if !e.LiveTrading() {
		var hySz, hxSz float64
		for _, f := range allFills {
			if getStr(f, "coin") == CoinSKHY {
				hySz += numToFloat(f["sz"])
			} else {
				hxSz += numToFloat(f["sz"])
			}
		}
		return PairResult{
			OK: true, RequestedHy: requestedHy, RequestedHx: requestedHx,
			FilledHy: hySz, FilledHx: hxSz, AvgHy: pSKHY, AvgHx: pSKHX,
			Fees: 0, ClosedPnl: 0, FillsVerified: false, FillDetails: allFills,
		}
	}

	time.Sleep(FillWait)
	fills := e.fetchFillsSince(startMS)
	relevant := make([]map[string]any, 0, len(fills))
	for _, f := range fills {
		coin := getStr(f, "coin")
		if coin == CoinSKHY || coin == CoinSKHX {
			relevant = append(relevant, f)
		}
	}

	var filledHy, filledHx, fees, closedPnl float64
	hyFills := make([]map[string]any, 0, 2)
	hxFills := make([]map[string]any, 0, 2)
	for _, f := range relevant {
		coin := getStr(f, "coin")
		if isHyCoin(coin) {
			hyFills = append(hyFills, f)
			filledHy += getFloat(f, "sz")
		} else {
			hxFills = append(hxFills, f)
			filledHx += getFloat(f, "sz")
		}
		fees += math.Abs(getFloat(f, "fee"))
		closedPnl += getFloat(f, "closedPnl")
	}
	avgHy := fillVWAP(hyFills, pSKHY)
	avgHx := fillVWAP(hxFills, pSKHX)
	verified := len(hyFills) > 0 && len(hxFills) > 0

	if filledHy <= 0 {
		filledHy = requestedHy
	}
	if filledHx <= 0 {
		filledHx = requestedHx
	}
	return PairResult{
		OK: true, RequestedHy: requestedHy, RequestedHx: requestedHx,
		FilledHy: filledHy, FilledHx: filledHx, AvgHy: avgHy, AvgHx: avgHx,
		Fees: fees, ClosedPnl: closedPnl, FillsVerified: verified, FillDetails: relevant,
	}
}

func isHyCoin(coin string) bool {
	return strings.Contains(coin, "SKHY") && !strings.Contains(coin, "SKHX") && !strings.Contains(coin, "NIX")
}

func fillVWAP(fills []map[string]any, fallback float64) float64 {
	var totalSz, totalCost float64
	for _, f := range fills {
		sz := getFloat(f, "sz")
		px := getFloat(f, "px")
		if sz > 0 && px > 0 {
			totalSz += sz
			totalCost += sz * px
		}
	}
	if totalSz > 0 {
		return totalCost / totalSz
	}
	return fallback
}

func (e *ShortGridEngine) fetchFillsSince(startMS int64) []map[string]any {
	if e.walletAddr == "" {
		return nil
	}
	for attempt := 1; attempt <= 3; attempt++ {
		res, err := e.api.Info(context.Background(), map[string]any{
			"type":            "userFillsByTime",
			"user":            e.walletAddr,
			"startTime":       startMS,
			"aggregateByTime": false,
		}, 10*time.Second)
		if err != nil {
			logErrorf("获取成交记录失败(%d/3): %v", attempt, err)
			time.Sleep(400 * time.Millisecond)
			continue
		}
		list := asSlice(res)
		out := make([]map[string]any, 0, len(list))
		for _, item := range list {
			if m := asMap(item); m != nil {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func (e *ShortGridEngine) handlePairFailure(actionName string, result PairResult) {
	reason := result.Reason
	if len(reason) > 120 {
		reason = reason[:120]
	}
	e.addLog(fmt.Sprintf("[交易失败] %s: %s", actionName, reason))
	e.halt(fmt.Sprintf("%s失败，停止继续交易: %s", actionName, reason))
}

func (e *ShortGridEngine) emergencyFlatten(pSKHY, pSKHX float64, reason string) {
	e.addLog("[🚨 紧急平仓] " + reason)
	e.flattenAll(pSKHY, pSKHX, reason)
}

// FlattenAll 保护性全平。
func (e *ShortGridEngine) FlattenAll(pSKHY, pSKHX float64, reason string) bool {
	e.orderMu.Lock()
	defer e.orderMu.Unlock()
	return e.flattenAllLocked(pSKHY, pSKHX, reason)
}

func (e *ShortGridEngine) flattenAll(pSKHY, pSKHX float64, reason string) bool {
	return e.FlattenAll(pSKHY, pSKHX, reason)
}

func (e *ShortGridEngine) flattenAllLocked(pSKHY, pSKHX float64, reason string) bool {
	if !e.LiveTrading() {
		e.mu.Lock()
		e.posSKHY = 0
		e.posSKHX = 0
		e.positionsHeld = 0
		e.mu.Unlock()
		return true
	}

	posHy, posHx, ok := e.syncChainPositionsWithRetry()
	if !ok {
		e.addLog("[平仓失败] 无法读取链上仓位: " + reason)
		return false
	}
	okAll := true

	if math.Abs(posHy) > Epsilon(e.decHY) {
		isBuy := posHy < 0
		px := MakeLimitPrice(pSKHY, isBuy, PairSlippagePct)
		res := e.placeOrderWithRetry(CoinSKHY, isBuy, RoundSize(math.Abs(posHy), e.decHY), px, true, "保护性平仓-SKHY")
		okAll = okAll && res.OK
	}
	if math.Abs(posHx) > Epsilon(e.decHX) {
		isBuy := posHx < 0 // 平多头应卖出
		px := MakeLimitPrice(pSKHX, isBuy, PairSlippagePct)
		res := e.placeOrderWithRetry(CoinSKHX, isBuy, RoundSize(math.Abs(posHx), e.decHX), px, true, "保护性平仓-SKHX")
		okAll = okAll && res.OK
	}

	time.Sleep(FillWait)
	if posHy2, posHx2, ok2 := e.syncChainPositionsWithRetry(); ok2 {
		e.mu.Lock()
		e.posSKHY = math.Abs(posHy2)
		e.posSKHX = math.Abs(posHx2)
		if math.Abs(posHy2) <= Epsilon(e.decHY) && math.Abs(posHx2) <= Epsilon(e.decHX) {
			e.positionsHeld = 0
		}
		e.mu.Unlock()
	}
	e.addLog(fmt.Sprintf("[保护性平仓完成] reason=%s, all_orders_ok=%v", reason, okAll))
	return okAll
}

// ------------------------------
// 仓位与启动对准
// ------------------------------

func (e *ShortGridEngine) syncChainPositionsWithRetry() (float64, float64, bool) {
	if e.walletAddr == "" {
		return 0, 0, false
	}
	for attempt := 1; attempt <= 3; attempt++ {
		res, err := e.api.Info(context.Background(), map[string]any{
			"type": "clearinghouseState",
			"user": e.walletAddr,
			"dex":  "xyz",
		}, 10*time.Second)
		if err != nil {
			logErrorf("读取链上仓位失败(%d/3): %v", attempt, err)
			time.Sleep(400 * time.Millisecond)
			continue
		}
		var rY, rX float64
		for _, item := range asSlice(asMap(res)["assetPositions"]) {
			p := asMap(asMap(item)["position"])
			if p == nil {
				continue
			}
			coin := strings.ToUpper(getStr(p, "coin"))
			szi := getFloat(p, "szi")
			switch {
			case strings.Contains(coin, "SKHX") || strings.Contains(coin, "NIX"):
				rX = szi
			case strings.Contains(coin, "SKHY"):
				rY = szi
			}
		}
		e.mu.Lock()
		e.posSKHY = math.Abs(rY)
		e.posSKHX = math.Abs(rX)
		e.mu.Unlock()
		return rY, rX, true
	}
	return 0, 0, false
}

// AlignPositionsOnStartup 人工确认后的启动对准。
func (e *ShortGridEngine) AlignPositionsOnStartup(pSKHY, pSKHX float64, realHy, realHx float64) bool {
	defer func() {
		if r := recover(); r != nil {
			LogError(e.globals, "启动对准异常", fmt.Errorf("%v", r))
			e.addLog(fmt.Sprintf("[启动对准异常] %v", r))
		}
	}()

	actualLayers := e.deriveLayerCount(realHy, realHx, pSKHY, pSKHX)
	targetLayers := e.RequiredLayers(calculateK(pSKHY, pSKHX))
	diffNotional := (float64(targetLayers) - actualLayers) * e.qPerGrid

	var szHy, szHx float64
	action := ""
	if math.Abs(diffNotional) > 100.0 {
		if diffNotional > 0 {
			szHy = RoundSize(diffNotional/pSKHY, e.decHY)
			szHx = RoundSize(diffNotional/pSKHX, e.decHX)
			action = "ENTRY"
			e.addLog(fmt.Sprintf("[启动对准] 补充约 %.1f 名义价值", diffNotional))
		} else {
			szHy = RoundSize(math.Abs(diffNotional)/pSKHY, e.decHY)
			szHx = RoundSize(math.Abs(diffNotional)/pSKHX, e.decHX)
			action = "EXIT"
			e.addLog(fmt.Sprintf("[启动对准] 减少约 %.1f 名义价值", math.Abs(diffNotional)))
		}

		level := targetLayers
		if level < 1 {
			level = 1
		}
		result := e.executePair(action, level, pSKHY, pSKHX, szHy, szHx, calculateK(pSKHY, pSKHX))
		if !result.OK {
			e.addLog("[启动对准失败] " + result.Reason)
			return false
		}
	}

	var posHy, posHx float64
	var ok bool
	if e.LiveTrading() {
		posHy, posHx, ok = e.syncChainPositionsWithRetry()
		if !ok {
			e.addLog("[启动对准失败] 无法读取对准后的链上仓位")
			return false
		}
	} else {
		posHy, posHx = realHy, realHx
		if action == "ENTRY" {
			posHy -= szHy
			posHx += szHx
		} else if action == "EXIT" {
			posHy += szHy
			posHx -= szHx
		}
	}

	healthy, reason := e.positionsAreHealthy(posHy, posHx, pSKHY, pSKHX)
	if !healthy {
		e.addLog("[启动对准失败] " + reason)
		return false
	}

	held := int(math.Round(e.deriveLayerCount(posHy, posHx, pSKHY, pSKHX)))
	e.mu.Lock()
	e.posSKHY = math.Abs(posHy)
	e.posSKHX = math.Abs(posHx)
	if held > e.maxGrids {
		e.mu.Unlock()
		e.addLog(fmt.Sprintf("[启动对准] 链上仓位约 %d 层，超过上限 %d；将按当前实际仓位接管，不再新增网格", held, e.MaxGrids()))
		e.mu.Lock()
		held = e.maxGrids
	}
	e.positionsHeld = held
	for lvl := 1; lvl <= held; lvl++ {
		if _, exists := e.layerEntryData[lvl]; !exists {
			e.layerEntryData[lvl] = LayerEntry{
				K:         calculateK(pSKHY, pSKHX),
				Fee:       0,
				EntryPxHy: pSKHY,
				EntryPxHx: pSKHX,
			}
		}
	}
	e.isAligned = true
	decHY, decHX := e.decHY, e.decHX
	e.mu.Unlock()

	e.addLog(fmt.Sprintf("[启动对准] 接管第 %d 层 | SKHY=%.*f SKHX=%.*f", held, decHY, posHy, decHX, posHx))
	return true
}

// RequiredLayers 按当前 K 计算目标层数。
func (e *ShortGridEngine) RequiredLayers(k float64) int {
	if k < e.p1 {
		return 0
	}
	n := int(math.Floor((k-e.p1)/e.x)) + 1
	if n > e.maxGrids {
		n = e.maxGrids
	}
	if n < 0 {
		n = 0
	}
	return n
}

// NextTriggers 返回下一建仓线/止盈线。
func (e *ShortGridEngine) NextTriggers() (entry *float64, exit *float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.positionsHeld < e.maxGrids {
		v := e.p1 + float64(e.positionsHeld)*e.x
		entry = &v
	}
	if e.positionsHeld > 0 {
		v := e.p1 + float64(e.positionsHeld-2)*e.x
		exit = &v
	}
	return entry, exit
}

// ------------------------------
// UI 访问器
// ------------------------------

func (e *ShortGridEngine) addLog(text string) { e.globals.AddLog(text) }

// PositionsHeld 当前层数
func (e *ShortGridEngine) PositionsHeld() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.positionsHeld
}

// PosSKHY 程序账本 SKHY 数量（正数）
func (e *ShortGridEngine) PosSKHY() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.posSKHY
}

// PosSKHX 程序账本 SKHX 数量（正数）
func (e *ShortGridEngine) PosSKHX() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.posSKHX
}

// MaxGrids 当前层数上限（可能被保证金缓冲下调）
func (e *ShortGridEngine) MaxGrids() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.maxGrids
}

// LiveTrading 是否实盘
func (e *ShortGridEngine) LiveTrading() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.liveTrading
}

// IsAligned 是否完成对准
func (e *ShortGridEngine) IsAligned() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.isAligned
}

// SetAligned 供启动对准流程设置
func (e *ShortGridEngine) SetAligned(v bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isAligned = v
}

// Halted 是否停机
func (e *ShortGridEngine) Halted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.halted
}

// HaltReason 停机原因
func (e *ShortGridEngine) HaltReason() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.haltReason
}

// KSmoothed 平滑 K 值
func (e *ShortGridEngine) KSmoothed() *float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.kSmoothed == nil {
		return nil
	}
	v := *e.kSmoothed
	return &v
}

// WorkerRunning 工作协程是否在跑
func (e *ShortGridEngine) WorkerRunning() bool { return e.workerUp.Load() }

// DecHY SKHY 数量小数位
func (e *ShortGridEngine) DecHY() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.decHY
}

// DecHX SKHX 数量小数位
func (e *ShortGridEngine) DecHX() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.decHX
}
