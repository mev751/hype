package hl

import (
	"fmt"
	"strings"
)

const (
	lineEqual = "=========================================================================="
	lineDash  = "--------------------------------------------------------------------------"
)

func fmtPriceOpt(v float64, ok bool) string {
	if !ok {
		return "Wait..."
	}
	return fmt.Sprintf("%.6f", v)
}

func fmtTrigger(v *float64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%.5f", *v)
}

// RenderUI 渲染终端全景视图（与 Python 版布局一致）。
func RenderUI(snap Snapshot, prices *PriceState, eng *ShortGridEngine, wsConnected bool, elapsedSeconds int64) {
	bidHy, askHy, hasHy := prices.Snapshot(SymSKHY)
	bidHx, askHx, hasHx := prices.Snapshot(SymSKHX)

	var kRaw *float64
	if hasHy && hasHx {
		v := calculateK(bidHy, askHx)
		kRaw = &v
	}

	modeText := "初始化中..."
	held := 0
	posY, posX := 0.0, 0.0
	var nxtEntry, nxtExit *float64
	alignTxt := "初始化..."
	statusTxt := "等待中"
	maxGridsText := fmt.Sprintf("%d", MaxGridsDefault)

	if eng != nil {
		if eng.LiveTrading() {
			modeText = "[实盘交易开启 🟢]"
		} else {
			modeText = "[模拟运行模式 🟡]"
		}
		held = eng.PositionsHeld()
		posY = eng.PosSKHY()
		posX = eng.PosSKHX()
		maxGridsText = fmt.Sprintf("%d", eng.MaxGrids())
		nxtEntry, nxtExit = eng.NextTriggers()
		if eng.LiveTrading() {
			if eng.IsAligned() {
				alignTxt = "对齐成功"
			} else {
				alignTxt = "未对准/禁止交易"
			}
		} else {
			alignTxt = "模拟对准完成"
		}
		if eng.Halted() {
			statusTxt = "已停机 ⛔"
		} else if eng.WorkerRunning() {
			statusTxt = "工作中 ⚙️"
		} else {
			statusTxt = "空闲中"
		}
	}

	valProgY := -posY * bidHy
	valProgX := posX * askHx
	valRealY := snap.RealPosSKHY * bidHy
	valRealX := snap.RealPosSKHX * askHx

	totalNetPnl := snap.RealizedPnl + snap.FloatingPnl + snap.Funding - snap.Fees

	channelText := "[重连中...]"
	if wsConnected {
		channelText = "[连通] ●"
	}

	lines := []string{
		"  HL SKHY/SKHX 相对价值做空网格机器人 v3.9 (Go)",
		fmt.Sprintf("  模式: %s | 对齐: [%s] | 引擎: [%s]", modeText, alignTxt, statusTxt),
		fmt.Sprintf("  时间: %s | 运行时间: %s | 通道: %s",
			formatNow(), FormatRuntime(elapsedSeconds), channelText),
		"  " + lineEqual,
		"  [ 账户资产与盈亏透视 (链上同步) ]",
	}

	if eng == nil || eng.walletAddr == "" {
		lines = append(lines, "  (未填写 WALLET_ADDR，无法追踪资产与对账)")
	} else {
		if snap.HasEquity {
			lines = append(lines, fmt.Sprintf("  当前总净值: %.2f | 可用保证金: %.2f | 总净盈亏: %+.2f",
				snap.CurrentEquity, snap.WithdrawableMargin, totalNetPnl))
			lines = append(lines, fmt.Sprintf("  盈亏拆解: 网格实现 %+.2f | 当前浮动 %+.2f | 资金费 %+.2f | 手续费 -%.2f",
				snap.RealizedPnl, snap.FloatingPnl, snap.Funding, snap.Fees))
			lines = append(lines, fmt.Sprintf("  策略统计: 累计建仓 %d 次 | 累计止盈 %d 次 | 网格估算收益 %+.2f",
				snap.EntryCount, snap.TPCount, snap.TrueProfit))
		}
		lines = append(lines, fmt.Sprintf("  程序账本: SKHY=%.4f (%+.1fk) | SKHYNIX=%.4f (%+.1fk)",
			-posY, valProgY/1000, posX, valProgX/1000))
		lines = append(lines, fmt.Sprintf("  链上真实: SKHY=%.4f (%+.1fk) | SKHYNIX=%.4f (%+.1fk)",
			snap.RealPosSKHY, valRealY/1000, snap.RealPosSKHX, valRealX/1000))
		if eng != nil && eng.Halted() {
			reason := eng.HaltReason()
			if len(reason) > 100 {
				reason = reason[:100]
			}
			lines = append(lines, "  ⛔ 停机原因: "+reason)
		}
	}

	lines = append(lines,
		"  "+lineDash,
		"  [ 行情盘口与策略状态 ]",
		fmt.Sprintf("  # SKHY-USDC    : 买1=%s  | 卖1=%s", fmtPriceOpt(bidHy, hasHy), fmtPriceOpt(askHy, hasHy)),
		fmt.Sprintf("  # SKHYNIX-USDC : 买1=%s | 卖1=%s", fmtPriceOpt(bidHx, hasHx), fmtPriceOpt(askHx, hasHx)),
		fmt.Sprintf("  # 基准P1=%v | 步长X=%v | 单次=%v | 杠杆=%d", P1, StepX, QUSDC, Leverage),
		fmt.Sprintf("  # 拆分=%d笔(%.2fs) | 持有=[%d/%s层]", SplitN, SliceDelay.Seconds(), held, maxGridsText),
		"  "+lineEqual,
	)

	emaKStr := "Wait..."
	if eng != nil {
		if v := eng.KSmoothed(); v != nil {
			emaKStr = fmt.Sprintf("%.6f", *v)
		}
	}
	rawKStr := "Wait..."
	if kRaw != nil {
		rawKStr = fmt.Sprintf("%.6f", *kRaw)
	}

	entryStr := "锁定封顶"
	if nxtEntry != nil {
		entryStr = fmt.Sprintf("%.5f", *nxtEntry)
	}
	exitStr := "空仓无"
	if nxtExit != nil {
		exitStr = fmt.Sprintf("%.5f", *nxtExit)
	}

	lines = append(lines,
		fmt.Sprintf("  # 下一建仓线(做空): %s", entryStr),
		fmt.Sprintf("  # 当前平滑值(原始): %s (%s)", emaKStr, rawKStr),
		fmt.Sprintf("  # 下一平仓线(止盈): %s", exitStr),
		"  "+lineEqual,
		"  [ 底层交易与对账日志 ]",
	)
	if len(snap.Logs) == 0 {
		lines = append(lines, "  <策略监控中...等待行情触发>")
	} else {
		for _, l := range snap.Logs {
			lines = append(lines, "  "+l)
		}
	}
	lines = append(lines, "  "+lineEqual, "  [Ctrl+C 停止交易并退出；默认不自动平仓]", "")

	// ANSI 清屏 + 光标归位（替代 os.system("clear")，避免每次 fork 进程）
	fmt.Print("\x1b[2J\x1b[H")
	fmt.Println(strings.Join(lines, "\n"))
}
