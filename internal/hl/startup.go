package hl

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

// PerformStartupConfirmationCheck 启动对准与建仓前的人工确认。
// 无有效行情时直接失败，绝不解除交易保护（与 Python 版一致）。
func PerformStartupConfirmationCheck(eng *ShortGridEngine, prices *PriceState) bool {
	if !eng.LiveTrading() {
		eng.SetAligned(true)
		return true
	}

	fmt.Println("\n[启动对准评估] 正在读取链上持仓与盘口行情...")
	var pSKHY, pSKHX float64
	fresh := false
	for i := 0; i < 20; i++ {
		if b, a, ok := prices.Fresh(); ok {
			pSKHY, pSKHX, fresh = b, a, true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !fresh {
		fmt.Println("[错误] 盘口未就绪或行情已过期，已拒绝启动交易。")
		eng.SetAligned(false)
		return false
	}

	realHy, realHx, ok := eng.syncChainPositionsWithRetry()
	if !ok {
		fmt.Println("[错误] 无法读取链上仓位，已拒绝启动交易。")
		eng.SetAligned(false)
		return false
	}

	healthy, reason := eng.positionsAreHealthy(realHy, realHx, pSKHY, pSKHX)
	if !healthy {
		fmt.Printf("[错误] 启动仓位检查失败: %s\n", reason)
		fmt.Println("请先人工处理成单腿、反向或严重失衡仓位后再启动。")
		eng.SetAligned(false)
		return false
	}

	k := calculateK(pSKHY, pSKHX)
	targetLayers := eng.RequiredLayers(k)
	actualLayers := eng.deriveLayerCount(realHy, realHx, pSKHY, pSKHX)
	diffNotional := (float64(targetLayers) - actualLayers) * eng.qPerGrid

	fmt.Printf("[启动评估] k=%.4f | 目标层数=%d | 当前约=%.2f 层\n", k, targetLayers, actualLayers)
	if math.Abs(diffNotional) <= 100.0 {
		fmt.Println("[启动对准] 当前仓位已满足目标，无需交易，直接接管。")
		eng.adoptStartupState(targetLayers, realHy, realHx)
		eng.SetAligned(true)
		return true
	}

	direction := "补充建仓"
	if diffNotional < 0 {
		direction = "减少仓位"
	}
	fmt.Println("\n" + strings.Repeat("=", 72))
	fmt.Printf("  [⚠️ 启动对准确认] %s约 %.1f 名义价值\n", direction, math.Abs(diffNotional))
	fmt.Printf("  - 目标: 第 %d 层 (%.1f)\n", targetLayers, float64(targetLayers)*eng.qPerGrid)
	fmt.Printf("  - 当前: 约 %.2f 层 (%.1f)\n", actualLayers, actualLayers*eng.qPerGrid)
	fmt.Printf("  - SKHY=%.*f, SKHX=%.*f\n", eng.decHY, realHy, eng.decHX, realHx)
	fmt.Println(strings.Repeat("=", 72))
	fmt.Print("  请输入数字 1 并回车确认执行对准 (输入其他内容退出): ")

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		fmt.Println("\n[取消执行] 已中断。")
		return false
	}
	if strings.TrimSpace(scanner.Text()) != "1" {
		fmt.Println("\n[取消执行] 未输入 1，程序退出。")
		return false
	}
	return eng.AlignPositionsOnStartup(pSKHY, pSKHX, realHy, realHx)
}

func (e *ShortGridEngine) adoptStartupState(targetLayers int, realHy, realHx float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.positionsHeld = targetLayers
	e.posSKHY = math.Abs(realHy)
	e.posSKHX = math.Abs(realHx)
}

// CheckStopLoss 浮动亏损紧急停止检查。
func CheckStopLoss(eng *ShortGridEngine, prices *PriceState, g *Globals, stopLossUSDC float64) {
	if eng == nil || !eng.LiveTrading() || eng.Halted() {
		return
	}
	if stopLossUSDC <= 0 {
		return
	}
	floating := g.FloatingPnl()
	if floating > -stopLossUSDC {
		return
	}
	reason := fmt.Sprintf("浮动亏损 %.2f <= -%.2f", floating, stopLossUSDC)
	if p1, p2, ok := prices.Fresh(); ok {
		eng.RequestEmergencyFlatten(p1, p2, reason)
	} else {
		eng.halt(reason)
	}
}
