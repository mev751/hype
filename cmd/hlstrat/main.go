// Command hlstrat 是 HL_SKHY_STRAT_3.9.py 的 Go 版本：
// Hyperliquid SKHY/SKHX 相对价值做空网格机器人。
//
// 环境变量与 Python 版一致：
//
//	HL_SECRET_KEY      私钥（不填则为模拟模式）
//	HL_WALLET_ADDR     钱包地址（不填则由私钥推导）
//	HL_STOP_LOSS_USDC  浮动亏损紧急停止阈值（0 关闭）
//	HL_LOG_PATH        日志路径（默认 hype_strategy.log）
package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mev751/hype/internal/hl"
)

func main() {
	cfg := hl.LoadConfig()
	if f := hl.SetupLogging(cfg.LogPath); f != nil {
		defer f.Close()
	}

	fmt.Print("\x1b[2J\x1b[H")
	fmt.Println("正在启动 SKHY/SKHX 相对价值网格机器人 v3.9 (Go)...")

	if cfg.SecretKey != "" && cfg.WalletAddr == "" {
		key, err := hl.LoadPrivateKey(cfg.SecretKey)
		if err != nil {
			fmt.Printf("无法从 HL_SECRET_KEY 推导钱包地址: %v\n", err)
			os.Exit(1)
		}
		cfg.WalletAddr = hl.AddressFromKey(key)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := hl.NewGlobals(time.Now().UnixMilli())
	api := hl.NewAPI(hl.MainnetAPIURL)
	prices := hl.NewPriceState()

	if cfg.WalletAddr != "" {
		go hl.FetchAccountEquity(ctx, api, cfg.WalletAddr, g)
		go hl.FetchTradeHistory(ctx, api, cfg.WalletAddr, g)
	}

	eng, err := hl.NewShortGridEngine(ctx, cfg, g, api,
		hl.QUSDC, hl.SplitN, hl.MaxGridsDefault, hl.P1, hl.StepX)
	if err != nil {
		fmt.Printf("引擎初始化失败: %v\n", err)
		os.Exit(1)
	}

	hl.FetchInitialPrices(ctx, api, prices)

	ws := hl.NewMarketWS(prices, func(_ string, _ float64, _ float64) {
		if p1, p2, ok := prices.Fresh(); ok {
			eng.OnTick(hl.CalculateK(p1, p2), p1, p2)
		}
	})
	go ws.Run(ctx)

	time.Sleep(1500 * time.Millisecond)
	if !hl.PerformStartupConfirmationCheck(eng, prices) {
		fmt.Println("\n启动保护触发，程序退出。")
		os.Exit(1)
	}

	eng.StartWorker()
	startTime := time.Now()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			fmt.Println("\n正在停止交易工作线程...")
			eng.StopWorker()
			if hl.FlattenOnExit && eng.LiveTrading() {
				if p1, p2, ok := prices.Fresh(); ok {
					fmt.Println("FLATTEN_ON_EXIT=True，正在执行保护性平仓...")
					eng.FlattenAll(p1, p2, "程序退出")
				}
			}
			fmt.Println("已停止自动交易。")
			return
		case <-ticker.C:
			hl.CheckStopLoss(eng, prices, g, cfg.StopLossUSDC)
			hl.RenderUI(g.Snapshot(), prices, eng, ws.Connected(),
				int64(math.Round(time.Since(startTime).Seconds())))
		}
	}
}
