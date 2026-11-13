// Command hlstrat 是 HL_SKHY_STRAT_3.9.py 的 Go 版本：
// Hyperliquid SKHY/SKHX 相对价值做空网格机器人。
//
// 策略参数全部来自 TOML 配置文件（默认 ./hype.toml），保存即热加载；
// 密钥等敏感项仍走环境变量：
//
//	HL_SECRET_KEY      私钥（不填则为模拟模式）
//	HL_WALLET_ADDR     钱包地址（不填则由私钥推导）
//	HL_LOG_PATH        日志路径（默认 hype_strategy.log）
//	HL_CONFIG          配置文件路径（默认 hype.toml，可用 -config 覆盖）
//	HL_STOP_LOSS_USDC  浮动亏损紧急停止阈值（设置后覆盖配置文件，0 关闭）
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mev751/hype/internal/hl"
)

func main() {
	configFlag := flag.String("config", "", "TOML 配置路径（默认 ./hype.toml，或环境变量 HL_CONFIG）")
	flag.Parse()

	env := hl.LoadEnv()
	if *configFlag != "" {
		env.ConfigPath = *configFlag
	}

	store, err := hl.NewConfigStore(env.ConfigPath)
	if err != nil {
		fmt.Printf("配置文件错误（%s）: %v\n", env.ConfigPath, err)
		os.Exit(1)
	}
	if f := hl.SetupLogging(env.LogPath); f != nil {
		defer f.Close()
	}

	fmt.Print("\x1b[2J\x1b[H")
	fmt.Println("正在启动 SKHY/SKHX 相对价值网格机器人 v3.9 (Go)...")
	if store.Exists() {
		fmt.Printf("已加载配置: %s\n", store.Path())
	} else {
		fmt.Printf("未找到 %s，暂用内置默认参数（创建该文件后会自动热加载）\n", store.Path())
	}
	if env.StopLossUSDC != nil {
		fmt.Printf("注意: HL_STOP_LOSS_USDC=%v 将覆盖配置文件中的 risk.stop_loss_usdc\n", *env.StopLossUSDC)
	}

	if env.SecretKey != "" && env.WalletAddr == "" {
		key, err := hl.LoadPrivateKey(env.SecretKey)
		if err != nil {
			fmt.Printf("无法从 HL_SECRET_KEY 推导钱包地址: %v\n", err)
			os.Exit(1)
		}
		env.WalletAddr = hl.AddressFromKey(key)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := hl.NewGlobals(time.Now().UnixMilli())
	api := hl.NewAPI(hl.MainnetAPIURL)
	prices := hl.NewPriceState()
	prices.SetMaxAge(time.Duration(store.Current().Execution.PriceMaxAge * float64(time.Second)))

	if env.WalletAddr != "" {
		go hl.FetchAccountEquity(ctx, api, env.WalletAddr, g)
		go hl.FetchTradeHistory(ctx, api, env.WalletAddr, g)
	}

	eng, err := hl.NewShortGridEngine(ctx, env, store, g, api)
	if err != nil {
		fmt.Printf("引擎初始化失败: %v\n", err)
		os.Exit(1)
	}

	// 热加载：新配置同时推给引擎与行情层
	store.OnChange(func(cfg hl.Config) {
		eng.ApplyConfig(cfg)
		prices.SetMaxAge(time.Duration(cfg.Execution.PriceMaxAge * float64(time.Second)))
	})
	go store.Watch(ctx, func(msg string) {
		g.AddLog(msg)
	})

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
			if store.Current().Risk.FlattenOnExit && eng.LiveTrading() {
				if p1, p2, ok := prices.Fresh(); ok {
					fmt.Println("flatten_on_exit=true，正在执行保护性平仓...")
					eng.FlattenAll(p1, p2, "程序退出")
				}
			}
			fmt.Println("已停止自动交易。")
			return
		case <-ticker.C:
			cfgNow := store.Current()
			stopLoss := cfgNow.Risk.StopLossUSDC
			if env.StopLossUSDC != nil {
				stopLoss = *env.StopLossUSDC
			}
			hl.CheckStopLoss(eng, prices, g, stopLoss)
			hl.RenderUI(g.Snapshot(), prices, eng, ws.Connected(),
				int64(math.Round(time.Since(startTime).Seconds())))
		}
	}
}
