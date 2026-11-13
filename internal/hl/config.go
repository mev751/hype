package hl

import (
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// ==========================================
// ⚙️ 交易策略与 API 参数配置区（与 Python v3.9 对齐）
// ==========================================

const (
	QUSDC  = 1500.0 // 每层每条腿的目标名义价值 (USDC)
	SplitN = 1      // 单次网格订单切分次数，必须 >= 1
	// MaxGridsDefault 最大建仓层数
	MaxGridsDefault = 14
	P1              = 1.45  // 建仓触发基准线
	StepX           = 0.003 // 网格步长
	Leverage        = 5     // 期望杠杆
	EMAAlpha        = 0.25  // K 值 EMA 平滑系数

	// SliceDelay 切片间隔
	SliceDelay = 750 * time.Millisecond

	// PriceMaxAge 任一腿盘口超过该时长即禁止交易
	PriceMaxAge     = 5 * time.Second
	PairSlippagePct = 0.02

	OrderRetries    = 3
	OrderRetrySleep = 250 * time.Millisecond
	FillWait        = 1 * time.Second

	HedgeImbalanceTolerance = 0.15
	MarginBufferMultiplier  = 1.5

	AutoFlattenOnError          = true
	AutoFlattenOnReconcileError = false
	FlattenOnExit               = false
	EnforceExchangeLeverage     = true

	MaxActionsPerTick = 20
)

const (
	// CoinSKHY 空头腿（ADR）
	CoinSKHY = "xyz:SKHY"
	// CoinSKHX 多头腿（韩股本尊，显示为 SKHYNIX）
	CoinSKHX = "xyz:SKHX"

	// MainnetAPIURL Hyperliquid 主网
	MainnetAPIURL = "https://api.hyperliquid.xyz"

	SymSKHY = "SKHY-USDC"
	SymSKHX = "SKHYNIX-USDC"
)

// CoinToDisplay 链上币名 → 终端显示名
var CoinToDisplay = map[string]string{
	CoinSKHY: SymSKHY,
	CoinSKHX: SymSKHX,
}

// Config 运行期配置（环境变量注入）
type Config struct {
	SecretKey    string
	WalletAddr   string
	LogPath      string
	StopLossUSDC float64
}

// LoadConfig 读取环境变量，行为与 Python 版一致。
func LoadConfig() Config {
	cfg := Config{
		SecretKey:  strings.TrimSpace(os.Getenv("HL_SECRET_KEY")),
		WalletAddr: strings.TrimSpace(os.Getenv("HL_WALLET_ADDR")),
		LogPath:    os.Getenv("HL_LOG_PATH"),
	}
	if cfg.LogPath == "" {
		cfg.LogPath = "hype_strategy.log"
	}
	raw := strings.TrimSpace(os.Getenv("HL_STOP_LOSS_USDC"))
	if raw != "" && raw != "0" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			cfg.StopLossUSDC = v
		}
	}
	return cfg
}

// RoundSize Python round(size, decimals) 的等价实现（四舍五入到小数位）。
func RoundSize(size float64, decimals int) float64 {
	scale := math.Pow10(decimals)
	return math.Round(size*scale) / scale
}

// MakeLimitPrice 生成 IOC 保护限价：最多 5 位有效数字。
func MakeLimitPrice(price float64, isBuy bool, slippage float64) float64 {
	factor := 1.0 + slippage
	if !isBuy {
		factor = 1.0 - slippage
	}
	// %.5g 等价：strconv 'g' + 精度 5
	s := strconv.FormatFloat(price*factor, 'g', 5, 64)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return price * factor
	}
	return v
}

// FloatToWire Hyperliquid 线上数值格式：最多 8 位小数，去掉尾随 0。
func FloatToWire(x float64) string {
	s := strconv.FormatFloat(x, 'f', 8, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	if s == "" || s == "-0" {
		s = "0"
	}
	return s
}

// Epsilon 返回 10^-decimals，用于判断仓位是否可忽略。
func Epsilon(decimals int) float64 {
	return math.Pow10(-decimals)
}
