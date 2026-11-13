package hl

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	// OrderRetrySleep 下单重试间隔（固定，不参与热加载）
	OrderRetrySleep = 250 * time.Millisecond
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

// Env 运行期环境变量（密钥/日志/配置路径），不参与热加载。
type Env struct {
	SecretKey    string
	WalletAddr   string
	LogPath      string
	ConfigPath   string
	StopLossUSDC *float64 // HL_STOP_LOSS_USDC 设置时覆盖配置文件
}

// LoadEnv 读取环境变量；策略参数不在这里（见 Config / hype.toml）。
func LoadEnv() Env {
	env := Env{
		SecretKey:  strings.TrimSpace(os.Getenv("HL_SECRET_KEY")),
		WalletAddr: strings.TrimSpace(os.Getenv("HL_WALLET_ADDR")),
		LogPath:    os.Getenv("HL_LOG_PATH"),
		ConfigPath: strings.TrimSpace(os.Getenv("HL_CONFIG")),
	}
	if env.LogPath == "" {
		env.LogPath = "hype_strategy.log"
	}
	if env.ConfigPath == "" {
		env.ConfigPath = "hype.toml"
	}
	if raw := strings.TrimSpace(os.Getenv("HL_STOP_LOSS_USDC")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			env.StopLossUSDC = &v
		}
	}
	return env
}

// StrategyConfig 网格策略参数（热加载生效）
type StrategyConfig struct {
	QUSDC      float64 `toml:"q_usdc"`
	SplitN     int     `toml:"split_n"`
	MaxGrids   int     `toml:"max_grids"`
	P1         float64 `toml:"p1"`
	StepX      float64 `toml:"step_x"`
	SliceDelay float64 `toml:"slice_delay"`
	Leverage   float64 `toml:"leverage"`
	EMAAlpha   float64 `toml:"ema_alpha"`
}

// ExecutionConfig 下单与行情执行参数（热加载生效）
type ExecutionConfig struct {
	PairSlippagePct        float64 `toml:"pair_slippage_pct"`
	PriceMaxAge            float64 `toml:"price_max_age"`
	HedgeImbalanceTol      float64 `toml:"hedge_imbalance_tolerance"`
	MarginBufferMultiplier float64 `toml:"margin_buffer_multiplier"`
	FillWait               float64 `toml:"fill_wait"`
	OrderRetries           int     `toml:"order_retries"`
	MaxActionsPerTick      int     `toml:"max_actions_per_tick"`
}

// RiskConfig 风控参数（热加载生效）
type RiskConfig struct {
	StopLossUSDC              float64 `toml:"stop_loss_usdc"`
	FlattenOnExit             bool    `toml:"flatten_on_exit"`
	AutoFlattenOnError        bool    `toml:"auto_flatten_on_error"`
	AutoFlattenOnReconcileErr bool    `toml:"auto_flatten_on_reconcile_error"`
}

// ExchangeConfig 交易所侧行为（仅启动时读取）
type ExchangeConfig struct {
	EnforceLeverage bool `toml:"enforce_leverage"`
}

// Config 完整配置（对应 hype.toml）
type Config struct {
	Strategy  StrategyConfig  `toml:"strategy"`
	Execution ExecutionConfig `toml:"execution"`
	Risk      RiskConfig      `toml:"risk"`
	Exchange  ExchangeConfig  `toml:"exchange"`
}

// DefaultConfig 内置默认值 = Python v3.9 原始参数
func DefaultConfig() Config {
	return Config{
		Strategy: StrategyConfig{
			QUSDC: 1500, SplitN: 1, MaxGrids: 14,
			P1: 1.45, StepX: 0.003,
			SliceDelay: 0.75, Leverage: 5, EMAAlpha: 0.25,
		},
		Execution: ExecutionConfig{
			PairSlippagePct: 0.02, PriceMaxAge: 5.0,
			HedgeImbalanceTol: 0.15, MarginBufferMultiplier: 1.5,
			FillWait: 1.0, OrderRetries: 3, MaxActionsPerTick: 20,
		},
		Risk: RiskConfig{
			StopLossUSDC: 0, FlattenOnExit: false,
			AutoFlattenOnError: true, AutoFlattenOnReconcileErr: false,
		},
		Exchange: ExchangeConfig{EnforceLeverage: true},
	}
}

// ParseConfig 解析 TOML（缺省字段沿用默认值；未知字段报错并指出名称，避免拼写错误静默失效）。
func ParseConfig(data []byte) (Config, error) {
	if err := checkUnknownKeys(data); err != nil {
		return Config{}, err
	}
	cfg := DefaultConfig()
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("解析 TOML 失败: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// knownKeys 通过 toml tag 反射出某结构体允许的字段名。
func knownKeys(t reflect.Type) map[string]bool {
	out := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		tag := strings.Split(t.Field(i).Tag.Get("toml"), ",")[0]
		if tag != "" && tag != "-" {
			out[tag] = true
		}
	}
	return out
}

// checkUnknownKeys 找出配置文件中未知的键，并给出「段.键」形式的提示。
func checkUnknownKeys(data []byte) error {
	var root map[string]any
	if err := toml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("解析 TOML 失败: %w", err)
	}

	sections := map[string]reflect.Type{
		"strategy":  reflect.TypeOf(StrategyConfig{}),
		"execution": reflect.TypeOf(ExecutionConfig{}),
		"risk":      reflect.TypeOf(RiskConfig{}),
		"exchange":  reflect.TypeOf(ExchangeConfig{}),
	}

	var unknown []string
	for key, val := range root {
		sectionType, ok := sections[key]
		if !ok {
			unknown = append(unknown, key)
			continue
		}
		sub, ok := val.(map[string]any)
		if !ok {
			continue
		}
		allowed := knownKeys(sectionType)
		for subKey := range sub {
			if !allowed[subKey] {
				unknown = append(unknown, key+"."+subKey)
			}
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("未知配置项: %s（请检查拼写）", strings.Join(unknown, ", "))
	}
	return nil
}

// Validate 参数合法性校验。
func (c Config) Validate() error {
	s := c.Strategy
	switch {
	case s.QUSDC <= 0:
		return fmt.Errorf("strategy.q_usdc 必须 > 0，当前 %v", s.QUSDC)
	case s.SplitN < 1:
		return fmt.Errorf("strategy.split_n 必须 >= 1，当前 %d", s.SplitN)
	case s.MaxGrids < 0:
		return fmt.Errorf("strategy.max_grids 不能为负，当前 %d", s.MaxGrids)
	case s.P1 <= 0:
		return fmt.Errorf("strategy.p1 必须 > 0，当前 %v", s.P1)
	case s.StepX <= 0:
		return fmt.Errorf("strategy.step_x 必须 > 0，当前 %v", s.StepX)
	case s.SliceDelay < 0:
		return fmt.Errorf("strategy.slice_delay 不能为负，当前 %v", s.SliceDelay)
	case s.Leverage < 1 || s.Leverage > 10:
		return fmt.Errorf("strategy.leverage 必须在 1~10 之间（两币上限 10x），当前 %v", s.Leverage)
	case s.EMAAlpha <= 0 || s.EMAAlpha > 1:
		return fmt.Errorf("strategy.ema_alpha 必须在 (0,1] 之间，当前 %v", s.EMAAlpha)
	}

	e := c.Execution
	switch {
	case e.PairSlippagePct < 0 || e.PairSlippagePct > 1:
		return fmt.Errorf("execution.pair_slippage_pct 必须在 [0,1] 之间，当前 %v", e.PairSlippagePct)
	case e.PriceMaxAge <= 0:
		return fmt.Errorf("execution.price_max_age 必须 > 0，当前 %v", e.PriceMaxAge)
	case e.HedgeImbalanceTol <= 0 || e.HedgeImbalanceTol >= 1:
		return fmt.Errorf("execution.hedge_imbalance_tolerance 必须在 (0,1) 之间，当前 %v", e.HedgeImbalanceTol)
	case e.MarginBufferMultiplier < 1:
		return fmt.Errorf("execution.margin_buffer_multiplier 必须 >= 1，当前 %v", e.MarginBufferMultiplier)
	case e.FillWait < 0:
		return fmt.Errorf("execution.fill_wait 不能为负，当前 %v", e.FillWait)
	case e.OrderRetries < 1:
		return fmt.Errorf("execution.order_retries 必须 >= 1，当前 %d", e.OrderRetries)
	case e.MaxActionsPerTick < 1:
		return fmt.Errorf("execution.max_actions_per_tick 必须 >= 1，当前 %d", e.MaxActionsPerTick)
	}

	if c.Risk.StopLossUSDC < 0 {
		return fmt.Errorf("risk.stop_loss_usdc 不能为负，当前 %v", c.Risk.StopLossUSDC)
	}
	return nil
}

// Diff 返回与旧配置的差异描述（用于热加载日志）。
func (c Config) Diff(old Config) []string {
	var out []string
	os := old.Strategy
	ns := c.Strategy
	addF := func(name string, oldV, newV float64) {
		if oldV != newV {
			out = append(out, fmt.Sprintf("%s %v→%v", name, oldV, newV))
		}
	}
	addI := func(name string, oldV, newV int) {
		if oldV != newV {
			out = append(out, fmt.Sprintf("%s %d→%d", name, oldV, newV))
		}
	}
	addB := func(name string, oldV, newV bool) {
		if oldV != newV {
			out = append(out, fmt.Sprintf("%s %v→%v", name, oldV, newV))
		}
	}

	addF("q_usdc", os.QUSDC, ns.QUSDC)
	addI("split_n", os.SplitN, ns.SplitN)
	addI("max_grids", os.MaxGrids, ns.MaxGrids)
	addF("p1", os.P1, ns.P1)
	addF("step_x", os.StepX, ns.StepX)
	addF("slice_delay", os.SliceDelay, ns.SliceDelay)
	addF("leverage", os.Leverage, ns.Leverage)
	addF("ema_alpha", os.EMAAlpha, ns.EMAAlpha)

	oe, ne := old.Execution, c.Execution
	addF("pair_slippage_pct", oe.PairSlippagePct, ne.PairSlippagePct)
	addF("price_max_age", oe.PriceMaxAge, ne.PriceMaxAge)
	addF("hedge_imbalance_tolerance", oe.HedgeImbalanceTol, ne.HedgeImbalanceTol)
	addF("margin_buffer_multiplier", oe.MarginBufferMultiplier, ne.MarginBufferMultiplier)
	addF("fill_wait", oe.FillWait, ne.FillWait)
	addI("order_retries", oe.OrderRetries, ne.OrderRetries)
	addI("max_actions_per_tick", oe.MaxActionsPerTick, ne.MaxActionsPerTick)

	addF("stop_loss_usdc", old.Risk.StopLossUSDC, c.Risk.StopLossUSDC)
	addB("flatten_on_exit", old.Risk.FlattenOnExit, c.Risk.FlattenOnExit)
	addB("auto_flatten_on_error", old.Risk.AutoFlattenOnError, c.Risk.AutoFlattenOnError)
	addB("auto_flatten_on_reconcile_error", old.Risk.AutoFlattenOnReconcileErr, c.Risk.AutoFlattenOnReconcileErr)
	addB("enforce_leverage", old.Exchange.EnforceLeverage, c.Exchange.EnforceLeverage)
	return out
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
