// Package obs 提供每跳时间戳、指标接口与行情录制/回放契约。
//
// 架构要求（见 docs/architecture.md §8）：没有这套埋点，延迟优化与事故复盘都无法进行。
package obs

// Stamp 单笔订单的每跳时间戳（monotonic 纳秒；0 表示该跳未发生）
type Stamp struct {
	RecvNS     int64 // WS 帧到达
	ParseNS    int64 // 解析完成
	DecisionNS int64 // 策略做出决策
	SignNS     int64 // 签名完成
	SendNS     int64 // HTTP 请求写出
	AckNS      int64 // 收到下单回报
	FillNS     int64 // 收到成交回报
}

// DecisionToSend 决策到发出的耗时（ns）
func (s Stamp) DecisionToSend() int64 {
	if s.DecisionNS == 0 || s.SendNS == 0 {
		return 0
	}
	return s.SendNS - s.DecisionNS
}

// SendToAck 发出到回报的耗时（ns）
func (s Stamp) SendToAck() int64 {
	if s.SendNS == 0 || s.AckNS == 0 {
		return 0
	}
	return s.AckNS - s.SendNS
}

// Frame 记录/回放的单帧
type Frame struct {
	VenueID uint8  // 0=hyperliquid, 1=binance
	RecvNS  int64  // 本地接收时间（monotonic）
	Payload []byte // 原始 WS 帧
}

// Recorder 原始帧记录器（分段落盘，由实现决定压缩与切分）
type Recorder interface {
	Write(f Frame) error
	Close() error
}

// Replayer 回放器
type Replayer interface {
	// Next 返回下一帧；ok=false 表示结束
	Next() (Frame, bool, error)
	Close() error
}

// Metrics 指标接口（Prometheus 适配在实现层完成，核心层只依赖此接口）
type Metrics interface {
	Inc(name string, labels map[string]string)
	Observe(name string, v float64, labels map[string]string)
	Gauge(name string, v float64, labels map[string]string)
}

// NopMetrics 空实现（测试/关闭指标时使用）
type NopMetrics struct{}

func (NopMetrics) Inc(string, map[string]string)              {}
func (NopMetrics) Observe(string, float64, map[string]string) {}
func (NopMetrics) Gauge(string, float64, map[string]string)   {}
