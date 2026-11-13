package hl

import (
	"fmt"
	"time"
)

// calculateK 价差信号：K = SKHY买一 × 10 / SKHX卖一
func calculateK(bidHy, askHx float64) float64 {
	if askHx == 0 {
		return 0
	}
	return bidHy * 10.0 / askHx
}

// CalculateK 对外暴露的 K 计算。
func CalculateK(bidHy, askHx float64) float64 { return calculateK(bidHy, askHx) }

// nowHM 当前时分秒，用于日志前缀。
func nowHM() string { return time.Now().Format("15:04:05") }

// formatNow 终端 UI 时间戳（与 Python strftime("%Y-%m-%d %H:%M:%S") 一致）
func formatNow() string { return time.Now().Format("2006-01-02 15:04:05") }

// FormatRuntime 运行时长格式化（与 Python 一致）。
func FormatRuntime(totalSeconds int64) string {
	days := totalSeconds / 86400
	rem := totalSeconds % 86400
	hours := rem / 3600
	rem %= 3600
	minutes := rem / 60
	secs := rem % 60
	return fmt.Sprintf("%d天 %02d时 %02d分 %02d秒", days, hours, minutes, secs)
}
