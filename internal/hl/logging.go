package hl

import (
	"log"
	"os"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

// SetupLogging 初始化文件日志；失败时回退到 stderr（与 Python 行为一致）。
func SetupLogging(path string) *os.File {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
		return nil
	}
	logger = log.New(f, "", log.LstdFlags)
	return f
}

func logInfo(format string, args ...any) {
	logger.Printf("[INFO] "+format, args...)
}

func logErrorf(format string, args ...any) {
	logger.Printf("[ERROR] "+format, args...)
}

// LogError 记录错误到文件日志，并写入终端日志环。
func LogError(g *Globals, contextMsg string, err error) {
	msg := "[错误] " + contextMsg + ": " + err.Error()
	logger.Printf("[ERROR] %s", msg)
	if g != nil {
		g.AddLog(msg)
	}
}
