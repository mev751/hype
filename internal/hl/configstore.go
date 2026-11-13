package hl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConfigStore 持有 TOML 配置并监视文件变化，实现热加载。
type ConfigStore struct {
	path string

	mu        sync.RWMutex
	cfg       Config
	exists    bool
	modTime   time.Time
	size      int64
	listeners []func(Config)
	lastDiff  []string

	// 失败节流：同一坏版本（mtime+size 相同）只报一次错，避免刷屏
	failMod  time.Time
	failSize int64
}

// NewConfigStore 加载配置；文件不存在时使用内置默认值，文件存在但非法则返回错误（启动即失败）。
func NewConfigStore(path string) (*ConfigStore, error) {
	s := &ConfigStore{path: path, cfg: DefaultConfig()}
	if err := s.loadInitial(); err != nil {
		return nil, err
	}
	return s, nil
}

// Path 配置文件路径。
func (s *ConfigStore) Path() string { return s.path }

// Exists 配置文件是否存在。
func (s *ConfigStore) Exists() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exists
}

func (s *ConfigStore) loadInitial() error {
	st, err := os.Stat(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	cfg, err := ParseConfig(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", s.path, err)
	}
	s.mu.Lock()
	s.cfg = cfg
	s.exists = true
	s.modTime = st.ModTime()
	s.size = st.Size()
	s.mu.Unlock()
	return nil
}

// Current 当前生效配置（快照拷贝）。
func (s *ConfigStore) Current() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// OnChange 注册配置变更回调（回调按注册顺序执行）。
func (s *ConfigStore) OnChange(fn func(Config)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// Watch 每秒轮询文件 mtime/size，变化时重新解析并广播；解析失败保留旧配置。
func (s *ConfigStore) Watch(ctx context.Context, notify func(string)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := s.reloadIfChanged()
			if err != nil {
				msg := "[配置] ❌ 热加载失败，继续使用旧配置: " + err.Error()
				logErrorf("%s", msg)
				if notify != nil {
					notify(msg)
				}
				continue
			}
			if !changed {
				continue
			}
			cfg := s.Current()
			s.mu.RLock()
			listeners := append([]func(Config){}, s.listeners...)
			diff := append([]string{}, s.lastDiff...)
			s.mu.RUnlock()

			for _, fn := range listeners {
				fn(cfg)
			}

			msg := "[配置] ✅ 已热加载 " + filepath.Base(s.path)
			if len(diff) > 0 {
				msg += "：" + strings.Join(diff, ", ")
			} else {
				msg += "（无非参数变化）"
			}
			logInfo("%s", msg)
			if notify != nil {
				notify(msg)
			}
		}
	}
}

// ReloadNow 立即重载一次（测试与手动触发用）。
func (s *ConfigStore) ReloadNow() (bool, error) { return s.reloadIfChanged() }

func (s *ConfigStore) reloadIfChanged() (bool, error) {
	st, err := os.Stat(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	s.mu.RLock()
	same := s.exists && s.modTime.Equal(st.ModTime()) && s.size == st.Size()
	failedSame := s.failMod.Equal(st.ModTime()) && s.failSize == st.Size()
	s.mu.RUnlock()
	if same || failedSame {
		return false, nil
	}

	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, err
	}
	cfg, err := ParseConfig(raw)
	if err != nil {
		s.mu.Lock()
		s.failMod = st.ModTime()
		s.failSize = st.Size()
		s.mu.Unlock()
		return false, err
	}

	s.mu.Lock()
	diff := cfg.Diff(s.cfg)
	s.cfg = cfg
	s.exists = true
	s.modTime = st.ModTime()
	s.size = st.Size()
	s.failMod = time.Time{}
	s.failSize = 0
	s.lastDiff = diff
	s.mu.Unlock()

	return len(diff) > 0, nil
}

// secondsToDuration 秒（浮点）转 Duration。
func secondsToDuration(v float64) time.Duration {
	return time.Duration(v * float64(time.Second))
}
