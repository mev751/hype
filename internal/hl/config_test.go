package hl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte("[strategy]\nq_usdc = 800\n"))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if cfg.Strategy.QUSDC != 800 {
		t.Fatalf("q_usdc 未生效: %v", cfg.Strategy.QUSDC)
	}
	// 未写的字段保留默认值
	if cfg.Strategy.P1 != 1.45 || cfg.Strategy.StepX != 0.003 {
		t.Fatalf("默认值未保留: %+v", cfg.Strategy)
	}
	if cfg.Execution.PriceMaxAge != 5.0 || !cfg.Exchange.EnforceLeverage {
		t.Fatalf("execution/exchange 默认值异常: %+v %+v", cfg.Execution, cfg.Exchange)
	}
}

func TestParseConfigRejectsUnknownField(t *testing.T) {
	_, err := ParseConfig([]byte("[strategy]\nq_uds = 1\n"))
	if err == nil {
		t.Fatal("拼写错误的字段应报错，而不是静默忽略")
	}
	if !strings.Contains(err.Error(), "q_uds") {
		t.Fatalf("错误信息应指出字段名: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"split_n=0", "[strategy]\nsplit_n = 0\n"},
		{"step_x=0", "[strategy]\nstep_x = 0\n"},
		{"leverage=20", "[strategy]\nleverage = 20\n"},
		{"ema_alpha=0", "[strategy]\nema_alpha = 0\n"},
		{"price_max_age=0", "[execution]\nprice_max_age = 0\n"},
		{"stop_loss<0", "[risk]\nstop_loss_usdc = -1\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(tc.body)); err == nil {
				t.Fatalf("应校验失败: %s", tc.body)
			}
		})
	}
}

func TestConfigDiff(t *testing.T) {
	old := DefaultConfig()
	cfg := DefaultConfig()
	cfg.Strategy.P1 = 1.46
	cfg.Execution.PriceMaxAge = 8
	diff := cfg.Diff(old)
	joined := strings.Join(diff, ",")
	if !strings.Contains(joined, "p1 1.45→1.46") || !strings.Contains(joined, "price_max_age 5→8") {
		t.Fatalf("diff 内容不符: %v", diff)
	}
}

func TestConfigStoreHotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hype.toml")
	if err := os.WriteFile(path, []byte("[strategy]\nq_usdc = 1000\nleverage = 5\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewConfigStore(path)
	if err != nil {
		t.Fatalf("加载失败: %v", err)
	}
	if got := store.Current().Strategy.QUSDC; got != 1000 {
		t.Fatalf("初始配置不符: %v", got)
	}

	var applied []Config
	store.OnChange(func(c Config) { applied = append(applied, c) })

	// 写入新配置（模拟用户保存文件）
	if err := os.WriteFile(path, []byte("[strategy]\nq_usdc = 1200\nleverage = 6\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 保证 mtime 变化（部分文件系统时间粒度较粗）
	future := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(path, future, future)

	changed, err := store.ReloadNow()
	if err != nil {
		t.Fatalf("热加载失败: %v", err)
	}
	if !changed {
		t.Fatal("应检测到变更")
	}
	// ReloadNow 不触发回调（回调由 Watch 执行），这里直接校验存储值
	cfg := store.Current()
	if cfg.Strategy.QUSDC != 1200 || cfg.Strategy.Leverage != 6 {
		t.Fatalf("新配置未生效: %+v", cfg.Strategy)
	}

	// 写入非法配置：应报错且保留旧值
	if err := os.WriteFile(path, []byte("[strategy]\nstep_x = 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	future = future.Add(2 * time.Second)
	_ = os.Chtimes(path, future, future)
	if _, err := store.ReloadNow(); err == nil {
		t.Fatal("非法配置应报错")
	}
	if got := store.Current().Strategy.StepX; got != 0.003 {
		t.Fatalf("非法配置不应覆盖旧值，实际 step_x=%v", got)
	}
}
