// Package book 提供归一化盘口的双缓冲缓存。
//
// 写侧只有 feed 单协程（Publish），读侧是策略循环（Get），
// 用 RWMutex 保护 map 指针的替换；Snapshot 内嵌的切片为只读按值拷贝的视图，
// 发布后不再被修改（feed 侧每次构造新切片）。
package book

import (
	"strings"
	"sync"
	"time"

	"github.com/mev751/hype/internal/hft/venue"
)

// Cache 盘口缓存
type Cache struct {
	mu   sync.RWMutex
	data map[string]venue.Snapshot
}

// New 构造缓存
func New() *Cache {
	return &Cache{data: make(map[string]venue.Snapshot, 16)}
}

func key(v, symbol string) string {
	var b strings.Builder
	b.Grow(len(v) + 1 + len(symbol))
	b.WriteString(v)
	b.WriteByte('|')
	b.WriteString(symbol)
	return b.String()
}

// Publish 发布一次快照（写侧唯一入口，单协程调用）。
func (c *Cache) Publish(s venue.Snapshot) {
	k := key(s.Venue, s.Symbol)
	c.mu.Lock()
	c.data[k] = s
	c.mu.Unlock()
}

// Get 读取快照（读侧可并发）。
func (c *Cache) Get(v, symbol string) (venue.Snapshot, bool) {
	c.mu.RLock()
	s, ok := c.data[key(v, symbol)]
	c.mu.RUnlock()
	return s, ok
}

// Stale 返回超过 maxAge 未更新的标的（"venue|symbol" 列表）。
func (c *Cache) Stale(now time.Time, maxAge time.Duration) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for k, s := range c.data {
		if now.Sub(s.RecvTS) > maxAge {
			out = append(out, k)
		}
	}
	return out
}

// Size 当前缓存标的数
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}
