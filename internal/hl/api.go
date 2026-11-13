package hl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// API Hyperliquid REST 客户端（/info 与 /exchange）
type API struct {
	BaseURL string
	Client  *http.Client
}

// NewAPI 构造 REST 客户端。
func NewAPI(baseURL string) *API {
	return &API{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 15 * time.Second},
	}
}

// Post 向指定路径发 JSON 请求，返回动态解析后的结果（数字为 json.Number）。
func (a *API) Post(ctx context.Context, path string, payload any, timeout time.Duration) (any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := a.Client
	if timeout > 0 && timeout != a.Client.Timeout {
		c := *a.Client
		c.Timeout = timeout
		client = &c
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w (%s)", err, truncate(string(raw), 200))
	}
	return out, nil
}

// Info 调用 /info。
func (a *API) Info(ctx context.Context, payload map[string]any, timeout time.Duration) (any, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return a.Post(ctx, "/info", payload, timeout)
}

// ExchangePayload /exchange 请求体
type ExchangePayload struct {
	Action       any       `json:"action"`
	Nonce        int64     `json:"nonce"`
	Signature    Signature `json:"signature"`
	VaultAddress *string   `json:"vaultAddress"`
	ExpiresAfter *int64    `json:"expiresAfter"`
}

// Exchange 提交已签名的动作。
func (a *API) Exchange(ctx context.Context, payload ExchangePayload) (any, error) {
	return a.Post(ctx, "/exchange", payload, 15*time.Second)
}

// ---------------- 线上 JSON 结构 ----------------

type limitTifJSON struct {
	Tif string `json:"tif"`
}

type orderTypeJSON struct {
	Limit limitTifJSON `json:"limit"`
}

type orderWireJSON struct {
	A int           `json:"a"`
	B bool          `json:"b"`
	P string        `json:"p"`
	S string        `json:"s"`
	R bool          `json:"r"`
	T orderTypeJSON `json:"t"`
	C string        `json:"c,omitempty"`
}

type orderActionJSON struct {
	Type     string          `json:"type"`
	Orders   []orderWireJSON `json:"orders"`
	Grouping string          `json:"grouping"`
}

type updateLeverageJSON struct {
	Type     string `json:"type"`
	Asset    int    `json:"asset"`
	IsCross  bool   `json:"isCross"`
	Leverage int    `json:"leverage"`
}

func toOrderActionJSON(orders []OrderWire, grouping string) orderActionJSON {
	jsonOrders := make([]orderWireJSON, 0, len(orders))
	for _, o := range orders {
		jsonOrders = append(jsonOrders, orderWireJSON{
			A: o.Asset,
			B: o.IsBuy,
			P: o.Px,
			S: o.Sz,
			R: o.ReduceOnly,
			T: orderTypeJSON{Limit: limitTifJSON{Tif: "Ioc"}},
			C: o.Cloid,
		})
	}
	return orderActionJSON{Type: "order", Orders: jsonOrders, Grouping: grouping}
}

// ---------------- 动态 JSON 取值助手 ----------------

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func getStr(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	}
	return ""
}

func numToFloat(v any) float64 {
	switch t := v.(type) {
	case nil:
		return 0
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0
		}
		return f
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0
		}
		return f
	case float64:
		return t
	case int64:
		return float64(t)
	case bool:
		if t {
			return 1
		}
		return 0
	}
	return 0
}

func getFloat(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	return numToFloat(m[key])
}

func getInt(m map[string]any, key string) int64 {
	if m == nil {
		return 0
	}
	return int64(getFloat(m, key))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// BuildAssetMap 复刻 Python SDK 的币种 → asset id 映射：
// 主 dex 偏移 0，builder-deployed dex 偏移 110000 + i*10000（i 为 perpDexs 中非空项的序号）。
func BuildAssetMap(ctx context.Context, api *API) (map[string]int, map[int]int, error) {
	resp, err := api.Info(ctx, map[string]any{"type": "perpDexs"}, 10*time.Second)
	if err != nil {
		return nil, nil, fmt.Errorf("获取 perpDexs 失败: %w", err)
	}
	dexNames := make([]string, 0, 8)
	for _, item := range asSlice(resp) {
		m := asMap(item)
		if m == nil {
			continue
		}
		if name := getStr(m, "name"); name != "" {
			dexNames = append(dexNames, name)
		}
	}

	offsets := map[string]int{"": 0}
	for i, name := range dexNames {
		offsets[name] = 110000 + i*10000
	}

	coinToAsset := make(map[string]int, 256)
	assetToSz := make(map[int]int, 256)

	loadMeta := func(dex string, offset int) error {
		payload := map[string]any{"type": "meta"}
		if dex != "" {
			payload["dex"] = dex
		}
		meta, err := api.Info(ctx, payload, 10*time.Second)
		if err != nil {
			return fmt.Errorf("获取 meta(%q) 失败: %w", dex, err)
		}
		universe := asSlice(asMap(meta)["universe"])
		for idx, item := range universe {
			um := asMap(item)
			if um == nil {
				continue
			}
			asset := offset + idx
			name := getStr(um, "name")
			if name == "" {
				continue
			}
			coinToAsset[name] = asset
			assetToSz[asset] = int(getInt(um, "szDecimals"))
		}
		return nil
	}

	if err := loadMeta("", 0); err != nil {
		return nil, nil, err
	}
	for name, offset := range offsets {
		if name == "" {
			continue
		}
		if err := loadMeta(name, offset); err != nil {
			return nil, nil, err
		}
	}
	return coinToAsset, assetToSz, nil
}
