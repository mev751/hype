package hl

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
)

// 测试私钥：Hardhat 默认账户 #1，仅用于离线签名向量校验，切勿用于实盘。
const testKeyHex = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"

func hexToBig(t *testing.T, s string) *big.Int {
	t.Helper()
	v, ok := new(big.Int).SetString(strings.TrimPrefix(s, "0x"), 16)
	if !ok {
		t.Fatalf("非法十六进制: %s", s)
	}
	return v
}

// 向量由 hyperliquid-python-sdk 0.24.0 生成，确保 msgpack + EIP-712 + secp256k1 逐字节一致。
func TestSignL1ActionVectors(t *testing.T) {
	key, err := LoadPrivateKey(testKeyHex)
	if err != nil {
		t.Fatalf("加载私钥失败: %v", err)
	}
	if addr := AddressFromKey(key); addr != "0x70997970c51812dc3a010c7d01b50e0d17dc79c8" {
		t.Fatalf("地址推导不符: %s", addr)
	}

	cases := []struct {
		name  string
		enc   func() []byte
		nonce int64
		hash  string
		r     string
		s     string
		v     int
	}{
		{
			name: "update_leverage",
			enc: func() []byte {
				return EncodeUpdateLeverageAction(110020, true, 5)
			},
			nonce: 1700000000000,
			hash:  "0x4bfb8652f28d46b80a19ddc56271bd7a3e6a53dd2b102051d3dad84c53a44fee",
			r:     "0x58533347b60885ee55f23bf26ac5720516073b3d02f8366cf826e5e0e1c3530",
			s:     "0x231fa5d1af5712d041aa7135f9ec4b9ebdd1ba519d1dd85b765c8c50726725c9",
			v:     27,
		},
		{
			name: "order_cloid",
			enc: func() []byte {
				return EncodeOrderAction([]OrderWire{{
					Asset: 110020, IsBuy: true, Px: "918.1", Sz: "1.6337", ReduceOnly: false,
					Cloid: "0x0123456789abcdef0123456789abcdef",
				}}, "na")
			},
			nonce: 1700000000123,
			hash:  "0xd3c0344609355d5c1e2df48579253d00f5fe8f1841ec24a9be56f7de5057c4ac",
			r:     "0x3364983ba37cacd67a2f03350822a0e5d2a0b26f256243db0e94dd7753649de0",
			s:     "0x473785a62ec1f0f609129f3b83a465dcebd64ba556b12216a43035b8d9009d68",
			v:     28,
		},
		{
			name: "order_no_cloid",
			enc: func() []byte {
				return EncodeOrderAction([]OrderWire{{
					Asset: 110005, IsBuy: false, Px: "134.99", Sz: "11.1123", ReduceOnly: true,
				}}, "na")
			},
			nonce: 1700000000456,
			hash:  "0x2a826a857a4e4ffa65de9f3b93f24cd6e3e768b8b3a04f6b8df722291a1cc89a",
			r:     "0x9dbfee632503211c148a64c6ebab6ddb2b76bfb358baaa62ddcded090ad4cd44",
			s:     "0x6a6b76dc842f5efeb616b8c4006124575775063792d1cbe752f82b6777024f33",
			v:     28,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig, err := SignL1Action(key, tc.enc(), tc.nonce)
			if err != nil {
				t.Fatalf("签名失败: %v", err)
			}
			actionHash := ActionHash(tc.enc(), tc.nonce)
			if got := "0x" + hex.EncodeToString(actionHash[:]); got != tc.hash {
				t.Fatalf("action hash 不符:\n got %s\nwant %s", got, tc.hash)
			}
			if hexToBig(t, sig.R).Cmp(hexToBig(t, tc.r)) != 0 {
				t.Fatalf("r 不符:\n got %s\nwant %s", sig.R, tc.r)
			}
			if hexToBig(t, sig.S).Cmp(hexToBig(t, tc.s)) != 0 {
				t.Fatalf("s 不符:\n got %s\nwant %s", sig.S, tc.s)
			}
			if sig.V != tc.v {
				t.Fatalf("v 不符: got %d want %d", sig.V, tc.v)
			}
		})
	}
}

func TestFloatToWire(t *testing.T) {
	cases := map[float64]string{
		918.1:       "918.1",
		134.99:      "134.99",
		1.4723:      "1.4723",
		1.450000:    "1.45",
		100:         "100",
		0.0001:      "0.0001",
		1.6337000000: "1.6337",
	}
	for in, want := range cases {
		if got := FloatToWire(in); got != want {
			t.Errorf("FloatToWire(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestLimitPrice(t *testing.T) {
	// 918.1 * 1.02 = 936.462 → 5 位有效数字 936.46
	if got := MakeLimitPrice(918.1, true, 0.02); got != 936.46 {
		t.Fatalf("MakeLimitPrice(buy) = %v, want 936.46", got)
	}
	// 134.99 * 0.98 = 132.2902 → 5 位有效数字 132.29
	if got := MakeLimitPrice(134.99, false, 0.02); got != 132.29 {
		t.Fatalf("MakeLimitPrice(sell) = %v, want 132.29", got)
	}
}
