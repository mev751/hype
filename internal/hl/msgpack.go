package hl

// 与 hyperliquid-python-sdk 的 msgpack 编码逐字节一致的极小子集实现。
// 关键点：字段插入顺序必须与 Python dict 构造顺序完全一致，签名才能通过校验。

type mpWriter struct {
	b []byte
}

func (w *mpWriter) raw(p []byte) { w.b = append(w.b, p...) }

func (w *mpWriter) mapHeader(n int) {
	if n <= 15 {
		w.b = append(w.b, 0x80|byte(n))
		return
	}
	w.b = append(w.b, 0xde, byte(n>>8), byte(n))
}

func (w *mpWriter) arrayHeader(n int) {
	if n <= 15 {
		w.b = append(w.b, 0x90|byte(n))
		return
	}
	w.b = append(w.b, 0xdc, byte(n>>8), byte(n))
}

func (w *mpWriter) str(s string) {
	n := len(s)
	switch {
	case n <= 31:
		w.b = append(w.b, 0xa0|byte(n))
	case n <= 255:
		w.b = append(w.b, 0xd9, byte(n))
	case n <= 65535:
		w.b = append(w.b, 0xda, byte(n>>8), byte(n))
	default:
		w.b = append(w.b, 0xdb, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	w.b = append(w.b, s...)
}

func (w *mpWriter) boolean(v bool) {
	if v {
		w.b = append(w.b, 0xc3)
		return
	}
	w.b = append(w.b, 0xc2)
}

// integer 按 Python msgpack 的最小表示法编码。
func (w *mpWriter) integer(v int64) {
	switch {
	case v >= 0:
		switch {
		case v <= 127:
			w.b = append(w.b, byte(v))
		case v <= 255:
			w.b = append(w.b, 0xcc, byte(v))
		case v <= 65535:
			w.b = append(w.b, 0xcd, byte(v>>8), byte(v))
		case v <= 4294967295:
			w.b = append(w.b, 0xce, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		default:
			w.b = append(w.b, 0xcf, byte(uint64(v)>>56), byte(uint64(v)>>48), byte(uint64(v)>>40),
				byte(uint64(v)>>32), byte(uint64(v)>>24), byte(uint64(v)>>16), byte(uint64(v)>>8), byte(v))
		}
	default:
		switch {
		case v >= -32:
			w.b = append(w.b, byte(v))
		case v >= -128:
			w.b = append(w.b, 0xd0, byte(v))
		case v >= -32768:
			w.b = append(w.b, 0xd1, byte(v>>8), byte(v))
		case v >= -2147483648:
			w.b = append(w.b, 0xd2, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
		default:
			u := uint64(v)
			w.b = append(w.b, 0xd3, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
				byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
		}
	}
}

// OrderWire 对应 Python 的 OrderWire TypedDict。
type OrderWire struct {
	Asset      int
	IsBuy      bool
	Px         string // 已按 FloatToWire 规范化
	Sz         string
	ReduceOnly bool
	Cloid      string // 为空表示不带 c 字段
}

// EncodeOrderAction 编码 order 动作：
//
//	{"type":"order","orders":[{"a","b","p","s","r","t"[,"c"]}],"grouping":<grouping>}
func EncodeOrderAction(orders []OrderWire, grouping string) []byte {
	w := &mpWriter{}
	w.mapHeader(3)
	w.str("type")
	w.str("order")
	w.str("orders")
	w.arrayHeader(len(orders))
	for _, o := range orders {
		n := 6
		if o.Cloid != "" {
			n = 7
		}
		w.mapHeader(n)
		w.str("a")
		w.integer(int64(o.Asset))
		w.str("b")
		w.boolean(o.IsBuy)
		w.str("p")
		w.str(o.Px)
		w.str("s")
		w.str(o.Sz)
		w.str("r")
		w.boolean(o.ReduceOnly)
		w.str("t")
		w.mapHeader(1)
		w.str("limit")
		w.mapHeader(1)
		w.str("tif")
		w.str("Ioc")
		if o.Cloid != "" {
			w.str("c")
			w.str(o.Cloid)
		}
	}
	w.str("grouping")
	w.str(grouping)
	return w.b
}

// EncodeUpdateLeverageAction 编码 updateLeverage 动作。
func EncodeUpdateLeverageAction(asset int, isCross bool, leverage int) []byte {
	w := &mpWriter{}
	w.mapHeader(4)
	w.str("type")
	w.str("updateLeverage")
	w.str("asset")
	w.integer(int64(asset))
	w.str("isCross")
	w.boolean(isCross)
	w.str("leverage")
	w.integer(int64(leverage))
	return w.b
}
