//go:build !rust

package hotpath

import (
	"crypto/ecdsa"

	"github.com/mev751/hype/internal/hl"
)

// goSigner 纯 Go 签名实现：复用已通过 Python SDK 向量校验的签名代码。
type goSigner struct {
	key *ecdsa.PrivateKey
}

// NewSigner 构造签名器（纯 Go 实现）。
func NewSigner(privKeyHex string) (Signer, error) {
	key, err := hl.LoadPrivateKey(privKeyHex)
	if err != nil {
		return nil, err
	}
	return &goSigner{key: key}, nil
}

// SignAction 对 msgpack 编码后的动作签名。
func (s *goSigner) SignAction(actionBytes []byte, nonce int64) (string, string, int, error) {
	sig, err := hl.SignL1Action(s.key, actionBytes, nonce)
	if err != nil {
		return "", "", 0, err
	}
	return sig.R, sig.S, sig.V, nil
}

// Backend 实现标识
func (s *goSigner) Backend() string { return "go" }

// BackendVersion 纯 Go 实现无 ABI 版本概念，返回 0。
func BackendVersion() uint32 { return 0 }
