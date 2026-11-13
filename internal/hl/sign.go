package hl

import (
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// Signature Hyperliquid L1 动作签名（v 为 27/28）
type Signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V int    `json:"v"`
}

var (
	domainTypeHash = crypto.Keccak256([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	agentTypeHash  = crypto.Keccak256([]byte("Agent(string source,bytes32 connectionId)"))
	nameHash       = crypto.Keccak256([]byte("Exchange"))
	versionHash    = crypto.Keccak256([]byte("1"))
	sourceHash     = crypto.Keccak256([]byte("a")) // mainnet phantom agent source
)

// LoadPrivateKey 解析 0x 前缀的私钥。
func LoadPrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(hexKey), "0x")
	if len(raw) != 64 {
		return nil, errors.New("私钥长度不是 64 位十六进制")
	}
	return crypto.HexToECDSA(raw)
}

// AddressFromKey 推导小写钱包地址。
func AddressFromKey(key *ecdsa.PrivateKey) string {
	return strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
}

// ActionHash keccak(msgpack(action) || nonce_be8 || 0x00)
func ActionHash(actionBytes []byte, nonce int64) [32]byte {
	buf := make([]byte, 0, len(actionBytes)+9)
	buf = append(buf, actionBytes...)
	var nb [8]byte
	binary.BigEndian.PutUint64(nb[:], uint64(nonce))
	buf = append(buf, nb[:]...)
	buf = append(buf, 0x00) // vault_address 为 None
	var out [32]byte
	copy(out[:], crypto.Keccak256(buf))
	return out
}

func concat(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// eip712Digest 计算 Agent 类型 EIP-712 digest：
// domain = {name:"Exchange", version:"1", chainId:1337, verifyingContract:0x0}
func eip712Digest(connectionID [32]byte) [32]byte {
	var chainID [32]byte
	binary.BigEndian.PutUint64(chainID[24:], 1337)
	var zeroAddr [32]byte // address 右对齐，20 字节零

	domainSeparator := crypto.Keccak256(concat(domainTypeHash, nameHash, versionHash, chainID[:], zeroAddr[:]))
	structHash := crypto.Keccak256(concat(agentTypeHash, sourceHash, connectionID[:]))
	var out [32]byte
	copy(out[:], crypto.Keccak256(concat([]byte{0x19, 0x01}, domainSeparator, structHash)))
	return out
}

// SignL1Action 对 msgpack 编码后的动作签名。
func SignL1Action(key *ecdsa.PrivateKey, actionBytes []byte, nonce int64) (Signature, error) {
	actionHash := ActionHash(actionBytes, nonce)
	digest := eip712Digest(actionHash)
	raw, err := crypto.Sign(digest[:], key)
	if err != nil {
		return Signature{}, err
	}
	v := int(raw[64]) + 27
	return Signature{
		R: "0x" + hex.EncodeToString(raw[0:32]),
		S: "0x" + hex.EncodeToString(raw[32:64]),
		V: v,
	}, nil
}
