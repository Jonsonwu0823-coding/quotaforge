// Package apikey 生成 QF 发给客户端的 Key，并定义 Key 的哈希。配置里只存哈希。
package apikey

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

// prefix 是 Key 专用的前缀，和调用号的 qf- 区分开：“qfk_ 加 43 个 base64url 字符”只会是一把 Key，
// 日志和代码里泄露的 Key 能按它精确扫出来。
const prefix = "qfk_"

// Generate 返回一把新 Key：prefix 加 32 字节 crypto/rand 随机数的 base64url（无填充，43 个字符）。
func Generate() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand.Read 不会返回错误
	return prefix + base64.RawURLEncoding.EncodeToString(b)
}

// Hash 是 Key 的 SHA-256，小写十六进制。配置里存的就是它。
func Hash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
