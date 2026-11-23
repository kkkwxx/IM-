package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	AccessTTL  = 2 * time.Hour
	RefreshTTL = 30 * 24 * time.Hour
)

// HashPassword bcrypt 自带随机盐，cost=10 约 100ms 量级，抗 GPU 暴破。
func HashPassword(pw string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(pw), 10)
	return string(h), err
}

func CheckPassword(hash, pw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

type Claims struct {
	UID string `json:"uid"`
	jwt.RegisteredClaims
}

func IssueAccessToken(uid string, secret []byte, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		UID: uid,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

// ParseAccessToken 验签 + 校验 exp，返回 uid。纯本地计算，不查库——
// 这是 access token 用 JWT 的意义：gateway 每连接验一次，零 IO。
func ParseAccessToken(token string, secret []byte) (string, error) {
	t, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return secret, nil
	})
	if err != nil {
		return "", err
	}
	claims, ok := t.Claims.(*Claims)
	if !ok || claims.UID == "" {
		return "", fmt.Errorf("invalid claims")
	}
	return claims.UID, nil
}

// NewRefreshToken 返回 (本体, SHA-256哈希)。本体给客户端，哈希存 DB。
// refresh 不用 JWT：它要的是可吊销（DB 删除即失效），不需要自包含。
func NewRefreshToken() (string, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	token := hex.EncodeToString(b)
	return token, HashToken(token), nil
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}