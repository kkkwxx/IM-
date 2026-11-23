package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Gateway struct {
	WSAddr        string `json:"ws_addr"`
	Addr          string `json:"addr"` // 实例标识：路由表值与 Pub/Sub 频道名，M3 起不再监听 gRPC
	LogicGRPCAddr string `json:"logic_grpc_addr"`
	RedisAddr     string `json:"redis_addr"`
}

type Logic struct {
	GRPCAddr  string `json:"grpc_addr"`
	HTTPAddr  string `json:"http_addr"`
	MySQLDSN  string `json:"mysql_dsn"`
	RedisAddr string `json:"redis_addr"`
}

type Config struct {
	Gateway   Gateway `json:"gateway"`
	Logic     Logic   `json:"logic"`
	JWTSecret string  `json:"jwt_secret"` // gateway 验签与 logic 签发共享
}

// Load 读取 JSON 配置。
// [PRODUCTION NOTE] 生产环境敏感项（密钥等）应来自环境变量或密钥管理服务，不落盘明文。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}