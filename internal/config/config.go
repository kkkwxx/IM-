package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Gateway struct {
	WSAddr        string `json:"ws_addr"`
	GRPCAddr      string `json:"grpc_addr"`
	LogicGRPCAddr string `json:"logic_grpc_addr"`
}

type Logic struct {
	GRPCAddr        string `json:"grpc_addr"`
	GatewayGRPCAddr string `json:"gateway_grpc_addr"`
	MySQLDSN        string `json:"mysql_dsn"`
	RedisAddr       string `json:"redis_addr"`
}

type Config struct {
	Gateway Gateway `json:"gateway"`
	Logic   Logic   `json:"logic"`
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