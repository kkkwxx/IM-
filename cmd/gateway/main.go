package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"im/api/imv1"
	"im/internal/config"
	"im/internal/gateway"
)

func main() {
	cfgPath := flag.String("config", "configs/config.json", "config file path")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	// [PRODUCTION NOTE] 生产环境服务间通信应启用 mTLS，而非 insecure
	logicConn, err := grpc.NewClient(cfg.Gateway.LogicGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial logic", "err", err)
		os.Exit(1)
	}
	defer logicConn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := gateway.NewServer(cfg.Gateway, imv1.NewLogicClient(logicConn))
	if err := srv.Run(ctx); err != nil {
		slog.Error("gateway run", "err", err)
		os.Exit(1)
	}
}