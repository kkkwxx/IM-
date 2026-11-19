package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/redis/go-redis/v9"

	"im/api/imv1"
	"im/internal/config"
	"im/internal/logic"
	"im/internal/seq"
	"im/internal/store"
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

	// [PRODUCTION NOTE] M3 起 gateway 地址不再写死在配置里，
	// 而是查 Redis 全局路由表得到接收者所在实例，再定向 Push。
	gwConn, err := grpc.NewClient(cfg.Logic.GatewayGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial gateway", "err", err)
		os.Exit(1)
	}
	defer gwConn.Close()

	st, err := store.Open(context.Background(), cfg.Logic.MySQLDSN)
	if err != nil {
		slog.Error("open mysql", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.Logic.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		slog.Error("ping redis", "err", err)
		os.Exit(1)
	}
	alloc := seq.NewAllocator(rdb, st)

	lis, err := net.Listen("tcp", cfg.Logic.GRPCAddr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	logic.Register(grpcSrv, imv1.NewGatewayClient(gwConn), st, alloc)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()

	slog.Info("logic started", "grpc", cfg.Logic.GRPCAddr)
	if err := grpcSrv.Serve(lis); err != nil {
		slog.Error("logic serve", "err", err)
		os.Exit(1)
	}
}