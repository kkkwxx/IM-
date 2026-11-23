package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/redis/go-redis/v9"

	"im/internal/auth"
	"im/internal/config"
	"im/internal/logic"
	"im/internal/pubsub"
	"im/internal/route"
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
	routes := route.New(rdb, route.DefaultTTL)
	pub := pubsub.NewRedisPublisher(rdb)

	lis, err := net.Listen("tcp", cfg.Logic.GRPCAddr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	logic.Register(grpcSrv, st, alloc, routes, pub)

	// 鉴权 HTTP：register/login/refresh 是一次性请求，与 gRPC 并列暴露
	httpSrv := &http.Server{
		Addr:    cfg.Logic.HTTPAddr,
		Handler: auth.NewHandlers(st, []byte(cfg.JWTSecret)).Routes(),
	}
	go func() {
		slog.Info("auth http started", "addr", cfg.Logic.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http serve", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
	}()

	slog.Info("logic started", "grpc", cfg.Logic.GRPCAddr)
	if err := grpcSrv.Serve(lis); err != nil {
		slog.Error("logic serve", "err", err)
		os.Exit(1)
	}
}