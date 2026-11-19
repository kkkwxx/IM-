package gateway

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"

	"im/api/imv1"
	"im/internal/config"
)

// Server 是接入层：对客户端暴露 WebSocket，对 logic 暴露 gRPC Push 服务。
type Server struct {
	imv1.UnimplementedGatewayServer
	cfg      config.Gateway
	hub      *hub
	logic    imv1.LogicClient
	upgrader websocket.Upgrader
}

func NewServer(cfg config.Gateway, logic imv1.LogicClient) *Server {
	return &Server{
		cfg:   cfg,
		hub:   newHub(),
		logic: logic,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// [PRODUCTION NOTE] 生产环境必须校验 Origin，防止跨站 WebSocket 劫持
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// Push 实现 imv1.GatewayServer：logic 通过它把消息推给本实例上的在线用户。
func (s *Server) Push(ctx context.Context, req *imv1.PushRequest) (*imv1.PushResponse, error) {
	return &imv1.PushResponse{Delivered: s.hub.deliver(req.UserId, req.Envelope)}, nil
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "missing user_id", http.StatusBadRequest)
		return
	}
	// [PRODUCTION NOTE] M2 引入 JWT：握手阶段校验 token，拒绝非法身份

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("upgrade failed", "err", err)
		return
	}

	c := newConn(userID, ws)
	s.hub.register(c)
	go c.writePump()
	c.readPump(s.hub, s.logic) // 阻塞直到连接断开
}

// Run 同时启动 WebSocket HTTP 服务与 gRPC Push 服务，阻塞直到 ctx 取消。
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	httpSrv := &http.Server{Addr: s.cfg.WSAddr, Handler: mux}

	grpcSrv := grpc.NewServer()
	imv1.RegisterGatewayServer(grpcSrv, s)
	lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() { errCh <- httpSrv.ListenAndServe() }()
	go func() { errCh <- grpcSrv.Serve(lis) }()

	slog.Info("gateway started", "ws", s.cfg.WSAddr, "grpc", s.cfg.GRPCAddr)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		slog.Error("server error", "err", err)
	}

	s.hub.closeAll()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
	return nil
}