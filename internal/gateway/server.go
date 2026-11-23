package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
	"im/internal/auth"
	"im/internal/config"
	"im/internal/pubsub"
	"im/internal/route"
)

// routeTable 是 gateway 对全局路由表的最小依赖（单测可用假件，不拖 Redis）。
type routeTable interface {
	Register(ctx context.Context, uid, connID, gwAddr string) error
	Renew(ctx context.Context, uid, connID, gwAddr string) error
	Remove(ctx context.Context, uid, connID string) error
}

// Server 是接入层：对客户端暴露 WebSocket，订阅自己的 Pub/Sub 频道接收推送任务。
// M3 起反向 gRPC Push 服务器退役——推送传输由 Redis Pub/Sub 承担，gateway 不再监听 gRPC。
type Server struct {
	cfg      config.Gateway
	hub      *hub
	logic    imv1.LogicClient
	secret   []byte
	routes   routeTable
	rdb      *redis.Client
	upgrader websocket.Upgrader
}

func NewServer(cfg config.Gateway, logic imv1.LogicClient, jwtSecret []byte, routes routeTable, rdb *redis.Client) *Server {
	return &Server{
		cfg:    cfg,
		hub:    newHub(),
		logic:  logic,
		secret: jwtSecret,
		routes: routes,
		rdb:    rdb,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// [PRODUCTION NOTE] 生产环境必须校验 Origin，防止跨站 WebSocket 劫持
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	// AuthN 在握手层：本地验签（零 IO），失败不 upgrade、不分配连接资源。
	// [PRODUCTION NOTE] query 传 token 会进 access log；生产用短期一次性 ticket 替代。
	uid, err := auth.ParseAccessToken(r.URL.Query().Get("token"), s.secret)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("upgrade failed", "err", err)
		return
	}

	c := newConn(uid, ws)
	s.hub.register(c)

	// 路由登记失败则拒绝连接：让客户端退避重连，而不是带着"推不到"的静默缺陷服务
	regCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = s.routes.Register(regCtx, uid, c.id, s.cfg.Addr)
	cancel()
	if err != nil {
		slog.Error("route register failed", "user", uid, "err", err)
		s.hub.remove(uid, c)
		c.shutdown()
		return
	}

	connCtx, stopHeartbeat := context.WithCancel(context.Background())
	go s.heartbeat(connCtx, uid, c.id)
	go c.writePump()
	c.readPump(s.logic) // 阻塞直到连接断开
	stopHeartbeat()
	s.hub.remove(uid, c)
	rmCtx, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	if err := s.routes.Remove(rmCtx, uid, c.id); err != nil {
		slog.Warn("route remove failed (TTL will heal)", "user", uid, "err", err)
	}
	cancel2()
	c.shutdown()
}

// heartbeat 按 TTL/3 续期自己的路由条目；gateway 崩溃则续期停止，条目最迟 TTL 过期。
func (s *Server) heartbeat(ctx context.Context, uid, connID string) {
	t := time.NewTicker(route.DefaultTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := s.routes.Renew(cctx, uid, connID, s.cfg.Addr)
			cancel()
			if err != nil {
				slog.Warn("route renew failed", "user", uid, "err", err)
			}
		}
	}
}

// subscribePush 订阅本实例专属频道；go-redis 的 Channel() 自带断线重订阅。
func (s *Server) subscribePush(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, pubsub.Channel(s.cfg.Addr))
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			sub.Close()
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			req := &imv1.PushRequest{}
			if err := proto.Unmarshal([]byte(msg.Payload), req); err != nil {
				slog.Warn("bad push payload", "err", err)
				continue
			}
			s.hub.deliver(req.UserId, req.Envelope)
		}
	}
}

// Run 启动 WebSocket 服务与 Pub/Sub 订阅，阻塞直到 ctx 取消。
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	httpSrv := &http.Server{Addr: s.cfg.WSAddr, Handler: mux}

	go s.subscribePush(ctx)

	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()

	slog.Info("gateway started", "ws", s.cfg.WSAddr, "addr", s.cfg.Addr)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		slog.Error("server error", "err", err)
	}

	s.hub.closeAll()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdownCtx)
	return nil
}