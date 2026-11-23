package logic

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"im/api/imv1"
	"im/internal/store"
)

// 窄接口：logic 只依赖它用到的存储/发号能力，单测可用假件模拟每一步失败。
type messageStore interface {
	GetByMsgID(ctx context.Context, convID, msgID string) (*store.Message, error)
	InsertMessage(ctx context.Context, m *store.Message) error
	ListMessages(ctx context.Context, convID string, since int64, limit int) ([]store.Message, error)
}

type seqAllocator interface {
	Alloc(ctx context.Context, convID string) (int64, error)
}

type routeLookup interface {
	Lookup(ctx context.Context, uid string) ([]string, error)
}

type publisher interface {
	Publish(ctx context.Context, gatewayAddr string, req *imv1.PushRequest) error
}

// Service 是业务层，承担可靠投递核心：先持久化并分配会话级 seq，再推送。
// 推送失败不影响可靠性，接收方上线后按 seq 增量同步补齐。
type Service struct {
	imv1.UnimplementedLogicServer
	store  messageStore
	alloc  seqAllocator
	routes routeLookup
	pub    publisher
}

func NewService(st messageStore, alloc seqAllocator, routes routeLookup, pub publisher) *Service {
	return &Service{store: st, alloc: alloc, routes: routes, pub: pub}
}

// convID 单聊会话键：双方 uid 排序拼接，保证 alice→bob 与 bob→alice 落在同一会话。
// [PRODUCTION NOTE] M4 群聊为 "g:{groupID}"，同一张 messages 表不变（读扩散时间线）。
func convID(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return "p:" + a + ":" + b
}

func (s *Service) SendMessage(ctx context.Context, req *imv1.SendMessageRequest) (*imv1.SendMessageResponse, error) {
	env := req.GetEnvelope()
	if env == nil || env.From == "" || env.To == "" || env.MsgId == "" {
		return nil, status.Error(codes.InvalidArgument, "envelope must have msg_id, from and to")
	}
	if env.Type != imv1.MsgType_MSG_TYPE_TEXT {
		return nil, status.Error(codes.InvalidArgument, "unsupported message type")
	}

	env.TimestampMs = time.Now().UnixMilli()
	conv := convID(env.From, env.To)

	// 1. 查重：落库成功但响应丢失的重试在这里幂等返回原 seq，不领新号。
	existing, err := s.store.GetByMsgID(ctx, conv, env.MsgId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dedup query: %v", err)
	}
	if existing != nil {
		return &imv1.SendMessageResponse{MsgId: env.MsgId, Seq: uint64(existing.Seq), Delivered: true}, nil
	}

	// 2. 领号：Redis 原子 INCR。失败则客户端重试领新号，旧号成为无害空洞。
	seq, err := s.alloc.Alloc(ctx, conv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "seq alloc: %v", err)
	}

	// 3. 落库：唯一索引是幂等与防重号的最终防线。
	m := &store.Message{
		ConvID:    conv,
		Seq:       seq,
		MsgID:     env.MsgId,
		Sender:    env.From,
		Content:   env.Content,
		CreatedAt: time.UnixMilli(env.TimestampMs),
	}
	if err := s.store.InsertMessage(ctx, m); err != nil {
		switch {
		case errors.Is(err, store.ErrDuplicateMsg):
			// 并发重试竞态：查重与落库之间另一请求已插入。返回那条的 seq。
			existing, qerr := s.store.GetByMsgID(ctx, conv, env.MsgId)
			if qerr != nil || existing == nil {
				return nil, status.Errorf(codes.Internal, "dedup after conflict: %v", qerr)
			}
			return &imv1.SendMessageResponse{MsgId: env.MsgId, Seq: uint64(existing.Seq), Delivered: true}, nil
		case errors.Is(err, store.ErrDuplicateSeq):
			slog.Error("seq collision defense triggered", "conv", conv, "seq", seq, "msg_id", env.MsgId)
			return nil, status.Errorf(codes.Internal, "seq collision")
		default:
			// 落库失败绝不推送：否则接收方看到一条永远同步不到的消息。
			return nil, status.Errorf(codes.Internal, "persist: %v", err)
		}
	}

	env.Seq = uint64(seq)

	// 4. 推送：尽力而为的通知通道，失败仅记录，可靠性由落库+seq 同步保证。
	pushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	delivered := s.fanout(pushCtx, env)
	return &imv1.SendMessageResponse{MsgId: env.MsgId, Seq: uint64(seq), Delivered: delivered}, nil
}

// fanout 查接收方与发送方双方的路由，向每台目标 gateway 的专属频道发布。
// 发送方也要查：其所有设备（含发送设备本人）都应收到这条消息，实现多端同步；
// 发送设备会同时收到本地回执与扇出副本，客户端按 seq 去重（M2.5 已具备）。
func (s *Service) fanout(ctx context.Context, env *imv1.Envelope) bool {
	delivered := false
	for _, uid := range []string{env.To, env.From} {
		gws, err := s.routes.Lookup(ctx, uid)
		if err != nil {
			slog.Error("route lookup failed", "uid", uid, "err", err)
			continue
		}
		for _, gw := range gws {
			if err := s.pub.Publish(ctx, gw, &imv1.PushRequest{UserId: uid, Envelope: env}); err != nil {
				slog.Error("publish failed", "gw", gw, "uid", uid, "err", err)
				continue
			}
			if uid == env.To {
				delivered = true
			}
		}
	}
	return delivered
}

var _ imv1.LogicServer = (*Service)(nil)

const (
	defaultSyncLimit = 100
	maxSyncLimit     = 200
)

// singleChatPeers 解析单聊会话键 "p:a:b"，返回双方 uid。
// [PRODUCTION NOTE] M4 群聊 "g:{groupID}" 的成员校验需查群成员表。
func singleChatPeers(conv string) (string, string, bool) {
	parts := strings.Split(conv, ":")
	if len(parts) != 3 || parts[0] != "p" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// Sync 增量同步：客户端凭水位拉取 seq > since 的消息。
// 安全：user_id 由 gateway 强制改写为连接身份，这里校验会话成员，防越权读他人会话。
func (s *Service) Sync(ctx context.Context, req *imv1.SyncRequest) (*imv1.SyncResponse, error) {
	if req.GetConvId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sync must have conv_id and user_id")
	}
	a, b, ok := singleChatPeers(req.ConvId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "unsupported conv_id format")
	}
	if req.UserId != a && req.UserId != b {
		return nil, status.Error(codes.PermissionDenied, "not a member of this conversation")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = defaultSyncLimit
	}
	if limit > maxSyncLimit {
		limit = maxSyncLimit
	}

	// 多查一条判定 has_more，省一次 COUNT
	ms, err := s.store.ListMessages(ctx, req.ConvId, int64(req.Since), limit+1)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sync query: %v", err)
	}
	hasMore := len(ms) > limit
	if hasMore {
		ms = ms[:limit]
	}

	envs := make([]*imv1.Envelope, len(ms))
	for i, m := range ms {
		peer := a
		if m.Sender == a {
			peer = b
		}
		envs[i] = &imv1.Envelope{
			MsgId:       m.MsgID,
			Type:        imv1.MsgType_MSG_TYPE_TEXT,
			From:        m.Sender,
			To:          peer,
			Content:     m.Content,
			TimestampMs: m.CreatedAt.UnixMilli(),
			Seq:         uint64(m.Seq),
		}
	}
	return &imv1.SyncResponse{ConvId: req.ConvId, Messages: envs, HasMore: hasMore}, nil
}

// Register 把业务服务挂到 gRPC server。
func Register(srv *grpc.Server, st messageStore, alloc seqAllocator, routes routeLookup, pub publisher) {
	imv1.RegisterLogicServer(srv, NewService(st, alloc, routes, pub))
}