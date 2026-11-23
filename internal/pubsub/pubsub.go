package pubsub

import (
	"context"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
)

// Publisher 是推送通道抽象：M3 用 Redis Pub/Sub，将来换 Kafka 只换实现（锁定设计）。
// Pub/Sub 不保证送达——恰好匹配可靠性模型：推送只是通知，真相在库，客户端 SYNC 兜底。
type Publisher interface {
	Publish(ctx context.Context, gatewayAddr string, req *imv1.PushRequest) error
}

// Channel 每台 gateway 只订阅自己的专属频道，logic 查路由后定向发布，避免全局广播。
func Channel(gatewayAddr string) string { return "push:" + gatewayAddr }

type RedisPublisher struct {
	rdb *redis.Client
}

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

func (p *RedisPublisher) Publish(ctx context.Context, gatewayAddr string, req *imv1.PushRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, Channel(gatewayAddr), data).Err()
}