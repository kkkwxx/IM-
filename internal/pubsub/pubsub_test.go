package pubsub

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
)

// 集成测试需要真实 Redis：
// IM_TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/pubsub/
//
// 频道名带测试名后缀，避免与其他测试/实例串扰。
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("IM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IM_TEST_REDIS_ADDR not set, skipping integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestPublishRoundTrip(t *testing.T) {
	rdb := testRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gw := "gw-" + t.Name()

	sub := rdb.Subscribe(ctx, Channel(gw))
	defer sub.Close()
	// Pub/Sub 不存离线消息：必须先确认订阅生效再发布，否则消息直接丢失
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	req := &imv1.PushRequest{
		UserId:   "alice",
		Envelope: &imv1.Envelope{MsgId: "m1", Seq: 7, From: "bob", To: "alice", Content: "hi"},
	}
	if err := NewRedisPublisher(rdb).Publish(ctx, gw, req); err != nil {
		t.Fatal(err)
	}

	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Channel != Channel(gw) {
		t.Fatalf("channel %q, want %q", msg.Channel, Channel(gw))
	}
	got := &imv1.PushRequest{}
	if err := proto.Unmarshal([]byte(msg.Payload), got); err != nil {
		t.Fatal(err)
	}
	if got.UserId != "alice" || got.Envelope.MsgId != "m1" || got.Envelope.Seq != 7 || got.Envelope.Content != "hi" {
		t.Fatalf("payload roundtrip wrong: %+v", got)
	}
}

// 定向性：发布到 gw-b 的频道，订阅 gw-a 的一方绝不能收到（对比全局广播方案的核心差异）。
func TestPublishIsTargeted(t *testing.T) {
	rdb := testRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	me, other := "gw-a-"+t.Name(), "gw-b-"+t.Name()

	sub := rdb.Subscribe(ctx, Channel(me))
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	if err := NewRedisPublisher(rdb).Publish(ctx, other, &imv1.PushRequest{UserId: "alice"}); err != nil {
		t.Fatal(err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer waitCancel()
	if _, err := sub.ReceiveMessage(waitCtx); err == nil {
		t.Fatal("message published to another gateway's channel must not arrive here")
	}
}