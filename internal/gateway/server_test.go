package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
	"im/internal/config"
)

// loopbackLogic 模拟 logic+Push 回路：领 seq 后投递给本 hub 上的接收者。
// 用它隔离测试 gateway 的 WS 接入与推送链路，不依赖真实 logic 进程。
type loopbackLogic struct {
	h       *hub
	seq     atomic.Uint64
	mu      sync.Mutex
	syncReq []*imv1.SyncRequest
}

func (l *loopbackLogic) SendMessage(ctx context.Context, in *imv1.SendMessageRequest, opts ...grpc.CallOption) (*imv1.SendMessageResponse, error) {
	env := proto.Clone(in.Envelope).(*imv1.Envelope)
	env.Seq = l.seq.Add(1)
	return &imv1.SendMessageResponse{
		MsgId:     env.MsgId,
		Seq:       env.Seq,
		Delivered: l.h.deliver(env.To, env),
	}, nil
}

func (l *loopbackLogic) Sync(ctx context.Context, in *imv1.SyncRequest, opts ...grpc.CallOption) (*imv1.SyncResponse, error) {
	l.mu.Lock()
	l.syncReq = append(l.syncReq, proto.Clone(in).(*imv1.SyncRequest))
	l.mu.Unlock()
	return &imv1.SyncResponse{
		ConvId:   in.ConvId,
		Messages: []*imv1.Envelope{{MsgId: "synced-1", Seq: in.Since + 1, From: "peer", Content: "catched up"}},
		HasMore:  false,
	}, nil
}

func dialTestClient(t *testing.T, srvURL, userID string) *websocket.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srvURL, "http") + "/ws?user_id=" + userID
	ws, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", userID, err)
	}
	t.Cleanup(func() { ws.Close() })
	return ws
}

func sendText(t *testing.T, ws *websocket.Conn, from, to, content string) {
	t.Helper()
	data, err := proto.Marshal(&imv1.Frame{Payload: &imv1.Frame_Message{Message: &imv1.Envelope{
		MsgId: "test-" + content, Type: imv1.MsgType_MSG_TYPE_TEXT,
		From: from, To: to, Content: content,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}
}

func readEnv(t *testing.T, ws *websocket.Conn) *imv1.Envelope {
	t.Helper()
	return readFrame(t, ws).GetMessage()
}

func readFrame(t *testing.T, ws *websocket.Conn) *imv1.Frame {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	frame := &imv1.Frame{}
	if err := proto.Unmarshal(data, frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestEndToEndSingleChat(t *testing.T) {
	s := NewServer(config.Gateway{}, nil)
	s.logic = &loopbackLogic{h: s.hub}
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice := dialTestClient(t, srv.URL, "alice")
	bob := dialTestClient(t, srv.URL, "bob")

	sendText(t, alice, "alice", "bob", "hello bob")
	got := readEnv(t, bob)
	if got.From != "alice" || got.Content != "hello bob" || got.Type != imv1.MsgType_MSG_TYPE_TEXT {
		t.Fatalf("bob got %+v", got)
	}
	if got.Seq == 0 {
		t.Fatal("pushed message must carry seq")
	}
	ack := readEnv(t, alice)
	if ack.From != "alice" || ack.MsgId != got.MsgId || ack.Seq != got.Seq {
		t.Fatalf("alice ack should echo her message with same seq, got %+v", ack)
	}

	sendText(t, bob, "bob", "alice", "hi alice")
	got = readEnv(t, alice)
	if got.From != "bob" || got.Content != "hi alice" {
		t.Fatalf("alice got %+v", got)
	}
}

func TestOfflineSendGetsAckWithSeq(t *testing.T) {
	s := NewServer(config.Gateway{}, nil)
	s.logic = &loopbackLogic{h: s.hub}
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice := dialTestClient(t, srv.URL, "alice")
	sendText(t, alice, "alice", "ghost", "anyone there?")

	// M2 语义：离线是正常路径，消息已落库，发送者收到带 seq 的回执而非系统提示
	got := readEnv(t, alice)
	if got.Type != imv1.MsgType_MSG_TYPE_TEXT || got.From != "alice" || got.Seq == 0 {
		t.Fatalf("expected ack with seq, got %+v", got)
	}
}

func TestFromIsForcedToConnIdentity(t *testing.T) {
	s := NewServer(config.Gateway{}, nil)
	s.logic = &loopbackLogic{h: s.hub}
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice := dialTestClient(t, srv.URL, "alice")
	bob := dialTestClient(t, srv.URL, "bob")

	// alice 伪造 from=mallory，gateway 必须改写回 alice
	sendText(t, alice, "mallory", "bob", "spoofed")
	got := readEnv(t, bob)
	if got.From != "alice" {
		t.Fatalf("from should be forced to alice, got %q", got.From)
	}
}

func TestSyncRoundTripForcesIdentity(t *testing.T) {
	s := NewServer(config.Gateway{}, nil)
	lb := &loopbackLogic{h: s.hub}
	s.logic = lb
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice := dialTestClient(t, srv.URL, "alice")
	// 伪造 user_id=mallory 试图越权同步，gateway 必须改写为连接身份 alice
	data, err := proto.Marshal(&imv1.Frame{Payload: &imv1.Frame_SyncReq{SyncReq: &imv1.SyncRequest{
		ConvId: "p:alice:bob", Since: 4, UserId: "mallory",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := alice.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}

	resp := readFrame(t, alice).GetSyncResp()
	if resp == nil || len(resp.Messages) != 1 || resp.Messages[0].Seq != 5 {
		t.Fatalf("sync resp wrong: %+v", resp)
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if len(lb.syncReq) != 1 || lb.syncReq[0].UserId != "alice" {
		t.Fatalf("sync user_id must be forced to alice, got %+v", lb.syncReq)
	}
}