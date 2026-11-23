package gateway

import (
	"context"
	"errors"
	"net"
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
	"im/internal/auth"
	"im/internal/config"
)

var testSecret = []byte("test-secret")

func testToken(t *testing.T, uid string) string {
	t.Helper()
	tok, err := auth.IssueAccessToken(uid, testSecret, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// fakeRoutes 内存版路由表，记录登记/删除，可注入登记失败。
type fakeRoutes struct {
	mu           sync.Mutex
	entries      map[string]string // uid|connID → gwAddr
	failRegister bool
}

func newFakeRoutes() *fakeRoutes { return &fakeRoutes{entries: map[string]string{}} }

func (f *fakeRoutes) Register(ctx context.Context, uid, connID, gwAddr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRegister {
		return errors.New("redis down")
	}
	f.entries[uid+"|"+connID] = gwAddr
	return nil
}

func (f *fakeRoutes) Renew(ctx context.Context, uid, connID, gwAddr string) error {
	return f.Register(ctx, uid, connID, gwAddr)
}

func (f *fakeRoutes) Remove(ctx context.Context, uid, connID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, uid+"|"+connID)
	return nil
}

func (f *fakeRoutes) has(uid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.entries {
		if strings.HasPrefix(k, uid+"|") {
			return true
		}
	}
	return false
}

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
	// 模拟真实 fanout：接收方 + 发送方双方扇出（发送设备会收到回执+副本，客户端按 seq 去重）
	delivered := l.h.deliver(env.To, env)
	l.h.deliver(env.From, env)
	return &imv1.SendMessageResponse{
		MsgId:     env.MsgId,
		Seq:       env.Seq,
		Delivered: delivered,
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
	u := "ws" + strings.TrimPrefix(srvURL, "http") + "/ws?token=" + testToken(t, userID)
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
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
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
	// M3 多端同步：发送设备收到本地回执 + 扇出副本，两帧同 msg_id 同 seq（客户端按 seq 去重）
	for i := range 2 {
		echo := readEnv(t, alice)
		if echo.From != "alice" || echo.MsgId != got.MsgId || echo.Seq != got.Seq {
			t.Fatalf("alice echo %d should carry her message with same seq, got %+v", i, echo)
		}
	}

	sendText(t, bob, "bob", "alice", "hi alice")
	got = readEnv(t, alice)
	if got.From != "bob" || got.Content != "hi alice" {
		t.Fatalf("alice got %+v", got)
	}
}

func TestOfflineSendGetsAckWithSeq(t *testing.T) {
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
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
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
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
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
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

func TestHandshakeRejectsInvalidToken(t *testing.T) {
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)
	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	for _, u := range []string{
		base,                       // 无 token
		base + "?token=garbage",    // 非法格式
		base + "?token=" + testToken(t, "alice") + "x", // 篡改签名
	} {
		_, resp, err := websocket.DefaultDialer.Dial(u, nil)
		if err == nil {
			t.Fatalf("dial %s should fail", u)
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("want 401 for %s, got resp=%v err=%v", u, resp, err)
		}
	}
}

func TestIdentityComesFromTokenNotQuery(t *testing.T) {
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
	s.logic = &loopbackLogic{h: s.hub}
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	// token 身份是 alice，query 里塞 user_id=mallory 必须被无视
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=" + testToken(t, "alice") + "&user_id=mallory"
	alice, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { alice.Close() })
	bob := dialTestClient(t, srv.URL, "bob")

	sendText(t, alice, "mallory", "bob", "who am i")
	if got := readEnv(t, bob); got.From != "alice" {
		t.Fatalf("identity must come from token (alice), got %q", got.From)
	}
}

// waitForRoute 轮询 fakeRoutes 直到条件满足（登记/删除发生在握手后的 goroutine 里，异步）。
func waitForRoute(t *testing.T, fr *fakeRoutes, uid string, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for fr.has(uid) != want {
		if time.Now().After(deadline) {
			t.Fatalf("route entry for %s: want present=%v, timed out", uid, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRouteLifecycleRegisteredAndRemoved(t *testing.T) {
	fr := newFakeRoutes()
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, fr, nil)
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice := dialTestClient(t, srv.URL, "alice")
	waitForRoute(t, fr, "alice", true)

	alice.Close()
	waitForRoute(t, fr, "alice", false)
}

func TestRouteRegisterFailureRejectsConn(t *testing.T) {
	fr := newFakeRoutes()
	fr.failRegister = true
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, fr, nil)
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	// 握手（upgrade）先于路由登记，dial 会成功；登记失败后服务端必须立即断开
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=" + testToken(t, "alice")
	ws, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })

	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = ws.ReadMessage()
	if err == nil {
		t.Fatal("server must close conn when route register fails")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("conn still open after register failure (read timed out instead of closed)")
	}
}

func TestMultiDeviceFanout(t *testing.T) {
	s := NewServer(config.Gateway{Addr: "test-gw"}, nil, testSecret, newFakeRoutes(), nil)
	s.logic = &loopbackLogic{h: s.hub}
	srv := httptest.NewServer(http.HandlerFunc(s.handleWS))
	t.Cleanup(srv.Close)

	alice1 := dialTestClient(t, srv.URL, "alice")
	alice2 := dialTestClient(t, srv.URL, "alice")
	bob := dialTestClient(t, srv.URL, "bob")

	// 接收方多端：bob 发给 alice，alice 两台设备都要收到
	sendText(t, bob, "bob", "alice", "to both devices")
	for i, ws := range []*websocket.Conn{alice1, alice2} {
		got := readEnv(t, ws)
		if got.From != "bob" || got.Content != "to both devices" || got.Seq == 0 {
			t.Fatalf("alice device %d got %+v", i, got)
		}
	}
	// 发送方 bob 自己也收到两帧：本地回执 + 扇出副本（同 seq，客户端去重），先消费掉
	for i := range 2 {
		if echo := readEnv(t, bob); echo.Content != "to both devices" || echo.Seq == 0 {
			t.Fatalf("bob echo %d wrong: %+v", i, echo)
		}
	}

	// 发送方多端：alice 设备1 发出，设备2 要收到同 seq 的同步副本
	sendText(t, alice1, "alice", "bob", "from dev1")
	got := readEnv(t, bob)
	echo := readEnv(t, alice2)
	if echo.MsgId != got.MsgId || echo.Seq != got.Seq || echo.Content != "from dev1" {
		t.Fatalf("alice dev2 should sync sender's message, got %+v (bob got %+v)", echo, got)
	}
}