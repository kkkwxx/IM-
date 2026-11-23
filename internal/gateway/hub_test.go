package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"im/api/imv1"
)

// testConn 建立一条真实 WebSocket 连接用于测试（shutdown 需要真实 ws 才能唤醒读泵）。
func testConn(t *testing.T, userID string) *conn {
	t.Helper()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := newConn(userID, ws)
	t.Cleanup(c.shutdown)
	return c
}

func TestHubDeliver(t *testing.T) {
	h := newHub()
	c := testConn(t, "alice")
	h.register(c)

	env := &imv1.Envelope{MsgId: "m1", To: "alice"}
	if !h.deliver("alice", env) {
		t.Fatal("deliver to online user should succeed")
	}
	select {
	case got := <-c.sendCh:
		if got.GetMessage().GetMsgId() != "m1" {
			t.Fatalf("got msg %q, want m1", got.GetMessage().GetMsgId())
		}
	default:
		t.Fatal("envelope not queued")
	}

	if h.deliver("nobody", env) {
		t.Fatal("deliver to offline user should fail")
	}
}

func TestHubRemoveOnlySelf(t *testing.T) {
	h := newHub()
	c1 := testConn(t, "alice")
	c2 := testConn(t, "alice")
	h.register(c1)
	h.register(c2) // 多端共存

	// c1 的读泵退出时可能调用 remove，但不能误删 c2
	h.remove("alice", c1)
	if !h.deliver("alice", &imv1.Envelope{MsgId: "m2"}) {
		t.Fatal("c2 should still be registered")
	}
	select {
	case got := <-c2.sendCh:
		if got.GetMessage().GetMsgId() != "m2" {
			t.Fatalf("got %q, want m2", got.GetMessage().GetMsgId())
		}
	default:
		t.Fatal("c2 queue empty")
	}
}

// M3 多端同步：同 uid 多设备共存，deliver 扇出到每台设备，不再踢旧登录。
func TestHubMultiDeviceCoexist(t *testing.T) {
	h := newHub()
	c1 := testConn(t, "alice")
	c2 := testConn(t, "alice")
	h.register(c1)
	h.register(c2)

	if !h.deliver("alice", &imv1.Envelope{MsgId: "m2"}) {
		t.Fatal("deliver should succeed")
	}
	for i, c := range []*conn{c1, c2} {
		select {
		case got := <-c.sendCh:
			if got.GetMessage().GetMsgId() != "m2" {
				t.Fatalf("device %d got %q, want m2", i, got.GetMessage().GetMsgId())
			}
		default:
			t.Fatalf("device %d should receive the fanout", i)
		}
		select {
		case <-c.closeCh:
			t.Fatalf("device %d must not be shut down", i)
		default:
		}
	}
}

func TestBackpressureKicksSlowConsumer(t *testing.T) {
	h := newHub()
	c := testConn(t, "slow")
	h.register(c)

	for i := 0; i < sendQueue; i++ {
		c.sendCh <- &imv1.Frame{}
	}
	if h.deliver("slow", &imv1.Envelope{MsgId: "overflow"}) {
		t.Fatal("deliver to full queue should fail (backpressure)")
	}
	select {
	case <-c.closeCh:
	default:
		t.Fatal("slow consumer should be disconnected")
	}
}