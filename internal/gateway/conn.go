package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second
	maxMsgSize = 64 << 10
	sendQueue  = 256
	rpcTimeout = 3 * time.Second
)

// conn 的并发模型：readPump 与 writePump 各一个 goroutine。
// gorilla/websocket 的写不并发安全，所有写操作必须经 sendCh 由 writePump 串行化。
type conn struct {
	id        string // 每连接唯一，路由表 field 键；多端共存与精确清理的基础
	userID    string
	ws        *websocket.Conn
	sendCh    chan *imv1.Frame
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newConn(userID string, ws *websocket.Conn) *conn {
	return &conn{
		id:      newID(),
		userID:  userID,
		ws:      ws,
		sendCh:  make(chan *imv1.Frame, sendQueue),
		closeCh: make(chan struct{}),
	}
}

// trySend 非阻塞投递。队列满说明客户端消费过慢（背压），
// 返回 false 由调用方断开连接，而不是无界缓冲拖垮网关。
func (c *conn) trySend(f *imv1.Frame) bool {
	select {
	case <-c.closeCh:
		return false
	case c.sendCh <- f:
		return true
	default:
		return false
	}
}

func (c *conn) shutdown() {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		// 关闭底层连接以唤醒阻塞中的 ReadMessage
		c.ws.Close()
	})
}

func (c *conn) readPump(logic imv1.LogicClient) {
	defer c.shutdown()

	c.ws.SetReadLimit(maxMsgSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		return c.ws.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("connection read error", "user", c.userID, "err", err)
			}
			return
		}

		frame := &imv1.Frame{}
		if err := proto.Unmarshal(data, frame); err != nil {
			slog.Warn("malformed frame", "user", c.userID, "err", err)
			continue
		}
		switch p := frame.Payload.(type) {
		case *imv1.Frame_Message:
			c.handleMessage(logic, p.Message)
		case *imv1.Frame_SyncReq:
			c.handleSync(logic, p.SyncReq)
		}
	}
}

func (c *conn) handleMessage(logic imv1.LogicClient, env *imv1.Envelope) {
	if env.Type != imv1.MsgType_MSG_TYPE_TEXT || env.To == "" {
		return
	}
	env.From = c.userID // 强制改写为连接身份，防止客户端伪造发送者
	if env.MsgId == "" {
		env.MsgId = newID()
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	resp, err := logic.SendMessage(ctx, &imv1.SendMessageRequest{Envelope: env})
	cancel()
	if err != nil {
		slog.Error("logic.SendMessage failed", "user", c.userID, "err", err)
		c.trySend(systemFrame(c.userID, "消息发送失败，请重试"))
		return
	}
	// 发送回执：原消息带上落库 seq 回显，客户端据此推进同步水位。
	// 离线不再提示——消息已落库，接收方上线后按 seq 同步，属正常路径。
	env.Seq = resp.Seq
	c.trySend(&imv1.Frame{Payload: &imv1.Frame_Message{Message: env}})
}

func (c *conn) handleSync(logic imv1.LogicClient, req *imv1.SyncRequest) {
	req.UserId = c.userID // 强制连接身份，logic 据此做会话成员校验，防越权同步
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	resp, err := logic.Sync(ctx, req)
	cancel()
	if err != nil {
		slog.Error("logic.Sync failed", "user", c.userID, "err", err)
		c.trySend(systemFrame(c.userID, "同步失败，请重试"))
		return
	}
	c.trySend(&imv1.Frame{Payload: &imv1.Frame_SyncResp{SyncResp: resp}})
}

func (c *conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.shutdown()
	}()

	for {
		select {
		case <-c.closeCh:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			c.ws.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		case f := <-c.sendCh:
			data, err := proto.Marshal(f)
			if err != nil {
				slog.Error("marshal frame", "user", c.userID, "err", err)
				continue
			}
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
				slog.Warn("connection write error", "user", c.userID, "err", err)
				return
			}
		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func systemFrame(to, content string) *imv1.Frame {
	return &imv1.Frame{Payload: &imv1.Frame_Message{Message: &imv1.Envelope{
		MsgId:       newID(),
		Type:        imv1.MsgType_MSG_TYPE_SYSTEM,
		From:        "system",
		To:          to,
		Content:     content,
		TimestampMs: time.Now().UnixMilli(),
	}}}
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand 失败意味着系统熵源异常，不可恢复
	}
	return hex.EncodeToString(b)
}