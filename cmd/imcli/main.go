package main

import (
	"bufio"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
)

const (
	syncLimit    = 100
	readTimeout  = 90 * time.Second
	writeTimeout = 5 * time.Second
	backoffMax   = 30 * time.Second
)

// imcli 是带本地水位缓存的验证客户端：启动先渲染缓存，再增量 SYNC，推送有洞补拉，断线退避重连。
// 用法: imcli -user alice -peer bob -password alice123
func main() {
	addr := flag.String("addr", "ws://127.0.0.1:8081/ws", "gateway websocket address")
	user := flag.String("user", "", "my user id")
	peer := flag.String("peer", "", "peer user id")
	token := flag.String("token", "", "access token (debug override; skips login/refresh)")
	password := flag.String("password", "", "password for /login and auto refresh")
	httpAddr := flag.String("http", "http://127.0.0.1:8082", "logic auth http address")
	dir := flag.String("dir", ".imcli", "local cache root directory")
	flag.Parse()

	if *user == "" || *peer == "" || (*token == "" && *password == "") {
		fmt.Fprintln(os.Stderr, "usage: imcli -user alice -peer bob -password <pwd>  (or -token <access_token>)")
		os.Exit(1)
	}

	convID := conversationID(*user, *peer)
	userDir := filepath.Join(*dir, *user)
	c, err := openCache(userDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open cache:", err)
		os.Exit(1)
	}

	// 先渲染本地历史（秒开，不等网络），再走网络增量
	hist := c.history(convID)
	fmt.Printf("[cache] %d messages, watermark=%d\n", len(hist), c.watermark(convID))
	for _, m := range hist {
		fmt.Printf("[%s] %s\n", m.From, m.Content)
	}

	var ac *authClient
	accessToken := *token
	if accessToken == "" {
		ac = newAuthClient(*httpAddr, *user, *password, userDir)
		accessToken, err = ac.ensureAccessToken()
		if err != nil {
			fmt.Fprintln(os.Stderr, "auth:", err)
			os.Exit(1)
		}
	}

	lines := make(chan string)
	go scanLines(lines)

	// 重连独立 goroutine，主 goroutine 永远留在 stdin select 上：
	// 断线期间输入立即报错，不会被隐式排队到重连后发出。
	var cur atomic.Pointer[session]
	ctx, cancel := context.WithCancel(context.Background())
	go manageConn(ctx, &cur, *addr, accessToken, ac, c, convID, *user, *peer)

	for line := range lines {
		env := &imv1.Envelope{
			MsgId:       newID(),
			Type:        imv1.MsgType_MSG_TYPE_TEXT,
			From:        *user,
			To:          *peer,
			Content:     line,
			TimestampMs: time.Now().UnixMilli(),
		}
		s := cur.Load()
		if s == nil || !s.trySend(&imv1.Frame{Payload: &imv1.Frame_Message{Message: env}}) {
			fmt.Fprintln(os.Stderr, "[send] 连接不可用，消息未发送")
		}
	}
	if s := cur.Load(); s != nil {
		s.flush()
		s.close()
	}
	cancel()
}

// manageConn 维护"当前连接"：断线退避重连，重连成功先 SYNC 再发布给主 goroutine。
func manageConn(ctx context.Context, cur *atomic.Pointer[session], addr, token string, ac *authClient, c *cache, convID, user, peer string) {
	backoff := time.Second
	retried401 := false
	for ctx.Err() == nil {
		ws, resp, err := websocket.DefaultDialer.DialContext(ctx, addr+"?token="+url.QueryEscape(token), nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if resp != nil && resp.StatusCode == 401 && ac != nil && !retried401 {
				retried401 = true
				t, rerr := ac.ensureAccessToken()
				if rerr != nil {
					fmt.Fprintln(os.Stderr, "renew token:", rerr)
					os.Exit(1)
				}
				token = t
				continue
			}
			d := withJitter(backoff)
			fmt.Fprintf(os.Stderr, "[reconnect] dial failed: %v; retry in %v\n", err, d.Round(time.Millisecond))
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second
		retried401 = false
		fmt.Printf("[connected] %s -> %s\n", user, peer)

		sess := newSession(ws, c, convID, user, peer)
		sess.run()
		cur.Store(sess)
		select {
		case <-sess.done:
			cur.Store(nil)
			sess.close()
			fmt.Fprintln(os.Stderr, "[disconnected] 准备重连…")
		case <-ctx.Done():
			return // stdin EOF：主 goroutine 负责 flush + close
		}
	}
}

// conversationID 必须与服务端 logic.convID 完全一致：uid 排序后拼接。
func conversationID(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return "p:" + a + ":" + b
}

func scanLines(lines chan<- string) {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			lines <- line
		}
	}
	close(lines)
}

func nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > backoffMax {
		return backoffMax
	}
	return next
}

// withJitter 加 0~50% 随机抖动：大量客户端同时被断线时，重连时刻被打散，不会同一秒砸向 gateway。
func withJitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int64N(int64(d/2)+1))
}

// session 管理一条 WS 连接的生命周期。出站帧全部走 sendCh + writePump，
// 与服务端同款单写者模式：readLoop（触发 SYNC）和主 goroutine（发消息）不直接写 conn。
type session struct {
	ws        *websocket.Conn
	cache     *cache
	convID    string
	user      string
	peer      string
	sendCh    chan *imv1.Frame
	done      chan struct{}
	closeOnce sync.Once
}

func newSession(ws *websocket.Conn, c *cache, convID, user, peer string) *session {
	return &session{
		ws:     ws,
		cache:  c,
		convID: convID,
		user:   user,
		peer:   peer,
		sendCh: make(chan *imv1.Frame, 256),
		done:   make(chan struct{}),
	}
}

func (s *session) run() {
	s.ws.SetReadLimit(64 << 10)
	s.ws.SetReadDeadline(time.Now().Add(readTimeout))
	s.ws.SetPingHandler(func(appData string) error {
		s.ws.SetReadDeadline(time.Now().Add(readTimeout))
		return s.ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})
	go s.writePump()
	go s.readLoop()
	// 连接建立（含重连）后必须先 SYNC 补齐断线窗口，再进入正常收发
	s.requestSync(s.cache.watermark(s.convID))
}

func (s *session) stop() {
	s.closeOnce.Do(func() { close(s.done) })
}

// flush 在正常退出前等出站队列排空：stdin EOF 紧跟最后一条消息时，
// 不等待就直接 close 会让 writePump 在 select 里随机选中 done，最后一帧被丢弃。
func (s *session) flush() {
	for i := 0; i < 100 && len(s.sendCh) > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // 队列空不代表最后一帧写完，给在途写留一个写周期
}

func (s *session) close() {
	s.ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	s.stop()
	s.ws.Close()
}

func (s *session) trySend(f *imv1.Frame) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.sendCh <- f:
		return true
	case <-s.done:
		return false
	default: // 队列满：背压，丢弃并提示，不阻塞读循环
		return false
	}
}

func (s *session) requestSync(since uint64) {
	if !s.cache.beginSync(s.convID) {
		return
	}
	ok := s.trySend(&imv1.Frame{Payload: &imv1.Frame_SyncReq{SyncReq: &imv1.SyncRequest{
		ConvId: s.convID,
		Since:  since,
		Limit:  syncLimit,
	}}})
	if !ok {
		s.cache.endSync(s.convID)
	}
}

func (s *session) writePump() {
	for {
		select {
		case f := <-s.sendCh:
			data, err := proto.Marshal(f)
			if err != nil {
				fmt.Fprintln(os.Stderr, "marshal:", err)
				continue
			}
			s.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := s.ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
				s.stop()
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *session) readLoop() {
	for {
		_, data, err := s.ws.ReadMessage()
		if err != nil {
			s.stop()
			return
		}
		s.ws.SetReadDeadline(time.Now().Add(readTimeout))
		frame := &imv1.Frame{}
		if err := proto.Unmarshal(data, frame); err != nil {
			fmt.Fprintln(os.Stderr, "unmarshal:", err)
			continue
		}
		switch p := frame.Payload.(type) {
		case *imv1.Frame_Message:
			env := p.Message
			if env.Type == imv1.MsgType_MSG_TYPE_SYSTEM {
				fmt.Printf("\n[system] %s\n> ", env.Content)
				continue
			}
			m := StoredMsg{Seq: env.Seq, MsgID: env.MsgId, From: env.From, Content: env.Content, Ts: env.TimestampMs}
			committed, needSync := s.cache.applyPush(s.convID, m)
			render(committed)
			if needSync {
				s.requestSync(s.cache.watermark(s.convID))
			}
		case *imv1.Frame_SyncResp:
			msgs := make([]StoredMsg, 0, len(p.SyncResp.Messages))
			for _, env := range p.SyncResp.Messages {
				msgs = append(msgs, StoredMsg{Seq: env.Seq, MsgID: env.MsgId, From: env.From, Content: env.Content, Ts: env.TimestampMs})
			}
			committed := s.cache.applySync(s.convID, msgs)
			render(committed)
			wm := s.cache.watermark(s.convID)
			fmt.Printf("\n[sync] +%d new, watermark=%d, has_more=%v\n> ", len(committed), wm, p.SyncResp.HasMore)
			s.cache.endSync(s.convID)
			if p.SyncResp.HasMore || s.cache.hasPending(s.convID) {
				s.requestSync(wm)
			}
		}
	}
}

func render(msgs []StoredMsg) {
	for _, m := range msgs {
		fmt.Printf("\n[%s] %s\n> ", m.From, m.Content)
	}
}

func newID() string {
	b := make([]byte, 8)
	crand.Read(b)
	return hex.EncodeToString(b)
}