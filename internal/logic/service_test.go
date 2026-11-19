package logic

import (
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"im/api/imv1"
	"im/internal/store"
)

type fakeGateway struct {
	imv1.UnimplementedGatewayServer
	mu        sync.Mutex
	got       []*imv1.PushRequest
	delivered bool
	pushErr   error
}

func (f *fakeGateway) Push(ctx context.Context, req *imv1.PushRequest) (*imv1.PushResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, req)
	if f.pushErr != nil {
		return nil, f.pushErr
	}
	return &imv1.PushResponse{Delivered: f.delivered}, nil
}

func (f *fakeGateway) pushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

// fakeStore 内存版存储，保留两个唯一索引的冲突语义，可注入任意一步失败。
type fakeStore struct {
	mu        sync.Mutex
	byMsg     map[string]*store.Message // conv|msg_id
	bySeq     map[string]bool           // conv|seq
	getErr    error
	insertErr error
	listErr   error
	lastLimit int
}

func newFakeStore() *fakeStore {
	return &fakeStore{byMsg: map[string]*store.Message{}, bySeq: map[string]bool{}}
}

func (f *fakeStore) GetByMsgID(ctx context.Context, convID, msgID string) (*store.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.byMsg[convID+"|"+msgID], nil
}

func (f *fakeStore) InsertMessage(ctx context.Context, m *store.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return f.insertErr
	}
	if _, ok := f.byMsg[m.ConvID+"|"+m.MsgID]; ok {
		return store.ErrDuplicateMsg
	}
	if f.bySeq[m.ConvID+"|"+strconv.FormatInt(m.Seq, 10)] {
		return store.ErrDuplicateSeq
	}
	f.byMsg[m.ConvID+"|"+m.MsgID] = m
	f.bySeq[m.ConvID+"|"+strconv.FormatInt(m.Seq, 10)] = true
	return nil
}

func (f *fakeStore) ListMessages(ctx context.Context, convID string, since int64, limit int) ([]store.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLimit = limit
	if f.listErr != nil {
		return nil, f.listErr
	}
	var ms []store.Message
	for _, m := range f.byMsg {
		if m.ConvID == convID && m.Seq > since {
			ms = append(ms, *m)
		}
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Seq < ms[j].Seq })
	if len(ms) > limit {
		ms = ms[:limit]
	}
	return ms, nil
}

// fakeAlloc 按会话单调发号，可注入失败。
type fakeAlloc struct {
	mu      sync.Mutex
	counter map[string]int64
	err     error
	calls   int
}

func newFakeAlloc() *fakeAlloc { return &fakeAlloc{counter: map[string]int64{}} }

func (f *fakeAlloc) Alloc(ctx context.Context, convID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	f.counter[convID]++
	return f.counter[convID], nil
}

// newTestService 用 bufconn 起一个内存 gRPC gateway，返回直连它的 logic Service。
func newTestService(t *testing.T, gw *fakeGateway, st messageStore, alloc seqAllocator) *Service {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	imv1.RegisterGatewayServer(gs, gw)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return NewService(imv1.NewGatewayClient(conn), st, alloc)
}

func textEnv(to string) *imv1.Envelope {
	return &imv1.Envelope{MsgId: "m1", Type: imv1.MsgType_MSG_TYPE_TEXT, From: "alice", To: to, Content: "hi"}
}

func send(t *testing.T, svc *Service, env *imv1.Envelope) (*imv1.SendMessageResponse, error) {
	t.Helper()
	return svc.SendMessage(context.Background(), &imv1.SendMessageRequest{Envelope: env})
}

func TestSendMessageDelivered(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	resp, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Delivered || resp.MsgId != "m1" || resp.Seq != 1 {
		t.Fatalf("got %+v", resp)
	}
	if gw.pushCount() != 1 {
		t.Fatalf("want 1 push, got %d", gw.pushCount())
	}
	gw.mu.Lock()
	pushed := gw.got[0]
	gw.mu.Unlock()
	if pushed.UserId != "bob" || pushed.Envelope.Seq != 1 || pushed.Envelope.TimestampMs == 0 {
		t.Fatalf("pushed envelope wrong: %+v", pushed)
	}
	if m := st.byMsg["p:alice:bob|m1"]; m == nil || m.Seq != 1 || m.Sender != "alice" {
		t.Fatalf("persisted message wrong: %+v", m)
	}
}

func TestConvIDSymmetric(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	if _, err := send(t, svc, textEnv("bob")); err != nil {
		t.Fatal(err)
	}
	reply := &imv1.Envelope{MsgId: "m2", Type: imv1.MsgType_MSG_TYPE_TEXT, From: "bob", To: "alice", Content: "yo"}
	resp, err := send(t, svc, reply)
	if err != nil {
		t.Fatal(err)
	}
	// 双向消息必须落同一会话、共享同一 seq 序列
	if resp.Seq != 2 || st.byMsg["p:alice:bob|m2"] == nil {
		t.Fatalf("reply should share conversation, resp=%+v", resp)
	}
}

func TestSendMessageOffline(t *testing.T) {
	gw := &fakeGateway{delivered: false}
	svc := newTestService(t, gw, newFakeStore(), newFakeAlloc())

	resp, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Delivered || resp.Seq != 1 {
		t.Fatalf("offline: stored with seq but delivered=false, got %+v", resp)
	}
}

func TestPushFailureIsNotSendFailure(t *testing.T) {
	gw := &fakeGateway{pushErr: status.Error(codes.Unavailable, "gateway down")}
	st := newFakeStore()
	svc := newTestService(t, gw, st, newFakeAlloc())

	resp, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatalf("push channel failure must not fail the send: %v", err)
	}
	if resp.Delivered || resp.Seq != 1 || st.byMsg["p:alice:bob|m1"] == nil {
		t.Fatalf("message must be persisted despite push failure, resp=%+v", resp)
	}
}

func TestInvalidEnvelope(t *testing.T) {
	svc := newTestService(t, &fakeGateway{delivered: true}, newFakeStore(), newFakeAlloc())
	for _, env := range []*imv1.Envelope{
		{Type: imv1.MsgType_MSG_TYPE_TEXT, From: "alice"},                    // 缺 to
		{Type: imv1.MsgType_MSG_TYPE_TEXT, From: "alice", To: "bob"},         // 缺 msg_id
		{MsgId: "m1", Type: imv1.MsgType_MSG_TYPE_SYSTEM, From: "a", To: "b"}, // 非法类型
	} {
		_, err := send(t, svc, env)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("want InvalidArgument for %+v, got %v", env, err)
		}
	}
}

// 思考题场景：领号成功但落库失败 → 重试领新号，旧号成空洞，不重号。
func TestRetryAfterPersistFailureGetsNewSeq(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	st.insertErr = errors.New("db down")
	if _, err := send(t, svc, textEnv("bob")); status.Code(err) != codes.Internal {
		t.Fatalf("persist failure must fail the send, got %v", err)
	}
	if gw.pushCount() != 0 {
		t.Fatal("must NOT push when persist failed")
	}

	st.insertErr = nil
	resp, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seq != 2 {
		t.Fatalf("retry should get fresh seq 2 (1 is a harmless gap), got %d", resp.Seq)
	}
	if st.bySeq["p:alice:bob|1"] {
		t.Fatal("seq 1 must remain unoccupied")
	}
}

// 落库成功但响应丢失 → 重试幂等返回原 seq，不领新号、不重复推送。
func TestRetryAfterSuccessIsIdempotent(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	r1, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := send(t, svc, textEnv("bob"))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Seq != r2.Seq || alloc.calls != 1 || gw.pushCount() != 1 || len(st.byMsg) != 1 {
		t.Fatalf("retry must be idempotent: r1=%+v r2=%+v allocCalls=%d pushes=%d rows=%d",
			r1, r2, alloc.calls, gw.pushCount(), len(st.byMsg))
	}
}

// 并发重试竞态：查重落空后另一请求先插入，INSERT 撞 uk_conv_msg → 返回已有 seq。
func TestConcurrentSameMsgIDOneSeq(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	const n = 8
	seqs := make([]uint64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := send(t, svc, textEnv("bob"))
			if err != nil {
				errs[i] = err
				return
			}
			seqs[i] = resp.Seq
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("send %d failed: %v", i, err)
		}
		if seqs[i] != seqs[0] {
			t.Fatalf("all retries must converge to one seq, got %v", seqs)
		}
	}
	if len(st.byMsg) != 1 || gw.pushCount() != 1 {
		t.Fatalf("one row one push, got rows=%d pushes=%d", len(st.byMsg), gw.pushCount())
	}
}

func TestDedupQueryFailureFailsSend(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	st.getErr = errors.New("db down")
	svc := newTestService(t, gw, st, alloc)

	if _, err := send(t, svc, textEnv("bob")); status.Code(err) != codes.Internal {
		t.Fatalf("want Internal, got %v", err)
	}
	if alloc.calls != 0 || gw.pushCount() != 0 {
		t.Fatal("must not alloc or push when dedup query fails")
	}
}

func TestAllocFailureFailsSend(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	alloc.err = errors.New("redis down")
	svc := newTestService(t, gw, st, alloc)

	if _, err := send(t, svc, textEnv("bob")); status.Code(err) != codes.Internal {
		t.Fatalf("want Internal, got %v", err)
	}
	if len(st.byMsg) != 0 || gw.pushCount() != 0 {
		t.Fatal("must not persist or push when alloc fails")
	}
}

// 防线场景：发号器异常发出已占用的 seq → uk_conv_seq 拒绝，告警且不推送。
func TestSeqCollisionDefense(t *testing.T) {
	gw := &fakeGateway{delivered: true}
	st, alloc := newFakeStore(), newFakeAlloc()
	svc := newTestService(t, gw, st, alloc)

	other := &imv1.Envelope{MsgId: "other", Type: imv1.MsgType_MSG_TYPE_TEXT, From: "alice", To: "bob", Content: "first"}
	if _, err := send(t, svc, other); err != nil {
		t.Fatal(err)
	}
	// 模拟发号器回退：再次发出 1
	alloc.mu.Lock()
	alloc.counter["p:alice:bob"] = 0
	alloc.mu.Unlock()

	if _, err := send(t, svc, textEnv("bob")); status.Code(err) != codes.Internal {
		t.Fatalf("seq collision must fail the send, got %v", err)
	}
	if gw.pushCount() != 1 {
		t.Fatalf("colliding message must not be pushed, pushes=%d", gw.pushCount())
	}
}

func seedMessages(t *testing.T, st *fakeStore, conv string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		sender := "alice"
		if i%2 == 0 {
			sender = "bob"
		}
		m := &store.Message{
			ConvID: conv, Seq: int64(i), MsgID: "m" + strconv.Itoa(i),
			Sender: sender, Content: "hi", CreatedAt: time.Now(),
		}
		if err := st.InsertMessage(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSyncRejectsNonMember(t *testing.T) {
	svc := newTestService(t, &fakeGateway{}, newFakeStore(), newFakeAlloc())
	for _, req := range []*imv1.SyncRequest{
		{ConvId: "p:alice:bob", UserId: "mallory"}, // 越权：不是会话成员
		{ConvId: "p:alice:bob"},                    // 缺身份
		{ConvId: "g:123", UserId: "alice"},         // M4 群聊格式，暂不支持
		{ConvId: "garbage", UserId: "alice"},
	} {
		_, err := svc.Sync(context.Background(), req)
		if status.Code(err) != codes.PermissionDenied && status.Code(err) != codes.InvalidArgument {
			t.Fatalf("req %+v: want PermissionDenied/InvalidArgument, got %v", req, err)
		}
	}
}

func TestSyncPaginationAndHasMore(t *testing.T) {
	st := newFakeStore()
	seedMessages(t, st, "p:alice:bob", 6)
	svc := newTestService(t, &fakeGateway{}, st, newFakeAlloc())

	r1, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "bob", Since: 0, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Messages) != 3 || !r1.HasMore || r1.Messages[0].Seq != 1 || r1.Messages[2].Seq != 3 {
		t.Fatalf("page1 wrong: %+v", r1)
	}
	// 会话成员视角：alice 发的消息 To=bob，bob 发的消息 To=alice
	if r1.Messages[0].From != "alice" || r1.Messages[0].To != "bob" || r1.Messages[1].To != "alice" {
		t.Fatalf("envelope conversion wrong: %+v", r1.Messages)
	}

	r2, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "bob", Since: 3, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Messages) != 3 || r2.HasMore || r2.Messages[2].Seq != 6 {
		t.Fatalf("page2 wrong: %+v", r2)
	}

	r3, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "bob", Since: 6, Limit: 3})
	if err != nil || len(r3.Messages) != 0 || r3.HasMore {
		t.Fatalf("caught up: %+v err=%v", r3, err)
	}
}

func TestSyncLimitClamped(t *testing.T) {
	st := newFakeStore()
	svc := newTestService(t, &fakeGateway{}, st, newFakeAlloc())

	if _, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "alice"}); err != nil {
		t.Fatal(err)
	}
	if st.lastLimit != defaultSyncLimit+1 {
		t.Fatalf("default limit: query limit=%d, want %d", st.lastLimit, defaultSyncLimit+1)
	}
	if _, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "alice", Limit: 99999}); err != nil {
		t.Fatal(err)
	}
	if st.lastLimit != maxSyncLimit+1 {
		t.Fatalf("clamped limit: query limit=%d, want %d", st.lastLimit, maxSyncLimit+1)
	}
}

func TestSyncQueryFailure(t *testing.T) {
	st := newFakeStore()
	st.listErr = errors.New("db down")
	svc := newTestService(t, &fakeGateway{}, st, newFakeAlloc())
	_, err := svc.Sync(context.Background(), &imv1.SyncRequest{ConvId: "p:alice:bob", UserId: "alice"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal, got %v", err)
	}
}