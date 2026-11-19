package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// 集成测试需要真实 MySQL：
// IM_TEST_MYSQL_DSN="root:imdev123@tcp(127.0.0.1:3306)/im?parseTime=true" go test ./internal/store/
//
// 隔离策略：每个测试独占一个 conv_id（t.Name()），只清理自己的数据。
// 不做 TRUNCATE——go test 并行跑多个包时，全局清库会互相破坏。
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("IM_TEST_MYSQL_DSN not set, skipping integration test")
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	conv := "test:" + t.Name()
	clean := func() {
		s.db.Exec("DELETE FROM messages WHERE conv_id = ?", conv)
	}
	clean()
	t.Cleanup(func() { clean(); s.Close() })
	return s, conv
}

func msg(convID string, seq int64, msgID string) *Message {
	return &Message{
		ConvID: convID, Seq: seq, MsgID: msgID,
		Sender: "alice", Content: "hi", CreatedAt: time.Now(),
	}
}

func TestInsertAndMaxSeq(t *testing.T) {
	s, conv := testStore(t)
	ctx := context.Background()

	if max, err := s.MaxSeq(ctx, conv); err != nil || max != 0 {
		t.Fatalf("empty conv: max=%d err=%v", max, err)
	}
	if err := s.InsertMessage(ctx, msg(conv, 1, "m1")); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertMessage(ctx, msg(conv, 2, "m2")); err != nil {
		t.Fatal(err)
	}
	if max, _ := s.MaxSeq(ctx, conv); max != 2 {
		t.Fatalf("max=%d, want 2", max)
	}
	if max, _ := s.MaxSeq(ctx, conv+":other"); max != 0 {
		t.Fatalf("other conv max=%d, want 0", max)
	}
}

func TestDuplicateMsgIDIsIdempotent(t *testing.T) {
	s, conv := testStore(t)
	ctx := context.Background()

	if err := s.InsertMessage(ctx, msg(conv, 1, "m1")); err != nil {
		t.Fatal(err)
	}
	// 客户端重试：同 msg_id、不同 seq（重试时重新领了号）
	err := s.InsertMessage(ctx, msg(conv, 2, "m1"))
	if err != ErrDuplicateMsg {
		t.Fatalf("want ErrDuplicateMsg, got %v", err)
	}
	got, err := s.GetByMsgID(ctx, conv, "m1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Seq != 1 {
		t.Fatalf("original seq should win, got %d", got.Seq)
	}
}

func TestDuplicateSeqRejected(t *testing.T) {
	s, conv := testStore(t)
	ctx := context.Background()

	if err := s.InsertMessage(ctx, msg(conv, 1, "m1")); err != nil {
		t.Fatal(err)
	}
	err := s.InsertMessage(ctx, msg(conv, 1, "m2"))
	if err != ErrDuplicateSeq {
		t.Fatalf("want ErrDuplicateSeq, got %v", err)
	}
}

func TestGetByMsgIDNotFound(t *testing.T) {
	s, conv := testStore(t)
	got, err := s.GetByMsgID(context.Background(), conv, "nope")
	if err != nil || got != nil {
		t.Fatalf("want (nil,nil), got (%v,%v)", got, err)
	}
}

func TestListMessagesRangeOrderLimit(t *testing.T) {
	s, conv := testStore(t)
	ctx := context.Background()

	// 故意留空洞：seq 1,3,5（2,4 是领号后落库失败的废号）
	for _, seq := range []int64{1, 3, 5} {
		if err := s.InsertMessage(ctx, msg(conv, seq, "m"+string(rune('0'+seq)))); err != nil {
			t.Fatal(err)
		}
	}
	// 别的会话的数据不能串进来（第二个会话也要纳入清理，否则 -count=2 残留冲突）
	other := conv + ":other"
	s.db.Exec("DELETE FROM messages WHERE conv_id = ?", other)
	t.Cleanup(func() { s.db.Exec("DELETE FROM messages WHERE conv_id = ?", other) })
	if err := s.InsertMessage(ctx, msg(other, 2, "x1")); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListMessages(ctx, conv, 0, 2)
	if err != nil || len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 3 {
		t.Fatalf("page1: %+v err=%v", got, err)
	}
	got, err = s.ListMessages(ctx, conv, 3, 10)
	if err != nil || len(got) != 1 || got[0].Seq != 5 {
		t.Fatalf("page2 should skip gap and return seq5: %+v err=%v", got, err)
	}
	got, err = s.ListMessages(ctx, conv, 5, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("caught up: want empty, got %+v err=%v", got, err)
	}
}