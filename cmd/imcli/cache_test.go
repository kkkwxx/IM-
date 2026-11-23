package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func testCache(t *testing.T) *cache {
	t.Helper()
	c, err := openCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func msg(seq uint64, content string) StoredMsg {
	return StoredMsg{Seq: seq, MsgID: "m" + strconv.FormatUint(seq, 10), From: "alice", Content: content}
}

func TestPushContiguousAdvance(t *testing.T) {
	c := testCache(t)
	for i := uint64(1); i <= 3; i++ {
		committed, needSync := c.applyPush("p:a:b", msg(i, "x"))
		if needSync || len(committed) != 1 || committed[0].Seq != i {
			t.Fatalf("seq %d: committed=%v needSync=%v", i, committed, needSync)
		}
	}
	if wm := c.watermark("p:a:b"); wm != 3 {
		t.Fatalf("watermark = %d, want 3", wm)
	}
	if len(c.history("p:a:b")) != 3 {
		t.Fatal("history length != 3")
	}
}

func TestPushDuplicateDropped(t *testing.T) {
	c := testCache(t)
	c.applyPush("p:a:b", msg(1, "x"))
	c.applyPush("p:a:b", msg(2, "y"))
	committed, needSync := c.applyPush("p:a:b", msg(1, "x-retry"))
	if committed != nil || needSync {
		t.Fatalf("duplicate should be dropped, got %v needSync=%v", committed, needSync)
	}
	if len(c.history("p:a:b")) != 2 {
		t.Fatal("duplicate leaked into history")
	}
}

func TestPushHoleBufferedThenDrainedBySync(t *testing.T) {
	c := testCache(t)
	c.applyPush("p:a:b", msg(1, "one"))
	committed, needSync := c.applyPush("p:a:b", msg(3, "three"))
	if committed != nil || !needSync {
		t.Fatalf("hole push should buffer and request sync, got %v needSync=%v", committed, needSync)
	}
	if !c.hasPending("p:a:b") {
		t.Fatal("pending should hold seq 3")
	}
	if wm := c.watermark("p:a:b"); wm != 1 {
		t.Fatalf("watermark must not jump over hole, got %d", wm)
	}
	// SYNC 补回 seq 2，pending 里的 3 应被连续排水
	committed = c.applySync("p:a:b", []StoredMsg{msg(2, "two")})
	if len(committed) != 2 || committed[0].Seq != 2 || committed[1].Seq != 3 {
		t.Fatalf("drain should commit [2 3], got %v", committed)
	}
	if wm := c.watermark("p:a:b"); wm != 3 {
		t.Fatalf("watermark = %d, want 3", wm)
	}
	if c.hasPending("p:a:b") {
		t.Fatal("pending should be empty after drain")
	}
}

func TestPushHoleDuplicateBuffered(t *testing.T) {
	c := testCache(t)
	c.applyPush("p:a:b", msg(5, "five"))
	committed, needSync := c.applyPush("p:a:b", msg(5, "five-again"))
	if committed != nil || needSync {
		t.Fatal("duplicate pending push should be dropped without re-triggering sync")
	}
}

func TestSyncBatchAdvancesAndDedups(t *testing.T) {
	c := testCache(t)
	batch := []StoredMsg{msg(1, "a"), msg(2, "b"), msg(3, "c")}
	if got := c.applySync("p:a:b", batch); len(got) != 3 {
		t.Fatalf("first sync should commit 3, got %d", len(got))
	}
	if got := c.applySync("p:a:b", batch); len(got) != 0 {
		t.Fatalf("replayed sync should commit 0, got %d", len(got))
	}
	if wm := c.watermark("p:a:b"); wm != 3 {
		t.Fatalf("watermark = %d, want 3", wm)
	}
}

func TestSyncPermanentGapHarmless(t *testing.T) {
	c := testCache(t)
	// seq 1..4 是发号失败留下的永久空洞，范围查询直接从 5 开始
	if got := c.applySync("p:a:b", []StoredMsg{msg(5, "e"), msg(6, "f")}); len(got) != 2 {
		t.Fatalf("got %d committed", len(got))
	}
	if wm := c.watermark("p:a:b"); wm != 6 {
		t.Fatalf("watermark = %d, want 6 (coverage proof jumps gaps)", wm)
	}
}

func TestSyncCoversPendingWithoutDoubleCommit(t *testing.T) {
	c := testCache(t)
	c.applyPush("p:a:b", msg(2, "two")) // 洞：pending 持有 2
	committed := c.applySync("p:a:b", []StoredMsg{msg(1, "one"), msg(2, "two")})
	if len(committed) != 2 {
		t.Fatalf("batch should commit [1 2] once, got %v", committed)
	}
	if len(c.history("p:a:b")) != 2 {
		t.Fatal("seq 2 committed twice")
	}
}

func TestAtomicSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	c, err := openCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.applyPush("p:alice:bob", msg(1, "hello"))
	c.applyPush("p:alice:bob", msg(2, "world"))

	// Windows 文件名不允许冒号，必须已替换
	if _, err := os.Stat(filepath.Join(dir, "p_alice_bob.json")); err != nil {
		t.Fatalf("sanitized file missing: %v", err)
	}
	// 崩溃残留的 tmp 文件不影响加载
	if err := os.WriteFile(filepath.Join(dir, "p_alice_bob.json.tmp"), []byte("{torn"), 0o600); err != nil {
		t.Fatal(err)
	}

	c2, err := openCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if wm := c2.watermark("p:alice:bob"); wm != 2 {
		t.Fatalf("reloaded watermark = %d, want 2", wm)
	}
	hist := c2.history("p:alice:bob")
	if len(hist) != 2 || hist[0].Content != "hello" || hist[1].Content != "world" {
		t.Fatalf("reloaded history = %v", hist)
	}
}

func TestCorruptCacheFileStartsFresh(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "p_a_b.json"), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := openCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if wm := c.watermark("p:a:b"); wm != 0 {
		t.Fatalf("corrupt file should start from 0, got %d", wm)
	}
	c.applyPush("p:a:b", msg(1, "x")) // 应能覆盖损坏文件
	c2, _ := openCache(dir)
	if wm := c2.watermark("p:a:b"); wm != 1 {
		t.Fatalf("watermark after overwrite = %d, want 1", wm)
	}
}

func TestSyncInFlightGuard(t *testing.T) {
	c := testCache(t)
	if !c.beginSync("p:a:b") {
		t.Fatal("first beginSync should win")
	}
	if c.beginSync("p:a:b") {
		t.Fatal("second beginSync must be rejected while in flight")
	}
	c.endSync("p:a:b")
	if !c.beginSync("p:a:b") {
		t.Fatal("beginSync after endSync should win")
	}
}

func TestConversationIDMatchesServer(t *testing.T) {
	if got := conversationID("bob", "alice"); got != "p:alice:bob" {
		t.Fatalf("conversationID = %q, want p:alice:bob", got)
	}
	if got := conversationID("alice", "bob"); got != "p:alice:bob" {
		t.Fatalf("conversationID not symmetric: %q", got)
	}
}

func TestBackoffCapsAndJitterBounds(t *testing.T) {
	d := time.Second
	for i := 0; i < 10; i++ {
		d = nextBackoff(d)
	}
	if d != backoffMax {
		t.Fatalf("backoff = %v, want cap %v", d, backoffMax)
	}
	if nextBackoff(backoffMax) != backoffMax {
		t.Fatal("backoff must stay at cap")
	}
	for i := 0; i < 200; i++ {
		base := 4 * time.Second
		got := withJitter(base)
		if got < base || got > base+base/2 {
			t.Fatalf("jitter out of [1x,1.5x]: %v", got)
		}
	}
}