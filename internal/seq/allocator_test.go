package seq

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"im/internal/store"
)

// 集成测试需要真实 MySQL + Redis：
// IM_TEST_MYSQL_DSN=... IM_TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/seq/
//
// 隔离策略：每个测试独占 conv_id，只清理自己的行和键。
// 不做 FlushDB/TRUNCATE——go test 并行跑多个包时会互相破坏，
// 且 FlushDB 会误伤共享 Redis 里的其他数据。
func testAllocator(t *testing.T) (*Allocator, *redis.Client, *store.Store, string) {
	t.Helper()
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	addr := os.Getenv("IM_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("IM_TEST_MYSQL_DSN / IM_TEST_REDIS_ADDR not set, skipping integration test")
	}
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	conv := "test:" + t.Name()
	key := "seq:" + conv
	clean := func() {
		rdb.Del(context.Background(), key)
		st.DB().Exec("DELETE FROM messages WHERE conv_id = ?", conv)
	}
	clean()
	t.Cleanup(func() { clean(); rdb.Close(); st.Close() })
	return NewAllocator(rdb, st), rdb, st, conv
}

func insert(t *testing.T, st *store.Store, conv string, seq int64) {
	t.Helper()
	m := &store.Message{ConvID: conv, Seq: seq, MsgID: fmt.Sprintf("m%d", seq),
		Sender: "alice", Content: "hi", CreatedAt: time.Now()}
	if err := st.InsertMessage(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func TestAllocSequential(t *testing.T) {
	a, _, _, conv := testAllocator(t)
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		n, err := a.Alloc(ctx, conv)
		if err != nil || n != i {
			t.Fatalf("alloc=%d err=%v, want %d", n, err, i)
		}
	}
}

func TestAllocConcurrentUniqueAndDense(t *testing.T) {
	a, _, _, conv := testAllocator(t)
	ctx := context.Background()
	const n = 100

	var wg sync.WaitGroup
	nums := make([]int64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nums[i], errs[i] = a.Alloc(ctx, conv)
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	for i, v := range nums {
		if v != int64(i+1) {
			t.Fatalf("seq set not dense 1..%d: got %v", n, nums)
		}
	}
}

// 核心恢复测试：Redis 键丢失后，发号器必须从 DB MAX(seq) 恢复，不能回退。
// 用 DEL 单键模拟丢数据（等价于重启丢失该键，且不伤及其他数据）。
func TestRecoverAfterRedisLoss(t *testing.T) {
	a, rdb, st, conv := testAllocator(t)
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		n, err := a.Alloc(ctx, conv)
		if err != nil {
			t.Fatal(err)
		}
		insert(t, st, conv, n)
	}

	if err := rdb.Del(ctx, "seq:"+conv).Err(); err != nil {
		t.Fatal(err)
	}
	n, err := a.Alloc(ctx, conv)
	if err != nil || n != 6 {
		t.Fatalf("after redis loss: alloc=%d err=%v, want 6", n, err)
	}
}

// 空洞无害：领了号没落库（模拟崩溃），恢复后从 DB 实际水位继续，空洞被跳过。
func TestGapAfterCrashIsHarmless(t *testing.T) {
	a, rdb, st, conv := testAllocator(t)
	ctx := context.Background()

	// 领 1..3，但只落库 1 和 3（2 是崩溃留下的空洞）
	for i := int64(1); i <= 3; i++ {
		if _, err := a.Alloc(ctx, conv); err != nil {
			t.Fatal(err)
		}
	}
	insert(t, st, conv, 1)
	insert(t, st, conv, 3)

	if err := rdb.Del(ctx, "seq:"+conv).Err(); err != nil {
		t.Fatal(err)
	}
	n, err := a.Alloc(ctx, conv)
	if err != nil || n != 4 {
		t.Fatalf("alloc=%d err=%v, want 4 (continue from db max=3)", n, err)
	}
}