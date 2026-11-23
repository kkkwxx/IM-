package route

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 集成测试需要真实 Redis：
// IM_TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/route/
//
// 隔离策略：每个测试独占 uid（route:rt_<TestName>），只清理自己的键，不做 FlushDB。
func testTable(t *testing.T) (*Table, *redis.Client, string) {
	t.Helper()
	addr := os.Getenv("IM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IM_TEST_REDIS_ADDR not set, skipping integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	uid := "rt_" + t.Name()
	clean := func() { rdb.Del(context.Background(), key(uid)) }
	clean()
	t.Cleanup(func() { clean(); rdb.Close() })
	return New(rdb, DefaultTTL), rdb, uid
}

func lookupJoined(t *testing.T, tb *Table, uid string) string {
	t.Helper()
	gws, err := tb.Lookup(context.Background(), uid)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(gws, ",")
}

func TestRegisterLookup(t *testing.T) {
	tb, _, uid := testTable(t)
	ctx := context.Background()

	if got := lookupJoined(t, tb, uid); got != "" {
		t.Fatalf("unknown uid should have no routes, got %q", got)
	}
	if err := tb.Register(ctx, uid, "c1", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if got := lookupJoined(t, tb, uid); got != "gw-1" {
		t.Fatalf("got %q, want gw-1", got)
	}
}

func TestMultiDeviceDedupAndSort(t *testing.T) {
	tb, _, uid := testTable(t)
	ctx := context.Background()

	// 三台设备分布在两台 gateway：同 gw 去重，结果排序稳定
	for _, r := range []struct{ conn, gw string }{{"c1", "gw-2"}, {"c2", "gw-1"}, {"c3", "gw-2"}} {
		if err := tb.Register(ctx, uid, r.conn, r.gw); err != nil {
			t.Fatal(err)
		}
	}
	if got := lookupJoined(t, tb, uid); got != "gw-1,gw-2" {
		t.Fatalf("got %q, want gw-1,gw-2", got)
	}
}

func TestRemoveOnlyOwnField(t *testing.T) {
	tb, _, uid := testTable(t)
	ctx := context.Background()

	if err := tb.Register(ctx, uid, "c1", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if err := tb.Register(ctx, uid, "c2", "gw-2"); err != nil {
		t.Fatal(err)
	}
	// c1 断开只删自己的 field，c2（另一台设备/另一台 gateway）不受影响
	if err := tb.Remove(ctx, uid, "c1"); err != nil {
		t.Fatal(err)
	}
	if got := lookupJoined(t, tb, uid); got != "gw-2" {
		t.Fatalf("after c1 removal got %q, want gw-2", got)
	}
	if err := tb.Remove(ctx, uid, "c2"); err != nil {
		t.Fatal(err)
	}
	if got := lookupJoined(t, tb, uid); got != "" {
		t.Fatalf("after all removals got %q, want empty", got)
	}
}

// gateway 崩溃残留：field 时间戳超过 TTL → 读时过滤，不需要后台清道夫。
func TestStaleAndMalformedFieldsFiltered(t *testing.T) {
	tb, rdb, uid := testTable(t)
	ctx := context.Background()

	stale := time.Now().Add(-DefaultTTL - time.Second).UnixMilli()
	if err := rdb.HSet(ctx, key(uid),
		"dead", "gw-dead|"+strconv.FormatInt(stale, 10),
		"garbage", "no-timestamp-separator",
		"badts", "gw-bad|not-a-number",
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := tb.Register(ctx, uid, "alive", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if got := lookupJoined(t, tb, uid); got != "gw-1" {
		t.Fatalf("stale/malformed fields must be filtered, got %q", got)
	}
}

func TestRenewRefreshesTimestampAndTTL(t *testing.T) {
	tb, rdb, uid := testTable(t)
	ctx := context.Background()

	if err := tb.Register(ctx, uid, "c1", "gw-1"); err != nil {
		t.Fatal(err)
	}
	tsOf := func() int64 {
		val, err := rdb.HGet(ctx, key(uid), "c1").Result()
		if err != nil {
			t.Fatal(err)
		}
		i := strings.LastIndex(val, "|")
		ts, err := strconv.ParseInt(val[i+1:], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}
	first := tsOf()
	time.Sleep(5 * time.Millisecond)
	if err := tb.Renew(ctx, uid, "c1", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if second := tsOf(); second <= first {
		t.Fatalf("renew must refresh timestamp: first=%d second=%d", first, second)
	}
	ttl, err := rdb.TTL(ctx, key(uid)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DefaultTTL {
		t.Fatalf("key TTL must be in (0, %v], got %v", DefaultTTL, ttl)
	}
}

// 并发登记/续期/删除各自 field：connID 唯一使互不干扰，最终全部清空。
func TestConcurrentRegisterRenewRemove(t *testing.T) {
	tb, _, uid := testTable(t)
	ctx := context.Background()

	const n = 16
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connID := "c" + strconv.Itoa(i)
			gw := "gw-" + strconv.Itoa(i%3)
			if err := tb.Register(ctx, uid, connID, gw); err != nil {
				t.Errorf("register %s: %v", connID, err)
				return
			}
			if err := tb.Renew(ctx, uid, connID, gw); err != nil {
				t.Errorf("renew %s: %v", connID, err)
			}
			if err := tb.Remove(ctx, uid, connID); err != nil {
				t.Errorf("remove %s: %v", connID, err)
			}
		}(i)
	}
	wg.Wait()
	if got := lookupJoined(t, tb, uid); got != "" {
		t.Fatalf("all fields removed, got %q", got)
	}
}