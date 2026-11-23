package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// 隔离策略同 store_test：每测试独占 username/uid，只清理自己的数据。
func testUserStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("IM_TEST_MYSQL_DSN not set, skipping integration test")
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	uid := "t_" + t.Name()
	clean := func() {
		s.db.Exec("DELETE FROM users WHERE username = ?", uid)
		s.db.Exec("DELETE FROM refresh_tokens WHERE uid = ?", uid)
	}
	clean()
	t.Cleanup(func() { clean(); s.Close() })
	return s, uid
}

func TestCreateUserDuplicate(t *testing.T) {
	s, uid := testUserStore(t)
	ctx := context.Background()

	if err := s.CreateUser(ctx, uid, "hash"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateUser(ctx, uid, "hash"); err != ErrDuplicateUser {
		t.Fatalf("want ErrDuplicateUser, got %v", err)
	}
	u, err := s.GetUserByName(ctx, uid)
	if err != nil || u == nil || u.PasswordHash != "hash" {
		t.Fatalf("get: %+v err=%v", u, err)
	}
	if u, err := s.GetUserByName(ctx, uid+"_ghost"); err != nil || u != nil {
		t.Fatalf("ghost: want (nil,nil), got (%+v,%v)", u, err)
	}
}

func TestRefreshTokenRotation(t *testing.T) {
	s, uid := testUserStore(t)
	ctx := context.Background()

	if err := s.SaveRefreshToken(ctx, uid, "hash1", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	rt, err := s.ConsumeRefreshToken(ctx, "hash1")
	if err != nil || rt == nil || rt.UID != uid {
		t.Fatalf("first consume: %+v err=%v", rt, err)
	}
	// 旋转后旧 token 立即作废
	if rt, err := s.ConsumeRefreshToken(ctx, "hash1"); err != nil || rt != nil {
		t.Fatalf("replay: want (nil,nil), got (%+v,%v)", rt, err)
	}
}

func TestRefreshTokenExpired(t *testing.T) {
	s, uid := testUserStore(t)
	ctx := context.Background()

	if err := s.SaveRefreshToken(ctx, uid, "hash-exp", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if rt, err := s.ConsumeRefreshToken(ctx, "hash-exp"); err != nil || rt != nil {
		t.Fatalf("expired: want (nil,nil), got (%+v,%v)", rt, err)
	}
}

// 并发重放：8 个请求同时消费同一 token，事务行锁保证只有一个成功。
func TestConcurrentRefreshReplay(t *testing.T) {
	s, uid := testUserStore(t)
	ctx := context.Background()

	if err := s.SaveRefreshToken(ctx, uid, "hash-race", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt, err := s.ConsumeRefreshToken(ctx, "hash-race")
			if err != nil {
				t.Errorf("consume: %v", err)
				return
			}
			if rt != nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("exactly one consume must win, got %d", wins)
	}
}