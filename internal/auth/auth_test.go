package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"im/internal/store"
)

var testSecret = []byte("test-secret")

func TestPasswordRoundTrip(t *testing.T) {
	h, err := HashPassword("s3cret!")
	if err != nil {
		t.Fatal(err)
	}
	if !CheckPassword(h, "s3cret!") {
		t.Fatal("correct password rejected")
	}
	if CheckPassword(h, "wrong") {
		t.Fatal("wrong password accepted")
	}
}

func TestJWTRoundTripAndRejections(t *testing.T) {
	tok, err := IssueAccessToken("alice", testSecret, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	uid, err := ParseAccessToken(tok, testSecret)
	if err != nil || uid != "alice" {
		t.Fatalf("parse: uid=%q err=%v", uid, err)
	}

	expired, _ := IssueAccessToken("alice", testSecret, -time.Minute)
	if _, err := ParseAccessToken(expired, testSecret); err == nil {
		t.Fatal("expired token accepted")
	}
	if _, err := ParseAccessToken(tok+"x", testSecret); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := ParseAccessToken(tok, []byte("other-secret")); err == nil {
		t.Fatal("wrong-secret token accepted")
	}
}

// fakeUserStore 内存版，保留 ConsumeRefreshToken 的原子消费语义。
type fakeUserStore struct {
	mu     sync.Mutex
	users  map[string]*store.User
	tokens map[string]*store.RefreshToken
}

func newFakeUserStore() *fakeUserStore {
	return &fakeUserStore{users: map[string]*store.User{}, tokens: map[string]*store.RefreshToken{}}
}

func (f *fakeUserStore) CreateUser(ctx context.Context, username, hash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.users[username]; ok {
		return store.ErrDuplicateUser
	}
	f.users[username] = &store.User{Username: username, PasswordHash: hash}
	return nil
}

func (f *fakeUserStore) GetUserByName(ctx context.Context, username string) (*store.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.users[username], nil
}

func (f *fakeUserStore) SaveRefreshToken(ctx context.Context, uid, hash string, exp time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens[hash] = &store.RefreshToken{UID: uid, TokenHash: hash, ExpiresAt: exp}
	return nil
}

func (f *fakeUserStore) ConsumeRefreshToken(ctx context.Context, hash string) (*store.RefreshToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rt := f.tokens[hash]
	if rt == nil {
		return nil, nil
	}
	delete(f.tokens, hash)
	if time.Now().After(rt.ExpiresAt) {
		return nil, nil
	}
	return rt, nil
}

func post(t *testing.T, h *Handlers, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func decodePair(t *testing.T, rec *httptest.ResponseRecorder) tokenPair {
	t.Helper()
	var p tokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return p
}

func TestRegisterLoginRefreshFlow(t *testing.T) {
	h := NewHandlers(newFakeUserStore(), testSecret)

	if rec := post(t, h, "/register", `{"username":"alice","password":"alice123"}`); rec.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(t, h, "/register", `{"username":"alice","password":"alice123"}`); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate register: want 409, got %d", rec.Code)
	}
	if rec := post(t, h, "/register", `{"username":"bob","password":"123"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("short password: want 400, got %d", rec.Code)
	}

	rec := post(t, h, "/login", `{"username":"alice","password":"alice123"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("login: %d %s", rec.Code, rec.Body.String())
	}
	p1 := decodePair(t, rec)
	if uid, err := ParseAccessToken(p1.AccessToken, testSecret); err != nil || uid != "alice" {
		t.Fatalf("access token: uid=%q err=%v", uid, err)
	}

	// refresh 旋转：旧 token 换新对，旧 token 立即作废
	rec = post(t, h, "/refresh", `{"refresh_token":"`+p1.RefreshToken+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh: %d %s", rec.Code, rec.Body.String())
	}
	p2 := decodePair(t, rec)
	if p2.RefreshToken == p1.RefreshToken {
		t.Fatal("refresh must rotate to a new token")
	}
	if rec := post(t, h, "/refresh", `{"refresh_token":"`+p1.RefreshToken+`"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("replayed old refresh: want 401, got %d", rec.Code)
	}
}

func TestLoginRejections(t *testing.T) {
	h := NewHandlers(newFakeUserStore(), testSecret)
	post(t, h, "/register", `{"username":"alice","password":"alice123"}`)

	for _, body := range []string{
		`{"username":"alice","password":"wrong"}`,
		`{"username":"ghost","password":"alice123"}`,
	} {
		if rec := post(t, h, "/login", body); rec.Code != http.StatusUnauthorized {
			t.Fatalf("login %s: want 401, got %d", body, rec.Code)
		}
	}
	if rec := post(t, h, "/refresh", `{"refresh_token":"nonexistent"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown refresh: want 401, got %d", rec.Code)
	}
}