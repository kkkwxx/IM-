package auth

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"im/internal/store"
)

// userStore 是 auth 对存储层的最小依赖。
type userStore interface {
	CreateUser(ctx context.Context, username, passwordHash string) error
	GetUserByName(ctx context.Context, username string) (*store.User, error)
	SaveRefreshToken(ctx context.Context, uid, tokenHash string, expiresAt time.Time) error
	ConsumeRefreshToken(ctx context.Context, tokenHash string) (*store.RefreshToken, error)
}

type Handlers struct {
	store  userStore
	secret []byte
}

func NewHandlers(st userStore, secret []byte) *Handlers {
	return &Handlers{store: st, secret: secret}
}

func (h *Handlers) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", h.register)
	mux.HandleFunc("POST /login", h.login)
	mux.HandleFunc("POST /refresh", h.refresh)
	return mux
}

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type tokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decodeCreds(w http.ResponseWriter, r *http.Request) (credentials, bool) {
	var c credentials
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return c, false
	}
	return c, true
}

func (h *Handlers) register(w http.ResponseWriter, r *http.Request) {
	c, ok := decodeCreds(w, r)
	if !ok {
		return
	}
	if c.Username == "" || len(c.Username) > 32 || len(c.Password) < 6 {
		writeErr(w, http.StatusBadRequest, "username 1-32 chars, password >= 6 chars")
		return
	}
	hash, err := HashPassword(c.Password)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "hash password")
		return
	}
	if err := h.store.CreateUser(r.Context(), c.Username, hash); err != nil {
		if errors.Is(err, store.ErrDuplicateUser) {
			writeErr(w, http.StatusConflict, "username already exists")
			return
		}
		slog.Error("create user", "err", err)
		writeErr(w, http.StatusInternalServerError, "create user")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"uid": c.Username})
}

func (h *Handlers) login(w http.ResponseWriter, r *http.Request) {
	c, ok := decodeCreds(w, r)
	if !ok {
		return
	}
	u, err := h.store.GetUserByName(r.Context(), c.Username)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "query user")
		return
	}
	// 用户不存在与密码错误返回同一文案，不泄露用户名是否存在
	if u == nil || !CheckPassword(u.PasswordHash, c.Password) {
		writeErr(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	pair, err := h.issuePair(r.Context(), u.Username)
	if err != nil {
		slog.Error("issue token pair", "err", err)
		writeErr(w, http.StatusInternalServerError, "issue tokens")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

func (h *Handlers) refresh(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RefreshToken string `json:"refresh_token"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RefreshToken == "" {
		writeErr(w, http.StatusBadRequest, "refresh_token required")
		return
	}
	// 原子消费：旧 token 立即作废（rotation），并发重放只有一个能成功
	rt, err := h.store.ConsumeRefreshToken(r.Context(), HashToken(body.RefreshToken))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "consume refresh token")
		return
	}
	if rt == nil {
		writeErr(w, http.StatusUnauthorized, "refresh token invalid or expired")
		return
	}
	pair, err := h.issuePair(r.Context(), rt.UID)
	if err != nil {
		slog.Error("issue token pair", "err", err)
		writeErr(w, http.StatusInternalServerError, "issue tokens")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

func (h *Handlers) issuePair(ctx context.Context, uid string) (*tokenPair, error) {
	access, err := IssueAccessToken(uid, h.secret, AccessTTL)
	if err != nil {
		return nil, err
	}
	refresh, refreshHash, err := NewRefreshToken()
	if err != nil {
		return nil, err
	}
	if err := h.store.SaveRefreshToken(ctx, uid, refreshHash, time.Now().Add(RefreshTTL)); err != nil {
		return nil, err
	}
	return &tokenPair{
		AccessToken:  access,
		RefreshToken: refresh,
		ExpiresIn:    int64(AccessTTL.Seconds()),
	}, nil
}