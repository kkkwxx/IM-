package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var errRefreshInvalid = errors.New("refresh token invalid or expired")

type tokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// authClient 负责 access token 的获取与续期；refresh_token 单独一个文件，
// 与会话缓存分开，避免清缓存时把登录态一起清掉。
type authClient struct {
	httpAddr string
	username string
	password string
	dir      string
	client   *http.Client
}

func newAuthClient(httpAddr, username, password, dir string) *authClient {
	return &authClient{
		httpAddr: strings.TrimRight(httpAddr, "/"),
		username: username,
		password: password,
		dir:      dir,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *authClient) refreshPath() string {
	return filepath.Join(a.dir, "refresh_token")
}

func (a *authClient) loadRefreshToken() string {
	data, err := os.ReadFile(a.refreshPath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func (a *authClient) saveRefreshToken(tok string) {
	tmp := a.refreshPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(tok), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "[auth] write tmp:", err)
		return
	}
	if err := os.Rename(tmp, a.refreshPath()); err != nil {
		fmt.Fprintln(os.Stderr, "[auth] rename:", err)
	}
}

func (a *authClient) post(path string, body any) (*tokenPair, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.Post(a.httpAddr+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, errRefreshInvalid
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: http %d", path, resp.StatusCode)
	}
	var pair tokenPair
	if err := json.NewDecoder(resp.Body).Decode(&pair); err != nil {
		return nil, err
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		return nil, fmt.Errorf("%s: empty token pair", path)
	}
	return &pair, nil
}

func (a *authClient) login() (*tokenPair, error) {
	return a.post("/login", map[string]string{"username": a.username, "password": a.password})
}

func (a *authClient) refresh(tok string) (*tokenPair, error) {
	return a.post("/refresh", map[string]string{"refresh_token": tok})
}

// ensureAccessToken 优先用本地 refresh token 续期（rotation 后立刻保存新 token），
// refresh 失效才回落到密码登录。网络错误直接返回，不浪费一次有效的 refresh token。
func (a *authClient) ensureAccessToken() (string, error) {
	if rt := a.loadRefreshToken(); rt != "" {
		pair, err := a.refresh(rt)
		if err == nil {
			a.saveRefreshToken(pair.RefreshToken)
			return pair.AccessToken, nil
		}
		if !errors.Is(err, errRefreshInvalid) {
			return "", err
		}
	}
	pair, err := a.login()
	if err != nil {
		return "", err
	}
	a.saveRefreshToken(pair.RefreshToken)
	return pair.AccessToken, nil
}