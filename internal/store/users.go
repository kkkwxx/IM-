package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type User struct {
	ID           int64     `db:"id"`
	Username     string    `db:"username"`
	PasswordHash string    `db:"password_hash"`
	CreatedAt    time.Time `db:"created_at"`
}

type RefreshToken struct {
	ID        int64     `db:"id"`
	UID       string    `db:"uid"`
	TokenHash string    `db:"token_hash"`
	ExpiresAt time.Time `db:"expires_at"`
	CreatedAt time.Time `db:"created_at"`
}

var ErrDuplicateUser = errors.New("username already exists")

func (s *Store) CreateUser(ctx context.Context, username, passwordHash string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)`,
		username, passwordHash, time.Now())
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 && strings.Contains(me.Message, "uk_username") {
			return ErrDuplicateUser
		}
		return err
	}
	return nil
}

// GetUserByName 未找到时返回 (nil, nil)。
func (s *Store) GetUserByName(ctx context.Context, username string) (*User, error) {
	var u User
	err := s.db.GetContext(ctx, &u,
		`SELECT id, username, password_hash, created_at FROM users WHERE username = ?`, username)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *Store) SaveRefreshToken(ctx context.Context, uid, tokenHash string, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO refresh_tokens (uid, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)`,
		uid, tokenHash, expiresAt, time.Now())
	return err
}

// ConsumeRefreshToken 原子地"校验并作废"：事务内行锁读取，未过期则删除并返回。
// 旋转必须原子——否则两个并发 refresh 都读到同一行，旧 token 被用两次（重放窗口）。
// 不存在/已过期/已消费均返回 (nil, nil)。
func (s *Store) ConsumeRefreshToken(ctx context.Context, tokenHash string) (*RefreshToken, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var rt RefreshToken
	err = tx.GetContext(ctx, &rt,
		`SELECT id, uid, token_hash, expires_at, created_at
		 FROM refresh_tokens WHERE token_hash = ? FOR UPDATE`, tokenHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if time.Now().After(rt.ExpiresAt) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE id = ?`, rt.ID); err != nil {
			return nil, err
		}
		return nil, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE id = ?`, rt.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &rt, nil
}