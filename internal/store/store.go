package store

import (
	"context"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Store struct {
	db *sqlx.DB
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sqlx.ConnectContext(ctx, "mysql", dsn)
	if err != nil {
		return nil, err
	}
	// [PRODUCTION NOTE] 池参数应随压测调整；MaxOpenConns 需与 MySQL max_connections 协调
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// DB 暴露底层句柄，供测试清理数据等受控场景使用。
func (s *Store) DB() *sqlx.DB {
	return s.db
}