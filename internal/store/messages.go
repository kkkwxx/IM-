package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Message struct {
	ID        int64     `db:"id"`
	ConvID    string    `db:"conv_id"`
	Seq       int64     `db:"seq"`
	MsgID     string    `db:"msg_id"`
	Sender    string    `db:"sender"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}

var (
	ErrDuplicateMsg = errors.New("duplicate msg_id") // 幂等命中：调用方应查出已有 seq 返回
	ErrDuplicateSeq = errors.New("duplicate seq")    // 发号异常：seq 重号，正确性防线被触发
)

func (s *Store) InsertMessage(ctx context.Context, m *Message) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO messages (conv_id, seq, msg_id, sender, content, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		m.ConvID, m.Seq, m.MsgID, m.Sender, m.Content, m.CreatedAt)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 {
			// 区分两个唯一索引：幂等冲突是正常业务路径，重号是异常
			if strings.Contains(me.Message, "uk_conv_msg") {
				return ErrDuplicateMsg
			}
			if strings.Contains(me.Message, "uk_conv_seq") {
				return ErrDuplicateSeq
			}
		}
		return err
	}
	return nil
}

// GetByMsgID 未找到时返回 (nil, nil)。
func (s *Store) GetByMsgID(ctx context.Context, convID, msgID string) (*Message, error) {
	var m Message
	err := s.db.GetContext(ctx, &m,
		`SELECT id, conv_id, seq, msg_id, sender, content, created_at
		 FROM messages WHERE conv_id = ? AND msg_id = ?`, convID, msgID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// MaxSeq 返回会话当前最大 seq，空会话返回 0。供发号器初始化用。
func (s *Store) MaxSeq(ctx context.Context, convID string) (int64, error) {
	var max int64
	err := s.db.GetContext(ctx, &max,
		`SELECT COALESCE(MAX(seq), 0) FROM messages WHERE conv_id = ?`, convID)
	return max, err
}

// ListMessages 游标分页：seq > since 升序取 limit 条。
// 走 (conv_id, seq) 复合索引，范围扫描天然有序，无 filesort；禁用 OFFSET。
func (s *Store) ListMessages(ctx context.Context, convID string, since int64, limit int) ([]Message, error) {
	var ms []Message
	err := s.db.SelectContext(ctx, &ms,
		`SELECT id, conv_id, seq, msg_id, sender, content, created_at
		 FROM messages WHERE conv_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`,
		convID, since, limit)
	return ms, err
}