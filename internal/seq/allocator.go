package seq

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// MaxSeqStore 是发号器对存储层的最小依赖（接口隔离，便于测试与替换）。
type MaxSeqStore interface {
	MaxSeq(ctx context.Context, convID string) (int64, error)
}

// Allocator 会话级 seq 发号器。
// 正常路径：一次 Redis EVAL 完成原子领号。
// 恢复路径：Redis 键缺失（如重启丢数据）时，从 MySQL MAX(seq) 初始化，防止 seq 回退重号。
type Allocator struct {
	rdb   *redis.Client
	store MaxSeqStore
}

func NewAllocator(rdb *redis.Client, store MaxSeqStore) *Allocator {
	return &Allocator{rdb: rdb, store: store}
}

// 键不存在且 ARGV[1] == -1 时返回 -1，通知调用方走 DB 初始化；
// 否则原子地"按需初始化 + INCR"。GET/SET/INCR 在脚本内串行执行，并发安全。
var allocScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then
  if ARGV[1] == '-1' then
    return -1
  end
  redis.call('SET', KEYS[1], ARGV[1])
end
return redis.call('INCR', KEYS[1])
`)

func (a *Allocator) Alloc(ctx context.Context, convID string) (int64, error) {
	key := "seq:" + convID

	n, err := allocScript.Run(ctx, a.rdb, []string{key}, -1).Int64()
	if err != nil {
		return 0, fmt.Errorf("seq alloc: %w", err)
	}
	if n >= 0 {
		return n, nil
	}

	// 键缺失：从 DB 恢复水位。并发下多个请求同时读到相同 MAX，
	// 脚本内 SET 只对第一个生效，其余直接 INCR，不会回退。
	max, err := a.store.MaxSeq(ctx, convID)
	if err != nil {
		return 0, fmt.Errorf("seq init from db: %w", err)
	}
	n, err = allocScript.Run(ctx, a.rdb, []string{key}, max).Int64()
	if err != nil {
		return 0, fmt.Errorf("seq alloc after init: %w", err)
	}
	return n, nil
}