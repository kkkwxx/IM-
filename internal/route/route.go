package route

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL 路由条目存活时间；gateway 心跳按 TTL/3 续期，崩溃后最迟 TTL 过期。
const DefaultTTL = 30 * time.Second

// Table 全局连接路由表：uid → {connID → gateway 实例}。
// 结构：HASH route:{uid}，field=connID（每连接唯一），value="gwAddr|登记/续期毫秒时间戳"。
// connID 唯一使"各删各的 field"天然安全——误删竞态在数据结构层面就无法表达。
type Table struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Table {
	return &Table{rdb: rdb, ttl: ttl}
}

func key(uid string) string { return "route:" + uid }

// 原子完成"写 field + 刷 TTL"；登记与心跳续期复用同一脚本（HSET 幂等覆盖）。
var registerScript = redis.NewScript(`
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`)

func (t *Table) Register(ctx context.Context, uid, connID, gwAddr string) error {
	val := gwAddr + "|" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	return registerScript.Run(ctx, t.rdb, []string{key(uid)}, connID, val, int(t.ttl.Seconds())).Err()
}

func (t *Table) Renew(ctx context.Context, uid, connID, gwAddr string) error {
	return t.Register(ctx, uid, connID, gwAddr)
}

// Remove 只删自己连接的 field，不碰同 uid 其他设备/其他 gateway 的条目。
func (t *Table) Remove(ctx context.Context, uid, connID string) error {
	return t.rdb.HDel(ctx, key(uid), connID).Err()
}

// Lookup 返回 uid 的新鲜连接所在 gateway 集合（排序去重）。
// 过期 field（gateway 崩溃残留）读时过滤——读时自愈，不需要后台清道夫。
// 不主动 HDEL 过期 field：避免与并发续期竞态；残留量以设备数为界，key 整体 TTL 兜底。
func (t *Table) Lookup(ctx context.Context, uid string) ([]string, error) {
	all, err := t.rdb.HGetAll(ctx, key(uid)).Result()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	seen := make(map[string]struct{})
	for _, val := range all {
		i := strings.LastIndex(val, "|")
		if i <= 0 {
			continue
		}
		ts, err := strconv.ParseInt(val[i+1:], 10, 64)
		if err != nil || now-ts > t.ttl.Milliseconds() {
			continue
		}
		seen[val[:i]] = struct{}{}
	}
	gws := make([]string, 0, len(seen))
	for gw := range seen {
		gws = append(gws, gw)
	}
	sort.Strings(gws)
	return gws, nil
}