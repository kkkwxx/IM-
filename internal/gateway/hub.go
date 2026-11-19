package gateway

import (
	"sync"

	"im/api/imv1"
)

// hub 维护 userID -> 连接 的本地路由表。
// [PRODUCTION NOTE] M1 用单把 RWMutex；M3 引入分片锁（按 userID 哈希分桶）
// 降低高并发下的锁竞争，并把"用户在哪台 gateway"上移到 Redis 全局路由表。
type hub struct {
	mu    sync.RWMutex
	conns map[string]*conn
}

func newHub() *hub {
	return &hub{conns: make(map[string]*conn)}
}

func (h *hub) register(c *conn) {
	h.mu.Lock()
	old := h.conns[c.userID]
	h.conns[c.userID] = c
	h.mu.Unlock()

	// [PRODUCTION NOTE] M1 单端在线、新登录踢旧连接；
	// M3 改为多端同步：uid -> 连接集合，推送扇出到所有设备。
	if old != nil && old != c {
		old.shutdown()
	}
}

func (h *hub) remove(userID string, c *conn) {
	h.mu.Lock()
	// 只删除自己，避免误删踢旧登录后注册的新连接
	if h.conns[userID] == c {
		delete(h.conns, userID)
	}
	h.mu.Unlock()
}

func (h *hub) deliver(userID string, env *imv1.Envelope) bool {
	h.mu.RLock()
	c := h.conns[userID]
	h.mu.RUnlock()

	if c == nil {
		return false
	}
	if !c.trySend(&imv1.Frame{Payload: &imv1.Frame_Message{Message: env}}) {
		c.shutdown()
		return false
	}
	return true
}

func (h *hub) closeAll() {
	h.mu.Lock()
	conns := h.conns
	h.conns = make(map[string]*conn)
	h.mu.Unlock()

	for _, c := range conns {
		c.shutdown()
	}
}