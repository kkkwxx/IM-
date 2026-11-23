package gateway

import (
	"sync"

	"im/api/imv1"
)

// hub 维护本地路由 uid → 连接集合（多端同步：推送扇出到该用户所有设备）。
// [PRODUCTION NOTE] 单把 RWMutex，临界区只做快照；M4 压测若显现锁竞争再按 uid 哈希分片。
type hub struct {
	mu    sync.RWMutex
	conns map[string]map[string]*conn // uid → connID → conn
}

func newHub() *hub {
	return &hub{conns: make(map[string]map[string]*conn)}
}

func (h *hub) register(c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[c.userID] == nil {
		h.conns[c.userID] = make(map[string]*conn)
	}
	h.conns[c.userID][c.id] = c
}

func (h *hub) remove(userID string, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// 只删自己：同 uid 其他设备不受影响
	if set := h.conns[userID]; set != nil && set[c.id] == c {
		delete(set, c.id)
		if len(set) == 0 {
			delete(h.conns, userID)
		}
	}
}

// deliver 扇出到该 uid 的所有设备，返回是否至少一台入队成功。
// 同一 env 指针被多个 writePump 并发 marshal——proto.Marshal 只读，并发安全。
func (h *hub) deliver(userID string, env *imv1.Envelope) bool {
	h.mu.RLock()
	targets := make([]*conn, 0, len(h.conns[userID]))
	for _, c := range h.conns[userID] {
		targets = append(targets, c)
	}
	h.mu.RUnlock()

	delivered := false
	for _, c := range targets {
		if c.trySend(&imv1.Frame{Payload: &imv1.Frame_Message{Message: env}}) {
			delivered = true
		} else {
			c.shutdown() // 背压：消费过慢的设备断开，不拖垮其他设备
		}
	}
	return delivered
}

func (h *hub) closeAll() {
	h.mu.Lock()
	conns := h.conns
	h.conns = make(map[string]map[string]*conn)
	h.mu.Unlock()

	for _, set := range conns {
		for _, c := range set {
			c.shutdown()
		}
	}
}