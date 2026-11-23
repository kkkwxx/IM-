package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// StoredMsg 是缓存里的消息形态，与 Envelope 的持久化字段一一对应。
type StoredMsg struct {
	Seq     uint64 `json:"seq"`
	MsgID   string `json:"msg_id"`
	From    string `json:"from"`
	Content string `json:"content"`
	Ts      int64  `json:"ts"`
}

// Conversation 水位与消息同文件同次 rename 落盘，崩溃不会出现撕裂状态。
// Watermark 语义：seq <= Watermark 的所有"存在过的"消息都已连续覆盖（发号失败留下的永久空洞不算缺失）。
type Conversation struct {
	Watermark uint64      `json:"watermark"`
	Messages  []StoredMsg `json:"messages"`
}

type convState struct {
	data    Conversation
	pending map[uint64]StoredMsg // seq > 水位+1 的推送暂存区，等 SYNC 补洞后排水
	syncing bool                 // 每会话最多一个在途 SYNC
}

type cache struct {
	dir   string
	mu    sync.Mutex
	convs map[string]*convState
}

func openCache(dir string) (*cache, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &cache{dir: dir, convs: map[string]*convState{}}, nil
}

// convID 含冒号（p:alice:bob），Windows 文件名不允许，统一替换成下划线。
func (c *cache) path(convID string) string {
	return filepath.Join(c.dir, strings.ReplaceAll(convID, ":", "_")+".json")
}

func (c *cache) get(convID string) *convState {
	if st, ok := c.convs[convID]; ok {
		return st
	}
	st := &convState{pending: map[uint64]StoredMsg{}}
	data, err := os.ReadFile(c.path(convID))
	if err == nil {
		if err := json.Unmarshal(data, &st.data); err != nil {
			fmt.Fprintln(os.Stderr, "[cache] 文件损坏，从零开始:", err)
		}
	}
	c.convs[convID] = st
	return st
}

func (c *cache) save(convID string) {
	st := c.convs[convID]
	data, err := json.Marshal(&st.data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[cache] marshal:", err)
		return
	}
	tmp := c.path(convID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "[cache] write tmp:", err)
		return
	}
	if err := os.Rename(tmp, c.path(convID)); err != nil {
		fmt.Fprintln(os.Stderr, "[cache] rename:", err)
	}
}

func (c *cache) watermark(convID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(convID).data.Watermark
}

func (c *cache) history(convID string) []StoredMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]StoredMsg(nil), c.get(convID).data.Messages...)
}

func (c *cache) beginSync(convID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.get(convID)
	if st.syncing {
		return false
	}
	st.syncing = true
	return true
}

func (c *cache) endSync(convID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.get(convID).syncing = false
}

func (c *cache) hasPending(convID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.get(convID).pending) > 0
}

func drain(st *convState) []StoredMsg {
	var committed []StoredMsg
	for {
		m, ok := st.pending[st.data.Watermark+1]
		if !ok {
			return committed
		}
		delete(st.pending, m.Seq)
		st.data.Messages = append(st.data.Messages, m)
		st.data.Watermark = m.Seq
		committed = append(committed, m)
	}
}

// applyPush 返回可渲染的新提交消息；needSync=true 表示出现洞，调用方应触发 SYNC。
// 去重发生在写入时，渲染层天然不会重复打印。
func (c *cache) applyPush(convID string, m StoredMsg) (committed []StoredMsg, needSync bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.get(convID)
	if m.Seq <= st.data.Watermark {
		return nil, false
	}
	if _, ok := st.pending[m.Seq]; ok {
		return nil, false
	}
	if m.Seq == st.data.Watermark+1 {
		st.data.Messages = append(st.data.Messages, m)
		st.data.Watermark = m.Seq
		committed = append(committed, m)
		committed = append(committed, drain(st)...)
		c.save(convID)
		return committed, false
	}
	st.pending[m.Seq] = m
	return nil, true
}

// applySync 整批 upsert：范围查询本身就是覆盖证明，水位直推本批最大 seq，再排水 pending。
func (c *cache) applySync(convID string, msgs []StoredMsg) []StoredMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.get(convID)
	var committed []StoredMsg
	for _, m := range msgs { // 服务端保证按 seq 升序
		if m.Seq <= st.data.Watermark {
			continue
		}
		delete(st.pending, m.Seq)
		st.data.Messages = append(st.data.Messages, m)
		st.data.Watermark = m.Seq
		committed = append(committed, m)
	}
	committed = append(committed, drain(st)...)
	for seq := range st.pending {
		if seq <= st.data.Watermark {
			delete(st.pending, seq)
		}
	}
	c.save(convID)
	return committed
}