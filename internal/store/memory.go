package store

import (
	"context"
	"sync"
)

// MemoryStore 进程内快照存储，主要用于单元测试。
// 用互斥锁保护，保证并发 Save 不会读到撕裂的快照。
type MemoryStore struct {
	mu     sync.Mutex
	snap   Snapshot
	exists bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Save(ctx context.Context, snap Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 深拷贝载荷，避免调用方在 Save 之后继续修改底层切片。
	data := make([]byte, len(snap.Data))
	copy(data, snap.Data)
	s.mu.Lock()
	s.snap = Snapshot{Version: snap.Version, Data: data}
	s.exists = true
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Load(ctx context.Context) (Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists {
		return Snapshot{}, false, nil
	}
	data := make([]byte, len(s.snap.Data))
	copy(data, s.snap.Data)
	return Snapshot{Version: s.snap.Version, Data: data}, true, nil
}
