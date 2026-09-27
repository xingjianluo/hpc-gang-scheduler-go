package store

import "context"

// Snapshot 是一次持久化的调度器状态快照。
// Store 不关心 Data 的内部结构，由调度器负责编解码（见 internal/scheduler/snapshot.go）。
type Snapshot struct {
	// Version 单调递增的快照版本号，每次持久化 +1。
	Version int64
	// Data 是快照载荷（JSON）。
	Data []byte
}

// Store 是调度器状态持久化接口。
//
// 语义：
//   - Save 必须具备原子性：进程崩溃后 Load 只能看到完整快照，不能看到写坏的半截文件。
//   - Load 在没有任何快照时返回 (Snapshot{}, false, nil)。
//   - 实现必须保证多个 goroutine 并发 Save 时快照不会互相撕裂。
type Store interface {
	Save(ctx context.Context, snap Snapshot) error
	Load(ctx context.Context) (Snapshot, bool, error)
}
