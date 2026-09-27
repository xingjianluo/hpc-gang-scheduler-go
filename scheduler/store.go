package scheduler

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Snapshot 是调度器持久化到磁盘的完整状态。
type Snapshot struct {
	Jobs  map[string]*Job  `json:"jobs"`
	Nodes map[string]*Node `json:"nodes"`
}

// Store 负责把调度状态落盘，用于节点/进程恢复后继续调度。
type Store struct {
	path string
}

// NewStore 创建一个写入指定路径的存储。
func NewStore(path string) *Store { return &Store{path: path} }

// Save 将快照原子写入磁盘（先写临时文件再 rename）。
func (s *Store) Save(snap *Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Load 从磁盘读取快照；文件不存在时返回 nil, nil。
func (s *Store) Load() (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	if snap.Jobs == nil {
		snap.Jobs = map[string]*Job{}
	}
	if snap.Nodes == nil {
		snap.Nodes = map[string]*Node{}
	}
	// 兼容旧记录：目录变更时确保路径正确。
	s.path = filepath.Clean(s.path)
	return &snap, nil
}
