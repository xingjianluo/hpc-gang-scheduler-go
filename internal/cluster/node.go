package cluster

import (
	"context"
	"sync"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// Node 进程内模拟节点。
//
// 它只负责：
//   - 声明自己的资源容量（资源账本在调度器侧，节点不做记账）；
//   - 维护本机正在运行的任务及其取消函数，供抢占杀任务 / 节点失联时使用。
type Node struct {
	Name     string
	Capacity types.Resources

	mu    sync.Mutex
	lost  bool
	tasks map[types.TaskID]context.CancelFunc
}

// NewNode 创建模拟节点。
func NewNode(name string, capacity types.Resources) *Node {
	return &Node{Name: name, Capacity: capacity, tasks: map[types.TaskID]context.CancelFunc{}}
}

// addTask 登记一个正在运行的任务。已存在或节点失联则返回 false。
func (n *Node) addTask(task types.TaskID, cancel context.CancelFunc) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lost {
		return false
	}
	if _, ok := n.tasks[task]; ok {
		return false
	}
	n.tasks[task] = cancel
	return true
}

// removeTask 移除任务登记，返回是否确实移除过。
func (n *Node) removeTask(task types.TaskID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.tasks[task]; !ok {
		return false
	}
	delete(n.tasks, task)
	return true
}

// cancelTask 取消某个任务（抢占时使用），任务实际结束由执行器发出事件。
func (n *Node) cancelTask(task types.TaskID) bool {
	n.mu.Lock()
	cancel, ok := n.tasks[task]
	n.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// markLost 标记节点失联，并取消其上所有任务。返回受影响任务列表。
func (n *Node) markLost() []types.TaskID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lost {
		return nil
	}
	n.lost = true
	tasks := make([]types.TaskID, 0, len(n.tasks))
	for t, cancel := range n.tasks {
		tasks = append(tasks, t)
		cancel()
	}
	n.tasks = map[types.TaskID]context.CancelFunc{}
	return tasks
}

// markUp 恢复节点。
func (n *Node) markUp() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	was := n.lost
	n.lost = false
	return was
}

func (n *Node) isLost() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lost
}

// runningTasks 快照当前运行任务（测试与日志用）。
func (n *Node) runningTasks() []types.TaskID {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]types.TaskID, 0, len(n.tasks))
	for t := range n.tasks {
		out = append(out, t)
	}
	return out
}
