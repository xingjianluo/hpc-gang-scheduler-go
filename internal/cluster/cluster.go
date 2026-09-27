package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// HandleFunc 是任务在节点上执行的回调（由调度器注入）。
// ctx 在任务被抢占、超时或节点失联时取消；instance 是调度器分配的启动代号，
// 回调发出的事件必须携带它；回调结束即代表任务终态事件已发出。
type HandleFunc func(ctx context.Context, task types.TaskID, node string, instance int)

// Cluster 进程内模拟集群：维护节点集合与各节点上正在运行的任务。
// 它不做任何调度与记账决策，只忠实执行调度器下达的启动/停止/失联指令。
type Cluster struct {
	mu    sync.Mutex
	nodes map[string]*Node
	order []string
}

// NewCluster 创建模拟集群。
func NewCluster() *Cluster {
	return &Cluster{nodes: map[string]*Node{}}
}

// AddNode 注册节点。重名会被忽略。
func (c *Cluster) AddNode(n *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nodes[n.Name]; ok {
		return
	}
	c.nodes[n.Name] = n
	c.order = append(c.order, n.Name)
}

// NodeNames 返回节点名（注册顺序）。
func (c *Cluster) NodeNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.order))
	copy(out, c.order)
	return out
}

func (c *Cluster) node(name string) (*Node, error) {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown node %q", name)
	}
	return n, nil
}

// Capacity 返回节点容量。
func (c *Cluster) Capacity(name string) (types.Resources, bool) {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return types.Resources{}, false
	}
	return n.Capacity, true
}

// Launch 在指定节点上启动一个任务。
// instance 是调度器分配的启动代号，会原样传给执行回调。
// 返回错误表示节点不存在/已失联/任务已在运行，此时调用方不应认为任务已启动。
// 成功后 run 在独立 goroutine 中执行；run 返回前集群会保留任务登记。
//
// 注意：任务的执行 context 故意不继承调用方 ctx —— 任务只允许被
// StopTask（抢占/超时）或 MarkNodeLost 取消，提交方请求结束、
// 调度器关闭都不应把在跑任务变成 failed。
func (c *Cluster) Launch(ctx context.Context, task types.TaskID, node string, instance int, run HandleFunc) error {
	n, err := c.node(node)
	if err != nil {
		return err
	}
	taskCtx, cancel := context.WithCancel(context.Background())
	if !n.addTask(task, cancel) {
		cancel()
		if n.isLost() {
			return fmt.Errorf("node %q is lost, cannot launch %s", node, task)
		}
		return fmt.Errorf("task %s already running on node %q", task, node)
	}
	go func() {
		defer n.removeTask(task)
		run(taskCtx, task, node, instance)
	}()
	return nil
}

// StopTask 取消任务（抢占/超时使用）。若任务不在该节点运行，返回 false。
func (c *Cluster) StopTask(task types.TaskID, node string) bool {
	c.mu.Lock()
	n, ok := c.nodes[node]
	c.mu.Unlock()
	if !ok {
		return false
	}
	return n.cancelTask(task)
}

// MarkNodeLost 把节点标记为失联并取消其上全部任务，返回受影响任务。
// 对已经失联的节点重复调用是幂等的，返回空列表。
func (c *Cluster) MarkNodeLost(name string) ([]types.TaskID, bool) {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	return n.markLost(), true
}

// MarkNodeUp 恢复节点，返回它之前是否处于失联状态。
func (c *Cluster) MarkNodeUp(name string) bool {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return false
	}
	return n.markUp()
}

// IsLost 查询节点是否失联（节点不存在视为失联）。
func (c *Cluster) IsLost(name string) bool {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return true
	}
	return n.isLost()
}

// RunningTasks 返回某节点当前登记的运行任务（测试用）。
func (c *Cluster) RunningTasks(name string) []types.TaskID {
	c.mu.Lock()
	n, ok := c.nodes[name]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	return n.runningTasks()
}

// StopAllTasks 取消全部节点上登记的任务（抢占/超时/失联之外的关闭路径使用）。
func (c *Cluster) StopAllTasks() {
	for _, name := range c.NodeNames() {
		for _, task := range c.RunningTasks(name) {
			c.StopTask(task, name)
		}
	}
}

// WaitIdle 等待所有节点上登记的任务都收敛（最多等到 timeout）。
// 关闭测试环境时用于确认在飞 goroutine 不会残留到下一个用例。
func (c *Cluster) WaitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := 0
		for _, node := range c.nodes {
			node.mu.Lock()
			n += len(node.tasks)
			node.mu.Unlock()
		}
		c.mu.Unlock()
		if n == 0 {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
