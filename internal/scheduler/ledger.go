package scheduler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// nodeAlloc 记录单节点容量、已分配量与任务分布。
type nodeAlloc struct {
	capacity types.Resources
	used     types.Resources
	// taskProc: 任务键 -> 该任务占用的资源量。一个任务至多落在一个节点上。
	taskProc map[string]types.Resources
}

func newNodeAlloc(capacity types.Resources) *nodeAlloc {
	return &nodeAlloc{capacity: capacity, taskProc: map[string]types.Resources{}}
}

// available 返回节点当前空闲资源。
func (a *nodeAlloc) available() types.Resources {
	return a.capacity.Sub(a.used)
}

// Ledger 是线程安全的资源账本。
//
// 核心不变量（任何时刻、任何 goroutine 下都必须成立）：
//
//	对每个节点：used = Σ 所有运行中任务占用量 <= capacity
//	每个任务键在 taskNode 中至多出现一次（一个任务只能落在一个节点上）
//
// 整组调度通过 TryReserveGang 实现：要么整组全部落账，要么账本完全不变，
// 绝不允许出现"启动了一部分任务、占用一部分资源"的中间状态。
type Ledger struct {
	mu       sync.Mutex
	nodes    map[string]*nodeAlloc
	order    []string
	taskNode map[string]string // 任务键 -> 所在节点，防止跨节点重复落账
}

func NewLedger() *Ledger {
	return &Ledger{nodes: map[string]*nodeAlloc{}, taskNode: map[string]string{}}
}

// AddNode 登记一个节点。重复登记同名节点会被忽略。
func (l *Ledger) AddNode(name string, capacity types.Resources) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.nodes[name]; ok {
		return
	}
	l.nodes[name] = newNodeAlloc(capacity)
	l.order = append(l.order, name)
}

// NodeNames 返回节点名（注册顺序）。
func (l *Ledger) NodeNames() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.order))
	copy(out, l.order)
	return out
}

// Capacity 返回节点容量。
func (l *Ledger) Capacity(name string) (types.Resources, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.nodes[name]
	if !ok {
		return types.Resources{}, false
	}
	return a.capacity, true
}

// TryReserveGang 尝试为一整组任务落账。
// 前置要求：同一任务不在账本中（重复落账会返回 false）。
// 成功返回 true 且账本被整体更新；任一节点放不下则返回 false 且账本保持原样。
func (l *Ledger) TryReserveGang(placements []Placement) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 先在局部变量上模拟整组落账；全部成功才提交。
	deltas := make(map[*nodeAlloc]types.Resources)
	taskNodes := make(map[string]string)
	taskProcs := make(map[string]types.Resources)

	for _, p := range placements {
		key := p.Task.String()
		a, ok := l.nodes[p.Node]
		if !ok {
			return false
		}
		// 同一任务不允许重复落账（本节点内或其它节点上均不可），
		// 防止并发路径把一份资源分配两次。
		if _, dup := l.taskNode[key]; dup {
			return false
		}
		if _, dup := taskNodes[key]; dup {
			return false
		}
		if !p.Proc.CanFit(a.capacity.Sub(a.used.Add(deltas[a]))) {
			return false
		}
		deltas[a] = deltas[a].Add(p.Proc)
		taskNodes[key] = p.Node
		taskProcs[key] = p.Proc
	}

	// 整组提交。
	for a, d := range deltas {
		a.used = a.used.Add(d)
	}
	for key, node := range taskNodes {
		l.nodes[node].taskProc[key] = taskProcs[key]
		l.taskNode[key] = node
	}
	return true
}

// Release 释放单个任务占用的资源。任务不存在时为空操作（保证抢占/失联路径可重入）。
func (l *Ledger) Release(task types.TaskID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := task.String()
	node, ok := l.taskNode[key]
	if !ok {
		return false
	}
	a := l.nodes[node]
	if proc, ok := a.taskProc[key]; ok {
		a.used = a.used.Sub(proc)
		delete(a.taskProc, key)
	}
	delete(l.taskNode, key)
	return true
}

// TaskNode 返回任务当前落账的节点（未运行返回 ""，false）。
func (l *Ledger) TaskNode(task types.TaskID) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	node, ok := l.taskNode[task.String()]
	return node, ok
}

// TaskProc 返回任务占用的资源量与其是否存在（未运行返回零值、false）。
func (l *Ledger) TaskProc(task types.TaskID) (types.Resources, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := task.String()
	node, ok := l.taskNode[key]
	if !ok {
		return types.Resources{}, false
	}
	proc, ok := l.nodes[node].taskProc[key]
	return proc, ok
}

// releaseOnNode 在节点失联时被使用前的检查辅助：返回某节点上的全部任务键。
func (l *Ledger) tasksOnNodeLocked(name string) []string {
	a := l.nodes[name]
	if a == nil {
		return nil
	}
	out := make([]string, 0, len(a.taskProc))
	for key := range a.taskProc {
		out = append(out, key)
	}
	return out
}

// ReleaseNode 清空一个失联节点上的全部账目并返回受影响任务键。
func (l *Ledger) ReleaseNode(name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := l.tasksOnNodeLocked(name)
	a := l.nodes[name]
	if a != nil {
		for key := range a.taskProc {
			delete(l.taskNode, key)
		}
		a.used = types.Resources{}
		a.taskProc = map[string]types.Resources{}
	}
	return keys
}

// UsedOn 返回某节点已分配资源。
func (l *Ledger) UsedOn(name string) types.Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.nodes[name]
	if !ok {
		return types.Resources{}
	}
	return a.used
}

// AvailableOn 返回某节点空闲资源。
func (l *Ledger) AvailableOn(name string) types.Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.nodes[name]
	if !ok {
		return types.Resources{}
	}
	return a.available()
}

// LedgerNodeSnapshot 账本快照中的单节点视图（供测试与日志使用）。
type LedgerNodeSnapshot struct {
	Name     string
	Capacity types.Resources
	Used     types.Resources
	Running  int
}

// Snapshot 返回账本快照，并校验 used <= capacity 不变量（违例直接 panic，
// 因为这代表资源被超分，是调度器级别的正确性缺陷）。
func (l *Ledger) Snapshot() map[string]LedgerNodeSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]LedgerNodeSnapshot, len(l.order))
	for _, name := range l.order {
		a := l.nodes[name]
		if a.used.Greater(a.capacity) {
			panic(fmt.Sprintf("ledger invariant violated on %s: used %v > capacity %v", name, a.used, a.capacity))
		}
		out[name] = LedgerNodeSnapshot{Name: name, Capacity: a.capacity, Used: a.used, Running: len(a.taskProc)}
	}
	return out
}

// FormatSnapshot 把账本快照渲染成可读的多行字符串，写入测试日志。
func FormatSnapshot(snap map[string]LedgerNodeSnapshot) string {
	var b strings.Builder
	for name, n := range snap {
		b.WriteString(fmt.Sprintf("    node=%s used={cpu:%d mem:%d gpu:%d} cap={cpu:%d mem:%d gpu:%d} running=%d\n",
			name, n.Used.CPU, n.Used.Memory, n.Used.GPU, n.Capacity.CPU, n.Capacity.Memory, n.Capacity.GPU, n.Running))
	}
	return b.String()
}
