package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/store"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// testHarness 组装一套进程内模拟环境：集群 + 执行器 + 调度器 + 内存快照。
type testHarness struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	clus   *cluster.Cluster
	st     *store.MemoryStore
	sch    *Scheduler
	exec   *cluster.Executor

	mu    sync.Mutex
	specs map[string]cluster.TaskSpec
	// failAttempts: 任务键 -> 前 N 次"真实失败尝试"返回 Fail（之后成功）。
	failAttempts map[string]int
	markClosed   func()
}

func newHarness(t *testing.T, cfg Config, nodes map[string]types.Resources) *testHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &testHarness{
		t:            t,
		ctx:          ctx,
		cancel:       cancel,
		clus:         cluster.NewCluster(),
		st:           store.NewMemoryStore(),
		specs:        map[string]cluster.TaskSpec{},
		failAttempts: map[string]int{},
	}
	for name, cap := range nodes {
		h.clus.AddNode(cluster.NewNode(name, cap))
	}
	var sch *Scheduler
	h.exec = cluster.NewExecutor(func(ev types.Event) {
		sch.HandleEvent(context.Background(), ev)
	}, func(id types.TaskID) cluster.TaskSpec {
		h.mu.Lock()
		defer h.mu.Unlock()
		if sp, ok := h.specs[id.Job]; ok {
			return sp
		}
		if n, ok := h.failAttempts[id.Job]; ok {
			fails, _ := sch.TaskFailCount(id)
			return cluster.TaskSpec{Duration: 5 * time.Millisecond, Fail: fails < n}
		}
		return cluster.TaskSpec{Duration: 10 * time.Millisecond}
	})
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 5 * time.Millisecond
	}
	var logMu sync.Mutex
	closed := false
	sch = New(cfg, h.clus, h.exec, h.st, func(format string, args ...any) {
		// 调度决策与账本快照全部进入测试日志，方便对照判定依据。
		// t.Logf 不允许在测试函数返回后调用；用锁 + closed 标记保护收尾阶段的在飞事件。
		logMu.Lock()
		defer logMu.Unlock()
		if !closed {
			t.Logf("[decision] "+format, args...)
		}
	})
	h.sch = sch
	h.markClosed = func() {
		logMu.Lock()
		closed = true
		logMu.Unlock()
	}
	return h
}

func (h *testHarness) start() {
	h.t.Helper()
	if err := h.sch.Recover(h.ctx); err != nil {
		h.t.Fatalf("recover: %v", err)
	}
	if err := h.sch.Start(h.ctx); err != nil {
		h.t.Fatalf("start: %v", err)
	}
}

func (h *testHarness) close() {
	h.t.Helper()
	// 1) 先停调度循环，确保关闭过程中不会再启动新任务；
	// 2) 取消全部在跑任务并等集群收敛，杜绝 goroutine 跨用例残留；
	// 3) 最后关闭日志回调（t.Logf 不允许在测试返回后调用）。
	h.cancel()
	h.sch.Close()
	h.clus.StopAllTasks()
	if !h.clus.WaitIdle(2 * time.Second) {
		h.t.Errorf("关闭时集群内仍有任务未收敛")
	}
	if h.markClosed != nil {
		h.markClosed()
	}
}

func (h *testHarness) setSpec(job string, sp cluster.TaskSpec) {
	h.mu.Lock()
	h.specs[job] = sp
	h.mu.Unlock()
}

func (h *testHarness) failFirstAttempts(job string, n int) {
	h.mu.Lock()
	h.failAttempts[job] = n
	h.mu.Unlock()
}

// submit 提交作业，忽略 ctx（任务生命周期与请求 ctx 解耦）。
func (h *testHarness) submit(spec types.JobSpec) error {
	return h.sch.Submit(context.Background(), spec)
}

// waitFor 轮询直到 cond 为真或超时。
func (h *testHarness) waitFor(desc string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	h.t.Fatalf("等待条件超时: %s\n最终账本:\n%s", desc, FormatSnapshot(h.sch.LedgerSnapshot()))
}

func (h *testHarness) waitJobState(name string, want types.JobState) {
	h.waitFor("job "+name+" == "+want.String(), 3*time.Second, func() bool {
		st, ok := h.sch.JobState(name)
		return ok && st == want
	})
}

func (h *testHarness) jobState(name string) types.JobState {
	st, _ := h.sch.JobState(name)
	return st
}

func (h *testHarness) taskState(id types.TaskID) types.TaskState {
	st, _ := h.sch.TaskState(id)
	return st
}

// countState 统计某作业处于指定状态的任务数。
func (h *testHarness) countState(job string, want types.TaskState) int {
	count := 0
	for i := 0; ; i++ {
		st, exists := h.sch.TaskState(tid(job, i))
		if !exists {
			break
		}
		if st == want {
			count++
		}
	}
	return count
}

// assertLedgerWithinCapacity 断言任何时刻账本都不超分。
func (h *testHarness) assertLedgerWithinCapacity() {
	h.t.Helper()
	for name, n := range h.sch.LedgerSnapshot() {
		if n.Used.Greater(n.Capacity) {
			h.t.Fatalf("节点 %s 资源超分: used=%v > cap=%v", name, n.Used, n.Capacity)
		}
	}
}

func (h *testHarness) logLedger() {
	h.t.Helper()
	h.t.Logf("资源账本快照:\n%s", FormatSnapshot(h.sch.LedgerSnapshot()))
}

func tid(job string, idx int) types.TaskID { return types.TaskID{Job: job, Index: idx} }
