package scheduler

import (
	"testing"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// TestPreemptInterruptibleGang 高优先级作业抢占标记为可中断的低优先级整组作业：
// 受害者整组回到队列、账本同步归还，抢占者整组启动；抢占者结束后受害者重新整组跑完。
func TestPreemptInterruptibleGang(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
		"n2": {CPU: 4},
	})
	h.setSpec("low", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "low", Tenant: "a", Priority: 1, Procs: 8,
		ProcPerTask: types.Resources{CPU: 1}, Interruptible: true,
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("low 整组占满 8 份", time.Second, func() bool {
		r := 0
		for _, n := range h.sch.LedgerSnapshot() {
			r += n.Running
		}
		return r == 8
	})
	h.assertLedgerWithinCapacity()

	// 高优先级作业需要全部 8 份：必须整组抢占 low 的 8 个任务才能启动。
	if err := h.submit(types.JobSpec{
		Name: "urgent", Tenant: "a", Priority: 10, Procs: 8,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("urgent 整组 RUNNING", time.Second, func() bool {
		for i := 0; i < 8; i++ {
			if h.taskState(tid("urgent", i)) != types.TaskRunning {
				return false
			}
		}
		return true
	})
	// 抢占后：low 全部回 PENDING，账本只记 urgent 的 8 份，绝无重复分配。
	for i := 0; i < 8; i++ {
		if got := h.taskState(tid("low", i)); got != types.TaskPending {
			t.Fatalf("low[%d] 被抢占后状态=%s，期望 PENDING", i, got)
		}
	}
	running := 0
	for name, n := range h.sch.LedgerSnapshot() {
		if n.Used.Greater(n.Capacity) {
			t.Fatalf("节点 %s 超分: %+v", name, n)
		}
		running += n.Running
	}
	if running != 8 {
		t.Fatalf("抢占后运行任务数=%d，期望恰好 8（只应有 urgent）", running)
	}
	if got := h.jobState("low"); got != types.JobQueued {
		t.Fatalf("被抢占作业状态=%s，期望 QUEUED", got)
	}

	// 抢占不消耗受害者重试预算。
	if fc, _ := h.sch.TaskFailCount(tid("low", 0)); fc != 0 {
		t.Fatalf("抢占导致 low FailCount=%d，期望 0（抢占不应消耗重试预算）", fc)
	}
	h.logLedger()
}

// TestNoPreemptNonInterruptible 不可中断的低优先级作业不能被抢占；
// 同级优先级也不能互相抢占。
func TestNoPreemptNonInterruptible(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
	})
	h.setSpec("steady", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "steady", Tenant: "a", Priority: 1, Procs: 4,
		ProcPerTask: types.Resources{CPU: 1}, Interruptible: false,
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("steady 占满节点", time.Second, func() bool {
		n := h.sch.LedgerSnapshot()["n1"]
		return n.Running == 4
	})

	// 高优先级但对方不可中断：抢占不动，urgent 整组排队。
	if err := h.submit(types.JobSpec{
		Name: "urgent", Tenant: "a", Priority: 10, Procs: 4,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := h.jobState("urgent"); got != types.JobQueued {
		t.Fatalf("不可中断作业不应被抢占，urgent 状态=%s，期望 QUEUED", got)
	}
	if got := h.jobState("steady"); got != types.JobRunning {
		t.Fatalf("steady 状态=%s，期望保持 RUNNING", got)
	}
	n := h.sch.LedgerSnapshot()["n1"]
	if n.Running != 4 {
		t.Fatalf("节点上运行任务数=%d，期望 4（steady 不应受影响）", n.Running)
	}

	// 同级优先级、即便可中断也不能抢占。
	h2 := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
	})
	h2.setSpec("peer", cluster.TaskSpec{Duration: 1 * time.Hour})
	h2.start()
	defer h2.close()
	if err := h2.submit(types.JobSpec{
		Name: "peer", Tenant: "a", Priority: 5, Procs: 4,
		ProcPerTask: types.Resources{CPU: 1}, Interruptible: true,
	}); err != nil {
		t.Fatal(err)
	}
	h2.waitFor("peer 占满节点", time.Second, func() bool {
		return h2.sch.LedgerSnapshot()["n1"].Running == 4
	})
	if err := h2.submit(types.JobSpec{
		Name: "peer2", Tenant: "a", Priority: 5, Procs: 4,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := h2.jobState("peer2"); got != types.JobQueued {
		t.Fatalf("同级作业不能互相抢占，peer2 状态=%s，期望 QUEUED", got)
	}
}
