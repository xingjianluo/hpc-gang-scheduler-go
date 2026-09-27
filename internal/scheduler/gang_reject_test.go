package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

func rejectReason(t *testing.T, err error) types.RejectReason {
	t.Helper()
	if err == nil {
		t.Fatal("期望提交被拒绝，实际成功")
	}
	re, ok := err.(*types.RejectError)
	if !ok {
		t.Fatalf("期望 *RejectError，得到 %T: %v", err, err)
	}
	return re.Reason
}

// TestGangRejectReasons 覆盖三种可区分的整组拒绝原因。
func TestGangRejectReasons(t *testing.T) {
	h := newHarness(t, Config{
		Quotas: map[string]types.Resources{
			"team-q": {CPU: 4, Memory: 16},
		},
	}, map[string]types.Resources{
		"n1": {CPU: 4, Memory: 16},
		"n2": {CPU: 4, Memory: 16},
	})
	h.start()
	defer h.close()

	// 1) 单任务没有任何节点放得下 -> insufficient_capacity
	err := h.submit(types.JobSpec{
		Name: "too-fat", Tenant: "team-a", Procs: 1,
		ProcPerTask: types.Resources{CPU: 100},
	})
	if got := rejectReason(t, err); got != types.RejectInsufficientCap {
		t.Fatalf("原因=%s，期望 %s", got, types.RejectInsufficientCap)
	}

	// 2) 整组总需求超过集群总容量（单任务放得下）-> insufficient_capacity
	err = h.submit(types.JobSpec{
		Name: "too-many", Tenant: "team-a", Procs: 20,
		ProcPerTask: types.Resources{CPU: 1, Memory: 1},
	})
	if got := rejectReason(t, err); got != types.RejectInsufficientCap {
		t.Fatalf("原因=%s，期望 %s", got, types.RejectInsufficientCap)
	}

	// 3) 超出租户配额 -> quota_exceeded
	err = h.submit(types.JobSpec{
		Name: "quota-bomb", Tenant: "team-q", Procs: 5,
		ProcPerTask: types.Resources{CPU: 1, Memory: 1},
	})
	if got := rejectReason(t, err); got != types.RejectQuotaExceeded {
		t.Fatalf("原因=%s，期望 %s", got, types.RejectQuotaExceeded)
	}

	// 4) 正常入队一个作业，再重复提交同名作业 -> duplicate_submission
	if err := h.submit(types.JobSpec{
		Name: "dup", Tenant: "team-a", Procs: 1,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatalf("首次提交不应失败: %v", err)
	}
	err = h.submit(types.JobSpec{
		Name: "dup", Tenant: "team-a", Procs: 1,
		ProcPerTask: types.Resources{CPU: 1},
	})
	if got := rejectReason(t, err); got != types.RejectDuplicate {
		t.Fatalf("原因=%s，期望 %s", got, types.RejectDuplicate)
	}

	// 被拒绝的作业完全不进系统：查不到状态，账本为空。
	for _, name := range []string{"too-fat", "too-many", "quota-bomb"} {
		if _, ok := h.sch.JobState(name); ok {
			t.Fatalf("被拒绝作业 %s 不应存在于系统中", name)
		}
	}
	h.logLedger()
}

// TestGangAtomicNoPartialStart 整组原子性：
// 集群总量能容纳 8 个任务（两节点各 4 份），先被 holder 占掉 3 份后只剩 5 份；
// 7 任务数组整组需要 7 份：释放 1 份后空闲 6 份仍不够、释放 2 份后空闲 7 份
// 才整组放行。任何中间状态都绝不能先启动一部分任务占资源。
func TestGangAtomicNoPartialStart(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
		"n2": {CPU: 4},
	})
	h.setSpec("holder", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.setSpec("array", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "holder", Tenant: "team-a", Procs: 3,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("holder 整组 RUNNING", time.Second, func() bool {
		for i := 0; i < 3; i++ {
			if h.taskState(tid("holder", i)) != types.TaskRunning {
				return false
			}
		}
		return true
	})
	if err := h.submit(types.JobSpec{
		Name: "array", Tenant: "team-a", Procs: 7,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}

	// array 必须整组排队：给足调度扫描机会（20 个调度 tick），
	// 期间持续断言没有任何 array 任务被部分启动、账本恰好只有 holder 的 3 份。
	for i := 0; i < 20; i++ {
		time.Sleep(5 * time.Millisecond)
		if got := h.jobState("array"); got != types.JobQueued {
			t.Fatalf("整组放不下时作业状态=%s，期望 QUEUED（不得部分启动）", got)
		}
		for j := 0; j < 7; j++ {
			if got := h.taskState(tid("array", j)); got != types.TaskPending {
				t.Fatalf("array[%d] 状态=%s，整组放不下时必须保持 PENDING", j, got)
			}
		}
		running := 0
		for _, n := range h.sch.LedgerSnapshot() {
			running += n.Running
			if n.Used.Greater(n.Capacity) {
				t.Fatalf("节点超分: %+v", n)
			}
		}
		if running != 3 {
			t.Fatalf("运行中任务数=%d，期望恰好 3（仅 holder，array 不得部分占用）", running)
		}
	}

	// 手动逐个结束 holder 任务释放资源（不依赖墙钟时间）：
	// 释放 1 份 -> 空闲 6 份，仍不够 7；释放 2 份 -> 空闲 7 份，整组放行。
	complete := func(job string, idx int) {
		inst, _ := h.sch.TaskAttempts(tid(job, idx))
		h.sch.HandleEvent(context.Background(), types.Event{
			TaskID: tid(job, idx), Kind: types.EventCompleted, NodeName: "",
			Instance: inst, Sequence: 1_000_000 + int64(idx),
		})
	}
	complete("holder", 0)
	time.Sleep(30 * time.Millisecond)
	if c := h.countState("array", types.TaskRunning); c != 0 {
		t.Fatalf("空闲 6 份（需 7）时 array 有 %d 个任务运行，必须整组等待", c)
	}
	complete("holder", 1)
	h.waitFor("array 整组回填 RUNNING", time.Second, func() bool {
		return h.countState("array", types.TaskRunning) == 7
	})
	if got := h.jobState("array"); got != types.JobRunning {
		t.Fatalf("回填后作业状态=%s，期望 RUNNING", got)
	}
	// holder[2] 仍在跑（1 份）+ array 7 份 = 8 份。
	running := 0
	for _, n := range h.sch.LedgerSnapshot() {
		running += n.Running
	}
	if running != 8 {
		t.Fatalf("回填后运行中任务数=%d，期望 8（holder[2] + array 7）", running)
	}
	h.assertLedgerWithinCapacity()
	h.logLedger()
}

// TestGangStartsAllOrNothing 正常整组：资源够时 N 个任务同时 RUNNING。
func TestGangStartsAllOrNothing(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
		"n2": {CPU: 4},
	})
	h.setSpec("array", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "array", Tenant: "team-a", Procs: 6,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("array 整组 RUNNING", time.Second, func() bool {
		for i := 0; i < 6; i++ {
			if h.taskState(tid("array", i)) != types.TaskRunning {
				return false
			}
		}
		return true
	})
	h.assertLedgerWithinCapacity()
	running := 0
	for _, n := range h.sch.LedgerSnapshot() {
		running += n.Running
	}
	if running != 6 {
		t.Fatalf("运行中任务数=%d，期望 6", running)
	}
	h.logLedger()
}
