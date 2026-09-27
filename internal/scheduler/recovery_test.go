package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/store"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// TestDuplicateAndStaleEvents 重复/乱序/旧实例事件不能重复计数或改回运行中：
//   - 同一终态事件重放多次，只生效一次；
//   - 抢占后旧实例迟到的 completed 不能把 PENDING 任务改回 RUNNING/COMPLETED；
//   - 乱序（高 seq 先到、低 seq 后到）的事件被忽略。
func TestDuplicateAndStaleEvents(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 2},
	})
	h.setSpec("job", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "job", Tenant: "a", Procs: 1,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("job RUNNING", time.Second, func() bool {
		return h.taskState(tid("job", 0)) == types.TaskRunning
	})

	// 拿到当前实例代号（Attempts 即当前实例号）。
	inst, _ := h.sch.TaskAttempts(tid("job", 0))

	// 1) 乱序：先发 seq=10 的 completed，再发 seq=9 的 completed，后者必须被忽略。
	h.sch.HandleEvent(context.Background(), types.Event{
		TaskID: tid("job", 0), Kind: types.EventCompleted, NodeName: "n1",
		Instance: inst, Sequence: 10,
	})
	h.waitFor("job COMPLETED", time.Second, func() bool {
		return h.taskState(tid("job", 0)) == types.TaskCompleted
	})
	h.sch.HandleEvent(context.Background(), types.Event{
		TaskID: tid("job", 0), Kind: types.EventCompleted, NodeName: "n1",
		Instance: inst, Sequence: 9,
	})
	// 2) 重复完成通知：再发一次 seq=10，终态保持。
	h.sch.HandleEvent(context.Background(), types.Event{
		TaskID: tid("job", 0), Kind: types.EventCompleted, NodeName: "n1",
		Instance: inst, Sequence: 10,
	})
	// 3) 终态任务收到 started 也不能改回运行中。
	h.sch.HandleEvent(context.Background(), types.Event{
		TaskID: tid("job", 0), Kind: types.EventStarted, NodeName: "n1",
		Instance: inst, Sequence: 11,
	})
	if got := h.taskState(tid("job", 0)); got != types.TaskCompleted {
		t.Fatalf("重复/乱序事件后任务状态=%s，期望保持 COMPLETED", got)
	}
	h.waitJobState("job", types.JobCompleted)

	// 4) 旧实例事件：抢占后新实例启动，旧实例的 completed 不能影响新实例。
	h2 := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 1},
	})
	h2.setSpec("low", cluster.TaskSpec{Duration: 1 * time.Hour})
	// hi 短任务：抢占 low 后很快结束，low 得以重新启动产生新实例。
	h2.start()
	defer h2.close()
	if err := h2.submit(types.JobSpec{
		Name: "low", Tenant: "a", Priority: 1, Procs: 1,
		ProcPerTask: types.Resources{CPU: 1}, Interruptible: true,
	}); err != nil {
		t.Fatal(err)
	}
	h2.waitFor("low RUNNING", time.Second, func() bool {
		return h2.taskState(tid("low", 0)) == types.TaskRunning
	})
	oldInst, _ := h2.sch.TaskAttempts(tid("low", 0))

	if err := h2.submit(types.JobSpec{
		Name: "hi", Tenant: "a", Priority: 9, Procs: 1,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	// 抢占发生：low 回 PENDING，随后 hi 结束、low 重新整组启动产生新实例。
	h2.waitFor("low 重新启动为新实例", 2*time.Second, func() bool {
		inst2, _ := h2.sch.TaskAttempts(tid("low", 0))
		return h2.taskState(tid("low", 0)) == types.TaskRunning && inst2 > oldInst
	})
	// 伪造旧实例迟到的 completed：必须被实例过滤忽略。
	h2.sch.HandleEvent(context.Background(), types.Event{
		TaskID: tid("low", 0), Kind: types.EventCompleted, NodeName: "n1",
		Instance: oldInst, Sequence: 9999,
	})
	if got := h2.taskState(tid("low", 0)); got != types.TaskRunning {
		t.Fatalf("旧实例事件把新实例状态改成 %s，期望保持 RUNNING", got)
	}
}

// TestRetryAndTimeout 失败重试预算与执行超时：
// 前两次真实失败后重试成功；另一个长任务超时、重试仍超时后整任务 FAILED。
func TestRetryAndTimeout(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 2},
	})
	h.failFirstAttempts("flaky", 2)
	h.setSpec("to", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "flaky", Tenant: "a", Procs: 1, Retries: 2,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.submit(types.JobSpec{
		Name: "to", Tenant: "a", Procs: 1, Retries: 1, TimeoutMs: 200,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}

	h.waitJobState("flaky", types.JobCompleted)
	h.waitJobState("to", types.JobFailed)

	fc, _ := h.sch.TaskFailCount(tid("flaky", 0))
	if fc != 2 {
		t.Fatalf("flaky 真实失败次数=%d，期望 2", fc)
	}
	fc2, _ := h.sch.TaskFailCount(tid("to", 0))
	if fc2 != 2 {
		t.Fatalf("to 超时失败次数=%d，期望 2（1 次首发 + 1 次重试）", fc2)
	}
	// 终态后账本归零。
	n := h.sch.LedgerSnapshot()["n1"]
	if n.Running != 0 || n.Used != (types.Resources{}) {
		t.Fatalf("终态后账本未归零: %+v", n)
	}
}

// TestNodeLostRepeatedAndRecovery 节点失联（含重复事件）把在跑任务整组打回队列、
// 账本同步回收；节点恢复后任务重新整组跑完。
func TestNodeLostRepeatedAndRecovery(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
		"n2": {CPU: 4},
	})
	h.setSpec("gang", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	if err := h.submit(types.JobSpec{
		Name: "gang", Tenant: "a", Procs: 6,
		ProcPerTask: types.Resources{CPU: 1},
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("gang 整组 RUNNING", time.Second, func() bool {
		c := 0
		for i := 0; i < 6; i++ {
			if h.taskState(tid("gang", i)) == types.TaskRunning {
				c++
			}
		}
		return c == 6
	})

	// 失联事件连发 3 次：只有第一次生效。
	for i := 0; i < 3; i++ {
		h.sch.HandleEvent(context.Background(), types.Event{Kind: types.EventNodeLost, NodeName: "n1"})
	}
	time.Sleep(50 * time.Millisecond)

	// n1 账本必须清零，原在 n1 的任务全部回 PENDING（n2 上的任务不受影响）。
	if n1 := h.sch.LedgerSnapshot()["n1"]; n1.Used != (types.Resources{}) || n1.Running != 0 {
		t.Fatalf("失联后 n1 未清零: %+v", n1)
	}
	pending := 0
	for i := 0; i < 6; i++ {
		switch h.taskState(tid("gang", i)) {
		case types.TaskPending:
			pending++
		case types.TaskRunning:
			// 只能在 n2 上。
		}
	}
	if pending == 0 {
		t.Fatal("失联后没有任何任务回队")
	}
	h.assertLedgerWithinCapacity()

	// 恢复节点，整组应重新补齐：最终 6 个任务全部 RUNNING。
	h.sch.HandleEvent(context.Background(), types.Event{Kind: types.EventNodeUp, NodeName: "n1"})
	h.waitFor("节点恢复后 gang 重新整组 RUNNING", 2*time.Second, func() bool {
		c := 0
		for i := 0; i < 6; i++ {
			if h.taskState(tid("gang", i)) == types.TaskRunning {
				c++
			}
		}
		return c == 6
	})
	h.assertLedgerWithinCapacity()

	// 失联不消耗重试预算。
	for i := 0; i < 6; i++ {
		if fc, _ := h.sch.TaskFailCount(tid("gang", i)); fc != 0 {
			t.Fatalf("gang[%d] 失联导致失败计数=%d，期望 0", i, fc)
		}
	}
	h.logLedger()
}

// TestRecoverFromSnapshot 调度器重建后从持久化快照继续：
// 已完成的作业保持完成、排队/运行的作业继续推进，不从头重新提交。
func TestRecoverFromSnapshot(t *testing.T) {
	clus := cluster.NewCluster()
	clus.AddNode(cluster.NewNode("n1", types.Resources{CPU: 2}))
	st := store.NewMemoryStore()

	// 第一个"进程"：跑一个短作业到完成，再启动一个长作业并失联节点，
	// 留下含终态作业 + 失联节点 + 排队任务的快照。
	{
		var sch *Scheduler
		exec := cluster.NewExecutor(func(ev types.Event) {
			sch.HandleEvent(context.Background(), ev)
		}, func(id types.TaskID) cluster.TaskSpec {
			if id.Job == "long" {
				return cluster.TaskSpec{Duration: 1 * time.Hour}
			}
			return cluster.TaskSpec{Duration: 10 * time.Millisecond}
		})
		sch = New(Config{TickInterval: 5 * time.Millisecond}, clus, exec, st,
			func(f string, a ...any) { t.Logf("[p1] "+f, a...) })
		if err := sch.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sch.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sch.Submit(context.Background(), types.JobSpec{
			Name: "done", Tenant: "a", Procs: 2,
			ProcPerTask: types.Resources{CPU: 1},
		}); err != nil {
			t.Fatal(err)
		}
		if err := sch.Submit(context.Background(), types.JobSpec{
			Name: "long", Tenant: "a", Procs: 1,
			ProcPerTask: types.Resources{CPU: 1},
		}); err != nil {
			t.Fatal(err)
		}
		wait := time.Now().Add(2 * time.Second)
		for time.Now().Before(wait) {
			if s, _ := sch.JobState("done"); s == types.JobCompleted {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if s, _ := sch.JobState("done"); s != types.JobCompleted {
			t.Fatalf("done 状态=%s", s)
		}
		// 模拟节点失联后进程退出：快照里留下失联状态。
		sch.HandleEvent(context.Background(), types.Event{Kind: types.EventNodeLost, NodeName: "n1"})
		time.Sleep(50 * time.Millisecond)
		sch.Close()
	}

	// 第二个"进程"：全新执行器与账本，只凭快照恢复。
	clus2 := cluster.NewCluster()
	clus2.AddNode(cluster.NewNode("n1", types.Resources{CPU: 2}))
	{
		var sch2 *Scheduler
		exec2 := cluster.NewExecutor(func(ev types.Event) {
			sch2.HandleEvent(context.Background(), ev)
		}, func(id types.TaskID) cluster.TaskSpec {
			return cluster.TaskSpec{Duration: 15 * time.Millisecond}
		})
		sch2 = New(Config{TickInterval: 5 * time.Millisecond}, clus2, exec2, st,
			func(f string, a ...any) { t.Logf("[p2] "+f, a...) })
		if err := sch2.Recover(context.Background()); err != nil {
			t.Fatalf("恢复失败: %v", err)
		}
		if err := sch2.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer sch2.Close()

		// 已完成作业从快照保持终态，不重新运行。
		if s, ok := sch2.JobState("done"); !ok || s != types.JobCompleted {
			t.Fatalf("恢复后 done 状态=%v ok=%v，期望 COMPLETED（从快照继续而非重头开始）", s, ok)
		}
		// long 的 RUNNING 记录应被恢复为 PENDING 并因 n1 仍失联而等待。
		if got, _ := sch2.TaskState(tid("long", 0)); got != types.TaskPending {
			t.Fatalf("恢复后 long[0] 状态=%s，期望 PENDING", got)
		}
		// 节点恢复后，long 被重新调度并跑完。
		sch2.HandleEvent(context.Background(), types.Event{Kind: types.EventNodeUp, NodeName: "n1"})
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if s, _ := sch2.JobState("long"); s == types.JobCompleted {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if s, _ := sch2.JobState("long"); s != types.JobCompleted {
			t.Fatalf("恢复推进后 long 状态=%s，期望 COMPLETED", s)
		}
	}
}
