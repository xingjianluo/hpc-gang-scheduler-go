package scheduler

import (
	"testing"
)

// TestGangExceedsClusterRejected 验证“整组注定跑不起来”的作业在提交时被整组拒绝，
// 且原因能与配额不足、单任务超规格区分开；异构节点下判断必须精确。
func TestGangExceedsClusterRejected(t *testing.T) {
	// 异构集群：n1 8 CPU，n2 4 CPU。
	nodes := []*Node{
		{ID: "n1", Capacity: Resource{CPU: 8, Memory: 1024}, Alive: true},
		{ID: "n2", Capacity: Resource{CPU: 4, Memory: 512}, Alive: true},
	}
	quotas := map[string]Resource{"bob": {CPU: 64, Memory: 8192}}
	s := New(nodes, quotas, nil)
	s.SetLogger(t.Logf)

	// 1) 单任务 5 CPU 能放进 n1，但每个节点最多放 1 个（8/5=1, 4/5=0），
	//    整组 2 个超过集群物理上限 1 —— 不能只看最宽节点觉得 10<=12 就放行。
	err := s.Submit(JobSpec{JobID: "gang-het", Owner: "bob", Count: 2, Requires: Resource{CPU: 5}})
	if RejectReasonOf(err) != RejectGangTooLarge {
		t.Fatalf("heterogeneous: want GANG_EXCEEDS_CLUSTER, got %v", err)
	}

	// 2) 总量看似够但装箱放不下：2 x 6CPU = 12 <= 集群总量 12，但 n1 只能放 1 个、
	//    n2 一个也放不下，上限 1，count=2 永远排不上。
	err = s.Submit(JobSpec{JobID: "gang-total-ok", Owner: "bob", Count: 2, Requires: Resource{CPU: 6}})
	if RejectReasonOf(err) != RejectGangTooLarge {
		t.Fatalf("total-fits-but-unplaceable: want GANG_EXCEEDS_CLUSTER, got %v", err)
	}

	// 2b) 整组总量直接超过集群总量：4 x 4CPU = 16 > 12。
	err = s.Submit(JobSpec{JobID: "gang-over-total", Owner: "bob", Count: 4, Requires: Resource{CPU: 4}})
	if RejectReasonOf(err) != RejectGangTooLarge {
		t.Fatalf("over-total: want GANG_EXCEEDS_CLUSTER, got %v", err)
	}

	// 3) 内存维度成为瓶颈：CPU 够，但每节点内存只够 1 个任务。
	err = s.Submit(JobSpec{JobID: "gang-mem", Owner: "bob", Count: 3, Requires: Resource{CPU: 1, Memory: 600}})
	if RejectReasonOf(err) != RejectGangTooLarge {
		t.Fatalf("memory-bound: want GANG_EXCEEDS_CLUSTER, got %v", err)
	}

	// 4) 边界：整组数恰好等于集群上限（2 个 4CPU 任务，n1/n2 各一个）必须放行。
	if err = s.Submit(JobSpec{JobID: "gang-exact", Owner: "bob", Count: 2, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatalf("exact-fit gang should be accepted, got %v", err)
	}
	s.Schedule()
	j, _ := s.Job("gang-exact")
	if j.State != JobRunning {
		t.Fatalf("exact-fit gang should run, got %s", j.State)
	}
	logLedger(t, s, "exact-fit-running")

	// 5) 三种拒绝原因可区分：单任务超规格、配额不足、整组超上限。
	s2 := New(nodes, map[string]Resource{"tiny": {CPU: 2, Memory: 128}, "roomy": {CPU: 64, Memory: 8192}}, nil)
	s2.SetLogger(t.Logf)
	if err = s2.Submit(JobSpec{JobID: "r-task", Owner: "roomy", Count: 1, Requires: Resource{CPU: 16}}); RejectReasonOf(err) != RejectResourceUnfits {
		t.Fatalf("want RESOURCE_NEVER_FITS, got %v", err)
	}
	if err = s2.Submit(JobSpec{JobID: "r-quota", Owner: "tiny", Count: 1, Requires: Resource{CPU: 4}}); RejectReasonOf(err) != RejectQuotaExceeded {
		t.Fatalf("want QUOTA_EXCEEDED, got %v", err)
	}
	if err = s2.Submit(JobSpec{JobID: "r-gang", Owner: "roomy", Count: 3, Requires: Resource{CPU: 6}}); RejectReasonOf(err) != RejectGangTooLarge {
		t.Fatalf("want GANG_EXCEEDS_CLUSTER, got %v", err)
	}
	assertLedgerSane(t, s)
	assertLedgerSane(t, s2)
}

// TestNodeLossGangRollbackRestart 验证节点失联时整组回退：健康节点上的同伴一起停下、
// 账本还干净、不消耗重试次数；资源不够时整组等待，够了再整组拉起。
func TestNodeLossGangRollbackRestart(t *testing.T) {
	nodes := []*Node{
		{ID: "n1", Capacity: Resource{CPU: 8}, Alive: true},
		{ID: "n2", Capacity: Resource{CPU: 8}, Alive: true},
		{ID: "n3", Capacity: Resource{CPU: 8}, Alive: true},
	}
	s := New(nodes, bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 作业 A：2 任务 x 8CPU，落在 n1、n2；作业 B 占住 n3。
	if err := s.Submit(JobSpec{JobID: "A", Owner: "bob", Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(JobSpec{JobID: "B", Owner: "carol", Count: 1, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	logLedger(t, s, "round1")
	ja, _ := s.Job("A")
	if ja.State != JobRunning {
		t.Fatalf("A should be running, got %s", ja.State)
	}

	// n1 失联：A 整组回退（n2 上的同伴也停下），只剩 n2 一台健康节点放不下整组，
	// 必须整组等待，不能半跑半等。
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: "n1"})
	logLedger(t, s, "after-node-lost")
	ja, _ = s.Job("A")
	if ja.State != JobQueued {
		t.Fatalf("A should roll back to queued, got %s", ja.State)
	}
	s.Schedule() // 资源不够，整组继续等待
	ja, _ = s.Job("A")
	if ja.State != JobQueued {
		t.Fatalf("A should stay queued while gang does not fit, got %s", ja.State)
	}
	for _, task := range ja.Tasks {
		if task.State == TaskRunning {
			t.Fatalf("half-running gang detected: task %s running while gang waits", task.Spec.TaskID)
		}
		if task.Attempt != 0 {
			t.Fatalf("rollback consumed retry: task %s attempt=%d", task.Spec.TaskID, task.Attempt)
		}
	}
	// 账本：只有 B 占 n3 的 8 CPU，其余全部还干净。
	for nid, n := range s.LedgerSnapshot() {
		want := int64(0)
		if nid == "n3" {
			want = 8
		}
		if n.Used.CPU != want {
			t.Fatalf("ledger dirty after rollback: node=%s used=%d want=%d", nid, n.Used.CPU, want)
		}
	}
	assertLedgerSane(t, s)

	// n1 恢复：整组在同一轮一起拉起。
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: "n1"})
	s.Schedule()
	logLedger(t, s, "after-gang-restart")
	ja, _ = s.Job("A")
	if ja.State != JobRunning {
		t.Fatalf("A should gang-restart after recovery, got %s", ja.State)
	}
	for _, task := range ja.Tasks {
		if task.State != TaskRunning {
			t.Fatalf("gang restart incomplete: task %s state=%s", task.Spec.TaskID, task.State)
		}
		if task.Generation != 2 {
			t.Fatalf("gang restart should bump generation: task %s gen=%d, want 2", task.Spec.TaskID, task.Generation)
		}
	}
	assertLedgerSane(t, s)
}

// findTask 按状态找任务，避免对抢占受害者选择的顺序做假设。
func findTask(j *Job, want TaskState) *Task {
	for _, t := range j.Tasks {
		if t.State == want {
			return t
		}
	}
	return nil
}

// TestCrossInstanceStaleEvents 验证跨实例迟到事件：任务被抢占/失联重排后已在新一代
// 重新运行，旧一代实例发来的完成/失败通知不能判定当前轮；重复通知不重复计数；
// 作业成败以当前运行代次为准。
func TestCrossInstanceStaleEvents(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 低优先级可抢占作业占满两节点（每个任务第一代实例，gen=1）。
	if err := s.Submit(JobSpec{JobID: "low", Owner: "bob", Priority: 1, Preemptible: true, Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	// 高优先级作业抢占其中一个任务。
	if err := s.Submit(JobSpec{JobID: "high", Owner: "carol", Priority: 10, Count: 1, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	logLedger(t, s, "after-preempt")
	jl, _ := s.Job("low")
	victim := findTask(jl, TaskPreempted)
	if victim == nil {
		t.Fatal("expected one preempted task")
	}
	survivor := findTask(jl, TaskRunning)
	if survivor == nil {
		t.Fatal("expected one surviving task")
	}
	t.Logf("victim=%s gen=%d, survivor=%s gen=%d", victim.Spec.TaskID, victim.Generation, survivor.Spec.TaskID, survivor.Generation)

	// 高优先级作业完成，被抢占任务在新一代实例（gen=2）重新拉起。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "high", TaskID: "high#0", Success: true, Generation: 1})
	s.Schedule()
	logLedger(t, s, "after-replace")
	jl, _ = s.Job("low")
	victim = findTask(jl, TaskRunning)
	if victim == nil || victim.Generation != 2 {
		t.Fatalf("victim should be re-running at gen=2, got %+v", victim)
	}

	// 旧实例（gen=1）迟到的“失败”通知：不能把当前这一轮判失败。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: victim.Spec.TaskID, Success: false, Generation: 1})
	// 旧实例迟到的“完成”通知：也不能把当前这一轮判完成。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: victim.Spec.TaskID, Success: true, Generation: 1})
	jl, _ = s.Job("low")
	if jl.FinishedN != 0 || jl.State != JobRunning {
		t.Fatalf("stale cross-instance events must be ignored: finishedN=%d state=%s", jl.FinishedN, jl.State)
	}
	if findTask(jl, TaskRunning) == nil {
		t.Fatal("current generation must keep running after stale events")
	}

	// 当前代次（gen=2）的完成通知才作数；重复送达不重复计数。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: victim.Spec.TaskID, Success: true, Generation: 2})
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: victim.Spec.TaskID, Success: true, Generation: 2})
	jl, _ = s.Job("low")
	if jl.FinishedN != 1 {
		t.Fatalf("duplicate completion double-counted: finishedN=%d", jl.FinishedN)
	}

	// 幸存任务（仍是第一代实例）用 gen=1 上报完成，作业整体成功——
	// 最终成败按各任务当前运行代次的通知判定。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: survivor.Spec.TaskID, Success: true, Generation: 1})
	jl, _ = s.Job("low")
	if jl.State != JobSucceeded {
		t.Fatalf("job should succeed based on current generations, got %s", jl.State)
	}
	// 终态后任何迟到通知（无论哪一代）都不能把任务改回运行或改判失败。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "low", TaskID: victim.Spec.TaskID, Success: false, Generation: 3})
	jl, _ = s.Job("low")
	if jl.State != JobSucceeded {
		t.Fatalf("terminal job state regressed: %s", jl.State)
	}
	logLedger(t, s, "final")
	assertLedgerSane(t, s)
}

// TestNodeLossRollbackStaleGeneration 验证节点失联整组回退并重新拉起后，
// 旧一代实例的迟到通知同样被代次检查拦截。
func TestNodeLossRollbackStaleGeneration(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	if err := s.Submit(JobSpec{JobID: "g", Owner: "bob", Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()

	// n1 失联 -> 整组回退 -> 节点恢复 -> 整组以新一代实例拉起（gen=2）。
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: "n1"})
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: "n1"})
	s.Schedule()
	j, _ := s.Job("g")
	if j.State != JobRunning {
		t.Fatalf("gang should restart after recovery, got %s", j.State)
	}
	for _, task := range j.Tasks {
		if task.Generation != 2 {
			t.Fatalf("restart should bump generation: task %s gen=%d", task.Spec.TaskID, task.Generation)
		}
	}

	// 旧一代（gen=1）迟到的完成/失败通知都被忽略，不计数、不改判。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g", TaskID: "g#0", Success: true, Generation: 1})
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g", TaskID: "g#1", Success: false, Generation: 1})
	j, _ = s.Job("g")
	if j.FinishedN != 0 || j.State != JobRunning {
		t.Fatalf("stale gen events after rollback must be ignored: finishedN=%d state=%s", j.FinishedN, j.State)
	}

	// 当前代次上报才作数，作业成功。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g", TaskID: "g#0", Success: true, Generation: 2})
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g", TaskID: "g#1", Success: true, Generation: 2})
	j, _ = s.Job("g")
	if j.State != JobSucceeded {
		t.Fatalf("job should succeed on current-generation completions, got %s", j.State)
	}
	logLedger(t, s, "final")
	assertLedgerSane(t, s)
}

