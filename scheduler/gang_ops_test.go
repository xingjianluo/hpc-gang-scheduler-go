package scheduler

import (
	"fmt"
	"testing"
)

// heteroNodes 返回规格不一致的节点：n1 8 CPU，n2 4 CPU。
func heteroNodes() []*Node {
	return []*Node{
		{ID: "n1", Capacity: Resource{CPU: 8, Memory: 1024}, Alive: true},
		{ID: "n2", Capacity: Resource{CPU: 4, Memory: 1024}, Alive: true},
	}
}

func TestGangExceedsClusterReject(t *testing.T) {
	s := New(heteroNodes(), map[string]Resource{
		"bob":   {CPU: 64, Memory: 8192}, // 配额足够大，排除配额因素
		"alice": {CPU: 4, Memory: 1024},  // 小配额，用于对照
	}, nil)
	s.SetLogger(t.Logf)

	// A：单任务放得下（8<=n1），但整组 2x8=16 超过集群总量 12。
	err := s.Submit(JobSpec{JobID: "too-big-total", Owner: "bob", Count: 2, Requires: Resource{CPU: 8}})
	if RejectReasonOf(err) != RejectGangExceedsCluster {
		t.Fatalf("want GANG_EXCEEDS_CLUSTER, got %v", err)
	}

	// B：整组总量 2x6=12 不超过集群总量 12，但节点规格不一致——
	// 6 只放得进 n1，第二个 6 无处可去。不能只看最宽节点或总量。
	err = s.Submit(JobSpec{JobID: "hetero-unfit", Owner: "bob", Count: 2, Requires: Resource{CPU: 6}})
	if RejectReasonOf(err) != RejectGangExceedsCluster {
		t.Fatalf("want GANG_EXCEEDS_CLUSTER for hetero case, got %v", err)
	}

	// C：与既有拒绝原因可区分——单任务超任何节点仍是 RESOURCE_NEVER_FITS。
	err = s.Submit(JobSpec{JobID: "per-task-unfit", Owner: "bob", Count: 1, Requires: Resource{CPU: 16}})
	if RejectReasonOf(err) != RejectResourceUnfits {
		t.Fatalf("want RESOURCE_NEVER_FITS, got %v", err)
	}
	// 整组放得下集群但超提交方配额，仍是 QUOTA_EXCEEDED。
	err = s.Submit(JobSpec{JobID: "quota-hit", Owner: "alice", Count: 2, Requires: Resource{CPU: 4}})
	if RejectReasonOf(err) != RejectQuotaExceeded {
		t.Fatalf("want QUOTA_EXCEEDED, got %v", err)
	}

	// 被拒绝的作业不能留在队列里占位。
	for _, id := range []string{"too-big-total", "hetero-unfit", "per-task-unfit", "quota-hit"} {
		if _, ok := s.Job(id); ok {
			t.Fatalf("rejected job %s must not be tracked", id)
		}
	}

	// D：恰好放得进空集群的作业（2x4：n1 一个、n2 一个）必须被接受，
	// 集群被占时整组等待，资源释放后能整组拉起。
	if err := s.Submit(JobSpec{JobID: "fits-exactly", Owner: "bob", Count: 2, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatalf("gang that exactly fits empty cluster must be accepted: %v", err)
	}
	s.Schedule()
	j, _ := s.Job("fits-exactly")
	if j.State != JobRunning {
		t.Fatalf("fits-exactly should run, got %s", j.State)
	}
	logLedger(t, s, "fits-exactly")
	assertLedgerSane(t, s)
}

func TestNodeLossGangRollbackPartialRestart(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 两个 8 CPU 任务分别落在 n1、n2。
	if err := s.Submit(JobSpec{JobID: "g1", Owner: "bob", Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("g1")
	t.Logf("placements: %s -> %s (gen=%d), %s -> %s (gen=%d)",
		j.Tasks[0].Spec.TaskID, j.Tasks[0].NodeID, j.Tasks[0].Generation,
		j.Tasks[1].Spec.TaskID, j.Tasks[1].NodeID, j.Tasks[1].Generation)

	// 先完成其中一个任务（成功终态）。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g1", TaskID: "g1#0", Success: true})
	j, _ = s.Job("g1")
	if j.Tasks[0].State != TaskSucceeded {
		t.Fatalf("g1#0 should be succeeded, got %s", j.Tasks[0].State)
	}

	// 用一个高优先级作业占住刚空出的 8 CPU，制造“资源不够”的窗口。
	if err := s.Submit(JobSpec{JobID: "blocker", Owner: "carol", Priority: 5, Count: 1, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	jb, _ := s.Job("blocker")
	if jb.State != JobRunning {
		t.Fatalf("blocker should be running, got %s", jb.State)
	}

	// 另一个任务所在节点失联：整组回退，但已成功的任务保持终态不重跑。
	j, _ = s.Job("g1")
	lostNode := j.Tasks[1].NodeID
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: lostNode})
	j, _ = s.Job("g1")
	if j.State != JobQueued {
		t.Fatalf("job should roll back to queued, got %s", j.State)
	}
	if j.Tasks[0].State != TaskSucceeded {
		t.Fatalf("succeeded task must survive rollback, got %s", j.Tasks[0].State)
	}
	if j.Tasks[1].State != TaskPending || j.Tasks[1].Attempt != 0 {
		t.Fatalf("rollback: task=%s attempt=%d, want PENDING attempt=0", j.Tasks[1].State, j.Tasks[1].Attempt)
	}
	logLedger(t, s, "rolled-back")
	assertLedgerSane(t, s)

	// 资源不够（健康节点被 blocker 占满、另一节点失联）时整组保持等待。
	s.Schedule()
	j, _ = s.Job("g1")
	if j.Tasks[1].State != TaskPending {
		t.Fatalf("gang must not restart without resources, task=%s", j.Tasks[1].State)
	}

	// 节点恢复：只重跑剩下的那个任务，已成功的任务不动。
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: lostNode})
	s.Schedule()
	j, _ = s.Job("g1")
	if j.Tasks[0].State != TaskSucceeded || j.Tasks[0].Generation != 1 {
		t.Fatalf("succeeded task must not be re-run: state=%s gen=%d", j.Tasks[0].State, j.Tasks[0].Generation)
	}
	if j.Tasks[1].State != TaskRunning || j.Tasks[1].Generation != 2 {
		t.Fatalf("remaining task should restart with gen=2: state=%s gen=%d", j.Tasks[1].State, j.Tasks[1].Generation)
	}
	logLedger(t, s, "restarted")
	assertLedgerSane(t, s)

	// 账本与运行中任务占用一致：blocker 8 + g1#1 8 = 16，不允许重复占用。
	var used int64
	for _, n := range s.LedgerSnapshot() {
		used += n.Used.CPU
	}
	if used != 16 {
		t.Fatalf("ledger used=%d, want 16 (no double allocation)", used)
	}

	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "g1", TaskID: "g1#1", Success: true, Generation: 2})
	j, _ = s.Job("g1")
	if j.State != JobSucceeded || j.FinishedN != 2 {
		t.Fatalf("job should succeed: state=%s finishedN=%d", j.State, j.FinishedN)
	}
	assertLedgerSane(t, s)
}

func TestStaleInstanceEvents(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 可抢占的低优先级作业，两个 8 CPU 任务占满集群。
	if err := s.Submit(JobSpec{JobID: "victim", Owner: "bob", Priority: 1, Preemptible: true, Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("victim")
	for _, task := range j.Tasks {
		if task.State != TaskRunning || task.Generation != 1 {
			t.Fatalf("victim tasks should run with gen=1: %s state=%s gen=%d", task.Spec.TaskID, task.State, task.Generation)
		}
	}

	// 高优先级作业到来，集群已满，必须抢占其中一个 victim 任务。
	if err := s.Submit(JobSpec{JobID: "urgent", Owner: "carol", Priority: 10, Count: 1, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ = s.Job("victim")
	var preemptedID string
	for _, task := range j.Tasks {
		if task.State == TaskPreempted {
			preemptedID = task.Spec.TaskID
		}
	}
	if preemptedID == "" {
		t.Fatal("one victim task should be preempted")
	}

	// 高优先级作业完成后，被抢占任务在新一轮重新跑起来（gen=2）。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "urgent", TaskID: "urgent#0", Success: true})
	s.Schedule()
	j, _ = s.Job("victim")
	var rerun *Task
	for _, task := range j.Tasks {
		if task.Spec.TaskID == preemptedID {
			rerun = task
		}
	}
	if rerun.State != TaskRunning || rerun.Generation != 2 {
		t.Fatalf("preempted task should rerun with gen=2: state=%s gen=%d", rerun.State, rerun.Generation)
	}
	logLedger(t, s, "rerun-gen2")

	// 上一轮实例（gen=1）迟到的完成通知：不能判当前这一轮完成。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: true, Generation: 1})
	j, _ = s.Job("victim")
	if j.FinishedN != 0 || rerun.State != TaskRunning {
		t.Fatalf("stale gen=1 completion must be ignored: finishedN=%d state=%s", j.FinishedN, rerun.State)
	}
	// 上一轮实例迟到的失败通知：也不能把当前这一轮判失败。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: false, Generation: 1})
	j, _ = s.Job("victim")
	if j.State == JobFailed {
		t.Fatalf("stale gen=1 failure must not fail current run: job=%s", j.State)
	}
	for _, task := range j.Tasks {
		if task.Spec.TaskID == preemptedID && task.State != TaskRunning {
			t.Fatalf("stale gen=1 failure changed current run: state=%s", task.State)
		}
	}

	// 未来代数（gen=3，对不上任何已知实例）同样忽略。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: true, Generation: 3})
	j, _ = s.Job("victim")
	if j.FinishedN != 0 {
		t.Fatalf("unknown generation must be ignored: finishedN=%d", j.FinishedN)
	}

	// 当前这一轮（gen=2）的完成通知被受理；重复送达不计翻倍。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: true, Generation: 2})
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: true, Generation: 2})
	j, _ = s.Job("victim")
	if j.FinishedN != 1 {
		t.Fatalf("current generation completion should count exactly once: finishedN=%d", j.FinishedN)
	}
	// 终态后任何迟到通知都不能把任务改回运行/失败。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "victim", TaskID: preemptedID, Success: false, Generation: 2})
	j, _ = s.Job("victim")
	for _, task := range j.Tasks {
		if task.Spec.TaskID == preemptedID && task.State != TaskSucceeded {
			t.Fatalf("terminal task regressed: %s", task.State)
		}
	}
	logLedger(t, s, "final")
	assertLedgerSane(t, s)
}

func TestStaleInstanceEventsAfterNodeLoss(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	if err := s.Submit(JobSpec{JobID: "nl", Owner: "bob", Count: 1, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("nl")
	lostNode := j.Tasks[0].NodeID
	gen := j.Tasks[0].Generation

	// 节点失联 -> 整组回退 -> 节点恢复 -> 重新拉起（gen+1）。
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: lostNode})
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: lostNode})
	s.Schedule()
	j, _ = s.Job("nl")
	if j.Tasks[0].State != TaskRunning || j.Tasks[0].Generation != gen+1 {
		t.Fatalf("task should rerun with gen=%d: state=%s gen=%d", gen+1, j.Tasks[0].State, j.Tasks[0].Generation)
	}

	// 失联前那一代实例迟到的失败通知不能影响新一轮。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "nl", TaskID: "nl#0", Success: false, Generation: gen})
	j, _ = s.Job("nl")
	if j.State == JobFailed || j.Tasks[0].State != TaskRunning {
		t.Fatalf("stale failure from lost node must be ignored: job=%s task=%s", j.State, j.Tasks[0].State)
	}
	// 当前一代的完成通知正常受理。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "nl", TaskID: "nl#0", Success: true, Generation: gen + 1})
	j, _ = s.Job("nl")
	if j.State != JobSucceeded {
		t.Fatalf("job should succeed on current generation, got %s", j.State)
	}
	logLedger(t, s, "final")
	assertLedgerSane(t, s)
}

// TestLegacyEventsWithoutGeneration 验证不携带代数的事件保持既有语义（兼容旧行为）。
func TestLegacyEventsWithoutGeneration(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	if err := s.Submit(JobSpec{JobID: "legacy", Owner: "bob", Count: 1, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	// Generation=0（未指定）：按状态机判断，RUNNING 即受理。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "legacy", TaskID: "legacy#0", Success: true})
	j, _ := s.Job("legacy")
	if j.State != JobSucceeded {
		t.Fatalf("legacy event without generation should be accepted, got %s", j.State)
	}
}

func TestGangRollbackConcurrency(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	for i := 0; i < 8; i++ {
		if err := s.Submit(JobSpec{JobID: fmt.Sprintf("rb%d", i), Owner: "bob", Count: 2, Requires: Resource{CPU: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	s.Schedule()

	done := make(chan struct{})
	go func() {
		for k := 0; k < 50; k++ {
			s.Schedule()
		}
		close(done)
	}()
	// 并发地反复失联/恢复节点，触发整组回退与重新拉起。
	for k := 0; k < 50; k++ {
		s.HandleEvent(Event{Type: EventNodeLost, NodeID: "n1"})
		s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: "n1"})
	}
	<-done
	s.Schedule()

	assertLedgerSane(t, s)
	// 账本与运行中任务占用必须一致（回退/重拉不能重复占用）。
	var ledgerUsed int64
	for _, n := range s.LedgerSnapshot() {
		ledgerUsed += n.Used.CPU
	}
	var running int64
	for i := 0; i < 8; i++ {
		j, _ := s.Job(fmt.Sprintf("rb%d", i))
		for _, task := range j.Tasks {
			if task.State == TaskRunning {
				running += 2
			}
		}
	}
	if ledgerUsed != running {
		t.Fatalf("ledger used=%d but running tasks hold=%d", ledgerUsed, running)
	}
	logLedger(t, s, "rollback-concurrent-final")
}
