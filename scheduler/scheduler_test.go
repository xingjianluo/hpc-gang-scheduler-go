package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func twoNodes() []*Node {
	return []*Node{
		{ID: "n1", Capacity: Resource{CPU: 8, Memory: 1024}, Alive: true},
		{ID: "n2", Capacity: Resource{CPU: 8, Memory: 1024}, Alive: true},
	}
}

func bigQuota() map[string]Resource {
	return map[string]Resource{
		"alice": {CPU: 8, Memory: 1024},
		"bob":   {CPU: 64, Memory: 8192},
		"carol": {CPU: 64, Memory: 8192},
	}
}

func logLedger(t *testing.T, s *Scheduler, prefix string) {
	t.Helper()
	for nid, n := range s.LedgerSnapshot() {
		t.Logf("%s ledger node=%s used=%+v cap=%+v alive=%v", prefix, nid, n.Used, n.Capacity, n.Alive)
	}
}

// assertLedgerSane 断言任何时刻已用资源不超过节点容量且不为负。
func assertLedgerSane(t *testing.T, s *Scheduler) {
	t.Helper()
	for nid, n := range s.LedgerSnapshot() {
		if !n.Used.Fits(n.Capacity) {
			t.Fatalf("node %s over-allocated: used=%+v cap=%+v", nid, n.Used, n.Capacity)
		}
		if n.Used.CPU < 0 || n.Used.Memory < 0 || n.Used.GPU < 0 {
			t.Fatalf("node %s negative usage: %+v", nid, n.Used)
		}
	}
}

func TestGangRejectReasons(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 正常提交：2 任务数组，每任务 4 CPU，共 8 CPU，占满 alice 配额。
	if err := s.Submit(JobSpec{JobID: "a1", Owner: "alice", Count: 2, Requires: Resource{CPU: 4, Memory: 256}}); err != nil {
		t.Fatalf("submit a1: %v", err)
	}
	// 重复提交。
	if err := s.Submit(JobSpec{JobID: "a1", Owner: "alice", Count: 1, Requires: Resource{CPU: 1}}); RejectReasonOf(err) != RejectDuplicate {
		t.Fatalf("want DUPLICATE, got %v", err)
	}
	// 超出配额（alice 配额 8 CPU 已占满）。
	if err := s.Submit(JobSpec{JobID: "a2", Owner: "alice", Count: 1, Requires: Resource{CPU: 1}}); RejectReasonOf(err) != RejectQuotaExceeded {
		t.Fatalf("want QUOTA_EXCEEDED, got %v", err)
	}
	// 单任务需求超过任何节点容量。
	if err := s.Submit(JobSpec{JobID: "b1", Owner: "bob", Count: 1, Requires: Resource{CPU: 16}}); RejectReasonOf(err) != RejectResourceUnfits {
		t.Fatalf("want RESOURCE_NEVER_FITS, got %v", err)
	}
	logLedger(t, s, "after-rejects")
	assertLedgerSane(t, s)
}

func TestGangAllOrNothing(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 整组需要 12 CPU，集群共 16 但单节点 8：先占 8，再提交需要 12 的整组作业。
	if err := s.Submit(JobSpec{JobID: "b1", Owner: "bob", Count: 2, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	// 需要 3*4=12 CPU，剩余 8，整组放不下，必须整体等待，不能部分启动。
	if err := s.Submit(JobSpec{JobID: "b2", Owner: "bob", Count: 3, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("b2")
	if j.State != JobQueued {
		t.Fatalf("b2 should stay queued, got %s", j.State)
	}
	for _, task := range j.Tasks {
		if task.State != TaskPending {
			t.Fatalf("partial gang start detected: task %s state=%s", task.Spec.TaskID, task.State)
		}
	}
	logLedger(t, s, "gang-blocked")
	// b1 占 8 CPU，b2 一个任务都不许启动。
	used := int64(0)
	for _, n := range s.LedgerSnapshot() {
		used += n.Used.CPU
	}
	if used != 8 {
		t.Fatalf("ledger used=%d, want 8 (no partial allocation)", used)
	}
	assertLedgerSane(t, s)
}

func TestPreemptionAndBackfill(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 低优先级可中断作业占满全部 16 CPU。
	if err := s.Submit(JobSpec{JobID: "low", Owner: "bob", Priority: 1, Preemptible: true, Count: 4, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("low")
	if j.State != JobRunning {
		t.Fatalf("low should be running, got %s", j.State)
	}
	logLedger(t, s, "low-running")

	// 高优先级作业到来，需要 8 CPU，应抢占 2 个低优先级任务。
	if err := s.Submit(JobSpec{JobID: "high", Owner: "carol", Priority: 10, Count: 2, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	jh, _ := s.Job("high")
	if jh.State != JobRunning {
		t.Fatalf("high should be running after preemption, got %s", jh.State)
	}
	jl, _ := s.Job("low")
	preempted := 0
	for _, task := range jl.Tasks {
		if task.State == TaskPreempted || task.State == TaskPending {
			preempted++
		}
	}
	if preempted != 2 {
		t.Fatalf("want 2 preempted tasks, got %d", preempted)
	}
	logLedger(t, s, "after-preempt")
	assertLedgerSane(t, s)

	// 高优先级作业完成后，被抢占的任务应被重新安置（回填）。
	for i := 0; i < 2; i++ {
		s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "high", TaskID: fmt.Sprintf("high#%d", i), Success: true})
	}
	s.Schedule()
	jl, _ = s.Job("low")
	for _, task := range jl.Tasks {
		if task.State != TaskRunning {
			t.Fatalf("preempted task %s not backfilled, state=%s", task.Spec.TaskID, task.State)
		}
	}
	logLedger(t, s, "after-backfill")
	assertLedgerSane(t, s)
}

func TestConcurrentSubmitAndSchedule(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	var wg sync.WaitGroup
	// 两个提交方并发提交 16 个作业，同时并发触发调度。
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := "bob"
			if i%2 == 1 {
				owner = "carol"
			}
			_ = s.Submit(JobSpec{
				JobID:    fmt.Sprintf("c%d", i),
				Owner:    owner,
				Priority: i % 5,
				Count:    1,
				Requires: Resource{CPU: 2},
			})
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 10; k++ {
				s.Schedule()
			}
		}()
	}
	wg.Wait()
	s.Schedule()

	// 账本一致性：已用不超过容量；运行中任务占用的资源总量与账本一致。
	assertLedgerSane(t, s)
	var ledgerUsed int64
	for _, n := range s.LedgerSnapshot() {
		ledgerUsed += n.Used.CPU
	}
	var running int64
	for i := 0; i < 16; i++ {
		j, ok := s.Job(fmt.Sprintf("c%d", i))
		if !ok {
			continue
		}
		for _, task := range j.Tasks {
			if task.State == TaskRunning {
				running += 2
			}
		}
	}
	if ledgerUsed != running {
		t.Fatalf("ledger used=%d but running tasks hold=%d", ledgerUsed, running)
	}
	logLedger(t, s, "concurrent-final")
}

func TestNodeLossDuplicateAndOutOfOrderEvents(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 每任务 8 CPU，两个任务分别落在 n1、n2。
	if err := s.Submit(JobSpec{JobID: "j1", Owner: "bob", Count: 2, Requires: Resource{CPU: 8}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	j, _ := s.Job("j1")
	if j.State != JobRunning {
		t.Fatalf("j1 should be running, got %s", j.State)
	}
	t.Logf("placements: %s -> %s, %s -> %s", j.Tasks[0].Spec.TaskID, j.Tasks[0].NodeID, j.Tasks[1].Spec.TaskID, j.Tasks[1].NodeID)

	// n1 失联：整组回退——n2 上的同伴也一起停下，作业整组退回排队，账本全部归还。
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: "n1"})
	// 重复的失联事件必须被忽略。
	s.HandleEvent(Event{Type: EventNodeLost, NodeID: "n1"})
	logLedger(t, s, "node-lost")

	j, _ = s.Job("j1")
	if j.State != JobQueued {
		t.Fatalf("job should roll back to queued, got %s", j.State)
	}
	for _, task := range j.Tasks {
		if task.State != TaskPending {
			t.Fatalf("gang rollback incomplete: task %s state=%s (half-running gang)", task.Spec.TaskID, task.State)
		}
		if task.Attempt != 0 {
			t.Fatalf("rollback must not consume retries: task %s attempt=%d", task.Spec.TaskID, task.Attempt)
		}
	}
	// 账本必须还干净：健康节点上的占用也已释放。
	for nid, n := range s.LedgerSnapshot() {
		if !n.Used.IsZero() {
			t.Fatalf("ledger not clean after rollback: node=%s used=%+v", nid, n.Used)
		}
	}

	// 乱序/迟到事件：已回退为 PENDING 的任务收到迟到的完成通知必须被忽略。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "j1", TaskID: "j1#0", Success: true})
	j, _ = s.Job("j1")
	if j.FinishedN != 0 {
		t.Fatalf("stale completion counted: finishedN=%d", j.FinishedN)
	}

	// 节点恢复后，整组重新拉起（两个任务同一轮一起启动）。
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: "n1"})
	s.HandleEvent(Event{Type: EventNodeRecovered, NodeID: "n1"}) // 重复恢复事件
	s.Schedule()
	j, _ = s.Job("j1")
	if j.State != JobRunning {
		t.Fatalf("job not gang-restarted after recovery, got %s", j.State)
	}
	for _, task := range j.Tasks {
		if task.State != TaskRunning {
			t.Fatalf("gang restart incomplete: task %s state=%s", task.Spec.TaskID, task.State)
		}
	}
	logLedger(t, s, "gang-restarted")

	// 一个任务完成，重复完成通知不能重复计数。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "j1", TaskID: "j1#0", Success: true})
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "j1", TaskID: "j1#0", Success: true})
	j, _ = s.Job("j1")
	if j.FinishedN != 1 {
		t.Fatalf("duplicate completion double-counted: finishedN=%d", j.FinishedN)
	}

	// 第二个任务完成，作业成功。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "j1", TaskID: "j1#1", Success: true})
	j, _ = s.Job("j1")
	if j.State != JobSucceeded {
		t.Fatalf("job should succeed, got %s (finishedN=%d)", j.State, j.FinishedN)
	}
	// 终态后迟到的失败事件不能把任务改回非终态。
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "j1", TaskID: "j1#1", Success: false})
	j, _ = s.Job("j1")
	if j.State != JobSucceeded {
		t.Fatalf("terminal job state regressed: %s", j.State)
	}
	logLedger(t, s, "final")
	assertLedgerSane(t, s)
}

func TestRetryAndTimeout(t *testing.T) {
	s := New(twoNodes(), bigQuota(), nil)
	s.SetLogger(t.Logf)

	// 失败重试：MaxRetries=1，第一次失败重试，第二次失败整组失败。
	if err := s.Submit(JobSpec{JobID: "r1", Owner: "bob", Count: 1, MaxRetries: 1, Requires: Resource{CPU: 2}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "r1", TaskID: "r1#0", Success: false})
	j, _ := s.Job("r1")
	if j.State != JobRunning || j.Tasks[0].State != TaskPending || j.Tasks[0].Attempt != 1 {
		t.Fatalf("expected retry pending attempt=1, got job=%s task=%s attempt=%d", j.State, j.Tasks[0].State, j.Tasks[0].Attempt)
	}
	s.Schedule()
	if j, _ = s.Job("r1"); j.Tasks[0].State != TaskRunning {
		t.Fatalf("retried task not running: %s", j.Tasks[0].State)
	}
	s.HandleEvent(Event{Type: EventTaskCompleted, JobID: "r1", TaskID: "r1#0", Success: false})
	j, _ = s.Job("r1")
	if j.State != JobFailed {
		t.Fatalf("expected job failed after retries exhausted, got %s", j.State)
	}
	logLedger(t, s, "retry-exhausted")

	// 执行超时：50ms 超时，Sleep 后下一轮 Schedule 判定失败（无重试，直接整组失败）。
	if err := s.Submit(JobSpec{JobID: "t1", Owner: "bob", Count: 1, ExecTimeout: 50 * time.Millisecond, Requires: Resource{CPU: 2}}); err != nil {
		t.Fatal(err)
	}
	s.Schedule()
	time.Sleep(100 * time.Millisecond)
	s.Schedule()
	j, _ = s.Job("t1")
	if j.State != JobFailed {
		t.Fatalf("expected timeout failure, got %s", j.State)
	}
	// 所有资源必须归还。
	var used int64
	for _, n := range s.LedgerSnapshot() {
		used += n.Used.CPU
	}
	if used != 0 {
		t.Fatalf("resources leaked after timeout: used=%d", used)
	}
	logLedger(t, s, "timeout-final")
	assertLedgerSane(t, s)
}

func TestRecoverFromStore(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir + "/state.json")

	s1 := New(twoNodes(), bigQuota(), store)
	s1.SetLogger(t.Logf)
	if err := s1.Submit(JobSpec{JobID: "p1", Owner: "bob", Count: 1, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Submit(JobSpec{JobID: "p2", Owner: "bob", Count: 4, Requires: Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	s1.Schedule() // p1 运行；p2 需要 16 CPU 放不下，保持排队
	logLedger(t, s1, "before-crash")

	// 模拟进程重启：用同一份持久化记录创建新调度器。
	s2 := New(nil, bigQuota(), store)
	s2.SetLogger(t.Logf)
	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	j1, _ := s2.Job("p1")
	if j1.State != JobRunning || j1.Tasks[0].State != TaskRunning {
		t.Fatalf("running job not recovered: job=%s task=%s", j1.State, j1.Tasks[0].State)
	}
	j2, _ := s2.Job("p2")
	if j2.State != JobQueued {
		t.Fatalf("queued job not recovered: %s", j2.State)
	}
	logLedger(t, s2, "after-recover")

	// 恢复后完成运行中的任务，排队作业应能被调度上，而不是从头开始。
	s2.HandleEvent(Event{Type: EventTaskCompleted, JobID: "p1", TaskID: "p1#0", Success: true})
	s2.Schedule()
	j2, _ = s2.Job("p2")
	if j2.State != JobRunning {
		t.Fatalf("queued job not scheduled after recovery, got %s", j2.State)
	}
	assertLedgerSane(t, s2)
}
