package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// RejectError 表示整组拒绝，Reason 给出可区分的原因。
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("job rejected (%s): %s", e.Reason, e.Detail)
}

// RejectReasonOf 从错误中提取拒绝原因；非拒绝错误返回 RejectNone。
func RejectReasonOf(err error) RejectReason {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return RejectNone
}

// Scheduler 是进程内的整组作业调度器。所有导出方法都持有同一把互斥锁，
// 保证并发提交/事件下资源账本不会被重复分配。
type Scheduler struct {
	mu     sync.Mutex
	nodes  map[string]*Node
	jobs   map[string]*Job
	queue  []*Job // 等待整组调度的作业（JobQueued）
	quotas map[string]Resource
	store  *Store
	logf   func(format string, args ...any)
	now    func() time.Time
}

// New 创建调度器。nodes 为模拟集群节点，quotas 为提交方配额，store 可为 nil（不持久化）。
func New(nodes []*Node, quotas map[string]Resource, store *Store) *Scheduler {
	s := &Scheduler{
		nodes:  map[string]*Node{},
		jobs:   map[string]*Job{},
		quotas: quotas,
		store:  store,
		logf:   func(string, ...any) {},
		now:    time.Now,
	}
	for _, n := range nodes {
		c := *n
		s.nodes[c.ID] = &c
	}
	return s
}

// SetLogger 设置调度决策日志输出函数（如 testing.T.Logf）。
func (s *Scheduler) SetLogger(f func(format string, args ...any)) { s.logf = f }

// Submit 提交作业（Count>1 即作业数组）。校验失败时整组拒绝并返回 *RejectError。
func (s *Scheduler) Submit(spec JobSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if spec.Count <= 0 {
		spec.Count = 1
	}
	if spec.SubmitTime.IsZero() {
		spec.SubmitTime = s.now()
	}
	if _, dup := s.jobs[spec.JobID]; dup {
		err := &RejectError{RejectDuplicate, "job " + spec.JobID + " already exists"}
		s.logf("SUBMIT REJECT job=%s reason=%s", spec.JobID, err.Reason)
		return err
	}
	fits := false
	for _, n := range s.nodes {
		if spec.Requires.Fits(n.Capacity) {
			fits = true
			break
		}
	}
	if !fits {
		err := &RejectError{RejectResourceUnfits, fmt.Sprintf("per-task %+v exceeds every node capacity", spec.Requires)}
		s.logf("SUBMIT REJECT job=%s reason=%s", spec.JobID, err.Reason)
		return err
	}
	if !s.gangFitsEmptyClusterLocked(spec) {
		err := &RejectError{RejectGangExceedsCluster, fmt.Sprintf("gang of %d x %+v exceeds whole-cluster capacity (empty-cluster trial placement failed)", spec.Count, spec.Requires)}
		s.logf("SUBMIT REJECT job=%s reason=%s detail=%s", spec.JobID, err.Reason, err.Detail)
		return err
	}
	if q, ok := s.quotas[spec.Owner]; ok {
		need := scale(spec.Requires, spec.Count)
		used := s.quotaUsageLocked(spec.Owner)
		if !used.Add(need).Fits(q) {
			err := &RejectError{RejectQuotaExceeded, fmt.Sprintf("owner=%s used=%+v need=%+v quota=%+v", spec.Owner, used, need, q)}
			s.logf("SUBMIT REJECT job=%s reason=%s detail=%s", spec.JobID, err.Reason, err.Detail)
			return err
		}
	}
	job := &Job{Spec: spec, State: JobQueued}
	for i := 0; i < spec.Count; i++ {
		job.Tasks = append(job.Tasks, &Task{
			Spec:  TaskSpec{TaskID: fmt.Sprintf("%s#%d", spec.JobID, i), Requires: spec.Requires},
			JobID: spec.JobID,
			State: TaskPending,
		})
	}
	s.jobs[spec.JobID] = job
	s.queue = append(s.queue, job)
	s.logf("SUBMIT OK job=%s owner=%s prio=%d count=%d per-task=%+v", spec.JobID, spec.Owner, spec.Priority, spec.Count, spec.Requires)
	s.persistLocked()
	return nil
}

// Schedule 执行一轮调度：超时检查 -> 安置重试/被抢占任务 -> 整组调度（含抢占与回填）。
func (s *Scheduler) Schedule() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkTimeoutsLocked(s.now())
	s.placePendingTasksLocked()
	s.gangScheduleLocked()
	s.persistLocked()
}

// HandleEvent 处理外部事件，允许重复或乱序到达。
func (s *Scheduler) HandleEvent(ev Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch ev.Type {
	case EventTaskCompleted:
		s.onTaskCompletedLocked(ev)
	case EventNodeLost:
		s.onNodeLostLocked(ev)
	case EventNodeRecovered:
		s.onNodeRecoveredLocked(ev)
	}
	s.persistLocked()
}

// Job 返回作业快照副本。
func (s *Scheduler) Job(jobID string) (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return nil, false
	}
	cp := *j
	cp.Tasks = append([]*Task(nil), j.Tasks...)
	return &cp, true
}

// LedgerSnapshot 返回当前资源账本快照（节点已用/容量），用于日志与测试断言。
func (s *Scheduler) LedgerSnapshot() map[string]Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Node, len(s.nodes))
	for id, n := range s.nodes {
		out[id] = *n
	}
	return out
}

// Recover 从持久化记录恢复调度状态，排队的作业重新入队，运行中的任务保持原状。
func (s *Scheduler) Recover() error {
	if s.store == nil {
		return nil
	}
	snap, err := s.store.Load()
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = snap.Nodes
	s.jobs = snap.Jobs
	s.queue = s.queue[:0]
	for _, j := range s.jobs {
		if j.State == JobQueued {
			s.queue = append(s.queue, j)
		}
	}
	s.logf("RECOVER jobs=%d nodes=%d queued=%d", len(s.jobs), len(s.nodes), len(s.queue))
	return nil
}

// ---- 内部实现（均须持锁调用） ----

func scale(r Resource, n int) Resource {
	return Resource{CPU: r.CPU * int64(n), Memory: r.Memory * int64(n), GPU: r.GPU * int64(n)}
}

// gangFitsEmptyClusterLocked 在“空集群”上试放整个作业组：把全部节点（含暂时
// 失联的，因为失联视为可恢复的）的容量清零后，用与正式调度相同的 first-fit
// 算法逐个安置 Count 个任务。试放失败说明整组需求超过集群物理上限，永远排不上。
// 节点规格不一致时该判断按真实安置结果给出，而不是只看最宽节点或总量。
func (s *Scheduler) gangFitsEmptyClusterLocked(spec JobSpec) bool {
	free := make(map[string]Resource, len(s.nodes))
	for id, n := range s.nodes {
		free[id] = n.Capacity
	}
	for i := 0; i < spec.Count; i++ {
		placed := false
		for _, nid := range sortedKeys(free) {
			if spec.Requires.Fits(free[nid]) {
				free[nid] = free[nid].Sub(spec.Requires)
				placed = true
				break
			}
		}
		if !placed {
			return false
		}
	}
	return true
}

// quotaUsageLocked 统计提交方当前占用配额的资源（排队+运行+待重放的任务）。
func (s *Scheduler) quotaUsageLocked(owner string) Resource {
	total := Resource{}
	for _, j := range s.jobs {
		if j.Spec.Owner != owner || j.State == JobSucceeded || j.State == JobFailed || j.State == JobRejected {
			continue
		}
		for _, t := range j.Tasks {
			if t.State == TaskPending || t.State == TaskRunning || t.State == TaskPreempted {
				total = total.Add(t.Spec.Requires)
			}
		}
	}
	return total
}

// placePendingTasksLocked 安置运行中作业里待重放的单任务（重试、被抢占、节点失联重入队）。
func (s *Scheduler) placePendingTasksLocked() {
	jobs := s.sortedJobsLocked()
	for _, j := range jobs {
		if j.State != JobRunning {
			continue
		}
		for _, t := range j.Tasks {
			if t.State != TaskPending && t.State != TaskPreempted {
				continue
			}
			if nid, ok := s.placeOneLocked(t.Spec.Requires); ok {
				t.State = TaskRunning
				t.NodeID = nid
				t.Generation++
				t.StartedAt = s.now()
				s.logf("REPLACE task=%s job=%s -> node=%s (attempt=%d gen=%d)", t.Spec.TaskID, j.Spec.JobID, nid, t.Attempt, t.Generation)
			}
		}
	}
}

// gangScheduleLocked 按优先级整组调度队列中的作业，必要时抢占，放不下的跳过（回填后续作业）。
func (s *Scheduler) gangScheduleLocked() {
	s.sortQueueLocked()
	remaining := s.queue[:0]
	for _, j := range s.queue {
		placements, ok := s.gangFitLocked(j)
		if !ok {
			if victims, pok := s.planPreemptionLocked(j); pok {
				for _, v := range victims {
					s.evictLocked(v)
				}
				placements, ok = s.gangFitLocked(j)
			}
		}
		if !ok {
			s.logf("BACKFILL SKIP job=%s prio=%d (gang does not fit yet)", j.Spec.JobID, j.Spec.Priority)
			remaining = append(remaining, j)
			continue
		}
		for _, t := range j.Tasks {
			nid, ok := placements[t.Spec.TaskID]
			if !ok {
				continue // 已终态的任务（如回退前已成功）不在本轮安置范围内
			}
			s.nodes[nid].Used = s.nodes[nid].Used.Add(t.Spec.Requires)
			t.State = TaskRunning
			t.NodeID = nid
			t.Generation++
			t.StartedAt = s.now()
		}
		j.State = JobRunning
		s.logf("GANG START job=%s prio=%d placements=%v ledger=%s", j.Spec.JobID, j.Spec.Priority, placements, s.ledgerStringLocked())
	}
	s.queue = remaining
}

// gangFitLocked 尝试为作业的全部任务找到节点（first-fit），成功返回 taskID->nodeID。
func (s *Scheduler) gangFitLocked(j *Job) (map[string]string, bool) {
	free := map[string]Resource{}
	for id, n := range s.nodes {
		if n.Alive {
			free[id] = n.Free()
		}
	}
	placements := map[string]string{}
	for _, t := range j.Tasks {
		if t.State != TaskPending && t.State != TaskPreempted {
			continue
		}
		placed := false
		for _, nid := range sortedKeys(free) {
			if t.Spec.Requires.Fits(free[nid]) {
				free[nid] = free[nid].Sub(t.Spec.Requires)
				placements[t.Spec.TaskID] = nid
				placed = true
				break
			}
		}
		if !placed {
			return nil, false
		}
	}
	return placements, true
}

// planPreemptionLocked 为放不下的高优先级作业挑选可抢占的受害者任务。
// 只有在抢占后整组能放下时才返回受害者列表，否则返回 nil（不做无谓抢占）。
func (s *Scheduler) planPreemptionLocked(j *Job) ([]*Task, bool) {
	type cand struct {
		task *Task
		prio int
	}
	var cands []cand
	for _, o := range s.jobs {
		if o.State != JobRunning || !o.Spec.Preemptible || o.Spec.Priority >= j.Spec.Priority {
			continue
		}
		for _, t := range o.Tasks {
			if t.State == TaskRunning {
				cands = append(cands, cand{t, o.Spec.Priority})
			}
		}
	}
	if len(cands) == 0 {
		return nil, false
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].prio < cands[b].prio })
	var victims []*Task
	ok := false
	for _, c := range cands {
		victims = append(victims, c.task)
		s.releaseLocked(c.task) // 试算：临时释放
		if _, ok = s.gangFitLocked(j); ok {
			break
		}
	}
	// 无论成功与否都回滚试算；成功时由调用方通过 evictLocked 正式释放。
	for _, v := range victims {
		if n, ok2 := s.nodes[v.NodeID]; ok2 {
			n.Used = n.Used.Add(v.Spec.Requires)
		}
	}
	if !ok {
		return nil, false
	}
	return victims, true
}

// evictLocked 正式抢占一个任务：回到待调度状态，资源账本同步归还。
func (s *Scheduler) evictLocked(t *Task) {
	s.releaseLocked(t)
	t.State = TaskPreempted
	t.NodeID = ""
	s.logf("PREEMPT task=%s job=%s ledger=%s", t.Spec.TaskID, t.JobID, s.ledgerStringLocked())
}

// releaseLocked 归还任务占用的节点资源（仅在任务仍记在某个节点账上时调用）。
func (s *Scheduler) releaseLocked(t *Task) {
	if t.NodeID == "" {
		return
	}
	if n, ok := s.nodes[t.NodeID]; ok && n.Alive {
		n.Used = n.Used.Sub(t.Spec.Requires)
	}
}

func (s *Scheduler) placeOneLocked(req Resource) (string, bool) {
	for _, nid := range sortedKeys2(s.nodes) {
		n := s.nodes[nid]
		if n.Alive && req.Fits(n.Free()) {
			n.Used = n.Used.Add(req)
			return nid, true
		}
	}
	return "", false
}

func (s *Scheduler) onTaskCompletedLocked(ev Event) {
	j, ok := s.jobs[ev.JobID]
	if !ok {
		s.logf("EVENT IGNORE task=%s job=%s: unknown job", ev.TaskID, ev.JobID)
		return
	}
	var t *Task
	for _, x := range j.Tasks {
		if x.Spec.TaskID == ev.TaskID {
			t = x
			break
		}
	}
	if t == nil {
		s.logf("EVENT IGNORE task=%s job=%s: unknown task", ev.TaskID, ev.JobID)
		return
	}
	// 跨实例去重：事件携带代数时，必须匹配任务当前运行实例的代数，
	// 否则是被抢占/失联重排后上一轮实例的迟到通知，不能据此判定当前这一轮。
	if ev.Generation > 0 && ev.Generation != t.Generation {
		s.logf("EVENT STALE-INSTANCE task=%s job=%s evGen=%d curGen=%d state=%s: ignored",
			t.Spec.TaskID, j.Spec.JobID, ev.Generation, t.Generation, t.State)
		return
	}
	if t.State.Terminal() {
		s.logf("EVENT DUP task=%s job=%s state=%s: already terminal, ignored", t.Spec.TaskID, j.Spec.JobID, t.State)
		return
	}
	if t.State != TaskRunning {
		s.logf("EVENT STALE task=%s job=%s state=%s: not running, ignored", t.Spec.TaskID, j.Spec.JobID, t.State)
		return
	}
	s.releaseLocked(t)
	t.NodeID = ""
	if ev.Success {
		t.State = TaskSucceeded
		j.FinishedN++
		s.logf("TASK DONE task=%s job=%s success=true ledger=%s", t.Spec.TaskID, j.Spec.JobID, s.ledgerStringLocked())
	} else {
		s.failTaskLocked(j, t, "completion reported failure")
	}
	s.updateJobStateLocked(j)
}

// failTaskLocked 处理一次任务失败：可重试则回到待调度，否则整组终止。
func (s *Scheduler) failTaskLocked(j *Job, t *Task, why string) {
	j.FinishedN++
	if t.Attempt < j.Spec.MaxRetries {
		t.Attempt++
		t.State = TaskPending
		t.NodeID = ""
		s.logf("TASK RETRY task=%s job=%s attempt=%d/%d why=%s", t.Spec.TaskID, j.Spec.JobID, t.Attempt, j.Spec.MaxRetries, why)
		return
	}
	t.State = TaskFailed
	j.FailedN++
	j.State = JobFailed
	// 整组语义：一个任务彻底失败即终止全组，释放其余任务的资源。
	for _, o := range j.Tasks {
		if o.State == TaskRunning {
			s.releaseLocked(o)
			o.NodeID = ""
			o.State = TaskFailed
			j.FinishedN++
		} else if o.State == TaskPending || o.State == TaskPreempted {
			o.State = TaskFailed
			j.FinishedN++
		}
	}
	s.logf("JOB FAILED job=%s task=%s why=%s ledger=%s", j.Spec.JobID, t.Spec.TaskID, why, s.ledgerStringLocked())
}

func (s *Scheduler) onNodeLostLocked(ev Event) {
	n, ok := s.nodes[ev.NodeID]
	if !ok || !n.Alive {
		s.logf("EVENT DUP node=%s: already lost or unknown, ignored", ev.NodeID)
		return
	}
	n.Alive = false
	n.Used = Resource{}
	// 整组回退：只要作业有一个运行中任务落在失联节点上，就把该作业在健康
	// 节点上的同伴也一起停下，整组退回队列等待重新整组拉起，避免同一个
	// 作业一半在跑、一半在等。回退不消耗任务的重试次数（Attempt 不变）。
	rolledBack := 0
	for _, j := range s.jobs {
		if j.State != JobRunning {
			continue
		}
		hit := false
		for _, t := range j.Tasks {
			if t.State == TaskRunning && t.NodeID == ev.NodeID {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		for _, t := range j.Tasks {
			if t.State != TaskRunning {
				continue // 已终态（如已成功）的任务保持原状，不重跑
			}
			s.releaseLocked(t) // 失联节点账本已清零，健康节点在此归还
			t.State = TaskPending
			t.NodeID = ""
		}
		j.State = JobQueued
		s.enqueueLocked(j)
		rolledBack++
		s.logf("GANG ROLLBACK job=%s node=%s: all running tasks requeued (attempts preserved)", j.Spec.JobID, ev.NodeID)
	}
	s.logf("NODE LOST node=%s rolledBackJobs=%d ledger=%s", ev.NodeID, rolledBack, s.ledgerStringLocked())
}

// enqueueLocked 把作业放回等待队列（幂等，避免重复入队）。
func (s *Scheduler) enqueueLocked(j *Job) {
	for _, q := range s.queue {
		if q == j {
			return
		}
	}
	s.queue = append(s.queue, j)
}

func (s *Scheduler) onNodeRecoveredLocked(ev Event) {
	n, ok := s.nodes[ev.NodeID]
	if !ok || n.Alive {
		s.logf("EVENT DUP node=%s: already alive or unknown, ignored", ev.NodeID)
		return
	}
	n.Alive = true
	n.Used = Resource{}
	s.logf("NODE RECOVERED node=%s ledger=%s", ev.NodeID, s.ledgerStringLocked())
}

// checkTimeoutsLocked 把执行超时的运行中任务按失败处理。
func (s *Scheduler) checkTimeoutsLocked(now time.Time) {
	for _, j := range s.jobs {
		if j.State != JobRunning || j.Spec.ExecTimeout <= 0 {
			continue
		}
		for _, t := range j.Tasks {
			if t.State == TaskRunning && !t.StartedAt.IsZero() && now.Sub(t.StartedAt) > j.Spec.ExecTimeout {
				s.releaseLocked(t)
				t.NodeID = ""
				s.failTaskLocked(j, t, "exec timeout")
			}
		}
		s.updateJobStateLocked(j)
	}
}

// updateJobStateLocked 在任务状态变化后重算作业状态。
func (s *Scheduler) updateJobStateLocked(j *Job) {
	if j.State == JobFailed || j.State == JobSucceeded {
		return
	}
	terminal := 0
	for _, t := range j.Tasks {
		if t.State.Terminal() {
			terminal++
		}
	}
	if terminal == len(j.Tasks) {
		if j.FailedN > 0 {
			j.State = JobFailed
		} else {
			j.State = JobSucceeded
		}
		s.logf("JOB %s job=%s finished=%d failed=%d", j.State, j.Spec.JobID, j.FinishedN, j.FailedN)
	}
}

func (s *Scheduler) sortQueueLocked() {
	sort.SliceStable(s.queue, func(a, b int) bool {
		x, y := s.queue[a].Spec, s.queue[b].Spec
		if x.Priority != y.Priority {
			return x.Priority > y.Priority
		}
		if !x.SubmitTime.Equal(y.SubmitTime) {
			return x.SubmitTime.Before(y.SubmitTime)
		}
		return x.JobID < y.JobID
	})
}

func (s *Scheduler) sortedJobsLocked() []*Job {
	out := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Spec.Priority != out[b].Spec.Priority {
			return out[a].Spec.Priority > out[b].Spec.Priority
		}
		return out[a].Spec.JobID < out[b].Spec.JobID
	})
	return out
}

func (s *Scheduler) ledgerStringLocked() string {
	out := ""
	for _, nid := range sortedKeys2(s.nodes) {
		n := s.nodes[nid]
		out += fmt.Sprintf("[%s used=%+v/%+v alive=%v] ", nid, n.Used, n.Capacity, n.Alive)
	}
	return out
}

func (s *Scheduler) persistLocked() {
	if s.store == nil {
		return
	}
	snap := &Snapshot{Jobs: s.jobs, Nodes: s.nodes}
	if err := s.store.Save(snap); err != nil {
		s.logf("PERSIST ERROR: %v", err)
	}
}

func sortedKeys(m map[string]Resource) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys2(m map[string]*Node) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
