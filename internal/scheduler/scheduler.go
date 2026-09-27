package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/store"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// Placement 是一个任务的落位决策。
type Placement struct {
	Task types.TaskID
	Node string
	Proc types.Resources
}

// Config 调度器配置。
type Config struct {
	// Quotas 按租户（提交方）给出"在队 + 在跑"整组资源的配额上限。
	// 未出现在 map 中的租户视为不限额。
	Quotas map[string]types.Resources
	// TickInterval 后台调度扫描间隔。
	TickInterval time.Duration
}

// Scheduler 是整组调度器。所有可变状态都由 mu 保护；
// 账本自身也带锁，但"预留 / 抢占 / 状态变更"的复合操作都在 mu 下完成，
// 保证多个提交方并发操作时同一份资源不会被重复分配。
type Scheduler struct {
	cfg  Config
	clus *cluster.Cluster
	exec *cluster.Executor
	st   store.Store
	logf func(format string, args ...any)

	mu            sync.Mutex
	jobs          map[string]*jobRecord
	order         []string
	lost          map[string]bool
	nextSubmitSeq int64
	version       int64

	notify chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc

	ledger *Ledger
}

// New 创建调度器。集群中的节点需要事先 AddNode 注册好。
func New(cfg Config, clus *cluster.Cluster, exec *cluster.Executor, st store.Store, logf func(string, ...any)) *Scheduler {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
	}
	s := &Scheduler{
		cfg:    cfg,
		clus:   clus,
		exec:   exec,
		st:     st,
		logf:   logf,
		jobs:   map[string]*jobRecord{},
		lost:   map[string]bool{},
		notify: make(chan struct{}, 1),
		ledger: NewLedger(),
	}
	for _, name := range clus.NodeNames() {
		cap, _ := clus.Capacity(name)
		s.ledger.AddNode(name, cap)
	}
	return s
}

// Start 启动后台调度循环。
func (s *Scheduler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.loop(ctx)
	s.requestSchedule()
	return nil
}

// Close 停止后台循环（不影响持久化状态）。
func (s *Scheduler) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *Scheduler) loop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.notify:
		}
		s.tick(ctx)
	}
}

// requestSchedule 唤醒一次调度扫描（非阻塞，重复唤醒会被合并）。
func (s *Scheduler) requestSchedule() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) log(format string, args ...any) {
	s.logf(format, args...)
}

func (s *Scheduler) logLedgerLocked(prefix string) {
	s.log("%s:\n%s", prefix, FormatSnapshot(s.ledger.Snapshot()))
}

// Submit 提交作业（Procs>1 即作业数组）。
// 拒绝时返回 *types.RejectError，作业完全不进入系统、不占用任何资源。
func (s *Scheduler) Submit(ctx context.Context, spec types.JobSpec) error {
	if spec.Procs < 1 {
		return types.NewRejectError(types.RejectInsufficientCap, "job %s procs must be >= 1", spec.Name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) 重复提交：同名作业（无论处于什么状态）一律整组拒绝。
	if _, exists := s.jobs[spec.Name]; exists {
		s.log("拒绝提交作业 %s：重复提交（同名作业已存在）", spec.Name)
		return types.NewRejectError(types.RejectDuplicate, "job %s already submitted", spec.Name)
	}

	// 2) 可行性：单任务至少能放进某个节点；整组总需求不超过集群总容量。
	//    这两条代表作业"永远不可能被调度"，立即整组拒绝（优先级高于配额判定）；
	//    仅当前资源被占用（临时不足）的作业仍进入队列等待回填/抢占。
	var totalCap types.Resources
	fitNode := false
	for _, name := range s.ledger.NodeNames() {
		cap, _ := s.ledger.Capacity(name)
		totalCap = totalCap.Add(cap)
		if spec.ProcPerTask.CanFit(cap) {
			fitNode = true
		}
	}
	if !fitNode || spec.TotalRequest().Greater(totalCap) {
		s.log("拒绝提交作业 %s：集群容量无法满足整组（存在可放入单任务的节点=%v，集群总容量 {cpu:%d mem:%d gpu:%d}，整组需求 {cpu:%d mem:%d gpu:%d}）",
			spec.Name, fitNode, totalCap.CPU, totalCap.Memory, totalCap.GPU,
			spec.TotalRequest().CPU, spec.TotalRequest().Memory, spec.TotalRequest().GPU)
		return types.NewRejectError(types.RejectInsufficientCap,
			"job %s can never fit the cluster (gang of %d tasks)", spec.Name, spec.Procs)
	}

	// 3) 配额：该租户"在队 + 在跑"的非终态资源加上本次整组需求，不得超过配额。
	if quota, ok := s.cfg.Quotas[spec.Tenant]; ok {
		used := types.Resources{}
		for _, j := range s.jobs {
			if j.Spec.Tenant == spec.Tenant && j.State != types.JobCompleted && j.State != types.JobFailed {
				used = used.Add(j.Spec.TotalRequest())
			}
		}
		if used.Add(spec.TotalRequest()).Greater(quota) {
			s.log("拒绝提交作业 %s：租户 %s 配额超限（已申请 {cpu:%d mem:%d gpu:%d}，本次 {cpu:%d mem:%d gpu:%d}，配额 {cpu:%d mem:%d gpu:%d}）",
				spec.Name, spec.Tenant,
				used.CPU, used.Memory, used.GPU,
				spec.TotalRequest().CPU, spec.TotalRequest().Memory, spec.TotalRequest().GPU,
				quota.CPU, quota.Memory, quota.GPU)
			return types.NewRejectError(types.RejectQuotaExceeded,
				"tenant %s quota exceeded: used+request exceeds limit", spec.Tenant)
		}
	}

	// 4) 入队。
	jr := &jobRecord{
		Spec:      spec,
		State:     types.JobQueued,
		SubmitSeq: s.nextSubmitSeq,
		Tasks:     make([]*taskRecord, spec.Procs),
	}
	s.nextSubmitSeq++
	for i := 0; i < spec.Procs; i++ {
		jr.Tasks[i] = &taskRecord{ID: types.TaskID{Job: spec.Name, Index: i}, State: types.TaskPending}
	}
	s.jobs[spec.Name] = jr
	s.order = append(s.order, spec.Name)

	s.log("作业 %s 已入队（租户=%s 优先级=%d 任务数=%d 可中断=%v 整组需求 {cpu:%d mem:%d gpu:%d}）",
		spec.Name, spec.Tenant, spec.Priority, spec.Procs, spec.Interruptible,
		spec.TotalRequest().CPU, spec.TotalRequest().Memory, spec.TotalRequest().GPU)
	if err := s.persistLocked(ctx); err != nil {
		return err
	}
	s.requestSchedule()
	return nil
}

// HandleEvent 接收执行事件。事件允许重复、乱序到达。
func (s *Scheduler) HandleEvent(ctx context.Context, ev types.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ev.Kind == types.EventNodeLost || ev.Kind == types.EventNodeUp {
		s.handleNodeEventLocked(ctx, ev)
		return
	}

	jr, tr, ok := s.lookupTaskLocked(ev.TaskID)
	if !ok {
		s.log("忽略事件 %s seq=%d（任务 %s 不存在）", ev.Kind, ev.Sequence, ev.TaskID)
		return
	}

	// 实例过滤：只接受任务"当前启动实例"的事件。
	// 被抢占/失联/超时杀死的旧实例随后才到达的事件一律忽略，
	// 防止旧实例的终态把新实例状态改回去。
	if ev.Instance != tr.Instance {
		s.log("忽略旧实例事件 %s（任务 %s：事件实例=%d，当前实例=%d，当前状态=%s）",
			ev.Kind, ev.TaskID, ev.Instance, tr.Instance, tr.State)
		return
	}

	// 同实例内按序号去重 / 乱序过滤。
	if ev.Sequence > 0 && ev.Sequence <= tr.LastSeq {
		s.log("忽略过期事件 %s seq=%d（任务 %s 已见到 seq=%d，当前状态=%s）",
			ev.Kind, ev.Sequence, ev.TaskID, tr.LastSeq, tr.State)
		return
	}
	if ev.Sequence > 0 {
		tr.LastSeq = ev.Sequence
	}

	switch ev.Kind {
	case types.EventStarted:
		// 仅用于对账：RUNNING 转换发生在调度器下发 Launch 时。
		if tr.State != types.TaskRunning {
			s.log("忽略 started 事件（任务 %s 当前状态=%s，非 RUNNING）", ev.TaskID, tr.State)
		}
	case types.EventCompleted, types.EventFailed, types.EventTimeout:
		s.handleTaskTerminalLocked(ctx, jr, tr, ev)
	default:
		s.log("忽略未知事件类型 %q（任务 %s）", ev.Kind, ev.TaskID)
	}
}

func (s *Scheduler) lookupTaskLocked(id types.TaskID) (*jobRecord, *taskRecord, bool) {
	jr, ok := s.jobs[id.Job]
	if !ok || id.Index < 0 || id.Index >= len(jr.Tasks) {
		return nil, nil, false
	}
	return jr, jr.Tasks[id.Index], true
}

func (s *Scheduler) handleTaskTerminalLocked(ctx context.Context, jr *jobRecord, tr *taskRecord, ev types.Event) {
	// 非运行中（含终态、已回队的被抢占任务）收到的终态通知一律忽略：
	// 重复完成不能重复计数，也不能把终态/排队任务改回运行中。
	if tr.State != types.TaskRunning {
		s.log("忽略重复/迟到的 %s 事件（任务 %s 当前状态=%s awaitingStop=%v）",
			ev.Kind, ev.TaskID, tr.State, tr.AwaitingStop)
		return
	}

	released := s.ledger.Release(tr.ID)

	if ev.Kind == types.EventCompleted {
		tr.State = types.TaskCompleted
		s.log("任务 %s 完成（节点=%s，账本释放=%v）", tr.ID, ev.NodeName, released)
	} else {
		// failed / timeout 才消耗用户重试预算；被抢占、失联的重排不走这里。
		tr.FailCount++
		if tr.FailCount <= jr.Spec.Retries {
			tr.State = types.TaskPending
			tr.Node = ""
			tr.AwaitingStop = false
			s.log("任务 %s 收到 %s，将重试（失败次数 %d，重试预算 %d，账本释放=%v）",
				tr.ID, ev.Kind, tr.FailCount, jr.Spec.Retries, released)
			s.recomputeJobStateLocked(jr)
			if err := s.persistLocked(ctx); err == nil {
				s.requestSchedule()
			}
			return
		}
		tr.State = types.TaskFailed
		s.log("任务 %s 失败且重试预算耗尽（%s，失败次数 %d，账本释放=%v）",
			tr.ID, ev.Kind, tr.FailCount, released)
	}

	tr.Node = ""
	tr.AwaitingStop = false
	s.recomputeJobStateLocked(jr)
	s.logLedgerLocked("任务 " + tr.ID.String() + " 到达终态后")
	if err := s.persistLocked(ctx); err != nil {
		s.log("持久化失败: %v", err)
	}
	s.requestSchedule()
}

// recomputeJobStateLocked 根据任务状态重算整组作业状态。
func (s *Scheduler) recomputeJobStateLocked(jr *jobRecord) {
	allTerminal, anyRunning, anyPending, anyFailed := true, false, false, false
	for _, t := range jr.Tasks {
		switch t.State {
		case types.TaskRunning:
			anyRunning = true
			allTerminal = false
		case types.TaskPending:
			anyPending = true
			allTerminal = false
		case types.TaskFailed:
			anyFailed = true
		}
	}
	switch {
	case anyRunning:
		jr.State = types.JobRunning
	case anyPending:
		jr.State = types.JobQueued
	case allTerminal && anyFailed:
		jr.State = types.JobFailed
	case allTerminal:
		jr.State = types.JobCompleted
	}
}

func (s *Scheduler) handleNodeEventLocked(ctx context.Context, ev types.Event) {
	switch ev.Kind {
	case types.EventNodeLost:
		if s.lost[ev.NodeName] {
			s.log("忽略重复的 node_lost 事件（节点 %s 已处于失联状态）", ev.NodeName)
			return
		}
		tasks, ok := s.clus.MarkNodeLost(ev.NodeName)
		if !ok {
			s.log("忽略 node_lost：未知节点 %s", ev.NodeName)
			return
		}
		s.lost[ev.NodeName] = true
		keys := s.ledger.ReleaseNode(ev.NodeName)
		s.log("节点 %s 失联：集群取消在跑任务 %d 个，账本回收任务键 %d 个 %v", ev.NodeName, len(tasks), len(keys), keys)
		// 整组语义：如果一个作业的部分成员在失联节点上，那么该作业在其它节点上
		// 仍然存活的成员也要一并停掉并整组回队——否则恢复后的 PENDING 成员无法
		// 与残存成员重新组成一个完整放置。账本随之逐任务同步归还。
		for _, j := range s.jobs {
			affected := false
			for _, t := range j.Tasks {
				if t.State == types.TaskRunning && t.Node == ev.NodeName {
					affected = true
					break
				}
			}
			if !affected {
				continue
			}
			for _, t := range j.Tasks {
				if t.State != types.TaskRunning {
					continue
				}
				if t.Node != ev.NodeName {
					// 停掉存活节点上的同伴任务，维持整组原子性。
					s.clus.StopTask(t.ID, t.Node)
					s.ledger.Release(t.ID)
					s.log("失联连带处理：同伴任务 %s（节点 %s）随整组回队", t.ID, t.Node)
				} else {
					s.log("失联处理：任务 %s 回到队列（已计失败次数 %d，不消耗重试预算）", t.ID, t.FailCount)
				}
				t.State = types.TaskPending
				t.Node = ""
				t.AwaitingStop = false
			}
			s.recomputeJobStateLocked(j)
		}
		s.logLedgerLocked("节点 " + ev.NodeName + " 失联后")
		if err := s.persistLocked(ctx); err != nil {
			s.log("持久化失败: %v", err)
		}
		s.requestSchedule()
	case types.EventNodeUp:
		was := s.clus.MarkNodeUp(ev.NodeName)
		delete(s.lost, ev.NodeName)
		s.log("节点 %s 恢复（之前失联=%v），账本容量继续可用", ev.NodeName, was)
		if err := s.persistLocked(ctx); err != nil {
			s.log("持久化失败: %v", err)
		}
		s.requestSchedule()
	}
}

// tick 执行一轮调度扫描。整个复合过程持主锁，避免并发重复分配。
func (s *Scheduler) tick(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queued := make([]*jobRecord, 0)
	for _, name := range s.order {
		if j := s.jobs[name]; j.State == types.JobQueued {
			queued = append(queued, j)
		}
	}
	// 优先级高者先调度；同优先级按提交先后（FIFO，保守回填）。
	sort.SliceStable(queued, func(i, j int) bool {
		if queued[i].Spec.Priority != queued[j].Spec.Priority {
			return queued[i].Spec.Priority > queued[j].Spec.Priority
		}
		return queued[i].SubmitSeq < queued[j].SubmitSeq
	})

	madeProgress := false
	for _, j := range queued {
		if s.tryScheduleJobLocked(ctx, j) {
			madeProgress = true
		}
	}
	if madeProgress {
		s.logLedgerLocked("本轮调度结束")
	}
}

// reclaimChunk 是抢占视角下可回收的一块资源。
type reclaimChunk struct {
	task *taskRecord
	node string
	proc types.Resources
}

// nodeWorkView 是一轮调度中某个节点的工作副本：
// 从真实 used 出发，被回收的块逐步扣减，新整组任务逐步累加。
type nodeWorkView struct {
	name     string
	cap      types.Resources
	workUsed types.Resources
	chunks   []reclaimChunk
	claimed  []bool
}

// tryScheduleJobLocked 尝试整组启动一个排队作业；资源不够时，
// 允许抢占严格更低优先级、且标记为可中断的在跑作业。
// 成功的充要条件是整组所有任务都能放下——任何一个放不下就完全不抢占、不启动。
func (s *Scheduler) tryScheduleJobLocked(ctx context.Context, j *jobRecord) bool {
	liveNodes := make([]string, 0)
	for _, name := range s.ledger.NodeNames() {
		if !s.lost[name] {
			liveNodes = append(liveNodes, name)
		}
	}

	pending := make([]*taskRecord, 0)
	for _, t := range j.Tasks {
		if t.State == types.TaskPending {
			pending = append(pending, t)
		}
	}
	if len(pending) == 0 {
		return false
	}

	// 收集受害者作业：严格低优先级 + 可中断；低优先级且早提交的优先被抢占。
	var victims []*jobRecord
	for _, name := range s.order {
		v := s.jobs[name]
		if v == j {
			continue
		}
		if v.State == types.JobRunning && v.Spec.Interruptible && v.Spec.Priority < j.Spec.Priority {
			victims = append(victims, v)
		}
	}
	sort.SliceStable(victims, func(a, b int) bool {
		if victims[a].Spec.Priority != victims[b].Spec.Priority {
			return victims[a].Spec.Priority < victims[b].Spec.Priority
		}
		return victims[a].SubmitSeq < victims[b].SubmitSeq
	})

	views := make(map[string]*nodeWorkView)
	for _, name := range liveNodes {
		cap, _ := s.ledger.Capacity(name)
		views[name] = &nodeWorkView{name: name, cap: cap, workUsed: s.ledger.UsedOn(name)}
	}
	for _, v := range victims {
		for _, t := range v.Tasks {
			if t.State != types.TaskRunning {
				continue
			}
			proc, ok := s.ledger.TaskProc(t.ID)
			if !ok {
				continue
			}
			vw := views[t.Node]
			vw.chunks = append(vw.chunks, reclaimChunk{task: t, node: t.Node, proc: proc})
			vw.claimed = append(vw.claimed, false)
		}
	}

	needVictims := map[string]*taskRecord{}
	placements := make([]Placement, 0, len(pending))

	// 贪心整组装箱：节点按注册顺序；先吃空闲容量，不够再按受害者顺序回收块。
	for _, t := range pending {
		proc := j.Spec.ProcPerTask
		chosen := ""
		var claimIdx []int
		for _, name := range liveNodes {
			vw := views[name]
			if proc.CanFit(vw.cap.Sub(vw.workUsed)) {
				chosen = name
				claimIdx = nil
				break
			}
			// 试回收：累积该节点上的受害者块直到能放下。
			trialUsed := vw.workUsed
			var idxs []int
			fit := false
			for i, ch := range vw.chunks {
				if vw.claimed[i] {
					continue
				}
				trialUsed = trialUsed.Sub(ch.proc)
				idxs = append(idxs, i)
				if proc.CanFit(vw.cap.Sub(trialUsed)) {
					fit = true
					break
				}
			}
			if fit {
				chosen = name
				claimIdx = idxs
				break
			}
		}
		if chosen == "" {
			// 整组装不下：本轮完全不动（尚未提交任何账本变更）。
			return false
		}
		vw := views[chosen]
		for _, idx := range claimIdx {
			vw.claimed[idx] = true
			vw.workUsed = vw.workUsed.Sub(vw.chunks[idx].proc)
			needVictims[vw.chunks[idx].task.ID.String()] = vw.chunks[idx].task
		}
		vw.workUsed = vw.workUsed.Add(proc)
		placements = append(placements, Placement{Task: t.ID, Node: chosen, Proc: proc})
	}

	// 执行抢占：被杀任务回 PENDING，账本同步归还。
	for _, vt := range needVictims {
		s.clus.StopTask(vt.ID, vt.Node)
		s.ledger.Release(vt.ID)
		vt.State = types.TaskPending
		vt.Node = ""
		vt.Preempted = true
		vt.AwaitingStop = true
		s.log("调度决策-抢占：任务 %s（作业优先级=%d）被作业 %s（优先级=%d）抢占，回队列并归还资源",
			vt.ID, s.jobs[vt.ID.Job].Spec.Priority, j.Spec.Name, j.Spec.Priority)
	}
	if len(needVictims) > 0 {
		seen := map[string]bool{}
		for _, vt := range needVictims {
			if !seen[vt.ID.Job] {
				seen[vt.ID.Job] = true
				s.recomputeJobStateLocked(s.jobs[vt.ID.Job])
			}
		}
	}

	// 整组落账：要么全部成功，要么账本完全不变。
	if !s.ledger.TryReserveGang(placements) {
		s.log("内部错误：作业 %s 整组预留被账本拒绝，放弃本轮", j.Spec.Name)
		return false
	}

	// 整组下发；任一 Launch 失败则回滚已启动部分并整组回队。
	launched := make([]Placement, 0, len(placements))
	timeout := time.Duration(j.Spec.TimeoutMs) * time.Millisecond
	for _, p := range placements {
		task := p.Task
		node := p.Node
		tr := j.Tasks[p.Task.Index]
		// 分配新的启动实例代号：旧实例的迟到事件之后会被实例过滤挡掉。
		tr.Instance++
		instance := tr.Instance
		err := s.clus.Launch(ctx, task, node, instance, func(runCtx context.Context, id types.TaskID, nd string, inst int) {
			s.exec.RunWithTimeout(runCtx, id, nd, inst, timeout)
		})
		if err != nil {
			s.log("作业 %s 在节点 %s 启动失败：%v，整组回滚", j.Spec.Name, node, err)
			tr.Instance--
			s.rollbackGangLocked(j, launched)
			return false
		}
		tr.State = types.TaskRunning
		tr.Node = node
		tr.Attempts++
		tr.LastSeq = 0
		tr.Preempted = false
		tr.AwaitingStop = false
		launched = append(launched, p)
	}

	j.State = types.JobRunning
	s.log("调度决策-整组启动：作业 %s 启动 %d/%d 个任务（抢占 %d 个任务）",
		j.Spec.Name, len(launched), len(j.Tasks), len(needVictims))
	for _, p := range placements {
		s.log("  分配 %s -> %s（{cpu:%d mem:%d gpu:%d}）",
			p.Task, p.Node, p.Proc.CPU, p.Proc.Memory, p.Proc.GPU)
	}
	if err := s.persistLocked(ctx); err != nil {
		s.log("持久化失败: %v", err)
	}
	return true
}

// rollbackGangLocked 在部分启动失败时回滚：杀已启动任务、归还账本、整组回队。
func (s *Scheduler) rollbackGangLocked(j *jobRecord, launched []Placement) {
	for _, p := range launched {
		s.clus.StopTask(p.Task, p.Node)
		s.ledger.Release(p.Task)
		tr := j.Tasks[p.Task.Index]
		tr.State = types.TaskPending
		tr.Node = ""
		tr.Attempts--
	}
	// 受害者状态在正常路径不会被回滚（Launch 失败属于极罕见的节点变化竞态）；
	// 它们已安全回到队列，账本容量仍然可用，下一轮重新调度即可。
	s.recomputeJobStateLocked(j)
}

// persistLocked 把当前状态写入快照存储。
func (s *Scheduler) persistLocked(ctx context.Context) error {
	s.version++
	data := &snapshotData{
		Version:       s.version,
		Jobs:          make([]*jobRecord, 0, len(s.order)),
		NextSubmitSeq: s.nextSubmitSeq,
	}
	for name := range s.lost {
		if s.lost[name] {
			data.LostNodes = append(data.LostNodes, name)
		}
	}
	sort.Strings(data.LostNodes)
	for _, name := range s.order {
		data.Jobs = append(data.Jobs, s.jobs[name])
	}
	b, err := encodeSnapshot(data)
	if err != nil {
		return err
	}
	return s.st.Save(ctx, store.Snapshot{Version: s.version, Data: b})
}

// Recover 从持久化快照恢复调度状态。
// 语义：快照里 RUNNING 的任务在新调度器里没有对应执行实体，一律整组回到队列，
// 由后台扫描重新整组分配；PENDING / 终态任务保持不变；失联节点状态继续保留。
// 恢复后作业从快照继续推进，而不是从头重新提交。
func (s *Scheduler) Recover(ctx context.Context) error {
	snap, ok, err := s.st.Load(ctx)
	if err != nil {
		return err
	}
	if !ok {
		s.log("未找到历史快照，冷启动")
		return nil
	}
	data, err := decodeSnapshot(snap.Data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.version = snap.Version
	s.nextSubmitSeq = data.NextSubmitSeq
	s.jobs = map[string]*jobRecord{}
	s.order = nil
	s.lost = map[string]bool{}
	for _, n := range data.LostNodes {
		s.lost[n] = true
		s.clus.MarkNodeLost(n)
	}
	requeued := 0
	for _, j := range data.Jobs {
		s.jobs[j.Spec.Name] = j
		s.order = append(s.order, j.Spec.Name)
		for _, t := range j.Tasks {
			if t.State == types.TaskRunning {
				t.State = types.TaskPending
				t.Node = ""
				t.AwaitingStop = false
				requeued++
			}
		}
		s.recomputeJobStateLocked(j)
	}
	s.log("从快照版本 %d 恢复：作业 %d 个，失联节点 %d 个，在跑任务回队 %d 个",
		s.version, len(data.Jobs), len(data.LostNodes), requeued)
	s.logLedgerLocked("恢复后")
	s.requestSchedule()
	return nil
}

// JobState 返回作业当前状态（测试/观察用）。
func (s *Scheduler) JobState(name string) (types.JobState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return 0, false
	}
	return j.State, true
}

// TaskState 返回任务当前状态（测试/观察用）。
func (s *Scheduler) TaskState(id types.TaskID) (types.TaskState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, ok := s.lookupTaskLocked(id)
	if !ok {
		return 0, false
	}
	return t.State, true
}

// LedgerSnapshot 返回资源账本快照（测试断言用，含 used<=capacity 校验）。
func (s *Scheduler) LedgerSnapshot() map[string]LedgerNodeSnapshot {
	return s.ledger.Snapshot()
}

// TaskAttempts 返回任务当前运行实例的启动序号（测试用）。
func (s *Scheduler) TaskAttempts(id types.TaskID) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, ok := s.lookupTaskLocked(id)
	if !ok {
		return 0, false
	}
	return t.Attempts, true
}

// TaskFailCount 返回任务已消耗重试预算的失败次数（测试/demo 用）。
func (s *Scheduler) TaskFailCount(id types.TaskID) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, ok := s.lookupTaskLocked(id)
	if !ok {
		return 0, false
	}
	return t.FailCount, true
}
