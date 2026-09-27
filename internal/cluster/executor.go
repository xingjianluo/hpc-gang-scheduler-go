package cluster

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// TaskSpec 描述一个任务该怎么模拟执行。
type TaskSpec struct {
	// Duration 正常执行时长（<=0 表示立即完成）。
	Duration time.Duration
	// Fail 设置为 true 时，执行结束后发出 failed 而非 completed。
	Fail bool
	// Timeout 任务超时时间（<=0 表示不设超时，由调度器下发时决定）。
	Timeout time.Duration
}

// Executor 把"任务开始/结束"事件回送给调度器，并负责执行超时计时。
//
// 事件保证：
//   - 每个 Run 恰好发出一对 started + 终态(completed/failed/timeout) 事件，
//     但事件到达调度器后仍可能因重发/乱序被去重，Executor 自己不负责去重。
//   - ctx 取消（被抢占或节点失联）时等待中断：Fail 的任务发 failed，
//     其余被取消的任务发 failed（由调度器判定是重试还是按抢占回队）。
//   - 同一个 Executor 发出的 Sequence 单调递增，调度器据此识别过期事件。
type Executor struct {
	emit func(ev types.Event)
	seq  atomic.Int64

	// Specs 允许测试/调用方按任务定制执行行为；缺省任务立即成功。
	specs func(task types.TaskID) TaskSpec
}

// NewExecutor 创建执行器。emit 用于把事件送回调度器。
// specs 可为 nil，表示所有任务立即成功。
func NewExecutor(emit func(ev types.Event), specs func(task types.TaskID) TaskSpec) *Executor {
	return &Executor{emit: emit, specs: specs}
}

func (e *Executor) nextSeq() int64 { return e.seq.Add(1) }

func (e *Executor) spec(task types.TaskID) TaskSpec {
	if e.specs == nil {
		return TaskSpec{}
	}
	return e.specs(task)
}

// Run 模拟一次任务执行，应在集群为该任务准备的 goroutine 中被调用。
// 不返回错误：任何执行结果都通过事件表达。行为取自 NewExecutor 注入的 specs。
func (e *Executor) Run(ctx context.Context, task types.TaskID, node string, instance int) {
	e.RunWith(ctx, task, node, instance, e.spec(task))
}

// RunWithTimeout 按 specs 配置的行为执行任务，仅由调度器叠加任务级超时
// （不会覆盖 Duration / Fail 等定制行为；timeout<=0 表示不超时）。
func (e *Executor) RunWithTimeout(ctx context.Context, task types.TaskID, node string, instance int, timeout time.Duration) {
	spec := e.spec(task)
	spec.Timeout = timeout
	e.RunWith(ctx, task, node, instance, spec)
}

// RunWith 用一次性 spec 覆盖默认行为执行任务（调度器用它下发任务级超时）。
func (e *Executor) RunWith(ctx context.Context, task types.TaskID, node string, instance int, spec TaskSpec) {
	e.emit(types.Event{TaskID: task, Kind: types.EventStarted, NodeName: node, Instance: instance, Sequence: e.nextSeq()})

	d := spec.Duration
	if d < 0 {
		d = 0
	}

	var finishKind string
	timer := time.NewTimer(d)
	defer timer.Stop()
	if spec.Timeout > 0 {
		deadline := time.NewTimer(spec.Timeout)
		defer deadline.Stop()
		select {
		case <-deadline.C:
			finishKind = types.EventTimeout
		case <-timer.C:
			finishKind, _ = execFinish(spec)
		case <-ctx.Done():
			finishKind = types.EventFailed
		}
	} else {
		select {
		case <-timer.C:
			finishKind, _ = execFinish(spec)
		case <-ctx.Done():
			finishKind = types.EventFailed
		}
	}

	e.emit(types.Event{TaskID: task, Kind: finishKind, NodeName: node, Instance: instance, Sequence: e.nextSeq()})
}

// execFinish 返回正常执行结束的事件类型。
func execFinish(spec TaskSpec) (string, bool) {
	if spec.Fail {
		return types.EventFailed, true
	}
	return types.EventCompleted, false
}
