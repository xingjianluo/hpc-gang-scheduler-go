package types

import "fmt"

// Resources 表示一份 CPU / 内存 / GPU 资源量。
// 所有维度都按非负整数处理；账本在任何时刻都必须保证 used <= capacity。
type Resources struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	GPU    int64 `json:"gpu"`
}

// Add 返回 r+o。
func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory, GPU: r.GPU + o.GPU}
}

// Sub 返回 r-o。调用方需自行保证结果非负（账本层会做断言）。
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory, GPU: r.GPU - o.GPU}
}

// CanFit 判断 r 是否能放进 capacity（逐维度 r <= capacity）。
func (r Resources) CanFit(capacity Resources) bool {
	return r.CPU <= capacity.CPU && r.Memory <= capacity.Memory && r.GPU <= capacity.GPU
}

// Greater 判断 r 是否在任一维度上严格大于 o（即放不进 o）。
func (r Resources) Greater(o Resources) bool {
	return r.CPU > o.CPU || r.Memory > o.Memory || r.GPU > o.GPU
}

// LessEqual 判断 r 是否逐维度小于等于 o。
func (r Resources) LessEqual(o Resources) bool {
	return r.CPU <= o.CPU && r.Memory <= o.Memory && r.GPU <= o.GPU
}

// IsZero 判断是否为空资源。
func (r Resources) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0 && r.GPU == 0
}

// MaxResources 返回逐维度最大值。
func MaxResources(a, b Resources) Resources {
	max := func(x, y int64) int64 {
		if x > y {
			return x
		}
		return y
	}
	return Resources{CPU: max(a.CPU, b.CPU), Memory: max(a.Memory, b.Memory), GPU: max(a.GPU, b.GPU)}
}

// TaskID 由作业名 + 数组下标构成，全局唯一标识一个任务。
type TaskID struct {
	Job   string `json:"job"`
	Index int    `json:"index"`
}

func (t TaskID) String() string { return fmt.Sprintf("%s[%d]", t.Job, t.Index) }

// TaskState 任务状态。
type TaskState int

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskCompleted
	TaskFailed
)

func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "PENDING"
	case TaskRunning:
		return "RUNNING"
	case TaskCompleted:
		return "COMPLETED"
	case TaskFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Terminal 判断是否为终态（终态任务不允许再被任何事件改回运行中）。
func (s TaskState) Terminal() bool { return s == TaskCompleted || s == TaskFailed }

// JobState 作业（整组）状态。
type JobState int

const (
	JobQueued JobState = iota
	JobRunning
	JobCompleted
	JobFailed
)

func (s JobState) String() string {
	switch s {
	case JobQueued:
		return "QUEUED"
	case JobRunning:
		return "RUNNING"
	case JobCompleted:
		return "COMPLETED"
	case JobFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// RejectReason 整组拒绝的可区分原因。
type RejectReason string

const (
	// RejectDuplicate 重复提交：同名作业已存在（无论处于什么状态）。
	RejectDuplicate RejectReason = "duplicate_submission"
	// RejectQuotaExceeded 超出租户配额：作业整组需求超过该租户的配额上限。
	RejectQuotaExceeded RejectReason = "quota_exceeded"
	// RejectInsufficientCap 资源不足：集群没有任何节点能容纳单个任务，
	// 或集群总容量不足以容纳整组，作业永远无法被调度。
	RejectInsufficientCap RejectReason = "insufficient_capacity"
)

// JobSpec 提交规格。Procs=1 为普通作业；>1 为作业数组（整组调度）。
type JobSpec struct {
	Name          string    `json:"name"`
	Tenant        string    `json:"tenant"`
	Priority      int       `json:"priority"`
	ProcPerTask   Resources `json:"proc_per_task"`
	Procs         int       `json:"procs"`
	Retries       int       `json:"retries"`
	TimeoutMs     int64     `json:"timeout_ms"`
	Interruptible bool      `json:"interruptible"`
}

// TotalRequest 整组资源需求。
func (s JobSpec) TotalRequest() Resources {
	total := s.ProcPerTask
	return Resources{
		CPU:    total.CPU * int64(s.Procs),
		Memory: total.Memory * int64(s.Procs),
		GPU:    total.GPU * int64(s.Procs),
	}
}

// Event 是调度器接收的执行事件，可能重复或乱序到达。
// Instance 标识事件来自任务的第几次启动（attempt），调度器用它识别
// "被抢占/失联后旧执行实例迟到的事件"；同一 Instance 内再用 Sequence
// 做乱序/重复判定。节点级事件不使用这两个字段。
type Event struct {
	TaskID   TaskID `json:"task_id"`
	Kind     string `json:"kind"`
	NodeName string `json:"node_name"`
	Instance int    `json:"instance"`
	Sequence int64  `json:"sequence"`
}

const (
	EventStarted   = "started"
	EventCompleted = "completed"
	EventFailed    = "failed"
	EventTimeout   = "timeout"
	EventNodeLost  = "node_lost"
	EventNodeUp    = "node_up"
)

// RejectError 携带可区分原因的提交拒绝错误。
type RejectError struct {
	Reason RejectReason
	Msg    string
}

func (e *RejectError) Error() string { return string(e.Reason) + ": " + e.Msg }

// NewRejectError 构造拒绝错误。
func NewRejectError(reason RejectReason, format string, args ...any) *RejectError {
	return &RejectError{Reason: reason, Msg: fmt.Sprintf(format, args...)}
}
