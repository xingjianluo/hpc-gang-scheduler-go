// Package scheduler 实现一个进程内的 HPC 整组（gang）作业调度器，
// 用模拟节点与资源池代替真实集群，支持整组调度、配额、优先级抢占、
// 资源回填、任务重试、超时与节点失联后的状态恢复。
package scheduler

import "time"

// Resource 表示一类可调度资源的数量（如 CPU 核数、内存 MB、GPU 卡数）。
type Resource struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	GPU    int64 `json:"gpu"`
}

// Add 返回两者之和。
func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory, GPU: r.GPU + o.GPU}
}

// Sub 返回两者之差（调用方需保证不为负）。
func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory, GPU: r.GPU - o.GPU}
}

// Fits 判断 r 是否不超过 cap。
func (r Resource) Fits(cap Resource) bool {
	return r.CPU <= cap.CPU && r.Memory <= cap.Memory && r.GPU <= cap.GPU
}

// IsZero 判断资源是否为空。
func (r Resource) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0 && r.GPU == 0
}

// TaskState 任务生命周期状态。
type TaskState string

const (
	TaskPending   TaskState = "PENDING"
	TaskRunning   TaskState = "RUNNING"
	TaskSucceeded TaskState = "SUCCEEDED"
	TaskFailed    TaskState = "FAILED"
	TaskPreempted TaskState = "PREEMPTED" // 被抢占，等待重新入队
)

// Terminal 判断任务是否已到达终态。
func (s TaskState) Terminal() bool {
	return s == TaskSucceeded || s == TaskFailed
}

// JobState 作业生命周期状态。
type JobState string

const (
	JobQueued    JobState = "QUEUED"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED"
	JobRejected  JobState = "REJECTED"
)

// RejectReason 整组拒绝的可区分原因。
type RejectReason string

const (
	RejectNone           RejectReason = ""
	RejectDuplicate      RejectReason = "DUPLICATE_SUBMISSION"
	RejectQuotaExceeded  RejectReason = "QUOTA_EXCEEDED"
	RejectResourceUnfits RejectReason = "RESOURCE_NEVER_FITS"
)

// TaskSpec 单个任务的资源需求与标识。
type TaskSpec struct {
	TaskID   string   `json:"task_id"`
	Requires Resource `json:"requires"`
}

// JobSpec 作业提交描述。Count>1 时按数组作业展开为 Count 个任务。
type JobSpec struct {
	JobID       string        `json:"job_id"`
	Owner       string        `json:"owner"`
	Priority    int           `json:"priority"`     // 数值越大优先级越高
	Preemptible bool          `json:"preemptible"`  // 是否可被高优先级作业抢占
	Count       int           `json:"count"`        // 任务数（作业数组大小）
	Requires    Resource      `json:"requires"`     // 每个任务的资源需求
	MaxRetries  int           `json:"max_retries"`  // 单任务失败最大重试次数
	ExecTimeout time.Duration `json:"exec_timeout"` // 单任务执行超时，0 表示不限制
	SubmitTime  time.Time     `json:"submit_time"`
}

// Task 运行时任务记录。
type Task struct {
	Spec      TaskSpec  `json:"spec"`
	JobID     string    `json:"job_id"`
	State     TaskState `json:"state"`
	NodeID    string    `json:"node_id"`
	Attempt   int       `json:"attempt"`
	StartedAt time.Time `json:"started_at,omitempty"`
}

// Job 运行时作业记录。
type Job struct {
	Spec      JobSpec      `json:"spec"`
	State     JobState     `json:"state"`
	Reject    RejectReason `json:"reject_reason,omitempty"`
	Tasks     []*Task      `json:"tasks"`
	FinishedN int          `json:"finished_n"` // 到达终态的任务数（去重后）
	FailedN   int          `json:"failed_n"`
}

// EventType 外部上报事件类型。
type EventType string

const (
	EventTaskCompleted EventType = "TASK_COMPLETED" // 任务完成（成功/失败由 Success 字段区分）
	EventNodeLost      EventType = "NODE_LOST"      // 节点失联
	EventNodeRecovered EventType = "NODE_RECOVERED" // 节点恢复
)

// Event 是节点/执行层上报给调度器的事件，允许重复或乱序到达。
type Event struct {
	Type      EventType `json:"type"`
	TaskID    string    `json:"task_id,omitempty"`
	JobID     string    `json:"job_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
	Success   bool      `json:"success,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}
