package scheduler

import (
	"encoding/json"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// taskRecord 任务运行期记录（受 scheduler 主锁保护）。
type taskRecord struct {
	ID    types.TaskID    `json:"id"`
	State types.TaskState `json:"state"`
	Node  string          `json:"node"`
	// Attempts 是当前运行实例的启动序号（每次 Launch 递增），仅用于
	// 把执行事件匹配到正确的实例，与重试预算无关。
	Attempts int `json:"attempts"`
	// FailCount 是真正消耗重试预算的失败次数（failed/timeout）；
	// 被抢占、节点失联导致的回队重排不计数。
	FailCount int `json:"fail_count"`
	// Preempted 表示当前的 PENDING 是被抢占导致的（用于日志/指标）。
	Preempted bool `json:"preempted"`
	// AwaitingStop 表示调度器已经主动停掉该任务（抢占/失联），
	// 旧执行实例随后送达的终态事件应被忽略，不能重复计数或改回运行中。
	AwaitingStop bool `json:"awaiting_stop"`
	// LastSeq 是该任务"当前实例"已接收事件的最大序号，用于同实例内重复/乱序过滤。
	LastSeq int64 `json:"last_seq"`
	// Instance 是调度器每次启动任务时分配的实例代号；
	// 旧实例（被抢占/失联/超时杀掉）迟到的事件会被识别并忽略。
	Instance int `json:"instance"`
}

// jobRecord 作业运行期记录。
type jobRecord struct {
	Spec      types.JobSpec  `json:"spec"`
	State     types.JobState `json:"state"`
	SubmitSeq int64          `json:"submit_seq"`
	Tasks     []*taskRecord  `json:"tasks"`
}

// snapshotData 持久化快照载荷。
type snapshotData struct {
	Version       int64        `json:"version"`
	LostNodes     []string     `json:"lost_nodes"`
	Jobs          []*jobRecord `json:"jobs"`
	NextSubmitSeq int64        `json:"next_submit_seq"`
}

func encodeSnapshot(d *snapshotData) ([]byte, error) {
	return json.Marshal(d)
}

func decodeSnapshot(b []byte) (*snapshotData, error) {
	d := &snapshotData{}
	if err := json.Unmarshal(b, d); err != nil {
		return nil, err
	}
	return d, nil
}
