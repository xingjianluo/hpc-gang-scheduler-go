// demo 在本机演示整组调度、抢占、节点失联与恢复的完整链路。
package main

import (
	"log"
	"os"
	"time"

	sched "github.com/xingjianluo/hpc-gang-scheduler-go/scheduler"
)

func main() {
	store := sched.NewStore("demo-state.json")
	defer os.Remove("demo-state.json")

	nodes := []*sched.Node{
		{ID: "node-1", Capacity: sched.Resource{CPU: 8, Memory: 1024}, Alive: true},
		{ID: "node-2", Capacity: sched.Resource{CPU: 8, Memory: 1024}, Alive: true},
	}
	quotas := map[string]sched.Resource{"team-a": {CPU: 16, Memory: 2048}, "team-b": {CPU: 16, Memory: 2048}}

	s := sched.New(nodes, quotas, store)
	s.SetLogger(func(f string, a ...any) { log.Printf(f, a...) })

	// 1. 低优先级可中断作业占满集群。
	must(s.Submit(sched.JobSpec{JobID: "batch-low", Owner: "team-a", Priority: 1, Preemptible: true, Count: 4, Requires: sched.Resource{CPU: 4}}))
	s.Schedule()

	// 2. 高优先级作业触发抢占。
	must(s.Submit(sched.JobSpec{JobID: "urgent", Owner: "team-b", Priority: 10, Count: 2, Requires: sched.Resource{CPU: 4}}))
	s.Schedule()

	// 3. 节点失联 -> 任务重入队 -> 节点恢复 -> 重新调度。
	s.HandleEvent(sched.Event{Type: sched.EventNodeLost, NodeID: "node-2"})
	s.HandleEvent(sched.Event{Type: sched.EventNodeRecovered, NodeID: "node-2"})
	s.Schedule()

	// 4. 高优先级作业完成，被抢占任务回填。
	s.HandleEvent(sched.Event{Type: sched.EventTaskCompleted, JobID: "urgent", TaskID: "urgent#0", Success: true})
	s.HandleEvent(sched.Event{Type: sched.EventTaskCompleted, JobID: "urgent", TaskID: "urgent#1", Success: true})
	s.Schedule()

	// 5. 从持久化状态恢复（模拟进程重启）。
	s2 := sched.New(nil, quotas, store)
	s2.SetLogger(func(f string, a ...any) { log.Printf(f, a...) })
	if err := s2.Recover(); err != nil {
		log.Fatal(err)
	}
	for id, n := range s2.LedgerSnapshot() {
		log.Printf("recovered ledger node=%s used=%+v cap=%+v alive=%v", id, n.Used, n.Capacity, n.Alive)
	}
	time.Sleep(10 * time.Millisecond)
	log.Println("demo done")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
