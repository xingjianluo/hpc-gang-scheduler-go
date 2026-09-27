package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/scheduler"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/store"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	snapPath := flag.String("snapshot", filepath.Join(os.TempDir(), "hpcsim-snapshot.json"), "快照文件路径")
	tick := flag.Duration("tick", 20*time.Millisecond, "调度扫描间隔")
	keep := flag.Bool("keep-snapshot", false, "结束后保留快照文件（用于演示恢复）")
	flag.Parse()

	logger := log.New(os.Stdout, "[scheduler] ", log.Ltime|log.Lmicroseconds)
	logf := func(format string, args ...any) { logger.Printf(format, args...) }

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1) 进程内模拟集群：3 个节点，总 CPU 10。
	clus := cluster.NewCluster()
	clus.AddNode(cluster.NewNode("node-a", types.Resources{CPU: 4, Memory: 16, GPU: 1}))
	clus.AddNode(cluster.NewNode("node-b", types.Resources{CPU: 4, Memory: 16, GPU: 1}))
	clus.AddNode(cluster.NewNode("node-c", types.Resources{CPU: 2, Memory: 8, GPU: 0}))

	st := store.NewFileStore(*snapPath)

	var sch *scheduler.Scheduler
	// 2) 执行器：batch-low 是长任务（等着被抢占）；
	//    flaky 仅首次尝试失败（演示失败->重试->成功）；slow 必然超时。
	exec := cluster.NewExecutor(func(ev types.Event) {
		sch.HandleEvent(ctx, ev)
	}, func(id types.TaskID) cluster.TaskSpec {
		switch id.Job {
		case "batch-low":
			return cluster.TaskSpec{Duration: 1 * time.Hour}
		case "flaky":
			// 前两次真正失败，第三次成功；配合 Retries=2 演示
			// "失败->重试->失败->重试->成功"（被抢占/失联不计入）。
			fails, _ := sch.TaskFailCount(id)
			return cluster.TaskSpec{Duration: 50 * time.Millisecond, Fail: fails < 2}
		case "slow":
			return cluster.TaskSpec{Duration: 10 * time.Second}
		default:
			return cluster.TaskSpec{Duration: 200 * time.Millisecond}
		}
	})

	sch = scheduler.New(scheduler.Config{
		TickInterval: *tick,
		Quotas: map[string]types.Resources{
			"team-a": {CPU: 40, Memory: 160},
			"team-b": {CPU: 40, Memory: 160},
			"team-q": {CPU: 4, Memory: 16},
		},
	}, clus, exec, st, logf)

	// 3) 从快照恢复（文件不存在则冷启动）。
	if err := sch.Recover(ctx); err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	if err := sch.Start(ctx); err != nil {
		return err
	}
	defer sch.Close()

	submit := func(spec types.JobSpec) {
		if err := sch.Submit(ctx, spec); err != nil {
			if re, ok := err.(*types.RejectError); ok {
				logf("提交被拒绝 reason=%s job=%s msg=%s", re.Reason, spec.Name, re.Msg)
				return
			}
			logf("提交失败 job=%s err=%v", spec.Name, err)
		}
	}

	// 4) 场景脚本。

	// 4.1 三类整组拒绝：容量不可能 / 重复 / 超配额。
	submit(types.JobSpec{Name: "too-big", Tenant: "team-b", Priority: 1, Procs: 1,
		ProcPerTask: types.Resources{CPU: 100}}) // 任何节点都放不下 -> insufficient_capacity
	submit(types.JobSpec{Name: "batch-low", Tenant: "team-a", Priority: 1, Procs: 4,
		ProcPerTask: types.Resources{CPU: 2, Memory: 4}, Interruptible: true})
	submit(types.JobSpec{Name: "batch-low", Tenant: "team-a", Priority: 1, Procs: 1,
		ProcPerTask: types.Resources{CPU: 1}}) // duplicate_submission
	submit(types.JobSpec{Name: "quota-bomb", Tenant: "team-q", Priority: 1, Procs: 4,
		ProcPerTask: types.Resources{CPU: 2, Memory: 2}}) // 整组 8 CPU > team-q 配额 4
	time.Sleep(300 * time.Millisecond) // 等 batch-low 整组跑起来并占满集群

	// 4.2 高优先级作业到达，抢占可中断的低优先级任务才能整组启动。
	submit(types.JobSpec{Name: "urgent", Tenant: "team-a", Priority: 10, Procs: 4,
		ProcPerTask: types.Resources{CPU: 2, Memory: 4}})
	time.Sleep(500 * time.Millisecond) // 等 urgent 整组跑完（200ms），被抢占的 batch-low 重新排队

	// 4.3 节点失联（连发两次模拟重复事件）：把重新跑起来的部分任务打回队列，然后恢复。
	logf(">>> 注入 node-a 失联（连发两次，模拟重复事件）")
	sch.HandleEvent(ctx, types.Event{Kind: types.EventNodeLost, NodeName: "node-a"})
	sch.HandleEvent(ctx, types.Event{Kind: types.EventNodeLost, NodeName: "node-a"})
	time.Sleep(300 * time.Millisecond)
	logf(">>> node-a 恢复，调度从持久化状态继续")
	sch.HandleEvent(ctx, types.Event{Kind: types.EventNodeUp, NodeName: "node-a"})
	time.Sleep(300 * time.Millisecond)

	// 4.4 失败重试 + 执行超时（这两个小作业会在节点恢复后的空闲容量上运行）。
	submit(types.JobSpec{Name: "flaky", Tenant: "team-a", Priority: 5, Procs: 1,
		ProcPerTask: types.Resources{CPU: 1, Memory: 1}, Retries: 2})
	submit(types.JobSpec{Name: "slow", Tenant: "team-a", Priority: 5, Procs: 1,
		ProcPerTask: types.Resources{CPU: 1, Memory: 1}, TimeoutMs: 300, Retries: 1})
	time.Sleep(1200 * time.Millisecond)

	logf("最终作业状态：")
	for _, name := range []string{"batch-low", "too-big", "quota-bomb", "urgent", "flaky", "slow"} {
		if stt, ok := sch.JobState(name); ok {
			logf("  %-12s %s", name, stt)
		} else {
			logf("  %-12s REJECTED(未入系统)", name)
		}
	}
	logf("最终资源账本快照:\n%s", scheduler.FormatSnapshot(sch.LedgerSnapshot()))

	if !*keep {
		_ = os.Remove(*snapPath)
	} else {
		logf("快照已保留在 %s，可重启程序观察 Recover 日志", *snapPath)
	}
	return nil
}
