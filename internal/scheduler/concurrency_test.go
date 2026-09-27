package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/cluster"
	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

// TestConcurrentSubmitNoDoubleAllocation 多个提交方并发提交作业数组：
// 在 -race 下持续断言账本不超分、同一任务不被重复计数、已用资源不超过节点容量。
func TestConcurrentSubmitNoDoubleAllocation(t *testing.T) {
	h := newHarness(t, Config{
		Quotas: map[string]types.Resources{
			"t1": {CPU: 1000, Memory: 4000},
			"t2": {CPU: 1000, Memory: 4000},
		},
	}, map[string]types.Resources{
		"n1": {CPU: 8, Memory: 32},
		"n2": {CPU: 8, Memory: 32},
		"n3": {CPU: 8, Memory: 32},
	})
	h.start()
	defer h.close()

	const submitters = 12
	const perSubmitter = 8
	var wg sync.WaitGroup
	var accepted, rejected int64

	stop := make(chan struct{})
	go func() {
		// 高频巡检账本不变量。
		for {
			select {
			case <-stop:
				return
			default:
				h.assertLedgerWithinCapacity()
				seen := map[string]bool{}
				for name, n := range h.sch.LedgerSnapshot() {
					if n.Running < 0 || n.Used.CPU < 0 {
						t.Errorf("节点 %s 非法计数 %+v", name, n)
					}
					// 任务数与已用 CPU 必须一致（每个任务 1 CPU）。
					if n.Used.CPU != int64(n.Running) {
						t.Errorf("节点 %s 已用 CPU=%d 与运行任务数=%d 不一致（疑似重复/漏记）",
							name, n.Used.CPU, n.Running)
					}
				}
				_ = seen
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for s := 0; s < submitters; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			tenant := "t1"
			if s%2 == 0 {
				tenant = "t2"
			}
			for k := 0; k < perSubmitter; k++ {
				name := fmt.Sprintf("s%02d-j%02d", s, k)
				err := h.submit(types.JobSpec{
					Name: name, Tenant: tenant, Priority: (s + k) % 5, Procs: 2,
					ProcPerTask:   types.Resources{CPU: 1, Memory: 2},
					Interruptible: k%2 == 0,
				})
				if err == nil {
					atomic.AddInt64(&accepted, 1)
				} else {
					atomic.AddInt64(&rejected, 1)
				}
			}
		}(s)
	}
	wg.Wait()

	// 等待所有作业收敛到终态。
	h.waitFor("所有并发作业收敛", 30*time.Second, func() bool {
		for s := 0; s < submitters; s++ {
			for k := 0; k < perSubmitter; k++ {
				name := fmt.Sprintf("s%02d-j%02d", s, k)
				st, ok := h.sch.JobState(name)
				if !ok {
					continue // 被拒绝（配额）的作业不在系统中
				}
				if st != types.JobCompleted && st != types.JobFailed {
					return false
				}
			}
		}
		return true
	})
	close(stop)

	// 收敛后账本必须全部归零。
	for name, n := range h.sch.LedgerSnapshot() {
		if n.Running != 0 || n.Used != (types.Resources{}) {
			t.Fatalf("收敛后节点 %s 仍有占用: %+v", name, n)
		}
	}
	t.Logf("并发提交结果：accepted=%d rejected=%d", accepted, rejected)
	h.logLedger()
}

// TestConcurrentSubmitDuplicateNames 并发提交同名作业：有且仅有一个成功，
// 其余必须全部以 duplicate_submission 被整组拒绝。
func TestConcurrentSubmitDuplicateNames(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 64},
	})
	h.start()
	defer h.close()

	const n = 32
	var wg sync.WaitGroup
	var wins, dupRejects int64
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := h.submit(types.JobSpec{
				Name: "same-name", Tenant: "a", Procs: 1,
				ProcPerTask: types.Resources{CPU: 1},
			})
			if err == nil {
				atomic.AddInt64(&wins, 1)
			} else if re, ok := err.(*types.RejectError); ok && re.Reason == types.RejectDuplicate {
				atomic.AddInt64(&dupRejects, 1)
			} else {
				t.Errorf("非预期错误: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("同名并发提交成功数=%d，期望恰好 1", wins)
	}
	if dupRejects != n-1 {
		t.Fatalf("重复拒绝数=%d，期望 %d", dupRejects, n-1)
	}
	h.waitJobState("same-name", types.JobCompleted)
}

// TestPreemptionUnderConcurrentLoad 抢占竞态：集群满载可中断低优先级作业期间，
// 并发提交多个高优先级作业，断言抢占过程中账本始终不超分，且最终全部收敛。
func TestPreemptionUnderConcurrentLoad(t *testing.T) {
	h := newHarness(t, Config{}, map[string]types.Resources{
		"n1": {CPU: 4},
		"n2": {CPU: 4},
	})
	h.setSpec("base", cluster.TaskSpec{Duration: 1 * time.Hour})
	h.start()
	defer h.close()

	// 占满 8 份的可中断底座。
	if err := h.submit(types.JobSpec{
		Name: "base", Tenant: "a", Priority: 1, Procs: 8,
		ProcPerTask: types.Resources{CPU: 1}, Interruptible: true,
	}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("base 占满集群", time.Second, func() bool {
		r := 0
		for _, n := range h.sch.LedgerSnapshot() {
			r += n.Running
		}
		return r == 8
	})

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				h.assertLedgerWithinCapacity()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = h.submit(types.JobSpec{
				Name: fmt.Sprintf("hi%d", i), Tenant: "a", Priority: 10 + i, Procs: 2,
				ProcPerTask: types.Resources{CPU: 1},
			})
		}(i)
	}
	wg.Wait()

	h.waitFor("6 个高优先级作业全部完成", 15*time.Second, func() bool {
		for i := 0; i < 6; i++ {
			if h.jobState(fmt.Sprintf("hi%d", i)) != types.JobCompleted {
				return false
			}
		}
		return true
	})
	close(stop)

	// 高优先级全部完成后，base 整组重新跑起来（8 份）。
	h.waitFor("base 重新整组占满", 10*time.Second, func() bool {
		r := 0
		for _, n := range h.sch.LedgerSnapshot() {
			r += n.Running
		}
		return r == 8
	})
	for i := 0; i < 8; i++ {
		if h.taskState(tid("base", i)) != types.TaskRunning {
			t.Fatalf("base[%d] 未重新运行", i)
		}
		if fc, _ := h.sch.TaskFailCount(tid("base", i)); fc != 0 {
			t.Fatalf("base[%d] 抢占消耗了失败计数 %d", i, fc)
		}
	}
	h.assertLedgerWithinCapacity()
	h.logLedger()
}
