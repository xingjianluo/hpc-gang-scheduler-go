package scheduler

import (
	"sync"
	"testing"

	"github.com/xingjianluo/hpc-gang-scheduler-go/internal/types"
)

func TestLedgerReserveAtomicAndInvariant(t *testing.T) {
	l := NewLedger()
	l.AddNode("n1", types.Resources{CPU: 2})
	l.AddNode("n2", types.Resources{CPU: 2})

	p := func(job string, i int, node string) Placement {
		return Placement{Task: types.TaskID{Job: job, Index: i}, Node: node, Proc: types.Resources{CPU: 1}}
	}

	// 成功整组预留 3 个任务。
	if !l.TryReserveGang([]Placement{p("j", 0, "n1"), p("j", 1, "n1"), p("j", 2, "n2")}) {
		t.Fatal("整组预留应当成功")
	}
	if got := l.UsedOn("n1"); got.CPU != 2 {
		t.Fatalf("n1 used=%d，期望 2", got.CPU)
	}

	// 第 4 个任务放不下：整组调用必须完全失败且账本不变。
	before := l.Snapshot()
	if l.TryReserveGang([]Placement{p("k", 0, "n1"), p("k", 1, "n2"), p("k", 2, "n2")}) {
		t.Fatal("容量不足时整组预留必须失败")
	}
	after := l.Snapshot()
	for name, b := range before {
		a := after[name]
		if b.Used != a.Used || b.Running != a.Running {
			t.Fatalf("失败的整组预留改动了账本：before[%s]=%+v after=%+v", name, b, a)
		}
	}

	// 重复落账同一任务必须失败。
	if l.TryReserveGang([]Placement{p("j", 0, "n2")}) {
		t.Fatal("同一任务重复落账必须失败")
	}

	// 释放后容量回来。
	l.Release(types.TaskID{Job: "j", Index: 0})
	if got := l.UsedOn("n1"); got.CPU != 1 {
		t.Fatalf("释放后 n1 used=%d，期望 1", got)
	}
}

func TestLedgerConcurrentNoOverallocation(t *testing.T) {
	l := NewLedger()
	l.AddNode("n1", types.Resources{CPU: 16})

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				placements := []Placement{{
					Task: types.TaskID{Job: "j", Index: g*1000 + k},
					Node: "n1",
					Proc: types.Resources{CPU: 1},
				}}
				if l.TryReserveGang(placements) {
					l.Release(placements[0].Task)
				}
			}
		}(g)
	}
	wg.Wait()
	if n := l.Snapshot()["n1"]; n.Used != (types.Resources{}) || n.Running != 0 {
		t.Fatalf("并发预留/释放后账本未归零: %+v", n)
	}
}
