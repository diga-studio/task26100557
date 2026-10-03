package canary

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
)

// 场景 3：32 个调用方并发领取与回执；旧回执（含跨重试成功后的）重复到达。
// 断言：每台终端无重叠指令、尝试数不超限、重复回执不改变最终结果。
func TestConcurrentClaimAndDuplicateReceipts(t *testing.T) {
	const (
		workers = 32
		total   = 500
		pilots  = 50
	)
	c := NewCoordinator()
	if err := c.CreateBatch("conc", names("d", total), names("d", pilots)); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	var mu sync.Mutex
	outstanding := map[string]string{} // terminal -> 在途指令 ID（测试侧重叠检测）
	attempts := map[string]int{}       // terminal -> 最大尝试序号
	var receipts []Receipt             // 所有已提交回执，用于结尾重放
	rng := rand.New(rand.NewSource(42))

	// recordIssue 检测同一终端不存在重叠指令。
	recordIssue := func(cmd Command) {
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := outstanding[cmd.Terminal]; ok {
			t.Errorf("terminal %s has overlapping commands: %s and %s", cmd.Terminal, prev, cmd.ID)
		}
		outstanding[cmd.Terminal] = cmd.ID
		if cmd.Kind == CommandUpgrade {
			if cmd.Attempt < 1 || cmd.Attempt > maxAttempts {
				t.Errorf("terminal %s attempt %d out of range", cmd.Terminal, cmd.Attempt)
			}
			if cmd.Attempt > attempts[cmd.Terminal] {
				attempts[cmd.Terminal] = cmd.Attempt
			}
		}
	}
	recordReceipt := func(cmd Command, ok bool) {
		mu.Lock()
		defer mu.Unlock()
		if outstanding[cmd.Terminal] != cmd.ID {
			t.Errorf("terminal %s receipt for %s, outstanding %s", cmd.Terminal, cmd.ID, outstanding[cmd.Terminal])
		}
		delete(outstanding, cmd.Terminal)
		receipts = append(receipts, Receipt{CommandID: cmd.ID, Success: ok})
	}
	// dupReceipt 随机重放一条旧回执（可能跨越后续重试成功之后到达）。
	dupReceipt := func() {
		mu.Lock()
		if len(receipts) == 0 {
			mu.Unlock()
			return
		}
		r := receipts[rng.Intn(len(receipts))]
		mu.Unlock()
		if err := c.Report("conc", r); err != nil {
			t.Errorf("duplicate Report(%s): %v", r.CommandID, err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			local := rand.New(rand.NewSource(seed))
			for {
				cmd, err := c.Next("conc")
				if errors.Is(err, ErrNoCommand) {
					s, serr := c.Status("conc")
					if serr != nil {
						t.Errorf("Status: %v", serr)
						return
					}
					if s.State == StateCompleted || s.State == StateRolledBack {
						return
					}
					continue // 指令被其他调用方领走在途，稍后再试
				}
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				recordIssue(cmd)
				// 升级 80% 成功；回滚按假设一定成功。
				ok := cmd.Kind == CommandRollback || local.Intn(100) < 80
				// 先在测试台账中销记再上报，保证台账与库内在途状态一致。
				recordReceipt(cmd, ok)
				if err := c.Report("conc", Receipt{CommandID: cmd.ID, Success: ok}); err != nil {
					t.Errorf("Report(%s): %v", cmd.ID, err)
					return
				}
				if local.Intn(10) < 2 {
					dupReceipt()
				}
			}
		}(int64(w) + 1)
	}
	wg.Wait()

	// 全部旧回执再重放一遍：不得改变任何计数与终态。
	before, err := c.Status("conc")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	mu.Lock()
	all := append([]Receipt{}, receipts...)
	mu.Unlock()
	for _, r := range all {
		if err := c.Report("conc", r); err != nil {
			t.Fatalf("replay Report(%s): %v", r.CommandID, err)
		}
	}
	after, err := c.Status("conc")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if before.State != after.State ||
		before.TotalFailed != after.TotalFailed ||
		before.PilotFailed != after.PilotFailed ||
		before.Succeeded != after.Succeeded ||
		before.RolledBack != after.RolledBack ||
		before.InFlight != after.InFlight {
		t.Fatalf("duplicate receipts changed state: %+v -> %+v", before, after)
	}
	for name, ts := range after.Terminals {
		if ts.Attempts > maxAttempts {
			t.Errorf("terminal %s attempts=%d exceeds %d", name, ts.Attempts, maxAttempts)
		}
		if ts.InFlight {
			t.Errorf("terminal %s still in flight at terminal state", name)
		}
	}

	// 终态一致性：若已回滚，回滚集合恰好等于升级成功集合；
	// 若发布完成，失败比例必须在阈值以内。
	switch after.State {
	case StateRolledBack:
		for name, ts := range after.Terminals {
			if ts.RolledBack != ts.Succeeded {
				t.Errorf("terminal %s succeeded=%v rolledBack=%v", name, ts.Succeeded, ts.RolledBack)
			}
		}
	case StateCompleted:
		if after.TotalFailed*5 > after.Total {
			t.Errorf("completed with failure ratio %d/%d over threshold", after.TotalFailed, after.Total)
		}
	default:
		t.Fatalf("unexpected final state %v", after.State)
	}
	t.Logf("final state=%v succeeded=%d failed=%d rolledBack=%d",
		after.State, after.Succeeded, after.TotalFailed, after.RolledBack)
}
