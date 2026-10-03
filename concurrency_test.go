package canary

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// TestConcurrentClaimAndReport：单批 5000 台终端、32 个调用方并发领取与回执。
// 验证：指令 ID 不重复、每台终端任一时刻最多一条在途指令、尝试数不超限、
// 未触线时最终发布完成；旧回执在重试成功后重复到达不改变最终结果。
func TestConcurrentClaimAndReport(t *testing.T) {
	const (
		total    = 5000
		canaries = 250
		workers  = 32
	)
	targets := make([]string, total)
	for i := range targets {
		targets[i] = fmt.Sprintf("dev-%04d", i)
	}
	c := NewCoordinator()
	if err := c.CreateBatch("big", targets, targets[:canaries]); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	// 结果模式：编号 %7==0 的终端前 3 次失败、第 4 次成功；其余一次成功。
	// 最终失败数为 0，不会触线。
	succeeds := func(deviceID string, attempt int) bool {
		n, err := strconv.Atoi(deviceID[len("dev-"):])
		if err != nil {
			t.Fatalf("bad device id %q", deviceID)
		}
		return n%7 != 0 || attempt == MaxAttempts
	}

	var mu sync.Mutex
	claimedIDs := map[string]bool{}
	outstanding := map[string]string{} // device -> 在途指令 ID

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				cmd, ok, err := c.Claim("big")
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if !ok {
					v, err := c.Batch("big")
					if err != nil {
						t.Errorf("Batch: %v", err)
						return
					}
					if v.Phase == PhaseReleased || v.Phase == PhaseRolledBack {
						return
					}
					runtime.Gosched()
					continue
				}
				mu.Lock()
				if claimedIDs[cmd.ID] {
					t.Errorf("command %s claimed twice", cmd.ID)
				}
				claimedIDs[cmd.ID] = true
				if prev, busy := outstanding[cmd.DeviceID]; busy {
					t.Errorf("device %s has overlapping commands %s and %s", cmd.DeviceID, prev, cmd.ID)
				}
				outstanding[cmd.DeviceID] = cmd.ID
				mu.Unlock()

				mu.Lock()
				delete(outstanding, cmd.DeviceID)
				mu.Unlock()
				// 回执可能立即产生该终端的下一条指令并被其他调用方领取，
				// 因此先在本地账本中清除在途记录再提交回执。
				if err := c.Report("big", cmd.ID, succeeds(cmd.DeviceID, cmd.Attempt)); err != nil {
					t.Errorf("Report(%s): %v", cmd.ID, err)
				}
			}
		}()
	}
	wg.Wait()

	v := mustPhase(t, c, "big", PhaseReleased)
	if v.FinalFailures != 0 || v.Succeeded != total {
		t.Fatalf("view = %+v, want 0 failures and %d successes", v, total)
	}
	// 尝试数不超限：%7==0 的终端恰好 4 次，其余恰好 1 次。
	for _, id := range targets {
		dv, err := c.Device("big", id)
		if err != nil {
			t.Fatalf("Device: %v", err)
		}
		if dv.Attempts < 1 || dv.Attempts > MaxAttempts {
			t.Fatalf("device %s attempts = %d, out of range", id, dv.Attempts)
		}
		n, _ := strconv.Atoi(id[len("dev-"):])
		want := 1
		if n%7 == 0 {
			want = MaxAttempts
		}
		if dv.Attempts != want {
			t.Fatalf("device %s attempts = %d, want %d", id, dv.Attempts, want)
		}
		if dv.State != DeviceSucceeded {
			t.Fatalf("device %s state = %s, want succeeded", id, dv.State)
		}
	}

	// 旧回执（含重试成功前的失败回执）重复到达：全部忽略，结果不变。
	mu.Lock()
	allIDs := make([]string, 0, len(claimedIDs))
	for id := range claimedIDs {
		allIDs = append(allIDs, id)
	}
	mu.Unlock()
	for _, id := range allIDs {
		if err := c.Report("big", id, false); err != nil {
			t.Fatalf("duplicate receipt %s: %v", id, err)
		}
		if err := c.Report("big", id, true); err != nil {
			t.Fatalf("duplicate receipt %s: %v", id, err)
		}
	}
	after, err := c.Batch("big")
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if after != v {
		t.Fatalf("duplicate receipts changed result: %+v -> %+v", v, after)
	}
	for _, id := range targets {
		dv, err := c.Device("big", id)
		if err != nil {
			t.Fatalf("Device: %v", err)
		}
		if dv.State != DeviceSucceeded {
			t.Fatalf("device %s state = %s after duplicates, want succeeded", id, dv.State)
		}
	}
}
