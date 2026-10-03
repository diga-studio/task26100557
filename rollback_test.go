package canary

import (
	"errors"
	"testing"
)

// TestRollbackWithInflightUpgrades：触线时仍有升级在途。已有成功终端先获回滚；
// 晚到的升级成功也获得回滚；在途失败不再重试；不能提前宣布终态；
// 最终回滚终端集合恰好等于所有升级成功终端的集合。
func TestRollbackWithInflightUpgrades(t *testing.T) {
	c := NewCoordinator()
	canaries := ids("c", 2)
	rest := ids("n", 8)
	mustCreate(t, c, "b3", append(canaries, rest...), canaries)

	// 试点成功，进入放大阶段。
	for _, id := range canaries {
		cmd := mustClaim(t, c, "b3")
		if cmd.DeviceID != id {
			t.Fatalf("cmd.DeviceID = %s, want %s", cmd.DeviceID, id)
		}
		mustReport(t, c, "b3", cmd.ID, true)
	}
	mustPhase(t, c, "b3", PhaseRollout)

	// n1、n2 升级成功；n3、n4 领取后在途。
	succeeded := map[string]bool{canaries[0]: true, canaries[1]: true}
	for _, id := range rest[:2] {
		cmd := mustClaim(t, c, "b3")
		mustReport(t, c, "b3", cmd.ID, true)
		succeeded[id] = true
	}
	inflight := map[string]string{}
	for _, id := range rest[2:4] {
		cmd := mustClaim(t, c, "b3")
		inflight[id] = cmd.ID
	}

	// n5、n6、n7 各 4 次尝试均失败 => 3/10 = 30% > 20%，触线。
	for _, id := range rest[4:7] {
		failDeviceFinally(t, c, "b3", id)
	}
	mustPhase(t, c, "b3", PhaseRollback)

	// 已成功的 4 台终端的回滚指令应立即可以领取（在途回执未到齐也可领）。
	early := map[string]bool{}
	for i := 0; i < 4; i++ {
		cmd := mustClaim(t, c, "b3")
		if cmd.Kind != CommandRollback {
			t.Fatalf("cmd = %+v, want rollback", cmd)
		}
		early[cmd.DeviceID] = true
	}
	for id := range succeeded {
		if !early[id] {
			t.Fatalf("succeeded device %s has no immediate rollback", id)
		}
	}
	// 此刻在途升级未回执、回滚未确认，不能宣布终态。
	mustPhase(t, c, "b3", PhaseRollback)

	// 晚到的升级成功回执也必须产生回滚。
	lateID := rest[2]
	mustReport(t, c, "b3", inflight[lateID], true)
	succeeded[lateID] = true
	// 在途升级失败：不再重试。
	failID := rest[3]
	mustReport(t, c, "b3", inflight[failID], false)

	// 领取剩余指令：只能有 lateID 一条回滚，n8 的升级已被取消。
	cmd := mustClaim(t, c, "b3")
	if cmd.Kind != CommandRollback || cmd.DeviceID != lateID {
		t.Fatalf("cmd = %+v, want rollback for %s", cmd, lateID)
	}
	if _, ok := claimMaybe(t, c, "b3"); ok {
		t.Fatalf("unexpected extra command (cancelled upgrade dispatched?)")
	}

	// 确认全部 5 条回滚（4 条已领取 + lateID 这条）。
	rolledBack := map[string]bool{lateID: true}
	mustReport(t, c, "b3", cmd.ID, true)
	for id := range early {
		dv, err := c.Device("b3", id)
		if err != nil {
			t.Fatalf("Device: %v", err)
		}
		mustReport(t, c, "b3", dv.CurrentID, true)
		rolledBack[id] = true
	}

	v := mustPhase(t, c, "b3", PhaseRolledBack)
	if v.RolledBack != len(succeeded) {
		t.Fatalf("rolledBack = %d, want %d", v.RolledBack, len(succeeded))
	}
	// 回滚终端集合恰好等于所有升级成功终端的集合。
	if len(rolledBack) != len(succeeded) {
		t.Fatalf("rolledBack set = %v, want %v", rolledBack, succeeded)
	}
	for id := range succeeded {
		if !rolledBack[id] {
			t.Fatalf("succeeded device %s not rolled back", id)
		}
	}
	// 升级失败的 n4 与未派发的 n8 不应有回滚。
	for _, id := range []string{failID, rest[7]} {
		dv, err := c.Device("b3", id)
		if err != nil {
			t.Fatalf("Device: %v", err)
		}
		if dv.State == DeviceRollingBack || dv.State == DeviceRolledBack {
			t.Fatalf("device %s should not roll back, state = %s", id, dv.State)
		}
	}
}

// TestDuplicateReceipts：回执乱序、重复，旧回执跨越重试成功后再次到达，
// 均不得改变尝试次数、失败比例、指令数量与终态。
func TestDuplicateReceipts(t *testing.T) {
	c := NewCoordinator()
	canaries := ids("c", 2)
	rest := ids("n", 3)
	mustCreate(t, c, "b4", append(canaries, rest...), canaries)

	// c1 首次失败，重试成功。
	cmd1 := mustClaim(t, c, "b4")
	mustReport(t, c, "b4", cmd1.ID, false)
	cmd2 := mustClaim(t, c, "b4")
	if cmd2.DeviceID != cmd1.DeviceID || cmd2.Attempt != 2 {
		t.Fatalf("cmd = %+v, want retry attempt 2 for %s", cmd2, cmd1.DeviceID)
	}
	mustReport(t, c, "b4", cmd2.ID, true)

	before, err := c.Batch("b4")
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	// 旧回执（首次失败）在重试成功后重复到达：忽略，不报错。
	if err := c.Report("b4", cmd1.ID, false); err != nil {
		t.Fatalf("duplicate receipt should be ignored, got %v", err)
	}
	// 成功回执重复到达：忽略。
	if err := c.Report("b4", cmd2.ID, true); err != nil {
		t.Fatalf("duplicate receipt should be ignored, got %v", err)
	}
	// 未知指令：报错。
	if err := c.Report("b4", "b4/ghost#u1", true); !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("unknown command err = %v, want ErrUnknownCommand", err)
	}
	after, err := c.Batch("b4")
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if before != after {
		t.Fatalf("duplicate receipts changed batch view: %+v -> %+v", before, after)
	}
	dv, err := c.Device("b4", cmd1.DeviceID)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if dv.Attempts != 2 || dv.State != DeviceSucceeded {
		t.Fatalf("device = %+v, want 2 attempts succeeded", dv)
	}

	// 完成批次后，终态前的重复回执仍不改变终态。
	for _, id := range append(canaries[1:], rest...) {
		cmd := mustClaim(t, c, "b4")
		if cmd.DeviceID != id {
			t.Fatalf("cmd.DeviceID = %s, want %s", cmd.DeviceID, id)
		}
		mustReport(t, c, "b4", cmd.ID, true)
	}
	mustPhase(t, c, "b4", PhaseReleased)
	if err := c.Report("b4", cmd1.ID, false); err != nil {
		t.Fatalf("duplicate receipt after terminal: %v", err)
	}
	mustPhase(t, c, "b4", PhaseReleased)
}

// TestExactThresholdDoesNotTrigger：放大阶段恰好 20% 最终失败不触发回滚。
func TestExactThresholdDoesNotTrigger(t *testing.T) {
	c := NewCoordinator()
	canaries := ids("c", 2)
	rest := ids("n", 8)
	mustCreate(t, c, "b5", append(canaries, rest...), canaries)

	for range canaries {
		cmd := mustClaim(t, c, "b5")
		mustReport(t, c, "b5", cmd.ID, true)
	}
	mustPhase(t, c, "b5", PhaseRollout)
	// 2 台最终失败（2/10 = 20%，不触发），其余成功。
	for _, id := range rest[:2] {
		failDeviceFinally(t, c, "b5", id)
		mustPhase(t, c, "b5", PhaseRollout)
	}
	for range rest[2:] {
		cmd := mustClaim(t, c, "b5")
		mustReport(t, c, "b5", cmd.ID, true)
	}
	v := mustPhase(t, c, "b5", PhaseReleased)
	if v.FinalFailures != 2 || v.FailureDivisor != 10 {
		t.Fatalf("failures = %d/%d, want 2/10", v.FinalFailures, v.FailureDivisor)
	}
}
