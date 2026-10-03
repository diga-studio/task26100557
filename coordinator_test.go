package canary

import "testing"

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + string(rune('a'+i/26)) + string(rune('a'+i%26)) + "-" + prefix
	}
	return out
}

func mustCreate(t *testing.T, c *Coordinator, batch string, targets, canaries []string) {
	t.Helper()
	if err := c.CreateBatch(batch, targets, canaries); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
}

func mustClaim(t *testing.T, c *Coordinator, batch string) Command {
	t.Helper()
	cmd, ok, err := c.Claim(batch)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !ok {
		t.Fatalf("Claim: expected a command, got none")
	}
	return cmd
}

func claimMaybe(t *testing.T, c *Coordinator, batch string) (Command, bool) {
	t.Helper()
	cmd, ok, err := c.Claim(batch)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return cmd, ok
}

func mustReport(t *testing.T, c *Coordinator, batch, cmdID string, success bool) {
	t.Helper()
	if err := c.Report(batch, cmdID, success); err != nil {
		t.Fatalf("Report(%s): %v", cmdID, err)
	}
}

func mustPhase(t *testing.T, c *Coordinator, batch string, want BatchPhase) BatchView {
	t.Helper()
	v, err := c.Batch(batch)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if v.Phase != want {
		t.Fatalf("phase = %s, want %s", v.Phase, want)
	}
	return v
}

// failDeviceFinally 让一台终端 4 次尝试全部失败（每次重试都是新指令）。
func failDeviceFinally(t *testing.T, c *Coordinator, batch, deviceID string) {
	t.Helper()
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		cmd := mustClaim(t, c, batch)
		if cmd.DeviceID != deviceID || cmd.Kind != CommandUpgrade || cmd.Attempt != attempt {
			t.Fatalf("cmd = %+v, want upgrade attempt %d for %s", cmd, attempt, deviceID)
		}
		mustReport(t, c, batch, cmd.ID, false)
	}
}

// TestCanaryOneFinalFailureStillPromotes：5 台试点中 1 台重试耗尽（恰好 20%），
// 不触发回滚，试点全部有结论后进入放大阶段。
func TestCanaryOneFinalFailureStillPromotes(t *testing.T) {
	c := NewCoordinator()
	targets := append(ids("c", 5), ids("n", 5)...)
	mustCreate(t, c, "b1", targets, ids("c", 5))

	// 试点阶段只能领取试点终端的升级。
	failDeviceFinally(t, c, "b1", ids("c", 5)[0])
	v := mustPhase(t, c, "b1", PhaseCanary) // 1/5 = 20%，不触发
	if v.FinalFailures != 1 || v.FailureDivisor != 5 {
		t.Fatalf("failures = %d/%d, want 1/5", v.FinalFailures, v.FailureDivisor)
	}
	for _, id := range ids("c", 5)[1:] {
		cmd := mustClaim(t, c, "b1")
		if cmd.DeviceID != id {
			t.Fatalf("cmd.DeviceID = %s, want %s", cmd.DeviceID, id)
		}
		mustReport(t, c, "b1", cmd.ID, true)
	}

	v = mustPhase(t, c, "b1", PhaseRollout)
	if v.FailureDivisor != 10 {
		t.Fatalf("rollout divisor = %d, want 10", v.FailureDivisor)
	}
	// 放大阶段只派发非试点终端；试点不再重复升级。
	for range ids("n", 5) {
		cmd := mustClaim(t, c, "b1")
		if cmd.Kind != CommandUpgrade || cmd.Attempt != 1 {
			t.Fatalf("cmd = %+v, want first upgrade", cmd)
		}
		for _, cid := range ids("c", 5) {
			if cmd.DeviceID == cid {
				t.Fatalf("canary %s got a repeated upgrade", cid)
			}
		}
		mustReport(t, c, "b1", cmd.ID, true)
	}
	mustPhase(t, c, "b1", PhaseReleased)
}

// TestCanaryTwoFinalFailuresTriggerRollback：5 台试点中 2 台重试耗尽（40%），
// 必须进入回滚；期间的可重试失败不能误触发。
func TestCanaryTwoFinalFailuresTriggerRollback(t *testing.T) {
	c := NewCoordinator()
	targets := append(ids("c", 5), ids("n", 5)...)
	mustCreate(t, c, "b2", targets, ids("c", 5))
	canaries := ids("c", 5)

	// 逐台驱动：前 3 次为可重试失败，不计入失败比例，不得触发回滚。
	failAttempt := func(deviceID string, attempt int) {
		t.Helper()
		cmd := mustClaim(t, c, "b2")
		if cmd.DeviceID != deviceID || cmd.Attempt != attempt || cmd.Kind != CommandUpgrade {
			t.Fatalf("cmd = %+v, want upgrade attempt %d for %s", cmd, attempt, deviceID)
		}
		mustReport(t, c, "b2", cmd.ID, false)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		failAttempt(canaries[0], attempt)
		mustPhase(t, c, "b2", PhaseCanary)
	}
	// 第一台第 4 次失败 => 最终失败（1/5 = 20%，仍不触发）。
	failAttempt(canaries[0], 4)
	mustPhase(t, c, "b2", PhaseCanary)
	// 第二台前 3 次可重试失败同样不得触发。
	for attempt := 1; attempt <= 3; attempt++ {
		failAttempt(canaries[1], attempt)
		mustPhase(t, c, "b2", PhaseCanary)
	}
	// 第二台第 4 次失败 => 2/5 = 40%，立即回滚。
	cmdCab4 := mustClaim(t, c, "b2")
	if cmdCab4.DeviceID != canaries[1] || cmdCab4.Attempt != 4 {
		t.Fatalf("cmd = %+v, want attempt 4 for %s", cmdCab4, canaries[1])
	}
	// 触线前让其余 3 台试点的升级已领取在途。
	inflight := map[string]string{}
	for _, id := range canaries[2:] {
		cmd := mustClaim(t, c, "b2")
		if cmd.DeviceID != id || cmd.Kind != CommandUpgrade {
			t.Fatalf("cmd = %+v, want upgrade for %s", cmd, id)
		}
		inflight[id] = cmd.ID
	}
	mustReport(t, c, "b2", cmdCab4.ID, false)
	mustPhase(t, c, "b2", PhaseRollback)

	// 在途升级在回滚期间回执成功：必须产生回滚指令，且不能提前宣布终态。
	for _, id := range canaries[2:] {
		mustReport(t, c, "b2", inflight[id], true)
		mustPhase(t, c, "b2", PhaseRollback)
	}
	// 此后只能领取回滚指令，共 3 台成功终端。
	rolled := map[string]bool{}
	for {
		cmd, ok := claimMaybe(t, c, "b2")
		if !ok {
			break
		}
		if cmd.Kind != CommandRollback {
			t.Fatalf("cmd = %+v, want rollback only", cmd)
		}
		rolled[cmd.DeviceID] = true
		mustReport(t, c, "b2", cmd.ID, true)
	}
	if len(rolled) != 3 {
		t.Fatalf("rolled back %d devices, want 3", len(rolled))
	}
	mustPhase(t, c, "b2", PhaseRolledBack)
}
