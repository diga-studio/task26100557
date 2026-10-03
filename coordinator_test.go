package canary

import (
	"errors"
	"fmt"
	"testing"
)

func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i+1)
	}
	return out
}

func concat(a, b []string) []string {
	return append(append([]string{}, a...), b...)
}

// drain 领取当前所有可派发指令。
func drain(t *testing.T, c *Coordinator, batch string) []Command {
	t.Helper()
	var cmds []Command
	for {
		cmd, err := c.Next(batch)
		if errors.Is(err, ErrNoCommand) {
			return cmds
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		cmds = append(cmds, cmd)
	}
}

func mustReport(t *testing.T, c *Coordinator, batch, id string, ok bool) {
	t.Helper()
	if err := c.Report(batch, Receipt{CommandID: id, Success: ok}); err != nil {
		t.Fatalf("Report(%s): %v", id, err)
	}
}

func mustState(t *testing.T, c *Coordinator, batch string) BatchStatus {
	t.Helper()
	s, err := c.Status(batch)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return s
}

// failFinally 让终端把当前在途尝试失败到底（补满 4 次失败）。
func failFinally(t *testing.T, c *Coordinator, batch, terminal string, cmd Command) {
	t.Helper()
	for {
		if cmd.Terminal != terminal || cmd.Kind != CommandUpgrade {
			t.Fatalf("expect upgrade for %s, got %+v", terminal, cmd)
		}
		mustReport(t, c, batch, cmd.ID, false)
		if cmd.Attempt == maxAttempts {
			return
		}
		cmds := drain(t, c, batch)
		if len(cmds) != 1 {
			t.Fatalf("expect 1 retry for %s, got %d", terminal, len(cmds))
		}
		cmd = cmds[0]
	}
}

// 场景 1a：5 台试点中 1 台重试耗尽（恰好 20%）不触线，可放大；
// 放大阶段累计 5/25=20% 也不触线，最终发布完成。
func TestPilotOneOfFiveFailsThenScale(t *testing.T) {
	c := NewCoordinator()
	pilots := names("p", 5)
	rest := names("t", 20)
	targets := concat(pilots, rest)
	if err := c.CreateBatch("b1", targets, pilots); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	cmds := drain(t, c, "b1")
	if len(cmds) != 5 {
		t.Fatalf("expect 5 pilot commands, got %d", len(cmds))
	}
	byTerminal := map[string]Command{}
	for _, cmd := range cmds {
		if cmd.Kind != CommandUpgrade || cmd.Attempt != 1 {
			t.Fatalf("unexpected command %+v", cmd)
		}
		byTerminal[cmd.Terminal] = cmd
	}

	// p1 重试耗尽：前 3 次失败不得触发任何最终失败计数。
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		mustReport(t, c, "b1", byTerminal["p1"].ID, false)
		s := mustState(t, c, "b1")
		if s.State != StatePilot {
			t.Fatalf("attempt %d: state=%v, want pilot", attempt, s.State)
		}
		if attempt < maxAttempts {
			if s.PilotFailed != 0 {
				t.Fatalf("attempt %d: retryable failure counted as final", attempt)
			}
			next := drain(t, c, "b1")
			if len(next) != 1 || next[0].Terminal != "p1" || next[0].Attempt != attempt+1 {
				t.Fatalf("attempt %d: expect single p1 retry, got %+v", attempt, next)
			}
			byTerminal["p1"] = next[0]
		}
	}
	if s := mustState(t, c, "b1"); s.PilotFailed != 1 {
		t.Fatalf("PilotFailed=%d, want 1", s.PilotFailed)
	}

	// 其余试点成功：1/5=20% 不触线，进入放大阶段。
	for _, p := range pilots[1:] {
		mustReport(t, c, "b1", byTerminal[p].ID, true)
	}
	if s := mustState(t, c, "b1"); s.State != StateScaling {
		t.Fatalf("state=%v, want scaling", s.State)
	}

	// 放大阶段：t1..t4 重试耗尽，其余成功。累计 5/25=20% 不触线。
	failing := map[string]bool{"t1": true, "t2": true, "t3": true, "t4": true}
	for {
		cmds := drain(t, c, "b1")
		if len(cmds) == 0 {
			break
		}
		for _, cmd := range cmds {
			if cmd.Kind != CommandUpgrade {
				t.Fatalf("unexpected rollback before threshold: %+v", cmd)
			}
			if cmd.Terminal == "p1" || (cmd.Terminal >= "p2" && cmd.Terminal <= "p5") {
				t.Fatalf("pilot %s re-upgraded", cmd.Terminal)
			}
			mustReport(t, c, "b1", cmd.ID, !failing[cmd.Terminal])
		}
		if s := mustState(t, c, "b1"); s.State == StateRollingBack {
			t.Fatalf("rollback triggered at %d/%d failures", s.TotalFailed, s.Total)
		}
	}

	s := mustState(t, c, "b1")
	if s.State != StateCompleted {
		t.Fatalf("state=%v, want completed", s.State)
	}
	if s.TotalFailed != 5 || s.Succeeded != 20 {
		t.Fatalf("failed=%d succeeded=%d, want 5/20", s.TotalFailed, s.Succeeded)
	}
	// 终态后不再派发。
	if _, err := c.Next("b1"); !errors.Is(err, ErrNoCommand) {
		t.Fatalf("Next after completed: %v", err)
	}
}

// 场景 1b：5 台试点中 2 台重试耗尽（40%）必须回滚。
func TestPilotTwoOfFiveFailTriggersRollback(t *testing.T) {
	c := NewCoordinator()
	pilots := names("p", 5)
	targets := concat(pilots, names("t", 5))
	if err := c.CreateBatch("b2", targets, pilots); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	cmds := drain(t, c, "b2")
	byTerminal := map[string]Command{}
	for _, cmd := range cmds {
		byTerminal[cmd.Terminal] = cmd
	}
	// p3,p4,p5 先成功。
	for _, p := range []string{"p3", "p4", "p5"} {
		mustReport(t, c, "b2", byTerminal[p].ID, true)
	}
	// p1 重试耗尽：1/5=20% 不触线。
	failFinally(t, c, "b2", "p1", byTerminal["p1"])
	if s := mustState(t, c, "b2"); s.State != StatePilot {
		t.Fatalf("state=%v, want pilot (1/5 not over threshold)", s.State)
	}
	// p2 重试耗尽：2/5=40% 触线。
	failFinally(t, c, "b2", "p2", byTerminal["p2"])
	if s := mustState(t, c, "b2"); s.State != StateRollingBack {
		t.Fatalf("state=%v, want rolling_back", s.State)
	}

	// 只派发 p3,p4,p5 的回滚，不再有升级。
	rb := drain(t, c, "b2")
	if len(rb) != 3 {
		t.Fatalf("expect 3 rollbacks, got %+v", rb)
	}
	for _, cmd := range rb {
		if cmd.Kind != CommandRollback {
			t.Fatalf("expect rollback, got %+v", cmd)
		}
	}
	if _, err := c.Next("b2"); !errors.Is(err, ErrNoCommand) {
		t.Fatalf("Next during drained rollback: %v", err)
	}
	for _, cmd := range rb {
		mustReport(t, c, "b2", cmd.ID, true)
	}
	s := mustState(t, c, "b2")
	if s.State != StateRolledBack {
		t.Fatalf("state=%v, want rolled_back", s.State)
	}
	if s.RolledBack != 3 || s.Succeeded != 3 {
		t.Fatalf("rolledBack=%d succeeded=%d, want 3/3", s.RolledBack, s.Succeeded)
	}
}

// 场景 2：触线时仍有升级在途；已有成功终端先获回滚，晚到成功也获回滚；
// 不提前宣布终态；最终回滚集合恰好等于全部升级成功集合。
func TestRollbackWithInFlightUpgrades(t *testing.T) {
	c := NewCoordinator()
	pilots := names("p", 2)
	targets := concat(pilots, names("t", 8))
	if err := c.CreateBatch("b3", targets, pilots); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	for _, cmd := range drain(t, c, "b3") {
		mustReport(t, c, "b3", cmd.ID, true)
	}
	if s := mustState(t, c, "b3"); s.State != StateScaling {
		t.Fatalf("state=%v, want scaling", s.State)
	}

	// 8 台非试点全部领出升级（在途）。
	cmds := drain(t, c, "b3")
	if len(cmds) != 8 {
		t.Fatalf("expect 8 upgrades, got %d", len(cmds))
	}
	byTerminal := map[string]Command{}
	for _, cmd := range cmds {
		byTerminal[cmd.Terminal] = cmd
	}
	// t1,t2,t3 成功。
	for _, name := range []string{"t1", "t2", "t3"} {
		mustReport(t, c, "b3", byTerminal[name].ID, true)
	}
	// t4,t5 重试耗尽：2/10=20% 不触线。
	failFinally(t, c, "b3", "t4", byTerminal["t4"])
	failFinally(t, c, "b3", "t5", byTerminal["t5"])
	if s := mustState(t, c, "b3"); s.State != StateScaling {
		t.Fatalf("state=%v, want scaling (2/10 not over threshold)", s.State)
	}
	// t6 失败 3 次后第 4 次保持在途；t8 首次尝试保持在途。
	for i := 0; i < 3; i++ {
		mustReport(t, c, "b3", byTerminal["t6"].ID, false)
		byTerminal["t6"] = drain(t, c, "b3")[0]
	}
	// t7 重试耗尽：3/10=30% 触线。此时 t6、t8 的升级仍在途。
	failFinally(t, c, "b3", "t7", byTerminal["t7"])
	if s := mustState(t, c, "b3"); s.State != StateRollingBack {
		t.Fatalf("state=%v, want rolling_back", s.State)
	}

	// 已有成功的 p1,p2,t1,t2,t3 立即获得回滚；不得派发任何升级。
	rb := drain(t, c, "b3")
	if len(rb) != 5 {
		t.Fatalf("expect 5 rollbacks, got %+v", rb)
	}
	rbIDs := map[string]bool{}
	for _, cmd := range rb {
		if cmd.Kind != CommandRollback {
			t.Fatalf("expect rollback only, got %+v", cmd)
		}
		rbIDs[cmd.ID] = true
	}

	// 晚到的升级成功回执（t6）也必须产生回滚。
	mustReport(t, c, "b3", byTerminal["t6"].ID, true)
	extra := drain(t, c, "b3")
	if len(extra) != 1 || extra[0].Kind != CommandRollback || extra[0].Terminal != "t6" {
		t.Fatalf("expect late rollback for t6, got %+v", extra)
	}
	rbIDs[extra[0].ID] = true

	// 回滚期间的升级失败回执（t8）不再重试。
	mustReport(t, c, "b3", byTerminal["t8"].ID, false)
	if cmds := drain(t, c, "b3"); len(cmds) != 0 {
		t.Fatalf("expect no retry during rollback, got %+v", cmds)
	}

	// 回滚未确认前不得宣布终态。
	if s := mustState(t, c, "b3"); s.State != StateRollingBack {
		t.Fatalf("premature terminal state: %v", s.State)
	}
	// 确认全部回滚。
	for id := range rbIDs {
		mustReport(t, c, "b3", id, true)
	}
	s := mustState(t, c, "b3")
	if s.State != StateRolledBack {
		t.Fatalf("state=%v, want rolled_back", s.State)
	}
	// 回滚集合恰好等于全部升级成功集合。
	want := map[string]bool{"p1": true, "p2": true, "t1": true, "t2": true, "t3": true, "t6": true}
	if s.Succeeded != len(want) || s.RolledBack != len(want) {
		t.Fatalf("succeeded=%d rolledBack=%d, want %d", s.Succeeded, s.RolledBack, len(want))
	}
	for name, ts := range s.Terminals {
		if ts.RolledBack != want[name] {
			t.Fatalf("terminal %s rolledBack=%v, want %v", name, ts.RolledBack, want[name])
		}
	}
}
