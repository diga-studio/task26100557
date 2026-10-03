package canary

import "fmt"

// device 批次内单台终端的内部状态。
type device struct {
	id         string
	canary     bool
	attempts   int      // 已领取的升级尝试次数
	succeeded  bool     // 升级最终成功
	failed     bool     // 升级最终失败（4 次尝试均失败）
	pendingID  string   // 已领取未回执的指令 ID，空表示无在途指令
	rollback   *Command // 已产生的回滚指令，nil 表示未产生
	rolledBack bool     // 回滚已确认
}

// final 报告终端是否已有升级的最终结论（成功或最终失败）。
func (d *device) final() bool { return d.succeeded || d.failed }

// batch 单个发布批次的内部状态。
type batch struct {
	id       string
	phase    BatchPhase
	devices  map[string]*device
	order    []string // 终端 ID 的稳定顺序（创建时给定）
	canaries map[string]bool

	ready    []*Command          // 可领取的指令队列（FIFO）
	claimed  map[string]*Command // 已领取未回执的指令，按指令 ID 索引
	receipts map[string]bool     // 已回执的指令 ID（用于去重）

	canaryFinalFailures int // 试点最终失败数
	totalFinalFailures  int // 全部目标最终失败数
	succeeded           int // 升级最终成功数
	rolledBack          int // 已确认回滚数
}

// newBatch 创建批次：初始为试点阶段，全部试点终端的首次升级进入可领取队列。
func newBatch(id string, targets, canaries []string) *batch {
	b := &batch{
		id:       id,
		phase:    PhaseCanary,
		devices:  make(map[string]*device, len(targets)),
		canaries: make(map[string]bool, len(canaries)),
		claimed:  make(map[string]*Command),
		receipts: make(map[string]bool),
	}
	for _, cid := range canaries {
		b.canaries[cid] = true
	}
	for _, tid := range targets {
		b.devices[tid] = &device{id: tid, canary: b.canaries[tid]}
		b.order = append(b.order, tid)
	}
	for _, cid := range canaries {
		b.enqueueUpgrade(b.devices[cid])
	}
	return b
}

// upgradeID 生成升级指令的稳定标识，attempt 从 1 开始。
func (b *batch) upgradeID(deviceID string, attempt int) string {
	return fmt.Sprintf("%s/%s#u%d", b.id, deviceID, attempt)
}

// rollbackID 生成回滚指令的稳定标识。
func (b *batch) rollbackID(deviceID string) string {
	return fmt.Sprintf("%s/%s#r1", b.id, deviceID)
}

// enqueueUpgrade 为终端生成下一次升级尝试指令并放入可领取队列。
func (b *batch) enqueueUpgrade(d *device) {
	b.ready = append(b.ready, b.newUpgrade(d))
}

// enqueueRetry 为失败重试生成新指令，并插到队首优先派发。
func (b *batch) enqueueRetry(d *device) {
	b.ready = append([]*Command{b.newUpgrade(d)}, b.ready...)
}

// newUpgrade 构造终端的下一次升级尝试指令。
func (b *batch) newUpgrade(d *device) *Command {
	attempt := d.attempts + 1
	return &Command{
		ID:       b.upgradeID(d.id, attempt),
		BatchID:  b.id,
		DeviceID: d.id,
		Kind:     CommandUpgrade,
		Attempt:  attempt,
	}
}

// enqueueRollback 为已升级成功的终端生成回滚指令并放入可领取队列。
func (b *batch) enqueueRollback(d *device) {
	cmd := &Command{
		ID:       b.rollbackID(d.id),
		BatchID:  b.id,
		DeviceID: d.id,
		Kind:     CommandRollback,
	}
	d.rollback = cmd
	b.ready = append(b.ready, cmd)
}

// claim 领取下一条可派发指令；没有时立即返回 ok=false。
func (b *batch) claim() (Command, bool) {
	if len(b.ready) == 0 {
		return Command{}, false
	}
	cmd := b.ready[0]
	b.ready = b.ready[1:]
	b.claimed[cmd.ID] = cmd
	d := b.devices[cmd.DeviceID]
	d.pendingID = cmd.ID
	if cmd.Kind == CommandUpgrade {
		d.attempts++
	}
	return *cmd, true
}

// report 处理执行回执。重复回执返回 ok=false 且不改变任何状态。
func (b *batch) report(commandID string, success bool) (ok bool, err error) {
	if b.receipts[commandID] {
		return false, nil // 重复回执，忽略
	}
	cmd, claimed := b.claimed[commandID]
	if !claimed {
		return false, ErrUnknownCommand
	}
	b.receipts[commandID] = true
	delete(b.claimed, commandID)
	d := b.devices[cmd.DeviceID]
	d.pendingID = ""

	if cmd.Kind == CommandRollback {
		d.rolledBack = true
		b.rolledBack++
		b.maybeFinish()
		return true, nil
	}

	if success {
		d.succeeded = true
		b.succeeded++
		if b.phase == PhaseRollback {
			// 回滚期间才到达的升级成功回执，必须补偿回滚。
			b.enqueueRollback(d)
		}
	} else if b.phase == PhaseRollback {
		// 回滚期间收到的升级失败回执不再重试。
	} else if d.attempts < MaxAttempts {
		b.enqueueRetry(d) // 可重试的失败：生成新指令
	} else {
		d.failed = true
		b.totalFinalFailures++
		if d.canary {
			b.canaryFinalFailures++
		}
		if b.failureRatioExceeded() {
			b.triggerRollback()
			return true, nil
		}
	}
	b.maybePromote()
	b.maybeFinish()
	return true, nil
}

// failureRatioExceeded 判断当前阶段失败比例是否严格超过 20%。
func (b *batch) failureRatioExceeded() bool {
	if b.phase == PhaseCanary {
		return b.canaryFinalFailures*5 > len(b.canaries)
	}
	return b.totalFinalFailures*5 > len(b.order)
}

// triggerRollback 进入回滚：取消全部未领取的升级/重试，
// 并为每台已升级成功的终端立即产生回滚指令。
func (b *batch) triggerRollback() {
	b.phase = PhaseRollback
	b.ready = b.ready[:0]
	for _, id := range b.order {
		if d := b.devices[id]; d.succeeded {
			b.enqueueRollback(d)
		}
	}
	b.maybeFinish()
}

// maybePromote 在试点全部有最终结论且未触发回滚时，进入放大阶段。
func (b *batch) maybePromote() {
	if b.phase != PhaseCanary {
		return
	}
	for cid := range b.canaries {
		if !b.devices[cid].final() {
			return
		}
	}
	b.phase = PhaseRollout
	for _, id := range b.order {
		if d := b.devices[id]; !d.canary {
			b.enqueueUpgrade(d)
		}
	}
}

// maybeFinish 检查是否到达终态；终态不可回退。
func (b *batch) maybeFinish() {
	switch b.phase {
	case PhaseRollback:
		// 全部已领取指令都已回执，且所有升级成功终端的回滚都已确认。
		if len(b.claimed) == 0 && b.rolledBack == b.succeeded {
			b.phase = PhaseRolledBack
		}
	case PhaseCanary, PhaseRollout:
		// 未触线且全部目标都有最终结论。
		for _, id := range b.order {
			if !b.devices[id].final() {
				return
			}
		}
		b.phase = PhaseReleased
	}
}

// view 生成批次状态快照。
func (b *batch) view() BatchView {
	v := BatchView{
		BatchID:       b.id,
		Phase:         b.phase,
		TotalTargets:  len(b.order),
		TotalCanaries: len(b.canaries),
		Succeeded:     b.succeeded,
		RolledBack:    b.rolledBack,
		Outstanding:   len(b.claimed),
		Pending:       len(b.ready),
	}
	if b.phase == PhaseCanary {
		v.FinalFailures = b.canaryFinalFailures
		v.FailureDivisor = v.TotalCanaries
	} else {
		v.FinalFailures = b.totalFinalFailures
		v.FailureDivisor = v.TotalTargets
	}
	return v
}

// deviceView 生成单台终端的状态快照。
func (b *batch) deviceView(deviceID string) (DeviceView, bool) {
	d, ok := b.devices[deviceID]
	if !ok {
		return DeviceView{}, false
	}
	v := DeviceView{
		DeviceID:  d.id,
		Canary:    d.canary,
		Attempts:  d.attempts,
		CurrentID: d.pendingID,
	}
	switch {
	case d.rolledBack:
		v.State = DeviceRolledBack
	case d.rollback != nil:
		v.State = DeviceRollingBack
	case d.failed:
		v.State = DeviceFailed
	case d.succeeded:
		v.State = DeviceSucceeded
	case d.pendingID != "":
		v.State = DeviceUpgrading
	default:
		v.State = DevicePending
	}
	return v, true
}
