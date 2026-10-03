package canary

import "fmt"

// maxAttempts 每台终端升级最大尝试次数（首次失败后再试 3 次）。
const maxAttempts = 4

// cmdMeta 已派发指令的元数据，用于按 ID 处理回执。
type cmdMeta struct {
	terminal string
	kind     CommandKind
}

// terminal 单个终端的内部状态。
type terminal struct {
	pilot          bool
	attempts       int  // 已派发的升级尝试次数
	inFlight       bool // 是否有已领取未回执的指令
	succeeded      bool // 升级已成功
	failed         bool // 升级最终失败（4 次均失败）
	rollbackIssued bool // 回滚指令已派发
	rollbackDone   bool // 回滚已确认
}

// batch 单个发布批次的内部状态。仅由 Coordinator 在持锁状态下访问。
type batch struct {
	id       string
	state    BatchState
	order    []string // 终端固定顺序，保证派发确定性
	terms    map[string]*terminal
	seq      map[string]int     // 每终端指令序号，生成稳定唯一 ID
	commands map[string]cmdMeta // 已派发指令 ID -> 元数据
	done     map[string]bool    // 已处理回执的指令 ID，保证幂等

	pilotTotal  int
	pilotFinal  int // 已有最终结论（成功或最终失败）的试点数
	pilotFailed int // 试点最终失败数
	totalFailed int // 全部最终失败数（含试点）
	succeeded   int // 升级成功终端数
	rolledBack  int // 回滚已确认终端数
	inFlight    int // 已领取未回执的指令数

	rollbackQueue []string // 待派发回滚的终端（按进入顺序）
}

func newBatch(id string, targets, pilots []string) *batch {
	b := &batch{
		id:         id,
		state:      StatePilot,
		terms:      make(map[string]*terminal, len(targets)),
		seq:        make(map[string]int, len(targets)),
		commands:   make(map[string]cmdMeta),
		done:       make(map[string]bool),
		pilotTotal: len(pilots),
	}
	for _, t := range targets {
		b.terms[t] = &terminal{}
		b.order = append(b.order, t)
	}
	for _, p := range pilots {
		b.terms[p].pilot = true
	}
	return b
}

// issue 为终端生成一条指令并登记为在途。
func (b *batch) issue(name string, kind CommandKind) Command {
	b.seq[name]++
	attempt := 0
	if kind == CommandUpgrade {
		b.terms[name].attempts++
		attempt = b.terms[name].attempts
	} else {
		b.terms[name].rollbackIssued = true
	}
	cmd := Command{
		ID:       fmt.Sprintf("%s/%s/%s#%d", b.id, name, kind, b.seq[name]),
		BatchID:  b.id,
		Terminal: name,
		Kind:     kind,
		Attempt:  attempt,
	}
	b.commands[cmd.ID] = cmdMeta{terminal: name, kind: kind}
	b.terms[name].inFlight = true
	b.inFlight++
	return cmd
}

// next 领取下一条指令。回滚优先；升级按固定终端顺序派发，不等待回执。
func (b *batch) next() (Command, error) {
	switch b.state {
	case StateRolledBack, StateCompleted:
		return Command{}, ErrNoCommand
	case StateRollingBack:
		if len(b.rollbackQueue) == 0 {
			return Command{}, ErrNoCommand
		}
		name := b.rollbackQueue[0]
		b.rollbackQueue = b.rollbackQueue[1:]
		return b.issue(name, CommandRollback), nil
	}

	// StatePilot / StateScaling：仅派发升级。
	for _, name := range b.order {
		t := b.terms[name]
		if b.state == StatePilot && !t.pilot {
			continue
		}
		if b.state == StateScaling && t.pilot {
			continue // 试点终端不再重复升级
		}
		if t.succeeded || t.failed || t.inFlight || t.attempts >= maxAttempts {
			continue
		}
		return b.issue(name, CommandUpgrade), nil
	}
	return Command{}, ErrNoCommand
}

// report 处理回执。重复回执幂等；处理后推进状态机。
func (b *batch) report(r Receipt) error {
	meta, ok := b.commands[r.CommandID]
	if !ok {
		return ErrUnknownCommand
	}
	if b.done[r.CommandID] {
		return nil // 重复回执：结果保证一致，直接忽略
	}
	b.done[r.CommandID] = true

	t := b.terms[meta.terminal]
	t.inFlight = false
	b.inFlight--

	if meta.kind == CommandRollback {
		// 回滚执行一定成功；失败回执按协议不出现，视为确认以保证终态可达。
		t.rollbackDone = true
		b.rolledBack++
	} else if r.Success {
		t.succeeded = true
		b.succeeded++
		if t.pilot {
			b.pilotFinal++
		}
		if b.state == StateRollingBack {
			// 回滚期间才收到的升级成功回执：必须补偿回滚。
			b.rollbackQueue = append(b.rollbackQueue, meta.terminal)
		}
	} else {
		// 升级失败：回滚期间不再重试，也不计最终失败。
		if b.state != StateRollingBack && t.attempts >= maxAttempts {
			t.failed = true
			b.totalFailed++
			if t.pilot {
				b.pilotFailed++
				b.pilotFinal++
			}
		}
	}

	b.advance()
	return nil
}

// advance 在回执处理后推进状态机：触线判断、阶段切换、终态判定。
func (b *batch) advance() {
	// 1. 失败比例触线：严格超过 20% 立即回滚。
	if b.state == StatePilot || b.state == StateScaling {
		over := false
		if b.state == StatePilot {
			over = b.pilotFailed*5 > b.pilotTotal
		} else {
			over = b.totalFailed*5 > len(b.order)
		}
		if over {
			b.state = StateRollingBack
			for _, name := range b.order {
				t := b.terms[name]
				if t.succeeded && !t.rollbackIssued {
					b.rollbackQueue = append(b.rollbackQueue, name)
				}
			}
		}
	}

	// 2. 试点全部有最终结论且未触线：进入放大阶段。
	if b.state == StatePilot && b.pilotFinal == b.pilotTotal {
		b.state = StateScaling
	}

	// 3. 终态判定。
	switch b.state {
	case StateRollingBack:
		// 全部已领取升级已回执（inFlight==0 同时保证在途回滚已确认），
		// 且所有升级成功终端的回滚都已确认。
		if b.inFlight == 0 && len(b.rollbackQueue) == 0 && b.rolledBack == b.succeeded {
			b.state = StateRolledBack
		}
	case StatePilot, StateScaling:
		allFinal := true
		for _, t := range b.terms {
			if !t.succeeded && !t.failed {
				allFinal = false
				break
			}
		}
		if allFinal {
			b.state = StateCompleted
		}
	}
}

// status 生成批次及各终端状态快照。
func (b *batch) status() BatchStatus {
	s := BatchStatus{
		ID:          b.id,
		State:       b.state,
		Total:       len(b.order),
		PilotTotal:  b.pilotTotal,
		PilotFailed: b.pilotFailed,
		TotalFailed: b.totalFailed,
		Succeeded:   b.succeeded,
		RolledBack:  b.rolledBack,
		InFlight:    b.inFlight,
		Terminals:   make(map[string]TerminalStatus, len(b.terms)),
	}
	for name, t := range b.terms {
		s.Terminals[name] = TerminalStatus{
			Terminal:   name,
			Pilot:      t.pilot,
			Attempts:   t.attempts,
			InFlight:   t.inFlight,
			Succeeded:  t.succeeded,
			Failed:     t.failed,
			RolledBack: t.rollbackDone,
		}
	}
	return s
}
