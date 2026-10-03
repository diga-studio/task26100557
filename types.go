// Package canary 提供终端升级的金丝雀（灰度）发布协调库。
//
// 本库只负责产生升级/回滚指令并接收执行回执，不执行任何实际脚本。
// 所有状态保存在进程内存中，由上层直接调用。
package canary

// CommandKind 指令类型。
type CommandKind int

const (
	// CommandUpgrade 升级指令（含首次升级与失败重试）。
	CommandUpgrade CommandKind = iota
	// CommandRollback 回滚指令。
	CommandRollback
)

// String 返回指令类型的可读名称。
func (k CommandKind) String() string {
	switch k {
	case CommandUpgrade:
		return "upgrade"
	case CommandRollback:
		return "rollback"
	default:
		return "unknown"
	}
}

// Command 是一条待派发给终端执行的指令。
type Command struct {
	ID       string      // 稳定标识，同一指令不变
	BatchID  string      // 所属批次
	DeviceID string      // 目标终端
	Kind     CommandKind // 指令类型
	Attempt  int         // 升级尝试序号（从 1 开始）；回滚指令为 0
}

// BatchPhase 批次阶段。
type BatchPhase int

const (
	// PhaseCanary 试点阶段：仅试点终端可领取升级。
	PhaseCanary BatchPhase = iota
	// PhaseRollout 放大阶段：全部试点有最终结论后，向其余终端派发升级。
	PhaseRollout
	// PhaseRollback 回滚阶段：失败比例触线，取消未派发升级并补偿已成功终端。
	PhaseRollback
	// PhaseReleased 发布完成：未触线且全部目标有最终结论（终态）。
	PhaseReleased
	// PhaseRolledBack 已回滚：全部在途升级已回执且所有回滚已确认（终态）。
	PhaseRolledBack
)

// String 返回批次阶段的可读名称。
func (p BatchPhase) String() string {
	switch p {
	case PhaseCanary:
		return "canary"
	case PhaseRollout:
		return "rollout"
	case PhaseRollback:
		return "rollback"
	case PhaseReleased:
		return "released"
	case PhaseRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// DeviceState 终端在批次内的状态。
type DeviceState int

const (
	// DevicePending 尚未领取过升级指令。
	DevicePending DeviceState = iota
	// DeviceUpgrading 有升级指令已领取、等待回执。
	DeviceUpgrading
	// DeviceSucceeded 升级最终成功。
	DeviceSucceeded
	// DeviceFailed 升级最终失败（4 次尝试均失败）。
	DeviceFailed
	// DeviceRollingBack 已产生回滚指令（可能已领取、等待回执）。
	DeviceRollingBack
	// DeviceRolledBack 回滚已确认。
	DeviceRolledBack
)

// String 返回终端状态的可读名称。
func (s DeviceState) String() string {
	switch s {
	case DevicePending:
		return "pending"
	case DeviceUpgrading:
		return "upgrading"
	case DeviceSucceeded:
		return "succeeded"
	case DeviceFailed:
		return "failed"
	case DeviceRollingBack:
		return "rolling_back"
	case DeviceRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// BatchView 批次状态快照，用于状态查询。
type BatchView struct {
	BatchID        string
	Phase          BatchPhase
	TotalTargets   int // 目标终端总数
	TotalCanaries  int // 试点终端总数
	FinalFailures  int // 最终失败数（当前阶段分子）
	FailureDivisor int // 当前阶段失败比例分母
	Succeeded      int // 升级最终成功数
	RolledBack     int // 已确认回滚数
	Outstanding    int // 已领取未回执的指令数
	Pending        int // 可领取但未领取的指令数
}

// DeviceView 终端状态快照，用于状态查询。
type DeviceView struct {
	DeviceID  string
	Canary    bool
	State     DeviceState
	Attempts  int    // 已派发的升级尝试次数
	CurrentID string // 当前已领取未回执的指令 ID，无则为空
}
