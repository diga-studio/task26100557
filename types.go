// Package canary 提供终端升级金丝雀发布的进程内协调库。
// 库只负责产生升级/回滚指令并接收执行回执，不执行任何实际脚本。
package canary

// CommandKind 指令类型。
type CommandKind int

const (
	// CommandUpgrade 升级指令（含重试）。
	CommandUpgrade CommandKind = iota
	// CommandRollback 回滚指令。
	CommandRollback
)

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

// Command 一条派发给终端的指令。ID 在批次内稳定唯一。
type Command struct {
	ID       string      // 稳定标识，格式 "<batch>/<terminal>/<kind>#<seq>"
	BatchID  string      // 所属批次
	Terminal string      // 目标终端
	Kind     CommandKind // 指令类型
	Attempt  int         // 升级尝试序号（1..4），回滚指令为 0
}

// Receipt 执行回执。同一指令的重复回执结果保证一致。
type Receipt struct {
	CommandID string // 对应 Command.ID
	Success   bool   // 执行是否成功
}

// BatchState 批次状态。
type BatchState int

const (
	// StatePilot 试点阶段：仅试点终端可领取升级。
	StatePilot BatchState = iota
	// StateScaling 放大阶段：试点全部有最终结论且未触线，向其余终端派发。
	StateScaling
	// StateRollingBack 回滚中：失败比例触线，取消未派发升级，补偿已成功终端。
	StateRollingBack
	// StateRolledBack 已回滚：全部在途升级回执到齐且所有回滚已确认。终态。
	StateRolledBack
	// StateCompleted 发布完成：未触线且全部目标有最终结论。终态。
	StateCompleted
)

func (s BatchState) String() string {
	switch s {
	case StatePilot:
		return "pilot"
	case StateScaling:
		return "scaling"
	case StateRollingBack:
		return "rolling_back"
	case StateRolledBack:
		return "rolled_back"
	case StateCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// TerminalStatus 单个终端的发布状态快照。
type TerminalStatus struct {
	Terminal   string // 终端标识
	Pilot      bool   // 是否试点终端
	Attempts   int    // 已派发的升级尝试次数（含在途）
	InFlight   bool   // 是否有已领取未回执的指令
	Succeeded  bool   // 升级是否已成功
	Failed     bool   // 升级是否最终失败（4 次尝试均失败）
	RolledBack bool   // 回滚是否已确认
}

// BatchStatus 批次状态快照。
type BatchStatus struct {
	ID          string
	State       BatchState
	Total       int // 目标终端总数
	PilotTotal  int // 试点终端数
	PilotFailed int // 试点最终失败数
	TotalFailed int // 全部最终失败数（含试点）
	Succeeded   int // 升级成功终端数
	RolledBack  int // 回滚已确认终端数
	InFlight    int // 已领取未回执的指令数
	Terminals   map[string]TerminalStatus
}
