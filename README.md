# canary — 终端升级金丝雀发布协调库

纯进程内存的 Go 库，由上层直接调用。只负责产生升级/回滚指令并接收执行
回执，**不执行任何实际脚本**；不处理超时、失联与崩溃恢复。

## 能力

| 方法 | 说明 |
| --- | --- |
| `NewCoordinator()` | 创建协调器，可管理多个独立批次 |
| `CreateBatch(batchID, targets, canaries)` | 创建发布批次；`canaries` 为 `targets` 的非空子集，创建后不变 |
| `Claim(batchID) (Command, bool, error)` | 领取下一条指令；无可派发指令时立即返回 `ok=false`，不等待 |
| `Report(batchID, commandID, success)` | 提交执行回执；重复回执被忽略，未知指令返回 `ErrUnknownCommand` |
| `Batch(batchID) (BatchView, error)` | 查询批次状态快照（阶段、失败比例分子/分母、各类计数） |
| `Device(batchID, deviceID) (DeviceView, error)` | 查询单台终端状态快照（状态、尝试次数、在途指令） |

## 使用示例

```go
c := canary.NewCoordinator()
_ = c.CreateBatch("fw-2.1", []string{"t1", "t2", "t3", "t4", "t5"}, []string{"t1"})

// 执行器循环领取指令（通常多个调用方并发执行）。
cmd, ok, _ := c.Claim("fw-2.1")
if ok {
    success := runOnDevice(cmd.DeviceID, cmd.Kind) // 由上层真正执行
    _ = c.Report("fw-2.1", cmd.ID, success)
}

view, _ := c.Batch("fw-2.1")
fmt.Println(view.Phase, view.FinalFailures, "/", view.FailureDivisor)
```

## 语义要点

- **指令**：`Command` 含稳定 `ID`（`批次/终端#u次数` 或 `#r1`）、类型
  （`upgrade`/`rollback`）与尝试序号。同一终端任一时刻最多一条在途指令。
- **重试**：首次失败后最多再试 3 次（共 4 次，重试优先派发），每次重试
  都是新指令；4 次均失败才算最终失败，可重试的失败不计入失败比例。
- **阈值**：试点阶段分母为试点总数，放大阶段分母为目标总数（含试点）；
  最终失败比例**严格超过 20%** 立即回滚，恰好 20% 不触发，逐条回执即时判断。
- **回滚**：取消全部未领取的升级/重试；已为每台升级成功终端立即产生可领取
  的回滚指令；回滚期间到达的升级成功回执同样产生回滚，失败回执不再重试。
- **终态**：未触线且全部目标有最终结论 → `released`；全部在途指令已回执
  且所有成功终端的回滚已确认 → `rolled_back`。终态不可回退。
- **并发**：所有操作由单把互斥锁串行化，效果等价于某串行顺序；状态查询
  与状态迁移在同一把锁内完成，不会读到"已触线仍可派发升级"的中间状态。

## 测试

```sh
go test ./...        # 全部单元测试
go test -race ./...  # 含 5000 终端 / 32 并发调用方的竞态测试
```

覆盖：试点 1/5 重试耗尽仍放大、2/5 重试耗尽必回滚、可重试失败不误触发、
恰好 20% 不触发、触线时在途升级的补偿回滚、终态时机、重复/乱序/跨重试的
旧回执去重，以及并发下的不重叠派发与尝试数上限。
