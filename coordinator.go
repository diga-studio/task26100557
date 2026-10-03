package canary

import (
	"errors"
	"sync"
)

// ErrBatchNotFound 批次不存在。
var ErrBatchNotFound = errors.New("canary: batch not found")

// ErrNoCommand 当前没有可派发的指令。
var ErrNoCommand = errors.New("canary: no command available")

// ErrUnknownCommand 回执对应的指令不存在。
var ErrUnknownCommand = errors.New("canary: unknown command id")

// ErrBatchExists 批次 ID 已存在。
var ErrBatchExists = errors.New("canary: batch already exists")

// ErrInvalidTargets 目标集合非法（为空，或试点不是目标的非空子集）。
var ErrInvalidTargets = errors.New("canary: invalid targets/pilots")

// Coordinator 管理多个独立发布批次，并发安全。
// 所有批次操作在同一把互斥锁下完成，效果等价于按调用先后关系的串行执行。
type Coordinator struct {
	mu      sync.Mutex
	batches map[string]*batch
}

// NewCoordinator 创建协调器。
func NewCoordinator() *Coordinator {
	return &Coordinator{batches: make(map[string]*batch)}
}

// CreateBatch 创建发布批次。targets 为目标终端集合，pilots 为其中非空试点子集。
func (c *Coordinator) CreateBatch(id string, targets, pilots []string) error {
	if err := validateTargets(targets, pilots); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.batches[id]; ok {
		return ErrBatchExists
	}
	c.batches[id] = newBatch(id, targets, pilots)
	return nil
}

// validateTargets 校验目标集合与试点子集。
func validateTargets(targets, pilots []string) error {
	if len(targets) == 0 || len(pilots) == 0 {
		return ErrInvalidTargets
	}
	seen := make(map[string]bool, len(targets))
	for _, t := range targets {
		if t == "" || seen[t] {
			return ErrInvalidTargets
		}
		seen[t] = true
	}
	for _, p := range pilots {
		if !seen[p] {
			return ErrInvalidTargets
		}
	}
	return nil
}

// Next 领取下一条指令。没有可派发指令时返回 ErrNoCommand，不等待回执。
func (c *Coordinator) Next(batchID string) (Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.batches[batchID]
	if !ok {
		return Command{}, ErrBatchNotFound
	}
	return b.next()
}

// Report 提交执行回执。重复回执幂等，不改变任何计数与终态。
func (c *Coordinator) Report(batchID string, r Receipt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.batches[batchID]
	if !ok {
		return ErrBatchNotFound
	}
	return b.report(r)
}

// Status 查询批次及各终端状态快照。
func (c *Coordinator) Status(batchID string) (BatchStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.batches[batchID]
	if !ok {
		return BatchStatus{}, ErrBatchNotFound
	}
	return b.status(), nil
}
