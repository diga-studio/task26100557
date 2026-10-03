package canary

import (
	"errors"
	"fmt"
	"sync"
)

// MaxAttempts 每台终端升级的最大尝试次数（首次 + 3 次重试）。
const MaxAttempts = 4

// ErrBatchExists 批次 ID 已存在。
var ErrBatchExists = errors.New("canary: batch already exists")

// ErrBatchNotFound 批次不存在。
var ErrBatchNotFound = errors.New("canary: batch not found")

// ErrUnknownCommand 回执对应的指令不存在或已被处理。
var ErrUnknownCommand = errors.New("canary: unknown command")

// ErrDeviceNotFound 终端不属于该批次。
var ErrDeviceNotFound = errors.New("canary: device not found")

// Coordinator 协调多个发布批次，所有状态保存在内存中，并发安全。
type Coordinator struct {
	mu      sync.Mutex
	batches map[string]*batch
}

// NewCoordinator 创建一个空的协调器。
func NewCoordinator() *Coordinator {
	return &Coordinator{batches: make(map[string]*batch)}
}

// CreateBatch 创建发布批次。targets 为目标终端集合，canaries 为其中非空的
// 试点子集；创建后两者不变。批次独立运行。
func (c *Coordinator) CreateBatch(batchID string, targets, canaries []string) error {
	if len(targets) == 0 {
		return errors.New("canary: targets must not be empty")
	}
	if len(canaries) == 0 {
		return errors.New("canary: canaries must not be empty")
	}
	targetSet := make(map[string]bool, len(targets))
	for _, t := range targets {
		if targetSet[t] {
			return fmt.Errorf("canary: duplicate target %q", t)
		}
		targetSet[t] = true
	}
	seenCanary := make(map[string]bool, len(canaries))
	for _, cid := range canaries {
		if !targetSet[cid] {
			return fmt.Errorf("canary: canary %q not in targets", cid)
		}
		if seenCanary[cid] {
			return fmt.Errorf("canary: duplicate canary %q", cid)
		}
		seenCanary[cid] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.batches[batchID]; exists {
		return ErrBatchExists
	}
	c.batches[batchID] = newBatch(batchID, targets, canaries)
	return nil
}

// Claim 领取下一条指令。没有可派发指令时立即返回 ok=false，不等待回执。
func (c *Coordinator) Claim(batchID string) (cmd Command, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, exists := c.batches[batchID]
	if !exists {
		return Command{}, false, ErrBatchNotFound
	}
	cmd, ok = b.claim()
	return cmd, ok, nil
}

// Report 提交执行回执。success 表示该指令执行成功。回执可能乱序、重复；
// 重复回执被忽略，不改变任何计数与终态。
func (c *Coordinator) Report(batchID, commandID string, success bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, exists := c.batches[batchID]
	if !exists {
		return ErrBatchNotFound
	}
	_, err := b.report(commandID, success)
	return err
}

// Batch 查询批次状态快照。
func (c *Coordinator) Batch(batchID string) (BatchView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, exists := c.batches[batchID]
	if !exists {
		return BatchView{}, ErrBatchNotFound
	}
	return b.view(), nil
}

// Device 查询批次内单个终端的状态快照。
func (c *Coordinator) Device(batchID, deviceID string) (DeviceView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, exists := c.batches[batchID]
	if !exists {
		return DeviceView{}, ErrBatchNotFound
	}
	v, ok := b.deviceView(deviceID)
	if !ok {
		return DeviceView{}, ErrDeviceNotFound
	}
	return v, nil
}
