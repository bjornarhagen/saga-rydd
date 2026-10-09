package main

import (
	"context"
	"sync"
	"time"
)

const (
	kernelIOContract = "linux_proc_io_samples_v1"
	kernelIOScope    = "owned_worker_process_and_waited_children"
	kernelIOTimeout  = 2 * time.Second
)

// These are kernel accounting fields, not device traffic or scanner budgets.
// write_bytes accounts page dirtying; cancelled writes are never subtracted.
type kernelIOCounters struct {
	RChar               uint64 `json:"rchar"`
	WChar               uint64 `json:"wchar"`
	SysCR               uint64 `json:"syscr"`
	SysCW               uint64 `json:"syscw"`
	ReadBytes           uint64 `json:"read_bytes"`
	WriteBytes          uint64 `json:"write_bytes"`
	CancelledWriteBytes uint64 `json:"cancelled_write_bytes"`
}

type kernelIOSample struct {
	Status            string            `json:"status"`
	Reason            string            `json:"reason"`
	StartedElapsedNS  int64             `json:"started_elapsed_ns"`
	FinishedElapsedNS int64             `json:"finished_elapsed_ns"`
	Counters          *kernelIOCounters `json:"counters"`
}

type kernelIOSummary struct {
	Contract        string            `json:"contract"`
	Scope           string            `json:"scope"`
	Status          string            `json:"status"`
	Reason          string            `json:"reason"`
	ObservedSamples int               `json:"observed_samples"`
	RefusedSamples  int               `json:"refused_samples"`
	Gaps            bool              `json:"gaps"`
	FirstElapsedNS  *int64            `json:"first_elapsed_ns"`
	LastElapsedNS   *int64            `json:"last_elapsed_ns"`
	FirstCounters   *kernelIOCounters `json:"first_counters"`
	LastCounters    *kernelIOCounters `json:"last_counters"`
	SampledDelta    *kernelIOCounters `json:"sampled_delta"`
	FullLifetime    bool              `json:"full_lifetime"`
	AtomicSnapshot  bool              `json:"atomic_snapshot"`
}

type kernelIOSource interface {
	read(context.Context, <-chan struct{}) (kernelIOCounters, string)
	close()
}

type kernelIOTracker struct {
	mu                           sync.Mutex
	source                       kernelIOSource
	done                         <-chan struct{}
	started                      time.Time
	now                          func() time.Time
	initialStatus, refusedReason string
	closed                       bool
	state                        kernelIOSummary
}

func bindKernelIO(ctx context.Context, c *child) *kernelIOTracker {
	bindCtx, cancel := context.WithTimeout(ctx, kernelIOTimeout)
	defer cancel()
	source, status, reason := openKernelIO(bindCtx, c)
	return newKernelIOTracker(source, c.done, c.started, time.Now, status, reason)
}

func newKernelIOTracker(source kernelIOSource, done <-chan struct{}, started time.Time, now func() time.Time, status, reason string) *kernelIOTracker {
	if status != "unsupported" && status != "unavailable" {
		status = "unavailable"
	}
	reason = boundedKernelIOReason(reason)
	return &kernelIOTracker{source: source, done: done, started: started, now: now, initialStatus: status, refusedReason: reason, state: kernelIOSummary{Contract: kernelIOContract, Scope: kernelIOScope, Status: status, Reason: reason}}
}

func boundedKernelIOReason(reason string) string {
	switch reason {
	case "", "platform_unsupported", "procfs_unavailable", "resolution_unsupported", "process_scope_unavailable", "process_namespace_unavailable", "process_identity_unavailable", "process_identity_changed", "attribute_shape_unavailable", "attribute_read_unavailable", "attribute_read_limit", "attribute_too_large", "counter_profile_unsupported", "not_sampled", "context_canceled", "deadline_exceeded", "context_unavailable", "worker_reaped", "observer_closed", "counter_regressed", "elapsed_regressed", "observation_unavailable":
		return reason
	default:
		return "observation_unavailable"
	}
}

func childReaped(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func kernelIOContextReason(ctx context.Context) string {
	if ctx.Err() == context.Canceled {
		return "context_canceled"
	}
	if ctx.Err() != nil {
		return "deadline_exceeded"
	}
	return ""
}

func (t *kernelIOTracker) sample(ctx context.Context) *kernelIOSample {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := t.now().Sub(t.started).Nanoseconds()
	out := &kernelIOSample{Status: "unavailable", StartedElapsedNS: max(start, 0), FinishedElapsedNS: max(start, 0)}
	refuse := func(status, reason string, terminal bool) *kernelIOSample {
		out.Status, out.Reason, out.Counters = status, reason, nil
		t.state.RefusedSamples++
		if terminal {
			t.refusedReason = reason
			t.initialStatus = status
			t.state.Gaps = true
			t.state.SampledDelta = nil
			t.state.Status, t.state.Reason = status, reason
			if t.source != nil {
				t.source.close()
				t.source = nil
			}
		}
		return out
	}
	// No proc open, read or numeric lookup after the reap notification.
	if childReaped(t.done) {
		return refuse("unavailable", "worker_reaped", false)
	}
	if t.closed {
		return refuse("unavailable", "observer_closed", false)
	}
	if t.source == nil {
		return refuse(t.initialStatus, t.refusedReason, false)
	}
	if ctx == nil {
		return refuse("unavailable", "context_unavailable", true)
	}
	readCtx, cancel := context.WithTimeout(ctx, kernelIOTimeout)
	defer cancel()
	if reason := kernelIOContextReason(readCtx); reason != "" {
		return refuse("unavailable", reason, true)
	}
	if start < 0 || (t.state.LastElapsedNS != nil && start < *t.state.LastElapsedNS) {
		return refuse("regressed", "elapsed_regressed", true)
	}
	counters, reason := t.source.read(readCtx, t.done)
	finish := t.now().Sub(t.started).Nanoseconds()
	out.FinishedElapsedNS = max(finish, 0)
	if childReaped(t.done) {
		return refuse("unavailable", "worker_reaped", false)
	}
	if contextReason := kernelIOContextReason(readCtx); contextReason != "" {
		return refuse("unavailable", contextReason, true)
	}
	if finish < start {
		return refuse("regressed", "elapsed_regressed", true)
	}
	if reason != "" {
		return refuse("unavailable", boundedKernelIOReason(reason), true)
	}
	if t.state.LastCounters != nil && !kernelIOCountersAtLeast(counters, *t.state.LastCounters) {
		return refuse("regressed", "counter_regressed", true)
	}
	out.Status, out.Reason, out.Counters = "observed", "sequential_kernel_accounting", cloneKernelIOCounters(&counters)
	t.state.ObservedSamples++
	if t.state.FirstCounters == nil {
		t.state.FirstCounters = cloneKernelIOCounters(&counters)
		t.state.FirstElapsedNS = cloneKernelIOTime(&finish)
	}
	t.state.LastCounters = cloneKernelIOCounters(&counters)
	t.state.LastElapsedNS = cloneKernelIOTime(&finish)
	t.state.Status, t.state.Reason = "observed", "sampled_prefix_only"
	if !t.state.Gaps {
		delta := kernelIOCounterDelta(counters, *t.state.FirstCounters)
		t.state.SampledDelta = &delta
	}
	return out
}

func kernelIOCountersAtLeast(a, b kernelIOCounters) bool {
	return a.RChar >= b.RChar && a.WChar >= b.WChar && a.SysCR >= b.SysCR && a.SysCW >= b.SysCW && a.ReadBytes >= b.ReadBytes && a.WriteBytes >= b.WriteBytes && a.CancelledWriteBytes >= b.CancelledWriteBytes
}
func kernelIOCounterDelta(a, b kernelIOCounters) kernelIOCounters {
	return kernelIOCounters{a.RChar - b.RChar, a.WChar - b.WChar, a.SysCR - b.SysCR, a.SysCW - b.SysCW, a.ReadBytes - b.ReadBytes, a.WriteBytes - b.WriteBytes, a.CancelledWriteBytes - b.CancelledWriteBytes}
}
func cloneKernelIOCounters(p *kernelIOCounters) *kernelIOCounters {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func cloneKernelIOTime(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func (t *kernelIOTracker) summary() *kernelIOSummary {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.state
	s.FirstCounters, s.LastCounters, s.SampledDelta = cloneKernelIOCounters(s.FirstCounters), cloneKernelIOCounters(s.LastCounters), cloneKernelIOCounters(s.SampledDelta)
	s.FirstElapsedNS, s.LastElapsedNS = cloneKernelIOTime(s.FirstElapsedNS), cloneKernelIOTime(s.LastElapsedNS)
	return &s
}
func (t *kernelIOTracker) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.source != nil {
		t.source.close()
		t.source = nil
	}
	t.closed = true
}
