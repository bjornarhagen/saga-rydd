package worker

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func TestWorkerCPUFeedbackWindows(t *testing.T) {
	for _, test := range []struct {
		name         string
		cpu, elapsed time.Duration
		wait         time.Duration
		capped       bool
	}{
		{name: "zero CPU", elapsed: time.Second},
		{name: "below target", cpu: time.Millisecond, elapsed: time.Second},
		{name: "at target", cpu: 10 * time.Millisecond, elapsed: time.Second},
		{name: "additional idle", cpu: 15 * time.Millisecond, elapsed: time.Second, wait: 500 * time.Millisecond},
		{name: "integer remainder", cpu: 2, elapsed: 199, wait: 1},
		{name: "exact cap", cpu: 37 * time.Second, elapsed: 100 * time.Second, wait: time.Hour},
		{name: "above cap", cpu: 37 * time.Second, elapsed: time.Second, wait: time.Hour, capped: true},
		{name: "large valid counter", cpu: time.Duration(math.MaxInt64), elapsed: time.Second, wait: time.Hour, capped: true},
		{name: "large elapsed and CPU", cpu: time.Duration(math.MaxInt64) / 100, elapsed: time.Duration(math.MaxInt64) - 99, wait: 92},
	} {
		t.Run(test.name, func(t *testing.T) {
			wait, capped := cpuFeedbackWait(test.cpu, test.elapsed)
			if wait != test.wait || capped != test.capped {
				t.Fatal("feedback differs", wait, capped, test.wait, test.capped)
			}
		})
	}
}

func TestWorkerCPUUnknownAndObservedZeroJSON(t *testing.T) {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		before      time.Duration
		after       time.Duration
		beforeError error
		afterError  error
		elapsed     time.Duration
		reason      string
	}{
		{name: "before error", beforeError: errors.New("synthetic observation failure"), elapsed: time.Second, reason: "cpu_observation_unavailable"},
		{name: "after error", afterError: errors.New("synthetic observation failure"), elapsed: time.Second, reason: "cpu_observation_unavailable"},
		{name: "negative before", before: -1, elapsed: time.Second, reason: "cpu_observation_invalid"},
		{name: "negative after", after: -1, elapsed: time.Second, reason: "cpu_observation_invalid"},
		{name: "counter rollback", before: time.Second, elapsed: time.Second, reason: "cpu_observation_regressed"},
		{name: "elapsed rollback", elapsed: -time.Second, reason: "elapsed_window_invalid"},
		{name: "zero elapsed", reason: "elapsed_window_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := newCPUBudget()
			budget.finish(cpuWindow{startedAt: start, before: test.before, err: test.beforeError}, start.Add(test.elapsed), test.after, test.afterError)
			observation := budget.observation
			if observation.Status != "unknown" || observation.Reason != test.reason || observation.WindowCPUNS != nil || observation.UnknownObservations != 1 || observation.NextAllowedAt != nil || observation.BackoffNS != 0 || !budget.nextAllowed.IsZero() {
				t.Fatal("unknown observation became a measured value", observation)
			}
			budget.finish(cpuWindow{startedAt: start, before: time.Second}, start.Add(time.Second), time.Second, nil)
			observation = budget.observation
			if observation.Status != "observed" || observation.Reason != "" || observation.WindowCPUNS == nil || *observation.WindowCPUNS != 0 || observation.UnknownObservations != 1 {
				t.Fatal("measured zero or prior unknown count lost", observation)
			}
			if !budget.nextAllowed.IsZero() || observation.NextAllowedAt != nil {
				t.Fatal("measured zero created a CPU gate", observation)
			}
		})
	}
	budget := newCPUBudget()
	assertCPUJSON := func(wantStatus string, wantCPU any) {
		t.Helper()
		raw, err := json.Marshal(budget.observation)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		value, exists := fields["window_cpu_ns"]
		if !exists || value != wantCPU || fields["status"] != wantStatus || fields["target_percent"] != float64(1) || fields["contract"] != "worker_process_cpu_window_v1" {
			t.Fatal("nullable observation contract differs", string(raw))
		}
	}
	assertCPUJSON("not_recorded", nil)
	budget.finish(cpuWindow{startedAt: start}, start.Add(time.Second), 0, nil)
	assertCPUJSON("observed", float64(0))
	budget.finish(cpuWindow{startedAt: start}, start.Add(time.Second), 0, errors.New("synthetic observation failure"))
	assertCPUJSON("unknown", nil)
}

func TestWorkerCPUWindowKeepsNoIdleCredit(t *testing.T) {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	budget := newCPUBudget()
	budget.finish(cpuWindow{startedAt: start}, start.Add(time.Second), 100*time.Millisecond, nil)
	if budget.observation.BackoffNS != int64(9*time.Second) || !budget.nextAllowed.Equal(start.Add(10*time.Second)) {
		t.Fatal("first window deadline differs", budget.observation)
	}
	// A delayed wake/idle period earns no credits for the next completed window.
	later := start.Add(24 * time.Hour)
	budget.finish(cpuWindow{startedAt: later, before: 100 * time.Millisecond}, later.Add(time.Second), 200*time.Millisecond, nil)
	if budget.observation.BackoffNS != int64(9*time.Second) || !budget.nextAllowed.Equal(later.Add(10*time.Second)) {
		t.Fatal("idle time erased the next feedback wait", budget.observation)
	}
	// The native cumulative counter is new on restart; live feedback is not a quota.
	restarted := newCPUBudget()
	if restarted.observation.Status != "not_recorded" || restarted.observation.WindowCPUNS != nil || !restarted.nextAllowed.IsZero() {
		t.Fatal("new instance retained live CPU state", restarted)
	}
}
