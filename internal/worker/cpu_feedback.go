package worker

import (
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Saved times are wall-clock evidence. Live CPU/cadence/WAL waits use the
// elapsed clock. Compare their remaining durations, never their timestamps.
type dispatchWait struct {
	duration time.Duration
	reason   string
}

func (w *dispatchWait) add(remaining time.Duration, reason string) {
	if remaining > w.duration {
		w.duration, w.reason = remaining, reason
	}
}

func (w *dispatchWait) wall(deadline, now time.Time, reason string) {
	if !deadline.IsZero() {
		w.add(deadline.UTC().Sub(now.UTC()), reason)
	}
}

func (w *dispatchWait) elapsed(deadline, now time.Time, reason string) {
	if !deadline.IsZero() {
		w.add(deadline.Sub(now), reason)
	}
}

type cpuFeedbackGate struct {
	token, status string
	savedDeadline time.Time
	elapsedDue    time.Time
}

func (gate *cpuFeedbackGate) update(feedback state.CPUFeedbackState, wall, elapsed time.Time) {
	token := ""
	if feedback.Window != nil {
		token = feedback.Window.Token
	}
	due := time.Time{}
	if feedback.NextAllowedAt != nil {
		due = feedback.NextAllowedAt.UTC()
	}
	if gate.token == token && gate.status == feedback.Status && gate.savedDeadline.Equal(due) {
		return // A wall adjustment or status query cannot shorten a live wait.
	}
	gate.token, gate.status, gate.savedDeadline = token, feedback.Status, due
	gate.elapsedDue = time.Time{}
	if !due.IsZero() {
		gate.elapsedDue = elapsed.Add(max(due.Sub(wall.UTC()), 0))
	}
}

func (gate *cpuFeedbackGate) add(wait *dispatchWait, feedback state.CPUFeedbackState, wall, elapsed time.Time) {
	if feedback.ClockHighWater != nil {
		wait.wall(*feedback.ClockHighWater, wall, "cpu_clock_rollback")
	}
	reason := "durable_cpu_backoff"
	if feedback.Status == "recovered_unknown" {
		reason = "cpu_recovery_backoff"
	}
	wait.wall(gate.savedDeadline, wall, reason)
	wait.elapsed(gate.elapsedDue, elapsed, reason)
}

func cpuMeasurement(observation CPUObservation) state.CPUWindowMeasurement {
	return state.CPUWindowMeasurement{CPUTimeNS: observation.WindowCPUNS,
		ElapsedNS: observation.WindowElapsedNS, Reason: observation.Reason}
}
