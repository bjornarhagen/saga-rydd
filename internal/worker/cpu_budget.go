package worker

import (
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// CPUObservation describes one completed worker window. It is live feedback,
// not a persisted CPU quota or an observation of physical I/O or energy use.
type CPUObservation struct {
	Contract            string     `json:"contract"`
	Source              string     `json:"source"`
	TargetPercent       int        `json:"target_percent"`
	Status              string     `json:"status"`
	Reason              string     `json:"reason,omitempty"`
	ObservedAt          *time.Time `json:"observed_at"`
	WindowCPUNS         *int64     `json:"window_cpu_ns"`
	WindowElapsedNS     *int64     `json:"window_elapsed_ns"`
	BackoffNS           int64      `json:"backoff_ns"`
	BackoffCapped       bool       `json:"backoff_capped"`
	NextAllowedAt       *time.Time `json:"next_allowed_at"`
	UnknownObservations uint64     `json:"unknown_observations"`
}

type cpuWindow struct {
	startedAt time.Time
	before    time.Duration
	err       error
}

type cpuBudget struct {
	observation CPUObservation
	nextAllowed time.Time // Retains the monotonic clock; exported times do not.
}

func newCPUBudget() cpuBudget {
	return cpuBudget{observation: CPUObservation{
		Contract: "worker_process_cpu_window_v1", Source: "getrusage_self",
		TargetPercent: 1, Status: "not_recorded",
	}}
}

func beginCPUWindow(observe func() (time.Duration, error)) cpuWindow {
	return beginCPUWindowAt(observe, time.Now())
}

func beginCPUWindowAt(observe func() (time.Duration, error), started time.Time) cpuWindow {
	window := cpuWindow{startedAt: started}
	window.before, window.err = observe()
	return window
}

func (budget *cpuBudget) finish(window cpuWindow, completed time.Time, after time.Duration, observationErr error) {
	budget.finishWithClocks(window, completed, completed, after, observationErr)
}

func (budget *cpuBudget) finishWithClocks(window cpuWindow, completedWall, completedElapsed time.Time, after time.Duration, observationErr error) {
	at := completedWall.UTC()
	unknown := budget.observation.UnknownObservations
	observation := newCPUBudget().observation
	observation.ObservedAt = &at
	observation.UnknownObservations = unknown
	elapsed := completedElapsed.Sub(window.startedAt)
	if elapsed > 0 {
		nanos := int64(elapsed)
		observation.WindowElapsedNS = &nanos
	}
	switch {
	case window.err != nil || observationErr != nil:
		observation.Reason = "cpu_observation_unavailable"
	case window.before < 0 || after < 0:
		observation.Reason = "cpu_observation_invalid"
	case after < window.before:
		observation.Reason = "cpu_observation_regressed"
	case elapsed <= 0:
		observation.Reason = "elapsed_window_invalid"
	default:
		delta := after - window.before
		nanos := int64(delta)
		observation.WindowCPUNS = &nanos
		observation.Status = "observed"
		wait, capped := cpuFeedbackWait(delta, elapsed)
		observation.BackoffNS, observation.BackoffCapped = int64(wait), capped
		budget.nextAllowed = time.Time{}
		if wait > 0 {
			budget.nextAllowed = completedElapsed.Add(wait)
			next := completedWall.Add(wait).UTC()
			observation.NextAllowedAt = &next
		}
		budget.observation = observation
		return
	}
	observation.Status = "unknown"
	if observation.UnknownObservations != ^uint64(0) {
		observation.UnknownObservations++
	}
	// Failed observations never become a fabricated zero. The worker retains
	// its configured cadence and all durable gates, without an inferred cap.
	budget.nextAllowed = time.Time{}
	budget.observation = observation
}

// cpuFeedbackWait adds enough idle time to give this window a 1% feedback
// period, capped at one hour. It retains no credit from earlier idle windows.
// Divide elapsed first so valid large counters cannot overflow CPU*100.
func cpuFeedbackWait(cpu, elapsed time.Duration) (time.Duration, bool) {
	return state.CPUFeedbackWait(cpu, elapsed)
}
