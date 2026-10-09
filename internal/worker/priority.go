package worker

import (
	"context"
	"time"
)

const ThreadPriorityContract = "experimental_source_thread_priority_v1"

// PrioritySetting separates the requested setting, the syscall's reply and a
// sequential read-back. A successful syscall is not proof of scheduling or
// physical I/O effects. Nil values are unknown, never an invented default.
type PrioritySetting struct {
	Policy    string `json:"policy"`
	Target    int    `json:"target"`
	Attempted bool   `json:"request_attempted"`
	Accepted  *bool  `json:"request_accepted"`
	Before    *int   `json:"before"`
	Observed  *int   `json:"observed"`
	Reason    string `json:"reason"`
}

// ThreadPriorityObservation is live, historical evidence for one source
// handler's locked OS thread. It is not persistent or a process-wide setting.
// Other threads and children are unexamined; newly created threads may inherit
// scheduling state. Fixed cadence and independent budgets remain necessary.
type ThreadPriorityObservation struct {
	Contract                    string           `json:"contract"`
	Scope                       string           `json:"scope"`
	Platform                    string           `json:"platform"`
	CheckedAt                   time.Time        `json:"checked_at"`
	CPU                         *PrioritySetting `json:"cpu"`
	IO                          *PrioritySetting `json:"io"`
	Background                  *PrioritySetting `json:"background"`
	ThreadDisposition           string           `json:"thread_disposition"`
	NewThreadsMayInherit        bool             `json:"new_threads_may_inherit"`
	WholeProcessVerified        bool             `json:"whole_process_verified"`
	EffectiveSchedulingVerified bool             `json:"effective_scheduling_verified"`
	PhysicalIOVerified          bool             `json:"physical_io_verified"`
	PhysicalPowerVerified       bool             `json:"physical_power_verified"`
}

func newThreadPriorityObservation(platform string) ThreadPriorityObservation {
	return ThreadPriorityObservation{
		Contract: ThreadPriorityContract, Scope: "experimental_source_handler_os_thread", Platform: platform,
		CheckedAt: time.Now().UTC(), ThreadDisposition: "locked_until_handler_goroutine_exit", NewThreadsMayInherit: true,
	}
}

// Every native call is finite bookkeeping in the source goroutine, outside the
// owning control loop. Failure is diagnostic and never bypasses fixed pacing.
// Callers must already hold runtime.LockOSThread and must not unlock afterward.
func requestPrioritySetting(ctx context.Context, policy string, target int, get func() (int, error), set func(int) error, preserve func(int) bool) PrioritySetting {
	r := PrioritySetting{Policy: policy, Target: target, Reason: "not_requested"}
	if ctx.Err() != nil {
		r.Reason = "canceled_before_observation"
		return r
	}
	before, err := get()
	if err != nil {
		r.Reason = "before_unavailable"
		return r
	}
	r.Before, r.Observed = &before, &before
	if preserve(before) {
		r.Reason = "existing_setting_preserved"
		return r
	}
	if ctx.Err() != nil {
		r.Reason = "canceled_before_request"
		return r
	}
	r.Attempted = true
	accepted := set(target) == nil
	r.Accepted = &accepted
	// The previous observation cannot stand in for a post-request read-back.
	r.Observed = nil
	if ctx.Err() != nil {
		r.Reason = "readback_canceled"
		return r
	}
	after, err := get()
	if err != nil {
		r.Reason = "readback_unavailable"
		return r
	}
	r.Observed = &after
	if accepted {
		r.Reason = "request_accepted_and_setting_observed"
	} else {
		r.Reason = "request_failed_and_setting_observed"
	}
	return r
}
