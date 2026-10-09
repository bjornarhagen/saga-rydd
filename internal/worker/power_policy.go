package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
)

const powerSampleInterval = 5 * time.Minute

var (
	errPowerElapsedRollback    = errors.New("power observation elapsed clock moved backwards")
	errPowerObservationInvalid = errors.New("power observation does not match the supported contract")
	// This process-wide slot is shared by successive in-process Run calls.
	// It starts no probe until an otherwise-admissible source turn asks.
	processPowerCoordinator = newPowerCoordinator(powerinfo.Observe, time.Now, time.Now)
)

type powerObserver func(context.Context) (powerinfo.Observation, error)

type powerCoordinator struct {
	mu                     sync.Mutex
	clockMu                sync.Mutex
	observer               powerObserver
	launch                 func(func())
	afterCompletionCapture func() // private generated ordering seam
	wallNow, elapsedNow    func() time.Time
	ticket                 *powerTicket
	nextID                 uint64
	wallHigh, elapsedHigh  time.Time
	lastWall, lastElapsed  time.Time
}

// Launch anchors and the done channel are fixed before goroutine launch.
// Only the one producer writes completion, immediately before closing done.
// A closed channel supports every reader without consuming the result.
type powerTicket struct {
	ID                        uint64
	LaunchWall, LaunchElapsed time.Time
	done                      chan struct{}
	ctx                       context.Context
	cancel                    context.CancelFunc
	refusal                   error // guarded by coordinator.mu until terminal publication
	completion                powerCompletion
}

type powerCompletion struct {
	ID                              uint64
	LaunchWall, LaunchElapsed       time.Time
	CompletedWall, CompletedElapsed time.Time
	Observation                     *powerinfo.Observation
	Err                             error
}

type powerPolicyDecision struct {
	Ticket                           *powerTicket
	Status, Reason                   string
	Observation                      *powerinfo.Observation
	SourceBackoff, Sampling, Started bool
	Wait, NextSampleWait             time.Duration
}

func newPowerCoordinator(observer powerObserver, wallNow, elapsedNow func() time.Time) *powerCoordinator {
	if wallNow == nil {
		wallNow = time.Now
	}
	if elapsedNow == nil {
		elapsedNow = time.Now
	}
	return &powerCoordinator{observer: observer, wallNow: wallNow, elapsedNow: elapsedNow, launch: func(run func()) { go run() }}
}

func clonePowerObservation(o *powerinfo.Observation) *powerinfo.Observation {
	if o == nil {
		return nil
	}
	cloned := *o
	if o.SystemBatteryDischargingObserved != nil {
		value := *o.SystemBatteryDischargingObserved
		cloned.SystemBatteryDischargingObserved = &value
	}
	if o.SupplyEntriesObserved != nil {
		value := *o.SupplyEntriesObserved
		cloned.SupplyEntriesObserved = &value
	}
	return &cloned
}

func (t *powerTicket) completed() (powerCompletion, bool) {
	if t == nil {
		return powerCompletion{}, false
	}
	select {
	case <-t.done:
		result := t.completion
		result.Observation = clonePowerObservation(result.Observation)
		return result, true
	default:
		return powerCompletion{}, false
	}
}

// cancelTicket cancels only this coordinator's exact occupied ticket. It
// never frees the slot, waits for a callback, or changes terminal evidence.
func (c *powerCoordinator) cancelTicket(t *powerTicket) bool {
	if t == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ticket != t || c.ticket.ID != t.ID {
		return false
	}
	select {
	case <-t.done:
		return false
	default:
	}
	if t.refusal == nil {
		t.refusal = context.Canceled
	}
	t.cancel()
	return true
}

func (c *powerCoordinator) clockSample() (time.Time, time.Time, error) {
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	wall, elapsed := c.wallNow().Round(0).UTC(), c.elapsedNow()
	var err error
	if !c.wallHigh.IsZero() && wall.Before(c.wallHigh) {
		err = powerinfo.ErrWallClockRollback
	}
	if !c.elapsedHigh.IsZero() && elapsed.Before(c.elapsedHigh) {
		err = errors.Join(err, errPowerElapsedRollback)
	}
	if wall.After(c.wallHigh) {
		c.wallHigh = wall
	}
	if elapsed.After(c.elapsedHigh) {
		c.elapsedHigh = elapsed
	}
	return wall, elapsed, err
}

func (c *powerCoordinator) sampleWaitLocked(wall, elapsed time.Time) time.Duration {
	wait := time.Duration(0)
	if !c.lastWall.IsZero() {
		wait = max(wait, c.lastWall.Add(powerSampleInterval).Sub(wall))
	}
	if !c.lastElapsed.IsZero() {
		wait = max(wait, c.lastElapsed.Add(powerSampleInterval).Sub(elapsed))
	}
	// These independent high-water waits cannot grant rollback credits.
	c.clockMu.Lock()
	wait = max(wait, c.wallHigh.Sub(wall), c.elapsedHigh.Sub(elapsed))
	c.clockMu.Unlock()
	return wait
}

func powerWindowError(t *powerTicket, wall, elapsed time.Time) error {
	if wall.Before(t.LaunchWall) {
		return powerinfo.ErrWallClockRollback
	}
	if elapsed.Before(t.LaunchElapsed) {
		return errPowerElapsedRollback
	}
	if !wall.Before(t.LaunchWall.Add(powerinfo.ObservationTimeout)) || !elapsed.Before(t.LaunchElapsed.Add(powerinfo.ObservationTimeout)) {
		return context.DeadlineExceeded
	}
	return nil
}

// sourcePolicy must only be called when a worker has otherwise-admissible
// due source work. Cached status never calls it. It does not dispatch work,
// consume quotas, reanchor observations, or probe while no source is due.
func (c *powerCoordinator) sourcePolicy(parent context.Context) powerPolicyDecision {
	return c.powerPolicy(parent, true)
}

// recheckSourcePolicy may refuse an already planned source turn, but never
// starts a probe after that turn has consumed its durable dispatch receipt.
func (c *powerCoordinator) recheckSourcePolicy(parent context.Context) powerPolicyDecision {
	return c.powerPolicy(parent, false)
}

func (c *powerCoordinator) powerPolicy(parent context.Context, allowStart bool) powerPolicyDecision {
	decision := powerPolicyDecision{Status: "unknown", Reason: "power_observer_unavailable"}
	if parent == nil || context.Cause(parent) != nil {
		return decision
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	wall, elapsed, clockErr := c.clockSample()
	decision.NextSampleWait = c.sampleWaitLocked(wall, elapsed)
	t := c.ticket
	if t != nil {
		decision.Ticket = t
		if result, ready := t.completed(); ready {
			if clockErr == nil && freshPowerCompletion(result, wall, elapsed) {
				decision.Observation = clonePowerObservation(result.Observation)
				if observedPowerDischarge(result.Observation) {
					decision.Status = "system_battery_discharging_observed"
					decision.Reason = "power_discharge_witness"
					decision.SourceBackoff = true
					decision.Wait = decision.NextSampleWait
				} else {
					decision.Reason = "power_sample_unknown"
				}
			} else {
				decision.Reason = powerCompletionReason(result, clockErr)
			}
			if decision.NextSampleWait > 0 || clockErr != nil {
				return decision
			}
		} else {
			decision.Sampling = true
			refusal := clockErr
			if refusal == nil {
				refusal = context.Cause(t.ctx)
			}
			if refusal == nil {
				refusal = powerWindowError(t, wall, elapsed)
			}
			if refusal != nil {
				if t.refusal == nil {
					t.refusal = refusal
				}
				t.cancel()
				decision.Reason = "power_observer_stalled"
				return decision // Actual callback return alone frees the slot.
			}
			decision.Status = "observing"
			decision.Reason = "power_sample_pending"
			decision.SourceBackoff = true
			decision.Wait = min(t.LaunchWall.Add(powerinfo.ObservationTimeout).Sub(wall), t.LaunchElapsed.Add(powerinfo.ObservationTimeout).Sub(elapsed))
			return decision
		}
	}
	if clockErr != nil {
		decision.Reason = "power_clock_rollback"
		return decision
	}
	if decision.NextSampleWait > 0 {
		decision.Reason = "power_sampling_interval"
		return decision
	}
	if c.observer == nil || c.nextID == ^uint64(0) {
		return decision
	}
	if !allowStart {
		decision.Status = "sample_required"
		decision.Reason = "power_sample_required"
		decision.Observation = nil
		decision.SourceBackoff = true
		decision.Wait = 0
		return decision
	}
	// Queue time belongs to this outer window. Neither Observe's own timer
	// nor a later Run can renew it or replace its occupied process slot.
	ctx, cancel := context.WithTimeout(parent, powerinfo.ObservationTimeout)
	c.nextID++
	t = &powerTicket{ID: c.nextID, LaunchWall: wall, LaunchElapsed: elapsed, done: make(chan struct{}), ctx: ctx, cancel: cancel}
	c.ticket = t
	c.lastWall, c.lastElapsed = wall, elapsed
	decision = powerPolicyDecision{Ticket: t, Status: "observing", Reason: "power_sample_pending", SourceBackoff: true, Sampling: true, Started: true, Wait: powerinfo.ObservationTimeout, NextSampleWait: powerSampleInterval}
	c.launch(func() { c.observe(t) })
	return decision
}

func (c *powerCoordinator) observe(t *powerTicket) {
	defer t.cancel()
	// A delayed goroutine must refuse before invoking even a new observer.
	c.mu.Lock()
	wall, elapsed, err := c.clockSample()
	if err == nil {
		err = context.Cause(t.ctx)
	}
	if err == nil {
		err = powerWindowError(t, wall, elapsed)
	}
	if t.refusal != nil {
		err = t.refusal
	}
	c.mu.Unlock()
	var observation powerinfo.Observation
	if err == nil {
		observation, err = c.observer(t.ctx)
	}
	// Capture terminal anchors immediately after callback return, before
	// queueing on the coordinator lock. They are never refreshed by reads.
	completedWall, completedElapsed, completionClockErr := c.clockSample()
	if completionClockErr != nil {
		err = completionClockErr
	}
	if cause := context.Cause(t.ctx); cause != nil {
		err = cause
	}
	if windowErr := powerWindowError(t, completedWall, completedElapsed); windowErr != nil {
		err = windowErr
	}
	if c.afterCompletionCapture != nil {
		c.afterCompletionCapture()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Clock samples are serialized separately from this publication lock.
	// A newer legitimate sample must not make these frozen return anchors
	// look like rollback. Only their capture-time guard can classify that.
	if t.refusal != nil {
		err = t.refusal
	}
	if cause := context.Cause(t.ctx); cause != nil && err == nil {
		err = cause
	}
	var saved *powerinfo.Observation
	if err == nil {
		if validPowerObservation(observation, t.LaunchWall, completedWall) {
			saved = clonePowerObservation(&observation)
		} else {
			err = errPowerObservationInvalid
		}
	}
	t.completion = powerCompletion{ID: t.ID, LaunchWall: t.LaunchWall, LaunchElapsed: t.LaunchElapsed, CompletedWall: completedWall, CompletedElapsed: completedElapsed, Observation: saved, Err: err}
	close(t.done)
}

func freshPowerCompletion(result powerCompletion, wall, elapsed time.Time) bool {
	if result.Err != nil || result.Observation == nil {
		return false
	}
	for _, pair := range [][2]time.Time{{wall, result.LaunchWall}, {wall, result.CompletedWall}, {elapsed, result.LaunchElapsed}, {elapsed, result.CompletedElapsed}} {
		age := pair[0].Sub(pair[1])
		if age < 0 || age >= powerSampleInterval {
			return false
		}
	}
	return true
}

func observedPowerDischarge(o *powerinfo.Observation) bool {
	return o != nil && o.SystemBatteryDischargingObserved != nil && *o.SystemBatteryDischargingObserved
}

func validPowerObservation(o powerinfo.Observation, launch, completed time.Time) bool {
	if o.Contract != powerinfo.Contract || !o.SequentialObservations || o.NamespaceAuthenticated || o.PhysicalPowerVerified || o.StartedAt.IsZero() || o.FinishedAt.IsZero() || o.StartedAt.Before(launch) || o.FinishedAt.Before(o.StartedAt) || o.FinishedAt.After(completed) {
		return false
	}
	if o.Platform != "linux" && o.Platform != "darwin" {
		return false
	}
	if o.Profile != powerinfo.LinuxProfile && o.Profile != "unsupported" {
		return false
	}
	if o.AttributeAttempts < 0 || o.AttributeAttempts > powerinfo.MaxAttributeAttempts || o.ReturnedAttributeBytes < 0 || o.ReturnedAttributeBytes > powerinfo.MaxReturnedAttributeBytes || o.ProvidersProcessed < 0 || o.ProvidersProcessed > powerinfo.MaxSupplyEntries || o.ConfirmedSystemBatteries < 0 || o.ConfirmedSystemBatteries > o.ProvidersProcessed || o.DischargingSystemBatteries < 0 || o.DischargingSystemBatteries > o.ConfirmedSystemBatteries || o.AmbiguousProviders < 0 || o.AmbiguousProviders > o.ProvidersProcessed {
		return false
	}
	if o.SupplyEntriesObserved != nil && (*o.SupplyEntriesObserved < 0 || *o.SupplyEntriesObserved > powerinfo.MaxSupplyEntries+1) {
		return false
	}
	if o.SystemBatteryDischargingObserved != nil {
		return *o.SystemBatteryDischargingObserved && o.Platform == "linux" && o.Profile == powerinfo.LinuxProfile && o.Status == "system_battery_discharging_observed" && o.DischargingSystemBatteries > 0
	}
	return o.Status == "unknown" && o.DischargingSystemBatteries == 0
}

func powerCompletionReason(result powerCompletion, clockErr error) string {
	if clockErr != nil || errors.Is(result.Err, powerinfo.ErrWallClockRollback) || errors.Is(result.Err, errPowerElapsedRollback) {
		return "power_clock_rollback"
	}
	if errors.Is(result.Err, context.Canceled) {
		return "power_sample_canceled"
	}
	if errors.Is(result.Err, context.DeadlineExceeded) {
		return "power_sample_deadline"
	}
	if result.Err != nil {
		return "power_sample_unavailable"
	}
	return "power_sample_stale"
}
