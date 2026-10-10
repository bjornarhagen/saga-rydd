package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Private fixtures preserve the actual limited marker type and backed outcomes.
// There is no public alternate accounting writer or native observer profile.
type cpuPeriodHooks struct {
	view     func(context.Context, *state.Store) (state.CPUChargeAdmissionState, error)
	activate func(context.Context, *state.Store, time.Time, state.CPUChargeLimits) (state.CPUChargeAdmissionState, error)
	recover  func(context.Context, *state.Store, time.Time) (state.CPUChargeState, state.CPUChargeAdmissionState, error)
	begin    func(context.Context, *state.Store, state.CPULimitedSessionStart) (state.CPULimitedSessionMarker, state.CPUChargeState, state.CPUChargeAdmissionState, error)
	sample   func(context.Context, *state.Store, state.CPULimitedSessionMarker, state.CPUSessionSample, bool) (state.CPUChargeState, state.CPUChargeAdmissionState, error)
	gap      func(context.Context, *state.Store, state.CPUTrackingGapRequest) (state.CPUChargeState, state.CPUChargeAdmissionState, error)
}

type cpuPeriodFence struct {
	start, limit int64
	elapsedDue   time.Time
	attached     bool
}
type cpuPeriodPolicy struct {
	saved     state.CPUChargeAdmissionState
	limits    state.CPUChargeLimits
	marker    state.CPULimitedSessionMarker
	hour, day cpuPeriodFence
	start     *state.CPULimitedSessionStart
	gap       *state.CPUTrackingGapRequest
}

func periodBlocked(slot state.CPUChargePeriod, limit int64, gap bool) bool {
	return limit > 0 && (gap || slot.PartialTracking || slot.BlockingUnknown || slot.AssignedCPUNS >= limit)
}
func (f *cpuPeriodFence) accept(slot state.CPUChargePeriod, limit int64, blocked bool, wall, elapsed time.Time) {
	if limit == 0 {
		*f = cpuPeriodFence{}
		return
	}
	if !blocked {
		return
	}
	if !f.attached || f.start != slot.StartNS || f.limit != limit {
		*f = cpuPeriodFence{start: slot.StartNS, limit: limit, attached: true,
			elapsedDue: maxTime(f.elapsedDue, elapsed.Add(max(time.Unix(0, slot.NextBoundaryNS).UTC().Sub(wall), 0)))}
	}
}
func (p *cpuPeriodPolicy) accept(saved state.CPUChargeAdmissionState, wall, elapsed time.Time) {
	p.saved = saved
	p.hour.accept(saved.Hour, p.limits.HourNS, periodBlocked(saved.Hour, p.limits.HourNS, saved.TrackingGapOpen), wall, elapsed)
	p.day.accept(saved.Day, p.limits.DayNS, periodBlocked(saved.Day, p.limits.DayNS, saved.TrackingGapOpen), wall, elapsed)
}

// An expired cached blocked slot permits selection and one real limited sample,
// never a receipt, claim or mutation. Only that sample can roll saved slots.
func (p *cpuPeriodPolicy) wait(wait *dispatchWait, wall, elapsed time.Time, selection bool) bool {
	if p == nil {
		return true
	}
	blocked := false
	for _, v := range []struct {
		slot  state.CPUChargePeriod
		limit int64
		fence cpuPeriodFence
		name  string
	}{
		{p.saved.Hour, p.limits.HourNS, p.hour, "cpu_hour_charge"},
		{p.saved.Day, p.limits.DayNS, p.day, "cpu_day_charge"},
	} {
		currentBlocked := periodBlocked(v.slot, v.limit, p.saved.TrackingGapOpen)
		reason := v.name + "_backoff"
		if currentBlocked {
			blocked = true
			if p.saved.TrackingGapOpen || v.slot.BlockingUnknown {
				reason = v.name + "_unknown"
			} else if v.slot.PartialTracking {
				reason = v.name + "_partial"
			}
			wait.wall(time.Unix(0, v.slot.NextBoundaryNS).UTC(), wall, reason)
		}
		// A wall rollover may clear the saved slot while a prior live elapsed
		// deadline remains. Saved coverage cannot erase that independent wait.
		if v.limit > 0 && v.fence.attached {
			wait.elapsed(v.fence.elapsedDue, elapsed, reason)
			if v.fence.elapsedDue.After(elapsed) {
				blocked = true
			}
		}
		if currentBlocked && wait.duration == 0 && wait.reason == "" {
			wait.reason = v.name + "_sample_required"
		}
	}
	return !blocked || selection && wait.duration == 0
}

// This query is run by the owning loop after its control listener is available.
// Actual activated state selects the protocol even when tracking is disabled.
func (p *cpuSessionPolicy) route(ctx context.Context, wall, elapsed time.Time) error {
	if !p.selectProtocol || p.routed {
		return nil
	}
	var a state.CPUChargeAdmissionState
	var err error
	if p.periodHooks != nil && p.periodHooks.view != nil {
		a, err = p.periodHooks.view(ctx, p.store)
	} else {
		a, err = p.store.CPUChargeAdmission(ctx)
	}
	if err != nil {
		return err
	}
	p.routed = true
	if a.Available {
		p.period = &cpuPeriodPolicy{limits: p.limits}
		p.period.accept(a, wall, elapsed)
		p.saved, err = p.store.CPUCharges(ctx)
		if err != nil {
			return err
		}
		if p.saved.Generation != a.CPUGeneration {
			return state.ErrCPUChargeAdmissionCorrupt
		}
		p.accept(p.saved, wall, elapsed, 0)
	} else if !p.tracking {
		p.bypass = true
		p.stage = 3
	}
	return nil
}

func (p *cpuSessionPolicy) openGap(ctx context.Context, wall, elapsed time.Time) error {
	a := p.period.saved
	var q state.CPUTrackingGapRequest
	if a.TrackingGapOpen && a.LatestGap != nil {
		q = a.LatestGap.Request // Exact historical retry does not renew its clock/revision.
	} else {
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		q = state.CPUTrackingGapRequest{ExpectedPolicyRevision: a.PolicyRevision, ExpectedGeneration: a.CPUGeneration, Nonce: hex.EncodeToString(nonce[:]), ObservedAt: wall}
	}
	p.period.gap = &q
	p.operation, p.operationAt = "tracking_gap", q.ObservedAt
	var saved state.CPUChargeState
	var next state.CPUChargeAdmissionState
	var err error
	if p.periodHooks != nil && p.periodHooks.gap != nil {
		saved, next, err = p.periodHooks.gap(ctx, p.store, q)
	} else {
		saved, next, err = p.store.OpenCPUTrackingGap(ctx, q)
	}
	if err != nil {
		return err
	}
	if !next.Available || !next.TrackingGapOpen {
		return state.ErrCPUChargeAdmissionCorrupt
	}
	p.saved = saved
	p.period.accept(next, wall, elapsed)
	p.bypass = true // Current explicit opt-out gates are inactive; old limits are history.
	p.stage = 3
	return nil
}
func (p *cpuSessionPolicy) activatePeriod(ctx context.Context, wall, elapsed time.Time) error {
	var a state.CPUChargeAdmissionState
	var err error
	p.operation, p.operationAt = "activate_period", wall
	if p.periodHooks != nil && p.periodHooks.activate != nil {
		a, err = p.periodHooks.activate(ctx, p.store, wall, p.limits)
	} else {
		a, err = p.store.ActivateCPUChargeAdmission(ctx, wall, p.limits)
	}
	if err != nil {
		return err
	}
	if !a.Available {
		return state.ErrCPUChargeAdmissionUnavailable
	}
	p.period = &cpuPeriodPolicy{limits: p.limits}
	p.period.accept(a, wall, elapsed)
	return nil
}

func cpuPeriodPublication(err error) bool {
	return errors.Is(err, state.ErrCPUChargeAdmissionPublication) || errors.Is(err, state.ErrCPUChargesPublication)
}
