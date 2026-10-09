package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// These operation hooks are private deterministic fixtures. Production always
// calls the one existing writer and observes cumulative native SELF CPU.
type cpuSessionHooks struct {
	activate func(context.Context, *state.Store, time.Time) (state.CPUChargeState, error)
	recover  func(context.Context, *state.Store, time.Time) (state.CPUChargeState, error)
	begin    func(context.Context, *state.Store, state.CPUSessionStart) (state.CPUSessionMarker, state.CPUChargeState, error)
	sample   func(context.Context, *state.Store, state.CPUSessionMarker, state.CPUSessionSample, bool) (state.CPUChargeState, error)
}

// One owning loop holds this policy. Its marker cannot survive Store reopen.
// Saved wall evidence and live elapsed debt are distinct admission predicates.
type cpuSessionPolicy struct {
	store           *state.Store
	instance        string
	observe         func() (time.Duration, error)
	wall, elapsed   func() time.Time
	hooks           *cpuSessionHooks
	stage           int
	saved           state.CPUChargeState
	marker          state.CPUSessionMarker
	start           *state.CPUSessionStart
	sample          *state.CPUSessionSample
	operation       string
	operationAt     time.Time
	wallHigh        time.Time
	elapsedHigh     time.Time
	elapsedDue      time.Time
	observedElapsed time.Time
	attached        bool
	refused         error
}

func (p *cpuSessionPolicy) fail(err error) {
	if err != nil && p.refused == nil {
		p.refused = err
	}
}

// Every live clock observation retains high-water. No status/pause callback
// samples native CPU, replaces a marker or reanchors an existing deadline.
func (p *cpuSessionPolicy) clocks() (time.Time, time.Time, error) {
	wall, elapsed := p.wall().Round(0).UTC(), p.elapsed()
	var err error
	if n := wall.UnixNano(); n <= 0 || !time.Unix(0, n).UTC().Equal(wall) || elapsed.IsZero() {
		err = state.ErrCPUChargesInvalid
	} else if !p.wallHigh.IsZero() && wall.Before(p.wallHigh) {
		err = state.ErrCPUChargesClockRollback
	} else if !p.elapsedHigh.IsZero() && elapsed.Before(p.elapsedHigh) {
		err = state.ErrCPUChargesUnknown
	}
	if wall.After(p.wallHigh) {
		p.wallHigh = wall
	}
	if elapsed.After(p.elapsedHigh) {
		p.elapsedHigh = elapsed
	}
	return wall, elapsed, err
}

func (p *cpuSessionPolicy) accept(saved state.CPUChargeState, wall, elapsed time.Time, intrinsic time.Duration) {
	if !p.attached {
		// Cold attachment projects inherited debt exactly once, before read
		// latency. A subsequent generation with the same deadline retains it.
		if saved.NextAllowedAt != nil {
			p.elapsedDue = elapsed.Add(max(saved.NextAllowedAt.Sub(wall), 0))
		}
		p.attached = true
	}
	if intrinsic > 0 {
		p.elapsedDue = maxTime(p.elapsedDue, elapsed.Add(intrinsic))
	}
	p.saved = saved
}

func (p *cpuSessionPolicy) wait(wait *dispatchWait) bool {
	if p == nil {
		return true
	}
	wall, elapsed, err := p.clocks()
	p.fail(err)
	if p.refused != nil {
		wait.reason = "cpu_session_refused"
		if errors.Is(p.refused, state.ErrCPUChargesPublication) {
			wait.reason = "cpu_session_outcome_unknown"
		} else if errors.Is(p.refused, state.ErrCPUChargesClockRollback) {
			wait.reason = "cpu_session_clock_refused"
		} else if errors.Is(p.refused, state.ErrCPUChargesUnknown) {
			wait.reason = "cpu_session_unknown"
		}
		return false
	}
	if p.saved.ClockHighWater != nil {
		wait.wall(*p.saved.ClockHighWater, wall, "cpu_session_clock_wait")
	}
	if p.stage < 3 {
		if wait.duration == 0 {
			wait.reason = "cpu_session_startup"
		}
		return false
	}
	if p.saved.NextAllowedAt != nil {
		wait.wall(*p.saved.NextAllowedAt, wall, "cpu_session_backoff")
	}
	wait.elapsed(p.elapsedDue, elapsed, "cpu_session_backoff")
	return wait.duration == 0
}

func (p *cpuSessionPolicy) startup(ctx context.Context) {
	if p == nil || p.refused != nil || p.stage >= 3 {
		return
	}
	wall, elapsed, err := p.clocks()
	if err != nil {
		p.fail(err)
		return
	}
	if p.saved.ClockHighWater != nil && wall.Before(*p.saved.ClockHighWater) {
		return // The event-loop wall fence wakes once; no native polling.
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var saved state.CPUChargeState
	switch p.stage {
	case 0:
		p.operation, p.operationAt = "activate", wall
		if p.hooks != nil && p.hooks.activate != nil {
			saved, err = p.hooks.activate(callCtx, p.store, wall)
		} else {
			saved, err = p.store.ActivateCPUCharges(callCtx, wall)
		}
		if err == nil {
			p.accept(saved, wall, elapsed, 0)
		}
	case 1:
		p.operation, p.operationAt = "recover", wall
		wasActive := p.saved.Session != nil && p.saved.Session.Status == "active"
		if p.hooks != nil && p.hooks.recover != nil {
			saved, err = p.hooks.recover(callCtx, p.store, wall)
		} else {
			saved, err = p.store.RecoverCPUSession(callCtx, wall)
		}
		if err == nil {
			intrinsic := time.Duration(0)
			if wasActive {
				intrinsic = state.CPUChargesUnknownDelay
			}
			p.accept(saved, wall, elapsed, intrinsic)
		}
	case 2:
		cpu, observationErr := p.observe()
		// Prefix repayment begins after the actual native observation, never
		// at process birth, startup, or before a slow observation callback.
		wall, elapsed, err = p.clocks()
		if err != nil {
			p.fail(err)
			return
		}
		var nonce [32]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			p.fail(err)
			return
		}
		request := state.CPUSessionStart{ExpectedGeneration: p.saved.Generation, Nonce: hex.EncodeToString(nonce[:]), Instance: p.instance, ObservedAt: wall}
		if observationErr != nil {
			request.Reason = "cpu_observation_unavailable"
		} else {
			nanos := int64(cpu)
			request.SelfCPUNS = &nanos
		}
		p.start = &request
		p.operation, p.operationAt = "begin", wall
		if p.hooks != nil && p.hooks.begin != nil {
			p.marker, saved, err = p.hooks.begin(callCtx, p.store, request)
		} else {
			p.marker, saved, err = p.store.BeginCPUSession(callCtx, request)
		}
		if err == nil {
			if p.marker.Generation() == 0 || saved.Session == nil || saved.Status != "active" {
				err = state.ErrCPUChargesStale
			} else {
				p.accept(saved, wall, elapsed, time.Duration(saved.Session.LastBackoffNS))
				p.observedElapsed = elapsed
			}
		}
	}
	if err != nil {
		p.fail(err)
		return
	}
	p.stage++
}

// sampleAtBoundary is called only after otherwise-admissible work selection,
// completed accounting, or a known-safe graceful finish. No native idle poll.
func (p *cpuSessionPolicy) sampleAtBoundary(ctx context.Context, finish bool) bool {
	if p == nil {
		return true
	}
	if p.refused != nil || p.stage != 3 || p.saved.Status != "active" || p.saved.Session == nil {
		return false
	}
	if err := ctx.Err(); err != nil {
		p.fail(err)
		return false
	}
	if p.saved.Session.LastOrdinal == math.MaxInt64 {
		p.fail(state.ErrCPUChargesOverflow)
		return false
	}
	cpu, observationErr := p.observe()
	wall, elapsed, clockErr := p.clocks()
	request := state.CPUSessionSample{Ordinal: p.saved.Session.LastOrdinal + 1, ObservedAt: wall}
	if observationErr != nil {
		request.Reason = "cpu_observation_unavailable"
	} else {
		nanos := int64(cpu)
		request.SelfCPUNS = &nanos
	}
	if duration := elapsed.Sub(p.observedElapsed); clockErr == nil && duration > 0 {
		nanos := int64(duration)
		request.ElapsedNS = &nanos
	}
	p.sample = &request // Retained unchanged on every uncertain outcome.
	p.operation, p.operationAt = "sample", wall
	if finish {
		p.operation = "finish"
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var saved state.CPUChargeState
	var err error
	if p.hooks != nil && p.hooks.sample != nil {
		saved, err = p.hooks.sample(callCtx, p.store, p.marker, request, finish)
	} else if finish {
		saved, err = p.store.FinishCPUSession(callCtx, p.marker, request)
	} else {
		saved, err = p.store.SampleCPUSession(callCtx, p.marker, request)
	}
	if err == nil && clockErr == nil {
		p.accept(saved, wall, elapsed, time.Duration(saved.Session.LastBackoffNS))
		p.observedElapsed = elapsed
		return true
	}
	p.fail(errors.Join(err, clockErr))
	return false
}

func (p *cpuSessionPolicy) error() error {
	if p == nil || p.refused == nil {
		return nil
	}
	if p.start != nil {
		if p.start.ExpectedGeneration == math.MaxInt64 {
			return fmt.Errorf("worker CPU session expected generation %d nonce %s (new work refused; inspect saved CPU charges before explicit restart): %w", p.start.ExpectedGeneration, p.start.Nonce, p.refused)
		}
		return fmt.Errorf("worker CPU session generation %d nonce %s (new work refused; inspect saved CPU charges before explicit restart): %w", p.start.ExpectedGeneration+1, p.start.Nonce, p.refused)
	}
	if p.operation != "" {
		return fmt.Errorf("worker CPU session %s request at %s (new work refused; inspect saved CPU charges before explicit restart): %w", p.operation, p.operationAt.Format(time.RFC3339Nano), p.refused)
	}
	return fmt.Errorf("worker CPU session tracking (new work refused; inspect saved CPU charges before explicit restart): %w", p.refused)
}

func (p *cpuSessionPolicy) finish() error {
	if p == nil {
		return nil
	}
	if p.refused == nil && p.stage == 3 && p.saved.Status == "active" {
		// Stop has already joined the handler and all existing settlements.
		// Cancellation of Run cannot discard this finite graceful accounting.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		p.sampleAtBoundary(ctx, true)
		cancel()
	}
	return p.error()
}
