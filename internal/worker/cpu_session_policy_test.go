package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type sessionClock struct{ wall, elapsed time.Time }

func (c *sessionClock) advance(d time.Duration) { c.wall = c.wall.Add(d); c.elapsed = c.elapsed.Add(d) }

func sessionPolicyFixture(t *testing.T, cpu func() (time.Duration, error)) (*cpuSessionPolicy, *sessionClock) {
	t.Helper()
	dir, _ := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	c := &sessionClock{wall: time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC), elapsed: time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)}
	p := &cpuSessionPolicy{store: w, instance: strings.Repeat("a", 32), observe: cpu, wall: func() time.Time { return c.wall }, elapsed: func() time.Time { return c.elapsed }}
	return p, c
}

func sessionStartup(t *testing.T, p *cpuSessionPolicy) {
	t.Helper()
	for i := 0; i < 3; i++ {
		p.startup(context.Background())
	}
	if p.stage != 3 || p.refused != nil {
		t.Fatal(p.stage, p.error())
	}
}

func TestCPUSessionPrefixUsesPostObservationAnchorAndNoAgeCredit(t *testing.T) {
	p, clock := sessionPolicyFixture(t, nil)
	p.observe = func() (time.Duration, error) { clock.advance(5 * time.Minute); return time.Millisecond, nil }
	before := clock.elapsed
	sessionStartup(t, p)
	if want := before.Add(5*time.Minute + 100*time.Millisecond); !p.elapsedDue.Equal(want) {
		t.Fatal(p.elapsedDue, want)
	}
	wait := dispatchWait{}
	if p.wait(&wait) || wait.duration != 100*time.Millisecond {
		t.Fatal("prefix used pre-observation age", wait)
	}
	clock.wall = clock.wall.Add(time.Hour)
	wait = dispatchWait{}
	if p.wait(&wait) || wait.duration != 100*time.Millisecond {
		t.Fatal("wall jump erased prospective elapsed debt", wait)
	}
	clock.elapsed = clock.elapsed.Add(100 * time.Millisecond)
	wait = dispatchWait{}
	if !p.wait(&wait) {
		t.Fatal(wait, p.error())
	}
}

func TestCPUSessionRetainsInheritedAnchorAndAddsHiddenFreshDebt(t *testing.T) {
	p, clock := sessionPolicyFixture(t, func() (time.Duration, error) { return 0, nil })
	due := clock.wall.Add(time.Hour)
	saved := state.CPUChargeState{NextAllowedAt: &due}
	p.accept(saved, clock.wall, clock.elapsed, 0)
	initial := p.elapsedDue
	clock.advance(time.Minute)
	p.accept(saved, clock.wall, clock.elapsed, time.Second)
	if !p.elapsedDue.Equal(initial) {
		t.Fatal("same inherited deadline reanchored", p.elapsedDue, initial)
	}
	clock.wall = clock.wall.Add(10 * time.Minute)
	clock.elapsed = clock.elapsed.Add(2 * time.Hour)
	p.accept(saved, clock.wall, clock.elapsed, 200*time.Millisecond)
	if !p.elapsedDue.Equal(clock.elapsed.Add(200 * time.Millisecond)) {
		t.Fatal("new debt hidden by older saved deadline", p.elapsedDue)
	}
}

func TestCPUSessionSampleAnchorPrecedesPublicationLatency(t *testing.T) {
	value := time.Millisecond
	p, clock := sessionPolicyFixture(t, func() (time.Duration, error) { return value, nil })
	sessionStartup(t, p)
	clock.advance(100 * time.Millisecond)
	value = 2 * time.Millisecond
	p.hooks = &cpuSessionHooks{sample: func(ctx context.Context, w *state.Store, marker state.CPUSessionMarker, sample state.CPUSessionSample, finish bool) (state.CPUChargeState, error) {
		got, err := w.SampleCPUSession(ctx, marker, sample)
		clock.advance(2 * time.Minute)
		return got, err
	}}
	anchor := clock.elapsed
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if !p.observedElapsed.Equal(anchor) {
		t.Fatal("publication latency became observation anchor", p.observedElapsed, anchor)
	}
	if p.saved.Session.LastSample == nil || *p.saved.Session.LastSample.ElapsedNS != int64(100*time.Millisecond) {
		t.Fatal(p.saved.Session)
	}
}

func TestCPUSessionUnknownAndClockFailuresLatchWithoutNewGeneration(t *testing.T) {
	for _, failure := range []string{"unavailable", "regressed", "elapsed", "wall"} {
		t.Run(failure, func(t *testing.T) {
			value, unavailable := time.Millisecond, false
			calls := 0
			p, clock := sessionPolicyFixture(t, func() (time.Duration, error) {
				calls++
				if unavailable {
					return 0, errors.New("fixture native observation failure")
				}
				return value, nil
			})
			sessionStartup(t, p)
			clock.advance(time.Second)
			switch failure {
			case "unavailable":
				unavailable = true
			case "regressed":
				value = 0
			case "elapsed":
				clock.elapsed = clock.elapsed.Add(-2 * time.Second)
			case "wall":
				clock.wall = clock.wall.Add(-2 * time.Second)
			}
			if p.sampleAtBoundary(context.Background(), false) || p.refused == nil {
				t.Fatal("failed observation admitted", p.saved)
			}
			count := calls
			p.startup(context.Background())
			p.sampleAtBoundary(context.Background(), false)
			_ = p.finish()
			if calls != count || p.marker.Generation() != 1 {
				t.Fatal("closed tracking automatically restarted", calls, count)
			}
			saved, err := p.store.CPUCharges(context.Background())
			if err != nil || saved.Generation != 1 || saved.Status != "unknown" || saved.ChargedCPUNS != int64(time.Millisecond) {
				t.Fatal(saved, err)
			}
		})
	}
}

func TestCPUSessionUncertainBeginRetainsCandidateAndNeverRemints(t *testing.T) {
	calls := 0
	p, _ := sessionPolicyFixture(t, func() (time.Duration, error) { calls++; return 0, nil })
	p.hooks = &cpuSessionHooks{begin: func(ctx context.Context, w *state.Store, start state.CPUSessionStart) (state.CPUSessionMarker, state.CPUChargeState, error) {
		marker, saved, err := w.BeginCPUSession(ctx, start)
		if err != nil {
			return marker, saved, err
		}
		return marker, saved, errors.Join(state.ErrCPUChargesPublication, context.Canceled)
	}}
	for i := 0; i < 3; i++ {
		p.startup(context.Background())
	}
	if !errors.Is(p.error(), state.ErrCPUChargesPublication) || p.marker.Generation() != 1 || p.start == nil {
		t.Fatal(p.error(), p.marker)
	}
	frozen := *p.start
	for i := 0; i < 10; i++ {
		p.startup(context.Background())
		p.sampleAtBoundary(context.Background(), false)
	}
	if calls != 1 || p.start.Nonce != frozen.Nonce || !p.start.ObservedAt.Equal(frozen.ObservedAt) {
		t.Fatal("uncertain request changed or retried", calls, p.start)
	}
	if p.finish() == nil {
		t.Fatal("uncertain finish became success")
	}
	saved, err := p.store.CPUCharges(context.Background())
	if err != nil || saved.Generation != 1 || saved.Status != "active" {
		t.Fatal(saved, err)
	}
}

func TestCPUSessionStartupSavedClockFenceDoesNotPollOrMutate(t *testing.T) {
	p, clock := sessionPolicyFixture(t, func() (time.Duration, error) { t.Fatal("CPU polled before wall fence"); return 0, nil })
	future := clock.wall.Add(time.Hour)
	if _, err := p.store.ActivateCPUCharges(context.Background(), future); err != nil {
		t.Fatal(err)
	}
	p.startup(context.Background())
	if p.stage != 1 || p.refused != nil {
		t.Fatal(p.stage, p.error())
	}
	for i := 0; i < 4; i++ {
		p.startup(context.Background())
		wait := dispatchWait{}
		if p.wait(&wait) || wait.duration != time.Hour {
			t.Fatal(wait)
		}
	}
	saved, err := p.store.CPUCharges(context.Background())
	if err != nil || saved.Generation != 0 || !saved.ClockHighWater.Equal(future) {
		t.Fatal(saved, err)
	}
	clock.advance(time.Hour)
	p.startup(context.Background())
	if p.stage != 2 || p.refused != nil {
		t.Fatal(p.stage, p.error())
	}
}

func TestCPUSessionUncertainStartupRetainsExactOperationAfterControlClocks(t *testing.T) {
	for _, operation := range []string{"activate", "recover"} {
		t.Run(operation, func(t *testing.T) {
			p, clock := sessionPolicyFixture(t, func() (time.Duration, error) { t.Fatal("uncertain startup polled native CPU"); return 0, nil })
			if operation == "recover" {
				if _, err := p.store.ActivateCPUCharges(context.Background(), clock.wall); err != nil {
					t.Fatal(err)
				}
				zero := int64(0)
				if _, _, err := p.store.BeginCPUSession(context.Background(), state.CPUSessionStart{ExpectedGeneration: 0, Nonce: strings.Repeat("b", 64), Instance: p.instance, ObservedAt: clock.wall, SelfCPUNS: &zero}); err != nil {
					t.Fatal(err)
				}
				p.startup(context.Background())
			}
			calls := 0
			p.hooks = &cpuSessionHooks{}
			if operation == "activate" {
				p.hooks.activate = func(ctx context.Context, w *state.Store, at time.Time) (state.CPUChargeState, error) {
					calls++
					saved, err := w.ActivateCPUCharges(ctx, at)
					if err == nil {
						err = state.ErrCPUChargesPublication
					}
					return saved, err
				}
			} else {
				p.hooks.recover = func(ctx context.Context, w *state.Store, at time.Time) (state.CPUChargeState, error) {
					calls++
					saved, err := w.RecoverCPUSession(ctx, at)
					if err == nil {
						err = state.ErrCPUChargesPublication
					}
					return saved, err
				}
			}
			at := clock.wall
			p.startup(context.Background())
			for i := 0; i < 4; i++ {
				clock.advance(time.Minute)
				p.wait(&dispatchWait{})
				p.startup(context.Background())
			}
			if calls != 1 || p.operation != operation || !p.operationAt.Equal(at) || !errors.Is(p.error(), state.ErrCPUChargesPublication) || !strings.Contains(p.error().Error(), at.Format(time.RFC3339Nano)) {
				t.Fatal("startup uncertainty lost exact request", calls, p.operation, p.operationAt, p.error())
			}
			saved, err := p.store.CPUCharges(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if operation == "activate" && saved.Generation != 0 || operation == "recover" && (saved.Generation != 1 || saved.RecoveredSessions != 1) {
				t.Fatal("startup replayed/new generation", saved)
			}
		})
	}
}
