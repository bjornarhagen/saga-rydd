package worker

import (
	"context"
	"errors"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"strings"
	"testing"
	"time"
)

func periodPolicyFixture(t *testing.T, tracking bool, limits state.CPUChargeLimits) (*cpuSessionPolicy, *sessionClock) {
	t.Helper()
	p, c := sessionPolicyFixture(t, func() (time.Duration, error) { return 0, nil })
	p.selectProtocol, p.tracking, p.limits = true, tracking, limits
	return p, c
}
func TestCPUPeriodPolicyActualProtocolAndDistinctLimitedMarker(t *testing.T) {
	for _, v := range []struct {
		name     string
		tracking bool
		limits   state.CPUChargeLimits
		version  int
	}{
		{"untracked", false, state.CPUChargeLimits{}, 14}, {"legacy", true, state.CPUChargeLimits{}, 15}, {"limited", true, state.CPUChargeLimits{HourNS: 1}, 16},
	} {
		t.Run(v.name, func(t *testing.T) {
			p, _ := periodPolicyFixture(t, v.tracking, v.limits)
			sessionStartup(t, p)
			summary, err := p.store.Summary(context.Background())
			if err != nil || summary.Schema != v.version {
				t.Fatal(summary, err)
			}
			if v.version == 16 {
				if p.period == nil || p.period.marker.Generation() != 1 || p.marker.Generation() != 0 {
					t.Fatal("limited authority was weakened", p)
				}
			}
			if v.version == 14 && !p.bypass {
				t.Fatal("untracked routed to observation")
			}
		})
	}
}
func TestCPUPeriodPolicyExpiredEvidenceAllowsSelectionButNotDispatch(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: int64(time.Second), DayNS: int64(time.Second)})
	sessionStartup(t, p)
	if p.period == nil || !p.period.saved.Hour.PartialTracking || !p.period.saved.Day.PartialTracking {
		t.Fatal(p.period)
	}
	wait := dispatchWait{}
	if p.selectionWait(&wait) || wait.reason != "cpu_day_charge_partial" || wait.duration != 14*time.Hour {
		t.Fatal(wait)
	}
	// Wall forward cannot erase the independently anchored elapsed wait.
	c.wall = c.wall.Add(24 * time.Hour)
	wait = dispatchWait{}
	if p.selectionWait(&wait) || wait.duration != 14*time.Hour {
		t.Fatal(wait)
	}
	c.elapsed = c.elapsed.Add(14 * time.Hour)
	wait = dispatchWait{}
	if !p.selectionWait(&wait) {
		t.Fatal(wait, p.error())
	}
	wait = dispatchWait{}
	if p.wait(&wait) || wait.duration != 0 {
		t.Fatal("cached expired evidence granted dispatch", wait)
	}
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	wait = dispatchWait{}
	if !p.wait(&wait) || p.period.saved.Day.PartialTracking || p.period.saved.Hour.PartialTracking {
		t.Fatal(wait, p.period.saved)
	}
}
func TestCPUPeriodPolicyIndependentAnchorsNeverReanchorOnViews(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: 1, DayNS: 1})
	sessionStartup(t, p)
	hour, day := p.period.hour.elapsedDue, p.period.day.elapsedDue
	c.advance(time.Minute)
	p.period.accept(p.period.saved, c.wall, c.elapsed)
	if !p.period.hour.elapsedDue.Equal(hour) || !p.period.day.elapsedDue.Equal(day) {
		t.Fatal("same evidence reanchored")
	}
	c.advance(59 * time.Minute)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.PartialTracking || !p.period.saved.Day.PartialTracking || !p.period.day.elapsedDue.Equal(day) {
		t.Fatal(p.period)
	}
	// A newly exceeded hour gets a new hour anchor without altering day evidence.
	p.observe = func() (time.Duration, error) { return time.Nanosecond, nil }
	c.advance(time.Second)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if !p.period.hour.elapsedDue.After(hour) || !p.period.day.elapsedDue.Equal(day) {
		t.Fatal(p.period)
	}
}
func TestCPUPeriodPolicyGapOnlyUsesSavedExactRequestAndNoNative(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: 1})
	sessionStartup(t, p)
	saved := p.period.saved
	q := state.CPUTrackingGapRequest{ExpectedPolicyRevision: saved.PolicyRevision, ExpectedGeneration: saved.CPUGeneration, Nonce: strings.Repeat("c", 64), ObservedAt: c.wall}
	if _, _, err := p.store.OpenCPUTrackingGap(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	p2 := &cpuSessionPolicy{store: p.store, instance: strings.Repeat("b", 32), observe: func() (time.Duration, error) { t.Fatal("gap sampled native SESSION CPU"); return 0, nil }, wall: p.wall, elapsed: p.elapsed, selectProtocol: true}
	p2.startup(context.Background())
	if p2.refused != nil || p2.stage != 3 || !p2.bypass || p2.period == nil || p2.period.marker.Generation() != 0 {
		t.Fatal(p2.error(), p2)
	}
	if *p2.period.gap != q {
		t.Fatal("open gap was renewed", p2.period.gap, q)
	}
	before, _ := p.store.CPUChargeAdmission(context.Background())
	c.advance(time.Hour)
	p2.wait(&dispatchWait{})
	p2.sampleAtBoundary(context.Background(), false)
	_ = p2.finish()
	after, _ := p.store.CPUChargeAdmission(context.Background())
	if !after.ClockHighWater.Equal(*before.ClockHighWater) || after.PolicyRevision != before.PolicyRevision {
		t.Fatal(before, after)
	}
}
func TestCPUPeriodPolicyLimitedUncertaintyClockAndOutcomeMarkerRefuse(t *testing.T) {
	for _, failure := range []string{"uncertain", "zero_marker", "elapsed"} {
		t.Run(failure, func(t *testing.T) {
			p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: 1})
			if failure != "elapsed" {
				p.periodHooks = &cpuPeriodHooks{begin: func(ctx context.Context, w *state.Store, q state.CPULimitedSessionStart) (state.CPULimitedSessionMarker, state.CPUChargeState, state.CPUChargeAdmissionState, error) {
					marker, c, a, err := w.BeginCPUSessionLimited(ctx, q)
					if err != nil {
						return marker, c, a, err
					}
					if failure == "uncertain" {
						return marker, c, a, errors.Join(state.ErrCPUChargeAdmissionPublication, context.Canceled)
					}
					return state.CPULimitedSessionMarker{}, c, a, nil
				}}
			}
			for i := 0; i < 3; i++ {
				p.startup(context.Background())
			}
			if failure == "elapsed" {
				c.elapsed = c.elapsed.Add(-time.Second)
				p.selectionWait(&dispatchWait{})
			}
			if p.refused == nil {
				t.Fatal("failed limited authority admitted")
			}
			generation := p.saved.Generation
			for i := 0; i < 3; i++ {
				p.startup(context.Background())
				p.sampleAtBoundary(context.Background(), false)
			}
			if p.saved.Generation != generation || p.marker.Generation() != 0 {
				t.Fatal("reminted or bare marker")
			}
		})
	}
}

func TestCPUPeriodPolicyIndependentHourDayCapsRetainFullCharges(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: int64(time.Second), DayNS: int64(2 * time.Second)})
	value := time.Duration(0)
	p.observe = func() (time.Duration, error) { return value, nil }
	sessionStartup(t, p)
	c.advance(14 * time.Hour)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	value = time.Second
	c.advance(2 * time.Minute)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.AssignedCPUNS != int64(time.Second) || p.period.saved.Day.AssignedCPUNS != int64(time.Second) {
		t.Fatal(p.period.saved)
	}
	w := dispatchWait{}
	if p.selectionWait(&w) || w.reason != "cpu_hour_charge_backoff" {
		t.Fatal("hour equality failed admission", w)
	}
	c.advance(time.Hour)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.AssignedCPUNS != 0 || p.period.saved.Day.AssignedCPUNS != int64(time.Second) {
		t.Fatal("hour rollover cleared day", p.period.saved)
	}
	value = 3 * time.Second
	c.advance(4 * time.Minute)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.AssignedCPUNS != int64(2*time.Second) || p.period.saved.Day.AssignedCPUNS != int64(3*time.Second) || p.saved.ChargedCPUNS != int64(3*time.Second) {
		t.Fatal("overshoot was dropped", p.period.saved, p.saved)
	}
	w = dispatchWait{}
	if p.selectionWait(&w) || w.reason != "cpu_day_charge_backoff" {
		t.Fatal("day gate was coupled to hour", w)
	}
}
func TestCPUPeriodPolicyGapReenableAndChangedProfileRemainPartial(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: 1})
	sessionStartup(t, p)
	gap := &cpuSessionPolicy{store: p.store, instance: strings.Repeat("b", 32), observe: func() (time.Duration, error) { t.Fatal("gap session probe"); return 0, nil }, wall: p.wall, elapsed: p.elapsed, selectProtocol: true}
	gap.startup(context.Background())
	if gap.refused != nil || !gap.bypass {
		t.Fatal(gap.error())
	}
	before := gap.period.saved
	again := &cpuSessionPolicy{store: p.store, instance: strings.Repeat("c", 32), observe: func() (time.Duration, error) { return 0, nil }, wall: p.wall, elapsed: p.elapsed, selectProtocol: true, tracking: true, limits: state.CPUChargeLimits{DayNS: 2}}
	for i := 0; i < 3; i++ {
		again.startup(context.Background())
	}
	if again.refused != nil || again.stage != 3 || again.period == nil || again.period.marker.Generation() != 2 || again.period.marker.PolicyRevision() != before.PolicyRevision+1 {
		t.Fatal(again.error(), again.period)
	}
	if again.period.saved.TrackingGapOpen || !again.period.saved.Hour.PartialTracking || !again.period.saved.Day.PartialTracking || again.period.saved.Limits != (state.CPUChargeLimits{DayNS: 2}) {
		t.Fatal(again.period.saved)
	}
	c.advance(time.Second)
	w := dispatchWait{}
	if again.selectionWait(&w) || w.reason != "cpu_day_charge_unknown" {
		t.Fatal("coverage/debt regained on opt-in", w)
	}
}

func TestCPUPeriodPolicyWallRollCannotEraseOrShortenLiveFence(t *testing.T) {
	p, c := periodPolicyFixture(t, true, state.CPUChargeLimits{HourNS: int64(time.Second)})
	value := time.Duration(0)
	p.observe = func() (time.Duration, error) { return value, nil }
	sessionStartup(t, p)
	original := p.period.hour.elapsedDue
	c.wall = c.wall.Add(2 * time.Hour)
	c.elapsed = c.elapsed.Add(time.Millisecond)
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.PartialTracking || !p.period.hour.elapsedDue.Equal(original) {
		t.Fatal(p.period)
	}
	w := dispatchWait{}
	if p.selectionWait(&w) || w.duration != time.Hour-time.Millisecond {
		t.Fatal("saved wall rollover erased live fence", w)
	}
	c.wall = c.wall.Add(10 * time.Minute)
	c.elapsed = c.elapsed.Add(time.Millisecond)
	value = time.Second
	if !p.sampleAtBoundary(context.Background(), false) {
		t.Fatal(p.error())
	}
	if p.period.saved.Hour.AssignedCPUNS != int64(time.Second) || !p.period.hour.elapsedDue.Equal(original) {
		t.Fatal("new blocked slot shortened earlier elapsed fence", p.period)
	}
	w = dispatchWait{}
	if p.wait(&w) || w.duration != time.Hour-2*time.Millisecond {
		t.Fatal(w)
	}
}
