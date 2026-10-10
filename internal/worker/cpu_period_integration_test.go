package worker

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type periodWorkerClock struct{ wall, elapsed atomic.Int64 }

func newPeriodWorkerClock() *periodWorkerClock {
	c := &periodWorkerClock{}
	n := time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	c.wall.Store(n)
	c.elapsed.Store(n)
	return c
}
func (c *periodWorkerClock) advance(d time.Duration) { c.wall.Add(int64(d)); c.elapsed.Add(int64(d)) }
func (c *periodWorkerClock) wallNow() time.Time      { return time.Unix(0, c.wall.Load()).UTC() }
func (c *periodWorkerClock) elapsedNow() time.Time   { return time.Unix(0, c.elapsed.Load()) }

func periodSaved(t *testing.T, dir string) state.CPUChargeAdmissionState {
	t.Helper()
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a, err := r.CPUChargeAdmission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func activatePeriodWorkerFixture(t *testing.T, dir string, at time.Time, active bool) {
	t.Helper()
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = w.ActivateCPUCharges(context.Background(), at); err != nil {
		t.Fatal(err)
	}
	if _, err = w.ActivateCPUChargeAdmission(context.Background(), at, state.CPUChargeLimits{HourNS: int64(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if active {
		zero := int64(0)
		_, _, _, err = w.BeginCPUSessionLimited(context.Background(), state.CPULimitedSessionStart{Start: state.CPUSessionStart{Nonce: strings.Repeat("d", 64), Instance: strings.Repeat("e", 32), ObservedAt: at, SelfCPUNS: &zero}, Limits: state.CPUChargeLimits{HourNS: int64(time.Second)}})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestCPUPeriodWorkerPartialBlocksSetupButControlsRemainAvailable(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	cfg.Scan.CPUChargeSecondsPerHour = 1
	c := newPeriodWorkerClock()
	var observations atomic.Int64
	ready, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow,
		cpuObserve:        func() (time.Duration, error) { c.advance(time.Millisecond); return 0, nil },
		cpuSessionObserve: func() (time.Duration, error) { observations.Add(1); c.advance(time.Millisecond); return 0, nil }})
	if ready.InventoryMode != nil {
		t.Fatal("unaccounted setup before listener", ready)
	}
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_hour_charge_partial" })
	before := periodSaved(t, dir)
	for i := 0; i < 3; i++ {
		s := control(t, dir, "pause")
		if s.InventoryMode != nil || s.ActiveJob != 0 {
			t.Fatal(s)
		}
		control(t, dir, "status")
		control(t, dir, "resume")
	}
	if observations.Load() != 1 || before.PolicyRevision != 1 || before.CPUOrdinal != 0 {
		t.Fatal(observations.Load(), before)
	}
	// A wall jump alone must not shorten the original live elapsed anchor.
	c.wall.Add(int64(2 * time.Hour))
	control(t, dir, "resume")
	s := control(t, dir, "status")
	if s.InventoryMode != nil || observations.Load() != 1 {
		t.Fatal(s, observations.Load())
	}
	c.elapsed.Add(int64(2 * time.Hour))
	control(t, dir, "resume")
	waitUntil(t, func() bool { return control(t, dir, "status").InventoryMode != nil })
	waitUntil(t, func() bool { return periodSaved(t, dir).CPUOrdinal >= 6 })
	if observations.Load() != 7 {
		t.Fatal("three eligible setup units missing paired boundaries", observations.Load())
	}
	after := periodSaved(t, dir)
	if after.Hour.PartialTracking || !after.Day.PartialTracking || after.CPUOrdinal != 6 {
		t.Fatal(after)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	if got := sessionSaved(t, dir); got.Status != "finished" || got.UnknownTailSessions != 1 {
		t.Fatal(got)
	}
}
func TestCPUPeriodWorkerActual16RoutesZeroTrackingAndGapOptOut(t *testing.T) {
	for _, tracking := range []bool{true, false} {
		t.Run(map[bool]string{true: "tracked_zero", false: "gap_only"}[tracking], func(t *testing.T) {
			dir, cfg := sessionWorkerFixture(t)
			c := newPeriodWorkerClock()
			activatePeriodWorkerFixture(t, dir, c.wallNow(), false)
			cfg.Scan.CPUSessionCharges = tracking
			var sessions, feedback atomic.Int64
			hooks := &cpuSessionHooks{activate: func(context.Context, *state.Store, time.Time) (state.CPUChargeState, error) {
				t.Error("bare API reached actual16")
				return state.CPUChargeState{}, state.ErrCPUChargeAdmissionPolicyRequired
			}}
			_, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow, cpuSessionHooks: hooks,
				cpuSessionObserve: func() (time.Duration, error) { sessions.Add(1); c.advance(time.Millisecond); return 0, nil },
				cpuObserve:        func() (time.Duration, error) { feedback.Add(1); c.advance(time.Millisecond); return 0, nil }})
			waitUntil(t, func() bool { return control(t, dir, "status").InventoryMode != nil })
			a := periodSaved(t, dir)
			if tracking {
				waitUntil(t, func() bool { return periodSaved(t, dir).CPUOrdinal == 6 })
				a = periodSaved(t, dir)
				if a.TrackingGapOpen || a.Limits != (state.CPUChargeLimits{}) || a.CPUGeneration != 1 || sessions.Load() != 7 {
					t.Fatal(a, sessions.Load())
				}
			} else {
				if !a.TrackingGapOpen || a.Limits.HourNS != int64(time.Second) || a.CPUGeneration != 0 || sessions.Load() != 0 || feedback.Load() == 0 {
					t.Fatal("gap-only confused SESSION/native-window accounting", a, sessions.Load(), feedback.Load())
				}
				before := a
				for i := 0; i < 3; i++ {
					control(t, dir, "pause")
					control(t, dir, "status")
					control(t, dir, "resume")
				}
				after := periodSaved(t, dir)
				if after.PolicyRevision != before.PolicyRevision || !after.GapOpenedAt.Equal(*before.GapOpenedAt) || sessions.Load() != 0 {
					t.Fatal(before, after)
				}
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}
func TestCPUPeriodWorkerGapUncertaintyAndMalformedStateKeepControls(t *testing.T) {
	for _, failure := range []string{"gap_uncertain", "malformed"} {
		t.Run(failure, func(t *testing.T) {
			dir, cfg := sessionWorkerFixture(t)
			cfg.Scan.CPUSessionCharges = false
			c := newPeriodWorkerClock()
			activatePeriodWorkerFixture(t, dir, c.wallNow(), true)
			options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow, cpuSessionObserve: func() (time.Duration, error) { t.Error("failed/gap role sampled SESSION CPU"); return 0, nil }}
			if failure == "gap_uncertain" {
				options.cpuPeriodHooks = &cpuPeriodHooks{gap: func(ctx context.Context, w *state.Store, q state.CPUTrackingGapRequest) (state.CPUChargeState, state.CPUChargeAdmissionState, error) {
					cpu, a, err := w.OpenCPUTrackingGap(ctx, q)
					if err != nil {
						return cpu, a, err
					}
					return cpu, a, state.ErrCPUChargeAdmissionPublication
				}}
			} else {
				db, err := sql.Open("sqlite", dir+"/state.sqlite3")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec("UPDATE worker_cpu_charge_admission SET state_json=x'7b7d'"); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			_, done := start(t, dir, cfg, options)
			waitUntil(t, func() bool {
				return strings.HasPrefix(control(t, dir, "status").WaitReason, "cpu_session_") && control(t, dir, "status").WaitReason != "cpu_session_startup"
			})
			for i := 0; i < 3; i++ {
				s := control(t, dir, "pause")
				if s.InventoryMode != nil || s.ActiveJob != 0 {
					t.Fatal("refused setup or work mutated", s)
				}
				control(t, dir, "resume")
			}
			control(t, dir, "stop")
			err := <-done
			if failure == "gap_uncertain" && !errors.Is(err, state.ErrCPUChargeAdmissionPublication) || failure == "malformed" && !errors.Is(err, state.ErrCPUChargeAdmissionCorrupt) {
				t.Fatal(err)
			}
		})
	}
}
func TestCPUPeriodWorkerBoundaryOvershootRefusesBeforeSetupAndReceipt(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	cfg.Scan.CPUChargeSecondsPerHour = 1
	c := newPeriodWorkerClock()
	var samples atomic.Int64
	_, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow,
		cpuSessionObserve: func() (time.Duration, error) {
			n := samples.Add(1)
			c.advance(time.Millisecond)
			if n == 1 {
				return 0, nil
			}
			return 2 * time.Second, nil
		}, cpuObserve: func() (time.Duration, error) { c.advance(time.Millisecond); return 0, nil }})
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_hour_charge_partial" })
	c.advance(2 * time.Hour)
	control(t, dir, "resume")
	waitUntil(t, func() bool { return periodSaved(t, dir).Hour.AssignedCPUNS == int64(2*time.Second) })
	for i := 0; i < 3; i++ {
		s := control(t, dir, "status")
		if s.InventoryMode != nil || s.ActiveJob != 0 {
			t.Fatal("sample/debt overshoot granted setup", s)
		}
	}
	if samples.Load() != 2 {
		t.Fatal("no idle polling permitted", samples.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
	if sessionSaved(t, dir).ChargedCPUNS != int64(2*time.Second) {
		t.Fatal("overshoot dropped")
	}
}

// Expired cached slots permit a new observation, not even a temporary claim.
// A clock change after the sample can expire both live anchors without rolling
// its saved slot; only a second real sample may then authorize the exact job.
func TestCPUPeriodWorkerExpiredPostSampleEvidenceCannotClaim(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	cfg.Scan.CPUChargeSecondsPerHour = 1
	c := newPeriodWorkerClock()
	due := c.wallNow().Add(-time.Hour)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, due); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var observations, handled atomic.Int64
	var jumped atomic.Bool
	hooks := &cpuPeriodHooks{sample: func(ctx context.Context, w *state.Store, marker state.CPULimitedSessionMarker, q state.CPUSessionSample, finish bool) (state.CPUChargeState, state.CPUChargeAdmissionState, error) {
		if q.Ordinal == 8 {
			actualDue, err := w.NextJobDue(ctx, []string{"fixture"})
			if err != nil || !actualDue.Equal(due) {
				t.Errorf("expired cached evidence changed an unclaimed job before resampling: %v, %v", actualDue, err)
			}
		}
		var cpu state.CPUChargeState
		var admission state.CPUChargeAdmissionState
		var err error
		if finish {
			cpu, admission, err = w.FinishCPUSessionLimited(ctx, marker, q)
		} else {
			cpu, admission, err = w.SampleCPUSessionLimited(ctx, marker, q)
		}
		if err == nil && q.Ordinal == 7 {
			if admission.Hour.AssignedCPUNS != int64(time.Second) {
				t.Error("missing genuine cap equality", admission)
			}
			c.advance(2 * time.Hour)
			jumped.Store(true)
		}
		return cpu, admission, err
	}}
	_, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow, cpuPeriodHooks: hooks,
		cpuObserve: func() (time.Duration, error) { c.advance(time.Millisecond); return 0, nil },
		cpuSessionObserve: func() (time.Duration, error) {
			n := observations.Add(1)
			c.advance(time.Millisecond)
			if n >= 8 {
				return time.Second, nil
			}
			return 0, nil
		},
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { handled.Add(1); return Result{Done: true}, nil }}})
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_hour_charge_partial" })
	c.advance(2 * time.Hour)
	control(t, dir, "resume")
	waitUntil(t, func() bool { return periodSaved(t, dir).CPUOrdinal >= 9 })
	if !jumped.Load() || handled.Load() != 1 {
		t.Fatal("fresh resampling did not precede one generic dispatch", jumped.Load(), handled.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestCPUPeriodWorkerBeginWallRolloverRetainsActivationElapsedFence(t *testing.T) {
	for _, profile := range []struct {
		name         string
		hour, day    int
		jump, remain time.Duration
	}{
		{"hour", 1, 0, 2 * time.Hour, time.Hour}, {"day", 0, 1, 24 * time.Hour, 14 * time.Hour},
	} {
		t.Run(profile.name, func(t *testing.T) {
			dir, cfg := sessionWorkerFixture(t)
			cfg.Scan.CPUChargeSecondsPerHour, cfg.Scan.CPUChargeSecondsPerDay = profile.hour, profile.day
			c := newPeriodWorkerClock()
			var observations atomic.Int64
			ready, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: c.wallNow, elapsedNow: c.elapsedNow,
				cpuObserve: func() (time.Duration, error) { c.advance(time.Millisecond); return 0, nil },
				cpuSessionObserve: func() (time.Duration, error) {
					if observations.Add(1) == 1 {
						c.wall.Add(int64(profile.jump))
					}
					c.advance(time.Millisecond)
					return 0, nil
				}})
			if ready.InventoryMode != nil {
				t.Fatal("setup preceded listener", ready)
			}
			waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_"+profile.name+"_charge_backoff" })
			a := periodSaved(t, dir)
			if a.CPUOrdinal != 0 || a.Hour.PartialTracking || (profile.day > 0 && a.Day.PartialTracking) {
				t.Fatal("fixture did not genuinely roll saved slot at Begin", a)
			}
			for i := 0; i < 3; i++ {
				control(t, dir, "pause")
				control(t, dir, "resume")
				s := control(t, dir, "status")
				if s.InventoryMode != nil || s.ActiveJob != 0 {
					t.Fatal("wall-only rollover bypassed elapsed setup guard", s)
				}
			}
			if observations.Load() != 1 {
				t.Fatal("idle/status renewed CPU observation", observations.Load())
			}
			r, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			budget, err := r.DispatchBudget(context.Background(), c.wallNow(), cfg.Scan.MaxScanChunksPerDay)
			r.Close()
			if err != nil || budget.Used != 0 {
				t.Fatal("unready period charged a source receipt", budget, err)
			}
			c.elapsed.Add(int64(profile.remain))
			control(t, dir, "resume")
			waitUntil(t, func() bool { return periodSaved(t, dir).CPUOrdinal >= 6 })
			if control(t, dir, "status").InventoryMode == nil || observations.Load() != 7 {
				t.Fatal("elapsed admission did not permit paired setup", observations.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}
