package worker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Legacy cadence/CPU fixtures exercise the explicit unavailable fallback without
// starting asynchronous work or touching hardware. Dedicated power fixtures below
// supply real asynchronous callbacks, including unknown/blocked results.
func fixturePowerCoordinator(wall, elapsed func() time.Time) *powerCoordinator {
	return newPowerCoordinator(nil, wall, elapsed)
}

// Register before starting Run: even readiness/assertion failures cancel the
// worker, unblock the selected callback exactly once and join its real return.
func cleanupWorkerPowerFixture(t *testing.T, p *powerCoordinator, release chan struct{}, runCancel *context.CancelFunc, runDone *<-chan error) func() {
	t.Helper()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		// A failed Run join must still join the released callback. Deferred
		// cleanup also runs when a test helper calls Fatal/Goexit.
		defer func() {
			p.mu.Lock()
			ticket := p.ticket
			p.mu.Unlock()
			if ticket != nil {
				p.cancelTicket(ticket)
				waitPowerTicket(t, ticket)
			}
		}()
		if *runCancel != nil {
			(*runCancel)()
		}
		unblock()
		if *runDone != nil {
			waitExit(t, *runDone)
		}
	})
	return unblock
}

func TestWorkerPowerNoProbeWithoutAdmissibleDueSource(t *testing.T) {
	for _, mode := range []string{"idle", "paused", "future_source", "dispatch_quota", "metadata_quota"} {
		t.Run(mode, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			w, err := state.OpenWriter(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
				t.Fatal(err)
			}
			if mode == "paused" {
				err = w.SetPaused(context.Background(), true)
			}
			if mode == "future_source" {
				err = w.EnqueueJob(context.Background(), 1, state.ScanKind, nil, time.Now().Add(time.Hour))
			}
			if mode == "dispatch_quota" {
				c.Scan.MaxScanChunksPerDay = 1
				_, err = w.ReserveScanChunk(context.Background(), time.Now(), time.Millisecond, 1)
			}
			if mode == "metadata_quota" {
				c.Scan.MetadataAttemptsPerDay = 1
			}
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			var probes atomic.Int32
			p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				probes.Add(1)
				return powerinfo.Observation{}, errors.New("must not probe")
			}, nil, nil)
			_, done := start(t, dir, c, Options{ExperimentalScan: mode != "idle", powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second})
			time.Sleep(50 * time.Millisecond)
			status := control(t, dir, "status")
			if probes.Load() != 0 || status.InventoryMetrics != nil {
				t.Fatal("probed inadmissible source", mode, status, probes.Load())
			}
			if mode != "idle" && (status.Power == nil || status.Power.LastDecisionStatus != "not_evaluated") {
				t.Fatal(status.Power)
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerPowerPendingNoReceiptAndResponsiveControls(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	entered, release := make(chan struct{}), make(chan struct{})
	p := newPowerCoordinator(func(ctx context.Context) (powerinfo.Observation, error) {
		close(entered)
		<-release
		return powerinfo.Observation{}, ctx.Err()
	}, nil, nil)
	var runCancel context.CancelFunc
	var runDone <-chan error
	unblock := cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second}, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no admitted sample")
	}
	before := control(t, dir, "status")
	if before.Power == nil || before.Power.LastDecisionStatus != "observing" || before.Power.CallbackReturned == nil || *before.Power.CallbackReturned || before.Dispatch.Used != 0 || before.CPUFeedback.Window != nil || before.InventoryMetrics != nil {
		t.Fatal("pending sample charged or read source", before)
	}
	if !control(t, dir, "pause").Paused {
		t.Fatal("pause failed")
	}
	control(t, dir, "stop")
	waitExit(t, done)
	// Stopping does not wait for the deliberately blocked callback or free it.
	if _, ready := p.ticket.completed(); ready {
		t.Fatal("cancellation fabricated return")
	}
	unblock()
	select {
	case <-p.ticket.done:
	case <-time.After(time.Second):
		t.Fatal("callback return not published")
	}
}

func TestWorkerPowerPendingAllowsMaintenanceAndGeneric(t *testing.T) {
	dir, historical, _ := workerRetirementFixture(t, 300)
	source := fairWorkerFiles(t, 1)
	c := config.Default()
	c.Roots = []string{historical, source}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	p := newPowerCoordinator(func(ctx context.Context) (powerinfo.Observation, error) {
		close(entered)
		<-release
		return powerinfo.Observation{}, ctx.Err()
	}, nil, nil)
	var generic, sourceCalls atomic.Int64
	options := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second, Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }}}
	noRetirementSource(t, &options, &sourceCalls)
	var runCancel context.CancelFunc
	var runDone <-chan error
	_ = cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, options, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sample missing")
	}
	waitUntil(t, func() bool {
		return generic.Load() == 1 && control(t, dir, "status").WaitReason == "power_source_backoff"
	})
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	if err != nil || summary.Entries != 4 || sourceCalls.Load() != 0 {
		t.Fatal("power blocked maintenance or permitted source", summary, sourceCalls.Load(), err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPowerDisabledDoesNotProbeOrDeferSource(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.PauseOnBattery = false
	var probes atomic.Int32
	release := make(chan struct{})
	p := newPowerCoordinator(func(ctx context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		<-release
		return powerinfo.Observation{}, ctx.Err()
	}, nil, nil)
	var runCancel context.CancelFunc
	var runDone <-chan error
	_ = cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second}, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	waitUntil(t, func() bool { return control(t, dir, "status").InventoryMetrics != nil })
	status := control(t, dir, "status")
	if probes.Load() != 0 || status.Power.PolicyEnabled || status.Power.LastDecisionStatus != "not_evaluated" || status.Power.LastDecisionReason != "power_policy_disabled" {
		t.Fatal(status, probes.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPowerReceiptRecheckNeverStartsProbe(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	wall, elapsed := time.Now().UTC(), time.Now()
	var clockCalls, wallJump, abortAt, probeAt atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	p := newPowerCoordinator(func(ctx context.Context) (powerinfo.Observation, error) {
		probeAt.Store(time.Now().UnixNano())
		close(entered)
		<-release
		return powerinfo.Observation{}, ctx.Err()
	}, func() time.Time {
		n := clockCalls.Add(1)
		if n == 2 {
			abortAt.Store(time.Now().UnixNano())
			wallJump.Store(int64(time.Hour))
		}
		if n >= 2 {
			return wall.Add(2 * time.Second)
		}
		return wall
	}, func() time.Time {
		if clockCalls.Load() >= 2 {
			return elapsed.Add(2 * time.Second)
		}
		return elapsed
	})
	launchWall, launchElapsed := wall.Add(-powerSampleInterval+time.Second), elapsed.Add(-powerSampleInterval+time.Second)
	old := &powerTicket{ID: 1, LaunchWall: launchWall, LaunchElapsed: launchElapsed, done: make(chan struct{})}
	old.completion = powerCompletion{ID: 1, LaunchWall: launchWall, LaunchElapsed: launchElapsed, CompletedWall: launchWall.Add(time.Second), CompletedElapsed: launchElapsed.Add(time.Second), Observation: &powerinfo.Observation{Status: "unknown"}}
	close(old.done)
	p.ticket = old
	p.nextID = 1
	p.lastWall = launchWall
	p.lastElapsed = launchElapsed
	var runCancel context.CancelFunc
	var runDone <-chan error
	_ = cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, Options{ExperimentalScan: true, powerCoordinator: p, Interval: 80 * time.Millisecond, WorkDuration: time.Second, wallNow: func() time.Time { return time.Now().Add(time.Duration(wallJump.Load())) }}, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("uncharged replan did not start required sample")
	}
	if elapsed := time.Duration(probeAt.Load() - abortAt.Load()); elapsed < 70*time.Millisecond {
		t.Fatal("forward wall jump bypassed charged receipt elapsed cadence", elapsed)
	}
	status := control(t, dir, "status")
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	charged, err := r.DispatchBudget(context.Background(), wall, c.Scan.MaxScanChunksPerDay)
	r.Close()
	if err != nil || charged.Used != 1 {
		t.Fatal("receipt lost original UTC-day charge", charged, err)
	}
	wantUsed := 1
	if status.Dispatch != nil && status.Dispatch.Day != wall.Format(time.DateOnly) {
		wantUsed = 0
	}
	if status.Dispatch == nil || status.Dispatch.Used != wantUsed || status.CPUFeedback.Window != nil || status.InventoryMetrics != nil || p.nextID != 2 {
		t.Fatal("receipt recheck claimed/read or restarted probe", status, p.nextID)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPowerCanceledRunRetainsBlockedSlot(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MaxScanChunksPerDay = 1
	var probes atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	p := newPowerCoordinator(func(ctx context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		close(entered)
		<-release
		return powerinfo.Observation{}, ctx.Err()
	}, nil, nil)
	options := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second}
	var runCancel context.CancelFunc
	var runDone <-chan error
	_ = cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, options, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("missing probe")
	}
	control(t, dir, "stop")
	waitExit(t, done)
	_, done, cancel = startCancelable(t, dir, c, options, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	waitUntil(t, func() bool { return control(t, dir, "status").InventoryMetrics != nil })
	status := control(t, dir, "status")
	if probes.Load() != 1 || status.Power.CallbackReturned == nil || *status.Power.CallbackReturned || status.Power.LastDecisionReason != "power_observer_stalled" || status.Dispatch.Used != 1 {
		t.Fatal("restart replaced blocked callback or stopped fallback source", status, probes.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPowerFreshDischargeAndSparseResample(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MaxScanChunksPerDay = 1
	var offset atomic.Int64
	wall := func() time.Time { return time.Now().Add(time.Duration(offset.Load())).UTC() }
	elapsed := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	var probes atomic.Int32
	p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		at := wall()
		n := probes.Add(1)
		if n == 1 {
			yes := true
			return powerinfo.Observation{Contract: powerinfo.Contract, Platform: "linux", Profile: powerinfo.LinuxProfile, Status: "system_battery_discharging_observed", Reason: "fixture_discharge", StartedAt: at, FinishedAt: at, SequentialObservations: true, ProvidersProcessed: 1, ConfirmedSystemBatteries: 1, DischargingSystemBatteries: 1, SystemBatteryDischargingObserved: &yes}, nil
		}
		return powerinfo.Observation{Contract: powerinfo.Contract, Platform: "darwin", Profile: "unsupported", Status: "unknown", Reason: "fixture_unknown", StartedAt: at, FinishedAt: at, SequentialObservations: true}, nil
	}, wall, elapsed)
	_, done := start(t, dir, c, Options{ExperimentalScan: true, powerCoordinator: p, wallNow: wall, elapsedNow: elapsed, Interval: time.Millisecond, WorkDuration: time.Second})
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.Power.LastDecisionReason == "power_discharge_witness" && s.WaitReason == "power_source_backoff"
	})
	before := control(t, dir, "status")
	for i := 0; i < 3; i++ {
		_ = control(t, dir, "status")
	}
	if probes.Load() != 1 || before.Dispatch.Used != 0 || before.InventoryMetrics != nil || before.CPUFeedback.Window != nil {
		t.Fatal("positive witness read/charged source or status probed", before, probes.Load())
	}
	control(t, dir, "pause")
	offset.Store(int64(powerSampleInterval + time.Second))
	control(t, dir, "resume")
	waitUntil(t, func() bool { s := control(t, dir, "status"); return s.InventoryMetrics != nil && s.ActiveJob == 0 })
	after := control(t, dir, "status")
	if probes.Load() != 2 || after.Dispatch.Used != 1 || after.Power.LastSourceBackoff == nil || *after.Power.LastSourceBackoff || after.Power.Observation == nil || after.Power.Observation.SystemBatteryDischargingObserved != nil {
		t.Fatal("sparse unknown sample invented external power or catch-up", after, probes.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPowerCompletionWakeRechecksDispatchQuota(t *testing.T) {
	// Maintenance consumes the sole turn while the source sample is pending.
	// Callback completion must not dispatch the previously admissible source.
	dir, historical, _ := workerRetirementFixture(t, 300)
	source := fairWorkerFiles(t, 1)
	c := config.Default()
	c.Roots = []string{historical, source}
	c.Scan.MaxScanChunksPerDay = 1
	release := make(chan struct{})
	var probes atomic.Int32
	clock := time.Now
	p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		<-release
		at := clock().UTC()
		return powerinfo.Observation{Contract: powerinfo.Contract, Platform: "darwin", Profile: "unsupported", Status: "unknown", Reason: "fixture_unknown", StartedAt: at, FinishedAt: at, SequentialObservations: true}, nil
	}, nil, nil)
	var sourceCalls atomic.Int64
	options := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second}
	noRetirementSource(t, &options, &sourceCalls)
	var runCancel context.CancelFunc
	var runDone <-chan error
	unblock := cleanupWorkerPowerFixture(t, p, release, &runCancel, &runDone)

	_, done, cancel := startCancelable(t, dir, c, options, func(cancel context.CancelFunc, done <-chan error) { runCancel, runDone = cancel, done })
	runCancel, runDone = cancel, done
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.Dispatch != nil && s.Dispatch.Used == 1 && s.WaitReason == "daily_chunk_limit"
	})
	before := control(t, dir, "status")
	unblock()
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.Power.CallbackReturned != nil && *s.Power.CallbackReturned
	})
	after := control(t, dir, "status")
	if before.Dispatch.Used != after.Dispatch.Used || sourceCalls.Load() != 0 || probes.Load() != 1 || after.InventoryMetrics != nil || after.Power.LastDecisionAt == nil || !after.Power.LastDecisionAt.Equal(*before.Power.LastDecisionAt) {
		t.Fatal("completion bypassed quota or status renewed admission", before, after, sourceCalls.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestPowerCachedCompletionAndIndependentWaits(t *testing.T) {
	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	elapsed := time.Now()
	ticket := &powerTicket{ID: 7, LaunchWall: wall, LaunchElapsed: elapsed, done: make(chan struct{})}
	var cache sourcePowerState
	cache.update(powerPolicyDecision{Ticket: ticket, Status: "observing", Reason: "power_sample_pending", SourceBackoff: true, Wait: 5 * time.Second}, wall)
	for _, clocks := range [][2]time.Time{{wall.Add(6 * time.Second), elapsed}, {wall, elapsed.Add(6 * time.Second)}, {wall.Add(-time.Second), elapsed}} {
		if got := cache.remaining(clocks[0], clocks[1]); got != 0 {
			t.Fatal("invalid pending window retained wait", got)
		}
	}
	cache.update(powerPolicyDecision{Ticket: ticket, Status: "system_battery_discharging_observed", SourceBackoff: true}, wall)
	if got := cache.remaining(wall.Add(time.Hour), elapsed.Add(time.Minute)); got != 0 {
		t.Fatal("stale witness retained source wait", got)
	}
	trueValue := true
	ticket.completion = powerCompletion{ID: 7, LaunchWall: wall, LaunchElapsed: elapsed, CompletedWall: wall.Add(time.Second), CompletedElapsed: elapsed.Add(time.Second), Observation: &powerinfo.Observation{SystemBatteryDischargingObserved: &trueValue, Status: "system_battery_discharging_observed"}}
	close(ticket.done)
	saved := cache.snapshot()
	for _, clocks := range [][2]time.Time{{wall.Add(time.Hour), elapsed.Add(time.Minute)}, {wall.Add(time.Minute), elapsed.Add(time.Hour)}, {wall.Add(-time.Second), elapsed}} {
		if got := cache.remaining(clocks[0], clocks[1]); got != 0 {
			t.Fatal("invalid witness retained wait", clocks, got)
		}
	}
	if got := cache.remaining(wall.Add(time.Minute), elapsed.Add(2*time.Minute)); got != 4*time.Minute {
		t.Fatal("fresh source wait changed anchors", got)
	}
	if cache.wake == nil || saved.CallbackReturned == nil || !*saved.CallbackReturned || saved.SampleCompletedAt == nil || saved.LastDecisionAt == nil || !saved.LastDecisionAt.Equal(wall) {
		t.Fatal(saved)
	}
	*saved.Observation.SystemBatteryDischargingObserved = false
	if !*cache.snapshot().Observation.SystemBatteryDischargingObserved {
		t.Fatal("status exposed completion storage")
	}
}
