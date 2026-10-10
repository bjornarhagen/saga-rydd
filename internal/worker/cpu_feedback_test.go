package worker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Listener readiness does not promise a live dispatch projection while staged
// setup is held by recovered CPU debt. Inspect the saved charge at the exact
// fixture wall observation instead; this reader does not reserve another turn.
func savedWorkerDispatchBudget(t *testing.T, store *state.Store, observed time.Time, limit int) state.DispatchBudget {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	budget, err := store.DispatchBudget(ctx, observed, limit)
	if err != nil {
		t.Fatal("saved dispatch budget", err)
	}
	return budget
}

func TestWorkerCPUFeedbackIndependentClockAdmission(t *testing.T) {
	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	elapsed := time.Now()
	due := wall.Add(10 * time.Minute)
	feedback := state.CPUFeedbackState{Status: "observed", NextAllowedAt: &due,
		ClockHighWater: &wall, Window: &state.CPUWindowRecord{Token: "exact-window"}}
	var gate cpuFeedbackGate
	gate.update(feedback, wall, elapsed)
	jumped := wall.Add(time.Hour)
	gate.update(feedback, jumped, elapsed) // Status queries must not shorten it.
	wait := dispatchWait{}
	wait.wall(jumped.Add(time.Minute), jumped, "job_retry")
	gate.add(&wait, feedback, jumped, elapsed)
	if wait.duration != 10*time.Minute || wait.reason != "durable_cpu_backoff" {
		t.Fatal("mixed wall/elapsed timestamps erased the live CPU wait", wait)
	}
	wait = dispatchWait{}
	gate.add(&wait, feedback, jumped, elapsed.Add(10*time.Minute-time.Nanosecond))
	if wait.duration != time.Nanosecond {
		t.Fatal("elapsed boundary was admitted early", wait)
	}
	wait = dispatchWait{}
	gate.add(&wait, feedback, wall.Add(-time.Minute), elapsed.Add(time.Hour))
	if wait.duration != 11*time.Minute {
		t.Fatal("elapsed passage erased saved wall restrictions", wait)
	}
	wait = dispatchWait{}
	gate.add(&wait, feedback, jumped, elapsed.Add(10*time.Minute))
	if wait.duration != 0 {
		t.Fatal("both completed deadlines did not release one admission", wait)
	}
}

func TestWorkerCPUFeedbackDefiniteBeginDenialReleasesUnstartedJob(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	options.scannerNew = func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		calls.Add(1)
		return nil, errors.New("source constructor must not run")
	}
	options.cpuBegin = func(ctx context.Context, store *state.Store, turn *state.FairInventoryTurn, now time.Time, start state.CPUWindowStart) (state.CPUWindowMarker, error) {
		return store.BeginCPUWindow(ctx, turn, now.Add(-time.Hour), start)
	}
	_, done := start(t, dir, c, options)
	select {
	case err := <-done:
		if !errors.Is(err, state.ErrCPUFeedbackInvalid) || calls.Load() != 0 {
			t.Fatal("definite CPU refusal reached source", err, calls.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("definite admission refusal did not stop")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.RunningJobs != 0 || summary.PendingJobs != 1 || summary.Entries != 0 {
		t.Fatal("definite refusal retained a running unstarted lease", summary, err)
	}
	feedback, err := r.CPUFeedback(context.Background())
	if err != nil || feedback.Status != "untracked" || feedback.Window != nil {
		t.Fatal("refused CPU publication initialized accounting", feedback, err)
	}
	budget, err := r.DispatchBudget(context.Background(), time.Now(), c.Scan.MaxScanChunksPerDay)
	if err != nil || budget.Used != 1 {
		t.Fatal("refusal refunded its root turn", budget, err)
	}
}

func TestWorkerCPUFeedbackLostSettlementPreservesProgressAndRecovery(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	failure := errors.New("generated feedback write failure")
	var firstToken string
	options := Options{ExperimentalScan: true, Interval: time.Second, WorkDuration: time.Second}
	options.cpuSettle = func(ctx context.Context, store *state.Store, marker state.CPUWindowMarker, _ time.Time, _ state.CPUWindowMeasurement) (state.CPUFeedbackState, error) {
		firstToken = marker.Token()
		summary, err := store.Summary(ctx)
		if err != nil || summary.RunningJobs != 0 || summary.Entries != 2 {
			return state.CPUFeedbackState{}, errors.New("feedback was attempted before source progress commit")
		}
		return state.CPUFeedbackState{}, failure
	}
	_, done := start(t, dir, c, options)
	select {
	case err := <-done:
		if !errors.Is(err, failure) || firstToken == "" || !strings.Contains(err.Error(), firstToken) {
			t.Fatal("lost CPU settlement did not retain exact recovery identity", err, firstToken)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("uncertain accounting allowed further dispatch")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	before, err := r.CPUFeedback(context.Background())
	if err != nil || before.Status != "pending" || before.Window == nil || before.Window.Token != firstToken || before.Window.CPUTimeNS != nil {
		t.Fatal(before, err)
	}
	restarted, done := start(t, dir, c, Options{ExperimentalScan: true, Interval: time.Second, WorkDuration: time.Second})
	if restarted.CPUFeedback == nil || restarted.CPUFeedback.Status != "recovered_unknown" || restarted.CPUFeedback.RecoveredUnknownWindows != 1 || restarted.CPUFeedback.Window == nil || restarted.CPUFeedback.Window.Token != firstToken || restarted.CPUFeedback.Window.CPUTimeNS != nil || restarted.CPUFeedback.Window.SettledAt == nil || restarted.CPUFeedback.NextAllowedAt == nil || restarted.CPU == nil || restarted.CPU.Status != "not_recorded" || restarted.CPU.WindowCPUNS != nil {
		t.Fatal("restart reused a process CPU baseline", restarted)
	}
	deadline := *restarted.CPUFeedback.NextAllowedAt
	if deadline.Sub(*restarted.CPUFeedback.Window.SettledAt) != state.CPUUnknownRecoveryDelay {
		t.Fatal("policy hour was shortened", restarted.CPUFeedback)
	}
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_recovery_backoff" })
	control(t, dir, "pause")
	control(t, dir, "resume")
	time.Sleep(30 * time.Millisecond)
	blocked := control(t, dir, "status")
	blockedBudget := savedWorkerDispatchBudget(t, r, time.Now(), c.Scan.MaxScanChunksPerDay)
	if blocked.ActiveJob != 0 || blocked.InventoryMetrics != nil || blockedBudget.Used != 1 || blocked.CPUFeedback == nil || blocked.CPUFeedback.Window == nil || blocked.CPUFeedback.RecoveredUnknownWindows != 1 || blocked.CPUFeedback.NextAllowedAt == nil || !blocked.CPUFeedback.NextAllowedAt.Equal(deadline) {
		t.Fatal("controls bypassed or renewed recovery cooldown", blocked, blockedBudget)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	// Advance private fixture clocks, not the product's one-hour policy. The
	// saved evidence remains unknown and each new native observation starts fresh.
	var offset atomic.Int64
	options = Options{ExperimentalScan: true, Interval: time.Second, WorkDuration: time.Second,
		wallNow:    func() time.Time { return time.Now().Add(time.Duration(offset.Load())) },
		elapsedNow: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }}
	_, done = start(t, dir, c, options)
	var beforeAdvance Snapshot
	waitUntil(t, func() bool {
		beforeAdvance = control(t, dir, "status")
		return beforeAdvance.WaitReason == "cpu_recovery_backoff" && beforeAdvance.CPUFeedback != nil && beforeAdvance.CPUFeedback.Window != nil && beforeAdvance.CPUFeedback.NextAllowedAt != nil && beforeAdvance.CPU != nil
	})
	control(t, dir, "pause")
	beforeObservation := options.wallNow()
	beforeBudget := savedWorkerDispatchBudget(t, r, beforeObservation, c.Scan.MaxScanChunksPerDay)
	if beforeAdvance.ActiveJob != 0 || beforeAdvance.InventoryMetrics != nil || beforeAdvance.CPUFeedback.Window.Token != firstToken || !beforeAdvance.CPUFeedback.NextAllowedAt.Equal(deadline) {
		t.Fatal("synthetic clock fixture started source before recovery deadline", beforeAdvance, beforeBudget)
	}
	offset.Store(int64(state.CPUUnknownRecoveryDelay + time.Second))
	control(t, dir, "resume")
	var afterBudget state.DispatchBudget
	var afterObservation time.Time
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		afterObservation = options.wallNow()
		afterBudget = savedWorkerDispatchBudget(t, r, afterObservation, c.Scan.MaxScanChunksPerDay)
		want := beforeBudget.Used + 1
		if afterBudget.Day != beforeBudget.Day {
			want = 1
		}
		return afterBudget.Used == want && s.CPUFeedback != nil && s.CPUFeedback.Window != nil && s.CPUFeedback.Status != "pending" && s.CPUFeedback.Window.Token != firstToken
	})
	control(t, dir, "pause")
	resumed := control(t, dir, "status")
	afterObservation = options.wallNow()
	afterBudget = savedWorkerDispatchBudget(t, r, afterObservation, c.Scan.MaxScanChunksPerDay)
	t.Logf("saved dispatch before/after fixture clock advance: observed=%s day=%s used=%d; observed=%s day=%s used=%d", beforeObservation.UTC().Format(time.RFC3339Nano), beforeBudget.Day, beforeBudget.Used, afterObservation.UTC().Format(time.RFC3339Nano), afterBudget.Day, afterBudget.Used)
	want := beforeBudget.Used + 1
	if afterBudget.Day != beforeBudget.Day {
		want = 1
	}
	if afterBudget.Used != want || resumed.CPUFeedback == nil || resumed.CPUFeedback.Window == nil || resumed.CPUFeedback.RecoveredUnknownWindows != 1 || resumed.CPUFeedback.Window.Token == firstToken {
		t.Fatal("synthetic late wake granted catch-up turns", resumed, beforeBudget, afterBudget)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCPUFeedbackAccountsFailedProgressCommit(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	options.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
		batch, err := scanner.NextPermitted(ctx, job, permit)
		batch.Generation = -1 // The owner must refuse this generated invalid batch.
		return batch, err
	}
	_, done := start(t, dir, c, options)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "invalid directory observation") {
			t.Fatal("invalid source progress was accepted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed progress commit allowed further dispatch")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	feedback, err := r.CPUFeedback(context.Background())
	if err != nil || feedback.Status != "observed" || feedback.Window.CPUTimeNS == nil || *feedback.Window.CPUTimeNS != 0 || feedback.Window.ElapsedNS == nil {
		t.Fatal("returned commit failure discarded known CPU evidence", feedback, err)
	}
	summary, err := r.Summary(context.Background())
	if err != nil || summary.Entries != 0 || summary.CompleteDirectories != 0 || summary.RunningJobs != 1 {
		t.Fatal("invalid batch escaped as saved coverage", summary, err)
	}
}

// The selected root is generated. This seeds only accounting/job evidence; no
// source constructor or source operation runs before the worker owns the store.
func pendingCPUWorkerFixture(t *testing.T, at time.Time, completed ...bool) (string, config.Config, string) {
	t.Helper()
	count := 1
	if len(completed) > 0 && completed[0] {
		count = 0
	}
	root := fairWorkerFiles(t, count)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx := context.Background()
	if err := w.SyncRoots(ctx, c.Roots); err != nil {
		t.Fatal(err)
	}
	if err := w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	scope, err := w.ResolveFairInventoryRoots(ctx, c.Roots)
	if err != nil {
		t.Fatal(err)
	}
	if now := time.Now(); at.Before(now) {
		at = now
	}
	if _, err := w.ReserveScanChunk(ctx, at, time.Millisecond, c.Scan.MaxScanChunksPerDay); err != nil {
		t.Fatal(err)
	}
	turn, err := w.ClaimFairInventoryTurn(ctx, scope, at, time.Minute, true, at)
	if err != nil || turn == nil {
		t.Fatal(turn, err)
	}
	marker, err := w.BeginCPUWindow(ctx, turn, at, state.CPUWindowStart{Instance: strings.Repeat("b", 32), WindowStartedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed) > 0 && completed[0] {
		scanner, err := inventory.New(c.Roots, nil, []string{dir})
		if err != nil {
			t.Fatal(err)
		}
		batch, err := scanner.Next(ctx, *turn.Job)
		scanner.Close()
		if err != nil || !batch.Complete {
			t.Fatal(batch, err)
		}
		if err := w.CommitScan(ctx, *turn.Job, batch); err != nil {
			t.Fatal(err)
		}
		step, err := w.RetireInventoryForRoot(ctx, turn.RootID)
		if err != nil || step.Remaining {
			t.Fatal(step, err)
		}
		if _, err := w.ScheduleInventoryRevisit(ctx, turn.RootID, time.Now(), 24*time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	return dir, c, marker.Token()
}

func TestWorkerCPUFeedbackRollbackTimerRecoversBeforeAnotherDispatch(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending_source", true: "completed_source_future_revisit"}[completed], func(t *testing.T) {
			dir, c, token := pendingCPUWorkerFixture(t, time.Now(), completed)
			r, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			before, err := r.CPUFeedback(context.Background())
			if err != nil || before.ClockHighWater == nil {
				t.Fatal(before, err)
			}
			at := *before.ClockHighWater
			var advanced atomic.Bool
			var recoveryCalls atomic.Int64
			options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
				wallNow: func() time.Time {
					if advanced.Load() {
						return at.Add(time.Millisecond)
					}
					return at.Add(-50 * time.Millisecond)
				}}
			options.cpuRecover = func(ctx context.Context, w *state.Store, now time.Time) (state.CPUFeedbackState, error) {
				recoveryCalls.Add(1)
				return w.RecoverCPUWindow(ctx, now)
			}
			ready, done := start(t, dir, c, options)
			if ready.CPUFeedback == nil || ready.CPUFeedback.Status != "pending" {
				t.Fatal("initial rollback was not retained", ready)
			}
			// Keep the wall clock held until the elapsed timer is armed. Status reads
			// do not reschedule; no pause/resume/control wake reaches the boundary.
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.CPUFeedback != nil && s.CPUFeedback.Status == "pending" && s.WaitReason == "cpu_clock_rollback"
			})
			advanced.Store(true)
			waitUntil(t, func() bool {
				feedback, err := r.CPUFeedback(context.Background())
				return err == nil && feedback.Status == "recovered_unknown"
			})
			status := control(t, dir, "status")
			budget := savedWorkerDispatchBudget(t, r, options.wallNow(), c.Scan.MaxScanChunksPerDay)
			if status.CPUFeedback == nil || status.CPUFeedback.Window == nil || status.CPUFeedback.Window.Token != token || status.CPUFeedback.RecoveredUnknownWindows != 1 || budget.Used != 1 || status.ActiveJob != 0 || status.InventoryMetrics != nil || recoveryCalls.Load() != 2 {
				t.Fatal("rollback boundary claimed work before recovering the old window", status, budget, recoveryCalls.Load())
			}
			if completed {
				due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
				summary, summaryErr := r.Summary(context.Background())
				if err != nil || summaryErr != nil || !due.After(at.Add(23*time.Hour)) || summary.Entries != 1 || summary.CompleteDirectories != 1 || summary.RunningJobs != 0 {
					t.Fatal("accounting timer replayed or delayed the future source cursor", due, summary, err, summaryErr)
				}
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerCPUFeedbackRecoveryPublicationWallJumpPreservesElapsedHour(t *testing.T) {
	dir, c, token := pendingCPUWorkerFixture(t, time.Now())
	var offset atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		wallNow: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }}
	options.cpuRecover = func(ctx context.Context, w *state.Store, now time.Time) (state.CPUFeedbackState, error) {
		feedback, err := w.RecoverCPUWindow(ctx, now)
		if err == nil {
			offset.Store(int64(2 * time.Hour))
		}
		return feedback, err
	}
	ready, done := start(t, dir, c, options)
	if ready.CPUFeedback == nil || ready.CPUFeedback.Status != "recovered_unknown" || ready.CPUFeedback.Window == nil || ready.CPUFeedback.Window.Token != token || ready.CPUFeedback.NextAllowedAt == nil || ready.CPUFeedback.Window.SettledAt == nil || ready.CPUFeedback.NextAllowedAt.Sub(*ready.CPUFeedback.Window.SettledAt) != time.Hour {
		t.Fatal("recovery did not save its exact hour", ready)
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_recovery_backoff" })
	time.Sleep(30 * time.Millisecond)
	status := control(t, dir, "status")
	budget := savedWorkerDispatchBudget(t, r, options.wallNow(), c.Scan.MaxScanChunksPerDay)
	wantUsed := 1
	if budget.Day != ready.CPUFeedback.Window.DispatchReservedAt.UTC().Format("2006-01-02") {
		wantUsed = 0
	}
	if budget.Used != wantUsed || status.ActiveJob != 0 || status.InventoryMetrics != nil || status.CPUFeedback == nil || status.CPUFeedback.RecoveredUnknownWindows != 1 {
		t.Fatal("wall change during recovery erased elapsed admission", status, budget)
	}
	control(t, dir, "pause")
	control(t, dir, "resume")
	status = control(t, dir, "status")
	budget = savedWorkerDispatchBudget(t, r, options.wallNow(), c.Scan.MaxScanChunksPerDay)
	wantUsed = 1
	if budget.Day != ready.CPUFeedback.Window.DispatchReservedAt.UTC().Format("2006-01-02") {
		wantUsed = 0
	}
	if budget.Used != wantUsed || status.CPUFeedback == nil || status.CPUFeedback.Window == nil || status.CPUFeedback.Window.Token != token {
		t.Fatal("control refreshed the wait anchor", status, budget)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCPUFeedbackReturnedSourceCancellationSettles(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	entered := make(chan struct{})
	options := Options{ExperimentalScan: true, Interval: time.Second, WorkDuration: time.Second}
	options.scannerNext = func(ctx context.Context, _ *inventory.Scanner, _ state.Job, _ inventory.APIPermit) (state.ScanBatch, error) {
		close(entered)
		<-ctx.Done()
		return state.ScanBatch{}, ctx.Err()
	}
	_, done := start(t, dir, c, options)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("source cancellation fixture did not enter")
	}
	control(t, dir, "stop")
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	feedback, err := r.CPUFeedback(context.Background())
	if err != nil || feedback.Status != "observed" || feedback.Window.CPUTimeNS == nil || *feedback.Window.CPUTimeNS != 0 || feedback.Window.ElapsedNS == nil || *feedback.Window.ElapsedNS <= 0 || feedback.RecoveredUnknownWindows != 0 {
		t.Fatal("returned canceled work was mislabeled as an interrupted measurement", feedback, err)
	}
	summary, err := r.Summary(context.Background())
	if err != nil || summary.RunningJobs != 0 || summary.PendingJobs != 1 || summary.Entries != 0 {
		t.Fatal("canceled source lost its retryable cursor", summary, err)
	}
}

func TestWorkerCPUFeedbackUnknownReturnedObservationRetainsCadence(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	options := Options{ExperimentalScan: true, Interval: time.Second, WorkDuration: time.Second,
		cpuObserve: func() (time.Duration, error) { return 0, errors.New("generated unavailable native CPU observation") }}
	_, done := start(t, dir, c, options)
	var status Snapshot
	waitUntil(t, func() bool {
		status = control(t, dir, "status")
		return status.CPUFeedback != nil && status.CPUFeedback.Status == "completed_unknown"
	})
	control(t, dir, "pause")
	if status.CPU.Status != "unknown" || status.CPUFeedback.Window.CPUTimeNS != nil || status.CPUFeedback.Window.ElapsedNS == nil || status.CPUFeedback.NextAllowedAt != nil || status.CPUFeedback.CompletedUnknownWindows != 1 || status.CPUFeedback.RecoveredUnknownWindows != 0 || status.Dispatch.Used != 1 {
		t.Fatal("unavailable completed observation became zero or a crash cooldown", status)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}
