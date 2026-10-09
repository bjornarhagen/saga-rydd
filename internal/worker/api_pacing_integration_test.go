package worker

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func startPacingWorker(t *testing.T, dir string, c config.Config, options Options) (Snapshot, <-chan error) {
	t.Helper()
	snapshot, done, _ := startCancelable(t, dir, c, options, func(cancel context.CancelFunc, done <-chan error) {
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("paced fixture worker did not return after cancellation")
			}
		})
	})
	return snapshot, done
}

func TestAPIPacingPermanentBlockPreservesFutureGenericPlan(t *testing.T) {
	dir, c := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	if err = w.EnqueueJob(ctx, 1, state.ScanKind, []byte("."), now); err != nil {
		t.Fatal(err)
	}
	future := now.Add(time.Hour)
	if err = w.EnqueueJob(ctx, 1, "fixture", nil, future); err != nil {
		t.Fatal(err)
	}
	roots, err := w.ResolveFairInventoryRoots(ctx, c.Roots)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := w.MetadataBudget(ctx, now, c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := nextFairInventoryPlan(ctx, w, roots, []string{"fixture"}, now, metadata, 0, false, "api_capacity_blocked")
	if err != nil || !plan.generic || !plan.due.Equal(future) || plan.waitReason != "job_retry" || plan.allowSource {
		t.Fatal(plan, err)
	}
	plan, err = nextFairInventoryPlan(ctx, w, roots, nil, now, metadata, 0, false, "api_profile_blocked")
	if err != nil || !plan.due.IsZero() || plan.waitReason != "api_profile_blocked" {
		t.Fatal(plan, err)
	}
	// Existing nonpermanent source gates keep their established retry timing.
	plan, err = nextFairInventoryPlan(ctx, w, roots, nil, now, metadata, 0, false)
	if err != nil || plan.due.IsZero() {
		t.Fatal(plan, err)
	}
}

func TestWorkerAPIPacingCapacityBlocksSourceWithoutPolling(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.APIAttemptsPerSecond = 100
	var samples, probes, sourceCalls atomic.Int64
	power := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		return powerinfo.Observation{}, nil
	}, nil, nil)
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, powerCoordinator: power,
		inventoryStateObserve: func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return state.InventoryStateBudget{}, errors.New("unexpected sample")
		},
		scannerPacedNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
			sourceCalls.Add(1)
			return nil, errors.New("unexpected source")
		}}
	_, done := startPacingWorker(t, dir, c, o)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "api_capacity_blocked" })
	before := control(t, dir, "status")
	for _, command := range []string{"status", "pause", "status", "resume", "status"} {
		control(t, dir, command)
	}
	time.Sleep(80 * time.Millisecond)
	after := control(t, dir, "status")
	if before.APIPacing == nil || before.APIPacing.CapacityReady || (before.Dispatch != nil && before.Dispatch.Used != 0) || (after.Dispatch != nil && after.Dispatch.Used != 0) || after.Metadata.TotalCharges != nil || (after.CPUFeedback != nil && after.CPUFeedback.Window != nil) || after.InventoryMetrics != nil || samples.Load() != 0 || probes.Load() != 0 || sourceCalls.Load() != 0 {
		t.Fatal(before, after, samples.Load(), probes.Load(), sourceCalls.Load())
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := r.DispatchBudget(context.Background(), time.Now(), c.Scan.MaxScanChunksPerDay)
	r.Close()
	if err != nil || budget.Used != 0 {
		t.Fatal("static source refusal charged dispatch", budget, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAPIPacingBlockAllowsMaintenanceAndFutureGeneric(t *testing.T) {
	dir, historical, _ := workerRetirementFixture(t, 300)
	source := fairWorkerFiles(t, 1)
	c := config.Default()
	c.Roots = []string{historical, source}
	c.Scan.APIAttemptsPerSecond = 100
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(300*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var samples, probes, sourceCalls, generic atomic.Int64
	genericDone := make(chan struct{}, 1)
	power := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		return powerinfo.Observation{}, nil
	}, nil, nil)
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, powerCoordinator: power,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
			generic.Add(1)
			genericDone <- struct{}{}
			return Result{Done: true}, nil
		}},
		inventoryStateObserve: func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return state.InventoryStateBudget{}, errors.New("unexpected sample")
		},
		scannerPacedNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
			sourceCalls.Add(1)
			return nil, errors.New("unexpected source")
		}}
	_, done := startPacingWorker(t, dir, c, o)
	// No control request wakes the worker while the generic future timer is
	// pending. Its own due timer must survive the permanent source refusal.
	select {
	case <-genericDone:
	case <-time.After(5 * time.Second):
		t.Fatal("future generic timer was lost")
	}
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "api_capacity_blocked" })
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	before := control(t, dir, "status")
	time.Sleep(80 * time.Millisecond)
	after := control(t, dir, "status")
	if err != nil || summary.Entries != 4 || before.Dispatch.Used < 1 || after.Dispatch.Used != before.Dispatch.Used || after.Metadata.TotalCharges != nil || samples.Load() != 0 || probes.Load() != 0 || sourceCalls.Load() != 0 || generic.Load() != 1 {
		t.Fatal(summary, err, before, after, samples.Load(), probes.Load(), sourceCalls.Load(), generic.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAPIPacingProfileRefusalSettlesOnceAndPreservesJob(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.APIAttemptsPerSecond = 100000
	c.Scan.PauseOnBattery = false
	var calls atomic.Int64
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		scannerPacedNew: func(ctx context.Context, _ []string, _ []string, _ []string, permit inventory.APIPermit, _ ...inventory.Option) (*inventory.Scanner, error) {
			calls.Add(1)
			if err := permit(ctx, inventory.APIPathResolution); err != nil {
				return nil, err
			}
			return nil, errors.Join(inventory.ErrAPIPermitDenied, inventory.ErrAPIPacingProfile)
		}}
	_, done := startPacingWorker(t, dir, c, o)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "api_profile_blocked" })
	before := control(t, dir, "status")
	for _, command := range []string{"status", "pause", "resume", "status"} {
		control(t, dir, command)
	}
	time.Sleep(80 * time.Millisecond)
	after := control(t, dir, "status")
	if calls.Load() != 1 || before.Dispatch.Used != 1 || after.Dispatch.Used != 1 || before.Metadata.TotalCharges == nil || before.Metadata.TotalCharges.Observed != 1 || before.Metadata.TotalCharges.UnknownReserved != 0 || after.Metadata.TotalCharges.Observed != 1 || after.CPUFeedback.Status == "pending" || after.APIPacing.CapacityReady {
		t.Fatal(before, after, calls.Load())
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
	r.Close()
	if err != nil || due.IsZero() {
		t.Fatal("refused profile lost unstarted source progress", due, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAPIPacingSavesValidatedPartialThenContinues(t *testing.T) {
	root := fairWorkerFiles(t, 3)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.APIAttemptsPerSecond = 100000
	c.Scan.PauseOnBattery = false
	c.Scan.MetadataPerSecond = 100000
	var batches, entries atomic.Int64
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	o.scannerPacedNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, permit inventory.APIPermit, capacity inventory.APIEntryCapacity) (state.ScanBatch, error) {
		checks := 0
		b, err := s.NextPermittedPaced(ctx, j, permit, func(ctx context.Context, calls int64) (bool, error) {
			ready, err := capacity(ctx, calls)
			if err != nil || !ready {
				return ready, err
			}
			checks++
			return checks <= 1, nil
		})
		if err == nil {
			batches.Add(1)
			entries.Add(int64(len(b.Entries)))
		}
		return b, err
	}
	_, done := startPacingWorker(t, dir, c, o)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_revisit" })
	s := control(t, dir, "status")
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	// Scheduler/API work can already exceed the spacing on a busy race runner.
	// A zero wait then means no extra delay was needed. Deterministic pacer tests
	// separately verify actual wait accounting; this fixture verifies dispatch
	// through the configured profile and validated one-entry partial commits.
	if err != nil || batches.Load() < 3 || entries.Load() != 3 || summary.Entries != 4 || summary.CompleteDirectories != 1 || s.Metadata.TotalCharges.Observed < 1 || s.Metadata.TotalCharges.UnknownReserved != 0 || s.APIPacing == nil || s.APIPacing.RatePerSecond != 100000 || !s.APIPacing.CapacityReady || s.APIPacing.Waiting {
		t.Fatal(summary, err, batches.Load(), entries.Load(), s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAPIPacingDefaultAndIdleRemainUnpaced(t *testing.T) {
	for _, experimental := range []bool{false, true} {
		dir, c := fixture(t)
		if !experimental {
			c.Scan.APIAttemptsPerSecond = 100000
		}
		var paced atomic.Int64
		o := Options{ExperimentalScan: experimental, Interval: time.Millisecond, WorkDuration: time.Second,
			scannerPacedNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
				paced.Add(1)
				return nil, errors.New("unexpected paced mode")
			}}
		_, done := startPacingWorker(t, dir, c, o)
		if s := control(t, dir, "status"); s.APIPacing != nil || paced.Load() != 0 {
			t.Fatal(experimental, s, paced.Load())
		}
		control(t, dir, "stop")
		waitExit(t, done)
	}
}
