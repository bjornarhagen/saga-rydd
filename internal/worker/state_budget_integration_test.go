package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestWorkerInventoryStateNoSampleWithoutAdmissibleDueSource(t *testing.T) {
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
			switch mode {
			case "paused":
				err = w.SetPaused(context.Background(), true)
			case "future_source":
				err = w.EnqueueJob(context.Background(), 1, state.ScanKind, nil, time.Now().Add(time.Hour))
			case "dispatch_quota":
				c.Scan.MaxScanChunksPerDay = 1
				_, err = w.ReserveScanChunk(context.Background(), time.Now(), time.Millisecond, 1)
			case "metadata_quota":
				c.Scan.MetadataAttemptsPerDay = 1
			}
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			var samples, probes atomic.Int64
			p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				probes.Add(1)
				return powerinfo.Observation{}, errors.New("unexpected probe")
			}, nil, nil)
			o := Options{ExperimentalScan: mode != "idle", powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
				inventoryStateObserve: func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
					samples.Add(1)
					return state.InventoryStateBudget{}, errors.New("unexpected sample")
				}}
			_, done := start(t, dir, c, o)
			time.Sleep(30 * time.Millisecond)
			s := control(t, dir, "status")
			if samples.Load() != 0 || probes.Load() != 0 || s.InventoryMetrics != nil || (s.InventoryState != nil && s.InventoryState.LastAttemptStartedAt != nil) {
				t.Fatal(mode, s, samples.Load(), probes.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerInventoryStateReachedUnknownAndControlsRetainRetry(t *testing.T) {
	for _, mode := range []string{"equal", "unknown", "changed", "canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			var samples, probes, sources atomic.Int64
			p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				probes.Add(1)
				return powerinfo.Observation{}, nil
			}, nil, nil)
			o := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
				inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
					samples.Add(1)
					r := fixtureInventoryStateReport(limit, limit, time.Now())
					switch mode {
					case "unknown":
						return state.InventoryStateBudget{}, errors.New("private source must not be printed")
					case "changed":
						r.Available, r.Status, r.Reason, r.DatabaseBytes, r.WALBytes, r.TotalBytes = false, "unavailable", "inventory_state_changed", nil, nil, nil
					case "canceled":
						return state.InventoryStateBudget{}, context.Canceled
					case "deadline":
						return state.InventoryStateBudget{}, context.DeadlineExceeded
					}
					return r, nil
				}}
			noRetirementSource(t, &o, &sources)
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_state_source_backoff" })
			before := control(t, dir, "status")
			if samples.Load() != 1 || probes.Load() != 0 || sources.Load() != 0 || before.Dispatch.Used != 0 || before.CPUFeedback.Window != nil || before.InventoryState.LastDecisionStatus != "source_deferred" || before.InventoryState.RetryAfterAt == nil {
				t.Fatal(before, samples.Load(), probes.Load(), sources.Load())
			}
			for _, command := range []string{"status", "pause", "status", "resume", "status"} {
				control(t, dir, command)
			}
			time.Sleep(20 * time.Millisecond)
			after := control(t, dir, "status")
			if samples.Load() != 1 || !reflect.DeepEqual(before.InventoryState, after.InventoryState) || after.Dispatch.Used != 0 {
				t.Fatal("controls renewed retry or sampled/charged", before, after, samples.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerInventoryStateAllowsMaintenanceAndGeneric(t *testing.T) {
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
	var samples, probes, sources, generic atomic.Int64
	p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		probes.Add(1)
		return powerinfo.Observation{}, nil
	}, nil, nil)
	o := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }},
		inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return fixtureInventoryStateReport(limit, limit, time.Now()), nil
		}}
	noRetirementSource(t, &o, &sources)
	_, done := start(t, dir, c, o)
	waitUntil(t, func() bool {
		return generic.Load() == 1 && control(t, dir, "status").WaitReason == "inventory_state_source_backoff"
	})
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	if err != nil || summary.Entries != 4 || samples.Load() != 1 || probes.Load() != 0 || sources.Load() != 0 {
		t.Fatal(summary, err, samples.Load(), probes.Load(), sources.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerInventoryStatePostReceiptRefusalRetainsChargeAndCadence(t *testing.T) {
	for _, mode := range []string{"reached", "unknown", "changed", "deadline", "expired_wall", "expired_elapsed", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			c.Scan.PauseOnBattery = false
			var samples, sources, wallOffset, elapsedOffset atomic.Int64
			wall := func() time.Time { return time.Now().Add(time.Duration(wallOffset.Load())).UTC() }
			elapsed := func() time.Time { return time.Now().Add(time.Duration(elapsedOffset.Load())) }
			o := Options{ExperimentalScan: true, Interval: 80 * time.Millisecond, WorkDuration: time.Second, wallNow: wall, elapsedNow: elapsed,
				inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
					n := samples.Add(1)
					r := fixtureInventoryStateReport(limit, 1, wall())
					if n != 2 {
						return r, nil
					}
					switch mode {
					case "reached":
						return fixtureInventoryStateReport(limit, limit, wall()), nil
					case "unknown":
						return state.InventoryStateBudget{}, errors.New("unknown state")
					case "changed":
						r.Available, r.Status, r.Reason, r.DatabaseBytes, r.WALBytes, r.TotalBytes = false, "unavailable", "inventory_state_changed", nil, nil, nil
					case "deadline":
						return state.InventoryStateBudget{}, context.DeadlineExceeded
					case "expired_wall":
						wallOffset.Store(int64(3 * time.Second))
					case "expired_elapsed":
						elapsedOffset.Store(int64(3 * time.Second))
					case "rollback":
						wallOffset.Store(-int64(time.Second))
					}
					return r, nil
				}}
			noRetirementSource(t, &o, &sources)
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.InventoryState != nil && s.InventoryState.LastAttemptPhase == "after_receipt"
			})
			before := control(t, dir, "status")
			if samples.Load() != 2 || sources.Load() != 0 || before.Dispatch.Used != 1 || before.InventoryState.LastDecisionStatus != "source_deferred" || before.CPUFeedback.Window != nil || before.ActiveJob != 0 {
				t.Fatal(mode, before, samples.Load(), sources.Load())
			}
			wallOffset.Store(int64(time.Hour))
			control(t, dir, "pause")
			control(t, dir, "resume")
			time.Sleep(15 * time.Millisecond)
			after := control(t, dir, "status")
			if samples.Load() != 2 || after.Dispatch.Used > 1 || sources.Load() != 0 || !reflect.DeepEqual(before.InventoryState, after.InventoryState) {
				t.Fatal("charged abort renewed/erased cadence or retry", before, after, samples.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

type inventoryStateJobEvidence struct {
	ID           int64
	Kind, Status string
	Path, Cursor []byte
}

func inventoryStateSavedJobs(t *testing.T, dir string) []inventoryStateJobEvidence {
	t.Helper()
	u := url.URL{Scheme: "file", Path: filepath.Join(dir, state.Filename), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id,kind,status,path,cursor FROM jobs ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []inventoryStateJobEvidence
	for rows.Next() {
		var item inventoryStateJobEvidence
		if err = rows.Scan(&item.ID, &item.Kind, &item.Status, &item.Path, &item.Cursor); err != nil {
			t.Fatal(err)
		}
		result = append(result, item)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestWorkerInventoryStateRealThresholdRetainsHistoryAcrossRestart(t *testing.T) {
	ctx := context.Background()
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MaxStateBytes = config.MinStateBytes
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(ctx, c.Roots); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err = w.EnqueueJob(ctx, 1, "owner-history", []byte(fmt.Sprintf("history-%02d", i)), time.Now()); err != nil {
			t.Fatal(err)
		}
		j, e := w.ClaimJob(ctx, []string{"owner-history"}, time.Now(), time.Minute)
		if e != nil || j == nil {
			t.Fatal(j, e)
		}
		if err = w.FinishJob(ctx, *j, false, bytes.Repeat([]byte{byte(i)}, state.MaxCursorBytes), time.Now().Add(24*time.Hour), ""); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.EnqueueJob(ctx, 1, state.ScanKind, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if err = w.FinishJob(ctx, *j, false, []byte("retained unfinished cursor"), time.Unix(0, 1), ""); err != nil {
		t.Fatal(err)
	}
	report, err := w.InventoryStateBudget(ctx, c.Scan.MaxStateBytes)
	if err != nil || !report.Available || report.Status != "limit_reached" {
		t.Fatal("fixture did not reach actual logical threshold", report, err)
	}
	w.Close()
	before := inventoryStateSavedJobs(t, dir)
	var sources atomic.Int64
	for run := 0; run < 2; run++ {
		o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
		noRetirementSource(t, &o, &sources)
		_, done := start(t, dir, c, o)
		waitUntil(t, func() bool {
			s := control(t, dir, "status")
			return s.InventoryState.Observation != nil && s.InventoryState.Observation.Status == "limit_reached"
		})
		s := control(t, dir, "status")
		if sources.Load() != 0 || s.Dispatch.Used != 0 || s.CPUFeedback.Window != nil || s.InventoryState.Observation.TotalBytes == nil || *s.InventoryState.Observation.TotalBytes < c.Scan.MaxStateBytes {
			t.Fatal(s, sources.Load())
		}
		control(t, dir, "pause")
		control(t, dir, "resume")
		control(t, dir, "stop")
		waitExit(t, done)
		if after := inventoryStateSavedJobs(t, dir); !reflect.DeepEqual(before, after) {
			t.Fatal("threshold or restart changed owner history/pending cursor", len(before), len(after))
		}
	}
}

func TestWorkerInventoryStateMidPassRetainsExactCursor(t *testing.T) {
	root := fairWorkerFiles(t, 400)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.PauseOnBattery = false
	c.Scan.MetadataPerSecond = 100000
	var calls atomic.Int64
	var mu sync.Mutex
	var saved []inventoryStateJobEvidence
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
			n := calls.Add(1)
			if n == 3 {
				rows := inventoryStateSavedJobs(t, dir)
				mu.Lock()
				saved = rows
				mu.Unlock()
			}
			if n >= 3 {
				return fixtureInventoryStateReport(limit, limit, time.Now()), nil
			}
			return fixtureInventoryStateReport(limit, 1, time.Now()), nil
		}}
	_, done := start(t, dir, c, o)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_state_source_backoff" })
	mu.Lock()
	before := saved
	mu.Unlock()
	if len(before) == 0 || len(before[0].Cursor) == 0 || before[0].Status != "pending" {
		t.Fatal("fixture did not save unfinished source progress", before)
	}
	if !reflect.DeepEqual(before, inventoryStateSavedJobs(t, dir)) || calls.Load() != 3 {
		t.Fatal("threshold changed the unfinished source cursor")
	}
	control(t, dir, "stop")
	waitExit(t, done)
	// Reopening resamples, but refuses without constructing a new scanner.
	var sourceCalls atomic.Int64
	o.inventoryStateObserve = func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
		return fixtureInventoryStateReport(limit, limit, time.Now()), nil
	}
	noRetirementSource(t, &o, &sourceCalls)
	_, done = start(t, dir, c, o)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_state_source_backoff" })
	control(t, dir, "stop")
	waitExit(t, done)
	if sourceCalls.Load() != 0 || !reflect.DeepEqual(before, inventoryStateSavedJobs(t, dir)) {
		t.Fatal("restart changed or replayed pending source")
	}
}

func TestWorkerInventoryStateReturnedCancellationRetainsChargedReceipt(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.PauseOnBattery = false
	var samples, sources atomic.Int64
	var runCancel context.CancelFunc
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		inventoryStateObserve: func(ctx context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
			if samples.Add(1) == 2 {
				runCancel()
				return state.InventoryStateBudget{}, ctx.Err()
			}
			return fixtureInventoryStateReport(limit, 1, time.Now()), nil
		}}
	noRetirementSource(t, &o, &sources)
	_, done, _ := startCancelable(t, dir, c, o, func(cancel context.CancelFunc, _ <-chan error) { runCancel = cancel })
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	budget, err := r.DispatchBudget(context.Background(), time.Now(), c.Scan.MaxScanChunksPerDay)
	if err != nil || budget.Used != 1 || samples.Load() != 2 || sources.Load() != 0 {
		t.Fatal(budget, err, samples.Load(), sources.Load())
	}
	feedback, err := r.CPUFeedback(context.Background())
	if err != nil || feedback.Window != nil {
		t.Fatal(feedback, err)
	}
}

func TestWorkerInventoryStateRetryRequiresBothClocks(t *testing.T) {
	for _, first := range []string{"wall", "elapsed"} {
		t.Run(first, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			c.Scan.PauseOnBattery = false
			var samples, sourceSteps, wallOffset, elapsedOffset atomic.Int64
			wall := func() time.Time { return time.Now().Add(time.Duration(wallOffset.Load())).UTC() }
			elapsed := func() time.Time { return time.Now().Add(time.Duration(elapsedOffset.Load())) }
			o := Options{ExperimentalScan: true, Interval: 80 * time.Millisecond, WorkDuration: time.Second, wallNow: wall, elapsedNow: elapsed,
				inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
					if samples.Add(1) == 1 {
						return fixtureInventoryStateReport(limit, limit, wall()), nil
					}
					return fixtureInventoryStateReport(limit, 1, wall()), nil
				}}
			o.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
				sourceSteps.Add(1)
				return scanner.NextPermitted(ctx, job, permit)
			}
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_state_source_backoff" })
			before := control(t, dir, "status").InventoryState
			if first == "wall" {
				wallOffset.Store(int64(6 * time.Minute))
			} else {
				elapsedOffset.Store(int64(6 * time.Minute))
			}
			control(t, dir, "pause")
			control(t, dir, "resume")
			time.Sleep(15 * time.Millisecond)
			one := control(t, dir, "status")
			if samples.Load() != 1 || one.Dispatch.Used != 0 || one.InventoryMetrics != nil || !reflect.DeepEqual(before, one.InventoryState) {
				t.Fatal("one clock erased retry", one, samples.Load())
			}
			wallOffset.Store(int64(6 * time.Minute))
			elapsedOffset.Store(int64(6 * time.Minute))
			control(t, dir, "pause")
			control(t, dir, "resume")
			waitUntil(t, func() bool { s := control(t, dir, "status"); return s.InventoryMetrics != nil && s.ActiveJob == 0 })
			control(t, dir, "pause")
			waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
			if sourceSteps.Load() < 1 || samples.Load() != 1+2*sourceSteps.Load() {
				t.Fatal("retry skipped fresh pre/post admission samples", samples.Load(), sourceSteps.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerInventoryStateCachedPowerWaitDoesNotResample(t *testing.T) {
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
	var samples, sources, generic atomic.Int64
	o := Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }},
		inventoryStateObserve: func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return fixtureInventoryStateReport(limit, 1, time.Now()), nil
		}}
	noRetirementSource(t, &o, &sources)
	var cancel context.CancelFunc
	var done <-chan error
	cleanupWorkerPowerFixture(t, p, release, &cancel, &done)
	_, done, cancel = startCancelable(t, dir, c, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitPowerEntered(t, entered)
	waitUntil(t, func() bool {
		return generic.Load() == 1 && control(t, dir, "status").WaitReason == "power_source_backoff"
	})
	if samples.Load() != 1 || sources.Load() != 0 {
		t.Fatal("cached power gate repeated state probes", samples.Load(), sources.Load())
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	if err != nil || summary.Entries != 4 {
		t.Fatal("power gate blocked eligible maintenance", summary, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerInventoryStateClaimUsesValidatedClockReading(t *testing.T) {
	for _, mode := range []string{"boundary", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			c.Scan.PauseOnBattery = false
			var samples, sources, armedReads, stamp atomic.Int64
			wall := func() time.Time {
				n := armedReads.Load()
				if n < 0 {
					return time.Now().UTC()
				}
				n = armedReads.Add(1)
				reserved := time.Unix(0, stamp.Load()).UTC()
				if n == 1 {
					return reserved.Add(state.FairInventoryClaimWindow - 2*time.Nanosecond)
				}
				if n == 2 {
					return reserved.Add(state.FairInventoryClaimWindow - time.Nanosecond)
				}
				if mode == "rollback" {
					return reserved.Add(-time.Nanosecond)
				}
				return reserved.Add(state.FairInventoryClaimWindow)
			}
			armedReads.Store(-1)
			var cancelRun context.CancelFunc
			var begins atomic.Int64
			o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: wall,
				inventoryStateObserve: func(sampleCtx context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
					r := fixtureInventoryStateReport(limit, 1, wall())
					if samples.Add(1) == 2 {
						u := url.URL{Scheme: "file", Path: filepath.Join(dir, state.Filename), RawQuery: "mode=ro"}
						db, err := sql.Open("sqlite", u.String())
						if err != nil {
							t.Fatal(err)
						}
						var reserved int64
						err = db.QueryRowContext(sampleCtx, "SELECT last_start_ns FROM scan_dispatch WHERE id=1").Scan(&reserved)
						db.Close()
						if err != nil {
							t.Fatal(err)
						}
						stamp.Store(reserved)
						armedReads.Store(0)
					}
					return r, nil
				},
				cpuBegin: func(_ context.Context, _ *state.Store, _ *state.FairInventoryTurn, now time.Time, _ state.CPUWindowStart) (state.CPUWindowMarker, error) {
					begins.Add(1)
					cancelRun()
					return state.CPUWindowMarker{}, state.ErrCPUFeedbackDeferred
				}}
			noRetirementSource(t, &o, &sources)
			_, done, _ := startCancelable(t, dir, c, o, func(cancel context.CancelFunc, _ <-chan error) { cancelRun = cancel })
			waitExit(t, done)
			if begins.Load() != 1 || samples.Load() != 2 || sources.Load() != 0 {
				t.Fatal("claim took a new unvalidated clock reading", begins.Load(), samples.Load(), sources.Load())
			}
		})
	}
}
