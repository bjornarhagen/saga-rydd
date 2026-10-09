package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func sessionWorkerFixture(t *testing.T) (string, config.Config) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	cfg := config.Default()
	root := filepath.Join(t.TempDir(), "generated-source")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Roots = []string{root}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), cfg.Roots); err != nil {
		t.Fatal(err)
	}
	w.Close()
	cfg.Scan.CPUSessionCharges = true
	// Pure capacity refuses source work, leaving generic fixture jobs available.
	cfg.Scan.APIAttemptsPerSecond = 1
	cfg.Scan.PauseOnBattery = false
	return dir, cfg
}

func sessionSaved(t *testing.T, dir string) state.CPUChargeState {
	t.Helper()
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	saved, err := r.CPUCharges(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func TestCPUSessionWorkerIdleAndDefaultOffDoNotActivateOrObserve(t *testing.T) {
	for _, mode := range []string{"idle_flag", "experimental_off"} {
		t.Run(mode, func(t *testing.T) {
			dir, fixtureCfg := sessionWorkerFixture(t)
			cfg := fixtureCfg
			options := Options{Interval: 10 * time.Millisecond, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { t.Error("inactive gate sampled native CPU"); return 0, nil }}
			if mode == "experimental_off" {
				options.ExperimentalScan = true
				cfg.Scan.CPUSessionCharges = false
			}
			_, done := start(t, dir, cfg, options)
			for i := 0; i < 3; i++ {
				control(t, dir, "status")
				control(t, dir, "pause")
				control(t, dir, "resume")
			}
			if sessionSaved(t, dir).Available {
				t.Fatal("inactive gate activated history")
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestCPUSessionWorkerFiniteBoundariesAndCachedControls(t *testing.T) {
	dir, fixtureCfg := sessionWorkerFixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var observations, handled atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		cpuSessionObserve: func() (time.Duration, error) { observations.Add(1); return 0, nil },
		Handlers:          map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { handled.Add(1); return Result{Done: true}, nil }},
	}
	_, done := start(t, dir, fixtureCfg, options)
	waitUntil(t, func() bool {
		return sessionSaved(t, dir).Session != nil && sessionSaved(t, dir).Session.LastOrdinal >= 2
	})
	if observations.Load() != 3 || handled.Load() != 1 {
		t.Fatal("unexpected native sample/handler count", observations.Load(), handled.Load())
	}
	for i := 0; i < 4; i++ {
		control(t, dir, "pause")
		control(t, dir, "status")
		control(t, dir, "resume")
	}
	if observations.Load() != 3 {
		t.Fatal("idle controls polled native CPU", observations.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
	saved := sessionSaved(t, dir)
	if observations.Load() != 4 || saved.Status != "finished" || saved.UnknownTailSessions != 1 || saved.Session.LastOrdinal != 3 {
		t.Fatal(observations.Load(), saved)
	}
}

func TestCPUSessionWorkerUnknownKeepsControlsButDispatchesNothing(t *testing.T) {
	dir, fixtureCfg := sessionWorkerFixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var observations, handled atomic.Int64
	_, done := start(t, dir, fixtureCfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		cpuSessionObserve: func() (time.Duration, error) { observations.Add(1); return 0, errors.New("generated unknown SELF") },
		Handlers:          map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { handled.Add(1); return Result{Done: true}, nil }},
	})
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_session_unknown" })
	for i := 0; i < 3; i++ {
		control(t, dir, "pause")
		control(t, dir, "resume")
		control(t, dir, "status")
	}
	if observations.Load() != 1 || handled.Load() != 0 {
		t.Fatal(observations.Load(), handled.Load())
	}
	control(t, dir, "stop")
	if err := <-done; !errors.Is(err, state.ErrCPUChargesUnknown) {
		t.Fatal(err)
	}
	if got := sessionSaved(t, dir); got.Generation != 1 || got.Status != "unknown" {
		t.Fatal(got)
	}
}

func TestCPUSessionWorkerExplicitOptOutPreservesActivatedHistory(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	_, firstDone := start(t, dir, cfg, Options{ExperimentalScan: true, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { return 0, nil }})
	waitUntil(t, func() bool { return sessionSaved(t, dir).Status == "active" })
	control(t, dir, "stop")
	waitExit(t, firstDone)
	before := sessionSaved(t, dir)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	cfg.Scan.CPUSessionCharges = false
	var handled atomic.Int64
	_, done := start(t, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { t.Error("opt-out sampled session CPU"); return 0, nil }, Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { handled.Add(1); return Result{Done: true}, nil }}})
	waitUntil(t, func() bool { return handled.Load() == 1 })
	control(t, dir, "stop")
	waitExit(t, done)
	after := sessionSaved(t, dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("opt-out rewrote/refunded charge history", before, after)
	}
}

func TestCPUSessionWorkerUncertainAdmissionKeepsFrozenRequestAndNoReceipt(t *testing.T) {
	dir, fixtureCfg := sessionWorkerFixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", []byte("old cursor"), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var samples, handled atomic.Int64
	hooks := &cpuSessionHooks{sample: func(ctx context.Context, w *state.Store, marker state.CPUSessionMarker, sample state.CPUSessionSample, finish bool) (state.CPUChargeState, error) {
		samples.Add(1)
		got, err := w.SampleCPUSession(ctx, marker, sample)
		if err != nil {
			return got, err
		}
		return got, state.ErrCPUChargesPublication
	}}
	_, done := start(t, dir, fixtureCfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { return 0, nil }, cpuSessionHooks: hooks,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { handled.Add(1); return Result{Done: true}, nil }},
	})
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_session_outcome_unknown" })
	for i := 0; i < 4; i++ {
		control(t, dir, "resume")
		control(t, dir, "status")
	}
	status := control(t, dir, "status")
	if samples.Load() != 1 || handled.Load() != 0 || status.Dispatch == nil || status.Dispatch.Used != 0 {
		t.Fatal(samples.Load(), handled.Load(), status)
	}
	control(t, dir, "stop")
	if err := <-done; !errors.Is(err, state.ErrCPUChargesPublication) {
		t.Fatal(err)
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.RunningJobs != 0 || summary.PendingJobs < 1 {
		t.Fatal(summary, err)
	}
}

func TestCPUSessionWorkerCancelSettlesHandlerThenFinishes(t *testing.T) {
	dir, fixtureCfg := sessionWorkerFixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", []byte("prior"), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	entered := make(chan struct{})
	_, done, cancel := startCancelable(t, dir, fixtureCfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { return 0, nil },
		Handlers: map[string]Handler{"fixture": func(ctx context.Context, _ state.Job) (Result, error) {
			close(entered)
			<-ctx.Done()
			return Result{}, ctx.Err()
		}},
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not join")
	}
	saved := sessionSaved(t, dir)
	if saved.Status != "finished" || saved.Session.LastOrdinal != 3 {
		t.Fatal("missing completion/final accounting", saved)
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.RunningJobs != 0 {
		t.Fatal("canceled lease not settled before finish", summary, err)
	}
}

func TestCPUSessionWorkerUncooperativeHandlerLeavesOpenRecoveryEvidence(t *testing.T) {
	dir, fixtureCfg := sessionWorkerFixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var observations atomic.Int64
	var ownCancel context.CancelFunc
	var ownDone <-chan error
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if ownCancel != nil {
			ownCancel()
		}
		if ownDone != nil {
			select {
			case <-ownDone:
			case <-time.After(8 * time.Second):
				t.Error("generated worker leaked")
			}
		}
		select {
		case <-entered:
		case <-time.After(time.Millisecond):
			return
		}
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("generated blocked handler leaked")
		}
	})
	_, done, _ := startCancelable(t, dir, fixtureCfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: 10 * time.Second,
		cpuSessionObserve: func() (time.Duration, error) { observations.Add(1); return 0, nil },
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
			defer close(exited)
			close(entered)
			<-release
			return Result{Done: true}, nil
		}},
	}, func(cancel context.CancelFunc, done <-chan error) { ownCancel, ownDone = cancel, done })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not entered")
	}
	control(t, dir, "stop")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not stop") {
			t.Fatal(err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("uncooperative stop was unbounded")
	}
	saved := sessionSaved(t, dir)
	if observations.Load() != 2 || saved.Status != "active" || !saved.OpenTailUnobserved || saved.Session.LastOrdinal != 1 {
		t.Fatal("fabricated graceful finish", observations.Load(), saved)
	}
}

func TestCPUSessionWorkerDoesNotSampleIneligibleSourceOrFutureWork(t *testing.T) {
	for _, gate := range []string{"daily", "metadata", "paused", "future", "state"} {
		t.Run(gate, func(t *testing.T) {
			dir, cfg := sessionWorkerFixture(t)
			var observations, sourceCalls atomic.Int64
			options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuSessionObserve: func() (time.Duration, error) { observations.Add(1); return 0, nil }, scannerNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
				sourceCalls.Add(1)
				return nil, errors.New("unexpected source admission")
			}}
			w, err := state.OpenWriter(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			switch gate {
			case "daily":
				cfg.Scan.APIAttemptsPerSecond = 0
				cfg.Scan.MaxScanChunksPerDay = 1
				_, err = w.ReserveScanChunk(context.Background(), time.Now(), time.Millisecond, 1)
			case "metadata":
				cfg.Scan.APIAttemptsPerSecond = 0
				cfg.Scan.MetadataAttemptsPerDay = 1
			case "paused":
				err = w.SetPaused(context.Background(), true)
			case "future":
				err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(time.Hour))
				options.Handlers = map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
					t.Error("future job dispatched")
					return Result{}, nil
				}}
			case "state":
				cfg.Scan.APIAttemptsPerSecond = 0
				options.inventoryStateObserve = func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
					return state.InventoryStateBudget{}, errors.New("generated unavailable state sample")
				}
			}
			w.Close()
			if err != nil {
				t.Fatal(err)
			}
			_, done := start(t, dir, cfg, options)
			waitUntil(t, func() bool { return sessionSaved(t, dir).Status == "active" })
			// A status request barriers behind the completed startup stage. For
			// state gating, wait for its finite fixed-backoff observation.
			if gate == "state" {
				waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "inventory_state_source_backoff" })
			}
			for i := 0; i < 3; i++ {
				control(t, dir, "status")
			}
			if observations.Load() != 1 || sourceCalls.Load() != 0 {
				t.Fatal("ineligible work sampled/admitted", observations.Load(), sourceCalls.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestCPUSessionLateElapsedRefusalRetainsReceiptAndUnstartedCursor(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	cfg.Scan.APIAttemptsPerSecond = 0
	wall := time.Now().UTC()
	var elapsedNS, observations, sourceCalls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		wallNow: func() time.Time { return wall }, elapsedNow: func() time.Time { return wall.Add(time.Duration(elapsedNS.Load())) },
		cpuSessionObserve: func() (time.Duration, error) {
			observations.Add(1)
			elapsedNS.Add(int64(time.Millisecond))
			return 0, nil
		},
		cpuBegin: func(ctx context.Context, w *state.Store, turn *state.FairInventoryTurn, at time.Time, start state.CPUWindowStart) (state.CPUWindowMarker, error) {
			marker, err := w.BeginCPUWindow(ctx, turn, at, start)
			elapsedNS.Add(-int64(3 * time.Millisecond))
			return marker, err
		},
		scannerNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
			sourceCalls.Add(1)
			return nil, errors.New("late gate admitted source")
		},
	}
	_, done := start(t, dir, cfg, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_session_unknown" })
	status := control(t, dir, "status")
	if sourceCalls.Load() != 0 || observations.Load() != 2 || status.Dispatch == nil || status.Dispatch.Used != 1 {
		t.Fatal("late refusal lost receipt/source scope", sourceCalls.Load(), observations.Load(), status)
	}
	control(t, dir, "stop")
	if err := <-done; !errors.Is(err, state.ErrCPUChargesUnknown) {
		t.Fatal(err)
	}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	job, err := w.ClaimJob(context.Background(), []string{state.ScanKind}, wall.Add(time.Second), time.Second)
	if err != nil || job == nil || len(job.Cursor) != 0 || string(job.Path) != "." {
		t.Fatal("unstarted exact listing was not preserved", job, err)
	}
}

// Inspect only the generated fixture's bounded saved maintenance controls. This
// independent SQL oracle detects cursor/proof changes before any row purge.
func sessionRetirementControls(t *testing.T, dir string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT 'r',root_id,hex(path),generation,hex(cursor) FROM subtree_reconcile
 UNION ALL SELECT 't',root_id,hex(path),scan_revision,printf('%d:%d',preserve_entry,phase) FROM subtree_retirement ORDER BY 1,2,3 LIMIT 129`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var kind, path, cursor string
		var root, generation int64
		if err = rows.Scan(&kind, &root, &path, &generation, &cursor); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%s:%d:%s:%d:%s", kind, root, path, generation, cursor))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(result) >= 129 {
		t.Fatal("fixture controls exceeded oracle bound")
	}
	return result
}

func TestCPUSessionLateMaintenanceRefusalLeavesExactRetirementProgress(t *testing.T) {
	dir, root, _ := workerRetirementFixture(t, 513)
	cfg := config.Default()
	cfg.Roots = []string{root}
	cfg.Scan.CPUSessionCharges = true
	cfg.Scan.PauseOnBattery = false
	before := sessionRetirementControls(t, dir)
	if len(before) == 0 {
		t.Fatal("fixture has no maintenance")
	}
	wall := time.Now().UTC()
	var elapsedNS, observations, sourceCalls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		wallNow: func() time.Time { return wall }, elapsedNow: func() time.Time { return wall.Add(time.Duration(elapsedNS.Load())) },
		cpuSessionObserve: func() (time.Duration, error) {
			observations.Add(1)
			elapsedNS.Add(int64(time.Millisecond))
			return 0, nil
		},
		cpuBegin: func(ctx context.Context, w *state.Store, turn *state.FairInventoryTurn, at time.Time, start state.CPUWindowStart) (state.CPUWindowMarker, error) {
			if turn.Kind != state.FairInventoryMaintenance {
				t.Error("fixture did not select maintenance")
			}
			marker, err := w.BeginCPUWindow(ctx, turn, at, start)
			elapsedNS.Add(-int64(3 * time.Millisecond))
			return marker, err
		},
	}
	noRetirementSource(t, &options, &sourceCalls)
	_, done := start(t, dir, cfg, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_session_unknown" })
	after := sessionRetirementControls(t, dir)
	status := control(t, dir, "status")
	if !reflect.DeepEqual(before, after) || observations.Load() != 2 || sourceCalls.Load() != 0 || status.Dispatch == nil || status.Dispatch.Used != 1 {
		t.Fatal("late maintenance mutated progress/lost receipt", before, after, observations.Load(), sourceCalls.Load(), status)
	}
	control(t, dir, "stop")
	if err := <-done; !errors.Is(err, state.ErrCPUChargesUnknown) {
		t.Fatal(err)
	}
}
