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
			o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: mode != "idle", powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
				inventoryStateObserve: func(context.Context, state.InventoryStateBudgetSampler, int64) (state.InventoryStateBudget, error) {
					samples.Add(1)
					return state.InventoryStateBudget{}, errors.New("unexpected sample")
				}})
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
			var samples, probes, sources, cpuReads atomic.Int64
			p := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				probes.Add(1)
				return powerinfo.Observation{}, nil
			}, nil, nil)
			o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
				inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
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
				}})
			noRetirementSource(t, &o, &sources)
			o.cpuObserve = func() (time.Duration, error) { cpuReads.Add(1); return 0, nil }
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.WaitReason == "inventory_state_source_backoff" && s.InventoryState != nil && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil && s.InventoryState.RetryAfterAt != nil
			})
			before := control(t, dir, "status")
			beforeCPU := cpuReads.Load()
			if samples.Load() != 1 || probes.Load() != 0 || sources.Load() != 0 || before.Dispatch.Used != 0 || before.CPUFeedback.Window != nil || before.InventoryState.LastDecisionStatus != "source_deferred" || before.InventoryState.RetryAfterAt == nil {
				t.Fatal(before, samples.Load(), probes.Load(), sources.Load())
			}
			if before.InventoryState.LastAttemptStartedAt == nil || !before.InventoryState.RetryAfterAt.Equal(before.InventoryState.LastAttemptStartedAt.Add(inventoryStateRetryInterval)) {
				t.Fatal("completion renewed the frozen retry anchor", before.InventoryState)
			}
			for _, command := range []string{"status", "pause", "status", "resume", "status"} {
				control(t, dir, command)
			}
			time.Sleep(20 * time.Millisecond)
			after := control(t, dir, "status")
			if samples.Load() != 1 || cpuReads.Load() != beforeCPU || !reflect.DeepEqual(before.InventoryState, after.InventoryState) || after.Dispatch.Used != 0 {
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
	o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }},
		inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return fixtureInventoryStateReport(limit, limit, time.Now()), nil
		}})
	noRetirementSource(t, &o, &sources)
	// This finite pre result must be returned before the existing maintenance
	// can revoke a still-pending SOURCE intent. Production has no such wait.
	o.inventoryStatePhaseStarted = returnedInventoryStatePrePhaseForFixture
	_, done := start(t, dir, c, o)
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return generic.Load() == 1 && s.WaitReason == "inventory_state_source_backoff" && s.InventoryState != nil && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil
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
			o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: 80 * time.Millisecond, WorkDuration: time.Second, wallNow: wall, elapsedNow: elapsed,
				inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
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
				}})
			noRetirementSource(t, &o, &sources)
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.InventoryState != nil && s.InventoryState.LastAttemptPhase == "after_receipt" && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil && s.InventoryState.LastDecisionReason != "inventory_state_pending"
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
	o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second})
	for run := 0; run < 2; run++ {
		noRetirementSource(t, &o, &sources)
		_, done := start(t, dir, c, o)
		waitUntil(t, func() bool {
			s := control(t, dir, "status")
			return (run == 0 && s.InventoryState != nil && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil && s.InventoryState.Observation != nil && s.InventoryState.Observation.Status == "limit_reached") || (run == 1 && s.WaitReason == "inventory_state_source_backoff")
		})
		s := control(t, dir, "status")
		if sources.Load() != 0 || (s.Dispatch != nil && s.Dispatch.Used != 0) || s.CPUFeedback.Window != nil || (run == 0 && (s.InventoryState.Observation.TotalBytes == nil || *s.InventoryState.Observation.TotalBytes < c.Scan.MaxStateBytes)) || (run == 1 && s.InventoryState.Observation != nil) {
			t.Fatal(s, sources.Load())
		}
		// A retained process retry can defer SOURCE before a restarted live query.
		// Require the saved zero charge on both Runs while preserving every job.
		readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
		reader, readErr := state.OpenReader(readCtx, dir)
		if readErr != nil {
			readCancel()
			t.Fatal(readErr)
		}
		budget, budgetErr := reader.DispatchBudget(readCtx, time.Now(), c.Scan.MaxScanChunksPerDay)
		closeErr := reader.Close()
		readCancel()
		if budgetErr != nil || closeErr != nil || budget.Used != 0 {
			t.Fatal("threshold or retained retry acquired charged work", budget, budgetErr, closeErr)
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
	o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
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
		}})
	_, done := start(t, dir, c, o)
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.WaitReason == "inventory_state_source_backoff" && s.InventoryState != nil && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil && s.InventoryState.Observation != nil && s.InventoryState.Observation.Status == "limit_reached"
	})
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
	// The same fixture process retains the prior failed-attempt retry.
	// Reopening cannot resample or reconstruct the old scope report.
	var sourceCalls atomic.Int64
	o.inventoryStateObserve = func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
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
	o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second,
		inventoryStateObserve: func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			if samples.Add(1) == 2 {
				runCancel()
				return state.InventoryStateBudget{}, ctx.Err()
			}
			return fixtureInventoryStateReport(limit, 1, time.Now()), nil
		}})
	noRetirementSource(t, &o, &sources)
	_, done, _ := startCancelable(t, dir, c, o, func(cancel context.CancelFunc, _ <-chan error) { runCancel = cancel })
	// Run preserves its canceled lifetime after the routing session finishes.
	// Reject any added finish error rather than accepting errors.Is alone.
	select {
	case exitErr := <-done:
		joined, ok := exitErr.(interface{ Unwrap() []error })
		if !ok {
			t.Fatal("worker did not return its joined cancellation cause", exitErr)
		}
		causes := joined.Unwrap()
		if len(causes) != 1 || causes[0] != context.Canceled {
			t.Fatal("worker returned an unexpected finish cause", exitErr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("worker did not stop")
	}
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

// Keep only the current phase pair. Unused or refused attempts may be replaced;
// every source entry must consume its own completed usable pair exactly once.
type inventoryStateSourcePairForFixture struct {
	mu             sync.Mutex
	pre, post      inventoryStateSourcePhaseForFixture
	lastConsumedID uint64
	consumed       int64
	failure        error
}

type inventoryStateSourcePhaseForFixture struct {
	returned   bool
	id         uint64
	request    inventoryStateRequest
	launch     inventoryStateClockPair
	completion inventoryStateCompletion
}

func (p *inventoryStateSourcePairForFixture) record(phase string, ticket *inventoryStateTicket) {
	result, returned := ticket.completed()
	evidence := inventoryStateSourcePhaseForFixture{returned: returned, completion: result}
	if returned {
		// These ticket values are immutable. Completion publication is joined
		// before reading them; no mutable intent or coordinator state is read.
		evidence.id, evidence.request, evidence.launch = ticket.ID, ticket.request, ticket.launch
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if phase == "before_dispatch" {
		p.pre, p.post = evidence, inventoryStateSourcePhaseForFixture{}
	} else {
		p.post = evidence
	}
}

func (p inventoryStateSourcePhaseForFixture) usable(phase string) bool {
	r := p.completion
	return p.returned && p.id != 0 && p.request.scope != nil && p.request.intent != nil && p.request.phase == phase &&
		r.ID == p.id && r.Phase == phase && r.Usable && r.Reason == "" && r.Observation != nil &&
		r.Observation.Available && r.Observation.Status == "below_limit" &&
		r.LaunchWall.Equal(p.launch.wall) && r.LaunchElapsed.Equal(p.launch.elapsed) &&
		!r.CompletedWall.Before(r.LaunchWall) && !r.CompletedElapsed.Before(r.LaunchElapsed)
}

func (p *inventoryStateSourcePairForFixture) consume() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	pre, post := p.pre, p.post
	p.pre, p.post = inventoryStateSourcePhaseForFixture{}, inventoryStateSourcePhaseForFixture{}
	var err error
	switch {
	case !pre.usable("before_dispatch") || !post.usable("after_receipt"):
		err = errors.New("source entry lacks a completed usable pre/post state pair")
	case pre.request.scope != post.request.scope || pre.request.intent != post.request.intent || pre.request.receipt != nil || post.request.receipt == nil || post.request.receipt.intent != post.request.intent:
		err = errors.New("source entry has mismatched pre/post state requests")
	case post.launch.wall.Before(pre.completion.CompletedWall) || post.launch.elapsed.Before(pre.completion.CompletedElapsed):
		err = errors.New("source entry has a post phase before its pre completion")
	case pre.id <= p.lastConsumedID || post.id <= pre.id:
		err = errors.New("source entry reused a state completion ticket")
	default:
		p.lastConsumedID = post.id
		p.consumed++
	}
	if err != nil && p.failure == nil {
		p.failure = err
	}
	return err
}

func (p *inventoryStateSourcePairForFixture) result() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.consumed, p.failure
}

func newInventoryStateSourceTicketForFixture(phase string, id uint64, scope *inventoryStateRunScope, intent *inventoryStateIntent, now time.Time) *inventoryStateTicket {
	ticket := &inventoryStateTicket{
		ID: id, request: inventoryStateRequest{scope: scope, intent: intent, phase: phase},
		launch: inventoryStateClockPair{wall: now, elapsed: now}, done: make(chan struct{}),
		completion: inventoryStateCompletion{ID: id, Phase: phase, LaunchWall: now, LaunchElapsed: now, CompletedWall: now, CompletedElapsed: now, Usable: true,
			Observation: &state.InventoryStateBudget{Available: true, Status: "below_limit"}},
	}
	if phase == "after_receipt" {
		ticket.request.receipt = &inventoryStateReceipt{intent: intent}
	}
	return ticket
}

func TestInventoryStateSourcePairForFixtureRejectsInvalidEvidence(t *testing.T) {
	for _, mode := range []string{"missing_post", "refused_pre", "refused_post", "mismatched_intent", "mismatched_receipt", "mismatched_completion", "post_before_pre_completion", "duplicate_ids", "reused_pair"} {
		t.Run(mode, func(t *testing.T) {
			scope, intent := &inventoryStateRunScope{}, &inventoryStateIntent{}
			now := time.Now()
			pre := newInventoryStateSourceTicketForFixture("before_dispatch", 1, scope, intent, now)
			post := newInventoryStateSourceTicketForFixture("after_receipt", 2, scope, intent, now)
			switch mode {
			case "refused_pre":
				pre.completion.Usable, pre.completion.Reason = false, "inventory_state_limit"
			case "refused_post":
				post.completion.Usable, post.completion.Reason = false, "inventory_state_canceled"
			case "mismatched_intent":
				post.request.intent = &inventoryStateIntent{}
			case "mismatched_receipt":
				post.request.receipt.intent = &inventoryStateIntent{}
			case "mismatched_completion":
				post.completion.ID++
			case "post_before_pre_completion":
				pre.completion.CompletedWall = now.Add(time.Millisecond)
				pre.completion.CompletedElapsed = now.Add(time.Millisecond)
			case "duplicate_ids":
				post.ID, post.completion.ID = pre.ID, pre.ID
			}
			close(pre.done)
			close(post.done)
			var pair inventoryStateSourcePairForFixture
			pair.record("before_dispatch", pre)
			if mode != "missing_post" {
				pair.record("after_receipt", post)
			}
			var wantConsumed int64
			if mode == "reused_pair" {
				if err := pair.consume(); err != nil {
					t.Fatal("valid pair refused", err)
				}
				wantConsumed = 1
				pair.record("before_dispatch", pre)
				pair.record("after_receipt", post)
			}
			if err := pair.consume(); err == nil {
				t.Fatal("invalid source evidence accepted")
			}
			if consumed, failure := pair.result(); consumed != wantConsumed || failure == nil {
				t.Fatal("invalid evidence was counted or lost", consumed, failure)
			}
		})
	}
}

func TestInventoryStateSourcePairForFixtureAllowsReplacedAttempt(t *testing.T) {
	scope, refusedIntent := &inventoryStateRunScope{}, &inventoryStateIntent{}
	now := time.Now()
	refused := newInventoryStateSourceTicketForFixture("before_dispatch", 1, scope, refusedIntent, now)
	refused.completion.Usable, refused.completion.Reason = false, "inventory_state_limit"
	close(refused.done)
	var pair inventoryStateSourcePairForFixture
	pair.record("before_dispatch", refused)
	if consumed, failure := pair.result(); consumed != 0 || failure != nil {
		t.Fatal("refused attempt poisoned the source witness", consumed, failure)
	}
	unusedIntent := &inventoryStateIntent{}
	unusedPre := newInventoryStateSourceTicketForFixture("before_dispatch", 2, scope, unusedIntent, now.Add(time.Millisecond))
	unusedPost := newInventoryStateSourceTicketForFixture("after_receipt", 3, scope, unusedIntent, now.Add(2*time.Millisecond))
	close(unusedPre.done)
	close(unusedPost.done)
	pair.record("before_dispatch", unusedPre)
	pair.record("after_receipt", unusedPost)
	if consumed, failure := pair.result(); consumed != 0 || failure != nil {
		t.Fatal("unused pair poisoned the source witness", consumed, failure)
	}
	intent := &inventoryStateIntent{}
	pre := newInventoryStateSourceTicketForFixture("before_dispatch", 4, scope, intent, now.Add(3*time.Millisecond))
	post := newInventoryStateSourceTicketForFixture("after_receipt", 5, scope, intent, now.Add(4*time.Millisecond))
	close(pre.done)
	close(post.done)
	pair.record("before_dispatch", pre)
	pair.record("after_receipt", post)
	if err := pair.consume(); err != nil {
		t.Fatal("replacement usable pair refused", err)
	}
	if consumed, failure := pair.result(); consumed != 1 || failure != nil {
		t.Fatal("replacement source witness was lost", consumed, failure)
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
			o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: 80 * time.Millisecond, WorkDuration: time.Second, wallNow: wall, elapsedNow: elapsed,
				inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
					if samples.Add(1) == 1 {
						return fixtureInventoryStateReport(limit, limit, wall()), nil
					}
					return fixtureInventoryStateReport(limit, 1, wall()), nil
				}})
			var sourcePair inventoryStateSourcePairForFixture
			o.inventoryStatePhaseStarted = func(phase string, ticket *inventoryStateTicket) error {
				if err := returnedInventoryStateFinitePhaseForFixture(phase, ticket); err != nil {
					return err
				}
				sourcePair.record(phase, ticket)
				return nil
			}
			o.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
				sourceSteps.Add(1)
				// Retain a failed witness for the owning test, but always exercise
				// the real scanner rather than failing from its handler goroutine.
				_ = sourcePair.consume()
				return scanner.NextPermitted(ctx, job, permit)
			}
			_, done := start(t, dir, c, o)
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.WaitReason == "inventory_state_source_backoff" && s.InventoryState != nil && s.InventoryState.CallbackReturned && !s.InventoryState.ProcessSlotOccupied && s.InventoryState.LastAttemptFinishedAt != nil && s.InventoryState.RetryAfterAt != nil
			})
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
			consumed, witnessErr := sourcePair.result()
			if sourceSteps.Load() < 1 || consumed != sourceSteps.Load() || witnessErr != nil {
				t.Fatal("retry skipped fresh completed pre/post admission samples", samples.Load(), sourceSteps.Load(), consumed, witnessErr)
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
	o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, powerCoordinator: p, Interval: time.Millisecond, WorkDuration: time.Second,
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }},
		inventoryStateObserve: func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			samples.Add(1)
			return fixtureInventoryStateReport(limit, 1, time.Now()), nil
		}})
	noRetirementSource(t, &o, &sources)
	o.inventoryStatePhaseStarted = returnedInventoryStatePrePhaseForFixture
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

// Only explicitly selected finite-observer fixtures install this hook. It is
// outside coordinator locks and never waits on a deliberately blocked pre call.
// Its five-second failure bound is separate from production control latency.
func returnedInventoryStatePrePhaseForFixture(phase string, ticket *inventoryStateTicket) error {
	if phase != "before_dispatch" {
		return nil
	}
	if ticket == nil {
		return errors.New("finite pre phase did not receive its exact ticket")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ticket.done:
		return nil
	case <-timer.C:
		return errors.New("finite pre callback did not return within five seconds")
	}
}

func TestWorkerInventoryStateClaimUsesValidatedClockReading(t *testing.T) {
	for _, mode := range []string{"boundary", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			root := fairWorkerFiles(t, 1)
			dir := filepath.Join(t.TempDir(), "state")
			c := config.Default()
			c.Roots = []string{root}
			c.Scan.PauseOnBattery = false
			var samples, sources, stamp, fixedClaimWall, jumpedWall atomic.Int64
			wall := func() time.Time {
				if jumped := jumpedWall.Load(); jumped != 0 {
					return time.Unix(0, jumped).UTC()
				}
				if fixed := fixedClaimWall.Load(); fixed != 0 {
					return time.Unix(0, fixed).UTC()
				}
				return time.Now().UTC()
			}
			var cancelRun context.CancelFunc
			var begins atomic.Int64
			o := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, wallNow: wall,
				inventoryStateObserve: func(sampleCtx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
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
						// Freeze one exact post-capture/claim clock, independent of
						// how many serialized async observations precede the claim.
						fixedClaimWall.Store(time.Now().UTC().UnixNano())
					}
					return r, nil
				},
				cpuBegin: func(_ context.Context, _ *state.Store, turn *state.FairInventoryTurn, now time.Time, _ state.CPUWindowStart) (state.CPUWindowMarker, error) {
					begins.Add(1)
					reserved := time.Unix(0, stamp.Load()).UTC()
					if turn == nil || turn.Job == nil {
						t.Error("missing exact source claim")
						cancelRun()
						return state.CPUWindowMarker{}, state.ErrCPUFeedbackDeferred
					}
					claimed := turn.Job.LeaseUntil.Add(-time.Second - 10*time.Second)
					if !now.Equal(claimed) || !claimed.Equal(time.Unix(0, fixedClaimWall.Load()).UTC()) || claimed.Before(reserved) || claimed.Sub(reserved) >= state.FairInventoryClaimWindow {
						t.Error("claim and Begin did not share the validated original-receipt wall", claimed, now, reserved)
					}
					if mode == "rollback" {
						jumpedWall.Store(reserved.Add(-time.Nanosecond).UnixNano())
					} else {
						jumpedWall.Store(reserved.Add(state.FairInventoryClaimWindow).UnixNano())
					}
					cancelRun()
					return state.CPUWindowMarker{}, state.ErrCPUFeedbackDeferred
				}})
			noRetirementSource(t, &o, &sources)
			_, done, _ := startCancelable(t, dir, c, o, func(cancel context.CancelFunc, _ <-chan error) { cancelRun = cancel })
			// Run preserves its canceled lifetime after the routing session finishes.
			// Reject any added finish error rather than accepting errors.Is alone.
			select {
			case exitErr := <-done:
				joined, ok := exitErr.(interface{ Unwrap() []error })
				if !ok {
					t.Fatal("worker did not return its joined cancellation cause", exitErr)
				}
				causes := joined.Unwrap()
				if len(causes) != 1 || causes[0] != context.Canceled {
					t.Fatal("worker returned an unexpected finish cause", exitErr)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("worker did not stop")
			}
			if !asyncSavedDispatchStart(t, dir).Equal(time.Unix(0, stamp.Load()).UTC()) {
				t.Fatal("charged receipt anchor changed")
			}
			for _, job := range inventoryStateSavedJobs(t, dir) {
				if job.Status != "pending" || len(job.Cursor) != 0 {
					t.Fatal("late clock changed unstarted progress", job)
				}
			}
			if begins.Load() != 1 || samples.Load() != 2 || sources.Load() != 0 {
				t.Fatal("claim took a new unvalidated clock reading", begins.Load(), samples.Load(), sources.Load())
			}
		})
	}
}
