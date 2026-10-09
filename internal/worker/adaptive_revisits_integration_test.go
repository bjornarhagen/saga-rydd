package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type adaptiveWorkerFixture struct {
	dir, root string
	config    config.Config
	options   Options
	offset    atomic.Int64
	listings  atomic.Int64
}

func newAdaptiveWorkerFixture(t *testing.T, files int) *adaptiveWorkerFixture {
	t.Helper()
	f := &adaptiveWorkerFixture{dir: filepath.Join(t.TempDir(), "state"), root: t.TempDir(), config: config.Default()}
	f.config.Roots = []string{f.root}
	f.config.Scan.AdaptiveRevisits, f.config.Scan.PauseOnBattery = true, false
	f.config.Scan.MetadataPerSecond = 100000
	if err := os.Mkdir(filepath.Join(f.root, "deep"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(f.root, "deep", fmt.Sprintf("file-%03d", i)), []byte("generated fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.options = Options{ExperimentalScan: true, Interval: 5 * time.Millisecond, WorkDuration: time.Second, wallNow: f.wall, elapsedNow: f.elapsed,
		scannerNext: func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
			batch, err := scanner.NextPermitted(ctx, job, permit)
			if err == nil && batch.Complete && batch.Fault == "" && string(job.Path) == "." {
				f.listings.Add(1)
			}
			return batch, err
		}}
	return f
}

func (f *adaptiveWorkerFixture) wall() time.Time {
	return time.Now().Add(time.Duration(f.offset.Load())).UTC()
}
func (f *adaptiveWorkerFixture) elapsed() time.Time {
	return time.Now().Add(time.Duration(f.offset.Load()))
}
func (f *adaptiveWorkerFixture) pause(t *testing.T) {
	t.Helper()
	control(t, f.dir, "pause")
	waitUntil(t, func() bool { return control(t, f.dir, "status").ActiveJob == 0 })
}
func (f *adaptiveWorkerFixture) due(t *testing.T) time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := state.OpenReader(ctx, f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	due, err := r.NextJobDue(ctx, []string{state.ScanKind})
	if err != nil || due.IsZero() {
		t.Fatal(due, err)
	}
	return due
}
func (f *adaptiveWorkerFixture) awaitPass(t *testing.T, listings int64, weekly bool) Snapshot {
	t.Helper()
	var snapshot Snapshot
	waitUntil(t, func() bool {
		snapshot = control(t, f.dir, "status")
		p := snapshot.AdaptiveRevisits
		return f.listings.Load() >= listings && snapshot.ActiveJob == 0 && p != nil && p.ActiveEpochs == 0 && snapshot.WaitReason == "inventory_revisit" && (p.WeeklyRoots == 1) == weekly
	})
	if snapshot.AdaptiveRevisits.CurrentContentVerified || snapshot.AdaptiveRevisits.CleanupApproved || !snapshot.AdaptiveRevisits.HistoricalMetadataOnly {
		t.Fatal("adaptive summary invented current authority", snapshot)
	}
	return snapshot
}
func (f *adaptiveWorkerFixture) advance(t *testing.T) {
	t.Helper()
	f.pause(t)
	due := f.due(t)
	f.offset.Store(int64(due.Add(time.Second).Sub(time.Now())))
	control(t, f.dir, "resume")
}
func (f *adaptiveWorkerFixture) learnWeekly(t *testing.T) <-chan error {
	t.Helper()
	_, done := start(t, f.dir, f.config, f.options)
	f.awaitPass(t, 1, false)
	f.advance(t)
	f.awaitPass(t, 2, false)
	f.advance(t)
	snapshot := f.awaitPass(t, 3, true)
	if snapshot.AdaptiveRevisits.StableRoots != 1 || !f.due(t).After(f.wall().Add(6*24*time.Hour)) {
		t.Fatal("two completed unchanged epochs did not produce a seven-day due", snapshot)
	}
	return done
}

func TestWorkerAdaptiveRevisitsLearnOnlyDrainedMetadataAndDetectDeepChanges(t *testing.T) {
	for _, mutation := range []string{"edit", "add", "remove", "replace"} {
		t.Run(mutation, func(t *testing.T) {
			f := newAdaptiveWorkerFixture(t, 2)
			done := f.learnWeekly(t)
			f.pause(t)
			rootBefore, err := os.Stat(f.root)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.root, "deep", "file-000")
			switch mutation {
			case "edit":
				err = os.WriteFile(path, []byte("changed generated descendant"), 0600)
			case "add":
				err = os.WriteFile(filepath.Join(f.root, "deep", "added"), nil, 0600)
			case "remove":
				err = os.Remove(path)
			case "replace":
				replacement := filepath.Join(f.root, "deep", "replacement")
				if err = os.WriteFile(replacement, []byte("generated fixture"), 0600); err == nil {
					err = os.Rename(replacement, path)
				}
			}
			rootAfter, statErr := os.Stat(f.root)
			if err != nil || statErr != nil || !rootBefore.ModTime().Equal(rootAfter.ModTime()) {
				t.Fatal("fixture changed root timestamp instead of only descendants", err, statErr)
			}
			f.advance(t)
			snapshot := f.awaitPass(t, 4, false)
			if snapshot.AdaptiveRevisits.StableRoots != 0 || f.due(t).After(f.wall().Add(25*time.Hour)) {
				t.Fatal("deep metadata change retained quiet credit", snapshot)
			}
			control(t, f.dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerAdaptiveRevisitsScopeChangeAndDisablePreserveUntouchedWork(t *testing.T) {
	for _, mode := range []string{"scope_change", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := newAdaptiveWorkerFixture(t, 1)
			done := f.learnWeekly(t)
			f.pause(t)
			beforeDue := f.due(t)
			control(t, f.dir, "stop")
			waitExit(t, done)
			if mode == "disabled" {
				f.config.Scan.AdaptiveRevisits = false
			} else {
				f.config.Excludes = []string{filepath.Join(f.root, "not-present")}
			}
			beforeListings := f.listings.Load()
			ready, done := start(t, f.dir, f.config, f.options)
			afterDue := f.due(t)
			if !afterDue.Equal(beforeDue.Add(-6*24*time.Hour)) || f.listings.Load() != beforeListings {
				t.Fatal("policy change reset work or failed its exact daily shortening", beforeDue, afterDue, f.listings.Load())
			}
			if mode == "disabled" {
				if ready.AdaptiveRevisits != nil {
					t.Fatal("disabled mode publishes adaptive observations", ready)
				}
			} else if ready.AdaptiveRevisits == nil || ready.AdaptiveRevisits.StableRoots != 0 {
				t.Fatal("changed scope retained quiet credit", ready)
			}
			control(t, f.dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerAdaptiveRevisitsResourceGatesKeepUnfinishedPass(t *testing.T) {
	for _, gate := range []string{"dispatch", "metadata", "state"} {
		t.Run(gate, func(t *testing.T) {
			f := newAdaptiveWorkerFixture(t, 301)
			var sourceCalls atomic.Int64
			original := f.options.scannerNext
			f.options.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
				sourceCalls.Add(1)
				return original(ctx, scanner, job, permit)
			}
			if gate == "dispatch" {
				f.config.Scan.MaxScanChunksPerDay = 2
			} else if gate == "metadata" {
				f.config.Scan.MetadataAttemptsPerDay = 2*inventory.MaxAPIAttemptAllowance + 100
			} else {
				f.options.inventoryStateObserve = func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
					total := limit - 1
					if sourceCalls.Load() >= 2 {
						total = limit
					}
					return fixtureInventoryStateReport(limit, total, f.wall()), nil
				}
			}
			_, done := start(t, f.dir, f.config, f.options)
			want := map[string]string{"dispatch": "daily_chunk_limit", "metadata": "daily_metadata_limit", "state": "inventory_state_source_backoff"}[gate]
			waitUntil(t, func() bool { return control(t, f.dir, "status").WaitReason == want })
			before := control(t, f.dir, "status")
			if sourceCalls.Load() != 2 || before.AdaptiveRevisits == nil || before.AdaptiveRevisits.ActiveEpochs != 1 || before.AdaptiveRevisits.StableRoots != 0 || before.AdaptiveRevisits.WeeklyRoots != 0 {
				t.Fatal("partial pass learned stability or bypassed gate", gate, before, sourceCalls.Load())
			}
			for _, command := range []string{"status", "pause", "resume", "status"} {
				control(t, f.dir, command)
			}
			time.Sleep(20 * time.Millisecond)
			if sourceCalls.Load() != 2 || control(t, f.dir, "status").Dispatch.Used != before.Dispatch.Used {
				t.Fatal("controls admitted partial pass despite gate", sourceCalls.Load())
			}
			control(t, f.dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerAdaptiveRevisitsIdleAndCachedStatusDoNotProbe(t *testing.T) {
	for _, experimental := range []bool{false, true} {
		t.Run(fmt.Sprint(experimental), func(t *testing.T) {
			f := newAdaptiveWorkerFixture(t, 1)
			f.options.ExperimentalScan = experimental
			var samples, sources atomic.Int64
			f.options.inventoryStateObserve = func(_ context.Context, _ *state.Store, limit int64) (state.InventoryStateBudget, error) {
				samples.Add(1)
				return fixtureInventoryStateReport(limit, limit, f.wall()), nil
			}
			f.options.scannerNext = func(context.Context, *inventory.Scanner, state.Job, inventory.APIPermit) (state.ScanBatch, error) {
				sources.Add(1)
				return state.ScanBatch{}, fmt.Errorf("unexpected generated source dispatch")
			}
			_, done := start(t, f.dir, f.config, f.options)
			want := "idle"
			if experimental {
				want = "inventory_state_source_backoff"
			}
			waitUntil(t, func() bool { return control(t, f.dir, "status").WaitReason == want })
			before := control(t, f.dir, "status")
			for _, command := range []string{"status", "pause", "status", "resume", "status"} {
				control(t, f.dir, command)
			}
			after := control(t, f.dir, "status")
			wantedSamples := int64(0)
			if experimental {
				wantedSamples = 1
			}
			if sources.Load() != 0 || samples.Load() != wantedSamples || !reflect.DeepEqual(before.AdaptiveRevisits, after.AdaptiveRevisits) || !experimental && after.AdaptiveRevisits != nil {
				t.Fatal("idle/status/control sampled or refreshed adaptive evidence", before, after, samples.Load(), sources.Load())
			}
			control(t, f.dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerAdaptiveRevisitsReturnedFailureCannotEarnQuietCredit(t *testing.T) {
	for _, mode := range []string{"canceled", "fault"} {
		t.Run(mode, func(t *testing.T) {
			f := newAdaptiveWorkerFixture(t, 1)
			var armed atomic.Bool
			var failures atomic.Int64
			original := f.options.scannerNext
			f.options.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
				if armed.Swap(false) {
					failures.Add(1)
					if mode == "canceled" {
						return state.ScanBatch{}, context.Canceled
					}
					return state.ScanBatch{Fault: "generated unavailable listing"}, nil
				}
				return original(ctx, scanner, job, permit)
			}
			_, done := start(t, f.dir, f.config, f.options)
			f.awaitPass(t, 1, false)
			f.advance(t)
			f.awaitPass(t, 2, false)
			// The next fully unchanged pass would otherwise reach two quiet
			// epochs. A returned failure must remain sticky through its retry.
			armed.Store(true)
			f.advance(t)
			if mode == "fault" {
				waitUntil(t, func() bool {
					s := control(t, f.dir, "status")
					return failures.Load() == 1 && s.ActiveJob == 0 && s.AdaptiveRevisits.UnknownEpochs == 1 && s.WaitReason == "job_retry"
				})
				f.advance(t) // Advance to the retained fault retry, not a new pass.
			}
			s := f.awaitPass(t, 3, false)
			if failures.Load() != 1 || s.AdaptiveRevisits.StableRoots != 0 {
				t.Fatal("failed epoch extended its interval", failures.Load(), s)
			}
			f.advance(t)
			f.awaitPass(t, 4, false)
			f.advance(t)
			f.awaitPass(t, 5, true)
			control(t, f.dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerAdaptiveRevisitsHealthyRootLearnsBesideFaultedRoot(t *testing.T) {
	f := newAdaptiveWorkerFixture(t, 301)
	healthy := t.TempDir()
	if err := os.WriteFile(filepath.Join(healthy, "generated-file"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	f.config.Roots = []string{f.root, healthy}
	var healthyListings, faults atomic.Int64
	f.options.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
		if string(job.RootPath) == f.root {
			faults.Add(1)
			return state.ScanBatch{Fault: "generated unavailable root"}, nil
		}
		batch, err := scanner.NextPermitted(ctx, job, permit)
		if err == nil && batch.Complete && batch.Fault == "" && string(job.Path) == "." {
			healthyListings.Add(1)
		}
		return batch, err
	}
	_, done := start(t, f.dir, f.config, f.options)
	for pass := int64(1); pass <= 3; pass++ {
		if pass > 1 {
			f.pause(t)
			f.offset.Store(int64(f.wall().Add(25 * time.Hour).Sub(time.Now())))
			control(t, f.dir, "resume")
		}
		waitUntil(t, func() bool {
			s := control(t, f.dir, "status")
			return healthyListings.Load() >= pass && s.ActiveJob == 0 && s.AdaptiveRevisits.ActiveEpochs == 0 && (s.AdaptiveRevisits.WeeklyRoots == 1) == (pass == 3)
		})
	}
	s := control(t, f.dir, "status")
	if faults.Load() < 1 || s.AdaptiveRevisits.TrackedRoots != 2 || s.AdaptiveRevisits.StableRoots != 1 || s.AdaptiveRevisits.UnknownEpochs != 1 || s.AdaptiveRevisits.DailyRoots != 1 {
		t.Fatal("faulted root blocked healthy learning or earned quiet credit", faults.Load(), s)
	}
	control(t, f.dir, "stop")
	waitExit(t, done)
}
