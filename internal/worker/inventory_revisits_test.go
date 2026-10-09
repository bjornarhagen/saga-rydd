package worker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestWorkerPeriodicRevisitFindsNewEntryWithoutRestart(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	if err := os.WriteFile(filepath.Join(root, "first"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	var passes atomic.Int64
	var latestGeneration atomic.Int64
	options := Options{ExperimentalScan: true, Interval: 5 * time.Millisecond, WorkDuration: time.Second, revisitInterval: 350 * time.Millisecond}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		b, err := s.NextPermitted(ctx, j, p)
		if err == nil && b.Complete {
			latestGeneration.Store(b.Generation)
			passes.Add(1)
		}
		return b, err
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return passes.Load() >= 1 && control(t, dir, "status").WaitReason == "inventory_revisit" })
	// Freeze dispatch before inspecting saved due evidence. A busy race runner
	// can pass the short revisit deadline after the live status observation.
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	pausedPasses := passes.Load()
	pausedGeneration := latestGeneration.Load()
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
	if err != nil || due.IsZero() {
		t.Fatal(due, err)
	}
	if err := os.WriteFile(filepath.Join(root, "new"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(max(time.Until(due), 0) + 20*time.Millisecond)
	if passes.Load() != pausedPasses || control(t, dir, "status").ActiveJob != 0 {
		t.Fatal("paused revisit dispatched", passes.Load())
	}
	control(t, dir, "resume")
	waitUntil(t, func() bool {
		summary, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return passes.Load() > pausedPasses && summary.Entries == 3 && summary.CompleteDirectories == 1 && summary.PendingJobs == 1 && summary.RunningJobs == 0 && control(t, dir, "status").WaitReason == "inventory_revisit"
	})
	if pausedGeneration == 0 || pausedGeneration == latestGeneration.Load() {
		t.Fatal("revisit reused listing generation")
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPeriodicRevisitKeepsOfflineEvidenceAndHealthyRootActive(t *testing.T) {
	dir, c := fixture(t)
	offline, healthy := t.TempDir(), t.TempDir()
	c.Roots = []string{offline, healthy}
	c.Scan.MetadataPerSecond = 100000
	if err := os.WriteFile(filepath.Join(offline, "saved"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	// Establish genuine saved metadata before the generated root goes offline.
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err := inventory.New(c.Roots, nil, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	for {
		j, e := w.ClaimJob(context.Background(), []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if j == nil {
			break
		}
		b, e := s.Next(context.Background(), *j)
		if e != nil || b.Fault != "" {
			t.Fatal(b, e)
		}
		if e = w.CommitScan(context.Background(), *j, b); e != nil {
			t.Fatal(e)
		}
	}
	s.Close()
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	parked := offline + "-parked"
	if err = os.Rename(offline, parked); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Rename(parked, offline); err != nil {
			t.Error(err)
		}
	})
	var failed, healthyPasses atomic.Int64
	options := Options{ExperimentalScan: true, Interval: 5 * time.Millisecond, WorkDuration: time.Second, revisitInterval: 150 * time.Millisecond}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		b, e := s.NextPermitted(ctx, j, p)
		if string(j.RootPath) == offline && b.Fault != "" {
			failed.Add(1)
		}
		if string(j.RootPath) == healthy && b.Complete {
			healthyPasses.Add(1)
		}
		return b, e
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return failed.Load() == 1 && healthyPasses.Load() >= 2 })
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.Entries != 3 || summary.DirectoryErrors != 1 || summary.PendingJobs < 1 || summary.PendingJobs > 2 || summary.RunningJobs != 0 || failed.Load() != 1 {
		t.Fatal("offline retry erased evidence/blocked healthy revisit", summary, failed.Load(), err)
	}
	// Pause can land after the healthy listing committed but before its bounded
	// reconciliation step has published the future revisit. The offline retry
	// remains queued, and the healthy root retains one of those exact states.
	if summary.PendingJobs == 1 {
		report, err := r.LargestFiles(context.Background(), 1, "")
		if err != nil {
			t.Fatal(err)
		}
		var healthyID int64
		for _, root := range report.Roots {
			if string(root.PathBytes) == healthy {
				healthyID = root.ID
			}
		}
		ready, err := r.InventoryRetirementRootPending(context.Background(), healthyID)
		if err != nil || !ready {
			t.Fatal("healthy root lost maintenance/revisit", ready, err)
		}
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPeriodicRevisitRetainsWideDirectoryContinuation(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	for i := 0; i < 301; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	type observation struct {
		generation int64
		names      []string
		complete   bool
	}
	batches := make(chan observation, 32)
	options := Options{ExperimentalScan: true, Interval: 5 * time.Millisecond, WorkDuration: time.Second, revisitInterval: time.Millisecond}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		b, e := s.NextPermitted(ctx, j, p)
		if e == nil && b.Fault == "" {
			o := observation{generation: b.Generation, complete: b.Complete}
			for _, entry := range b.Entries {
				o.names = append(o.names, string(entry.Path))
			}
			batches <- o
		}
		return b, e
	}
	_, done := start(t, dir, c, options)
	seen := map[string]bool{}
	var generation int64
	for completed := false; !completed; {
		select {
		case b := <-batches:
			if generation == 0 {
				generation = b.generation
			}
			if generation != b.generation {
				t.Fatal("periodic seed reset an unfinished stream")
			}
			for _, name := range b.names {
				if seen[name] {
					t.Fatal("replayed prefix", name)
				}
				seen[name] = true
			}
			completed = b.complete
		case <-time.After(5 * time.Second):
			t.Fatal("wide directory did not finish", len(seen))
		}
	}
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	if len(seen) != 301 {
		t.Fatal("missing first-pass members", len(seen))
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPeriodicRevisitCannotBypassDailyQuotas(t *testing.T) {
	for _, quota := range []string{"dispatch", "metadata"} {
		t.Run(quota, func(t *testing.T) {
			dir, c := fixture(t)
			c.Roots = []string{t.TempDir()}
			want := "daily_chunk_limit"
			if quota == "dispatch" {
				c.Scan.MaxScanChunksPerDay = 1
			} else {
				c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance + 30
				want = "daily_metadata_limit"
			}
			var calls atomic.Int64
			options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, revisitInterval: time.Millisecond}
			options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
				calls.Add(1)
				return s.NextPermitted(ctx, j, p)
			}
			_, done := start(t, dir, c, options)
			waitUntil(t, func() bool { return calls.Load() == 1 && control(t, dir, "status").WaitReason == want })
			used := control(t, dir, "status").Dispatch.Used
			if used != 1 && quota == "dispatch" {
				t.Fatal("source dispatch exceeded cap", used)
			}
			// Metadata denial still permits saved-only maintenance. Such work
			// shares dispatch charges, without another scanner reservation/call.
			if quota == "metadata" && used <= 1 {
				t.Fatal("saved maintenance was not dispatched", used)
			}
			control(t, dir, "pause")
			control(t, dir, "resume")
			snapshot := control(t, dir, "status")
			if calls.Load() != 1 || snapshot.Dispatch.Used != used || snapshot.ActiveJob != 0 {
				t.Fatal("revisit bypassed durable quota", snapshot, calls.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

func TestWorkerPeriodicStartupPagesReachRootAfterDisabledHistory(t *testing.T) {
	dir, c := fixture(t)
	c.Roots = []string{t.TempDir()}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	history := make([]string, 300)
	for i := range history {
		history[i] = fmt.Sprintf("/generated/disabled-root-%03d", i)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	// Model a generated legacy inventory without bypassing the new writer's
	// admission API. The selected known root remains after 300 disabled rows,
	// so startup must still advance its raw pagination cursor to reach it.
	db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	defer tx.Rollback()
	defer db.Close()
	for _, root := range append(history, c.Roots...) {
		if _, err = tx.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte(root)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
		w.Close()
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		if string(j.RootPath) != c.Roots[0] {
			return state.ScanBatch{}, fmt.Errorf("disabled root dispatched")
		}
		calls.Add(1)
		return s.NextPermitted(ctx, j, p)
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return calls.Load() == 1 && control(t, dir, "status").WaitReason == "inventory_revisit" })
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerPeriodicProductionIntervalAndOrdinaryIdle(t *testing.T) {
	for _, experimental := range []bool{false, true} {
		t.Run(fmt.Sprint(experimental), func(t *testing.T) {
			dir, c := fixture(t)
			c.Roots = []string{t.TempDir()}
			var calls, samples atomic.Int64
			options := Options{ExperimentalScan: experimental, Interval: time.Millisecond, WorkDuration: time.Second, cpuObserve: func() (time.Duration, error) { samples.Add(1); return 0, nil }}
			options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
				calls.Add(1)
				return s.NextPermitted(ctx, j, p)
			}
			_, done := start(t, dir, c, options)
			want := "idle"
			if experimental {
				want = "inventory_revisit"
			}
			waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == want })
			before := samples.Load()
			time.Sleep(50 * time.Millisecond)
			if samples.Load() != before {
				t.Fatal("future/idle wait polled dispatch", before, samples.Load())
			}
			r, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
			if err != nil {
				t.Fatal(err)
			}
			if experimental {
				if calls.Load() != 1 || !due.After(time.Now().Add(23*time.Hour)) {
					t.Fatal("production interval not 24h", due, calls.Load())
				}
			} else if calls.Load() != 0 || !due.IsZero() {
				t.Fatal("idle daemon scheduled scan", due, calls.Load())
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}
