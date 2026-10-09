package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func fairWorkerFiles(t *testing.T, count int) string {
	t.Helper()
	root := t.TempDir()
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%05d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestWorkerFairRootsInterleaveSourceMaintenanceAndRevisit(t *testing.T) {
	wide, small := fairWorkerFiles(t, 5001), fairWorkerFiles(t, 3)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{wide, small}
	c.Scan.MetadataPerSecond = 100000
	c.Scan.MaxScanChunksPerDay = 10000
	var mu sync.Mutex
	var generation int64
	seen := map[string]bool{}
	wideDone, smallDone := false, 0
	var failure error
	options := Options{ExperimentalScan: true, Interval: 20 * time.Millisecond, WorkDuration: time.Second, revisitInterval: 60 * time.Millisecond}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		b, err := s.NextPermitted(ctx, j, p)
		mu.Lock()
		defer mu.Unlock()
		if err == nil && b.Fault == "" {
			if string(j.RootPath) == wide {
				if generation == 0 {
					generation = b.Generation
				}
				if b.Generation != generation {
					failure = errors.New("rotation restarted the wide directory generation")
				}
				for _, entry := range b.Entries {
					path := string(entry.Path)
					if seen[path] {
						failure = errors.New("rotation replayed a wide-directory entry")
					}
					seen[path] = true
				}
				wideDone = wideDone || b.Complete
			} else if string(j.RootPath) == small && b.Complete {
				smallDone++
			}
		}
		return b, err
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return failure != nil || smallDone >= 2 || wideDone
	})
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	mu.Lock()
	if failure != nil || smallDone < 2 || wideDone || generation == 0 || len(seen) < state.MaxBatchEntries {
		t.Error("small root did not finish and revisit while wide root retained progress", failure, smallDone, wideDone, generation, len(seen))
	}
	mu.Unlock()
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerFairSourceQuotaAllowsOtherRootMaintenanceWithoutSpin(t *testing.T) {
	dir, historical, _ := workerRetirementFixture(t, 300)
	source := fairWorkerFiles(t, 1)
	c := config.Default()
	c.Roots = []string{historical, source}
	c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := w.ReserveMetadata(context.Background(), time.Now(), state.MetadataStartup, nil, c.Scan.MetadataAttemptsPerDay, c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SettleMetadata(context.Background(), reservation, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var calls, cpuSamples atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	noRetirementSource(t, &options, &calls)
	options.cpuObserve = func() (time.Duration, error) {
		cpuSamples.Add(1)
		return 0, nil
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_metadata_limit" })
	before := control(t, dir, "status")
	samples := cpuSamples.Load()
	time.Sleep(100 * time.Millisecond)
	after := control(t, dir, "status")
	if calls.Load() != 0 || before.Dispatch == nil || before.Dispatch.Used < 1 || after.Dispatch.Used != before.Dispatch.Used || cpuSamples.Load() != samples || after.InventoryMetrics != nil {
		t.Fatal("source quota caused reads or idle polling", before, after, calls.Load(), samples, cpuSamples.Load())
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.Entries != 4 {
		t.Fatal("other-root historical maintenance did not drain", summary, err)
	}
	if due, err := r.NextJobDue(context.Background(), []string{state.ScanKind}); err != nil || due.IsZero() {
		t.Fatal("quota-blocked source was lost", due, err)
	}
	if !control(t, dir, "pause").Paused || control(t, dir, "resume").Paused {
		t.Fatal("quota wait blocked controls")
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerFairRechecksRootEligibilityAfterCPUWait(t *testing.T) {
	wide, healthy := fairWorkerFiles(t, 301), fairWorkerFiles(t, 3)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{wide, healthy}
	c.Scan.MetadataPerSecond = 100000
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 2, state.ScanKind, []byte("."), time.Now().Add(300*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var samples int
	starts := make(chan string, 8)
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	options.cpuObserve = func() (time.Duration, error) {
		samples++
		if samples < 2 {
			return 0, nil
		}
		return 10 * time.Millisecond, nil
	}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		starts <- string(j.RootPath)
		return s.NextPermitted(ctx, j, p)
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "cpu_backoff" })
	before := control(t, dir, "status")
	if before.Dispatch.Used != 1 || <-starts != wide {
		t.Fatal("first source turn differs", before)
	}
	if !control(t, dir, "pause").Paused || control(t, dir, "resume").Paused {
		t.Fatal("CPU wait blocked controls")
	}
	select {
	case root := <-starts:
		if root != healthy {
			t.Fatal("CPU wait used stale root selection", root)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CPU wait did not dispatch the newly due root")
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerFairAdmissionBeforeSavedRootsOrSource(t *testing.T) {
	dir, c := fixture(t)
	c.Roots = make([]string, state.MaxFairInventoryRoots+1)
	for i := range c.Roots {
		c.Roots[i] = fmt.Sprintf("/bounded-fixture-%d", i)
	}
	var calls atomic.Int64
	options := Options{ExperimentalScan: true}
	noRetirementSource(t, &options, &calls)
	options.StartupCheck = func(context.Context) error { calls.Add(1); return nil }
	options.Ready = func(Snapshot) { calls.Add(1) }
	if err := Run(context.Background(), dir, c, options); !errors.Is(err, state.ErrFairInventoryInput) || calls.Load() != 0 {
		t.Fatal("admission crossed the startup/source boundary", err, calls.Load())
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := r.Summary(context.Background())
	r.Close()
	if err != nil || summary.EnabledRoots != 1 {
		t.Fatal("refused roots changed saved state", summary, err)
	}
	// The experimental admission bound must not change idle worker contracts.
	_, done := start(t, dir, c, Options{})
	control(t, dir, "stop")
	waitExit(t, done)
}
