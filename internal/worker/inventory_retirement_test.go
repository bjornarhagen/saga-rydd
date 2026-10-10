package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func savedRetirementPass(t *testing.T, w *state.Store, root string) {
	t.Helper()
	ctx := context.Background()
	if err := w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	s, err := inventory.New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 100; i++ {
		j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			return
		}
		b, err := s.Next(ctx, *j)
		if err != nil || b.Fault != "" {
			t.Fatal(b, err)
		}
		if err = w.CommitScan(ctx, *j, b); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("generated source pass did not drain")
}

func workerRetirementFixture(t *testing.T, count int) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(t.TempDir(), "state")
	for _, name := range []string{"gone", "gone2"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(root, "gone", fmt.Sprintf("file-%03d", i)), []byte("unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "gone2", "keep"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "outside"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), []string{root}); err != nil {
		t.Fatal(err)
	}
	savedRetirementPass(t, w, root)
	for i := 0; i < 100; i++ {
		step, err := w.RetireInventoryForRoot(context.Background(), 1)
		if err != nil {
			t.Fatal(step, err)
		}
		if !step.Remaining {
			break
		}
		if i == 99 {
			t.Fatal("first-pass maintenance did not drain")
		}
	}
	parked := filepath.Join(t.TempDir(), "parked")
	if err = os.Rename(filepath.Join(root, "gone"), parked); err != nil {
		t.Fatal(err)
	}
	savedRetirementPass(t, w, root)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, root, parked
}

func noRetirementSource(t *testing.T, options *Options, calls *atomic.Int64) {
	t.Helper()
	options.scannerNew = func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		calls.Add(1)
		return nil, errors.New("unexpected fixture scanner construction")
	}
	options.scannerNext = func(context.Context, *inventory.Scanner, state.Job, inventory.APIPermit) (state.ScanBatch, error) {
		calls.Add(1)
		return state.ScanBatch{}, errors.New("unexpected fixture scanner step")
	}
}

func TestWorkerSavedRetirementWithoutSourceQuotaAndWithPauseRestart(t *testing.T) {
	dir, root, parked := workerRetirementFixture(t, 1001)
	_, c := fixture(t)
	c.Roots = []string{root}
	c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := w.ReserveMetadata(context.Background(), time.Now(), state.MetadataStartup, nil, inventory.MaxAPIAttemptAllowance, c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SettleMetadata(context.Background(), reservation, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: 20 * time.Millisecond, WorkDuration: time.Second}
	noRetirementSource(t, &options, &calls)
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool {
		s, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return s.Entries > 4 && s.Entries < 1006
	})
	paused := control(t, dir, "pause")
	before, err := r.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	after, err := r.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !paused.Paused || before.Entries != after.Entries || calls.Load() != 0 {
		t.Fatal("pause/source boundary lost", before, after, calls.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
	// Restart must discover the proof left without any source job. It cannot
	// initialize a scanner or ask for another exhausted metadata allowance.
	ready, restarted := start(t, dir, c, options)
	done = restarted
	if !ready.Paused || ready.InventoryMetrics != nil || calls.Load() != 0 {
		t.Fatal("restart lost pause/source boundary", ready, calls.Load())
	}
	control(t, dir, "resume")
	finished := false
	var lastSummary state.Summary
	var lastSnapshot Snapshot
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		s, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		lastSummary, lastSnapshot = s, control(t, dir, "status")
		if s.Entries == 4 && s.PendingJobs == 1 && lastSnapshot.WaitReason == "inventory_revisit" {
			finished = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !finished {
		due, _ := r.NextJobDue(context.Background(), []string{state.ScanKind})
		pending, _ := r.HasSubtreeRetirement(context.Background())
		used := -1
		if lastSnapshot.Dispatch != nil {
			used = lastSnapshot.Dispatch.Used
		}
		t.Fatal("retirement/revisit not finished", lastSummary, lastSnapshot.WaitReason, used, due, pending)
	}
	snapshot := control(t, dir, "status")
	if calls.Load() != 0 || snapshot.InventoryMetrics != nil || snapshot.Priority != nil || snapshot.Metadata.TotalCharges == nil || snapshot.Metadata.TotalCharges.Reserved != inventory.MaxAPIAttemptAllowance || snapshot.Metadata.TotalCharges.Observed != 0 || snapshot.Metadata.TotalCharges.OutstandingReserved != 0 || snapshot.Metadata.TotalCharges.UnknownReserved != 0 || snapshot.Metadata.TotalCharges.KnownUnusedReserved != inventory.MaxAPIAttemptAllowance {
		t.Fatal("maintenance touched source allowance", snapshot, calls.Load())
	}
	if snapshot.Dispatch == nil || snapshot.Dispatch.Used < 8 {
		t.Fatal("maintenance was not paced/charged", snapshot.Dispatch)
	}
	content, err := os.ReadFile(filepath.Join(parked, "file-000"))
	if err != nil || string(content) != "unchanged" {
		t.Fatal("generated source body changed", content, err)
	}
	content, err = os.ReadFile(filepath.Join(root, "gone2", "keep"))
	if err != nil || string(content) != "keep" {
		t.Fatal("prefix neighbor source changed", content, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerSavedRetirementSharesDurableDailyCapAcrossRestart(t *testing.T) {
	dir, root, _ := workerRetirementFixture(t, 300)
	_, c := fixture(t)
	c.Roots = []string{root}
	c.Scan.MaxScanChunksPerDay = 1
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	noRetirementSource(t, &options, &calls)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 0; i < 2; i++ {
		_, done := start(t, dir, c, options)
		waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_chunk_limit" })
		snapshot := control(t, dir, "status")
		if calls.Load() != 0 || snapshot.Dispatch.Used != 1 || snapshot.InventoryMetrics != nil || snapshot.Metadata.Status != "untracked" {
			t.Fatal(snapshot, calls.Load())
		}
		if due, err := r.NextJobDue(context.Background(), []string{state.ScanKind}); err != nil || !due.IsZero() {
			t.Fatal("future revisit bypassed unfinished retirement", due, err)
		}
		if pending, err := r.HasSubtreeRetirement(context.Background()); err != nil || !pending {
			t.Fatal(pending, err)
		}
		control(t, dir, "pause")
		control(t, dir, "resume")
		if snapshot = control(t, dir, "status"); snapshot.Dispatch.Used != 1 {
			t.Fatal(snapshot)
		}
		control(t, dir, "stop")
		waitExit(t, done)
	}
}

func TestWorkerFairMaintenancePrecedesFutureSourceSharesCap(t *testing.T) {
	dir, root, _ := workerRetirementFixture(t, 300)
	healthy := t.TempDir()
	_, c := fixture(t)
	c.Roots = []string{root, healthy}
	c.Scan.MaxScanChunksPerDay = 1
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	// Reconciliation and the earlier fixed-table purge phases can commit
	// without changing the entry count. Prepare the first entry purge so the
	// single worker dispatch below must perform the next bounded entry purge.
	prepareCtx, prepareCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer prepareCancel()
	initial, err := w.Summary(prepareCtx)
	if err != nil || initial.Entries <= 2*state.MaxBatchEntries+4 {
		t.Fatal("missing generated stale maintenance evidence", initial, err)
	}
	var prepared state.Summary
	entryPurgeCommitted := false
	for i := 0; i < 32; i++ {
		step, stepErr := w.RetireInventoryForRoot(prepareCtx, 1)
		if stepErr != nil || !step.Eligible || !step.Worked || !step.Remaining {
			t.Fatal("entry purge preparation lost partial maintenance", i, step, stepErr)
		}
		prepared, err = w.Summary(prepareCtx)
		if err != nil {
			t.Fatal(err)
		}
		if prepared.Entries == initial.Entries {
			continue
		}
		if initial.Entries-prepared.Entries != state.MaxBatchEntries || prepared.Entries <= state.MaxBatchEntries+4 {
			t.Fatal("first prepared entry purge was not bounded and partial", initial, prepared)
		}
		entryPurgeCommitted = true
		break
	}
	if !entryPurgeCommitted {
		t.Fatal("entry purge preparation exceeded 32 saved maintenance turns")
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := state.OpenReader(prepareCtx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	before, err := r.Summary(prepareCtx)
	if err != nil || before.Entries != prepared.Entries {
		t.Fatal("prepared saved entry baseline changed", prepared, before, err)
	}
	prepareCancel()
	w, err = state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the future-source interval anchored after finite saved preparation.
	futureDue := time.Now().Add(400 * time.Millisecond)
	if err = w.EnqueueJob(context.Background(), 2, state.ScanKind, []byte("."), futureDue); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var committedObserved atomic.Bool
	committed := make(chan state.Summary, 1)
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second}
	options.cpuObserve = func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		summary, err := r.Summary(ctx)
		if err != nil {
			return 0, err
		}
		// Charge the committed maintenance window, not an earlier setup
		// observation. The first bounded retirement must reduce saved rows.
		if summary.Entries >= before.Entries {
			return 0, nil
		}
		if committedObserved.CompareAndSwap(false, true) {
			committed <- summary
		}
		return 10 * time.Millisecond, nil
	}
	noRetirementSource(t, &options, &calls)
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool {
		snapshot := control(t, dir, "status")
		return snapshot.WaitReason == "daily_chunk_limit"
	})
	select {
	case saved := <-committed:
		if saved.Entries <= 4 || before.Entries-saved.Entries != state.MaxBatchEntries {
			t.Fatal("CPU witness did not follow bounded partial maintenance", before, saved)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed maintenance CPU witness missing")
	}
	summary, err := r.Summary(context.Background())
	status := control(t, dir, "status")
	if err != nil || summary.Entries <= 4 || before.Entries-summary.Entries != state.MaxBatchEntries || calls.Load() != 0 || status.Dispatch == nil || status.Dispatch.Used != 1 || status.CPUFeedback == nil || status.CPUFeedback.Status != "observed" || status.CPUFeedback.Window == nil || status.CPUFeedback.Window.Kind != state.FairInventoryMaintenance || status.CPUFeedback.Window.CPUTimeNS == nil || *status.CPUFeedback.Window.CPUTimeNS != int64(10*time.Millisecond) || !status.CPUFeedback.Window.DispatchReservedAt.Before(futureDue) {
		t.Fatal("eligible maintenance did not use its fair turn before future source", summary, err, calls.Load(), status)
	}
	if due, err := r.NextJobDue(context.Background(), []string{state.ScanKind}); err != nil || !due.Equal(futureDue) {
		t.Fatal("future source job was lost", due, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerSavedRetirementKeepsDelayedRootAndRevisitsHealthyRoot(t *testing.T) {
	dir, root, _ := workerRetirementFixture(t, 300)
	healthy := t.TempDir()
	_, c := fixture(t)
	c.Roots = []string{root, healthy}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, state.ScanKind, []byte("interrupted"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var healthyCalls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, revisitInterval: 150 * time.Millisecond}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		if string(j.RootPath) != healthy {
			return state.ScanBatch{}, errors.New("delayed root was reset")
		}
		healthyCalls.Add(1)
		return s.NextPermitted(ctx, j, p)
	}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return healthyCalls.Load() >= 2 })
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.Entries != 306 {
		t.Fatal("blocked historical payload changed", summary, err)
	}
	if ready, err := r.InventoryRetirementRootPending(context.Background(), 1); err != nil || ready {
		t.Fatal(ready, err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}
