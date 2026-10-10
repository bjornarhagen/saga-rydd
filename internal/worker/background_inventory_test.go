package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

// This oracle reads only generated bodies and native metadata. It is independent
// of the scanner, compact rows and allocation reduction being checked.
func compactWorkerOracle(t *testing.T, dir string) (int64, int64, int, [32]byte) {
	t.Helper()
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var logical, allocated int64
	identities := map[[2]uint64]bool{}
	digest := sha256.New()
	for _, name := range names {
		path := filepath.Join(dir, name.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
			t.Fatal(path, err)
		}
		logical += int64(len(body))
		id := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
		if !identities[id] {
			allocated += st.Blocks * 512
			identities[id] = true
		}
		digest.Write([]byte(name.Name()))
		digest.Write([]byte{0})
		digest.Write(body)
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	return logical, allocated, len(identities), sum
}

func TestWorkerCompactBackgroundBeyondReportLimit(t *testing.T) {
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, dir := filepath.Join(temp, "project"), filepath.Join(temp, "state")
	modules := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(modules, 0700); err != nil {
		t.Fatal(err)
	}
	const count = state.DirectoryEntryLimit + 1
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(modules, fmt.Sprintf("file-%05d", i)), []byte{byte(i % 251)}, 0600); err != nil {
			t.Fatal(err)
		}
	}
	first := filepath.Join(modules, "file-00000")
	for _, p := range []string{filepath.Join(modules, "copy-one"), filepath.Join(modules, "copy-two"), filepath.Join(root, "ordinary")} {
		if err := os.Link(first, p); err != nil {
			t.Fatal(err)
		}
	}
	logical, allocated, identities, bodyBefore := compactWorkerOracle(t, modules)
	ordinaryBefore, err := os.ReadFile(filepath.Join(root, "ordinary"))
	if err != nil {
		t.Fatal(err)
	}
	if identities <= state.DirectoryEntryLimit {
		t.Fatal("fixture did not exceed bounded identity report", identities)
	}
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.CompactInventory = true
	c.Scan.PauseOnBattery = false
	c.Scan.MetadataPerSecond = 100000
	c.Scan.MaxScanChunksPerDay = 100000
	var admitted atomic.Int64
	// This fixture checks compact inventory and accounting endpoints, not native
	// CPU behavior. Race instrumentation must not add production CPU cooldowns
	// to its finite completion assertion; resource profiles use native SELF CPU.
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuObserve: func() (time.Duration, error) { return 0, nil }}
	options.scannerNew = func(ctx context.Context, roots, excludes, private []string, permit inventory.APIPermit, opts ...inventory.Option) (*inventory.Scanner, error) {
		return inventory.NewPermitted(ctx, roots, excludes, private, func(ctx context.Context, kind inventory.APICallKind) error {
			if err := permit(ctx, kind); err != nil {
				return err
			}
			admitted.Add(1)
			return nil
		}, opts...)
	}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
		return s.NextPermitted(ctx, j, func(ctx context.Context, kind inventory.APICallKind) error {
			if err := permit(ctx, kind); err != nil {
				return err
			}
			admitted.Add(1)
			return nil
		})
	}
	ready, done := startPacingWorker(t, dir, c, options)
	if ready.InventoryMode != nil || ready.AdaptiveRevisits != nil {
		t.Fatal("inventory configured before listener/accounting", ready)
	}
	waitUntil(t, func() bool { mode := control(t, dir, "status").InventoryMode; return mode != nil && mode.Compact })
	reader, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var report state.DirectoryReport
	var lastPollErr error
	drained := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		// Until the worker drains, this report falls back to examining 10,000
		// identities. Repeated fallback queries compete with the reduction being
		// tested, especially under concurrent package race instrumentation.
		live, pollErr := Send(context.Background(), dir, "status")
		if pollErr != nil {
			// A cooperative database turn can delay a read-only status reply.
			// Retry only transport timeouts within the unchanged fixture bound;
			// protocol, ownership and other errors remain immediate failures.
			var timeout net.Error
			if !errors.As(pollErr, &timeout) || !timeout.Timeout() {
				t.Fatal(pollErr)
			}
			lastPollErr = pollErr
		} else if live.WaitReason == "inventory_revisit" {
			drained = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !drained {
		t.Fatal("compact worker did not drain within its fixture bound", lastPollErr)
	}
	report, err = reader.MeasureDirectory(context.Background(), modules)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "recorded_complete" || report.Truncated || report.LogicalBytes == nil || *report.LogicalBytes != logical || report.AllocatedBytes == nil || *report.AllocatedBytes != allocated || report.AllocatedSizeSource != "cached_reduction" || report.InodeEntriesExamined != 0 || report.CompactedFiles != count+2 || report.RepeatedInodes != 2 || report.CurrentStateVerified {
		t.Fatal("complete reduction differs from independent native oracle", report, logical, allocated)
	}
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	live := control(t, dir, "status")
	summary, err := reader.Summary(context.Background())
	if err != nil || summary.Entries != 3 || live.InventoryMetrics == nil || live.Metadata == nil || live.Metadata.TotalCharges == nil || live.Metadata.TotalCharges.Observed != admitted.Load() || admitted.Load() <= count || live.Dispatch == nil || live.Dispatch.Used <= 1 {
		t.Fatal("compact worker did not use metered source and saved maintenance", summary, live, admitted.Load(), err)
	}
	due, err := reader.NextJobDue(context.Background(), []string{state.ScanKind})
	if err != nil || due.IsZero() {
		t.Fatal(due, err)
	}
	if wait := due.Sub(time.Now()); wait < 23*time.Hour || wait > 25*time.Hour {
		t.Fatal("compact mode did not keep fixed daily revisit timing", due, wait)
	}
	_, _, _, bodyAfter := compactWorkerOracle(t, modules)
	ordinaryAfter, err := os.ReadFile(filepath.Join(root, "ordinary"))
	if err != nil || bodyBefore != bodyAfter || !bytes.Equal(ordinaryBefore, ordinaryAfter) {
		t.Fatal("generated source bodies changed", err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCompactModeRefusesStartedDetailedPassAndResumesPreviousMode(t *testing.T) {
	root := fairWorkerFiles(t, 400)
	dir := filepath.Join(t.TempDir(), "state")
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 100000
	c.Scan.PauseOnBattery = false
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(ctx, c.Roots); err != nil {
		t.Fatal(err)
	}
	roots, err := w.ResolveFairInventoryRoots(ctx, c.Roots)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := w.ConfigureBackgroundInventoryMode(ctx, roots, false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.SeedBackgroundInventoryRevisitPage(ctx, scope, 0, time.Now(), state.InventoryRevisitInterval); err != nil {
		t.Fatal(err)
	}
	job, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
	if err != nil || job == nil {
		t.Fatal(job, err)
	}
	s, err := inventory.New(c.Roots, nil, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := s.Next(ctx, *job)
	if err != nil || batch.Complete || len(batch.Cursor) == 0 {
		t.Fatal(batch, err)
	}
	if err = w.CommitScan(ctx, *job, batch); err != nil {
		t.Fatal(err)
	}
	s.Close()
	w.Close()
	r, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := r.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dueBefore, err := r.NextJobDue(ctx, []string{state.ScanKind})
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	c.Scan.CompactInventory = true
	var calls atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, powerCoordinator: fixturePowerCoordinator(nil, nil), cpuObserve: func() (time.Duration, error) { return 0, nil }}
	options.scannerNew = func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		calls.Add(1)
		return nil, errors.New("unexpected source")
	}
	refusalCtx, refusalCancel := context.WithTimeout(ctx, 2*time.Second)
	defer refusalCancel()
	if err = Run(refusalCtx, dir, c, options); !errors.Is(err, state.ErrBackgroundInventoryModePending) || calls.Load() != 0 {
		t.Fatal("unsafe conversion was not refused before source construction", err, calls.Load())
	}
	r, err = state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	after, err := r.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dueAfter, err := r.NextJobDue(ctx, []string{state.ScanKind})
	if err != nil || before.Entries != after.Entries || before.PendingJobs != after.PendingJobs || before.RunningJobs != after.RunningJobs || before.CompleteDirectories != after.CompleteDirectories || !dueBefore.Equal(dueAfter) {
		t.Fatal("refusal changed saved work", before, after, dueBefore, dueAfter, err)
	}
	r.Close()
	c.Scan.CompactInventory = false
	_, done := startPacingWorker(t, dir, c, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second})
	r, err = state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitUntil(t, func() bool {
		sum, e := r.Summary(ctx)
		return e == nil && sum.Entries == 401 && sum.CompleteDirectories == 1 && control(t, dir, "status").WaitReason == "inventory_revisit"
	})
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCompactFixedRevisitReplacesSavedGeneration(t *testing.T) {
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	modules := filepath.Join(temp, "node_modules")
	if err = os.Mkdir(modules, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(modules, "old"), []byte("old generated body"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(temp, "state")
	c := config.Default()
	c.Roots = []string{modules}
	c.Scan.CompactInventory = true
	c.Scan.MetadataPerSecond = 100000
	c.Scan.PauseOnBattery = false
	var passes atomic.Int64
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, revisitInterval: 2 * time.Second}
	options.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		b, e := s.NextPermitted(ctx, j, p)
		if e == nil && b.Complete {
			passes.Add(1)
		}
		return b, e
	}
	_, done := startPacingWorker(t, dir, c, options)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitUntil(t, func() bool { return passes.Load() >= 1 && control(t, dir, "status").WaitReason == "inventory_revisit" })
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	baseline := passes.Load()
	if err = os.Remove(filepath.Join(modules, "old")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(modules, "new"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	// The private short profile preserves actual saved completion timestamps.
	// The large fixture above separately checks the production daily interval.
	due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
	if err != nil || due.IsZero() {
		t.Fatal(due, err)
	}
	time.Sleep(max(time.Until(due), 0) + 20*time.Millisecond)
	if passes.Load() != baseline {
		t.Fatal("paused source revisited")
	}
	control(t, dir, "resume")
	waitUntil(t, func() bool {
		m, e := r.MeasureDirectory(context.Background(), modules)
		return e == nil && passes.Load() > baseline && m.Status == "recorded_complete" && m.LogicalBytes != nil && *m.LogicalBytes == 3 && m.CompactedFiles == 1 && m.AllocatedSizeSource == "cached_reduction" && control(t, dir, "status").WaitReason == "inventory_revisit"
	})
	live := control(t, dir, "status")
	if live.AdaptiveRevisits != nil || live.InventoryMode == nil || !live.InventoryMode.Compact {
		t.Fatal("compact acquired quiet learning", live)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCompactConfigIdleAndMutualExclusion(t *testing.T) {
	dir, c := fixture(t)
	c.Scan.CompactInventory = true
	ready, done := startPacingWorker(t, dir, c, Options{})
	if ready.InventoryMode != nil || ready.InventoryMetrics != nil || ready.AdaptiveRevisits != nil {
		t.Fatal("idle worker applied source policy", ready)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	c.Scan.AdaptiveRevisits = true
	missing := filepath.Join(t.TempDir(), "uninitialized")
	if err := Run(context.Background(), missing, c, Options{ExperimentalScan: true}); !errors.Is(err, state.ErrBackgroundInventoryInput) {
		t.Fatal(err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid paired mode initialized state", err)
	}
	p := InventoryModeSnapshot{Compact: true}
	b, err := json.Marshal(map[string]any{"inventory_mode": p})
	if err != nil || len(b) > 40 {
		t.Fatal(len(b), err)
	}
}

func TestWorkerCompactQuotaKeepsMaintenanceAndFutureGeneric(t *testing.T) {
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	modules := filepath.Join(temp, "node_modules")
	if err = os.Mkdir(modules, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if err = os.WriteFile(filepath.Join(modules, fmt.Sprintf("file-%03d", i)), []byte("generated"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Join(temp, "state")
	ctx := context.Background()
	c := config.Default()
	c.Roots = []string{modules}
	c.Scan.CompactInventory = true
	c.Scan.MetadataPerSecond = 100000
	c.Scan.PauseOnBattery = false
	c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(ctx, c.Roots); err != nil {
		t.Fatal(err)
	}
	compact := true
	if _, err = w.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := inventory.New(c.Roots, nil, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	for {
		j, e := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if j == nil {
			break
		}
		b, e := scanner.Next(ctx, *j)
		if e != nil || b.Fault != "" {
			t.Fatal(b, e)
		}
		if e = w.CommitScan(ctx, *j, b); e != nil {
			t.Fatal(e)
		}
	}
	scanner.Close()
	source := filepath.Join(temp, "source")
	if err = os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	c.Roots = append(c.Roots, source)
	if err = w.SyncRoots(ctx, c.Roots); err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(ctx, 2, state.ScanKind, []byte("."), time.Now()); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(500 * time.Millisecond)
	if err = w.EnqueueJob(ctx, 1, "fixture", nil, future); err != nil {
		t.Fatal(err)
	}
	res, err := w.ReserveMetadata(ctx, time.Now(), state.MetadataStartup, nil, c.Scan.MetadataAttemptsPerDay, c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SettleMetadata(ctx, res, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var sourceCalls, observations atomic.Int64
	generic := make(chan struct{}, 1)
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
		generic <- struct{}{}
		return Result{Done: true}, nil
	}}}
	options.inventoryStateObserve = func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
		observations.Add(1)
		return state.InventoryStateBudget{}, errors.New("unexpected source observation")
	}
	options.scannerNew = func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		sourceCalls.Add(1)
		return nil, errors.New("unexpected source")
	}
	_, done := startPacingWorker(t, dir, c, options)
	// No control wake is sent before this future job fires.
	select {
	case <-generic:
	case <-time.After(5 * time.Second):
		t.Fatal("future generic timer was lost")
	}
	reader, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	waitUntil(t, func() bool {
		m, e := reader.MeasureDirectory(ctx, modules)
		return e == nil && m.AllocatedSizeSource == "cached_reduction" && m.Status == "recorded_complete" && control(t, dir, "status").WaitReason == "daily_metadata_limit"
	})
	before := control(t, dir, "status")
	if sourceCalls.Load() != 0 || observations.Load() != 0 || before.InventoryMetrics != nil || before.Dispatch == nil || before.Dispatch.Used == 0 || before.InventoryMode == nil || !before.InventoryMode.Compact {
		t.Fatal("compact quota bypassed resource gate or blocked saved maintenance", before, sourceCalls.Load(), observations.Load())
	}
	time.Sleep(60 * time.Millisecond)
	after := control(t, dir, "status")
	if after.Dispatch.Used != before.Dispatch.Used || sourceCalls.Load() != 0 || observations.Load() != 0 {
		t.Fatal("quota wait polled or charged", before, after)
	}
	control(t, dir, "pause")
	control(t, dir, "resume")
	control(t, dir, "stop")
	waitExit(t, done)
}
