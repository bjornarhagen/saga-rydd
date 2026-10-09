package state

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

var cpuCancelRegistration sync.Once
var cpuCancelRegistrationErr error
var cpuCancelCallbacksMu sync.Mutex
var cpuCancelCallbacks = map[int64]context.CancelFunc{}

// This exclusive-fixture context cancels after Commit releases the transaction's
// sole connection. The required pre-commit Err check sees that connection held.
// It exercises a successful commit with a canceled reply, without a product seam.
type cpuCommitCancelContext struct {
	context.Context
	cancel         context.CancelFunc
	store          *Store
	mu             sync.Mutex
	sawTransaction bool
}

func (ctx *cpuCommitCancelContext) Err() error {
	ctx.mu.Lock()
	if ctx.store.db.Stats().InUse > 0 {
		ctx.sawTransaction = true
	} else if ctx.sawTransaction {
		ctx.cancel()
	}
	ctx.mu.Unlock()
	return ctx.Context.Err()
}

func TestCPUFeedbackCommittedButCanceledReplyPreservesExactOutcome(t *testing.T) {
	for _, operation := range []string{"begin", "settle", "recover"} {
		t.Run(operation, func(t *testing.T) {
			now := cpuFeedbackNow()
			s, _, _, turn := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
			var marker CPUWindowMarker
			if operation != "begin" {
				marker = beginCPUFeedback(t, s, turn, now)
			}
			before := cpuFeedbackBytes(t, s)
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cpuCommitCancelContext{Context: base, cancel: cancel, store: s}
			measurement := CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(int64(100 * time.Millisecond)), ElapsedNS: cpuFeedbackInt(int64(time.Second))}
			var returned CPUFeedbackState
			var err error
			switch operation {
			case "begin":
				marker, err = s.BeginCPUWindow(ctx, turn, now, CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: now})
			case "settle":
				returned, err = s.SettleCPUWindow(ctx, marker, now.Add(time.Second), measurement)
			case "recover":
				returned, err = s.RecoverCPUWindow(ctx, now.Add(time.Second))
			}
			if !errors.Is(err, ErrCPUFeedbackPublication) || !errors.Is(err, context.Canceled) {
				t.Fatal("committed cancellation was represented as definite denial", operation, err)
			}
			if operation == "begin" {
				if marker.Token() == "" {
					t.Fatal("uncertain begin lost its provenance")
				}
			} else {
				raw, _ := json.Marshal(returned)
				if !bytes.Equal(raw, before) {
					t.Fatal("uncertain reply claimed a saved candidate", returned)
				}
			}
			saved, err := s.CPUFeedback(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "begin":
				if saved.Status != "pending" || saved.Window.Token != marker.Token() {
					t.Fatal(saved)
				}
			case "settle":
				if saved.Status != "observed" {
					t.Fatal(saved)
				}
				original := cpuFeedbackBytes(t, s)
				if _, err = s.SettleCPUWindow(context.Background(), marker, now.Add(time.Second), measurement); err != nil || !bytes.Equal(original, cpuFeedbackBytes(t, s)) {
					t.Fatal("retry changed committed settlement", err)
				}
			case "recover":
				if saved.Status != "recovered_unknown" || saved.RecoveredUnknownWindows != 1 {
					t.Fatal(saved)
				}
				original := cpuFeedbackBytes(t, s)
				if _, err = s.RecoverCPUWindow(context.Background(), now.Add(time.Minute)); err != nil || !bytes.Equal(original, cpuFeedbackBytes(t, s)) {
					t.Fatal("retry renewed committed recovery", err)
				}
			}
		})
	}
}

func TestCPUFeedbackCancellationDuringPublicationReturnsSavedPending(t *testing.T) {
	cpuCancelRegistration.Do(func() {
		cpuCancelRegistrationErr = sqlite.RegisterScalarFunction("rydd_test_cpu_cancel", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			id, _ := args[0].(int64)
			cpuCancelCallbacksMu.Lock()
			cancel := cpuCancelCallbacks[id]
			cpuCancelCallbacksMu.Unlock()
			if cancel != nil {
				cancel()
			}
			return int64(0), nil
		})
	})
	if cpuCancelRegistrationErr != nil {
		t.Fatal(cpuCancelRegistrationErr)
	}
	for i, operation := range []string{"settle", "recover"} {
		t.Run(operation, func(t *testing.T) {
			now := cpuFeedbackNow()
			s, _, _, turn := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
			marker := beginCPUFeedback(t, s, turn, now)
			before := cpuFeedbackBytes(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			id := int64(i + 1)
			cpuCancelCallbacksMu.Lock()
			cpuCancelCallbacks[id] = cancel
			cpuCancelCallbacksMu.Unlock()
			defer func() { cpuCancelCallbacksMu.Lock(); delete(cpuCancelCallbacks, id); cpuCancelCallbacksMu.Unlock() }()
			if _, err := s.db.Exec(fmt.Sprintf("CREATE TRIGGER cpu_cancel BEFORE INSERT ON worker_cpu_feedback BEGIN SELECT rydd_test_cpu_cancel(%d); END", id)); err != nil {
				t.Fatal(err)
			}
			var returned CPUFeedbackState
			var err error
			if operation == "settle" {
				returned, err = s.SettleCPUWindow(ctx, marker, now.Add(time.Second), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)})
			} else {
				returned, err = s.RecoverCPUWindow(ctx, now.Add(time.Second))
			}
			if !errors.Is(err, context.Canceled) || returned.Status != "pending" || returned.NextAllowedAt != nil || returned.CompletedUnknownWindows != 0 || returned.RecoveredUnknownWindows != 0 || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
				t.Fatal("canceled publication fabricated a saved terminal state", returned, err)
			}
		})
	}
}

func cpuFeedbackNow() time.Time     { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) }
func cpuFeedbackInt(n int64) *int64 { return &n }

func cpuFeedbackFixture(t *testing.T, at time.Time, kind string) (*Store, string, FairInventoryRoots, *FairInventoryTurn) {
	t.Helper()
	s, dir, scope := fairInventoryFixture(t)
	if kind == FairInventorySource {
		if err := s.EnqueueJob(context.Background(), 1, ScanKind, []byte("."), time.Unix(0, 1)); err != nil {
			t.Fatal(err)
		}
	} else {
		fairInventoryMaintenanceFixture(t, s, 1)
	}
	return s, dir, scope, fairInventoryClaim(t, s, scope, at, true)
}

func beginCPUFeedback(t *testing.T, s *Store, turn *FairInventoryTurn, at time.Time) CPUWindowMarker {
	t.Helper()
	marker, err := s.BeginCPUWindow(context.Background(), turn, at, CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: at.Add(-time.Millisecond)})
	if err != nil || !metadataDigest(marker.Token(), 64) {
		t.Fatal(marker.Token(), err)
	}
	return marker
}

func cpuFeedbackBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	state, err := s.CPUFeedback(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCPUFeedbackKnownSettlementAfterSourceProgressAndExactRetry(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, _, _, turn := cpuFeedbackFixture(t, now, FairInventorySource)
	marker := beginCPUFeedback(t, s, turn, now)
	if err := s.FinishJob(ctx, *turn.Job, true, nil, now, ""); err != nil {
		t.Fatal(err)
	}
	measurement := CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(int64(100 * time.Millisecond)), ElapsedNS: cpuFeedbackInt(int64(time.Second))}
	completed := now.Add(time.Second)
	state, err := s.SettleCPUWindow(ctx, marker, completed, measurement)
	if err != nil || state.Status != "observed" || state.Window.JobToken != turn.Job.Token || state.Window.BackoffNS != int64(9*time.Second) || state.NextAllowedAt == nil || !state.NextAllowedAt.Equal(now.Add(10*time.Second)) {
		t.Fatal(state, err)
	}
	// The saved snapshot must not retain pointers into the caller's counters.
	*measurement.CPUTimeNS = 0
	*measurement.ElapsedNS = 1
	if *state.Window.CPUTimeNS != int64(100*time.Millisecond) || *state.Window.ElapsedNS != int64(time.Second) {
		t.Fatal("saved view aliases caller measurements", state)
	}
	measurement = CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(int64(100 * time.Millisecond)), ElapsedNS: cpuFeedbackInt(int64(time.Second))}
	before := cpuFeedbackBytes(t, s)
	if _, err = s.SettleCPUWindow(ctx, marker, completed, measurement); err != nil || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
		t.Fatal("exact retry changed the first result", err)
	}
	for _, retry := range []struct {
		at time.Time
		m  CPUWindowMeasurement
	}{
		{completed.Add(time.Nanosecond), measurement},
		{completed, CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: measurement.ElapsedNS}},
		{completed, CPUWindowMeasurement{ElapsedNS: measurement.ElapsedNS, Reason: "cpu_observation_unavailable"}},
	} {
		if _, err = s.SettleCPUWindow(ctx, marker, retry.at, retry.m); !errors.Is(err, ErrCPUFeedbackStale) || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
			t.Fatal("changed retry rewrote the terminal result", err)
		}
	}
}

func TestCPUFeedbackMeasuredZeroNoIdleCreditAndDispatchReplay(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, _, scope, turn := cpuFeedbackFixture(t, now, FairInventorySource)
	marker := beginCPUFeedback(t, s, turn, now)
	zero := CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}
	state, err := s.SettleCPUWindow(ctx, marker, now.Add(time.Nanosecond), zero)
	if err != nil || state.Window.CPUTimeNS == nil || *state.Window.CPUTimeNS != 0 || state.NextAllowedAt != nil || state.Status != "observed" {
		t.Fatal("measured zero became unknown or a wait", state, err)
	}
	if again, err := s.BeginCPUWindow(ctx, turn, now.Add(2*time.Nanosecond), CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: now}); !errors.Is(err, ErrCPUFeedbackStale) || again.Token() != "" {
		t.Fatal("settlement removed permanent dispatch replay fence", again.Token(), err)
	}
	if err = s.FinishJob(ctx, *turn.Job, false, []byte("kept"), time.Unix(0, 1), ""); err != nil {
		t.Fatal(err)
	}
	// A long idle earns no credit for a later window.
	later := now.Add(24 * time.Hour)
	turn = fairInventoryClaim(t, s, scope, later, true)
	marker = beginCPUFeedback(t, s, turn, later)
	state, err = s.SettleCPUWindow(ctx, marker, later.Add(time.Second), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(int64(100 * time.Millisecond)), ElapsedNS: cpuFeedbackInt(int64(time.Second))})
	if err != nil || state.Window.BackoffNS != int64(9*time.Second) || !state.NextAllowedAt.Equal(later.Add(10*time.Second)) {
		t.Fatal("idle erased the next wait", state, err)
	}
}

func TestCPUFeedbackConsumedClaimReceiptAndMutationFences(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	for _, edit := range []string{"peek", "kind", "root", "job token", "job cursor", "job lease", "new dispatch", "expired receipt", "wrong writer"} {
		t.Run(edit, func(t *testing.T) {
			s, _, scope, turn := cpuFeedbackFixture(t, now, FairInventorySource)
			at := now
			switch edit {
			case "peek":
				turn.receipt = nil
			case "kind":
				turn.Kind = FairInventoryMaintenance
			case "root":
				turn.RootID = 2
			case "job token":
				turn.Job.Token = strings.Repeat("b", 32)
			case "job cursor":
				turn.Job.Cursor = []byte("changed")
			case "job lease":
				if _, err := s.db.Exec("UPDATE jobs SET lease_token=? WHERE id=?", strings.Repeat("b", 32), turn.Job.ID); err != nil {
					t.Fatal(err)
				}
			case "new dispatch":
				if _, err := s.ReserveScanChunk(ctx, now.Add(time.Millisecond), time.Millisecond, 100000); err != nil {
					t.Fatal(err)
				}
			case "expired receipt":
				at = now.Add(FairInventoryClaimWindow)
			case "wrong writer":
				other, _, _, _ := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
				s = other
			}
			marker, err := s.BeginCPUWindow(ctx, turn, at, CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: now})
			if err == nil || marker.Token() != "" {
				t.Fatal("changed or unclaimed turn admitted", marker.Token(), err)
			}
			state, err := s.CPUFeedback(ctx)
			if err != nil || state.Status != "untracked" || state.Window != nil {
				t.Fatal("denial fabricated tracking", state, err)
			}
			_ = scope
		})
	}
}

func TestCPUFeedbackCooldownExactBoundaryPendingAndRollback(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, _, scope, turn := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
	marker := beginCPUFeedback(t, s, turn, now)
	if _, err := s.BeginCPUWindow(ctx, turn, now.Add(time.Nanosecond), CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: now}); !errors.Is(err, ErrCPUFeedbackPending) {
		t.Fatal(err)
	}
	before := cpuFeedbackBytes(t, s)
	if _, err := s.SettleCPUWindow(ctx, marker, now.Add(-time.Nanosecond), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}); !errors.Is(err, ErrCPUFeedbackClockRollback) || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
		t.Fatal("rollback cleared pending", err)
	}
	state, err := s.SettleCPUWindow(ctx, marker, now.Add(time.Second), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(int64(100 * time.Millisecond)), ElapsedNS: cpuFeedbackInt(int64(time.Second))})
	if err != nil {
		t.Fatal(err)
	}
	turn = fairInventoryClaim(t, s, scope, state.NextAllowedAt.Add(-time.Nanosecond), false)
	if marker, err := s.BeginCPUWindow(ctx, turn, state.NextAllowedAt.Add(-time.Nanosecond), CPUWindowStart{Instance: strings.Repeat("b", 32), WindowStartedAt: now}); !errors.Is(err, ErrCPUFeedbackDeferred) || marker.Token() != "" {
		t.Fatal("early admission", marker.Token(), err)
	}
	if _, err = s.BeginCPUWindow(ctx, turn, *state.NextAllowedAt, CPUWindowStart{Instance: strings.Repeat("b", 32), WindowStartedAt: now}); err != nil {
		t.Fatal("exact boundary rejected", err)
	}
}

func TestCPUFeedbackCompletedUnknownAndSaturatedCounts(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, _, scope, turn := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
	marker := beginCPUFeedback(t, s, turn, now)
	if _, err := s.db.Exec("UPDATE worker_cpu_feedback SET completed_unknown=?", int64(math.MaxInt64-1)); err != nil {
		t.Fatal(err)
	}
	m := CPUWindowMeasurement{ElapsedNS: cpuFeedbackInt(int64(time.Second)), Reason: "cpu_observation_unavailable"}
	state, err := s.SettleCPUWindow(ctx, marker, now.Add(time.Second), m)
	if err != nil || state.Status != "completed_unknown" || state.Window.CPUTimeNS != nil || state.NextAllowedAt != nil || state.CompletedUnknownWindows != math.MaxInt64 || !state.UnknownCountSaturated {
		t.Fatal(state, err)
	}
	if _, err = s.SettleCPUWindow(ctx, marker, now.Add(time.Second), m); err != nil {
		t.Fatal(err)
	}
	turn = fairInventoryClaim(t, s, scope, now.Add(2*time.Second), false)
	marker = beginCPUFeedback(t, s, turn, now.Add(2*time.Second))
	state, err = s.SettleCPUWindow(ctx, marker, now.Add(3*time.Second), m)
	if err != nil || state.CompletedUnknownWindows != math.MaxInt64 || !state.UnknownCountSaturated {
		t.Fatal("unknown count overflow", state, err)
	}
}

func TestCPUFeedbackWriterFailureAndOverflowPreservePending(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, _, _, turn := cpuFeedbackFixture(t, now, FairInventoryMaintenance)
	marker := beginCPUFeedback(t, s, turn, now)
	before := cpuFeedbackBytes(t, s)
	if _, err := s.db.Exec("CREATE TRIGGER cpu_fail BEFORE INSERT ON worker_cpu_feedback BEGIN SELECT RAISE(ABORT,'fixture publication failure'); END"); err != nil {
		t.Fatal(err)
	}
	if returned, err := s.SettleCPUWindow(ctx, marker, now.Add(time.Second), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}); err == nil || returned.Status != "pending" || returned.Window.SettledAt != nil || returned.NextAllowedAt != nil || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
		t.Fatal("failed settlement reported or saved a terminal candidate", returned, err)
	}
	if returned, err := s.RecoverCPUWindow(ctx, now.Add(time.Second)); err == nil || returned.Status != "pending" || returned.RecoveredUnknownWindows != 0 || returned.NextAllowedAt != nil || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
		t.Fatal("failed recovery reported or saved a terminal candidate", returned, err)
	}
	if _, err := s.db.Exec("DROP TRIGGER cpu_fail"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.RecoverCPUWindow(canceled, now); !errors.Is(err, context.Canceled) || !bytes.Equal(before, cpuFeedbackBytes(t, s)) {
		t.Fatal(err)
	}
	state, err := s.RecoverCPUWindow(ctx, now.Add(time.Second))
	if err != nil || state.RecoveredUnknownWindows != 1 || state.NextAllowedAt == nil || !state.NextAllowedAt.Equal(now.Add(time.Second+time.Hour)) {
		t.Fatal(state, err)
	}
	if _, err = s.SettleCPUWindow(ctx, marker, now.Add(time.Second), CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}); !errors.Is(err, ErrCPUFeedbackStale) {
		t.Fatal("late settlement erased recovery", err)
	}
	nearMax := time.Unix(0, math.MaxInt64-int64(time.Minute)-1).UTC()
	s2, _, _, turn2 := cpuFeedbackFixture(t, nearMax, FairInventoryMaintenance)
	marker2 := beginCPUFeedback(t, s2, turn2, nearMax)
	before = cpuFeedbackBytes(t, s2)
	late := time.Unix(0, math.MaxInt64-1).UTC()
	if _, err = s2.RecoverCPUWindow(ctx, late); !errors.Is(err, ErrCPUFeedbackInvalid) || !bytes.Equal(before, cpuFeedbackBytes(t, s2)) {
		t.Fatal("deadline overflow lost pending", err)
	}
	if _, err = s2.SettleCPUWindow(ctx, marker2, late, CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(math.MaxInt64), ElapsedNS: cpuFeedbackInt(1)}); !errors.Is(err, ErrCPUFeedbackInvalid) || !bytes.Equal(before, cpuFeedbackBytes(t, s2)) {
		t.Fatal("feedback overflow lost pending", err)
	}
}

func TestCPUFeedbackReaderAndRestartRecoverExactlyOnce(t *testing.T) {
	ctx, now := context.Background(), cpuFeedbackNow()
	s, dir, _, turn := cpuFeedbackFixture(t, now, FairInventorySource)
	marker := beginCPUFeedback(t, s, turn, now)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	state, err := r.CPUFeedback(ctx)
	if err != nil || state.Status != "pending" || state.RecoveredUnknownWindows != 0 {
		t.Fatal(state, err)
	}
	if _, err = r.RecoverCPUWindow(ctx, now); !errors.Is(err, ErrCPUFeedbackReadOnly) {
		t.Fatal(err)
	}
	r.Close()
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	state, err = w.CPUFeedback(ctx)
	if err != nil || state.Status != "pending" {
		t.Fatal("opening recovered accounting", state, err)
	}
	if _, err = w.SettleCPUWindow(ctx, marker, now, CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}); !errors.Is(err, ErrCPUFeedbackInvalid) {
		t.Fatal("reopened writer accepted a former process baseline", err)
	}
	before := cpuFeedbackBytes(t, w)
	if _, err = w.RecoverCPUWindow(ctx, now.Add(-time.Nanosecond)); !errors.Is(err, ErrCPUFeedbackClockRollback) || !bytes.Equal(before, cpuFeedbackBytes(t, w)) {
		t.Fatal(err)
	}
	recoveredAt := now.Add(time.Minute)
	state, err = w.RecoverCPUWindow(ctx, recoveredAt)
	if err != nil || state.Status != "recovered_unknown" || state.Window.CPUTimeNS != nil || state.RecoveredUnknownWindows != 1 || !state.NextAllowedAt.Equal(recoveredAt.Add(time.Hour)) {
		t.Fatal(state, err)
	}
	terminal := cpuFeedbackBytes(t, w)
	if _, err = w.RecoverCPUWindow(ctx, recoveredAt.Add(time.Minute)); err != nil || !bytes.Equal(terminal, cpuFeedbackBytes(t, w)) {
		t.Fatal("repeat recovery renewed cooldown", err)
	}
	w.Close()
	w, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = w.RecoverCPUWindow(ctx, recoveredAt.Add(2*time.Hour)); err != nil || !bytes.Equal(terminal, cpuFeedbackBytes(t, w)) {
		t.Fatal("restart reset or extended cooldown", err)
	}
}

func TestCPUFeedbackCorruptionDoesNotResetTracking(t *testing.T) {
	for _, edit := range []string{
		"max_now_ns=0", "last_begun_dispatch_ns=1", "completed_unknown=0.5", "recovered_unknown=-1", "count_saturated=1", "token='bad'", "instance='bad'", "kind='other'", "root_id=0", "job_id=0", "job_token='bad'", "job_lease_until_ns=NULL", "dispatch_reserved_ns=0", "window_started_ns=0", "recorded_ns=0", "status='other'", "settled_ns=1", "cpu_time_ns=0", "elapsed_ns=0", "reason='other'", "backoff_ns=1", "backoff_capped=1", "next_allowed_ns=1", "singleton=2",
	} {
		t.Run(edit, func(t *testing.T) {
			now := cpuFeedbackNow()
			s, _, _, turn := cpuFeedbackFixture(t, now, FairInventorySource)
			marker := beginCPUFeedback(t, s, turn, now)
			if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON; UPDATE worker_cpu_feedback SET " + edit); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CPUFeedback(context.Background()); !errors.Is(err, ErrCPUFeedbackCorrupt) {
				t.Fatal("corrupt saved record accepted", err)
			}
			if _, err := s.RecoverCPUWindow(context.Background(), now); !errors.Is(err, ErrCPUFeedbackCorrupt) {
				t.Fatal("corrupt record recovered", err)
			}
			if _, err := s.SettleCPUWindow(context.Background(), marker, now, CPUWindowMeasurement{CPUTimeNS: cpuFeedbackInt(0), ElapsedNS: cpuFeedbackInt(1)}); !errors.Is(err, ErrCPUFeedbackCorrupt) {
				t.Fatal("corrupt record settled", err)
			}
		})
	}
}

func cpuFeedbackSchemaFixture(t *testing.T, version int, conflict bool) (string, string) {
	t.Helper()
	dir := privateDir(t)
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := connect(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < version; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec(fmt.Sprintf("PRAGMA application_id=0x52594444; PRAGMA user_version=%d; INSERT INTO settings VALUES('preserve',X'00ff')", version)); err != nil {
		t.Fatal(err)
	}
	if conflict {
		if _, err = s.db.Exec("CREATE TABLE worker_cpu_feedback(foreign_value TEXT)"); err != nil {
			t.Fatal(err)
		}
	}
	return dir, path
}

func TestCPUFeedbackOldSchemaReadOnlyCompatibility(t *testing.T) {
	for version := 4; version <= 11; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx := context.Background()
			dir, path := cpuFeedbackSchemaFixture(t, version, false)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			state, err := r.CPUFeedback(ctx)
			if err != nil || state.Available || state.Status != "unavailable" || state.Window != nil || state.PreTrackingCPU != "unknown" {
				t.Fatal(state, err)
			}
			r.Close()
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("saved reader migrated schema", err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			state, err = w.CPUFeedback(ctx)
			if err != nil || !state.Available || state.Status != "untracked" || state.TrackingStartedAt != nil || state.Window != nil {
				t.Fatal("migration fabricated a window", state, err)
			}
			var kept []byte
			if err = w.db.QueryRow("SELECT value FROM settings WHERE key='preserve'").Scan(&kept); err != nil || !bytes.Equal(kept, []byte{0, 255}) {
				t.Fatal(kept, err)
			}
		})
	}
}

func TestCPUFeedbackMigrationConflictPreservesSchema11(t *testing.T) {
	ctx := context.Background()
	dir, _ := cpuFeedbackSchemaFixture(t, 11, true)
	if _, err := OpenWriter(ctx, dir); err == nil {
		t.Fatal("conflicting table accepted")
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var version, ledger int
	if err = r.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 11 {
		t.Fatal(version, err)
	}
	if err = r.db.QueryRow("SELECT count(*) FROM schema_migrations WHERE version=12").Scan(&ledger); err != nil || ledger != 0 {
		t.Fatal(ledger, err)
	}
	state, err := r.CPUFeedback(ctx)
	if err != nil || state.Available {
		t.Fatal(state, err)
	}
}

func TestCPUFeedbackWaitOverflowAndCap(t *testing.T) {
	for _, test := range []struct {
		cpu, elapsed, want time.Duration
		capped             bool
	}{
		{0, time.Second, 0, false}, {15 * time.Millisecond, time.Second, 500 * time.Millisecond, false}, {2, 199, 1, false},
		{37 * time.Second, 100 * time.Second, time.Hour, false}, {37 * time.Second, time.Second, time.Hour, true},
		{time.Duration(math.MaxInt64), time.Second, time.Hour, true}, {time.Duration(math.MaxInt64) / 100, time.Duration(math.MaxInt64) - 99, 92, false},
	} {
		if got, capped := CPUFeedbackWait(test.cpu, test.elapsed); got != test.want || capped != test.capped {
			t.Fatal(got, capped, test)
		}
	}
}
