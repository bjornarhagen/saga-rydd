package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func fixture(t *testing.T) (string, config.Config) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	c := config.Default()
	c.Roots = []string{"/synthetic"}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	w.Close()
	return dir, c
}

func start(t *testing.T, dir string, c config.Config, options Options) (Snapshot, <-chan error) {
	t.Helper()
	snapshot, done, _ := startCancelable(t, dir, c, options)
	return snapshot, done
}

func startCancelable(t *testing.T, dir string, c config.Config, options Options, capture ...func(context.CancelFunc, <-chan error)) (Snapshot, <-chan error, context.CancelFunc) {
	t.Helper()
	// These fixtures test lifecycle/cadence, independently of host process CPU.
	if options.powerCoordinator == nil {
		options.powerCoordinator = fixturePowerCoordinator(options.wallNow, options.elapsedNow)
	}
	if options.ExperimentalScan && options.inventoryStateOwner == nil {
		t.Fatal("experimental fixture requires its explicit retained state owner")
	}
	if options.cpuObserve == nil {
		options.cpuObserve = func() (time.Duration, error) { return 0, nil }
	}
	// Native scheduling changes are exercised only in disposable child processes.
	if options.priorityRequest == nil {
		options.priorityRequest = func(context.Context) ThreadPriorityObservation {
			return newThreadPriorityObservation("fixture")
		}
	}
	ready := make(chan Snapshot, 1)
	options.Ready = func(s Snapshot) { ready <- s }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	for _, save := range capture {
		save(cancel, done)
	}
	go func() { defer close(done); done <- Run(ctx, dir, c, options) }()
	select {
	case s := <-ready:
		return s, done, cancel
	case err := <-done:
		t.Fatal("worker did not start", err)
	case <-time.After(5 * time.Second):
		t.Fatal("start timed out")
	}
	return Snapshot{}, nil, cancel
}

func control(t *testing.T, dir, command string) Snapshot {
	t.Helper()
	s, err := Send(context.Background(), dir, command)
	if err != nil {
		t.Fatal(command, err)
	}
	return s
}

func waitExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func waitUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition timed out")
}

// These selected fixtures keep their original predicates. The default wait is
// five seconds; the multi-turn fairness fixture selects its own finite wait.
// Only a failed wait collects sequential diagnostic scalars, under one separate
// two-second deadline. It reads no source and never receives a worker result.
type inventoryWaitStatus struct {
	Seen, Paused, Stopping                 bool
	ActiveJob                              int64
	WaitReason                             string
	DispatchPresent                        bool
	DispatchUsed                           int
	DispatchReason                         string
	StatePresent                           bool
	StatePhase, StateDecision, StateReason string
	SamplingState                          string
	CallbackReturned, ProcessSlotOccupied  bool
	AttemptStartedNS, AttemptFinishedNS    int64
	RetryAfterNS                           int64
	ObservationStatus, ObservationReason   string
	MetricsPresent, Throttled              bool
	EntryInspections, ThrottleWaitNS       uint64
	AdaptivePresent                        bool
	ActiveEpochs, StableRoots, WeeklyRoots int
}

type inventoryWaitSaved struct {
	SummarySeen, DueSeen                      bool
	Entries, CompleteDirectories              int64
	PendingJobs, RunningJobs, DirectoryErrors int64
	DueNS                                     int64
}

type inventoryWaitEvidence struct {
	CachedStatus                  inventoryWaitStatus
	CachedSaved                   inventoryWaitSaved
	Listings, WantedListings      int64
	WantedWeekly                  bool
	Generation                    int64
	WideEntries, SmallCompletions int
	WideComplete, SourceFailure   bool
}

func inventoryWaitCode(code string) string {
	switch code {
	case "", "paused", "stopping", "idle", "running", "cadence", "clock_rollback", "daily_chunk_limit",
		"job_retry", "inventory_revisit", "inventory_revisit_setup", "inventory_maintenance",
		"inventory_state_source_backoff", "power_source_backoff", "wal_backpressure",
		"cpu_backoff", "cpu_accounting_pending", "cpu_clock_rollback", "cpu_recovery_backoff",
		"cpu_hour_charge_partial", "cpu_day_charge_partial", "cpu_hour_charge_backoff", "cpu_day_charge_backoff",
		"daily_metadata_limit", "metadata_backoff", "api_pacing", "api_capacity_blocked", "api_profile_blocked",
		"before_dispatch", "after_receipt", "not_evaluated", "source_deferred", "source_admitted",
		"not_requested", "pending", "returned", "refused", "foreign_slot", "below_limit", "limit_reached",
		"inventory_state_pending", "inventory_state_process_slot", "inventory_state_retry", "inventory_state_limit",
		"inventory_state_clock", "inventory_state_timeout", "inventory_state_canceled", "inventory_state_unavailable",
		"inventory_state_changed", "inventory_state_overflow", "inventory_state_invalid", "inventory_state_expired":
		return code
	default:
		return "unrecognized"
	}
}

func inventoryWaitStatusFrom(s Snapshot) inventoryWaitStatus {
	d := inventoryWaitStatus{Seen: true, Paused: s.Paused, Stopping: s.Stopping, ActiveJob: s.ActiveJob, WaitReason: inventoryWaitCode(s.WaitReason)}
	if b := s.Dispatch; b != nil {
		d.DispatchPresent, d.DispatchUsed, d.DispatchReason = true, b.Used, inventoryWaitCode(b.Reason)
	}
	if p := s.InventoryState; p != nil {
		d.StatePresent = true
		d.StatePhase, d.StateDecision, d.StateReason = inventoryWaitCode(p.LastAttemptPhase), inventoryWaitCode(p.LastDecisionStatus), inventoryWaitCode(p.LastDecisionReason)
		d.SamplingState, d.CallbackReturned, d.ProcessSlotOccupied = inventoryWaitCode(p.SamplingState), p.CallbackReturned, p.ProcessSlotOccupied
		if p.LastAttemptStartedAt != nil {
			d.AttemptStartedNS = p.LastAttemptStartedAt.UnixNano()
		}
		if p.LastAttemptFinishedAt != nil {
			d.AttemptFinishedNS = p.LastAttemptFinishedAt.UnixNano()
		}
		if p.RetryAfterAt != nil {
			d.RetryAfterNS = p.RetryAfterAt.UnixNano()
		}
		if o := p.Observation; o != nil {
			d.ObservationStatus, d.ObservationReason = inventoryWaitCode(o.Status), inventoryWaitCode(o.Reason)
		}
	}
	if m := s.InventoryMetrics; m != nil {
		d.MetricsPresent, d.Throttled, d.EntryInspections, d.ThrottleWaitNS = true, m.Throttled, m.EntryInspections, m.ThrottleWaitNS
	}
	if a := s.AdaptiveRevisits; a != nil {
		d.AdaptivePresent, d.ActiveEpochs, d.StableRoots, d.WeeklyRoots = true, a.ActiveEpochs, a.StableRoots, a.WeeklyRoots
	}
	return d
}

func inventoryWaitSavedFrom(s state.Summary, due time.Time) inventoryWaitSaved {
	d := inventoryWaitSaved{SummarySeen: true, DueSeen: true, Entries: s.Entries, CompleteDirectories: s.CompleteDirectories, PendingJobs: s.PendingJobs, RunningJobs: s.RunningJobs, DirectoryErrors: s.DirectoryErrors}
	if !due.IsZero() {
		d.DueNS = due.UnixNano()
	}
	return d
}

func inventoryWaitErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}

func waitUntilInventoryFixture(t *testing.T, dir string, predicate func() bool, evidence func() inventoryWaitEvidence) {
	t.Helper()
	waitUntilInventoryFixtureWithin(t, dir, 5*time.Second, predicate, evidence)
}

// Completion ordering across real source and maintenance turns is separate
// from the unchanged start/control waits and production resource deadlines.
func waitUntilInventoryFixtureWithin(t *testing.T, dir string, timeout time.Duration, predicate func() bool, evidence func() inventoryWaitEvidence) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Cached predicate evidence and these later reads are not one snapshot.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d := struct {
		Contract, WorkerResult                                       string
		CachedPredicate                                              inventoryWaitEvidence
		LaterStatus                                                  inventoryWaitStatus
		LaterSaved                                                   inventoryWaitSaved
		StatusError, ReaderError, SummaryError, DueError, CloseError string
	}{Contract: "failure_only_sequential_observations_not_current", WorkerResult: "unknown_not_received", CachedPredicate: evidence(), StatusError: "not_attempted", ReaderError: "not_attempted", SummaryError: "not_attempted", DueError: "not_attempted", CloseError: "not_attempted"}
	live, err := Send(ctx, dir, "status")
	d.StatusError = inventoryWaitErrorClass(err)
	if err == nil {
		d.LaterStatus = inventoryWaitStatusFrom(live)
	}
	if ctx.Err() == nil {
		r, openErr := state.OpenReader(ctx, dir)
		d.ReaderError = inventoryWaitErrorClass(openErr)
		if openErr == nil {
			summary, summaryErr := r.Summary(ctx)
			d.SummaryError = inventoryWaitErrorClass(summaryErr)
			if summaryErr == nil {
				d.LaterSaved = inventoryWaitSavedFrom(summary, time.Time{})
				d.LaterSaved.DueSeen = false
			}
			if ctx.Err() == nil {
				due, dueErr := r.NextJobDue(ctx, []string{state.ScanKind})
				d.DueError = inventoryWaitErrorClass(dueErr)
				if dueErr == nil {
					d.LaterSaved.DueSeen = true
					if !due.IsZero() {
						d.LaterSaved.DueNS = due.UnixNano()
					}
				}
			}
			d.CloseError = inventoryWaitErrorClass(r.Close())
		}
	}
	encoded, encodeErr := json.Marshal(d)
	if encodeErr != nil || len(encoded) > 8<<10 {
		t.Log("bounded failure-only inventory diagnostics unavailable")
	} else {
		t.Log("failure-only inventory diagnostics:", string(encoded))
	}
	t.Fatal("condition timed out")
}

func TestLifecyclePersistentPauseAndWriterExclusion(t *testing.T) {
	dir, c := fixture(t)
	initial, done := start(t, dir, c, Options{})
	if initial.Paused || initial.Handlers != 0 {
		t.Fatal(initial)
	}
	if w, err := state.OpenWriter(context.Background(), dir); !errors.Is(err, localfs.ErrLocked) {
		if w != nil {
			w.Close()
		}
		t.Fatal("writer was not excluded", err)
	}
	if s := control(t, dir, "pause"); !s.Paused {
		t.Fatal(s)
	}
	if s := control(t, dir, "pause"); !s.Paused {
		t.Fatal("pause not idempotent")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if paused, err := r.Paused(context.Background()); err != nil || !paused {
		t.Fatal(paused, err)
	}
	r.Close()
	if s := control(t, dir, "stop"); !s.Stopping {
		t.Fatal(s)
	}
	waitExit(t, done)
	if _, err := Send(context.Background(), dir, "status"); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
	restarted, done := start(t, dir, c, Options{})
	if !restarted.Paused || restarted.Instance == initial.Instance {
		t.Fatal(restarted)
	}
	if s := control(t, dir, "resume"); s.Paused {
		t.Fatal(s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestChunkCheckpointPauseAndResume(t *testing.T) {
	dir, c := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var calls atomic.Int32
	starts := make(chan time.Time, 3)
	entered := make(chan struct{}, 1)
	h := func(ctx context.Context, j state.Job) (Result, error) {
		starts <- time.Now()
		n := calls.Add(1)
		if n == 1 {
			return Result{Cursor: []byte("checkpoint")}, nil
		}
		if string(j.Cursor) != "checkpoint" {
			return Result{}, errors.New("missing checkpoint")
		}
		if n == 2 {
			entered <- struct{}{}
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		return Result{Done: true}, nil
	}
	_, done := start(t, dir, c, fixtureInventoryStateOptions(t, Options{Handlers: map[string]Handler{"fixture": h}, Interval: 50 * time.Millisecond, WorkDuration: time.Second}))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second chunk not dispatched")
	}
	first, second := <-starts, <-starts
	if second.Sub(first) < 40*time.Millisecond {
		t.Fatal("chunks bypassed interval pacing", second.Sub(first))
	}
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	if calls.Load() != 2 {
		t.Fatal("dispatched while paused")
	}
	control(t, dir, "resume")
	waitUntil(t, func() bool { return calls.Load() == 3 && control(t, dir, "status").ActiveJob == 0 })
	control(t, dir, "stop")
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.PendingJobs != 0 || summary.RunningJobs != 0 {
		t.Fatal(summary, err)
	}
}

func TestFailedChunkPreservesCursorAndBacksOff(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			dir, c := fixture(t)
			ctx := context.Background()
			w, err := state.OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.EnqueueJob(ctx, 1, "fixture", nil, time.Now()); err != nil {
				t.Fatal(err)
			}
			w.Close()
			entered := make(chan time.Time, 1)
			h := func(context.Context, state.Job) (Result, error) {
				entered <- time.Now()
				if panics {
					panic("synthetic failure")
				}
				return Result{Done: true, Cursor: []byte("uncommitted")}, errors.New("synthetic failure")
			}
			_, done := start(t, dir, c, fixtureInventoryStateOptions(t, Options{Handlers: map[string]Handler{"fixture": h}, Interval: time.Millisecond}))
			var started time.Time
			select {
			case started = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("handler not called")
			}
			waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
			control(t, dir, "stop")
			waitExit(t, done)
			w, err = state.OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			due, err := w.NextJobDue(ctx, []string{"fixture"})
			if err != nil || due.Before(started.Add(2*time.Second)) {
				t.Fatal("missing retry backoff", due, err)
			}
			j, err := w.ClaimJob(ctx, []string{"fixture"}, due, time.Minute)
			if err != nil || j == nil || len(j.Cursor) != 0 || j.Attempts != 2 {
				t.Fatal("failed result committed or attempts lost", j, err)
			}
		})
	}
}

func TestControlValidationAndSocketProtection(t *testing.T) {
	dir, c := fixture(t)
	path, err := Endpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := localfs.EnsurePrivateDir(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), dir, c, Options{}); err == nil {
		t.Fatal("overwrote regular file at endpoint")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep" {
		t.Fatal("endpoint file modified")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Leave an actual stale socket to exercise cleanup after acquiring the lock.
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	os.Chmod(path, 0600)
	stale.Close()
	_, done := start(t, dir, c, Options{})
	for _, request := range []string{
		`{"version":999,"command":"pause"}`,
		`{"version":1,"command":"pause","unexpected":true}`,
		`{"version":1,"command":"unknown"}`,
		`{"version":1,"command":"pause"} {}`,
		strings.Repeat("x", 4097),
	} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte(request + "\n")); err != nil {
			t.Fatal(err)
		}
		var response Response
		err = json.NewDecoder(conn).Decode(&response)
		conn.Close()
		if err != nil || response.OK {
			t.Fatal(response, err)
		}
	}
	if s := control(t, dir, "status"); s.Paused {
		t.Fatal("invalid request changed pause")
	}
	if err := os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := Send(context.Background(), dir, "pause"); err == nil {
		t.Fatal("insecure endpoint accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestDailyDispatchCapAcrossWorkerRestart(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MaxScanChunksPerDay = 1
	for i := 0; i < 140; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	options := fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: 10 * time.Millisecond})
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_chunk_limit" })
	first := control(t, dir, "status")
	if first.ActiveJob != 0 || first.InventoryMetrics == nil || first.Metadata == nil || first.Metadata.TotalCharges == nil {
		t.Fatal("missing settled native scanner accounting", first)
	}
	m := first.InventoryMetrics
	apiAttempts := m.StatCalls + m.DirectoryOpenCalls + m.DirectoryReadCalls + m.FilesystemStatCalls + m.MountIdentityCalls + m.PathResolutionCalls
	if first.Metadata.TotalCharges.OutstandingReserved != 0 || first.Metadata.TotalCharges.Observed != int64(apiAttempts) || apiAttempts == 0 {
		t.Fatal("settled permits differ from actual scanner API counters", first.Metadata, m)
	}
	control(t, dir, "pause")
	control(t, dir, "resume")
	control(t, dir, "stop")
	waitExit(t, done)
	_, done = start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_chunk_limit" })
	snapshot := control(t, dir, "status")
	if snapshot.InventoryMetrics != nil {
		t.Fatal("quota-blocked restart eagerly constructed the scanner", snapshot)
	}
	if snapshot.Metadata == nil || snapshot.Metadata.TotalCharges == nil || snapshot.Metadata.TotalCharges.Reserved < 65536 || snapshot.Metadata.TotalCharges.OutstandingReserved != 0 {
		t.Fatal("missing retained metadata accounting", snapshot)
	}
	if snapshot.Dispatch == nil || snapshot.Dispatch.Used != 1 || snapshot.ActiveJob != 0 {
		t.Fatal(snapshot)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Entries != 129 {
		t.Fatalf("daily cap allowed extra batch: %+v", summary)
	}
}

func TestPacedInventoryControlsAndCompletion(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 5
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, done := start(t, dir, c, fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: 350 * time.Millisecond, WorkDuration: 300 * time.Millisecond}))
	waitUntil(t, func() bool { m := control(t, dir, "status").InventoryMetrics; return m != nil && m.Throttled })
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	control(t, dir, "resume")
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var lastSaved inventoryWaitSaved
	waitUntilInventoryFixture(t, dir, func() bool {
		summary, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		due, dueErr := r.NextJobDue(context.Background(), []string{state.ScanKind})
		if dueErr != nil {
			t.Fatal(dueErr)
		}
		lastSaved = inventoryWaitSavedFrom(summary, due)
		return summary.Entries == 6 && summary.CompleteDirectories == 1 && summary.PendingJobs == 1 && summary.RunningJobs == 0 && due.After(time.Now().Add(23*time.Hour))
	}, func() inventoryWaitEvidence { return inventoryWaitEvidence{CachedSaved: lastSaved} })
	m := control(t, dir, "status").InventoryMetrics
	if m == nil || m.EntryRatePerSecond != 5 || m.EntryInspections < 5 || m.ThrottleWaitNS == 0 {
		t.Fatal(m)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCPUBackoffControlsAndDelayedResume(t *testing.T) {
	dir, c := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var calls atomic.Int64
	var samples int64
	observe := func() (time.Duration, error) {
		samples++ // Called only by the owning worker loop.
		return time.Duration(samples/2) * 10 * time.Millisecond, nil
	}
	handler := func(context.Context, state.Job) (Result, error) {
		calls.Add(1)
		return Result{Cursor: []byte("saved progress")}, nil
	}
	_, done := start(t, dir, c, fixtureInventoryStateOptions(t, Options{Handlers: map[string]Handler{"fixture": handler}, Interval: time.Millisecond, cpuObserve: observe}))
	var first Snapshot
	waitUntil(t, func() bool {
		first = control(t, dir, "status")
		return first.WaitReason == "cpu_backoff" && first.ActiveJob == 0
	})
	if first.CPU == nil || first.CPU.Status != "observed" || first.CPU.WindowCPUNS == nil || *first.CPU.WindowCPUNS != int64(10*time.Millisecond) || first.CPU.NextAllowedAt == nil || first.CPU.BackoffNS <= 0 || calls.Load() != 1 {
		t.Fatal("missing completed window feedback", first, calls.Load())
	}
	control(t, dir, "pause")
	if resumed := control(t, dir, "resume"); resumed.CPU == nil || resumed.CPU.NextAllowedAt == nil || !resumed.CPU.NextAllowedAt.Equal(*first.CPU.NextAllowedAt) {
		t.Fatal("resume erased the pending CPU deadline", resumed)
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("resume bypassed CPU backoff", calls.Load())
	}
	control(t, dir, "pause")
	// Resume after the prior deadline to model a delayed wake without an OS sleep.
	time.Sleep(max(time.Until(*first.CPU.NextAllowedAt), 0) + 20*time.Millisecond)
	control(t, dir, "resume")
	waitUntil(t, func() bool { return calls.Load() == 2 && control(t, dir, "status").WaitReason == "cpu_backoff" })
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatal("delayed resume caused catch-up work", calls.Load())
	}
	stoppedAt := time.Now()
	control(t, dir, "stop")
	waitExit(t, done)
	if time.Since(stoppedAt) > time.Second {
		t.Fatal("CPU backoff delayed stop")
	}
}

func TestWorkerCPUUnknownUsesConfiguredCadence(t *testing.T) {
	dir, c := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var calls atomic.Int64
	var samples int
	observe := func() (time.Duration, error) {
		samples++
		if samples <= 2 {
			return 0, errors.New("synthetic observation unavailable")
		}
		return 0, nil
	}
	handler := func(context.Context, state.Job) (Result, error) {
		return Result{Done: calls.Add(1) == 2}, nil
	}
	_, done := start(t, dir, c, fixtureInventoryStateOptions(t, Options{Handlers: map[string]Handler{"fixture": handler}, Interval: 300 * time.Millisecond, cpuObserve: observe}))
	var first Snapshot
	waitUntil(t, func() bool {
		first = control(t, dir, "status")
		return first.CPU != nil && first.CPU.Status == "unknown"
	})
	if first.CPU.WindowCPUNS != nil || first.CPU.UnknownObservations != 1 || first.CPU.NextAllowedAt != nil || first.WaitReason != "cadence" || calls.Load() != 1 {
		t.Fatal("unknown CPU became a bound or bypassed cadence", first, calls.Load())
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("unknown CPU bypassed configured cadence")
	}
	waitUntil(t, func() bool {
		snapshot := control(t, dir, "status")
		return snapshot.WaitReason == "idle" && snapshot.CPU != nil && snapshot.CPU.Status == "observed" && snapshot.CPU.WindowCPUNS != nil && *snapshot.CPU.WindowCPUNS == 0 && snapshot.CPU.UnknownObservations == 1
	})
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerCPUWindowIncludesProgressCommit(t *testing.T) {
	for _, mode := range []string{"finished", "handler error", "canceled", "scan commit"} {
		t.Run(mode, func(t *testing.T) {
			dir, c := fixture(t)
			options := fixtureInventoryStateOptions(t, Options{Interval: time.Millisecond})
			if mode == "scan commit" {
				c.Roots = []string{t.TempDir()}
				options.ExperimentalScan = true
			} else {
				w, err := state.OpenWriter(context.Background(), dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
					t.Fatal(err)
				}
				w.Close()
				options.Handlers = map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
					switch mode {
					case "handler error":
						return Result{}, errors.New("synthetic handler failure")
					case "canceled":
						return Result{}, context.Canceled
					default:
						return Result{Done: true}, nil
					}
				}}
			}
			r, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			type savedObservation struct {
				summary state.Summary
				err     error
			}
			afterCommit := make(chan savedObservation, 1)
			var samples int
			options.cpuObserve = func() (time.Duration, error) {
				samples++
				if samples == 2 {
					summary, err := r.Summary(context.Background())
					afterCommit <- savedObservation{summary, err}
				}
				return time.Duration(samples/2) * 20 * time.Millisecond, nil
			}
			if mode == "scan commit" {
				var sourceEntered, committedObserved atomic.Bool
				options.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
					sourceEntered.Store(true)
					return scanner.NextPermitted(ctx, job, permit)
				}
				options.cpuObserve = func() (time.Duration, error) {
					// Setup has its own windows. Observe the first source window
					// after its progress commit, independent of setup call count.
					if !sourceEntered.Load() {
						return 0, nil
					}
					if committedObserved.CompareAndSwap(false, true) {
						summary, err := r.Summary(context.Background())
						afterCommit <- savedObservation{summary, err}
					}
					return 20 * time.Millisecond, nil
				}
			}
			_, done := start(t, dir, c, options)
			var saved savedObservation
			select {
			case saved = <-afterCommit:
			case <-time.After(5 * time.Second):
				t.Fatal("completed-window observation missing")
			}
			if saved.err != nil || saved.summary.RunningJobs != 0 {
				t.Fatal("CPU observed before progress was saved", saved)
			}
			if mode == "finished" && saved.summary.PendingJobs != 0 {
				t.Fatal("completion not committed before CPU observation", saved.summary)
			}
			if mode == "scan commit" {
				due, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
				if err != nil || saved.summary.Entries != 1 || saved.summary.CompleteDirectories != 1 || saved.summary.PendingJobs != 0 || !due.IsZero() {
					t.Fatal("inventory completion not committed before CPU observation", saved.summary, due, err)
				}
				if pending, err := r.HasSubtreeRetirement(context.Background()); err != nil || !pending {
					t.Fatal("future revisit bypassed saved maintenance", pending, err)
				}
			}
			if (mode == "handler error" || mode == "canceled") && saved.summary.PendingJobs != 1 {
				t.Fatal("interrupted progress not retained", saved.summary)
			}
			var snapshot Snapshot
			waitUntil(t, func() bool {
				snapshot = control(t, dir, "status")
				return snapshot.CPU != nil && snapshot.CPU.Status == "observed"
			})
			if snapshot.CPU.WindowCPUNS == nil || *snapshot.CPU.WindowCPUNS != int64(20*time.Millisecond) || snapshot.CPU.WindowElapsedNS == nil || *snapshot.CPU.WindowElapsedNS <= 0 {
				t.Fatal("completed/error/canceled CPU delta lost", snapshot.CPU)
			}
			if mode == "handler error" && snapshot.WaitReason != "job_retry" {
				t.Fatal("CPU reason concealed later handler retry", snapshot.WaitReason)
			}
			control(t, dir, "stop")
			waitExit(t, done)
		})
	}
}

// Allocate a fixture process lifetime explicitly where Options is constructed.
// Repeated Runs retain these Options (and owner); start never resets the slot.
func fixtureInventoryStateOptions(t *testing.T, options Options) Options {
	t.Helper()
	if options.inventoryStateOwner != nil {
		return options
	}
	wall, elapsed := options.wallNow, options.elapsedNow
	if wall == nil {
		wall = time.Now
	}
	if elapsed == nil {
		elapsed = time.Now
	}
	owner, err := newInventoryStateFixtureOwner(wall, elapsed)
	if err != nil {
		t.Fatal(err)
	}
	options.inventoryStateOwner = owner
	if options.inventoryStateObserve == nil && (options.wallNow != nil || options.elapsedNow != nil) {
		options.inventoryStateObserve = func(ctx context.Context, sampler state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			started := wall().Round(0).UTC()
			report, err := sampler.Observe(ctx, limit)
			if err == nil {
				report.SampleStartedAt, report.SampleFinishedAt = started, wall().Round(0).UTC()
			}
			return report, err
		}
	}
	return options
}
