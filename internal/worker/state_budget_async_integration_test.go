package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

// Every blocked callback fixture owns its release and Run join before assertions.
// No permanently stalled goroutine is left in the package process.
func cleanupAsyncStateWorker(t *testing.T, owner *inventoryStateFixtureOwner, release chan struct{}, cancel *context.CancelFunc, done *<-chan error) func() {
	t.Helper()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		if *cancel != nil {
			(*cancel)()
		}
		unblock()
		if *done != nil {
			select {
			case <-*done:
			case <-time.After(8 * time.Second):
				t.Error("async fixture Run did not join")
			}
		}
		if ticket := owner.coordinator.cachedStatus(nil).Ticket; ticket != nil {
			select {
			case <-ticket.done:
			case <-time.After(5 * time.Second):
				t.Error("released callback did not join")
			}
		}
	})
	return unblock
}

func asyncStateWorkerOptions(t *testing.T) Options {
	t.Helper()
	return fixtureInventoryStateOptions(t, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second})
}

func TestWorkerAsyncInventoryStateBlockedPreControlsAndRetainedRestart(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	cfg := config.Default()
	cfg.Roots = []string{root}
	cfg.Scan.PauseOnBattery = false
	entered, release := make(chan struct{}), make(chan struct{})
	o := asyncStateWorkerOptions(t)
	var samples, sources atomic.Int64
	o.inventoryStateObserve = func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		samples.Add(1)
		close(entered)
		<-release
		return fixtureInventoryStateReport(limit, 1, time.Now()), ctx.Err()
	}
	noRetirementSource(t, &o, &sources)
	var cancel context.CancelFunc
	var done <-chan error
	_ = cleanupAsyncStateWorker(t, o.inventoryStateOwner, release, &cancel, &done)
	_, done, cancel = startCancelable(t, dir, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitAsyncInventoryEvent(t, entered)
	first := control(t, dir, "status")
	if first.InventoryState.SamplingState != "pending" || !first.InventoryState.ProcessSlotOccupied || first.InventoryState.CallbackReturned || first.Dispatch == nil || first.Dispatch.Used != 0 || first.CPUFeedback.Window != nil || sources.Load() != 0 {
		t.Fatal(first, samples.Load(), sources.Load())
	}
	for _, command := range []string{"status", "pause", "resume", "status"} {
		control(t, dir, command)
	}
	if samples.Load() != 1 || sources.Load() != 0 {
		t.Fatal("control sampled or claimed source", samples.Load(), sources.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
	// The same fixture capability survives writer close. A different generated
	// store and threshold cannot borrow the old callback or reset its retry.
	other := filepath.Join(t.TempDir(), "other-state")
	cfg.Scan.MaxStateBytes++
	_, done, cancel = startCancelable(t, other, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitUntil(t, func() bool { return control(t, other, "status").InventoryState.SamplingState == "foreign_slot" })
	next := control(t, other, "status")
	if samples.Load() != 1 || next.InventoryState.Observation != nil || !next.InventoryState.ProcessSlotOccupied || (next.Dispatch != nil && next.Dispatch.Used != 0) || sources.Load() != 0 {
		t.Fatal("successor adopted or replaced callback", next, samples.Load())
	}
	// The foreign slot can defer SOURCE before this Run queries live dispatch.
	// Inspect the saved budget and window to prove it acquired no charged work.
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	reader, readErr := state.OpenReader(readCtx, other)
	if readErr != nil {
		readCancel()
		t.Fatal(readErr)
	}
	budget, budgetErr := reader.DispatchBudget(readCtx, time.Now(), cfg.Scan.MaxScanChunksPerDay)
	feedback, feedbackErr := reader.CPUFeedback(readCtx)
	closeErr := reader.Close()
	readCancel()
	if budgetErr != nil || feedbackErr != nil || closeErr != nil || budget.Used != 0 || feedback.Window != nil {
		t.Fatal("foreign slot acquired charged work", budget, feedback, budgetErr, feedbackErr, closeErr)
	}
	control(t, other, "stop")
	waitExit(t, done)
}

func TestWorkerAsyncInventoryStatePostCompletionUsesOriginalChargedReceipt(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	cfg := config.Default()
	cfg.Roots = []string{root}
	cfg.Scan.PauseOnBattery = false
	cfg.Scan.MaxScanChunksPerDay = 1
	cfg.Scan.MetadataPerSecond = 100000
	o := asyncStateWorkerOptions(t)
	o.Interval = 5 * time.Second // Longer than original claim window: own cadence must not deadlock completion.
	entered, release := make(chan struct{}), make(chan struct{})
	var samples atomic.Int64
	o.inventoryStateObserve = func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		if samples.Add(1) == 2 {
			close(entered)
			<-release
		}
		return fixtureInventoryStateReport(limit, 1, time.Now()), ctx.Err()
	}
	var cancel context.CancelFunc
	var done <-chan error
	unblock := cleanupAsyncStateWorker(t, o.inventoryStateOwner, release, &cancel, &done)
	_, done, cancel = startCancelable(t, dir, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitAsyncInventoryEvent(t, entered)
	before := control(t, dir, "status")
	if samples.Load() != 2 || before.Dispatch.Used != 1 || before.ActiveJob != 0 || before.CPUFeedback.Window != nil || before.InventoryState.LastAttemptPhase != "after_receipt" {
		t.Fatal(before, samples.Load())
	}
	reserved := asyncSavedDispatchStart(t, dir)
	unblock()
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.InventoryMetrics != nil && s.ActiveJob == 0 && s.CPUFeedback.Window != nil && s.CPUFeedback.Status != "pending"
	})
	after := control(t, dir, "status")
	if samples.Load() != 2 || after.Dispatch.Used != 1 || after.Metadata.TotalCharges == nil || after.Metadata.TotalCharges.Reserved == 0 || after.CPUFeedback.Window.DispatchReservedAt.IsZero() {
		t.Fatal(after, samples.Load())
	}
	if !after.CPUFeedback.Window.DispatchReservedAt.Equal(reserved) {
		t.Fatal("receipt anchor changed", before.Dispatch, after.CPUFeedback.Window)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAsyncInventoryStatePauseAttemptsRevokeBeforeEveryWrite(t *testing.T) {
	for _, phase := range []string{"before_dispatch", "after_receipt"} {
		for _, returned := range []bool{false, true} {
			for _, result := range []string{"success", "definite_failure", "returned_write_error"} {
				t.Run(fmt.Sprintf("%s/returned=%t/%s", phase, returned, result), func(t *testing.T) {
					root := fairWorkerFiles(t, 1)
					dir := filepath.Join(t.TempDir(), "state")
					cfg := config.Default()
					cfg.Roots = []string{root}
					cfg.Scan.PauseOnBattery = false
					o := asyncStateWorkerOptions(t)
					entered, release := make(chan struct{}), make(chan struct{})
					var samples, cpuReads, sources, writes atomic.Int64
					o.cpuObserve = func() (time.Duration, error) { cpuReads.Add(1); return 0, nil }
					target := int64(1)
					if phase == "after_receipt" {
						target = 2
					}
					if returned {
						o.inventoryStateOwner.coordinator.afterCompletionCapture = func() {
							if samples.Load() == target {
								close(entered)
								<-release
							}
						}
					}
					o.inventoryStateObserve = func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
						n := samples.Add(1)
						if n == target && !returned {
							close(entered)
							<-release
						}
						return fixtureInventoryStateReport(limit, 1, time.Now()), ctx.Err()
					}
					noRetirementSource(t, &o, &sources)
					var beforeCPU int64
					o.pauseStateWrite = func(ctx context.Context, w *state.Store, paused bool) error {
						writes.Add(1)
						c := o.inventoryStateOwner.coordinator
						c.mu.Lock()
						ticket := c.ticket
						abandoned := ticket != nil && ticket.request.intent.abandoned
						c.mu.Unlock()
						if !abandoned || cpuReads.Load() != beforeCPU+1 {
							t.Error("write preceded revocation/prefix closure", abandoned, cpuReads.Load(), beforeCPU)
						}
						if result == "definite_failure" {
							return errors.New("generated pause write refusal")
						}
						if err := w.SetPaused(ctx, paused); err != nil {
							return err
						}
						if result == "returned_write_error" {
							return errors.New("generated post-write error")
						}
						return nil
					}
					var cancel context.CancelFunc
					var done <-chan error
					unblock := cleanupAsyncStateWorker(t, o.inventoryStateOwner, release, &cancel, &done)
					_, done, cancel = startCancelable(t, dir, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
					waitAsyncInventoryEvent(t, entered)
					beforeCPU = cpuReads.Load()
					// Each actual pause UPDATE must revoke even when it fails or
					// commits before returning an error. Redundant resume is separately
					// exercised by the retained blocked-pre control sequence.
					_, err := Send(context.Background(), dir, "pause")
					if (err == nil) != (result == "success") || writes.Load() != 1 {
						t.Fatal(err, writes.Load())
					}
					control(t, dir, "stop")
					waitExit(t, done)
					unblock()
					if ticket := o.inventoryStateOwner.coordinator.cachedStatus(nil).Ticket; ticket != nil {
						waitAsyncInventory(t, ticket)
					}
					r, err := state.OpenReader(context.Background(), dir)
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					budget, err := r.DispatchBudget(context.Background(), time.Now(), cfg.Scan.MaxScanChunksPerDay)
					want := 0
					if phase == "after_receipt" {
						want = 1
					}
					feedback, feedbackErr := r.CPUFeedback(context.Background())
					if err != nil || feedbackErr != nil || budget.Used != want || feedback.Window != nil || samples.Load() != target || sources.Load() != 0 {
						t.Fatal(budget, feedback, err, feedbackErr, samples.Load(), sources.Load())
					}
				})
			}
		}
	}
}

func TestWorkerAsyncInventoryStateBlockedSourceAllowsIndependentGeneric(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	cfg := config.Default()
	cfg.Roots = []string{root}
	cfg.Scan.PauseOnBattery = false
	o := asyncStateWorkerOptions(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var samples, sources, generic atomic.Int64
	o.inventoryStateObserve = func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		samples.Add(1)
		close(entered)
		<-release
		return fixtureInventoryStateReport(limit, 1, time.Now()), ctx.Err()
	}
	noRetirementSource(t, &o, &sources)
	o.Handlers = map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) { generic.Add(1); return Result{Done: true}, nil }}
	// Seed a future generic job; its real timer must stay live while SOURCE is suppressed.
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), cfg.Roots); err == nil {
		err = w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now().Add(300*time.Millisecond))
	}
	w.Close()
	if err != nil {
		t.Fatal(err)
	}
	var cancel context.CancelFunc
	var done <-chan error
	_ = cleanupAsyncStateWorker(t, o.inventoryStateOwner, release, &cancel, &done)
	_, done, cancel = startCancelable(t, dir, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitAsyncInventoryEvent(t, entered)
	waitUntil(t, func() bool { return generic.Load() == 1 })
	s := control(t, dir, "status")
	if samples.Load() != 1 || sources.Load() != 0 || s.InventoryState.Observation != nil || s.Dispatch.Used != 0 || s.CPUFeedback.Window != nil {
		t.Fatal(s, samples.Load(), sources.Load(), generic.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func asyncSavedDispatchStart(t *testing.T, dir string) time.Time {
	t.Helper()
	u := url.URL{Scheme: "file", Path: filepath.Join(dir, state.Filename), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var stamp int64
	if err = db.QueryRowContext(ctx, "SELECT last_start_ns FROM scan_dispatch WHERE id=1").Scan(&stamp); err != nil {
		t.Fatal(err)
	}
	return time.Unix(0, stamp).UTC()
}

// Explicit finite typed shape keeps all resource sections, long counters/IDs,
// fixed error/reason strings and maximum admitted root count. It is an encoded
// size bound, not a claim that all these historical states coexist coherently.
func TestWorkerAsyncInventoryStateTypedControlResponseFitsExistingCap(t *testing.T) {
	const input = `{"version":1,"ok":false,"error":"unsupported protocol or trailing request data","status":{"pid":9223372036854775807,"instance":"ffffffffffffffffffffffffffffffff","started_at":"2262-04-11T23:47:16.854775807Z","paused":false,"stopping":false,"active_job":9223372036854775807,"recovered_jobs":9223372036854775807,"handlers":9223372036854775807,"wait_reason":"inventory_state_source_backoff","inventory_metrics":{"entry_inspections":18446744073709551615,"entry_rate_per_second":9223372036854775807,"throttle_wait_ns":18446744073709551615,"throttled":false,"stat_calls":18446744073709551615,"directory_open_calls":18446744073709551615,"directory_read_calls":18446744073709551615,"filesystem_stat_calls":18446744073709551615,"mount_identity_calls":18446744073709551615,"path_resolution_calls":18446744073709551615},"dispatch":{"day_utc":"2262-04-11","reserved_chunks":9223372036854775807,"limit":9223372036854775807,"next_allowed_at":"2262-04-11T23:47:16.854775807Z","wait_reason":"daily_chunk_limit"},"cpu":{"contract":"worker_process_cpu_window_v1","source":"getrusage_self","target_percent":9223372036854775807,"status":"not_recorded","reason":"cpu_observation_unavailable","observed_at":"2262-04-11T23:47:16.854775807Z","window_cpu_ns":9223372036854775807,"window_elapsed_ns":9223372036854775807,"backoff_ns":9223372036854775807,"backoff_capped":false,"next_allowed_at":"2262-04-11T23:47:16.854775807Z","unknown_observations":18446744073709551615},"cpu_feedback":{"contract":"worker_cpu_feedback_v1","available":false,"status":"completed_unknown","scope":"experimental_inventory_dispatch_windows","target_percent":9223372036854775807,"backoff_limit_ns":9223372036854775807,"unknown_recovery_backoff_ns":9223372036854775807,"tracking_started_at":"2262-04-11T23:47:16.854775807Z","clock_high_water":"2262-04-11T23:47:16.854775807Z","last_begun_dispatch_at":"2262-04-11T23:47:16.854775807Z","window":{"token":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","instance":"ffffffffffffffffffffffffffffffff","kind":"maintenance","root_id":9223372036854775807,"job_id":9223372036854775807,"job_token":"ffffffffffffffffffffffffffffffff","job_lease_until":"2262-04-11T23:47:16.854775807Z","dispatch_reserved_at":"2262-04-11T23:47:16.854775807Z","window_started_at":"2262-04-11T23:47:16.854775807Z","recorded_at":"2262-04-11T23:47:16.854775807Z","status":"completed_unknown","settled_at":"2262-04-11T23:47:16.854775807Z","cpu_time_ns":9223372036854775807,"elapsed_ns":9223372036854775807,"reason":"cpu_observation_unavailable","backoff_ns":9223372036854775807,"backoff_capped":false},"next_allowed_at":"2262-04-11T23:47:16.854775807Z","completed_unknown_windows":9223372036854775807,"recovered_unknown_windows":9223372036854775807,"unknown_count_saturated":false,"pre_tracking_cpu":"unknown"},"power":{"contract":"worker_source_power_policy_v1","last_decision_status":"system_battery_discharging_observed","last_decision_reason":"power_observer_unavailable","last_decision_at":"2262-04-11T23:47:16.854775807Z","last_source_backoff":false,"last_wait_ns":9223372036854775807,"last_next_sample_wait_ns":9223372036854775807,"ticket_id":18446744073709551615,"sample_launch_at":"2262-04-11T23:47:16.854775807Z","sample_completed_at":"2262-04-11T23:47:16.854775807Z","callback_returned":false,"sample_status":"system_battery_discharging_observed","sample_reason":"confirmed_system_battery_discharge_witness","observation":{"contract":"power_observation_v1","platform":"darwin","profile":"linux_sysfs_power_v1","status":"system_battery_discharging_observed","reason":"confirmed_system_battery_discharge_witness","started_at":"2262-04-11T23:47:16.854775807Z","finished_at":"2262-04-11T23:47:16.854775807Z","system_battery_discharging_observed":false,"supply_entries_observed":9223372036854775807,"providers_processed":9223372036854775807,"confirmed_system_batteries":9223372036854775807,"discharging_system_batteries":9223372036854775807,"ambiguous_providers":9223372036854775807,"attribute_attempts":9223372036854775807,"returned_attribute_bytes":9223372036854775807,"coverage_complete":false,"sequential_observations":false,"namespace_authenticated":false,"physical_power_verified":false},"policy_enabled":false,"source_only":false,"persistent":false,"physical_power_verified":false},"inventory_state":{"contract":"worker_inventory_state_admission_v2","policy_enabled":false,"source_only":false,"persistent":false,"limit":9223372036854775807,"retry_interval_ns":9223372036854775807,"last_attempt_phase":"before_dispatch","last_attempt_started_at":"2262-04-11T23:47:16.854775807Z","last_attempt_finished_at":"2262-04-11T23:47:16.854775807Z","last_decision_status":"source_deferred","last_decision_reason":"inventory_state_process_slot","last_source_backoff":false,"retry_started_at":"2262-04-11T23:47:16.854775807Z","retry_after_at":"2262-04-11T23:47:16.854775807Z","observation":{"contract":"inventory_state_budget_v1","scope":"configured_inventory_database_and_wal","available":false,"status":"limit_reached","reason":"inventory_state_unavailable","limit":9223372036854775807,"database_bytes":9223372036854775807,"wal_bytes":9223372036854775807,"total_bytes":9223372036854775807,"sample_started_at":"2262-04-11T23:47:16.854775807Z","sample_finished_at":"2262-04-11T23:47:16.854775807Z","sequential_observations":false,"hard_limit_enforced":false,"physical_allocation_verified":false,"namespace_authenticated":false,"other_stores_included":false},"sampling_state":"not_requested","callback_returned":false,"process_slot_occupied":false},"metadata":{"contract":"worker_scanner_metadata_reservations_v1","available":false,"status":"unavailable","reason":"settlement_required","day_utc":"2262-04-11","daily_limit":9223372036854775807,"tracking_started_at":"2262-04-11T23:47:16.854775807Z","clock_high_water":"2262-04-11T23:47:16.854775807Z","pre_tracking_usage":"unknown","day_charges":{"reserved_attempts":9223372036854775807,"observed_attempts":9223372036854775807,"unknown_reserved_attempts":9223372036854775807,"outstanding_reserved_attempts":9223372036854775807,"known_unused_reserved_attempts":9223372036854775807},"total_charges":{"reserved_attempts":9223372036854775807,"observed_attempts":9223372036854775807,"unknown_reserved_attempts":9223372036854775807,"outstanding_reserved_attempts":9223372036854775807,"known_unused_reserved_attempts":9223372036854775807},"next_allowed_at":"2262-04-11T23:47:16.854775807Z"},"thread_priority":{"contract":"experimental_source_thread_priority_v1","scope":"experimental_source_handler_os_thread","platform":"darwin","checked_at":"2262-04-11T23:47:16.854775807Z","cpu":{"policy":"darwin_thread_background","target":9223372036854775807,"request_attempted":false,"request_accepted":false,"before":9223372036854775807,"observed":9223372036854775807,"reason":"request_accepted_and_setting_observed"},"io":{"policy":"darwin_thread_background","target":9223372036854775807,"request_attempted":false,"request_accepted":false,"before":9223372036854775807,"observed":9223372036854775807,"reason":"request_accepted_and_setting_observed"},"background":{"policy":"darwin_thread_background","target":9223372036854775807,"request_attempted":false,"request_accepted":false,"before":9223372036854775807,"observed":9223372036854775807,"reason":"request_accepted_and_setting_observed"},"thread_disposition":"locked_until_handler_goroutine_exit","new_threads_may_inherit":false,"whole_process_verified":false,"effective_scheduling_verified":false,"physical_io_verified":false,"physical_power_verified":false},"api_pacing":{"rate_per_second":100000,"capacity_ready":false,"wait_ns":18446744073709551615,"waiting":false},"adaptive_revisits":{"policy_enabled":false,"cached_at":"2262-04-11T23:47:16.854775807Z","contract":"adaptive_historical_metadata_v1","tracked_roots":32,"active_epochs":32,"changed_epochs":32,"unknown_epochs":32,"stable_roots":32,"daily_roots":32,"weekly_roots":32,"historical_metadata_only":false,"current_content_verified":false,"cleanup_approved":false},"inventory_mode":{"compact":false}}}`
	var response Response
	if err := json.Unmarshal([]byte(input), &response); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded)+1 > 8192 {
		t.Fatal("typed response exceeds existing control cap", len(encoded)+1, err)
	}
	if response.Status.InventoryState == nil || response.Status.APIPacing == nil || response.Status.AdaptiveRevisits == nil || response.Status.InventoryMode == nil {
		t.Fatal("missing bounded section")
	}
	t.Logf("typed all-fields control reply: %d bytes including newline", len(encoded)+1)
}

func TestWorkerAsyncInventoryStateBlockedPostTransfersOnlyOriginalReceiptToMaintenance(t *testing.T) {
	dir, historical, _ := workerRetirementFixture(t, 300)
	healthy := fairWorkerFiles(t, 1)
	cfg := config.Default()
	cfg.Roots = []string{historical, healthy}
	cfg.Scan.PauseOnBattery = false
	cfg.Scan.MaxScanChunksPerDay = 1
	prepareCtx, prepareCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer prepareCancel()
	w, err := state.OpenWriter(prepareCtx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(prepareCtx, cfg.Roots); err != nil {
		w.Close()
		t.Fatal(err)
	}
	initial, err := w.Summary(prepareCtx)
	if err != nil {
		w.Close()
		t.Fatal(err)
	}
	var prepared state.Summary
	entryPurge := false
	for i := 0; i < 32; i++ {
		step, e := w.RetireInventoryForRoot(prepareCtx, 1)
		if e != nil || !step.Eligible || !step.Worked || !step.Remaining {
			w.Close()
			t.Fatal("saved preparation lost bounded partial work", step, e)
		}
		prepared, err = w.Summary(prepareCtx)
		if err != nil {
			w.Close()
			t.Fatal(err)
		}
		if prepared.Entries == initial.Entries {
			continue
		}
		if initial.Entries-prepared.Entries != state.MaxBatchEntries || prepared.Entries <= state.MaxBatchEntries+4 {
			w.Close()
			t.Fatal(initial, prepared)
		}
		entryPurge = true
		break
	}
	if !entryPurge {
		w.Close()
		t.Fatal("saved preparation exceeded 32 turns")
	}
	sourceDue := time.Now()
	if err = w.EnqueueJob(prepareCtx, 2, state.ScanKind, []byte("."), sourceDue); err != nil {
		w.Close()
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	prepareCancel()
	o := asyncStateWorkerOptions(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var samples, sources atomic.Int64
	o.inventoryStateObserve = func(ctx context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		if samples.Add(1) == 2 {
			close(entered)
			<-release
		}
		return fixtureInventoryStateReport(limit, 1, time.Now()), ctx.Err()
	}
	// Establish the exact finite pre-done/post-entered sequence before the
	// already-eligible maintenance can revoke sample authority. The post callback
	// remains blocked after this private hook returns; production has no hook.
	o.inventoryStatePhaseStarted = func(phase string, ticket *inventoryStateTicket) error {
		if phase == "before_dispatch" {
			return returnedInventoryStatePrePhaseForFixture(phase, ticket)
		}
		if phase != "after_receipt" {
			return errors.New("unexpected inventory state phase")
		}
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-entered:
			return nil
		case <-timer.C:
			return errors.New("blocked post callback did not enter within five seconds")
		}
	}
	noRetirementSource(t, &o, &sources)
	var cancel context.CancelFunc
	var done <-chan error
	_ = cleanupAsyncStateWorker(t, o.inventoryStateOwner, release, &cancel, &done)
	_, done, cancel = startCancelable(t, dir, cfg, o, func(c context.CancelFunc, d <-chan error) { cancel, done = c, d })
	waitAsyncInventoryEvent(t, entered)
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return s.WaitReason == "daily_chunk_limit" && s.CPUFeedback != nil && s.CPUFeedback.Window != nil && s.CPUFeedback.Status != "pending"
	})
	live := control(t, dir, "status")
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	after, err := r.Summary(context.Background())
	if err != nil || prepared.Entries-after.Entries != state.MaxBatchEntries || after.Entries <= 4 || sources.Load() != 0 || samples.Load() != 2 || live.Dispatch == nil || live.Dispatch.Used != 1 || live.CPUFeedback.Window.Kind != state.FairInventoryMaintenance || !live.CPUFeedback.Window.DispatchReservedAt.Equal(asyncSavedDispatchStart(t, dir)) || !live.InventoryState.ProcessSlotOccupied {
		t.Fatal("blocked post lost original receipt or partial maintenance", prepared, after, live, err, sources.Load(), samples.Load())
	}
	if due, e := r.NextJobDue(context.Background(), []string{state.ScanKind}); e != nil || !due.Equal(sourceDue) {
		t.Fatal("maintenance changed pending SOURCE", due, e)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAsyncInventoryStatePreSessionReceiptPostOrdering(t *testing.T) {
	root := fairWorkerFiles(t, 1)
	dir := filepath.Join(t.TempDir(), "state")
	cfg := config.Default()
	cfg.Roots = []string{root}
	cfg.Scan.PauseOnBattery = false
	cfg.Scan.CPUSessionCharges = true
	cfg.Scan.MaxScanChunksPerDay = 1
	cfg.Scan.MetadataPerSecond = 100000
	o := asyncStateWorkerOptions(t)
	var samples, sessionReads, preReads, postReads, preBytes, postBytes, sourceCalls atomic.Int64
	o.cpuSessionObserve = func() (time.Duration, error) { sessionReads.Add(1); return 0, nil }
	o.inventoryStateObserve = func(ctx context.Context, sampler state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		n := samples.Add(1)
		report, err := sampler.Observe(ctx, limit)
		if err != nil {
			return report, err
		}
		if !report.Available || report.Status != "below_limit" || report.TotalBytes == nil {
			t.Error("generated metadata footprint was not available below limit", report)
			return report, nil
		}
		switch n {
		case 1:
			preReads.Store(sessionReads.Load())
			preBytes.Store(*report.TotalBytes)
		case 2:
			postReads.Store(sessionReads.Load())
			postBytes.Store(*report.TotalBytes)
			if postBytes.Load() <= preBytes.Load() {
				t.Error("prerequisite session/receipt writes did not grow the genuine logical DB+WAL footprint", preBytes.Load(), postBytes.Load())
			}
			if postReads.Load() != preReads.Load()+1 {
				t.Error("selected session was not published exactly once between pre and post", preReads.Load(), postReads.Load())
			}
			if asyncSavedDispatchStart(t, dir).IsZero() {
				t.Error("post sample lacked committed receipt")
			}
		default:
			t.Error("unexpected extra state sample")
		}
		return report, ctx.Err()
	}
	o.scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
		sourceCalls.Add(1)
		if samples.Load() != 2 || sessionReads.Load() != postReads.Load() {
			t.Error("source admission took another session/state observation", samples.Load(), sessionReads.Load(), postReads.Load())
		}
		return scanner.NextPermitted(ctx, job, permit)
	}
	_, done := start(t, dir, cfg, o)
	waitUntil(t, func() bool {
		s := control(t, dir, "status")
		return sourceCalls.Load() == 1 && s.ActiveJob == 0 && s.CPUFeedback != nil && s.CPUFeedback.Window != nil && s.CPUFeedback.Status != "pending"
	})
	live := control(t, dir, "status")
	if samples.Load() != 2 || live.Dispatch == nil || live.Dispatch.Used != 1 || postReads.Load() != preReads.Load()+1 || postBytes.Load() <= preBytes.Load() {
		t.Fatal(live, samples.Load(), preReads.Load(), postReads.Load(), preBytes.Load(), postBytes.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestWorkerAsyncInventoryStateBareHooksCannotReplaceProductionDomain(t *testing.T) {
	for _, kind := range []string{"wall", "elapsed", "observer"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "not-created")
			cfg := config.Default()
			o := Options{ExperimentalScan: true}
			var callbacks atomic.Int64
			switch kind {
			case "wall":
				o.wallNow = time.Now
			case "elapsed":
				o.elapsedNow = time.Now
			case "observer":
				o.inventoryStateObserve = func(context.Context, state.InventoryStateBudgetSampler, int64) (state.InventoryStateBudget, error) {
					callbacks.Add(1)
					return state.InventoryStateBudget{}, nil
				}
			}
			if err := Run(context.Background(), dir, cfg, o); !errors.Is(err, errInventoryStateAsyncInput) {
				t.Fatal("bare hook replaced process lifetime", err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) || callbacks.Load() != 0 {
				t.Fatal("refusal entered state/observer", err, callbacks.Load())
			}
		})
	}
}
