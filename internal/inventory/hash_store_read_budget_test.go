package inventory

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func dailyHashLimits(cap int64) HashReadExecutionLimits {
	return HashReadExecutionLimits{RequestedBytesPerSecond: 1 << 30, DailyReservedByteLimit: cap}
}
func dailyOriginalHooks(f *hashStoreTestFixture, cap int64) hashStoreHooks {
	c := newHashPacingClock()
	c.wall = f.store.now()
	f.store.now = func() time.Time { return c.wall }
	l := dailyHashLimits(cap)
	return hashStoreHooks{pacing: c.pacer(l.RequestedBytesPerSecond), execution: &l}
}
func requireStoreReadBudget(t *testing.T, s *HashStore) *HashStoreReadBudget {
	t.Helper()
	b, e := s.StoreReadBudget(context.Background())
	if e != nil || b == nil {
		t.Fatal("shared saved accounting unavailable", b, e)
	}
	if b.Contract != HashStoreReadBudgetContract || !hashStoreDigest(b.StoreID) || b.CurrentReadPermissionEvaluated || b.CurrentConfiguredLimitEvaluated || b.PhysicalIOVerified {
		t.Fatal("shared accounting claimed current permission/physical I/O", b)
	}
	return b
}
func TestHashStoreReadBudgetOriginalQuantumTailEmptyAndChangingCap(t *testing.T) {
	ctx := context.Background()
	f, req := hashReadFixture(t, fullHashContents(65))
	c := hashReadApprove(t, f, req)
	before := hashStoreSnapshot(t, f.store)
	r, e := f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, 64))
	if e != nil || r.ReservedBytes != 64 || r.DurableOffset != 64 || r.StoreReadBudget == nil || r.ConfiguredDailyReservedByteLimit == nil || *r.ConfiguredDailyReservedByteLimit != 64 {
		t.Fatal(r, e)
	}
	b := requireStoreReadBudget(t, f.store)
	if b.ReservedBytes != 64 || b.TotalReservedBytes != 64 || b.TotalReadBytes != 64 || b.TotalUnknownReservedBytes != 0 || b.TotalOutstandingReservedBytes != 0 {
		t.Fatal(b)
	}
	for _, cap := range []int64{1, 63, 64} {
		r, e = f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, cap))
		if !errors.Is(e, ErrHashDeferred) || r.Code != "configured_daily_byte_limit" || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 {
			t.Fatal("lowered cap refunded or admitted", r, e)
		}
	}
	r, e = f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, 65))
	if e != nil || r.ReservedBytes != 1 || r.DurableOffset != 65 {
		t.Fatal(r, e)
	}
	requireFullHashDigest(t, r.Progress, fullHashContents(65))
	after := hashStoreSnapshot(t, f.store)
	if !reflect.DeepEqual(before.ReadConsent.Approval, after.ReadConsent.Approval) || after.SelectionID != before.SelectionID {
		t.Fatal("new restriction changed immutable consent/scope")
	}
	if b = requireStoreReadBudget(t, f.store); b.TotalReservedBytes != 65 || b.TotalReadBytes != 65 {
		t.Fatal(b)
	}
	if _, e = f.store.RunConsented(ctx, c.ID, f.source, f.scanner); !errors.Is(e, ErrHashStoreReadBudgetRequired) {
		t.Fatal("active shared ledger bypassed by old API", e)
	}
	if _, e = f.store.RunConsentedPaced(ctx, c.ID, f.source, f.scanner, 1<<20); !errors.Is(e, ErrHashStoreReadBudgetRequired) {
		t.Fatal("paced old API bypassed ledger", e)
	}
	if _, e = f.store.RunNext(ctx, f.source, f.scanner, 64, 128); !errors.Is(e, ErrHashStoreReadBudgetRequired) {
		t.Fatal("fixture-only API bypassed ledger", e)
	}
	// Empty full-hash bodies are supported by the session primitive; the
	// existing saved-selection API requires positive-size candidates. A zero
	// grant must still preserve the exact shared counters without a body call.
	scanner, targets := sampleFixture(t, []byte{})
	session := fullHashSession(t, scanner, targets[0])
	p := newHashPacingClock().pacer(1)
	progress, usage, e := session.step(ctx, 1, fileHashHooks{pacing: p})
	if e != nil || usage.RequestedBytes != 0 || usage.ReadBytes != 0 {
		t.Fatal(progress, usage, e)
	}
	tx, e := f.store.db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = chargeHashStoreReadBudget(ctx, tx, b, 0); e != nil {
		tx.Rollback()
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	if got := requireStoreReadBudget(t, f.store); !reflect.DeepEqual(got, b) {
		t.Fatal("zero charge changed accounting", got, b)
	}

}

func TestHashStoreReadBudgetInvalidCanceledAndLowRateInitialization(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(128))
	c := hashReadApprove(t, f, req)
	ctx := context.Background()
	for _, l := range []HashReadExecutionLimits{{0, 64}, {1<<30 + 1, 64}, {64, 0}, {64, 1<<50 + 1}} {
		if _, e := f.store.RunConsentedBudgeted(ctx, c.ID, f.source, f.scanner, l); !errors.Is(e, ErrHashReadExecutionLimits) {
			t.Fatal(e)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := f.store.RunConsentedBudgeted(canceled, c.ID, f.source, f.scanner, dailyHashLimits(64)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := f.store.RunConsentedBudgeted(ctx, strings.Repeat("0", 64), f.source, f.scanner, dailyHashLimits(64)); !errors.Is(e, ErrHashReadApprovalMissing) {
		t.Fatal(e)
	}
	if b, e := f.store.StoreReadBudget(ctx); e != nil || b != nil || hashChoiceSchemaVersion(t, f.store.db) != 2 {
		t.Fatal("invalid/canceled invocation initialized tracking", b, e)
	}
	r, e := f.store.RunConsentedBudgeted(ctx, c.ID, f.source, f.scanner, HashReadExecutionLimits{1, 64})
	if !errors.Is(e, ErrHashReadPacingCapacity) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 {
		t.Fatal(r, e)
	}
	b := requireStoreReadBudget(t, f.store)
	if b.TotalReservedBytes != 0 || hashChoiceSchemaVersion(t, f.store.db) != 7 {
		t.Fatal("definite capacity refusal lost zero-charge tracking", b)
	}
}

func TestHashStoreReadBudgetFreshJobsShareLegacyChargesAndExactRecovery(t *testing.T) {
	ctx := context.Background()
	f := freshRunFiles(t, 65, 2, "2", "1")
	meta := f.fresh.m.f.store
	second := saveFreshJobFixture(t, meta, f.fresh.request, hashChoiceJobKey(2))
	a := f.fresh.approval
	a.JobID, a.JobKey = second.ID, second.Record.JobKey
	secondConsent, e := meta.ApproveFreshRead(ctx, a)
	if e != nil {
		t.Fatal(e)
	}
	w := f.open(t)
	// A genuine pre-v7 interrupted reservation in job1 must be included even
	// when opening only job2; exact-job recovery must not silently settle job1.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("reservation seam not reached")
			}
		}()
		_, _ = w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{afterReserve: func() { panic("generated process-loss seam") }})
	}()
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenHashFreshRunWriter(ctx, f.fresh.request, second.ID)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	w.now = func() time.Time { return secondConsent.Approval.CreatedAt }
	r, e := w.runFreshConsented(ctx, secondConsent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 259))
	if e != nil || r.ReservedBytes != 64 || r.DurableOffset != 64 {
		t.Fatal(r, e)
	}
	b := requireStoreReadBudget(t, w)
	if b.TotalReservedBytes != 259 || b.TotalReadBytes != 194 || b.TotalOutstandingReservedBytes != 65 || b.TotalUnknownReservedBytes != 0 {
		t.Fatal("legacy/outstanding charges omitted", b)
	}
	first, e := w.FreshJob(ctx, f.fresh.job.ID)
	if e != nil || first.Progress[0].Status != "running" {
		t.Fatal("unrelated recovery broadened scope", first, e)
	}
	r, e = w.runFreshConsented(ctx, secondConsent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 259))
	if !errors.Is(e, ErrHashDeferred) || r.ReservedBytes != 0 || r.Code != "configured_daily_byte_limit" {
		t.Fatal(r, e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenHashFreshRunWriter(ctx, f.fresh.request, f.fresh.job.ID)
	if e != nil {
		t.Fatal(e)
	}
	w.now = func() time.Time { return secondConsent.Approval.CreatedAt }
	b = requireStoreReadBudget(t, w)
	if b.TotalReservedBytes != 259 || b.TotalOutstandingReservedBytes != 0 || b.TotalUnknownReservedBytes != 65 {
		t.Fatal("recovery recharged/refunded unknown usage", b)
	}
	if w.schemaVersion != 7 {
		t.Fatal("fresh recovery downgraded schema cache", w.schemaVersion)
	}
	if _, e = w.RunFreshConsentedPaced(ctx, f.consent.ID, f.fresh.m.f.scanner, 1<<20); !errors.Is(e, ErrHashStoreReadBudgetRequired) {
		t.Fatal("fresh legacy bypass", e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	reader, e := OpenHashReader(ctx, f.fresh.m.f.base)
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	reader.now = func() time.Time { t.Fatal("saved reader sampled current clock"); return time.Time{} }
	if got := requireStoreReadBudget(t, reader); !reflect.DeepEqual(got, b) {
		t.Fatal("saved reopen changed accounting", got, b)
	}
}

func TestHashStoreReadBudgetSavedProjectionAndCrossJobClock(t *testing.T) {
	ctx := context.Background()
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	r, e := w.RunFreshConsentedBudgeted(ctx, f.consent.ID, f.fresh.m.f.scanner, dailyHashLimits(130))
	if !errors.Is(e, ErrHashDeferred) || r.ReservedBytes != 0 {
		t.Fatal(r, e)
	}
	original := requireStoreReadBudget(t, w)
	higher := w.now().Add(time.Hour)
	// Offline lifecycle metadata can be newer than the shared admission clock.
	w.now = func() time.Time { return higher }
	second := saveFreshJobFixture(t, w, f.fresh.request, hashChoiceJobKey(2))
	req := f.fresh.approval
	req.JobID, req.JobKey = second.ID, second.Record.JobKey
	if _, e = w.ApproveFreshRead(ctx, req); e != nil {
		t.Fatal(e)
	}
	projected := requireStoreReadBudget(t, w)
	if projected.MaxNow != original.MaxNow || projected.ProjectedMaxNow != higher || projected.TotalReservedBytes != 130 {
		t.Fatal("metadata observation mislabeled shared/current time", projected, original)
	}
	w.now = func() time.Time { return higher.Add(-time.Second) }
	r, e = w.RunFreshConsentedBudgeted(ctx, f.consent.ID, f.fresh.m.f.scanner, dailyHashLimits(4096))
	if !errors.Is(e, ErrHashReadClockRollback) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 {
		t.Fatal("other job bypassed rollback", r, e)
	}
	final := requireStoreReadBudget(t, w)
	if final.MaxNow != higher || final.TotalReservedBytes != 130 {
		t.Fatal("rollback forgot projected high", final)
	}
}

func TestHashStoreReadBudgetUTCForwardAndPermanentPeak(t *testing.T) {
	ctx := context.Background()
	f, req := hashReadFixture(t, fullHashContents(128))
	c := hashReadApprove(t, f, req)
	// Start close enough to midnight while consent still spans the next day.
	midnight := c.Approval.CreatedAt.Truncate(24 * time.Hour).Add(24 * time.Hour)
	f.store.now = func() time.Time { return midnight.Add(-time.Hour) }
	r, e := f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, 64))
	if e != nil || r.ReservedBytes != 64 {
		t.Fatal(r, e)
	}
	f.store.now = func() time.Time { return midnight.Add(time.Second) }
	r, e = f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, 64))
	if e != nil || r.ReservedBytes != 64 {
		t.Fatal(r, e)
	}
	b := requireStoreReadBudget(t, f.store)
	if b.ReservedBytes != 64 || b.TotalReservedBytes != 128 || b.Day != midnight.Format(time.DateOnly) {
		t.Fatal("rollover refunded lifetime or reused prior-day charge", b)
	}
	f.store.now = func() time.Time { return midnight }
	r, e = f.store.RunConsentedBudgeted(ctx, c.ID, f.source, f.scanner, dailyHashLimits(4096))
	if !errors.Is(e, ErrHashReadClockRollback) || r.ReservedBytes != 0 {
		t.Fatal(r, e)
	}
}

func TestHashStoreReadBudgetConcurrentCallsAndReaderPurity(t *testing.T) {
	ctx := context.Background()
	f, req := hashReadFixture(t, fullHashContents(128), fullHashContents(128))
	c := hashReadApprove(t, f, req)
	base, start := f.store.now(), time.Now()
	f.store.now = func() time.Time { return base.Add(time.Since(start)) }
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := f.store.RunConsentedBudgeted(ctx, c.ID, f.source, f.scanner, dailyHashLimits(64))
			if e != nil && !errors.Is(e, ErrHashDeferred) {
				errs <- e
			}
			if r.ReservedBytes > 64 {
				errs <- errors.New("concurrent grant exceeded cap")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	b := requireStoreReadBudget(t, f.store)
	if b.TotalReservedBytes != 64 || b.ReservedBytes != 64 {
		t.Fatal("concurrent serialized calls overspent", b)
	}
	reader, e := OpenHashReader(ctx, f.base)
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	reader.now = func() time.Time { t.Fatal("saved projection sampled time"); return time.Time{} }
	before := hashStoreSnapshot(t, f.store)
	for range 3 {
		if got := requireStoreReadBudget(t, reader); !reflect.DeepEqual(got, b) {
			t.Fatal(got, b)
		}
	}
	if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("saved report mutated ledger")
	}
	if e = f.store.Close(); e != nil {
		t.Fatal(e)
	}
	// Leave generated original scope offline: saved-only reader must not reopen.
	if e = os.Rename(f.root, f.root+"-offline"); e != nil {
		t.Fatal(e)
	}
	defer os.Rename(f.root+"-offline", f.root)
	if got := requireStoreReadBudget(t, reader); !reflect.DeepEqual(got, b) {
		t.Fatal(got, b)
	}
}

func TestHashStoreReadBudgetRefusesBoundedCorruption(t *testing.T) {
	for _, kind := range []string{"shared_total", "shared_blob", "missing", "orphan_original", "original_clock", "overflow"} {
		t.Run(kind, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(128))
			c := hashReadApprove(t, f, req)
			if _, e := f.store.runConsented(context.Background(), c.ID, f.source, f.scanner, dailyOriginalHooks(f, 64)); e != nil {
				t.Fatal(e)
			}
			if _, e := f.store.db.Exec("DROP TRIGGER hash_store_read_budget_monotonic;PRAGMA ignore_check_constraints=ON"); e != nil {
				t.Fatal(e)
			}
			var e error
			switch kind {
			case "shared_total":
				_, e = f.store.db.Exec("UPDATE hash_store_read_budget SET total_reserved_bytes=65")
			case "shared_blob":
				_, e = f.store.db.Exec("UPDATE hash_store_read_budget SET reserved_bytes=?", strings.Repeat("x", 2<<20))
			case "missing":
				_, e = f.store.db.Exec("DROP TRIGGER hash_store_read_budget_no_delete;DELETE FROM hash_store_read_budget")
			case "orphan_original":
				_, e = f.store.db.Exec("PRAGMA foreign_keys=OFF;INSERT INTO hash_attempt VALUES(20,?,0,0,64,'2026-01-02','reserved',NULL,NULL,NULL)", strings.Repeat("0", 64))
			case "original_clock":
				stamp := time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
				_, e = f.store.db.Exec("UPDATE hash_budget SET day=?,max_now_ns=?", stamp.Format(time.DateOnly), stamp.UnixNano())
			case "overflow":
				_, e = f.store.db.Exec("UPDATE hash_budget SET total_reserved_bytes=?,total_requested_bytes=?,total_read_bytes=?", math.MaxInt64, int64(64), int64(64))
			}
			if e != nil {
				t.Fatal("fixture corruption could not be applied", e)
			}
			if b, e := f.store.StoreReadBudget(context.Background()); !errors.Is(e, ErrHashStoreReadBudgetCorrupt) || b != nil {
				t.Fatal("corrupt shared report accepted", b, e)
			}
			if e = f.store.Close(); e != nil {
				t.Fatal(e)
			}
			if reader, e := OpenHashReader(context.Background(), f.base); !errors.Is(e, ErrHashStoreReadBudgetCorrupt) {
				if reader != nil {
					reader.Close()
				}
				t.Fatal("reopen admitted corrupt shared ledger", e)
			}
		})
	}
}

func TestHashStoreReadBudgetAtomicInitializationAndCommitUncertainty(t *testing.T) {
	for _, stage := range []string{"cancel_before", "lost_reply"} {
		t.Run(stage, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(128))
			c := hashReadApprove(t, f, req)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := dailyOriginalHooks(f, 64)
			h.storeBudgetHooks = &hashStoreReadBudgetHooks{}
			if stage == "cancel_before" {
				h.storeBudgetHooks.beforeCommit = cancel
			} else {
				h.storeBudgetHooks.commit = func(tx *sql.Tx) error {
					if e := tx.Commit(); e != nil {
						return e
					}
					return errors.New("generated lost commit reply")
				}
			}
			r, e := f.store.runConsented(ctx, c.ID, f.source, f.scanner, h)
			if stage == "cancel_before" {
				if !errors.Is(e, context.Canceled) || hashChoiceSchemaVersion(t, f.store.db) != 2 {
					t.Fatal("canceled initialization published", r, e)
				}
			} else {
				if !errors.Is(e, ErrHashRecoveryRequired) || r.ApprovalID != c.ID || !f.store.poisoned {
					t.Fatal("uncertain init hid exact approval", r, e)
				}
				if _, e = f.store.RunConsentedBudgeted(context.Background(), c.ID, f.source, f.scanner, dailyHashLimits(64)); !errors.Is(e, ErrHashRecoveryRequired) {
					t.Fatal("poisoned writer retried", e)
				}
				if e = f.store.Close(); e != nil {
					t.Fatal(e)
				}
				reader, e := OpenHashReader(context.Background(), f.base)
				if e != nil {
					t.Fatal(e)
				}
				defer reader.Close()
				if b := requireStoreReadBudget(t, reader); b.TotalReservedBytes != 0 {
					t.Fatal("uncertain init invented source charge", b)
				}
			}
		})
	}
}

func dailyFreshHooks(s *HashStore, cap int64) hashFreshRunHooks {
	c := newHashPacingClock()
	c.wall = s.now()
	s.now = func() time.Time { return c.wall }
	l := dailyHashLimits(cap)
	return hashFreshRunHooks{pacing: c.pacer(l.RequestedBytesPerSecond), execution: &l}
}
func TestHashStoreReadBudgetLegacyOriginalAggregateUnderchargeRefusesBeforeMigration(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(64), fullHashContents(64))
	c := hashReadApprove(t, f, req)
	for range 2 {
		if _, e := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := f.store.db.Exec("UPDATE hash_budget SET reserved_bytes=64,requested_bytes=64,read_bytes=64,total_reserved_bytes=64,total_requested_bytes=64,total_read_bytes=64"); e != nil {
		t.Fatal(e)
	}
	// Each latest head/grant remains individually <=64. The aggregate of two
	// independent settled heads makes this reduced lifetime ledger impossible.
	if _, e := f.store.Snapshot(context.Background()); e != nil {
		t.Fatal("legacy per-row reader unexpectedly caught sum", e)
	}
	r, e := f.store.RunConsentedBudgeted(context.Background(), c.ID, f.source, f.scanner, dailyHashLimits(128))
	if !errors.Is(e, ErrHashStoreReadBudgetCorrupt) || r.ReservedBytes != 0 || hashChoiceSchemaVersion(t, f.store.db) != 2 {
		t.Fatal("undercounted legacy migration admitted", r, e)
	}
}
func TestHashStoreReadBudgetWaitCancellationRetainsChargeAndHighestClock(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(1024))
	consent := hashReadApprove(t, f, req)
	c := newHashPacingClock()
	c.wall = f.store.now()
	base := c.wall
	f.store.now = func() time.Time { return c.wall }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.onWait = func(context.Context, time.Duration) error { c.advance(time.Second); cancel(); return context.Canceled }
	l := dailyHashLimits(1024)
	h := hashStoreHooks{pacing: c.pacer(1024), execution: &l}
	r, e := f.store.runConsented(ctx, consent.ID, f.source, f.scanner, h)
	if !errors.Is(e, context.Canceled) || r.ReservedBytes != 1024 || r.Usage.RequestedBytes != 0 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
		t.Fatal(r, e)
	}
	b := requireStoreReadBudget(t, f.store)
	if b.MaxNow != base.Add(time.Second) || b.TotalReservedBytes != 1024 || b.TotalReadBytes != 0 || b.TotalOutstandingReservedBytes != 0 || b.TotalUnknownReservedBytes != 0 {
		t.Fatal("cancel lost permanent grant/high-water", b)
	}
}
func TestHashStoreReadBudgetFreshKnownCommitCancelKeepsVersionAndIdentity(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	h := dailyFreshHooks(w, 130)
	r, e := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, h)
	if !errors.Is(e, ErrHashDeferred) || r.ReservedBytes != 0 {
		t.Fatal(r, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	job, e := w.FreshJob(context.Background(), f.fresh.job.ID)
	if e != nil {
		t.Fatal(e)
	}
	tx, e := w.db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	commit := func(tx *sql.Tx) error {
		if e := tx.Commit(); e != nil {
			return e
		}
		cancel()
		return nil
	}
	e = w.commitFreshProgress(ctx, tx, job, nil, nil, commit, "generated known commit cancellation")
	if !errors.Is(e, context.Canceled) || errors.Is(e, ErrHashRecoveryRequired) || w.poisoned || w.schemaVersion != 7 {
		t.Fatal("known publication became uncertain/downgraded", e, w.poisoned, w.schemaVersion)
	}
}

func TestHashStoreReadBudgetOriginalThenFreshUsesOneConfiguredCeiling(t *testing.T) {
	ctx := context.Background()
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	// The exact saved choice covers three completed heads; the fourth original
	// file remains pending. Derive the private manual fixture directory exactly.
	if e := m.f.source.Close(); e != nil {
		t.Fatal(e)
	}
	if e := localfs.EnsurePrivateDir(filepath.Join(m.f.base, "manual")); e != nil {
		t.Fatal(e)
	}
	derived := filepath.Join(m.f.base, "manual", m.proposal.SourceLocator.InventoryKey)
	if e := os.Rename(m.f.stateDir, derived); e != nil {
		t.Fatal(e)
	}
	m.f.stateDir = derived
	var e error
	m.f.source, e = state.OpenWriter(ctx, derived)
	if e != nil {
		t.Fatal(e)
	}
	request := hashChoiceJobRequest(t, m)
	w := hashChoiceJobWriter(t, m, request)
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	req := HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192}
	freshConsent, e := w.ApproveFreshRead(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenExistingHashWriter(ctx, m.f.base)
	if e != nil {
		t.Fatal(e)
	}
	m.f.store = w
	w.now = func() time.Time { return freshConsent.Approval.CreatedAt }
	original, e := w.runConsented(ctx, m.consent.ID, m.f.source, m.f.scanner, dailyOriginalHooks(m.f, 260))
	if e != nil || original.WorkID != "4" || original.ReservedBytes != 65 || original.Progress.Status != "hash_observed" {
		t.Fatal(original, e)
	}
	b := requireStoreReadBudget(t, w)
	if b.TotalReservedBytes != 260 || b.TotalReadBytes != 260 {
		t.Fatal(b)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenHashFreshRunWriter(ctx, request, job.ID)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	w.now = func() time.Time { return b.MaxNow }
	fresh, e := w.runFreshConsented(ctx, freshConsent.ID, m.f.scanner, dailyFreshHooks(w, 324))
	if e != nil || fresh.Ordinal != 1 || fresh.HistoricalWorkID != "3" || fresh.ReservedBytes != 64 || fresh.DurableOffset != 64 {
		t.Fatal(fresh, e)
	}
	b = requireStoreReadBudget(t, w)
	if b.TotalReservedBytes != 324 || b.TotalReadBytes != 324 {
		t.Fatal("original/fresh limits were independent", b)
	}
	immutable, e := w.FreshJob(ctx, job.ID)
	if e != nil || !reflect.DeepEqual(immutable.Record, job.Record) || !reflect.DeepEqual(immutable.ReadConsent.Approval, freshConsent.Approval) {
		t.Fatal("configured cap rewrote exact fresh scope/consent", immutable, e)
	}
}

func TestHashStoreReadBudgetNamespaceBoundsMissingAndOrphanRefuse(t *testing.T) {
	for _, kind := range []string{"missing_budget", "orphan_budget", "extra_shared", "budget_type", "observation_type"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			if _, e := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 130)); !errors.Is(e, ErrHashDeferred) {
				t.Fatal(e)
			}
			if _, e := w.db.Exec("PRAGMA ignore_check_constraints=ON;PRAGMA foreign_keys=OFF"); e != nil {
				t.Fatal(e)
			}
			var e error
			switch kind {
			case "missing_budget":
				_, e = w.db.Exec("DROP TRIGGER hash_fresh_budget_no_delete;DELETE FROM hash_fresh_budget WHERE job_id=?", f.fresh.job.ID)
			case "orphan_budget":
				_, e = w.db.Exec("INSERT INTO hash_fresh_budget SELECT ?,day,max_now_ns,reserved_bytes,requested_bytes,read_bytes,unknown_reserved_bytes,total_reserved_bytes,total_requested_bytes,total_read_bytes,total_unknown_reserved_bytes FROM hash_fresh_budget WHERE job_id=?", "hash-choice-job-v1-"+strings.Repeat("0", 64), f.fresh.job.ID)
			case "extra_shared":
				_, e = w.db.Exec("INSERT INTO hash_store_read_budget SELECT 2,store_id,day,max_now_ns,reserved_bytes,total_reserved_bytes FROM hash_store_read_budget")
			case "budget_type":
				_, e = w.db.Exec("DROP TRIGGER hash_fresh_budget_monotonic;UPDATE hash_fresh_budget SET reserved_bytes=?", strings.Repeat("q", 2<<20))
			case "observation_type":
				_, e = w.db.Exec("DROP TRIGGER hash_fresh_read_observation_monotonic;UPDATE hash_fresh_read_observation SET max_now_ns=?", strings.Repeat("q", 2<<20))
			}
			if e != nil {
				t.Fatal("bounded corruption fixture failed", e)
			}
			if b, e := w.StoreReadBudget(context.Background()); !errors.Is(e, ErrHashStoreReadBudgetCorrupt) || b != nil {
				t.Fatal("corrupt namespace reported shared accounting", b, e)
			}
		})
	}
}

func TestHashStoreReadBudgetLegacyMixedDaysAndUninitializedConsentMigration(t *testing.T) {
	ctx := context.Background()
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	nextDay := f.consent.Approval.CreatedAt.Truncate(24 * time.Hour).Add(24 * time.Hour)
	w.now = func() time.Time { return nextDay.Add(20 * time.Second) }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("legacy reservation seam not reached")
			}
		}()
		_, _ = w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{afterReserve: func() { panic("generated legacy outstanding charge") }})
	}()
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e := OpenHashFreshJobWriter(ctx, f.fresh.request)
	if e != nil {
		t.Fatal(e)
	}
	w.now = func() time.Time { return nextDay.Add(time.Minute) }
	later := saveFreshJobFixture(t, w, f.fresh.request, hashChoiceJobKey(2))
	req := f.fresh.approval
	req.JobID, req.JobKey = later.ID, later.Record.JobKey
	laterConsent, e := w.ApproveFreshRead(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	later, e = w.FreshJob(ctx, later.ID)
	if e != nil || later.FreshBudget != nil || len(later.Progress) != 0 {
		t.Fatal("fixture later consent accidentally initialized fresh budget", later, e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenExistingHashWriter(ctx, f.fresh.m.f.base)
	if e != nil {
		t.Fatal(e)
	}
	clock := laterConsent.ClockHighWater
	w.now = func() time.Time { return clock }
	original, e := w.RunConsentedBudgeted(ctx, f.fresh.m.consent.ID, f.fresh.m.f.source, f.fresh.m.f.scanner, dailyHashLimits(32))
	if e != nil || original.Status != "idle" || original.ReservedBytes != 0 {
		t.Fatal(original, e)
	}
	seeded := requireStoreReadBudget(t, w)
	if seeded.Day != nextDay.Format(time.DateOnly) || seeded.ReservedBytes != 65 || seeded.TotalReservedBytes != 195 || seeded.TotalReadBytes != 130 || seeded.TotalOutstandingReservedBytes != 65 || seeded.OutstandingReservedBytes != 65 || seeded.TotalUnknownReservedBytes != 0 || seeded.MaxNow != clock {
		t.Fatal("migration reused older-day charges or omitted lifetime/outstanding/new consent clock", seeded)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	w, e = OpenHashFreshRunWriter(ctx, f.fresh.request, later.ID)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	w.now = func() time.Time { return clock }
	r, e := w.runFreshConsented(ctx, laterConsent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 129))
	if e != nil || r.ReservedBytes != 64 || r.DurableOffset != 64 {
		t.Fatal(r, e)
	}
	r, e = w.runFreshConsented(ctx, laterConsent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 32))
	if !errors.Is(e, ErrHashDeferred) || r.Code != "configured_daily_byte_limit" || r.ReservedBytes != 0 {
		t.Fatal("lowered cap became corruption/refund", r, e)
	}
	b := requireStoreReadBudget(t, w)
	if b.ReservedBytes != 129 || b.TotalReservedBytes != 259 || b.TotalOutstandingReservedBytes != 65 || b.OutstandingReservedBytes != 65 {
		t.Fatal(b)
	}
	w.now = func() time.Time { return clock.Add(-time.Second) }
	r, e = w.RunFreshConsentedBudgeted(ctx, laterConsent.ID, f.fresh.m.f.scanner, dailyHashLimits(4096))
	if !errors.Is(e, ErrHashReadClockRollback) || r.ReservedBytes != 0 {
		t.Fatal("cross-job newer day clock lost", r, e)
	}
}

func TestHashStoreReadBudgetReadAdmissionDayExpiryAndClockRefusalsRetainPeak(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		for _, cause := range []string{"day", "expiry", "rollback"} {
			t.Run(map[bool]string{false: "original", true: "fresh"}[fresh]+"/"+cause, func(t *testing.T) {
				c := newHashPacingClock()
				var s *HashStore
				var original *hashStoreTestFixture
				var ff *freshRunFixture
				var approval string
				var cap, prior int64
				if fresh {
					ff = freshRunFiles(t, 1024, 2, "2", "1")
					s = ff.open(t)
					approval = ff.consent.ID
					prior = 2048
					cap = prior + 64
					c.wall = ff.consent.Approval.CreatedAt
				} else {
					var req HashReadApprovalRequest
					original, req = hashReadFixture(t, fullHashContents(1024))
					consent := hashReadApprove(t, original, req)
					s = original.store
					approval = consent.ID
					cap = 64
					c.wall = consent.Approval.CreatedAt
				}
				dayEnd := c.wall.Truncate(24 * time.Hour).Add(24 * time.Hour)
				expiry := c.wall.Add(24 * time.Hour)
				if cause == "day" {
					c.wall = dayEnd.Add(-100 * time.Millisecond)
				}
				s.now = func() time.Time { return c.wall }
				peak := c.wall
				var p *hashReadPacer
				c.onWait = func(context.Context, time.Duration) error {
					switch cause {
					case "day":
						c.advance(200 * time.Millisecond)
					case "expiry":
						c.wall = expiry.Add(time.Second)
					case "rollback":
						c.advance(time.Second)
						peak = c.wall
						_, _, _ = p.observe(context.Background())
						c.wall = c.wall.Add(-time.Second / 2)
					}
					if c.wall.After(peak) {
						peak = c.wall
					}
					return nil
				}
				l := dailyHashLimits(cap)
				l.RequestedBytesPerSecond = 1024
				p = c.pacer(1024)
				want := ErrHashReadReservationDay
				if cause == "expiry" {
					want = ErrHashReadExpired
				}
				if cause == "rollback" {
					want = ErrHashReadClockRollback
				}
				var e error
				var granted, read, offset int64
				var sha string
				if fresh {
					r, err := s.runFreshConsented(context.Background(), approval, ff.fresh.m.f.scanner, hashFreshRunHooks{pacing: p, execution: &l})
					e = err
					granted, read, offset, sha = r.ReservedBytes, r.Usage.RequestedBytes, r.DurableOffset, r.Progress.SHA256
				} else {
					r, err := s.runConsented(context.Background(), approval, original.source, original.scanner, hashStoreHooks{pacing: p, execution: &l})
					e = err
					granted, read, offset, sha = r.ReservedBytes, r.Usage.RequestedBytes, r.DurableOffset, r.Progress.SHA256
				}
				if !errors.Is(e, want) || granted != 64 || read != 0 || offset != 0 || sha != "" {
					t.Fatal("denied pre-read fence lost charge or admitted bytes", e, granted, read, offset, sha)
				}
				b := requireStoreReadBudget(t, s)
				expectedPeak := p.maxWall
				// The forward peak is explicitly captured before the later rollback.
				if b.MaxNow != expectedPeak || b.TotalReservedBytes != prior+64 || b.TotalReadBytes != prior || b.TotalOutstandingReservedBytes != 0 || b.TotalUnknownReservedBytes != 0 {
					t.Fatal("read refusal lost highest captured wall/permanent grant", b, expectedPeak, peak)
				}
				if cause == "day" && b.ReservedBytes != 0 {
					t.Fatal("old-day reservation charged again on rollover", b)
				}
			})
		}
	}
}

func TestHashStoreReadBudgetChargedNamespaceClockCannotExceedSharedClock(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(128))
	c := hashReadApprove(t, f, req)
	ctx := context.Background()
	if _, e := f.store.runConsented(ctx, c.ID, f.source, f.scanner, dailyOriginalHooks(f, 64)); e != nil {
		t.Fatal(e)
	}
	nextDay := c.Approval.CreatedAt.Truncate(24 * time.Hour).Add(24 * time.Hour)
	f.store.now = func() time.Time { return nextDay }
	r, e := f.store.RunConsentedBudgeted(ctx, c.ID, f.source, f.scanner, dailyHashLimits(1))
	if !errors.Is(e, ErrHashDeferred) || r.ReservedBytes != 0 {
		t.Fatal(r, e)
	}
	before := requireStoreReadBudget(t, f.store)
	if before.ReservedBytes != 0 || before.TotalReservedBytes != 64 || before.Day != nextDay.Format(time.DateOnly) {
		t.Fatal(before)
	}
	// Preserve both shared/lifetime sums and all immutable completed prefixes.
	// A charged namespace's fabricated later day/high cannot be a metadata
	// observation: legitimate newer consent and zero-total initialization remain
	// covered by the separate mixed-day/projection fixtures.
	later := nextDay.Add(24 * time.Hour)
	if _, e = f.store.db.Exec("UPDATE hash_budget SET day=?,max_now_ns=?,reserved_bytes=0,requested_bytes=0,read_bytes=0,unknown_reserved_bytes=0", later.Format(time.DateOnly), later.UnixNano()); e != nil {
		t.Fatal(e)
	}
	if b, e := f.store.StoreReadBudget(ctx); !errors.Is(e, ErrHashStoreReadBudgetCorrupt) || b != nil {
		t.Fatal("charged namespace newer than coupled shared clock accepted", b, e)
	}
}

func TestHashStoreReadBudgetRetainsSpecificNamespaceCorruption(t *testing.T) {
	for _, kind := range []string{"job", "consent", "progress"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			run, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, dailyFreshHooks(w, 130))
			if !errors.Is(err, ErrHashDeferred) || run.ReservedBytes != 0 || run.Usage.ReadBytes != 0 {
				t.Fatal("fixture did not activate accounting without a new read", run, err)
			}
			before := requireStoreReadBudget(t, w)
			var namespace error
			switch kind {
			case "job":
				namespace = ErrHashFreshJobCorrupt
				_, err = w.db.Exec("DROP TRIGGER hash_fresh_job_no_update;UPDATE hash_fresh_job SET payload=? WHERE id=?", []byte("not a canonical job record"), f.fresh.job.ID)
			case "consent":
				namespace = ErrHashFreshReadCorrupt
				_, err = w.db.Exec("DROP TRIGGER hash_fresh_read_approval_no_update;UPDATE hash_fresh_read_approval SET payload=? WHERE approval_id=?", []byte("not a canonical consent record"), f.consent.ID)
			case "progress":
				namespace = ErrHashFreshProgressCorrupt
				_, err = w.db.Exec("DROP TRIGGER hash_fresh_budget_no_delete;DELETE FROM hash_fresh_budget WHERE job_id=?", f.fresh.job.ID)
			}
			if err != nil {
				t.Fatal("generated namespace corruption setup failed", err)
			}
			w.now = func() time.Time { panic("saved corruption check sampled a clock") }
			got, err := w.StoreReadBudget(context.Background())
			if got != nil || !errors.Is(err, ErrHashStoreReadBudgetCorrupt) || !errors.Is(err, namespace) {
				t.Fatal("shared validation lost namespace refusal or exposed a partial report", got, err)
			}
			var reserved, total int64
			if err = w.db.QueryRow("SELECT reserved_bytes,total_reserved_bytes FROM hash_store_read_budget WHERE id=1").Scan(&reserved, &total); err != nil || reserved != before.ReservedBytes || total != before.TotalReservedBytes {
				t.Fatal("saved corruption check changed shared charges", reserved, total, err)
			}
		})
	}
}
