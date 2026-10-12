package inventory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func requireFreshRecoveryDiagnostic(t *testing.T, err error, phase freshHashRecoveryPhase, cause string) *freshHashRecoveryDiagnostic {
	t.Helper()
	var d *freshHashRecoveryDiagnostic
	if !errors.Is(err, ErrHashRecoveryRequired) || !errors.As(err, &d) || d.phase != phase || d.cause != cause {
		t.Fatal("recovery lost its bounded phase/cause or primary error", err, phase, cause)
	}
	if len(err.Error()) > freshHashRecoveryMessageLimit || strings.Contains(err.Error(), "PRIVATE_RAW_CAUSE") {
		t.Fatal("recovery exposed an unbounded or raw cause", err)
	}
	for _, misleading := range []error{context.Canceled, context.DeadlineExceeded, ErrHashReadExpired, ErrHashReadRevoked,
		ErrHashReadClockRollback, ErrHashReadReservationDay, ErrHashFreshRunBinding, ErrHashFreshReadBinding,
		ErrHashReadBinding, ErrHashFreshReadLimits, ErrHashReadLimits, ErrHashFreshReadConfirmation,
		ErrHashReadConfirmation, ErrHashFreshReadApprovalMissing, ErrHashReadApprovalMissing,
		ErrHashReadPacingCapacity, ErrHashReadPacingInput, ErrHashDeferred, ErrHashStoreReadBudgetRequired,
		ErrHashStoreReadBudgetCorrupt, ErrHashStoreCorrupt, ErrHashFreshProgressCorrupt,
		ErrHashFreshReadCorrupt, ErrHashFreshJobCorrupt, ErrHashInventoryChanged, os.ErrPermission, os.ErrNotExist} {
		if errors.Is(err, misleading) {
			t.Fatal("diagnostic cause replaced recovery as the primary failure", err, misleading)
		}
	}
	return d
}

func TestHashFreshRecoveryDiagnosticClosedClassesAndPrimaryError(t *testing.T) {
	for _, tc := range []struct {
		cause error
		class string
	}{
		{context.DeadlineExceeded, "deadline"}, {errFreshHashDeadlineWindow, "deadline_window"},
		{context.Canceled, "canceled"}, {ErrHashReadExpired, "expiry"}, {ErrHashReadRevoked, "revocation"},
		{ErrHashReadClockRollback, "clock_rollback"}, {ErrHashReadReservationDay, "day_boundary"},
		{ErrHashFreshRunBinding, "scope_binding"}, {ErrHashFreshReadBinding, "scope_binding"},
		{ErrHashReadBinding, "scope_binding"}, {ErrHashFreshProgressCorrupt, "progress_corrupt"},
		{ErrHashFreshReadCorrupt, "read_consent_corrupt"}, {ErrHashFreshJobCorrupt, "job_corrupt"},
		{ErrHashStoreReadBudgetCorrupt, "shared_budget_corrupt"}, {ErrHashStoreReadBudgetRequired, "shared_budget_required"},
		{ErrHashDeferred, "budget_deferred"}, {ErrHashStoreCorrupt, "store_corrupt"},
		{os.ErrPermission, "permission"}, {os.ErrNotExist, "not_found"},
		{sql.ErrTxDone, "transaction_done"}, {sql.ErrConnDone, "connection_done"},
		{ErrHashRecoveryRequired, "recovery_required"}, {ErrHashFreshReadLimits, "other"},
		{ErrHashReadLimits, "other"}, {ErrHashFreshReadConfirmation, "other"}, {ErrHashReadConfirmation, "other"},
		{ErrHashFreshReadApprovalMissing, "other"}, {ErrHashReadApprovalMissing, "other"},
		{ErrHashReadPacingCapacity, "other"}, {ErrHashReadPacingInput, "other"},
		{ErrHashInventoryChanged, "other"}, {errors.New("PRIVATE_RAW_CAUSE"), "other"},
	} {
		t.Run(fmt.Sprint(tc.cause), func(t *testing.T) {
			wrapped := fmt.Errorf("PRIVATE_RAW_CAUSE /private/generated/%s: %w", strings.Repeat("x", 4096), tc.cause)
			err := freshHashRecovery(freshRecoverySettlementCommit, wrapped)
			requireFreshRecoveryDiagnostic(t, err, freshRecoverySettlementCommit, tc.class)
			if strings.Contains(err.Error(), "/private/") {
				t.Fatal("raw cause path leaked", err)
			}
		})
	}
	foreign := strings.Repeat("PRIVATE_RAW_CAUSE", 1024)
	err := freshHashRecovery(freshHashRecoveryPhase(foreign), errors.New(foreign))
	requireFreshRecoveryDiagnostic(t, err, freshRecoveryUnknown, "other")
	if got := freshHashRecoveryIfRequired(freshRecoveryWriterAdmission, context.Canceled); got != context.Canceled {
		t.Fatal("an ordinary non-recovery error changed", got)
	}
}

func TestHashFreshRecoveryDiagnosticKeepsOnlyOneEarlierPair(t *testing.T) {
	err := freshHashRecovery(freshRecoverySettlementCommit, os.ErrPermission)
	for i := 0; i < 32; i++ {
		prior := err
		err = freshHashRecoveryWithEarlier(freshRecoveryFinalBudget, context.DeadlineExceeded, prior)
		d := requireFreshRecoveryDiagnostic(t, err, freshRecoveryFinalBudget, "deadline")
		wantPhase, wantCause := freshRecoveryFinalBudget, "deadline"
		if i == 0 {
			wantPhase, wantCause = freshRecoverySettlementCommit, "permission"
		}
		if d.earlierPhase != wantPhase || d.earlierCause != wantCause || strings.Count(err.Error(), "earlier phase=") != 1 {
			t.Fatal("earlier diagnostics grew into a chain or lost the immediate failure", err)
		}
	}
	d := requireFreshRecoveryDiagnostic(t, freshHashRecoveryWithEarlier(freshRecoveryFinalBudget,
		ErrHashRecoveryRequired, fmt.Errorf("PRIVATE_RAW_CAUSE: %w", context.Canceled)), freshRecoveryFinalBudget, "recovery_required")
	if d.earlierPhase != freshRecoveryEarlier || d.earlierCause != "canceled" {
		t.Fatal("an earlier ordinary failure was lost", d)
	}
	inner := freshHashRecovery(freshRecoveryPostCommitStorage, os.ErrNotExist)
	preserved := freshHashRecoveryPreserve(freshRecoverySettlementCommit, fmt.Errorf("PRIVATE_RAW_CAUSE: %w", inner))
	requireFreshRecoveryDiagnostic(t, preserved, freshRecoveryPostCommitStorage, "not_found")
	final := freshHashRecoveryFinalBudget(ErrHashRecoveryRequired, preserved, context.DeadlineExceeded)
	d = requireFreshRecoveryDiagnostic(t, final, freshRecoveryFinalBudget, "deadline_observed")
	if d.earlierPhase != freshRecoveryPostCommitStorage || d.earlierCause != "not_found" {
		t.Fatal("final budget diagnostic replaced the exact preceding phase", d)
	}
	requireFreshRecoveryDiagnostic(t, freshHashRecoveryFinalBudget(ErrHashRecoveryRequired, nil, context.Canceled), freshRecoveryFinalBudget, "canceled_observed")
	requireFreshRecoveryDiagnostic(t, freshHashRecoveryFinalBudget(os.ErrPermission, nil, context.DeadlineExceeded), freshRecoveryFinalBudget, "permission")
	requireFreshRecoveryDiagnostic(t, freshHashRecoveryFinalBudget(ErrHashReadExpired, nil, context.DeadlineExceeded), freshRecoveryFinalBudget, "expiry")
	requireFreshRecoveryDiagnostic(t, freshHashRecoveryFinalBudget(inner, nil, context.DeadlineExceeded), freshRecoveryFinalBudget, "not_found")
}

func TestHashFreshRecoveryReservationAndSettlementAcknowledgements(t *testing.T) {
	for _, phase := range []string{"reserve_uncommitted", "reserve_committed", "settle_uncommitted", "settle_committed"} {
		t.Run(phase, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			reads := 0
			wantPhase, wantCause := freshRecoveryReservationCommit, "permission"
			injected := fmt.Errorf("PRIVATE_RAW_CAUSE: %w", os.ErrPermission)
			if strings.HasPrefix(phase, "settle") {
				wantPhase, wantCause = freshRecoverySettlementCommit, "expiry"
				injected = fmt.Errorf("PRIVATE_RAW_CAUSE: %w", ErrHashReadExpired)
			}
			lost := func(tx *sql.Tx) error {
				if strings.HasSuffix(phase, "_committed") {
					if err := tx.Commit(); err != nil {
						return err
					}
				}
				return injected
			}
			hooks := hashFreshRunHooks{file: fileHashHooks{afterRead: func(int) { reads++ }}}
			if strings.HasPrefix(phase, "reserve") {
				hooks.reserveCommit = lost
			} else {
				hooks.settleCommit = lost
			}
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
			requireFreshRecoveryDiagnostic(t, err, wantPhase, wantCause)
			if r.Status != "recovery_required" || r.Code != "publication_uncertain" || r.Progress.SHA256 != "" || r.DurableOffset != 0 || r.ReservedBytes != 65 {
				t.Fatal("uncertain acknowledgement exposed a positive result or lost its charge", r, err)
			}
			wantReads := 0
			if strings.HasPrefix(phase, "settle") {
				wantReads = 1
			}
			if reads != wantReads {
				t.Fatal("source access did not respect reservation acknowledgement", reads, wantReads)
			}
			if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
				t.Fatal("a poisoned writer retried automatically", e)
			}
			w = f.open(t)
			job := requireFreshRunSaved(t, f, w)
			work := job.Progress[0]
			switch phase {
			case "reserve_uncommitted":
				if job.FreshReservedBytes != 0 || work.Sequence != 0 || work.LatestAttempt != nil || work.DurableOffset != 0 {
					t.Fatal("uncommitted reservation survived", job)
				}
			case "reserve_committed", "settle_uncommitted":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 0 || job.FreshBudget.TotalUnknownReservedBytes != 65 || work.Sequence != 1 || work.DurableOffset != 0 || work.LatestAttempt == nil || work.LatestAttempt.Status != "interrupted_unknown" || work.LatestAttempt.ReadBytes != nil {
					t.Fatal("recovery refunded/fabricated usage or accepted a tentative digest", job)
				}
			case "settle_committed":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 65 || job.FreshBudget.TotalUnknownReservedBytes != 0 || work.Status != "complete" || work.DurableOffset != 65 || work.LatestAttempt == nil || work.LatestAttempt.Status != "settled" || work.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(65))) {
					t.Fatal("committed settlement was repeated or marked unknown", job)
				}
			}
		})
	}
}

func TestHashFreshRecoveryOuterDeadlineDiagnostic(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	read := false
	r, err := w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner,
		hashFreshRunHooks{file: fileHashHooks{afterRead: func(int) { read = true; <-ctx.Done() }}})
	requireFreshRecoveryDiagnostic(t, err, freshRecoverySettlementWindow, "deadline_window")
	if !read || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
		t.Fatal("deadline diagnostic lost charged tentative usage or exposed a digest", r, err)
	}
	if overdue := time.Since(deadline); overdue > time.Second {
		t.Fatal("diagnostic started another window beyond the original deadline", overdue)
	}
	if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
		t.Fatal("exhausted writer remained dispatchable", e)
	}
	before := requireFreshRunSaved(t, f, w)
	if before.Progress[0].Status != "running" || before.Progress[0].LatestAttempt.ReadBytes != nil || before.FreshReadBytes != 0 {
		t.Fatal("unsaved usage was fabricated or recovered automatically", before)
	}
	w = f.open(t)
	after := requireFreshRunSaved(t, f, w)
	if after.Progress[0].Status != "pending" || after.Progress[0].DurableOffset != 0 || after.Progress[0].LatestAttempt.Status != "interrupted_unknown" || after.Progress[0].LatestAttempt.ReadBytes != nil || after.FreshReservedBytes != 65 || after.FreshReadBytes != 0 || after.FreshBudget.TotalUnknownReservedBytes != 65 {
		t.Fatal("exact reopening refunded a charge or imported a tentative prefix", after)
	}
}

func TestHashFreshRecoverySharedBudgetFinalizationDiagnostic(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	c := newHashPacingClock()
	c.wall = w.now()
	w.now = func() time.Time { return c.wall }
	limits := dailyHashLimits(4096)
	p := c.pacer(limits.RequestedBytesPerSecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	confirmed := false
	var settled SavedFreshJob
	hooks := hashFreshRunHooks{pacing: p, execution: &limits, beforeStoreBudgetFinish: func(opCtx context.Context) {
		if operationDeadline, ok := opCtx.Deadline(); !ok || !operationDeadline.Equal(deadline) {
			t.Fatal("budget finisher did not retain the original operation deadline", operationDeadline, deadline)
		}
		var err error
		settled, err = w.readFreshJob(opCtx, w.db, f.fresh.job.ID)
		if err != nil || settled.FreshReservedBytes != 65 || settled.FreshRequestedBytes != 65 || settled.FreshReadBytes != 65 || settled.FreshBudget.TotalUnknownReservedBytes != 0 || settled.Progress[0].Status != "complete" || settled.Progress[0].LatestAttempt.Status != "settled" {
			t.Fatal("fixture did not acknowledge the genuine settlement before finalization", settled, err)
		}
		confirmed = true
		// Capture a genuinely newer operation-local clock observation, then
		// observe the same operation context's deadline before its budget latch.
		c.advance(time.Nanosecond)
		if _, _, err = p.observe(opCtx); err != nil {
			t.Fatal(err)
		}
		<-opCtx.Done()
		if err := opCtx.Err(); err != context.DeadlineExceeded {
			t.Fatal("budget finisher context did not observe its deadline", err)
		}
	}}
	r, err := w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner, hooks)
	// The finisher's sentinel hides its internal cause. Report the separately
	// observed context deadline without claiming it is that hidden cause.
	requireFreshRecoveryDiagnostic(t, err, freshRecoveryFinalBudget, "deadline_observed")
	if !confirmed || r.Status != "recovery_required" || r.Progress.SHA256 != "" || r.ReservedBytes != 65 || r.Usage.RequestedBytes != 65 || r.Usage.ReadBytes != 65 {
		t.Fatal("finalization uncertainty exposed a digest or lost known usage", r, err)
	}
	if overdue := time.Since(deadline); overdue > time.Second {
		t.Fatal("budget finalization started a new window after the original deadline", overdue)
	}
	if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
		t.Fatal("finalization failure left a dispatchable writer", e)
	}
	w = f.open(t)
	job := requireFreshRunSaved(t, f, w)
	if job.FreshReservedBytes != 65 || job.FreshRequestedBytes != 65 || job.FreshReadBytes != 65 || job.FreshBudget.TotalUnknownReservedBytes != 0 || !reflect.DeepEqual(job.Progress, settled.Progress) {
		t.Fatal("exact reopening repeated known settlement or fabricated unknown usage", job, settled)
	}
	shared := requireStoreReadBudget(t, w)
	if shared.TotalReservedBytes != 195 || shared.TotalRequestedBytes != 195 || shared.TotalReadBytes != 195 || shared.TotalUnknownReservedBytes != 0 || shared.TotalOutstandingReservedBytes != 0 {
		t.Fatal("finalization failure changed full original and fresh charges", shared)
	}
}
