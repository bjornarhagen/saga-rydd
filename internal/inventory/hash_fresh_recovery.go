package inventory

import (
	"context"
	"database/sql"
	"errors"
	"os"
)

type freshHashRecoveryPhase string

const (
	freshRecoveryUnknown            freshHashRecoveryPhase = "unknown"
	freshRecoveryEarlier            freshHashRecoveryPhase = "earlier_failure"
	freshRecoveryWriterAdmission    freshHashRecoveryPhase = "writer_admission"
	freshRecoveryFinalBudget        freshHashRecoveryPhase = "shared_budget_finish"
	freshRecoveryClockLatch         freshHashRecoveryPhase = "read_clock_latch"
	freshRecoveryRunningWork        freshHashRecoveryPhase = "existing_running_work"
	freshRecoveryUsage              freshHashRecoveryPhase = "usage_validation"
	freshRecoverySettlementWindow   freshHashRecoveryPhase = "settlement_window"
	freshRecoveryExpiryLatch        freshHashRecoveryPhase = "expiry_latch"
	freshRecoverySettlementBegin    freshHashRecoveryPhase = "settlement_begin"
	freshRecoverySettlementSnapshot freshHashRecoveryPhase = "settlement_snapshot"
	freshRecoverySettlementConsent  freshHashRecoveryPhase = "settlement_consent"
	freshRecoverySettlementClock    freshHashRecoveryPhase = "settlement_shared_clock"
	freshRecoverySettlementSHA      freshHashRecoveryPhase = "settlement_checkpoint"
	freshRecoverySettlementAttempt  freshHashRecoveryPhase = "settlement_attempt"
	freshRecoverySettlementProgress freshHashRecoveryPhase = "settlement_progress"
	freshRecoverySettlementBudget   freshHashRecoveryPhase = "settlement_budget"
	freshRecoverySettlementConfirm  freshHashRecoveryPhase = "settlement_confirmation"
	freshRecoverySettlementCommit   freshHashRecoveryPhase = "settlement_commit"
	freshRecoveryReservationCommit  freshHashRecoveryPhase = "reservation_commit"
	freshRecoveryLifecycleCommit    freshHashRecoveryPhase = "lifecycle_commit"
	freshRecoveryRecoveryCommit     freshHashRecoveryPhase = "recovery_commit"
	freshRecoveryProgressCommit     freshHashRecoveryPhase = "progress_commit"
	freshRecoveryPostCommitStorage  freshHashRecoveryPhase = "post_commit_storage"
)

const freshHashRecoveryMessageLimit = 512

// A failed time admission is observed here; it is distinct from an underlying
// operation returning context.DeadlineExceeded or an unspecified recovery error.
var errFreshHashDeadlineWindow = errors.New("fresh publication time window ended")

// These scalar classes describe an uncertain publication. They never retain
// the underlying error, raw message, path or a growing chain of earlier errors.
type freshHashRecoveryDiagnostic struct {
	phase, earlierPhase freshHashRecoveryPhase
	cause, earlierCause string
}

func freshHashRecoveryKnownPhase(p freshHashRecoveryPhase) freshHashRecoveryPhase {
	switch p {
	case freshRecoveryUnknown, freshRecoveryEarlier, freshRecoveryWriterAdmission,
		freshRecoveryFinalBudget, freshRecoveryClockLatch, freshRecoveryRunningWork,
		freshRecoveryUsage, freshRecoverySettlementWindow, freshRecoveryExpiryLatch,
		freshRecoverySettlementBegin, freshRecoverySettlementSnapshot,
		freshRecoverySettlementConsent, freshRecoverySettlementClock,
		freshRecoverySettlementSHA, freshRecoverySettlementAttempt,
		freshRecoverySettlementProgress, freshRecoverySettlementBudget,
		freshRecoverySettlementConfirm, freshRecoverySettlementCommit,
		freshRecoveryReservationCommit, freshRecoveryLifecycleCommit,
		freshRecoveryRecoveryCommit, freshRecoveryProgressCommit, freshRecoveryPostCommitStorage:
		return p
	default:
		return freshRecoveryUnknown
	}
}

func freshHashRecoveryKnownCause(c string) string {
	switch c {
	case "deadline", "deadline_window", "deadline_observed", "canceled", "canceled_observed", "expiry", "revocation", "clock_rollback", "day_boundary",
		"scope_binding", "progress_corrupt", "read_consent_corrupt", "job_corrupt",
		"shared_budget_corrupt", "shared_budget_required", "budget_deferred",
		"store_corrupt", "permission", "not_found", "transaction_done",
		"connection_done", "recovery_required", "other":
		return c
	default:
		return "other"
	}
}

func freshHashRecoveryCause(err error) string {
	var prior *freshHashRecoveryDiagnostic
	if errors.As(err, &prior) {
		return freshHashRecoveryKnownCause(prior.cause)
	}
	switch {
	case errors.Is(err, errFreshHashDeadlineWindow):
		return "deadline_window"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrHashReadExpired):
		return "expiry"
	case errors.Is(err, ErrHashReadRevoked):
		return "revocation"
	case errors.Is(err, ErrHashReadClockRollback):
		return "clock_rollback"
	case errors.Is(err, ErrHashReadReservationDay):
		return "day_boundary"
	case errors.Is(err, ErrHashFreshRunBinding), errors.Is(err, ErrHashFreshReadBinding), errors.Is(err, ErrHashReadBinding):
		return "scope_binding"
	case errors.Is(err, ErrHashFreshProgressCorrupt):
		return "progress_corrupt"
	case errors.Is(err, ErrHashFreshReadCorrupt):
		return "read_consent_corrupt"
	case errors.Is(err, ErrHashFreshJobCorrupt):
		return "job_corrupt"
	case errors.Is(err, ErrHashStoreReadBudgetCorrupt):
		return "shared_budget_corrupt"
	case errors.Is(err, ErrHashStoreReadBudgetRequired):
		return "shared_budget_required"
	case errors.Is(err, ErrHashDeferred):
		return "budget_deferred"
	case errors.Is(err, ErrHashStoreCorrupt):
		return "store_corrupt"
	case errors.Is(err, os.ErrPermission):
		return "permission"
	case errors.Is(err, os.ErrNotExist):
		return "not_found"
	case errors.Is(err, sql.ErrTxDone):
		return "transaction_done"
	case errors.Is(err, sql.ErrConnDone):
		return "connection_done"
	case errors.Is(err, ErrHashRecoveryRequired):
		return "recovery_required"
	default:
		return "other"
	}
}

func (e *freshHashRecoveryDiagnostic) Error() string {
	message := ErrHashRecoveryRequired.Error() + "; fresh phase=" + string(freshHashRecoveryKnownPhase(e.phase)) + "; cause=" + freshHashRecoveryKnownCause(e.cause)
	if e.earlierPhase != "" {
		message += "; earlier phase=" + string(freshHashRecoveryKnownPhase(e.earlierPhase)) + "; cause=" + freshHashRecoveryKnownCause(e.earlierCause)
	}
	return message
}

// Recovery alone is the primary error. Diagnostic expiry, budget, permission
// or binding classes must not select an earlier machine failure branch.
func (e *freshHashRecoveryDiagnostic) Unwrap() error { return ErrHashRecoveryRequired }

func freshHashRecovery(phase freshHashRecoveryPhase, cause error) error {
	return &freshHashRecoveryDiagnostic{phase: freshHashRecoveryKnownPhase(phase), cause: freshHashRecoveryCause(cause)}
}

func freshHashRecoveryPreserve(phase freshHashRecoveryPhase, cause error) error {
	var prior *freshHashRecoveryDiagnostic
	if !errors.As(cause, &prior) {
		return freshHashRecovery(phase, cause)
	}
	d := &freshHashRecoveryDiagnostic{phase: freshHashRecoveryKnownPhase(prior.phase), cause: freshHashRecoveryKnownCause(prior.cause)}
	if prior.earlierPhase != "" {
		d.earlierPhase, d.earlierCause = freshHashRecoveryKnownPhase(prior.earlierPhase), freshHashRecoveryKnownCause(prior.earlierCause)
	}
	return d
}

func freshHashRecoveryWithEarlier(phase freshHashRecoveryPhase, cause, earlier error) error {
	d := &freshHashRecoveryDiagnostic{phase: freshHashRecoveryKnownPhase(phase), cause: freshHashRecoveryCause(cause)}
	if earlier != nil {
		var prior *freshHashRecoveryDiagnostic
		if errors.As(earlier, &prior) {
			d.earlierPhase, d.earlierCause = freshHashRecoveryKnownPhase(prior.phase), freshHashRecoveryKnownCause(prior.cause)
		} else {
			d.earlierPhase, d.earlierCause = freshRecoveryEarlier, freshHashRecoveryCause(earlier)
		}
	}
	return d
}

func freshHashRecoveryFinalBudget(cause, earlier, observedContext error) error {
	d := freshHashRecoveryWithEarlier(freshRecoveryFinalBudget, cause, earlier).(*freshHashRecoveryDiagnostic)
	// The finisher can return an unspecified sentinel. A concurrently observed
	// context state supplies a separate qualified label, not an inferred cause.
	// Never replace a more specific error or diagnostic with a context fallback.
	if cause == ErrHashRecoveryRequired {
		switch observedContext {
		case context.DeadlineExceeded:
			d.cause = "deadline_observed"
		case context.Canceled:
			d.cause = "canceled_observed"
		}
	}
	return d
}

func freshHashRecoveryIfRequired(phase freshHashRecoveryPhase, err error) error {
	if errors.Is(err, ErrHashRecoveryRequired) {
		return freshHashRecovery(phase, err)
	}
	return err
}

func freshHashRecoveryCauseOr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

func freshHashCommitPhase(operation string) freshHashRecoveryPhase {
	switch operation {
	case "reservation", "refused reservation clock", "shared clock refusal", "deferred reservation clock":
		return freshRecoveryReservationCommit
	case "settlement":
		return freshRecoverySettlementCommit
	case "recovery":
		return freshRecoveryRecoveryCommit
	case "read lifecycle observation", "expiry observation":
		return freshRecoveryLifecycleCommit
	default:
		return freshRecoveryProgressCommit
	}
}
