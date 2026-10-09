package inventory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func (s *HashStore) RunFreshConsented(ctx context.Context, id string, scanner *Scanner) (HashFreshRunResult, error) {
	return s.runFreshConsented(ctx, id, scanner, hashFreshRunHooks{})
}

func (s *HashStore) freshSourceGuard(job SavedFreshJob) (*keeperChoiceMetadataCore, error) {
	core := &keeperChoiceMetadataCore{base: s.base, locator: cloneHashReadLocator(job.Record.Request.SourceLocator), choice: job.Record.Request.HistoricalChoice,
		storageIDs: s.freshJobRequest.storageIDs, protectedIDs: s.freshJobRequest.protectedIDs}
	// The inventory boundary includes the entire immutable original proposal;
	// only this fresh job's exact ordered roles may consume source bytes.
	for _, target := range s.freshJobRequest.proposal.Targets {
		digest, err := hashTargetDigest(target)
		if err != nil {
			return nil, ErrHashFreshProgressCorrupt
		}
		core.targets = append(core.targets, cloneHashTarget(target))
		core.targetDigests = append(core.targetDigests, digest)
	}
	return core, nil
}

func freshInventoryFailure(err error) error {
	if errors.Is(err, state.ErrFileSampleEvidence) || errors.Is(err, state.ErrFileSampleSchema) || errors.Is(err, state.ErrFileSampleSelection) {
		return ErrHashInventoryChanged
	}
	return err
}

func (s *HashStore) observeFreshRunApproval(ctx context.Context, id string) (SavedFreshJob, error) {
	return s.observeFreshRunApprovalAt(ctx, id, time.Time{})
}

func (s *HashStore) observeFreshRunApprovalAt(ctx context.Context, id string, now time.Time) (SavedFreshJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SavedFreshJob{}, err
	}
	defer tx.Rollback()
	job, err := s.readFreshJob(ctx, tx, s.freshRunJobID)
	if err != nil {
		return SavedFreshJob{}, err
	}
	if job.ReadConsent == nil || job.ReadConsent.ID != id || !equalHashFreshJobRequests(job.Record.Request, s.freshJobRequest.report) || len(job.Progress) != len(job.Work) || job.FreshBudget == nil {
		return SavedFreshJob{}, ErrHashFreshRunBinding
	}
	if now.IsZero() {
		now = s.now().UTC()
	}
	guardErr := s.observeHashFreshReadConsent(ctx, tx, job.ReadConsent, now)
	if guardErr != nil && !errors.Is(guardErr, ErrHashReadExpired) && !errors.Is(guardErr, ErrHashReadRevoked) && !errors.Is(guardErr, ErrHashReadClockRollback) {
		return SavedFreshJob{}, guardErr
	}
	if err = s.commitFreshProgress(ctx, tx, job, nil, nil, nil, "read lifecycle observation"); err != nil {
		return SavedFreshJob{}, err
	}
	return job, guardErr
}

// RunFreshConsented performs one bounded fresh step. It owns the derived
// inventory and accepts no source/path, caller allowance or continuation.
// Reservations are durable before bytes; every result is a sequential
// observation, never simultaneous equality, execution or cleanup authority.
func (s *HashStore) runFreshConsented(ctx context.Context, id string, scanner *Scanner, hooks hashFreshRunHooks) (result HashFreshRunResult, retErr error) {
	reserved := false
	if hooks.pacing != nil {
		defer func() {
			result.ReadPacing = hooks.pacing.observation()
			if hooks.pacing.refusal != nil && result.Status != "recovery_required" && !errors.Is(retErr, ErrHashRecoveryRequired) {
				retErr = hooks.pacing.refusal
				result.Progress.SHA256 = ""
				result.Status, result.Code = hashReadPacingRefusalState(retErr)
				result.Progress.Status, result.Progress.Code = result.Status, result.Code
			}
		}()
	}
	if !ValidHashFreshReadApprovalID(id) {
		return result, ErrHashFreshReadApprovalMissing
	}
	opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)
	defer opCancel()
	if err := s.acquire(opCtx, true); err != nil {
		return result, err
	}
	defer s.mu.Unlock()
	if s.freshRunJobID == "" || s.freshJobRequest == nil || len(s.freshRunBaselines) != len(s.freshJobRequest.report.Targets) {
		return result, ErrHashFreshRunBinding
	}
	if scanner == nil || scanner.closed.Load() {
		return result, blocked("scanner_closed", "An open scanner with current policy is required; no source bytes were read.")
	}
	stop := context.AfterFunc(s.life, opCancel)
	defer stop()
	if err := s.freshReadStorage(opCtx); err != nil {
		return result, err
	}
	var job SavedFreshJob
	var err error
	if hooks.pacing == nil {
		job, err = s.observeFreshRunApproval(opCtx, id)
	} else {
		job, err = s.observeFreshRunApprovalAt(opCtx, id, hashReadPacingClock(hooks.pacing, opCtx, s.now().UTC()))
	}
	if job.ID != "" {
		result = HashFreshRunResult{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: job.Record.Request.RequestID, ChoiceID: job.Record.Request.ChoiceID, ApprovalID: id, FreshBudget: job.FreshBudget}
	}
	if hooks.pacing != nil && job.ReadConsent != nil {
		defer func() {
			if reserved || !hooks.pacing.maxWall.After(job.ReadConsent.ClockHighWater) || s.poisoned {
				return
			}
			deadline, _ := opCtx.Deadline()
			latchDeadline := minTime(deadline, time.Now().Add(2*time.Second))
			var e error
			if time.Now().Before(latchDeadline) {
				latchCtx, latchCancel := context.WithDeadline(context.WithoutCancel(opCtx), latchDeadline)
				_, e = s.observeFreshRunApprovalAt(latchCtx, id, hooks.pacing.maxWall)
				latchCancel()
			} else {
				e = ErrHashRecoveryRequired
			}
			if e != nil && !errors.Is(e, ErrHashReadExpired) && !errors.Is(e, ErrHashReadClockRollback) && !errors.Is(e, ErrHashReadRevoked) {
				s.poisoned = true
				result.Progress.SHA256 = ""
				result.Status, result.Code, retErr = "recovery_required", "publication_uncertain", ErrHashRecoveryRequired
			}
		}()
	}
	if err != nil {
		result.Status, result.Code = "refused", hashReadErrorCode(err)
		return result, err
	}
	for _, progress := range job.Progress {
		if progress.Status == "running" {
			s.poisoned = true
			return result, ErrHashRecoveryRequired
		}
	}
	now := hashReadPacingClock(hooks.pacing, opCtx, s.now().UTC())
	if !validHashReadClock(now) || now.Before(job.ReadConsent.ClockHighWater) {
		result.Status, result.Code = "refused", "clock_rollback"
		return result, ErrHashReadClockRollback
	}
	if !now.Before(job.ReadConsent.Approval.ExpiresAt) {
		err = s.latchFreshRunExpiryWithinDeadline(opCtx, job, id)
		result.Status, result.Code = "refused", "read_consent_expired"
		if errors.Is(err, ErrHashRecoveryRequired) {
			result.Status, result.Code = "recovery_required", "publication_uncertain"
		}
		return result, err
	}
	remaining := job.ReadConsent.Approval.ExpiresAt.Sub(now)
	deadline, _ := opCtx.Deadline()
	// A timer that expires before this operation's own deadline could leave
	// no time to save permanent expiry. Refuse that window before opening the
	// inventory or making a reservation, without claiming saved expiry.
	if remaining <= time.Until(deadline) {
		result.Status, result.Code = "refused", "fresh_read_window_too_short"
		return result, ErrHashFreshReadWindow
	}
	readCtx, readCancel := context.WithTimeoutCause(opCtx, max(remaining, time.Duration(0)), ErrHashReadExpired)
	defer readCancel()
	readCtx, cancelCause := context.WithCancelCause(readCtx)
	defer cancelCause(context.Canceled)
	if hooks.pacing != nil {
		hooks.pacing.bind(job.ReadConsent.Approval.ExpiresAt, job.ReadConsent.ClockHighWater, "", cancelCause)
	}
	// Explicit wall-clock changes can cause expiry during live work. Persist
	// that observation using only time left in this operation's deadline.
	defer func() {
		if !errors.Is(context.Cause(readCtx), ErrHashReadExpired) || s.poisoned {
			return
		}
		result.Progress.SHA256 = ""
		retErr = s.latchFreshRunExpiryWithinDeadline(opCtx, job, id)
		if errors.Is(retErr, ErrHashRecoveryRequired) {
			result.Status, result.Code = "recovery_required", "publication_uncertain"
			return
		}
		result.Status, result.Code = "refused", "read_consent_expired"
		result.Progress.Status, result.Progress.Code = result.Status, result.Code
	}()
	rows, err := s.db.QueryContext(opCtx, "SELECT ordinal FROM hash_fresh_progress INDEXED BY hash_fresh_progress_pending WHERE job_id=? AND status='pending' ORDER BY ready_order,ordinal LIMIT 21", job.ID)
	if err != nil {
		return result, err
	}
	var ids []int
	for rows.Next() {
		var ordinal int
		if err = rows.Scan(&ordinal); err != nil {
			break
		}
		ids = append(ids, ordinal)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil || len(ids) > FileSampleTargetLimit {
		return result, hashFreshProgressFailure(opCtx, err)
	}
	if len(ids) == 0 {
		result.Status = "idle"
		return result, opCtx.Err()
	}
	now = hashReadPacingClock(hooks.pacing, readCtx, s.now().UTC())
	b := *job.FreshBudget
	if now.Format(time.DateOnly) > b.Day {
		b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes = 0, 0, 0, 0
	}
	available := min(max(job.ReadConsent.Approval.DailyReservedByteLimit-b.ReservedBytes, int64(0)), max(job.ReadConsent.Approval.LifetimeReservedByteLimit-b.TotalReservedBytes, int64(0)))
	var chosen hashStoredWork
	found := false
	for _, ordinal := range ids {
		w, readErr := readFreshHashWork(opCtx, s.db, job, ordinal)
		if readErr != nil {
			return result, readErr
		}
		left := job.Record.Request.Targets[ordinal-1].Target.File.Size - w.offset
		grant := min(FileHashStepByteLimit, available, left)
		if grant < left {
			grant -= grant % 64
		}
		if left == 0 || grant > 0 {
			chosen, found = w, true
			break
		}
	}
	if !found {
		result.Status, result.Code = "deferred", freshDeferralCode(job.ReadConsent, b, available)
		return result, ErrHashDeferred
	}
	if chosen.sequence == math.MaxInt64 {
		return result, ErrHashFreshProgressCorrupt
	}
	item := job.Record.Request.Targets[chosen.id-1]
	target := item.Target
	result.Ordinal, result.HistoricalWorkID, result.Role = chosen.id, item.Observation.WorkID, item.Role
	result.DurableOffset, result.Progress = chosen.offset, hashHistoricalProgress(target, chosen.checkpoint)
	if hooks.pacing != nil {
		if err = hooks.pacing.preflight(readCtx, target.File.Size-chosen.offset); err != nil {
			result.Status, result.Code = "refused", hashReadPacingCode(err)
			return result, err
		}
	}
	guard, err := s.freshSourceGuard(job)
	if err != nil {
		return result, err
	}
	sourceIDs, err := guard.checkMetadataSourceStorage(readCtx, nil)
	if err != nil {
		return result, err
	}
	source, err := state.OpenReader(readCtx, guard.metadataSourceDirectory())
	if err != nil {
		return result, err
	}
	defer source.Close()
	sourceIDs, err = guard.checkMetadataSourceStorage(readCtx, sourceIDs)
	if err != nil {
		return result, err
	}
	if hooks.beforeInventoryCheck != nil {
		hooks.beforeInventoryCheck()
	}
	if err = guard.compareMetadataInventory(readCtx, source); err != nil {
		result.Status, result.Code = "refused", "saved_inventory_changed"
		return result, freshInventoryFailure(err)
	}
	if hooks.afterInventoryCheck != nil {
		hooks.afterInventoryCheck()
	}
	if _, err = guard.checkMetadataSourceStorage(readCtx, sourceIDs); err != nil {
		return result, err
	}
	attempt, err := s.reserveFreshHashAttempt(readCtx, job, chosen, hooks)
	if err != nil {
		result.Status, result.Code = "refused", hashReadPacingCode(err)
		if errors.Is(err, ErrHashDeferred) {
			result.Status, result.Code = "deferred", attempt.guardCode
		}
		if errors.Is(err, ErrHashRecoveryRequired) {
			result.ReservedBytes = attempt.ReservedBytes
			result.Status, result.Code = "recovery_required", "publication_uncertain"
		}
		return result, err
	}
	reserved = true
	result.ReservedBytes = attempt.ReservedBytes
	if hooks.pacing != nil {
		hooks.pacing.bind(job.ReadConsent.Approval.ExpiresAt, attempt.clockHighWater, attempt.ReservationDay, cancelCause)
		hooks.file.pacing = hooks.pacing
	}
	if hooks.afterReserve != nil {
		hooks.afterReserve()
	}
	if now = hashReadPacingClock(hooks.pacing, readCtx, s.now().UTC()); !now.Before(job.ReadConsent.Approval.ExpiresAt) {
		cancelCause(ErrHashReadExpired)
	} else if !validHashReadClock(now) || now.Before(attempt.clockHighWater) {
		cancelCause(ErrHashReadClockRollback)
	}
	var usage FileReadUsage
	var progress FullHashProgress
	next, status, code := chosen.checkpoint, "pending", ""
	if err = s.freshReadStorage(readCtx); err == nil {
		_, err = guard.checkMetadataSourceStorage(readCtx, sourceIDs)
	}
	if err == nil {
		err = freshInventoryFailure(guard.compareMetadataInventory(readCtx, source))
	}
	if err == nil {
		var session *FullHashSession
		session, err = restoreHashSession(scanner, target, chosen.checkpoint)
		if err == nil {
			baseline := s.freshRunBaselines[chosen.id-1]
			session.core.initialMetadataGuard = func(live unix.Stat_t, volume, mount string) error {
				if volume != baseline.volume || mount != baseline.mount {
					return blocked("mount_boundary", "The file's mount differs from its historical full-hash metadata; no fresh bytes were read.")
				}
				if !baseline.stamp.matches(live) {
					return blocked("file_changed", "The file's identity, mode, link count or metadata differs from its historical full-hash metadata; no fresh bytes were read.")
				}
				return nil
			}
			progress, usage, err = session.step(readCtx, max(attempt.ReservedBytes, int64(1)), hooks.file)
			if err == nil {
				next = session.core.checkpoint
				if next.complete {
					status = "complete"
				}
			}
		}
	}
	if err != nil && readCtx.Err() == nil {
		status, code = "invalidated", freshFailureCode(err, progress.Code)
	}
	if s.life.Err() != nil {
		opCancel()
	}
	if now = hashReadPacingClock(hooks.pacing, readCtx, s.now().UTC()); !now.Before(job.ReadConsent.Approval.ExpiresAt) {
		cancelCause(ErrHashReadExpired)
	} else if !validHashReadClock(now) || now.Before(attempt.clockHighWater) {
		cancelCause(ErrHashReadClockRollback)
	}
	if hooks.beforeFinalInventoryCheck != nil {
		hooks.beforeFinalInventoryCheck()
	}
	if readCtx.Err() == nil {
		if _, checkErr := guard.checkMetadataSourceStorage(readCtx, sourceIDs); checkErr != nil {
			err = checkErr
		}
		if err == nil {
			err = freshInventoryFailure(guard.compareMetadataInventory(readCtx, source))
		}
		if err == nil {
			_, err = guard.checkMetadataSourceStorage(readCtx, sourceIDs)
		}
		if err == nil {
			err = s.freshReadStorage(readCtx)
		}
		if scanner.closed.Load() && err == nil {
			err = blocked("scanner_closed", "The scanner closed before fresh hashing finished.")
		}
		if err != nil {
			next, status, code = chosen.checkpoint, "invalidated", freshFailureCode(err, progress.Code)
		}
	}
	if closeErr := source.Close(); err == nil && closeErr != nil {
		err = closeErr
		next, status, code = chosen.checkpoint, "invalidated", "inventory_unavailable"
	}
	if hooks.pacing != nil {
		_, _, _ = hooks.pacing.observe(readCtx)
	}
	if readCtx.Err() != nil {
		next, status, code = chosen.checkpoint, "pending", ""
		err = context.Cause(readCtx)
	}
	result.Progress, result.Usage = progress, usage
	if usage.RequestedBytes < 0 || usage.ReadBytes < 0 || usage.ReadBytes > usage.RequestedBytes || usage.RequestedBytes > attempt.ReservedBytes || usage.Elapsed < 0 {
		s.poisoned = true
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Status, result.Code = "recovery_required", "publication_uncertain"
		return result, ErrHashRecoveryRequired
	}
	deadline, _ = opCtx.Deadline()
	settleDeadline := minTime(deadline, time.Now().Add(2*time.Second))
	if !time.Now().Before(settleDeadline) {
		s.poisoned = true
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Status, result.Code = "recovery_required", "publication_uncertain"
		return result, ErrHashRecoveryRequired
	}
	settleCtx, settleCancel := context.WithDeadline(context.WithoutCancel(opCtx), settleDeadline)
	defer settleCancel()
	durable, vetoed, settleErr := s.settleFreshHashAttempt(settleCtx, readCtx, cancelCause, job, chosen, attempt, next, status, code, usage, hooks)
	if settleErr != nil {
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Status, result.Code = "recovery_required", "publication_uncertain"
		return result, settleErr
	}
	result.DurableOffset = durable
	if vetoed {
		err = context.Cause(readCtx)
		status, code = "pending", ""
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
	}
	result.Status, result.Code = status, code
	if err == nil && hooks.pacing != nil && hooks.pacing.yielded && usage.RequestedBytes == 0 && attempt.ReservedBytes > 0 {
		result.Code = "pacing_window_exhausted"
	}
	if status == "complete" {
		result.Status = "hash_observed"
	}
	saved, readErr := s.readFreshJob(settleCtx, s.db, job.ID)
	if readErr != nil {
		result.Progress.SHA256 = ""
		return result, readErr
	}
	result.FreshBudget = saved.FreshBudget
	if canceled := readCtx.Err(); canceled != nil || opCtx.Err() != nil {
		result.Status, result.Code = "canceled", "request_canceled"
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Progress.Status, result.Progress.Code = result.Status, result.Code
		cause := context.Cause(readCtx)
		if cause == nil {
			cause = opCtx.Err()
		}
		if errors.Is(cause, ErrHashReadExpired) || errors.Is(cause, ErrHashReadClockRollback) || errors.Is(cause, ErrHashReadRevoked) || errors.Is(cause, ErrHashReadReservationDay) {
			result.Status, result.Code = "refused", hashReadPacingCode(cause)
			result.Progress.Status, result.Progress.Code = result.Status, result.Code
		}
		return result, cause
	}
	if err != nil {
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Progress.Status, result.Progress.Code = result.Status, result.Code
	}
	return result, err
}

func (s *HashStore) latchFreshRunExpiryWithinDeadline(ctx context.Context, job SavedFreshJob, id string) error {
	deadline, _ := ctx.Deadline()
	latchDeadline := minTime(deadline, time.Now().Add(2*time.Second))
	if time.Now().Before(latchDeadline) {
		latchCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), latchDeadline)
		defer cancel()
		if s.latchFreshRunExpiry(latchCtx, job, id) == nil {
			return ErrHashReadExpired
		}
	}
	s.poisoned = true
	return fmt.Errorf("fresh job %s with key %s expiry observation could not be confirmed inside the operation deadline; close and inspect saved state: %w", job.ID, job.Record.JobKey, ErrHashRecoveryRequired)
}

func (s *HashStore) latchFreshRunExpiry(ctx context.Context, job SavedFreshJob, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := s.readFreshJob(ctx, tx, job.ID)
	if err != nil {
		return err
	}
	if current.ReadConsent == nil || current.ReadConsent.ID != id || !equalHashFreshJobRequests(current.Record.Request, s.freshJobRequest.report) {
		return ErrHashFreshRunBinding
	}
	if current.ReadConsent.ExpiredObserved {
		return s.freshReadStorage(ctx)
	}
	now := s.now().UTC()
	if !validHashReadClock(now) || now.Before(current.ReadConsent.Approval.ExpiresAt) {
		now = current.ReadConsent.Approval.ExpiresAt
	}
	guardErr := s.observeHashFreshReadConsent(ctx, tx, current.ReadConsent, now)
	if guardErr != nil && !errors.Is(guardErr, ErrHashReadExpired) && !errors.Is(guardErr, ErrHashReadRevoked) {
		return guardErr
	}
	return s.commitFreshProgress(ctx, tx, current, nil, nil, nil, "expiry observation")
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func freshFailureCode(err error, code string) string {
	if errors.Is(err, ErrHashInventoryChanged) {
		return "saved_inventory_changed"
	}
	var failure liveError
	if errors.As(err, &failure) {
		return failure.code
	}
	if code != "" {
		return code
	}
	return "hash_unavailable"
}

func freshDeferralCode(c *HashFreshReadConsent, b HashBudget, available int64) string {
	if b.TotalReservedBytes >= c.Approval.LifetimeReservedByteLimit {
		return "lifetime_byte_limit"
	}
	if available == 0 {
		return "daily_byte_limit"
	}
	return "durable_quantum"
}

func (s *HashStore) reserveFreshHashAttempt(ctx context.Context, job SavedFreshJob, w hashStoredWork, hooks hashFreshRunHooks) (hashStoredAttempt, error) {
	var a hashStoredAttempt
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	if hooks.beforeReserveCommit != nil {
		hooks.beforeReserveCommit()
	}
	if hooks.pacing != nil {
		if e := hooks.pacing.preflight(ctx, job.Record.Request.Targets[w.id-1].Target.File.Size-w.offset); e != nil {
			return a, e
		}
	}
	current, err := s.readFreshJob(ctx, tx, job.ID)
	if err != nil {
		return a, err
	}
	if current.ReadConsent == nil || current.ReadConsent.ID != job.ReadConsent.ID || !equalHashFreshJobRequests(current.Record.Request, s.freshJobRequest.report) {
		return a, ErrHashFreshRunBinding
	}
	now := hashReadPacingClock(hooks.pacing, ctx, s.now().UTC())
	guardErr := s.observeHashFreshReadConsent(ctx, tx, current.ReadConsent, now)
	if guardErr != nil {
		if errors.Is(guardErr, ErrHashReadExpired) || errors.Is(guardErr, ErrHashReadRevoked) || errors.Is(guardErr, ErrHashReadClockRollback) {
			if err = s.commitFreshProgress(ctx, tx, current, nil, nil, hooks.reserveCommit, "refused reservation clock"); err != nil {
				return a, err
			}
		}
		return a, guardErr
	}
	b := *current.FreshBudget
	if now.Before(b.MaxNow) {
		return a, ErrHashReadClockRollback
	}
	if now.Format(time.DateOnly) > b.Day {
		b.Day = now.Format(time.DateOnly)
		b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes = 0, 0, 0, 0
	}
	b.MaxNow = now
	available := min(max(current.ReadConsent.Approval.DailyReservedByteLimit-b.ReservedBytes, int64(0)), max(current.ReadConsent.Approval.LifetimeReservedByteLimit-b.TotalReservedBytes, int64(0)))
	left := job.Record.Request.Targets[w.id-1].Target.File.Size - w.offset
	grant := min(FileHashStepByteLimit, available, left)
	if grant < left {
		grant -= grant % 64
	}
	if grant == 0 && left > 0 {
		a.guardCode = freshDeferralCode(current.ReadConsent, b, available)
		if err = s.commitFreshProgress(ctx, tx, current, nil, nil, hooks.reserveCommit, "deferred reservation clock"); err != nil {
			return a, err
		}
		return a, ErrHashDeferred
	}
	token, err := hashStoreToken()
	if err != nil {
		return a, err
	}
	a = hashStoredAttempt{HashAttempt: HashAttempt{Status: "reserved", ReservationDay: b.Day, ReservedBytes: grant}, nonce: token, sequence: w.sequence, offset: w.offset, clockHighWater: current.ReadConsent.ClockHighWater}
	b.ReservedBytes, err = addHashCounter(b.ReservedBytes, grant)
	if err == nil {
		b.TotalReservedBytes, err = addHashCounter(b.TotalReservedBytes, grant)
	}
	if err != nil {
		return a, ErrHashFreshProgressCorrupt
	}
	var cursor int64
	if err = tx.QueryRowContext(ctx, "SELECT next_order FROM hash_fresh_run_state WHERE job_id=?", job.ID).Scan(&cursor); err != nil || cursor == math.MaxInt64 {
		return a, hashFreshProgressFailure(ctx, err)
	}
	cursor++
	r, err := tx.ExecContext(ctx, "UPDATE hash_fresh_progress SET status='running',ready_order=? WHERE job_id=? AND ordinal=? AND status='pending' AND sequence=? AND checked_offset=?", cursor, job.ID, w.id, w.sequence, w.offset)
	if err != nil {
		return a, hashFreshProgressFailure(ctx, err)
	}
	if n, e := r.RowsAffected(); e != nil || n != 1 {
		return a, hashFreshProgressFailure(ctx, e)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE hash_fresh_run_state SET next_order=? WHERE job_id=?", cursor, job.ID); err != nil {
		return a, hashFreshProgressFailure(ctx, err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO hash_fresh_attempt VALUES(?,?,?,?,?,?,?,'reserved',NULL,NULL,NULL) ON CONFLICT(job_id,ordinal) DO UPDATE SET nonce=excluded.nonce,base_sequence=excluded.base_sequence,from_offset=excluded.from_offset,grant_bytes=excluded.grant_bytes,reservation_day=excluded.reservation_day,status='reserved',requested_bytes=NULL,read_bytes=NULL,elapsed_ns=NULL`, job.ID, w.id, a.nonce, w.sequence, w.offset, grant, b.Day); err != nil {
		return a, hashFreshProgressFailure(ctx, err)
	}
	if err = writeFreshHashBudget(ctx, tx, job.ID, b); err != nil {
		return a, hashFreshProgressFailure(ctx, err)
	}
	if _, err = s.readFreshJob(ctx, tx, job.ID); err != nil {
		return a, err
	}
	var admission func() error
	if hooks.pacing != nil {
		admission = func() error { return hooks.pacing.preflight(ctx, left) }
	}
	if err = s.commitFreshProgress(ctx, tx, job, nil, nil, hooks.reserveCommit, "reservation", admission); err != nil {
		return a, err
	}
	return a, nil
}

func (s *HashStore) settleFreshHashAttempt(ctx, readCtx context.Context, cancelCause context.CancelCauseFunc, job SavedFreshJob, w hashStoredWork, a hashStoredAttempt, next fullHashCheckpoint, status, code string, usage FileReadUsage, hooks hashFreshRunHooks) (int64, bool, error) {
	fail := func() (int64, bool, error) { s.poisoned = true; return w.offset, false, ErrHashRecoveryRequired }
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fail()
	}
	defer tx.Rollback()
	current, err := s.readFreshJob(ctx, tx, job.ID)
	if err != nil || current.ReadConsent == nil || current.ReadConsent.ID != job.ReadConsent.ID || !equalHashFreshJobRequests(current.Record.Request, s.freshJobRequest.report) {
		return fail()
	}
	if hooks.beforeSettleCommit != nil {
		hooks.beforeSettleCommit()
	}
	now := hashReadPacingSettlementTime(hooks.pacing, readCtx, s.now().UTC())
	if errors.Is(context.Cause(readCtx), ErrHashReadExpired) {
		now = hashReadMaxTime(now, current.ReadConsent.Approval.ExpiresAt)
	}
	guardErr := s.observeHashFreshReadConsent(ctx, tx, current.ReadConsent, now)
	if guardErr != nil && !errors.Is(guardErr, ErrHashReadExpired) && !errors.Is(guardErr, ErrHashReadClockRollback) && !errors.Is(guardErr, ErrHashReadRevoked) {
		return fail()
	}
	if guardErr != nil {
		cancelCause(guardErr)
	}
	vetoed := readCtx.Err() != nil || guardErr != nil
	if vetoed {
		next, status, code = w.checkpoint, "pending", ""
	}
	blob, err := encodeFreshHashCheckpoint(job, w.id, w.sequence+1, next)
	if err != nil {
		return fail()
	}
	durable := next.offset
	if !next.complete {
		durable -= durable % 64
	}
	r, err := tx.ExecContext(ctx, "UPDATE hash_fresh_attempt SET status='settled',requested_bytes=?,read_bytes=?,elapsed_ns=? WHERE job_id=? AND ordinal=? AND status='reserved' AND nonce=? AND base_sequence=? AND from_offset=?", usage.RequestedBytes, usage.ReadBytes, int64(usage.Elapsed), job.ID, w.id, a.nonce, w.sequence, w.offset)
	if err != nil {
		return fail()
	}
	if n, e := r.RowsAffected(); e != nil || n != 1 {
		return fail()
	}
	r, err = tx.ExecContext(ctx, "UPDATE hash_fresh_progress SET status=?,sequence=sequence+1,checked_offset=?,checkpoint=?,error_code=? WHERE job_id=? AND ordinal=? AND status='running' AND sequence=? AND checked_offset=?", status, durable, blob, code, job.ID, w.id, w.sequence, w.offset)
	if err != nil {
		return fail()
	}
	if n, e := r.RowsAffected(); e != nil || n != 1 {
		return fail()
	}
	b := *current.FreshBudget
	b.TotalRequestedBytes, err = addHashCounter(b.TotalRequestedBytes, usage.RequestedBytes)
	if err == nil {
		b.TotalReadBytes, err = addHashCounter(b.TotalReadBytes, usage.ReadBytes)
	}
	if err == nil && b.Day == a.ReservationDay {
		b.RequestedBytes, err = addHashCounter(b.RequestedBytes, usage.RequestedBytes)
		if err == nil {
			b.ReadBytes, err = addHashCounter(b.ReadBytes, usage.ReadBytes)
		}
	}
	if err != nil || writeFreshHashBudget(ctx, tx, job.ID, b) != nil {
		return fail()
	}
	if _, err = s.readFreshJob(ctx, tx, job.ID); err != nil {
		return fail()
	}
	if err = s.commitFreshProgress(ctx, tx, job, nil, hooks.afterSettleCommit, hooks.settleCommit, "settlement"); err != nil {
		return fail()
	}
	return durable, vetoed, nil
}
