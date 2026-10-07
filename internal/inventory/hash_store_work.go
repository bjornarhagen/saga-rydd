package inventory

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// HashRunResult distinguishes checked live progress from block-aligned durable
// progress and charged allowance. Actual usage never refunds the reservation.
type HashRunResult struct {
	ApprovalID    string           `json:"approval_id,omitempty"`
	Status        string           `json:"status"`
	Code          string           `json:"code,omitempty"`
	WorkID        string           `json:"work_id,omitempty"`
	Progress      FullHashProgress `json:"progress"`
	DurableOffset int64            `json:"durable_offset"`
	ReservedBytes int64            `json:"reserved_bytes"`
	Usage         FileReadUsage    `json:"usage"`
	Budget        *HashBudget      `json:"budget,omitempty"`
}

type hashStoreHooks struct {
	beforeReserveCommit func()
	afterReserve        func()
	beforeSettleCommit  func()
	afterSettleCommit   func()
	file                fileHashHooks
}

// RunNext selects one affordable job from the persisted finite round-robin
// queue. It freshly compares current saved inventory before source reads. The
// full granted allowance is durably charged before reading and never refunded.
// Cancellation settles known usage with the previous checkpoint. An uncertain
// publication stops further dispatch until storage is reopened and recovered.
// This foundation is fixture-tested; it supplies no production read authority.
func (s *HashStore) RunNext(ctx context.Context, source *state.Store, scanner *Scanner, allowance, dailyLimit int64) (HashRunResult, error) {
	return s.runNext(ctx, source, scanner, allowance, dailyLimit, hashStoreHooks{})
}

func (s *HashStore) runNext(ctx context.Context, source *state.Store, scanner *Scanner, allowance, dailyLimit int64, hooks hashStoreHooks) (HashRunResult, error) {
	return s.runHashStep(ctx, source, scanner, allowance, dailyLimit, hooks, "")
}

func (s *HashStore) runHashStep(ctx context.Context, source *state.Store, scanner *Scanner, allowance, dailyLimit int64, hooks hashStoreHooks, approvalID string) (result HashRunResult, retErr error) {
	if allowance < 1 || allowance > FileHashStepByteLimit || dailyLimit < 1 || dailyLimit > 1<<50 {
		return result, errors.New("hash allowance must be 1–1048576 and daily reservation limit 1–1125899906842624 bytes")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return result, err
	}
	defer s.mu.Unlock()
	if s.selectionOnly {
		return result, errors.New("hash selection writers cannot dispatch source reads")
	}
	stop := context.AfterFunc(s.life, cancel)
	defer stop()
	if source == nil || scanner == nil || scanner.closed.Load() {
		return result, errors.New("current source inventory and an open scanner are required")
	}
	if err := s.checkHashStorage(nil); err != nil {
		return result, err
	}
	snapshot, record, err := s.readHashSnapshot(ctx, s.db)
	if err != nil {
		return result, err
	}
	if record == nil {
		return result, errors.New("create an exact hash selection first")
	}
	if approvalID == "" && snapshot.ReadConsent != nil {
		result.Status, result.Code = "refused", "read_consent_required"
		return result, ErrHashReadBinding
	}
	var consent *HashReadConsent
	var cancelConsent context.CancelCauseFunc
	if approvalID != "" {
		observed, e := s.observeHashReadApproval(ctx, record, snapshot.SelectionID, snapshot.Budget, approvalID, s.now().UTC(), nil, nil)
		if e != nil {
			result.Status, result.Code = "refused", hashReadErrorCode(e)
			return result, e
		}
		consent = &observed
		result.ApprovalID = approvalID
		dailyLimit = consent.Approval.DailyReservedByteLimit
		// A remaining duration, rather than an absolute test/fake-clock date,
		// preserves the earlier five-second bound and expires cooperatively.
		remaining := consent.Approval.ExpiresAt.Sub(s.now().UTC())
		expiryCtx, expiryCancel := context.WithTimeoutCause(ctx, max(remaining, time.Duration(0)), ErrHashReadExpired)
		defer expiryCancel()
		ctx, cancelConsent = context.WithCancelCause(expiryCtx)
		defer cancelConsent(context.Canceled)
		defer func() {
			// The expiry timer is an observation even if it fired before a
			// reservation. Latch it without inventing source usage or progress.
			if !errors.Is(context.Cause(ctx), ErrHashReadExpired) || s.poisoned {
				return
			}
			observeCtx, observeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer observeCancel()
			b, e := readHashBudget(observeCtx, s.db)
			if e == nil {
				_, e = s.observeHashReadApproval(observeCtx, record, snapshot.SelectionID, b, approvalID, hashReadMaxTime(s.now().UTC(), consent.Approval.ExpiresAt), nil, nil)
			}
			if e != nil && !errors.Is(e, ErrHashReadExpired) {
				s.poisoned = true
				result.Progress.SHA256 = ""
				result.Status, result.Code = "recovery_required", "publication_uncertain"
				retErr = ErrHashRecoveryRequired
				return
			}
			result.Progress.SHA256 = ""
			result.Status, result.Code = "refused", "read_consent_expired"
			result.Progress.Status, result.Progress.Code = result.Status, result.Code
			retErr = ErrHashReadExpired
		}()
	}
	for _, work := range snapshot.Work {
		if work.Status == "running" {
			s.poisoned = true
			return result, ErrHashRecoveryRequired
		}
	}
	result.Budget = snapshot.Budget
	now := s.now().UTC()
	if now.UnixNano() < 0 || now.Year() < 1970 || now.Year() > 2261 {
		return result, errors.New("hash reservation clock is outside its supported range")
	}
	budget := HashBudget{Day: now.Format(time.DateOnly), MaxNow: now}
	if snapshot.Budget != nil {
		budget = *snapshot.Budget
		if now.Before(budget.MaxNow) || now.Format(time.DateOnly) < budget.Day {
			result.Status, result.Code = "deferred", "clock_rollback"
			return result, ErrHashDeferred
		}
		if now.Format(time.DateOnly) > budget.Day {
			budget.Day = now.Format(time.DateOnly)
			budget.ReservedBytes, budget.RequestedBytes, budget.ReadBytes, budget.UnknownReservedBytes = 0, 0, 0, 0
		}
		budget.MaxNow = now
	}
	rows, err := s.db.QueryContext(ctx, "SELECT id FROM hash_work INDEXED BY hash_work_pending WHERE status='pending' ORDER BY ready_order,id LIMIT 21")
	if err != nil {
		return result, err
	}
	var ids []int
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return result, err
	}
	if len(ids) > state.FileSampleTargetLimit {
		return result, ErrHashStoreCorrupt
	}
	if len(ids) == 0 {
		result.Status = "idle"
		return result, nil
	}
	available := max(dailyLimit-budget.ReservedBytes, int64(0))
	if consent != nil {
		available = min(available, max(consent.Approval.LifetimeReservedByteLimit-budget.TotalReservedBytes, int64(0)))
	}
	var chosen hashStoredWork
	var grant int64
	found := false
	for _, id := range ids {
		if id < 1 || id > len(record.Targets) {
			return result, ErrHashStoreCorrupt
		}
		w, e := readHashWork(ctx, s.db, record, snapshot.SelectionID, id)
		if e != nil {
			return result, e
		}
		remaining := record.Targets[id-1].File.Size - w.offset
		if remaining < 0 {
			return result, ErrHashStoreCorrupt
		}
		g := min(allowance, available, remaining)
		if g < remaining {
			g -= g % 64
		}
		if remaining == 0 || g > 0 {
			chosen, grant, found = w, g, true
			break
		}
	}
	if !found {
		result.Status, result.Code = "deferred", "durable_quantum"
		if available == 0 {
			result.Code = "daily_byte_limit"
			if consent != nil && budget.TotalReservedBytes >= consent.Approval.LifetimeReservedByteLimit {
				result.Code = "lifetime_byte_limit"
			}
		}
		return result, ErrHashDeferred
	}
	target := record.Targets[chosen.id-1]
	if chosen.sequence == math.MaxInt64 {
		return result, ErrHashStoreCorrupt
	}
	if err = s.checkHashStorage(&target); err != nil {
		return result, err
	}
	result.WorkID = strconv.Itoa(chosen.id)
	result.DurableOffset = chosen.offset
	result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
	if err = s.compareHashInventory(ctx, source, target); err != nil {
		if errors.Is(err, ErrHashInventoryChanged) {
			if e := s.invalidateHashWork(ctx, chosen, "saved_inventory_changed"); e != nil {
				return result, e
			}
			result.Status, result.Code = "invalidated", "saved_inventory_changed"
			result.Progress.Status, result.Progress.Code = "invalidated", result.Code
		}
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	attempt, err := s.reserveHashAttempt(ctx, record, snapshot.SelectionID, chosen, budget, grant, hooks.beforeReserveCommit, approvalID)
	if err != nil {
		if approvalID != "" {
			result.Status, result.Code = "refused", hashReadErrorCode(err)
			if errors.Is(err, ErrHashDeferred) {
				result.Status, result.Code = "deferred", attempt.guardCode
			}
		}
		return result, err
	}
	grant = attempt.ReservedBytes
	if consent != nil {
		consent.ClockHighWater = attempt.clockHighWater
	}
	result.ReservedBytes = grant
	if hooks.afterReserve != nil {
		hooks.afterReserve()
	}
	if consent != nil {
		now = s.now().UTC()
		if !now.Before(consent.Approval.ExpiresAt) {
			cancelConsent(ErrHashReadExpired)
		} else if !validHashReadClock(now) || now.Before(consent.ClockHighWater) {
			cancelConsent(ErrHashReadClockRollback)
		}
		readCtx, readCancel := context.WithTimeoutCause(ctx, max(consent.Approval.ExpiresAt.Sub(now), time.Duration(0)), ErrHashReadExpired)
		defer readCancel()
		ctx = readCtx
	}
	var session *FullHashSession
	if err = s.checkHashStorage(&target); err == nil {
		session, err = restoreHashSession(scanner, target, chosen.checkpoint)
	} else {
		err = blocked("hash_storage_changed", "Private hash storage changed or became unavailable. No source bytes were read.")
	}
	var progress FullHashProgress
	var usage FileReadUsage
	next := chosen.checkpoint
	status, code := "pending", ""
	if err == nil {
		progress, usage, err = session.step(ctx, max(grant, int64(1)), hooks.file)
		if err == nil {
			next = session.core.checkpoint
			if next.complete {
				status = "complete"
			}
		} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			status, code = "invalidated", progress.Code
			if code == "" {
				code = "hash_unavailable"
			}
		}
	} else {
		status, code = "invalidated", "hash_state_unsupported"
		var failure liveError
		if errors.As(err, &failure) {
			code = failure.code
		}
		progress = hashHistoricalProgress(target, chosen.checkpoint)
		progress.Status, progress.Code = status, code
	}
	// Close signals the lifetime context synchronously. Observe it here even
	// if its AfterFunc has not yet propagated cancellation to this request.
	if s.life.Err() != nil {
		cancel()
	}
	if consent != nil {
		now = s.now().UTC()
		if !now.Before(consent.Approval.ExpiresAt) {
			cancelConsent(ErrHashReadExpired)
		} else if !validHashReadClock(now) || now.Before(consent.ClockHighWater) {
			cancelConsent(ErrHashReadClockRollback)
		}
	}
	if canceled := ctx.Err(); canceled != nil {
		err = canceled
		next = chosen.checkpoint
		status, code = "pending", ""
		progress = hashHistoricalProgress(target, chosen.checkpoint)
	} else if err == nil {
		if e := s.compareHashInventory(ctx, source, target); e != nil {
			err = e
			next = chosen.checkpoint
			progress = hashHistoricalProgress(target, chosen.checkpoint)
			if errors.Is(e, ErrHashInventoryChanged) {
				status, code = "invalidated", "saved_inventory_changed"
				progress.Status, progress.Code = status, code
			} else {
				status, code = "pending", ""
			}
		}
	}
	result.Progress, result.Usage = progress, usage
	if usage.RequestedBytes < 0 || usage.ReadBytes < 0 || usage.ReadBytes > usage.RequestedBytes || usage.RequestedBytes > grant || usage.Elapsed < 0 {
		s.poisoned = true
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Status, result.Code = "recovery_required", "publication_uncertain"
		return result, ErrHashRecoveryRequired
	}
	settleCtx, settleCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer settleCancel()
	durable, vetoed, e := s.settleHashAttempt(settleCtx, ctx, record, snapshot.SelectionID, chosen, attempt, next, status, code, usage, hooks, approvalID, cancelConsent)
	if e != nil {
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
		result.Status, result.Code = "recovery_required", "publication_uncertain"
		return result, e
	}
	if vetoed {
		err = ctx.Err()
		status, code = "pending", ""
		result.Progress = hashHistoricalProgress(target, chosen.checkpoint)
	}
	result.DurableOffset = durable
	result.Status = status
	if status == "complete" {
		result.Status = "hash_observed"
	}
	result.Code = code
	settled, _, e := s.readHashSnapshot(settleCtx, s.db)
	if e != nil {
		result.Progress.SHA256 = ""
		return result, e
	}
	result.Budget = settled.Budget
	if canceled := ctx.Err(); canceled != nil {
		// Publication may already be durable. Keep its offset and accounting,
		// but never return a live digest for a canceled request. A separate
		// saved-only report can read the committed historical observation.
		result.Status, result.Code = "canceled", "request_canceled"
		result.Progress.Status, result.Progress.Code = "canceled", result.Code
		result.Progress.Source = "saved_hash_observations"
		result.Progress.SHA256 = ""
		result.Progress.Message = "The request was canceled. Any committed progress remains in the saved hashing record."
		if consent != nil && (errors.Is(context.Cause(ctx), ErrHashReadExpired) || errors.Is(context.Cause(ctx), ErrHashReadClockRollback)) {
			cause := context.Cause(ctx)
			result.Status, result.Code = "refused", hashReadErrorCode(cause)
			result.Progress.Status, result.Progress.Code = result.Status, result.Code
			return result, cause
		}
		return result, canceled
	}
	return result, err
}

func hashHistoricalProgress(t SavedFileTarget, c fullHashCheckpoint) FullHashProgress {
	return FullHashProgress{Status: "partial", Source: "live_full_file_hash", Contract: FileHashContract, InventoryID: t.InventoryID, RootID: t.Root.ID, FileID: t.File.ID, PathBytes: append([]byte(nil), t.File.PathBytes...), LogicalBytes: t.File.Size, Offset: c.offset, CheckedAt: c.checkedAt, Message: "The previous checked prefix is retained. This is not current-content verification."}
}

func (s *HashStore) compareHashInventory(ctx context.Context, source *state.Store, target SavedFileTarget) error {
	current, err := source.PrepareFileSampleSelection(ctx, target.InventoryID, []state.SameSizeFile{target.File})
	if err != nil {
		if errors.Is(err, state.ErrFileSampleEvidence) || errors.Is(err, state.ErrFileSampleSchema) || errors.Is(err, state.ErrFileSampleSelection) {
			return ErrHashInventoryChanged
		}
		return err
	}
	if len(current) != 1 {
		return ErrHashInventoryChanged
	}
	want, e := hashTargetDigest(target)
	if e != nil {
		return ErrHashStoreCorrupt
	}
	got, e := hashTargetDigest(current[0])
	if e != nil || got != want {
		return ErrHashInventoryChanged
	}
	return nil
}

func (s *HashStore) invalidateHashWork(ctx context.Context, w hashStoredWork, code string) error {
	r, err := s.db.ExecContext(ctx, "UPDATE hash_work SET status='invalidated',error_code=? WHERE id=? AND status='pending' AND sequence=?", code, w.id, w.sequence)
	if err != nil {
		s.poisoned = true
		return ErrHashRecoveryRequired
	}
	n, err := r.RowsAffected()
	if err != nil || n != 1 {
		s.poisoned = true
		return ErrHashRecoveryRequired
	}
	return nil
}

func addHashCounter(value, delta int64) (int64, error) {
	if value < 0 || delta < 0 || value > math.MaxInt64-delta {
		return 0, ErrHashStoreCorrupt
	}
	return value + delta, nil
}

func writeHashBudget(ctx context.Context, tx *sql.Tx, b HashBudget) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO hash_budget VALUES(1,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET day=excluded.day,max_now_ns=excluded.max_now_ns,reserved_bytes=excluded.reserved_bytes,requested_bytes=excluded.requested_bytes,read_bytes=excluded.read_bytes,unknown_reserved_bytes=excluded.unknown_reserved_bytes,total_reserved_bytes=excluded.total_reserved_bytes,total_requested_bytes=excluded.total_requested_bytes,total_read_bytes=excluded.total_read_bytes,total_unknown_reserved_bytes=excluded.total_unknown_reserved_bytes`, b.Day, b.MaxNow.UnixNano(), b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes, b.TotalReservedBytes, b.TotalRequestedBytes, b.TotalReadBytes, b.TotalUnknownReservedBytes)
	return err
}

func (s *HashStore) reserveHashAttempt(ctx context.Context, record *hashSelectionRecord, selectionID string, w hashStoredWork, b HashBudget, grant int64, beforeCommit func(), approvalID string) (hashStoredAttempt, error) {
	a := hashStoredAttempt{HashAttempt: HashAttempt{Status: "reserved", ReservationDay: b.Day, ReservedBytes: grant}, sequence: w.sequence, offset: w.offset}
	token, err := hashStoreToken()
	if err != nil {
		return a, err
	}
	a.nonce = token
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	if approvalID != "" {
		// This seam remains before any charge/work mutation. The real clock
		// and ledger are read after it, in the same reservation transaction.
		if beforeCommit != nil {
			beforeCommit()
			beforeCommit = nil
		}
		current, e := readHashBudget(ctx, tx)
		if e != nil {
			return a, e
		}
		now := s.now().UTC()
		c, guardErr := s.observeHashReadConsent(ctx, tx, record, selectionID, current, approvalID, now)
		if c == nil {
			return a, guardErr
		}
		if guardErr != nil {
			if e = tx.Commit(); e != nil {
				s.poisoned = true
				return a, ErrHashRecoveryRequired
			}
			return a, guardErr
		}
		a.clockHighWater = c.ClockHighWater
		b = HashBudget{Day: now.Format(time.DateOnly), MaxNow: now}
		if current != nil {
			b = *current
			if b.Day < now.Format(time.DateOnly) {
				b.Day = now.Format(time.DateOnly)
				b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes = 0, 0, 0, 0
			}
			b.MaxNow = now
		}
		available := min(max(c.Approval.DailyReservedByteLimit-b.ReservedBytes, int64(0)), max(c.Approval.LifetimeReservedByteLimit-b.TotalReservedBytes, int64(0)))
		remaining := record.Targets[w.id-1].File.Size - w.offset
		grant = min(grant, available, remaining, FileHashStepByteLimit)
		if grant < remaining {
			grant -= grant % 64
		}
		if grant == 0 && remaining > 0 {
			a.guardCode = "durable_quantum"
			if b.ReservedBytes >= c.Approval.DailyReservedByteLimit {
				a.guardCode = "daily_byte_limit"
			}
			if b.TotalReservedBytes >= c.Approval.LifetimeReservedByteLimit {
				a.guardCode = "lifetime_byte_limit"
			}
			if e = tx.Commit(); e != nil {
				s.poisoned = true
				return a, ErrHashRecoveryRequired
			}
			return a, ErrHashDeferred
		}
		a.ReservedBytes, a.ReservationDay = grant, b.Day
	}
	b.ReservedBytes, err = addHashCounter(b.ReservedBytes, grant)
	if err == nil {
		b.TotalReservedBytes, err = addHashCounter(b.TotalReservedBytes, grant)
	}
	if err != nil {
		return a, err
	}
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT next_order FROM hash_meta WHERE id=1 AND store_id=?", record.StoreID).Scan(&next); err != nil {
		return a, err
	}
	if next == math.MaxInt64 {
		return a, ErrHashStoreCorrupt
	}
	next++
	var selected string
	if err = tx.QueryRowContext(ctx, "SELECT selection_id FROM hash_selection WHERE id=1").Scan(&selected); err != nil || selected != selectionID {
		return a, ErrHashStoreCorrupt
	}
	r, err := tx.ExecContext(ctx, "UPDATE hash_work SET status='running',ready_order=? WHERE id=? AND status='pending' AND sequence=? AND checked_offset=?", next, w.id, w.sequence, w.offset)
	if err != nil {
		return a, err
	}
	n, err := r.RowsAffected()
	if err != nil || n != 1 {
		return a, ErrHashStoreCorrupt
	}
	if _, err = tx.ExecContext(ctx, "UPDATE hash_meta SET next_order=? WHERE id=1", next); err != nil {
		return a, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO hash_attempt VALUES(?,?,?,?,?,?,'reserved',NULL,NULL,NULL) ON CONFLICT(work_id) DO UPDATE SET nonce=excluded.nonce,base_sequence=excluded.base_sequence,from_offset=excluded.from_offset,grant_bytes=excluded.grant_bytes,reservation_day=excluded.reservation_day,status='reserved',requested_bytes=NULL,read_bytes=NULL,elapsed_ns=NULL`, w.id, a.nonce, w.sequence, w.offset, grant, b.Day); err != nil {
		return a, err
	}
	if err = writeHashBudget(ctx, tx, b); err != nil {
		return a, err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return a, err
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		return a, ErrHashRecoveryRequired
	}
	return a, nil
}

func (s *HashStore) settleHashAttempt(ctx, callerCtx context.Context, record *hashSelectionRecord, selectionID string, w hashStoredWork, a hashStoredAttempt, next fullHashCheckpoint, status, code string, usage FileReadUsage, hooks hashStoreHooks, approvalID string, cancelConsent context.CancelCauseFunc) (int64, bool, error) {
	if w.sequence == math.MaxInt64 {
		s.poisoned = true
		return w.offset, false, ErrHashRecoveryRequired
	}
	target := record.Targets[w.id-1]
	digest, err := hashTargetDigest(target)
	if err != nil {
		s.poisoned = true
		return w.offset, false, ErrHashRecoveryRequired
	}
	blob, err := encodeHashCheckpoint(hashCheckpointBinding{StoreID: record.StoreID, SelectionID: selectionID, WorkID: strconv.Itoa(w.id), Sequence: w.sequence + 1, TargetDigest: digest}, target, next)
	if err != nil {
		s.poisoned = true
		return w.offset, false, ErrHashRecoveryRequired
	}
	durable := next.offset
	if !next.complete {
		durable -= durable % 64
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.poisoned = true
		return w.offset, false, ErrHashRecoveryRequired
	}
	defer tx.Rollback()
	fail := func() (int64, bool, error) { s.poisoned = true; return w.offset, false, ErrHashRecoveryRequired }
	r, err := tx.ExecContext(ctx, `UPDATE hash_attempt SET status='settled',requested_bytes=?,read_bytes=?,elapsed_ns=? WHERE work_id=? AND status='reserved' AND nonce=? AND base_sequence=? AND from_offset=?`, usage.RequestedBytes, usage.ReadBytes, int64(usage.Elapsed), w.id, a.nonce, w.sequence, w.offset)
	if err != nil {
		return fail()
	}
	n, err := r.RowsAffected()
	if err != nil || n != 1 {
		return fail()
	}
	r, err = tx.ExecContext(ctx, `UPDATE hash_work SET status=?,sequence=sequence+1,checked_offset=?,checkpoint=?,error_code=? WHERE id=? AND status='running' AND sequence=? AND checked_offset=?`, status, durable, blob, code, w.id, w.sequence, w.offset)
	if err != nil {
		return fail()
	}
	n, err = r.RowsAffected()
	if err != nil || n != 1 {
		return fail()
	}
	b, err := readHashBudget(ctx, tx)
	if err != nil || b == nil {
		return fail()
	}
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
	if err != nil || writeHashBudget(ctx, tx, *b) != nil {
		return fail()
	}
	if hooks.beforeSettleCommit != nil {
		hooks.beforeSettleCommit()
	}
	if approvalID != "" {
		now := s.now().UTC()
		if errors.Is(context.Cause(callerCtx), ErrHashReadExpired) {
			c, e := s.readHashReadConsent(ctx, tx, record, selectionID, b)
			if e != nil || c == nil {
				return fail()
			}
			if now.Before(c.Approval.ExpiresAt) {
				now = c.Approval.ExpiresAt
			}
		}
		_, guardErr := s.observeHashReadConsent(ctx, tx, record, selectionID, b, approvalID, now)
		if guardErr != nil && !errors.Is(guardErr, ErrHashReadExpired) && !errors.Is(guardErr, ErrHashReadClockRollback) {
			return fail()
		}
		if guardErr != nil && cancelConsent != nil {
			cancelConsent(guardErr)
		}
	}
	vetoed := callerCtx.Err() != nil
	if vetoed {
		oldBlob, e := encodeHashCheckpoint(hashCheckpointBinding{StoreID: record.StoreID, SelectionID: selectionID, WorkID: strconv.Itoa(w.id), Sequence: w.sequence + 1, TargetDigest: digest}, target, w.checkpoint)
		if e != nil {
			return fail()
		}
		changed, e := tx.ExecContext(ctx, "UPDATE hash_work SET status='pending',checked_offset=?,checkpoint=?,error_code='' WHERE id=? AND status=? AND sequence=?", w.offset, oldBlob, w.id, status, w.sequence+1)
		if e != nil {
			return fail()
		}
		count, e := changed.RowsAffected()
		if e != nil || count != 1 {
			return fail()
		}
		durable = w.offset
	}
	if err = tx.Commit(); err != nil {
		return fail()
	}
	if hooks.afterSettleCommit != nil {
		hooks.afterSettleCommit()
	}
	return durable, vetoed, nil
}

// Recovery requires the lifetime writer lock. It never opens source paths or
// refunds unknown consumption; the reserved job was already rotated at claim.
func (s *HashStore) recoverHashWork(ctx context.Context) error {
	if s.readOnly || s.lock == nil {
		return errors.New("hash recovery requires the exclusive writer lock")
	}
	snapshot, record, err := s.readHashSnapshot(ctx, s.db)
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}
	for _, saved := range snapshot.Work {
		if saved.Status != "running" {
			continue
		}
		w, e := readHashWork(ctx, s.db, record, snapshot.SelectionID, mustHashWorkID(saved.ID))
		if e != nil {
			return e
		}
		a := w.attempt
		if a == nil || a.Status != "reserved" || w.sequence == math.MaxInt64 {
			return ErrHashStoreCorrupt
		}
		target := record.Targets[w.id-1]
		digest, e := hashTargetDigest(target)
		if e != nil {
			return e
		}
		blob, e := encodeHashCheckpoint(hashCheckpointBinding{StoreID: record.StoreID, SelectionID: snapshot.SelectionID, WorkID: saved.ID, Sequence: w.sequence + 1, TargetDigest: digest}, target, w.checkpoint)
		if e != nil {
			return e
		}
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		b, e := readHashBudget(ctx, tx)
		if e == nil && b != nil {
			b.TotalUnknownReservedBytes, e = addHashCounter(b.TotalUnknownReservedBytes, a.ReservedBytes)
			if e == nil && b.Day == a.ReservationDay {
				b.UnknownReservedBytes, e = addHashCounter(b.UnknownReservedBytes, a.ReservedBytes)
			}
		} else if e == nil {
			e = ErrHashStoreCorrupt
		}
		if e == nil {
			var changed sql.Result
			changed, e = tx.ExecContext(ctx, "UPDATE hash_attempt SET status='interrupted_unknown' WHERE work_id=? AND status='reserved' AND nonce=?", w.id, a.nonce)
			if e == nil {
				var count int64
				count, e = changed.RowsAffected()
				if e == nil && count != 1 {
					e = ErrHashStoreCorrupt
				}
			}
		}
		if e == nil {
			var changed sql.Result
			changed, e = tx.ExecContext(ctx, "UPDATE hash_work SET status='pending',sequence=sequence+1,checkpoint=? WHERE id=? AND status='running' AND sequence=?", blob, w.id, w.sequence)
			if e == nil {
				var count int64
				count, e = changed.RowsAffected()
				if e == nil && count != 1 {
					e = ErrHashStoreCorrupt
				}
			}
		}
		if e == nil {
			e = writeHashBudget(ctx, tx, *b)
		}
		if e == nil {
			e = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if e != nil {
			return e
		}
	}
	return nil
}

func mustHashWorkID(id string) int { value, _ := strconv.Atoi(id); return value }
