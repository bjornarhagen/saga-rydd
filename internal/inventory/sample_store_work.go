package inventory

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Usage survives every ordinary post-reservation error. UsageDurable describes
// known settlement, not whether an ephemeral sample report may be returned.
type SampleRunResult struct {
	StoreID       string           `json:"store_id,omitempty"`
	BatchID       string           `json:"batch_id,omitempty"`
	AttemptID     string           `json:"attempt_id,omitempty"`
	Status        string           `json:"status"`
	Code          string           `json:"code,omitempty"`
	ReservedBytes int64            `json:"reserved_bytes"`
	Usage         FileReadUsage    `json:"usage"`
	UsageKnown    bool             `json:"usage_known"`
	UsageDurable  bool             `json:"usage_durable"`
	Budget        *SampleBudget    `json:"budget,omitempty"`
	Report        FileSampleReport `json:"report"`
}

// All hooks are private deterministic generated-fixture seams. Production
// library entry points supply none; callers never submit usage or a grant.
type sampleStoreHooks struct {
	beforeReserveCommit func()
	afterReserve        func()
	beforeSettleCommit  func()
	afterSettleCommit   func()
	beforeRecoverCommit func()
	reserveCommit       func(*sql.Tx) error
	settleCommit        func(*sql.Tx) error
	recoverCommit       func(*sql.Tx) error
	sample              fileSampleHooks
}

func sampleBudgetProjection(a *SampleAttempt, high time.Time) SampleBudget {
	b := SampleBudget{Day: high.Format(time.DateOnly), MaxNow: high}
	if a == nil {
		return b
	}
	b.TotalReservedBytes = a.ReservedBytes
	switch a.Status {
	case "reserved":
		b.TotalOutstandingReservedBytes = a.ReservedBytes
	case "interrupted_unknown":
		b.TotalUnknownReservedBytes = a.ReservedBytes
	case "settled":
		if a.RequestedBytes != nil {
			b.TotalRequestedBytes = *a.RequestedBytes
		}
		if a.ReadBytes != nil {
			b.TotalReadBytes = *a.ReadBytes
		}
	}
	if a.ReservationDay == b.Day {
		b.ReservedBytes = b.TotalReservedBytes
		b.RequestedBytes = b.TotalRequestedBytes
		b.ReadBytes = b.TotalReadBytes
		b.UnknownReservedBytes = b.TotalUnknownReservedBytes
		b.OutstandingReservedBytes = b.TotalOutstandingReservedBytes
	}
	return b
}

func equalSampleBudget(a, b SampleBudget) bool {
	a.MaxNow = time.Time{}
	b.MaxNow = time.Time{}
	return a == b
}

func readSampleBudget(ctx context.Context, db hashQuery) (SampleBudget, error) {
	var b SampleBudget
	var high int64
	var valid bool
	columns := []string{"max_now_ns", "reserved_bytes", "requested_bytes", "read_bytes", "unknown_bytes", "outstanding_bytes", "total_reserved_bytes", "total_requested_bytes", "total_read_bytes", "total_unknown_bytes", "total_outstanding_bytes"}
	fields := []string{"substr(CAST(day AS BLOB),1,11)"}
	guards := []string{"typeof(id)='integer' AND id=1", "typeof(day)='text'"}
	for _, c := range columns {
		fields = append(fields, "CASE WHEN typeof("+c+")='integer' THEN "+c+" ELSE -1 END")
		guards = append(guards, "typeof("+c+")='integer'")
	}
	err := db.QueryRowContext(ctx, "SELECT "+strings.Join(fields, ",")+","+strings.Join(guards, " AND ")+" FROM sample_budget").Scan(&b.Day, &high, &b.ReservedBytes, &b.RequestedBytes, &b.ReadBytes, &b.UnknownReservedBytes, &b.OutstandingReservedBytes, &b.TotalReservedBytes, &b.TotalRequestedBytes, &b.TotalReadBytes, &b.TotalUnknownReservedBytes, &b.TotalOutstandingReservedBytes, &valid)
	if err != nil {
		return b, err
	}
	b.MaxNow = time.Unix(0, high).UTC()
	if !valid || high < 0 || !validHashReadClock(b.MaxNow) || b.Day != b.MaxNow.Format(time.DateOnly) {
		return b, ErrSampleStoreCorrupt
	}
	for _, n := range []int64{b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes, b.OutstandingReservedBytes, b.TotalReservedBytes, b.TotalRequestedBytes, b.TotalReadBytes, b.TotalUnknownReservedBytes, b.TotalOutstandingReservedBytes} {
		if n < 0 || n > FileSampleCallByteLimit {
			return b, ErrSampleStoreCorrupt
		}
	}
	return b, nil
}

func writeSampleBudget(ctx context.Context, tx *sql.Tx, b SampleBudget) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO sample_budget VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
 day=excluded.day,max_now_ns=excluded.max_now_ns,reserved_bytes=excluded.reserved_bytes,requested_bytes=excluded.requested_bytes,read_bytes=excluded.read_bytes,
 unknown_bytes=excluded.unknown_bytes,outstanding_bytes=excluded.outstanding_bytes,total_reserved_bytes=excluded.total_reserved_bytes,
 total_requested_bytes=excluded.total_requested_bytes,total_read_bytes=excluded.total_read_bytes,total_unknown_bytes=excluded.total_unknown_bytes,total_outstanding_bytes=excluded.total_outstanding_bytes`,
		b.Day, b.MaxNow.UnixNano(), b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes, b.OutstandingReservedBytes, b.TotalReservedBytes, b.TotalRequestedBytes, b.TotalReadBytes, b.TotalUnknownReservedBytes, b.TotalOutstandingReservedBytes)
	return err
}

func sampleOutcome(code string) bool {
	switch code {
	case "", "observed", "blocked", "request_canceled", "sample_failed", "saved_inventory_changed", "storage_unavailable", "clock_rollback", "interrupted_unknown":
		return true
	}
	return false
}

func readSampleAttempt(ctx context.Context, db hashQuery, storeID, batchID string, grant int64) (*SampleAttempt, error) {
	a := &SampleAttempt{}
	var store, batch string
	var high int64
	var requested, read, elapsed sql.NullInt64
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT substr(CAST(store_id AS BLOB),1,65),substr(CAST(batch_id AS BLOB),1,65),substr(CAST(nonce AS BLOB),1,65),substr(CAST(reservation_day AS BLOB),1,11),
 CASE WHEN typeof(reserved_at_ns)='integer' THEN reserved_at_ns ELSE -1 END,CASE WHEN typeof(grant_bytes)='integer' THEN grant_bytes ELSE -1 END,
 substr(CAST(status AS BLOB),1,20),COALESCE(CAST(substr(CAST(outcome AS BLOB),1,65) AS TEXT),''),
 CASE WHEN typeof(requested_bytes) IN ('integer','null') THEN requested_bytes ELSE NULL END,
 CASE WHEN typeof(read_bytes) IN ('integer','null') THEN read_bytes ELSE NULL END,
 CASE WHEN typeof(elapsed_ns) IN ('integer','null') THEN elapsed_ns ELSE NULL END,
 typeof(id)='integer' AND id=1 AND typeof(store_id)='text' AND typeof(batch_id)='text' AND typeof(nonce)='text' AND typeof(reservation_day)='text'
 AND typeof(reserved_at_ns)='integer' AND typeof(grant_bytes)='integer' AND typeof(status)='text' AND typeof(outcome)='text'
 AND typeof(requested_bytes) IN ('integer','null') AND typeof(read_bytes) IN ('integer','null') AND typeof(elapsed_ns) IN ('integer','null') FROM sample_attempt`).Scan(&store, &batch, &a.ID, &a.ReservationDay, &high, &a.ReservedBytes, &a.Status, &a.Code, &requested, &read, &elapsed, &valid)
	if err != nil {
		return nil, err
	}
	a.ReservedAt = time.Unix(0, high).UTC()
	if !valid || store != storeID || batch != batchID || !sampleHex(a.ID) || high < 0 || !validHashReadClock(a.ReservedAt) || a.ReservationDay != a.ReservedAt.Format(time.DateOnly) || a.ReservedBytes != grant || grant < 1 || grant > FileSampleCallByteLimit || !sampleOutcome(a.Code) {
		return nil, ErrSampleStoreCorrupt
	}
	switch a.Status {
	case "reserved":
		if a.Code != "" || requested.Valid || read.Valid || elapsed.Valid {
			return nil, ErrSampleStoreCorrupt
		}
	case "interrupted_unknown":
		if a.Code != "interrupted_unknown" || requested.Valid || read.Valid || elapsed.Valid {
			return nil, ErrSampleStoreCorrupt
		}
	case "settled":
		if a.Code == "" || a.Code == "interrupted_unknown" || !requested.Valid || !read.Valid || !elapsed.Valid || requested.Int64 < 0 || read.Int64 < 0 || read.Int64 > requested.Int64 || requested.Int64 > grant || elapsed.Int64 < 0 {
			return nil, ErrSampleStoreCorrupt
		}
		a.RequestedBytes = &requested.Int64
		a.ReadBytes = &read.Int64
		a.ElapsedNS = &elapsed.Int64
	default:
		return nil, ErrSampleStoreCorrupt
	}
	return a, nil
}

func (s *SampleStore) compareSampleInventory(ctx context.Context, source *state.Store, r *sampleBatchRecord) error {
	if source == nil {
		return ErrSampleInventoryChanged
	}
	expected := make([]state.SameSizeFile, len(r.Targets))
	for i, t := range r.Targets {
		expected[i] = t.File
	}
	current, err := source.PrepareFileSampleSelection(ctx, r.InventoryID, expected)
	if err != nil {
		if errors.Is(err, state.ErrFileSampleEvidence) || errors.Is(err, state.ErrFileSampleSchema) || errors.Is(err, state.ErrFileSampleSelection) || errors.Is(err, state.ErrFileSampleLimit) {
			return errors.Join(ErrSampleInventoryChanged, err)
		}
		return err
	}
	if len(current) != len(r.Targets) {
		return ErrSampleInventoryChanged
	}
	for i, t := range current {
		want, e := sampleTargetDigest(r.Targets[i])
		if e != nil {
			return e
		}
		got, e := sampleTargetDigest(t)
		if e != nil || got != want {
			return errors.Join(ErrSampleInventoryChanged, e)
		}
	}
	return nil
}

func sampleCommit(tx *sql.Tx, seam func(*sql.Tx) error) error {
	if seam != nil {
		return seam(tx)
	}
	return tx.Commit()
}

func (s *SampleStore) reserveSample(ctx context.Context, batchID string, hooks sampleStoreHooks) (*SampleAttempt, *sampleBatchRecord, *SampleBudget, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback()
	saved, r, err := s.readSampleSnapshot(ctx, tx)
	if err != nil {
		return nil, nil, nil, err
	}
	if r == nil || saved.BatchID != batchID {
		return nil, nil, nil, ErrSampleBatchMissing
	}
	if saved.Attempt != nil {
		if saved.Attempt.Status == "reserved" {
			return nil, nil, nil, ErrSampleRecoveryRequired
		}
		return nil, nil, nil, ErrSampleOneShot
	}
	if hooks.beforeReserveCommit != nil {
		hooks.beforeReserveCommit()
	}
	now := s.now().UTC()
	if !validHashReadClock(now) || now.Before(saved.Budget.MaxNow) {
		return nil, nil, nil, ErrSampleClockRollback
	}
	b := sampleBudgetProjection(nil, now)
	if r.Total > r.Policy.DailyReservedByteLimit || r.Total > r.Policy.LifetimeReservedByteLimit {
		if err = writeSampleBudget(ctx, tx, b); err != nil {
			return nil, nil, nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		if err = s.checkSampleStorage(nil); err != nil {
			return nil, nil, nil, err
		}
		if err = sampleCommit(tx, hooks.reserveCommit); err != nil {
			s.poisoned = true
			return nil, nil, nil, errors.Join(ErrSampleRecoveryRequired, err)
		}
		return nil, nil, &b, ErrSampleBudget
	}
	nonce, err := hashStoreToken()
	if err != nil {
		return nil, nil, nil, err
	}
	a := &SampleAttempt{ID: nonce, ReservationDay: now.Format(time.DateOnly), ReservedAt: now, ReservedBytes: r.Total, Status: "reserved"}
	if _, err = tx.ExecContext(ctx, "INSERT INTO sample_attempt VALUES(1,?,?,?,?,?,?,'reserved','',NULL,NULL,NULL)", saved.StoreID, batchID, nonce, a.ReservationDay, now.UnixNano(), r.Total); err != nil {
		return nil, nil, nil, err
	}
	b = sampleBudgetProjection(a, now)
	if err = writeSampleBudget(ctx, tx, b); err != nil {
		return nil, nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return nil, nil, nil, err
	}
	if err = sampleCommit(tx, hooks.reserveCommit); err != nil {
		s.poisoned = true
		return nil, nil, nil, errors.Join(ErrSampleRecoveryRequired, err)
	}
	confirmed, err := s.readSampleSnapshotPublic(ctx)
	if err != nil || confirmed.BatchID != batchID || confirmed.Attempt == nil || confirmed.Attempt.ID != a.ID || confirmed.Attempt.Status != "reserved" || confirmed.Attempt.ReservedBytes != a.ReservedBytes || confirmed.Budget == nil || !equalSampleBudget(*confirmed.Budget, b) {
		s.poisoned = true
		return nil, nil, nil, errors.Join(ErrSampleRecoveryRequired, ErrSampleStoreCorrupt, err)
	}
	return confirmed.Attempt, r, confirmed.Budget, nil
}

// RunSamples performs exactly one fully reserved observation of generated
// fixtures. The fixture-built core supplies no production sampling authority.
func (s *SampleStore) RunSamples(ctx context.Context, batchID string, source *state.Store, scanner *Scanner) (SampleRunResult, error) {
	return s.runSamples(ctx, batchID, source, scanner, sampleStoreHooks{})
}

func (s *SampleStore) runSamples(ctx context.Context, batchID string, source *state.Store, scanner *Scanner, hooks sampleStoreHooks) (result SampleRunResult, err error) {
	result.Status = "refused"
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	defer func() {
		if errors.Is(err, ErrSampleRecoveryRequired) && result.Status != "recovery_required" {
			result.Status = "recovery_required"
			result.Code = "publication_uncertain"
		}
		if e := ctx.Err(); e != nil {
			err = errors.Join(err, e)
			if result.Status != "recovery_required" {
				result.Code = "request_canceled"
			}
		}
		if s != nil && s.life != nil && s.life.Err() != nil {
			err = errors.Join(err, context.Canceled)
			if result.Status != "recovery_required" {
				result.Code = "request_canceled"
			}
		}
		if err != nil {
			result.Report = FileSampleReport{}
		}
	}()
	if !sampleHex(batchID) {
		return result, ErrSampleBatchMissing
	}
	if err = s.acquireSample(ctx, true); err != nil {
		return result, err
	}
	defer s.mu.Unlock()
	stop := context.AfterFunc(s.life, cancel)
	defer stop()
	if source == nil || scanner == nil || scanner.closed.Load() {
		return result, errors.New("current saved inventory and an open scanner are required")
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return result, err
	}
	saved, r, err := s.readSampleSnapshot(ctx, s.db)
	if err != nil {
		return result, err
	}
	result.StoreID = saved.StoreID
	result.BatchID = batchID
	result.Budget = saved.Budget
	if r == nil || saved.BatchID != batchID {
		return result, ErrSampleBatchMissing
	}
	if saved.Attempt != nil {
		if saved.Attempt.Status == "reserved" {
			return result, ErrSampleRecoveryRequired
		}
		return result, ErrSampleOneShot
	}
	if err = s.compareSampleInventory(ctx, source, r); err != nil {
		return result, err
	}
	a, r, b, err := s.reserveSample(ctx, batchID, hooks)
	if b != nil {
		result.Budget = b
	}
	if err != nil {
		return result, err
	}
	result.AttemptID = a.ID
	result.ReservedBytes = a.ReservedBytes
	if hooks.afterReserve != nil {
		hooks.afterReserve()
	}
	var report FileSampleReport
	code := "observed"
	if err = s.checkSampleStorage(nil); err == nil {
		report, result.Usage, err = scanner.observeFileSamplesWithUsage(ctx, r.Targets, hooks.sample)
	} else {
		code = "storage_unavailable"
	}
	// An acknowledged reservation followed by zero sampler calls has known zero
	// usage. Before reservation, UsageKnown remains false and no attempt exists.
	result.UsageKnown = true
	if s.life.Err() != nil {
		cancel()
	}
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
		code = "request_canceled"
	} else if err != nil && code != "storage_unavailable" {
		code = "sample_failed"
	} else if report.Status == "blocked" {
		code = "blocked"
	}
	if err == nil {
		if e := s.compareSampleInventory(ctx, source, r); e != nil {
			err = e
			code = "saved_inventory_changed"
		}
	}
	if e := s.checkSampleStorage(nil); e != nil {
		err = errors.Join(err, e)
		code = "storage_unavailable"
	}
	usage := result.Usage
	if usage.RequestedBytes < 0 || usage.ReadBytes < 0 || usage.ReadBytes > usage.RequestedBytes || usage.RequestedBytes > a.ReservedBytes || usage.Elapsed < 0 {
		s.poisoned = true
		result.Status = "recovery_required"
		result.Code = "publication_uncertain"
		return result, errors.Join(ErrSampleRecoveryRequired, err)
	}
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cleanupCancel()
	clockErr, settleErr := s.settleSample(cleanup, ctx, batchID, a, usage, code, hooks)
	if settleErr != nil {
		s.poisoned = true
		result.Status = "recovery_required"
		result.Code = "publication_uncertain"
		return result, errors.Join(ErrSampleRecoveryRequired, err, settleErr)
	}
	if clockErr != nil {
		err = errors.Join(err, clockErr)
		code = "clock_rollback"
	}
	settled, _, e := s.readSampleSnapshot(cleanup, s.db)
	storageErr := s.checkSampleStorage(nil)
	knownMatches := settled.BatchID == batchID && settled.Attempt != nil && settled.Attempt.ID == a.ID && settled.Attempt.Status == "settled" && settled.Attempt.ReservedBytes == a.ReservedBytes && settled.Attempt.RequestedBytes != nil && *settled.Attempt.RequestedBytes == usage.RequestedBytes && settled.Attempt.ReadBytes != nil && *settled.Attempt.ReadBytes == usage.ReadBytes && settled.Attempt.ElapsedNS != nil && *settled.Attempt.ElapsedNS == int64(usage.Elapsed)
	if e != nil || storageErr != nil || !knownMatches {
		s.poisoned = true
		result.Status = "recovery_required"
		result.Code = "publication_uncertain"
		return result, errors.Join(ErrSampleRecoveryRequired, ErrSampleStoreCorrupt, err, e, storageErr)
	}
	result.Budget = settled.Budget
	result.UsageDurable = true
	result.Status = "settled"
	result.Code = code
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
		result.Code = "request_canceled"
	}
	if s.life.Err() != nil {
		err = errors.Join(err, context.Canceled)
		result.Code = "request_canceled"
	}
	if err == nil {
		result.Report = report
	}
	return result, err
}

func (s *SampleStore) settleSample(ctx, callerCtx context.Context, batchID string, a *SampleAttempt, usage FileReadUsage, code string, hooks sampleStoreHooks) (error, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	saved, _, err := s.readSampleSnapshot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if saved.BatchID != batchID || saved.Attempt == nil || saved.Attempt.ID != a.ID || saved.Attempt.Status != "reserved" {
		return nil, ErrSampleStoreCorrupt
	}
	if hooks.beforeSettleCommit != nil {
		hooks.beforeSettleCommit()
	}
	if callerCtx.Err() != nil || s.life.Err() != nil {
		code = "request_canceled"
	}
	high := saved.Budget.MaxNow
	now := s.now().UTC()
	var clockErr error
	if !validHashReadClock(now) || now.Before(high) {
		clockErr = ErrSampleClockRollback
		code = "clock_rollback"
	} else if now.After(high) {
		high = now
	}
	if !sampleOutcome(code) || code == "" || code == "interrupted_unknown" {
		return nil, ErrSampleStoreCorrupt
	}
	changed, err := tx.ExecContext(ctx, "UPDATE sample_attempt SET status='settled',outcome=?,requested_bytes=?,read_bytes=?,elapsed_ns=? WHERE id=1 AND status='reserved' AND nonce=?", code, usage.RequestedBytes, usage.ReadBytes, int64(usage.Elapsed), a.ID)
	if err != nil {
		return nil, err
	}
	n, err := changed.RowsAffected()
	if err != nil || n != 1 {
		return nil, errors.Join(ErrSampleStoreCorrupt, err)
	}
	known := *a
	known.Status = "settled"
	known.Code = code
	known.RequestedBytes = &usage.RequestedBytes
	known.ReadBytes = &usage.ReadBytes
	elapsed := int64(usage.Elapsed)
	known.ElapsedNS = &elapsed
	if err = writeSampleBudget(ctx, tx, sampleBudgetProjection(&known, high)); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return nil, err
	}
	if err = sampleCommit(tx, hooks.settleCommit); err != nil {
		return nil, err
	}
	if hooks.afterSettleCommit != nil {
		hooks.afterSettleCommit()
	}
	return clockErr, nil
}

// Recover is explicit saved-only accounting recovery. An unfinished attempt
// becomes fully charged unknown consumption and can never be sampled again.
func (s *SampleStore) Recover(ctx context.Context) (SampleSnapshot, error) {
	return s.recoverSamples(ctx, sampleStoreHooks{})
}

func (s *SampleStore) recoverSamples(ctx context.Context, hooks sampleStoreHooks) (SampleSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquireSample(ctx, true); err != nil {
		return SampleSnapshot{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SampleSnapshot{}, err
	}
	defer tx.Rollback()
	saved, _, err := s.readSampleSnapshot(ctx, tx)
	if err != nil {
		return SampleSnapshot{}, err
	}
	if saved.Attempt == nil || saved.Attempt.Status != "reserved" {
		return saved, nil
	}
	if hooks.beforeRecoverCommit != nil {
		hooks.beforeRecoverCommit()
	}
	a := *saved.Attempt
	a.Status = "interrupted_unknown"
	a.Code = "interrupted_unknown"
	changed, err := tx.ExecContext(ctx, "UPDATE sample_attempt SET status='interrupted_unknown',outcome='interrupted_unknown' WHERE id=1 AND status='reserved' AND nonce=?", a.ID)
	if err != nil {
		return SampleSnapshot{}, err
	}
	n, err := changed.RowsAffected()
	if err != nil || n != 1 {
		return SampleSnapshot{}, errors.Join(ErrSampleStoreCorrupt, err)
	}
	if err = writeSampleBudget(ctx, tx, sampleBudgetProjection(&a, saved.Budget.MaxNow)); err != nil {
		return SampleSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return SampleSnapshot{}, err
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	if err = sampleCommit(tx, hooks.recoverCommit); err != nil {
		s.poisoned = true
		return SampleSnapshot{}, errors.Join(ErrSampleRecoveryRequired, err)
	}
	return s.readSampleSnapshotPublic(ctx)
}
