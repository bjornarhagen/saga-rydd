package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const HashStoreReadBudgetContract = "shared_hash_store_reservations_v1"

var (
	ErrHashReadExecutionLimits     = errors.New("hash execution requires 1–1073741824 requested bytes per second and 1–1125899906842624 daily reserved bytes")
	ErrHashStoreReadBudgetRequired = errors.New("this hash store requires an explicit configured daily reservation profile")
	ErrHashStoreReadBudgetCorrupt  = fmt.Errorf("shared hash reservation accounting is invalid or incompatible: %w", ErrHashStoreCorrupt)
)

// These operation restrictions grant no additional source-read authority. The
// immutable approval remains required with its original daily/lifetime caps.
type HashReadExecutionLimits struct {
	RequestedBytesPerSecond int64 `json:"requested_bytes_per_second"`
	DailyReservedByteLimit  int64 `json:"daily_reserved_byte_limit"`
}

// HashStoreReadBudget is saved accounting, not physical I/O or current read
// permission. StoredDay/Clock describe the singleton. The separate projection
// includes newer saved consent observations without writing or sampling time.
// A new invocation must supply its current configured cap; it is not persisted.
type HashStoreReadBudget struct {
	Contract                        string    `json:"contract"`
	StoreID                         string    `json:"store_id"`
	Day                             string    `json:"reservation_day_utc"`
	MaxNow                          time.Time `json:"clock_high_water"`
	ReservedBytes                   int64     `json:"reserved_bytes"`
	TotalReservedBytes              int64     `json:"total_reserved_bytes"`
	RequestedBytes                  int64     `json:"observed_requested_bytes"`
	ReadBytes                       int64     `json:"observed_read_bytes"`
	UnknownReservedBytes            int64     `json:"unknown_reserved_bytes"`
	OutstandingReservedBytes        int64     `json:"outstanding_reserved_bytes"`
	TotalRequestedBytes             int64     `json:"total_observed_requested_bytes"`
	TotalReadBytes                  int64     `json:"total_observed_read_bytes"`
	TotalUnknownReservedBytes       int64     `json:"total_unknown_reserved_bytes"`
	TotalOutstandingReservedBytes   int64     `json:"total_outstanding_reserved_bytes"`
	ProjectedDay                    string    `json:"saved_clock_projection_day_utc"`
	ProjectedMaxNow                 time.Time `json:"saved_clock_projection_high_water"`
	ProjectedReservedBytes          int64     `json:"saved_clock_projection_reserved_bytes"`
	CurrentReadPermissionEvaluated  bool      `json:"current_read_permission_evaluated"`
	CurrentConfiguredLimitEvaluated bool      `json:"current_configured_limit_evaluated"`
	PhysicalIOVerified              bool      `json:"physical_io_verified"`
}

type hashStoreReadBudgetHooks struct {
	beforeCommit, afterCommit func()
	commit                    func(*sql.Tx) error
}

type hashStoreReadAccounting struct {
	storeID          string
	high             time.Time
	budgets          []HashBudget
	outstanding      map[string]int64
	totalOutstanding int64
}

func validHashExecutionLimits(l HashReadExecutionLimits) bool {
	return l.RequestedBytesPerSecond >= 1 && l.RequestedBytesPerSecond <= 1<<30 && l.DailyReservedByteLimit >= 1 && l.DailyReservedByteLimit <= 1<<50
}

func (s *HashStore) RunConsentedBudgeted(ctx context.Context, id string, source *state.Store, scanner *Scanner, limits HashReadExecutionLimits) (HashRunResult, error) {
	if !validHashExecutionLimits(limits) {
		return HashRunResult{}, ErrHashReadExecutionLimits
	}
	if s == nil || s.now == nil {
		return HashRunResult{}, ErrHashReadExecutionLimits
	}
	p := newHashReadPacer(limits.RequestedBytesPerSecond, s.now, time.Now, nil)
	return s.runConsented(ctx, id, source, scanner, hashStoreHooks{pacing: p, execution: &limits})
}
func (s *HashStore) RunFreshConsentedBudgeted(ctx context.Context, id string, scanner *Scanner, limits HashReadExecutionLimits) (HashFreshRunResult, error) {
	if !validHashExecutionLimits(limits) {
		return HashFreshRunResult{}, ErrHashReadExecutionLimits
	}
	if s == nil || s.now == nil {
		return HashFreshRunResult{}, ErrHashReadExecutionLimits
	}
	p := newHashReadPacer(limits.RequestedBytesPerSecond, s.now, time.Now, nil)
	return s.runFreshConsented(ctx, id, scanner, hashFreshRunHooks{pacing: p, execution: &limits})
}

func hashStoreReadBudgetFailure(ctx context.Context, err error) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// Retain a recognized namespace's fixed public identity while keeping
	// shared validation authoritative. Never expose raw SQL/payload details.
	for _, namespace := range []error{ErrHashFreshProgressCorrupt, ErrHashFreshReadCorrupt, ErrHashFreshJobCorrupt, ErrHashKeeperChoiceCorrupt} {
		if errors.Is(err, namespace) {
			return errors.Join(ErrHashStoreReadBudgetCorrupt, namespace)
		}
	}
	return ErrHashStoreReadBudgetCorrupt
}
func hashReadSchemaVersion(ctx context.Context, db hashQuery) (int, error) {
	var v int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return 0, err
	}
	if v < 1 || v > 7 {
		return 0, ErrHashStoreReadBudgetCorrupt
	}
	return v, nil
}
func hashBudgetAdd(dst *int64, v int64) error {
	if v < 0 || *dst > math.MaxInt64-v {
		return ErrHashStoreReadBudgetCorrupt
	}
	*dst += v
	return nil
}

// All raw cardinality gates precede allocations. IDs/payloads are bounded in
// SQL before scanning; existing exact readers validate the reachable evidence,
// consent and usage/checkpoint relationships in this same transaction.
func (s *HashStore) hashStoreReadAccounting(ctx context.Context, db hashQuery) (hashStoreReadAccounting, error) {
	a := hashStoreReadAccounting{outstanding: map[string]int64{}}
	v, err := hashReadSchemaVersion(ctx, db)
	if err != nil {
		return a, err
	}
	for _, table := range []string{"hash_meta", "hash_selection", "hash_budget"} {
		var count int
		if err = db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT id FROM "+table+" LIMIT 2)").Scan(&count); err != nil || count > 1 || table == "hash_meta" && count != 1 {
			return a, hashStoreReadBudgetFailure(ctx, err)
		}
	}
	var originalAttempts, orphan int
	if err = db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM (SELECT work_id FROM hash_attempt LIMIT 21)),(SELECT count(*) FROM (SELECT a.work_id FROM hash_attempt a LEFT JOIN hash_work w ON a.work_id=w.id WHERE w.id IS NULL LIMIT 1))`).Scan(&originalAttempts, &orphan); err != nil || originalAttempts > FileSampleTargetLimit || orphan != 0 {
		return a, hashStoreReadBudgetFailure(ctx, err)
	}
	snapshot, _, err := s.readHashSnapshot(ctx, db)
	if err != nil {
		return a, hashStoreReadBudgetFailure(ctx, err)
	}
	a.storeID = snapshot.StoreID
	if err = validateOriginalReadAccounting(snapshot, originalAttempts); err != nil {
		return a, err
	}
	addBudget := func(b *HashBudget) error {
		if b != nil {
			if !validHashReadClock(b.MaxNow) {
				return ErrHashStoreReadBudgetCorrupt
			}
			a.budgets = append(a.budgets, *b)
			if b.MaxNow.After(a.high) {
				a.high = b.MaxNow
			}
		}
		return nil
	}
	if err = addBudget(snapshot.Budget); err != nil {
		return a, err
	}
	if snapshot.ReadConsent != nil && snapshot.ReadConsent.ClockHighWater.After(a.high) {
		a.high = snapshot.ReadConsent.ClockHighWater
	}
	addAttempt := func(at *HashAttempt) error {
		if at == nil || at.Status != "reserved" {
			return nil
		}
		n := a.outstanding[at.ReservationDay]
		if err := hashBudgetAdd(&n, at.ReservedBytes); err != nil {
			return err
		}
		a.outstanding[at.ReservationDay] = n
		return hashBudgetAdd(&a.totalOutstanding, at.ReservedBytes)
	}
	for _, w := range snapshot.Work {
		if err = addAttempt(w.LatestAttempt); err != nil {
			return a, err
		}
	}
	if snapshot.Budget == nil && a.totalOutstanding != 0 {
		return a, ErrHashStoreReadBudgetCorrupt
	}
	if v >= 4 {
		if _, err = hashFreshJobCount(ctx, db); err != nil {
			return a, hashStoreReadBudgetFailure(ctx, err)
		}
		if v >= 5 {
			if err = hashFreshReadCount(ctx, db); err != nil {
				return a, hashStoreReadBudgetFailure(ctx, err)
			}
		}
		if v >= 6 {
			if err = hashFreshProgressCount(ctx, db); err != nil {
				return a, hashStoreReadBudgetFailure(ctx, err)
			}
		}
		rows, e := db.QueryContext(ctx, "SELECT substr(CAST(id AS BLOB),1,83),typeof(id)='text' FROM hash_fresh_job ORDER BY id LIMIT 129")
		if e != nil {
			return a, hashStoreReadBudgetFailure(ctx, e)
		}
		ids := make([]string, 0, HashFreshJobLimit)
		for rows.Next() {
			var id string
			var valid bool
			if e = rows.Scan(&id, &valid); e != nil {
				break
			}
			if !valid || !ValidHashFreshJobID(id) || len(ids) >= HashFreshJobLimit {
				e = ErrHashStoreReadBudgetCorrupt
				break
			}
			ids = append(ids, id)
		}
		if e == nil {
			e = rows.Err()
		}
		_ = rows.Close()
		if e != nil {
			return a, hashStoreReadBudgetFailure(ctx, e)
		}
		for _, id := range ids {
			job, e := s.readFreshJob(ctx, db, id)
			if e != nil {
				return a, hashStoreReadBudgetFailure(ctx, e)
			}
			if err = addBudget(job.FreshBudget); err != nil {
				return a, err
			}
			if job.ReadConsent != nil && job.ReadConsent.ClockHighWater.After(a.high) {
				a.high = job.ReadConsent.ClockHighWater
			}
			for _, w := range job.Progress {
				if err = addAttempt(w.LatestAttempt); err != nil {
					return a, err
				}
			}
		}
	}
	return a, ctx.Err()
}
func (a hashStoreReadAccounting) projection(day string) (HashStoreReadBudget, error) {
	if !validHashReadClock(a.high) || !hashDay(day) {
		return HashStoreReadBudget{}, ErrHashStoreReadBudgetCorrupt
	}
	b := HashStoreReadBudget{Contract: HashStoreReadBudgetContract, StoreID: a.storeID, Day: day, ProjectedDay: a.high.Format(time.DateOnly), ProjectedMaxNow: a.high, OutstandingReservedBytes: a.outstanding[day], TotalOutstandingReservedBytes: a.totalOutstanding}
	for _, n := range a.budgets {
		for _, pair := range []struct {
			dst *int64
			v   int64
		}{{&b.TotalReservedBytes, n.TotalReservedBytes}, {&b.TotalRequestedBytes, n.TotalRequestedBytes}, {&b.TotalReadBytes, n.TotalReadBytes}, {&b.TotalUnknownReservedBytes, n.TotalUnknownReservedBytes}} {
			if err := hashBudgetAdd(pair.dst, pair.v); err != nil {
				return b, err
			}
		}
		if n.Day == day {
			for _, pair := range []struct {
				dst *int64
				v   int64
			}{{&b.ReservedBytes, n.ReservedBytes}, {&b.RequestedBytes, n.RequestedBytes}, {&b.ReadBytes, n.ReadBytes}, {&b.UnknownReservedBytes, n.UnknownReservedBytes}} {
				if err := hashBudgetAdd(pair.dst, pair.v); err != nil {
					return b, err
				}
			}
		}
		if n.Day == b.ProjectedDay {
			if err := hashBudgetAdd(&b.ProjectedReservedBytes, n.ReservedBytes); err != nil {
				return b, err
			}
		}
	}
	if b.TotalOutstandingReservedBytes > b.TotalReservedBytes-b.TotalRequestedBytes-b.TotalUnknownReservedBytes || b.OutstandingReservedBytes > b.ReservedBytes-b.RequestedBytes-b.UnknownReservedBytes {
		return b, ErrHashStoreReadBudgetCorrupt
	}
	return b, nil
}
func (s *HashStore) readHashStoreReadBudget(ctx context.Context, db hashQuery) (*HashStoreReadBudget, error) {
	v, err := hashReadSchemaVersion(ctx, db)
	if err != nil {
		return nil, err
	}
	if v < 7 {
		return nil, nil
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT id FROM hash_store_read_budget LIMIT 2)").Scan(&count); err != nil || count != 1 {
		return nil, hashStoreReadBudgetFailure(ctx, err)
	}
	var id int64
	var store, day string
	var high, reserved, total int64
	var valid bool
	err = db.QueryRowContext(ctx, `SELECT CASE WHEN typeof(id)='integer' THEN id ELSE -1 END,substr(CAST(store_id AS BLOB),1,65),substr(CAST(day AS BLOB),1,11),CASE WHEN typeof(max_now_ns)='integer' THEN max_now_ns ELSE -1 END,CASE WHEN typeof(reserved_bytes)='integer' THEN reserved_bytes ELSE -1 END,CASE WHEN typeof(total_reserved_bytes)='integer' THEN total_reserved_bytes ELSE -1 END,typeof(store_id)='text' AND typeof(day)='text' FROM hash_store_read_budget`).Scan(&id, &store, &day, &high, &reserved, &total, &valid)
	stamp := time.Unix(0, high).UTC()
	if err != nil || !valid || id != 1 || !hashStoreDigest(store) || !hashDay(day) || !validHashReadClock(stamp) || stamp.Format(time.DateOnly) != day || reserved < 0 || total < reserved {
		return nil, hashStoreReadBudgetFailure(ctx, err)
	}
	a, err := s.hashStoreReadAccounting(ctx, db)
	if err != nil {
		return nil, err
	}
	// A charged namespace clock advances only in its reservation TX, which
	// also saves the shared clock. Only zero-total initialization and saved
	// consent observations can legitimately be newer than this singleton.
	for _, n := range a.budgets {
		if n.TotalReservedBytes > 0 && n.MaxNow.After(stamp) {
			return nil, ErrHashStoreReadBudgetCorrupt
		}
	}
	b, err := a.projection(day)
	if err != nil {
		return nil, err
	}
	if b.StoreID != store || b.ReservedBytes != reserved || b.TotalReservedBytes != total {
		return nil, ErrHashStoreReadBudgetCorrupt
	}
	b.MaxNow = stamp
	if stamp.After(b.ProjectedMaxNow) {
		b.ProjectedMaxNow = stamp
		b.ProjectedDay = day
		b.ProjectedReservedBytes = reserved
	}
	return &b, ctx.Err()
}

// StoreReadBudget reads one bounded saved-only snapshot. It never initializes,
// recovers, observes the current clock, loads config or opens original files.
func (s *HashStore) StoreReadBudget(ctx context.Context) (*HashStoreReadBudget, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	b, err := s.readHashStoreReadBudget(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return b, ctx.Err()
}

func writeHashStoreReadBudget(ctx context.Context, tx *sql.Tx, b HashStoreReadBudget) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO hash_store_read_budget VALUES(1,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET day=excluded.day,max_now_ns=excluded.max_now_ns,reserved_bytes=excluded.reserved_bytes,total_reserved_bytes=excluded.total_reserved_bytes`, b.StoreID, b.Day, b.MaxNow.UnixNano(), b.ReservedBytes, b.TotalReservedBytes)
	return err
}

// Only an explicit budgeted dispatch creates schema7. All additive schemas and
// seed charges are one publication; refusal cannot leave a partially migrated
// schema. Existing namespace histories and their unknown charges are untouched.
func (s *HashStore) prepareHashStoreReadBudget(ctx context.Context, tx *sql.Tx, now time.Time) (*HashStoreReadBudget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validHashReadClock(now) {
		return nil, ErrHashReadClockRollback
	}
	v, err := hashReadSchemaVersion(ctx, tx)
	if err != nil {
		return nil, err
	}
	if v == 7 {
		return s.readHashStoreReadBudget(ctx, tx)
	}
	a, err := s.hashStoreReadAccounting(ctx, tx)
	if err != nil {
		return nil, err
	}
	if now.After(a.high) {
		a.high = now
	}
	if !validHashReadClock(a.high) {
		return nil, ErrHashStoreReadBudgetCorrupt
	}
	b, err := a.projection(a.high.Format(time.DateOnly))
	if err != nil {
		return nil, err
	}
	b.MaxNow = a.high
	schemas := []string{hashReadSchema, hashKeeperChoiceSchema, hashFreshJobSchema, hashFreshReadSchema, hashFreshProgressSchema}
	for i, schema := range schemas {
		if v < i+2 {
			if _, err = tx.ExecContext(ctx, schema); err != nil {
				return nil, hashStoreReadBudgetFailure(ctx, err)
			}
		}
	}
	if _, err = tx.ExecContext(ctx, hashStoreReadBudgetSchema); err != nil {
		return nil, hashStoreReadBudgetFailure(ctx, err)
	}
	if err = writeHashStoreReadBudget(ctx, tx, b); err != nil {
		return nil, err
	}
	return &b, nil
}

// Advance only shared admission time; use saved namespace usage to recapture
// day counters. Metadata lifecycle commands may legitimately be newer than the
// stored shared clock. That projection is folded in before another dispatch.
func (s *HashStore) advanceHashStoreReadBudget(ctx context.Context, tx *sql.Tx, now time.Time) (*HashStoreReadBudget, error) {
	b, err := s.readHashStoreReadBudget(ctx, tx)
	if err != nil || b == nil {
		return b, err
	}
	if !validHashReadClock(now) {
		return b, ErrHashReadClockRollback
	}
	high := b.ProjectedMaxNow
	if now.After(high) {
		high = now
	}
	a, err := s.hashStoreReadAccounting(ctx, tx)
	if err != nil {
		return nil, err
	}
	a.high = high
	next, err := a.projection(high.Format(time.DateOnly))
	if err != nil {
		return nil, err
	}
	next.MaxNow = high
	if err = writeHashStoreReadBudget(ctx, tx, next); err != nil {
		return nil, err
	}
	if now.Before(high) {
		return &next, ErrHashReadClockRollback
	}
	return &next, nil
}
func (s *HashStore) admitHashStoreReadBudget(ctx context.Context, limits *HashReadExecutionLimits, now time.Time, hooks ...*hashStoreReadBudgetHooks) (*HashStoreReadBudget, error) {
	if limits == nil {
		v, err := hashReadSchemaVersion(ctx, s.db)
		if err != nil {
			return nil, err
		}
		if v == 7 {
			return nil, ErrHashStoreReadBudgetRequired
		}
		return nil, nil
	}
	if !validHashExecutionLimits(*limits) {
		return nil, ErrHashReadExecutionLimits
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = s.prepareHashStoreReadBudget(ctx, tx, now); err != nil {
		return nil, err
	}
	b, guardErr := s.advanceHashStoreReadBudget(ctx, tx, now)
	if guardErr != nil && !errors.Is(guardErr, ErrHashReadClockRollback) {
		return nil, guardErr
	}
	var hook *hashStoreReadBudgetHooks
	if len(hooks) != 0 {
		hook = hooks[0]
	}
	if hook != nil && hook.beforeCommit != nil {
		hook.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if hook != nil && hook.commit != nil {
		err = hook.commit(tx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		s.poisoned = true
		return b, ErrHashRecoveryRequired
	}
	s.schemaVersion = 7
	if hook != nil && hook.afterCommit != nil {
		hook.afterCommit()
	}
	if e := ctx.Err(); e != nil {
		return b, e
	}
	return b, guardErr
}

func (s *HashStore) latchHashStoreReadBudget(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, guardErr := s.advanceHashStoreReadBudget(ctx, tx, now)
	if guardErr != nil && !errors.Is(guardErr, ErrHashReadClockRollback) {
		return guardErr
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		return ErrHashRecoveryRequired
	}
	return nil
}

// Call before namespace mutation, then persist the returned charged row in the
// SAME transaction after namespace reservation insertion. Never charge on
// settlement or recovery: the full grant already survived process loss.
func (s *HashStore) reserveHashStoreReadBudget(ctx context.Context, tx *sql.Tx, limits *HashReadExecutionLimits, now time.Time, grant int64) (*HashStoreReadBudget, int64, error) {
	if limits == nil {
		return nil, grant, nil
	}
	b, err := s.advanceHashStoreReadBudget(ctx, tx, now)
	if err != nil || b == nil {
		return b, 0, err
	}
	grant = min(grant, max(limits.DailyReservedByteLimit-b.ReservedBytes, int64(0)))
	return b, grant, nil
}
func chargeHashStoreReadBudget(ctx context.Context, tx *sql.Tx, b *HashStoreReadBudget, grant int64) error {
	if b == nil {
		return nil
	}
	if err := hashBudgetAdd(&b.ReservedBytes, grant); err != nil {
		return err
	}
	if err := hashBudgetAdd(&b.TotalReservedBytes, grant); err != nil {
		return err
	}
	return writeHashStoreReadBudget(ctx, tx, *b)
}

func (s *HashStore) finishHashStoreReadBudget(opCtx context.Context, p *hashReadPacer, out **HashStoreReadBudget, boundedOuter bool) error {
	if s.poisoned {
		return nil
	}
	deadline := time.Now().Add(2 * time.Second)
	if boundedOuter {
		if d, ok := opCtx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
	}
	// The already saved report remains qualified when the outer window is gone.
	// Any unsaved new wall observation requires exact recovery instead of a
	// positive result; it cannot be silently forgotten after cancellation.
	if !time.Now().Before(deadline) {
		if p != nil && *out != nil && p.maxWall.After((*out).ProjectedMaxNow) {
			return ErrHashRecoveryRequired
		}
		return nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	b, err := s.readHashStoreReadBudget(ctx, s.db)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrHashStoreReadBudgetCorrupt
	}
	if p != nil && p.maxWall.After(b.ProjectedMaxNow) {
		if err = s.latchHashStoreReadBudget(ctx, p.maxWall); err != nil {
			return err
		}
		b, err = s.readHashStoreReadBudget(ctx, s.db)
		if err != nil {
			return err
		}
	}
	*out = b
	return nil
}

func hashStoreBudgetErrorState(err error) (string, string) {
	switch {
	case errors.Is(err, ErrHashRecoveryRequired):
		return "recovery_required", "publication_uncertain"
	case errors.Is(err, ErrHashReadClockRollback):
		return "refused", "clock_rollback"
	case errors.Is(err, ErrHashStoreReadBudgetRequired):
		return "refused", "configured_budget_required"
	case errors.Is(err, ErrHashStoreReadBudgetCorrupt):
		return "refused", "shared_budget_invalid"
	case errors.Is(err, context.Canceled):
		return "canceled", "request_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "canceled", "deadline_exceeded"
	default:
		return "refused", "hash_unavailable"
	}
}

func validateOriginalReadAccounting(s HashSnapshot, attemptCount int) error {
	b := s.Budget
	if b == nil {
		if attemptCount != 0 {
			return ErrHashStoreReadBudgetCorrupt
		}
		for _, w := range s.Work {
			if w.DurableOffset != 0 || w.Sequence != 0 {
				return ErrHashStoreReadBudgetCorrupt
			}
		}
		return nil
	}
	if len(s.Work) == 0 {
		return ErrHashStoreReadBudgetCorrupt
	}
	var reserved, requested, read, unknown, outstanding, dayReserved, dayRequested, dayRead, dayUnknown, dayOutstanding, durable int64
	for _, w := range s.Work {
		if err := hashBudgetAdd(&durable, w.DurableOffset); err != nil {
			return err
		}
		a := w.LatestAttempt
		if a == nil {
			continue
		}
		if a.ReservationDay > b.Day {
			return ErrHashStoreReadBudgetCorrupt
		}
		if err := hashBudgetAdd(&reserved, a.ReservedBytes); err != nil {
			return err
		}
		sameDay := a.ReservationDay == b.Day
		if sameDay {
			if err := hashBudgetAdd(&dayReserved, a.ReservedBytes); err != nil {
				return err
			}
		}
		switch a.Status {
		case "settled":
			if a.RequestedBytes == nil || a.ReadBytes == nil {
				return ErrHashStoreReadBudgetCorrupt
			}
			if err := hashBudgetAdd(&requested, *a.RequestedBytes); err != nil {
				return err
			}
			if err := hashBudgetAdd(&read, *a.ReadBytes); err != nil {
				return err
			}
			if sameDay {
				if err := hashBudgetAdd(&dayRequested, *a.RequestedBytes); err != nil {
					return err
				}
				if err := hashBudgetAdd(&dayRead, *a.ReadBytes); err != nil {
					return err
				}
			}
		case "interrupted_unknown":
			if err := hashBudgetAdd(&unknown, a.ReservedBytes); err != nil {
				return err
			}
			if sameDay {
				if err := hashBudgetAdd(&dayUnknown, a.ReservedBytes); err != nil {
					return err
				}
			}
		case "reserved":
			if err := hashBudgetAdd(&outstanding, a.ReservedBytes); err != nil {
				return err
			}
			if sameDay {
				if err := hashBudgetAdd(&dayOutstanding, a.ReservedBytes); err != nil {
					return err
				}
			}
		default:
			return ErrHashStoreReadBudgetCorrupt
		}
	}
	if reserved > b.TotalReservedBytes || requested > b.TotalRequestedBytes || read > b.TotalReadBytes || unknown > b.TotalUnknownReservedBytes || outstanding > b.TotalReservedBytes-b.TotalRequestedBytes-b.TotalUnknownReservedBytes || durable > b.TotalReadBytes || dayReserved > b.ReservedBytes || dayRequested > b.RequestedBytes || dayRead > b.ReadBytes || dayUnknown > b.UnknownReservedBytes || dayOutstanding > b.ReservedBytes-b.RequestedBytes-b.UnknownReservedBytes {
		return ErrHashStoreReadBudgetCorrupt
	}
	return nil
}
