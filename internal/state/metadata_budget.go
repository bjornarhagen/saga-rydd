package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"time"
)

// This library reserves accounting credits only. It performs no scanner call
// and enforces no scanner, CPU, byte or physical-I/O limit by itself.
const MetadataBudgetContract = "worker_scanner_metadata_reservations_v1"
const MetadataAllowanceLimit int64 = 65536
const MetadataDailyLimit int64 = 1 << 50

type MetadataScope string

const (
	MetadataStartup MetadataScope = "startup"
	MetadataNext    MetadataScope = "next"
)

var (
	ErrMetadataInvalid       = errors.New("invalid metadata reservation input")
	ErrMetadataUnavailable   = errors.New("metadata reservation storage is unavailable")
	ErrMetadataReadOnly      = errors.New("metadata reservation storage is read-only")
	ErrMetadataCorrupt       = errors.New("metadata reservation accounting is invalid")
	ErrMetadataOverflow      = errors.New("metadata reservation accounting exceeds its integer bounds")
	ErrMetadataDeferred      = errors.New("metadata reservation is deferred")
	ErrMetadataClockRollback = errors.New("metadata reservation clock is before its durable high-water mark")
	ErrMetadataDailyQuota    = errors.New("metadata reservation exceeds the remaining daily allowance")
	ErrMetadataOutstanding   = errors.New("metadata reservation requires outstanding work to be settled or recovered")
	ErrMetadataStale         = errors.New("metadata reservation no longer belongs to this attempt")
)

type MetadataCharges struct {
	Reserved            int64 `json:"reserved_attempts"`
	Observed            int64 `json:"observed_attempts"`
	UnknownReserved     int64 `json:"unknown_reserved_attempts"`
	OutstandingReserved int64 `json:"outstanding_reserved_attempts"`
	KnownUnusedReserved int64 `json:"known_unused_reserved_attempts"`
}

// Charges are absent before tracking or when an old schema has no accounting.
// The first partial UTC day covers only reservations since TrackingStartedAt;
// earlier API usage is unknown, rather than inferred to be zero.
type MetadataBudget struct {
	Contract          string           `json:"contract"`
	Available         bool             `json:"available"`
	Status            string           `json:"status"`
	Reason            string           `json:"reason,omitempty"`
	Day               string           `json:"day_utc,omitempty"`
	Limit             int64            `json:"daily_limit"`
	TrackingStartedAt *time.Time       `json:"tracking_started_at"`
	ClockHighWater    *time.Time       `json:"clock_high_water"`
	PreTrackingUsage  string           `json:"pre_tracking_usage"`
	DayCharges        *MetadataCharges `json:"day_charges"`
	TotalCharges      *MetadataCharges `json:"total_charges"`
	NextAllowedAt     *time.Time       `json:"next_allowed_at"`
}

// A reservation is accounting provenance, never source-access permission.
// Future scanner guards must enforce cancellation, its finite allowance and
// expiry before each API attempt. Expiry is UTC midnight or the job's lease,
// whichever is earlier. Reservation fields remain fixed after settlement.
type MetadataReservation struct {
	Token          string        `json:"token"`
	Scope          MetadataScope `json:"scope"`
	JobID          int64         `json:"job_id,omitempty"`
	JobToken       string        `json:"job_token,omitempty"`
	Day            string        `json:"day_utc"`
	Allowance      int64         `json:"allowance"`
	StartedAt      time.Time     `json:"started_at"`
	ExpiresAt      time.Time     `json:"expires_at"`
	ClockHighWater time.Time     `json:"clock_high_water"`
}

type metadataRecord struct {
	MetadataReservation
	status   string
	observed sql.NullInt64
}

type metadataState struct {
	present            bool
	started, highWater int64
	day                string
	daily, total       MetadataCharges
	records            []metadataRecord
}

type metadataQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func metadataStorageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrMetadataUnavailable
}

func metadataTime(now time.Time) (time.Time, time.Time, error) {
	now = now.UTC()
	ns := now.UnixNano()
	if ns <= 0 || !time.Unix(0, ns).Equal(now) {
		return time.Time{}, time.Time{}, ErrMetadataInvalid
	}
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	midnight := day.AddDate(0, 0, 1)
	if midnight.UnixNano() <= ns || !time.Unix(0, midnight.UnixNano()).Equal(midnight) {
		return time.Time{}, time.Time{}, ErrMetadataInvalid
	}
	return now, midnight, nil
}

func metadataDigest(token string, size int) bool {
	if len(token) != size {
		return false
	}
	for _, c := range token {
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}

func metadataRead(ctx context.Context, q metadataQuery) (metadataState, error) {
	var state metadataState
	var typesOK bool
	err := q.QueryRowContext(ctx, `SELECT tracking_started_ns,day,max_now_ns,reserved,observed,unknown_reserved,
 total_reserved,total_observed,total_unknown_reserved,
 typeof(tracking_started_ns)='integer' AND typeof(day)='text' AND typeof(max_now_ns)='integer'
 AND typeof(reserved)='integer' AND typeof(observed)='integer' AND typeof(unknown_reserved)='integer'
 AND typeof(total_reserved)='integer' AND typeof(total_observed)='integer' AND typeof(total_unknown_reserved)='integer'
 FROM scan_metadata_budget WHERE singleton=1`).Scan(
		&state.started, &state.day, &state.highWater, &state.daily.Reserved, &state.daily.Observed, &state.daily.UnknownReserved,
		&state.total.Reserved, &state.total.Observed, &state.total.UnknownReserved, &typesOK)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, metadataStorageError(ctx, err)
	}
	state.present = err == nil
	if state.present && !typesOK {
		return state, ErrMetadataCorrupt
	}
	rows, err := q.QueryContext(ctx, `SELECT scope,token,job_id,job_token,day,started_ns,expires_ns,high_water_ns,allowance,status,observed
 ,typeof(scope)='text' AND typeof(token)='text' AND typeof(job_id)='integer' AND typeof(job_token)='text'
 AND typeof(day)='text' AND typeof(started_ns)='integer' AND typeof(expires_ns)='integer' AND typeof(high_water_ns)='integer'
 AND typeof(allowance)='integer' AND typeof(status)='text' AND (observed IS NULL OR typeof(observed)='integer')
 FROM scan_metadata_reservations ORDER BY scope LIMIT 3`)
	if err != nil {
		return state, metadataStorageError(ctx, err)
	}
	defer rows.Close()
	for rows.Next() {
		var record metadataRecord
		var started, expires, highWater int64
		if err = rows.Scan(&record.Scope, &record.Token, &record.JobID, &record.JobToken, &record.Day, &started, &expires, &highWater, &record.Allowance, &record.status, &record.observed, &typesOK); err != nil {
			return state, metadataStorageError(ctx, err)
		}
		if !typesOK {
			return state, ErrMetadataCorrupt
		}
		record.StartedAt, record.ExpiresAt, record.ClockHighWater = time.Unix(0, started).UTC(), time.Unix(0, expires).UTC(), time.Unix(0, highWater).UTC()
		state.records = append(state.records, record)
	}
	if err = rows.Err(); err != nil {
		return state, metadataStorageError(ctx, err)
	}
	if !state.present {
		if len(state.records) > 0 {
			return state, ErrMetadataCorrupt
		}
		return state, nil
	}
	day, dayErr := time.Parse(time.DateOnly, state.day)
	if state.started <= 0 || state.highWater < state.started || dayErr != nil || day.Format(time.DateOnly) != state.day || state.day < time.Unix(0, state.started).UTC().Format(time.DateOnly) || state.day > time.Unix(0, state.highWater).UTC().Format(time.DateOnly) || len(state.records) > 2 {
		return state, ErrMetadataCorrupt
	}
	if !metadataChargesValid(state.daily) || !metadataChargesValid(state.total) || state.total.Reserved < state.daily.Reserved || state.total.Observed < state.daily.Observed || state.total.UnknownReserved < state.daily.UnknownReserved {
		return state, ErrMetadataCorrupt
	}
	seen := map[MetadataScope]bool{}
	var retainedDaily, retainedTotal MetadataCharges
	for _, record := range state.records {
		if !metadataReservationValid(record.MetadataReservation) || seen[record.Scope] || record.StartedAt.UnixNano() < state.started || record.ClockHighWater.UnixNano() > state.highWater || record.Day > state.day {
			return state, ErrMetadataCorrupt
		}
		seen[record.Scope] = true
		retainedTotal.Reserved += record.Allowance
		if record.Day == state.day {
			retainedDaily.Reserved += record.Allowance
		}
		switch record.status {
		case "reserved":
			if record.observed.Valid || record.Day != state.day || record.Allowance > state.daily.Reserved-state.daily.OutstandingReserved {
				return state, ErrMetadataCorrupt
			}
			state.daily.OutstandingReserved += record.Allowance
			state.total.OutstandingReserved += record.Allowance
		case "settled":
			if !record.observed.Valid || record.observed.Int64 < 0 || record.observed.Int64 > record.Allowance {
				return state, ErrMetadataCorrupt
			}
			retainedTotal.Observed += record.observed.Int64
			if record.Day == state.day {
				retainedDaily.Observed += record.observed.Int64
			}
		case "unknown":
			if record.observed.Valid {
				return state, ErrMetadataCorrupt
			}
			retainedTotal.UnknownReserved += record.Allowance
			if record.Day == state.day {
				retainedDaily.UnknownReserved += record.Allowance
			}
		default:
			return state, ErrMetadataCorrupt
		}
	}
	if !metadataChargesValid(state.daily) || !metadataChargesValid(state.total) {
		return state, ErrMetadataCorrupt
	}
	if retainedDaily.Reserved > state.daily.Reserved || retainedDaily.Observed > state.daily.Observed || retainedDaily.UnknownReserved > state.daily.UnknownReserved || retainedTotal.Reserved > state.total.Reserved || retainedTotal.Observed > state.total.Observed || retainedTotal.UnknownReserved > state.total.UnknownReserved {
		return state, ErrMetadataCorrupt
	}
	state.daily.KnownUnusedReserved = state.daily.Reserved - state.daily.Observed - state.daily.UnknownReserved - state.daily.OutstandingReserved
	state.total.KnownUnusedReserved = state.total.Reserved - state.total.Observed - state.total.UnknownReserved - state.total.OutstandingReserved
	if state.total.KnownUnusedReserved < state.daily.KnownUnusedReserved {
		return state, ErrMetadataCorrupt
	}
	return state, nil
}

func metadataChargesValid(c MetadataCharges) bool {
	return c.Reserved >= 0 && c.Observed >= 0 && c.UnknownReserved >= 0 && c.OutstandingReserved >= 0 && c.Observed <= c.Reserved && c.UnknownReserved <= c.Reserved-c.Observed && c.OutstandingReserved <= c.Reserved-c.Observed-c.UnknownReserved
}

func metadataReservationValid(r MetadataReservation) bool {
	started, midnight, err := metadataTime(r.StartedAt)
	if err != nil || !metadataDigest(r.Token, 64) || r.Allowance < 1 || r.Allowance > MetadataAllowanceLimit || r.Day != started.Format(time.DateOnly) || !r.ClockHighWater.Equal(started) || !r.ExpiresAt.After(started) || r.ExpiresAt.After(midnight) {
		return false
	}
	_, _, err = metadataTime(r.ExpiresAt)
	if err != nil {
		return false
	}
	if r.Scope == MetadataStartup {
		return r.JobID == 0 && r.JobToken == "" && r.ExpiresAt.Equal(midnight)
	}
	return r.Scope == MetadataNext && r.JobID > 0 && metadataDigest(r.JobToken, 32)
}

func metadataSnapshot(state metadataState, now, midnight time.Time, limit int64) MetadataBudget {
	b := MetadataBudget{Contract: MetadataBudgetContract, Available: true, Status: "untracked", Limit: limit, PreTrackingUsage: "unknown"}
	if !state.present {
		return b
	}
	started, highWater := time.Unix(0, state.started).UTC(), time.Unix(0, state.highWater).UTC()
	b.TrackingStartedAt, b.ClockHighWater = &started, &highWater
	b.Status, b.Day = "tracked", state.day
	daily, total := state.daily, state.total
	b.DayCharges, b.TotalCharges = &daily, &total
	if now.UnixNano() < state.highWater {
		b.Status, b.Reason, b.NextAllowedAt = "deferred", "clock_rollback", &highWater
		return b
	}
	if now.Format(time.DateOnly) > state.day {
		if state.total.OutstandingReserved > 0 {
			b.Status, b.Reason = "deferred", "settlement_required"
			return b
		}
		b.Day = now.Format(time.DateOnly)
		b.DayCharges = &MetadataCharges{}
	}
	if b.DayCharges.Reserved >= limit {
		b.Status, b.Reason, b.NextAllowedAt = "deferred", "daily_metadata_limit", &midnight
	}
	return b
}

// MetadataBudget is a saved-only snapshot; it neither initializes tracking nor
// recovers reservations. Old readable inventory schemas report unavailable.
func (s *Store) MetadataBudget(ctx context.Context, now time.Time, dailyLimit int64) (MetadataBudget, error) {
	if ctx.Err() != nil {
		return MetadataBudget{}, ctx.Err()
	}
	now, midnight, err := metadataTime(now)
	if err != nil || dailyLimit < 1 || dailyLimit > MetadataDailyLimit {
		return MetadataBudget{}, ErrMetadataInvalid
	}
	if s.schema < 10 {
		return MetadataBudget{Contract: MetadataBudgetContract, Status: "unavailable", Reason: "schema_unavailable", Limit: dailyLimit, PreTrackingUsage: "unknown"}, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return MetadataBudget{}, metadataStorageError(ctx, err)
	}
	defer tx.Rollback()
	state, err := metadataRead(ctx, tx)
	if err != nil {
		return MetadataBudget{}, err
	}
	if err = tx.Commit(); err != nil {
		return MetadataBudget{}, metadataStorageError(ctx, err)
	}
	return metadataSnapshot(state, now, midnight, dailyLimit), nil
}

func (s *Store) metadataWriter() error {
	if s.readOnly {
		return ErrMetadataReadOnly
	}
	if s.schema < 10 {
		return ErrMetadataUnavailable
	}
	return nil
}

// ReserveMetadata charges the whole finite allowance before source work.
// Credits are never refunded. It permits at most one outstanding reservation
// per scope and fences Next reservations to the current scan job lease.
func (s *Store) ReserveMetadata(ctx context.Context, now time.Time, scope MetadataScope, job *Job, allowance, dailyLimit int64) (MetadataReservation, error) {
	if err := s.metadataWriter(); err != nil {
		return MetadataReservation{}, err
	}
	now, midnight, err := metadataTime(now)
	if err != nil || allowance < 1 || allowance > MetadataAllowanceLimit || dailyLimit < 1 || dailyLimit > MetadataDailyLimit || (scope != MetadataStartup && scope != MetadataNext) || (scope == MetadataStartup && job != nil) || (scope == MetadataNext && (job == nil || job.ID <= 0 || !metadataDigest(job.Token, 32))) {
		return MetadataReservation{}, ErrMetadataInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MetadataReservation{}, metadataStorageError(ctx, err)
	}
	defer tx.Rollback()
	state, err := metadataRead(ctx, tx)
	if err != nil {
		return MetadataReservation{}, err
	}
	if state.present && now.UnixNano() < state.highWater {
		return MetadataReservation{}, errors.Join(ErrMetadataDeferred, ErrMetadataClockRollback)
	}
	for _, record := range state.records {
		if record.status == "reserved" && (record.Scope == scope || record.Day != now.Format(time.DateOnly)) {
			return MetadataReservation{}, errors.Join(ErrMetadataDeferred, ErrMetadataOutstanding)
		}
	}
	if !state.present {
		state.present, state.started, state.day = true, now.UnixNano(), now.Format(time.DateOnly)
	} else if now.Format(time.DateOnly) > state.day {
		state.day, state.daily = now.Format(time.DateOnly), MetadataCharges{}
	}
	if state.daily.Reserved > dailyLimit || allowance > dailyLimit-state.daily.Reserved {
		return MetadataReservation{}, errors.Join(ErrMetadataDeferred, ErrMetadataDailyQuota)
	}
	if allowance > math.MaxInt64-state.total.Reserved {
		return MetadataReservation{}, ErrMetadataOverflow
	}
	r := MetadataReservation{Scope: scope, Day: state.day, Allowance: allowance, StartedAt: now, ExpiresAt: midnight, ClockHighWater: now}
	if scope == MetadataNext {
		var token, kind, status string
		var expires int64
		err = tx.QueryRowContext(ctx, "SELECT lease_token,lease_until_ns,kind,status FROM jobs WHERE id=?", job.ID).Scan(&token, &expires, &kind, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return MetadataReservation{}, ErrMetadataStale
		}
		if err != nil {
			return MetadataReservation{}, metadataStorageError(ctx, err)
		}
		if token != job.Token || kind != ScanKind || status != "running" || expires <= now.UnixNano() {
			return MetadataReservation{}, ErrMetadataStale
		}
		r.JobID, r.JobToken = job.ID, job.Token
		if expiry := time.Unix(0, expires).UTC(); expiry.Before(r.ExpiresAt) {
			r.ExpiresAt = expiry
		}
	}
	var nonce [32]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return MetadataReservation{}, ErrMetadataUnavailable
	}
	r.Token = hex.EncodeToString(nonce[:])
	state.highWater = now.UnixNano()
	state.daily.Reserved += allowance
	state.total.Reserved += allowance
	if err = metadataSave(ctx, tx, state); err != nil {
		return MetadataReservation{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO scan_metadata_reservations(scope,token,job_id,job_token,day,started_ns,expires_ns,high_water_ns,allowance,status,observed)
 VALUES(?,?,?,?,?,?,?,?,?,'reserved',NULL) ON CONFLICT(scope) DO UPDATE SET token=excluded.token,job_id=excluded.job_id,
 job_token=excluded.job_token,day=excluded.day,started_ns=excluded.started_ns,expires_ns=excluded.expires_ns,
 high_water_ns=excluded.high_water_ns,allowance=excluded.allowance,status='reserved',observed=NULL`, r.Scope, r.Token, r.JobID, r.JobToken, r.Day, r.StartedAt.UnixNano(), r.ExpiresAt.UnixNano(), r.ClockHighWater.UnixNano(), r.Allowance)
	if err != nil {
		return MetadataReservation{}, metadataStorageError(ctx, err)
	}
	if err = tx.Commit(); err != nil {
		return MetadataReservation{}, metadataStorageError(ctx, err)
	}
	return r, nil
}

func metadataSave(ctx context.Context, tx *sql.Tx, state metadataState) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO scan_metadata_budget VALUES(1,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(singleton) DO UPDATE SET tracking_started_ns=excluded.tracking_started_ns,day=excluded.day,max_now_ns=excluded.max_now_ns,
 reserved=excluded.reserved,observed=excluded.observed,unknown_reserved=excluded.unknown_reserved,
 total_reserved=excluded.total_reserved,total_observed=excluded.total_observed,total_unknown_reserved=excluded.total_unknown_reserved`,
		state.started, state.day, state.highWater, state.daily.Reserved, state.daily.Observed, state.daily.UnknownReserved, state.total.Reserved, state.total.Observed, state.total.UnknownReserved)
	return metadataStorageError(ctx, err)
}

func metadataSame(a, b MetadataReservation) bool {
	return a.Token == b.Token && a.Scope == b.Scope && a.JobID == b.JobID && a.JobToken == b.JobToken && a.Day == b.Day && a.Allowance == b.Allowance && a.StartedAt.Equal(b.StartedAt) && a.ExpiresAt.Equal(b.ExpiresAt) && a.ClockHighWater.Equal(b.ClockHighWater)
}

// SettleMetadata records known attempts, retaining the complete reservation
// charge. Exact retries are idempotent until the same scope is reserved again.
// Late settlement after expiry is allowed: accounting cannot undo past work.
func (s *Store) SettleMetadata(ctx context.Context, reservation MetadataReservation, observedAttempts int64, now time.Time) error {
	if err := s.metadataWriter(); err != nil {
		return err
	}
	now, _, err := metadataTime(now)
	if err != nil || !metadataReservationValid(reservation) || observedAttempts < 0 || observedAttempts > reservation.Allowance {
		return ErrMetadataInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return metadataStorageError(ctx, err)
	}
	defer tx.Rollback()
	state, err := metadataRead(ctx, tx)
	if err != nil {
		return err
	}
	var record *metadataRecord
	for i := range state.records {
		if metadataSame(state.records[i].MetadataReservation, reservation) {
			record = &state.records[i]
			break
		}
	}
	if record == nil || record.status == "unknown" {
		return ErrMetadataStale
	}
	if record.status == "settled" {
		if record.observed.Int64 == observedAttempts {
			return nil
		}
		return ErrMetadataStale
	}
	state.daily.Observed += observedAttempts
	state.total.Observed += observedAttempts
	// Keep the reservation day until every outstanding window is accounted
	// for. A later reservation advances the day, without hiding old charges.
	if now.UnixNano() > state.highWater {
		state.highWater = now.UnixNano()
	}
	if err = metadataSave(ctx, tx, state); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE scan_metadata_reservations SET status='settled',observed=? WHERE scope=? AND token=?", observedAttempts, reservation.Scope, reservation.Token)
	if err != nil {
		return metadataStorageError(ctx, err)
	}
	return metadataStorageError(ctx, tx.Commit())
}

// RecoverMetadataReservations explicitly converts outstanding credits to
// unknown charges after lost work. Opening a store or reading status never
// performs recovery. Repeating recovery adds no charge and reports zero.
func (s *Store) RecoverMetadataReservations(ctx context.Context, now time.Time) (int, error) {
	if err := s.metadataWriter(); err != nil {
		return 0, err
	}
	now, _, err := metadataTime(now)
	if err != nil {
		return 0, ErrMetadataInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, metadataStorageError(ctx, err)
	}
	defer tx.Rollback()
	state, err := metadataRead(ctx, tx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, record := range state.records {
		if record.status == "reserved" {
			count++
			state.daily.UnknownReserved += record.Allowance
			state.total.UnknownReserved += record.Allowance
		}
	}
	if count == 0 {
		return 0, nil
	}
	if now.UnixNano() > state.highWater {
		state.highWater = now.UnixNano()
	}
	if err = metadataSave(ctx, tx, state); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE scan_metadata_reservations SET status='unknown' WHERE status='reserved'"); err != nil {
		return 0, metadataStorageError(ctx, err)
	}
	if err = tx.Commit(); err != nil {
		return 0, metadataStorageError(ctx, err)
	}
	return count, nil
}
