package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"time"
)

const CPUFeedbackContract = "worker_cpu_feedback_v1"
const CPUFeedbackBackoffLimit = time.Hour
const CPUUnknownRecoveryDelay = time.Hour
const cpuFeedbackScope = "experimental_inventory_dispatch_windows"

var ErrCPUFeedbackInvalid = errors.New("invalid worker CPU feedback input")
var ErrCPUFeedbackUnavailable = errors.New("worker CPU feedback schema unavailable")
var ErrCPUFeedbackCorrupt = errors.New("invalid saved worker CPU feedback")
var ErrCPUFeedbackReadOnly = errors.New("worker CPU feedback requires the exclusive writer")
var ErrCPUFeedbackDeferred = errors.New("worker CPU feedback cooldown active")
var ErrCPUFeedbackClockRollback = errors.New("worker CPU feedback clock rollback")
var ErrCPUFeedbackPending = errors.New("worker CPU feedback settlement required")
var ErrCPUFeedbackStale = errors.New("worker CPU feedback marker or dispatch is stale")
var ErrCPUFeedbackPublication = errors.New("worker CPU feedback publication outcome uncertain")

type CPUWindowStart struct {
	Instance        string
	WindowStartedAt time.Time
}

// CPUWindowMarker is bound to this open writer and immutable admission evidence.
// It is accounting provenance, not permission to read or change source files.
type CPUWindowMarker struct {
	store  *Store
	record CPUWindowRecord
}

func (marker CPUWindowMarker) Token() string { return marker.record.Token }

type CPUWindowMeasurement struct {
	CPUTimeNS *int64
	ElapsedNS *int64
	Reason    string
}

type CPUWindowRecord struct {
	Token              string     `json:"token"`
	Instance           string     `json:"instance"`
	Kind               string     `json:"kind"`
	RootID             int64      `json:"root_id"`
	JobID              int64      `json:"job_id"`
	JobToken           string     `json:"job_token"`
	JobLeaseUntil      *time.Time `json:"job_lease_until"`
	DispatchReservedAt time.Time  `json:"dispatch_reserved_at"`
	WindowStartedAt    time.Time  `json:"window_started_at"`
	RecordedAt         time.Time  `json:"recorded_at"`
	Status             string     `json:"status"`
	SettledAt          *time.Time `json:"settled_at"`
	CPUTimeNS          *int64     `json:"cpu_time_ns"`
	ElapsedNS          *int64     `json:"elapsed_ns"`
	Reason             string     `json:"reason,omitempty"`
	BackoffNS          int64      `json:"backoff_ns"`
	BackoffCapped      bool       `json:"backoff_capped"`
}

// CPUFeedbackState is saved evidence. This view never evaluates current time,
// recovers a marker, grants source permission or fabricates a CPU observation.
type CPUFeedbackState struct {
	Contract                 string           `json:"contract"`
	Available                bool             `json:"available"`
	Status                   string           `json:"status"`
	Scope                    string           `json:"scope"`
	TargetPercent            int              `json:"target_percent"`
	BackoffLimitNS           int64            `json:"backoff_limit_ns"`
	UnknownRecoveryBackoffNS int64            `json:"unknown_recovery_backoff_ns"`
	TrackingStartedAt        *time.Time       `json:"tracking_started_at"`
	ClockHighWater           *time.Time       `json:"clock_high_water"`
	LastBegunDispatchAt      *time.Time       `json:"last_begun_dispatch_at"`
	Window                   *CPUWindowRecord `json:"window"`
	NextAllowedAt            *time.Time       `json:"next_allowed_at"`
	CompletedUnknownWindows  int64            `json:"completed_unknown_windows"`
	RecoveredUnknownWindows  int64            `json:"recovered_unknown_windows"`
	UnknownCountSaturated    bool             `json:"unknown_count_saturated"`
	PreTrackingCPU           string           `json:"pre_tracking_cpu"`
}

type fairInventoryReceipt struct {
	store      *Store
	root       int64
	path       []byte
	kind       string
	job        *Job
	reservedAt time.Time
}

func newFairInventoryReceipt(s *Store, turn *FairInventoryTurn, reservedAt time.Time) *fairInventoryReceipt {
	r := &fairInventoryReceipt{store: s, root: turn.RootID, path: bytes.Clone(turn.RootPath), kind: turn.Kind, reservedAt: reservedAt.UTC()}
	if turn.Job != nil {
		job := *turn.Job
		job.Path, job.Cursor, job.RootPath = bytes.Clone(job.Path), bytes.Clone(job.Cursor), bytes.Clone(job.RootPath)
		r.job = &job
	}
	return r
}

func sameCPUJob(a, b *Job) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID && a.RootID == b.RootID && a.Kind == b.Kind && a.Attempts == b.Attempts && a.Token == b.Token && a.LeaseUntil.Equal(b.LeaseUntil) && a.RootIdentity == b.RootIdentity && bytes.Equal(a.Path, b.Path) && bytes.Equal(a.Cursor, b.Cursor) && bytes.Equal(a.RootPath, b.RootPath)
}

func cpuFeedbackSnapshot(available bool) CPUFeedbackState {
	status := "unavailable"
	if available {
		status = "untracked"
	}
	return CPUFeedbackState{Contract: CPUFeedbackContract, Available: available, Status: status, Scope: cpuFeedbackScope, TargetPercent: 1, BackoffLimitNS: int64(CPUFeedbackBackoffLimit), UnknownRecoveryBackoffNS: int64(CPUUnknownRecoveryDelay), PreTrackingCPU: "unknown"}
}

func cpuFeedbackTime(at time.Time) (time.Time, error) {
	at = at.UTC()
	n := at.UnixNano()
	if n <= 0 || !time.Unix(0, n).Equal(at) {
		return time.Time{}, ErrCPUFeedbackInvalid
	}
	return at, nil
}

func cpuFeedbackAdd(at time.Time, wait time.Duration) (time.Time, error) {
	if wait < 0 || wait > CPUFeedbackBackoffLimit || at.UnixNano() > math.MaxInt64-int64(wait) {
		return time.Time{}, ErrCPUFeedbackInvalid
	}
	return cpuFeedbackTime(at.Add(wait))
}

// CPUFeedbackWait is the bounded 1% feedback formula. Callers validate that CPU
// is nonnegative and elapsed is positive. Divide first to avoid CPU*100 overflow.
func CPUFeedbackWait(cpu, elapsed time.Duration) (time.Duration, bool) {
	if cpu < 0 || elapsed <= 0 {
		return 0, false
	}
	headroom := cpu - elapsed/100
	if headroom <= 0 {
		return 0, false
	}
	if headroom > CPUFeedbackBackoffLimit/100+1 {
		return CPUFeedbackBackoffLimit, true
	}
	wait := headroom*100 - elapsed%100
	if wait > CPUFeedbackBackoffLimit {
		return CPUFeedbackBackoffLimit, true
	}
	return wait, false
}

func cpuUnknownReason(reason string) bool {
	switch reason {
	case "cpu_observation_unavailable", "cpu_observation_invalid", "cpu_observation_regressed", "elapsed_window_invalid":
		return true
	}
	return false
}

func cpuMeasurementValid(m CPUWindowMeasurement) bool {
	if m.ElapsedNS != nil && *m.ElapsedNS <= 0 {
		return false
	}
	if m.CPUTimeNS != nil {
		return *m.CPUTimeNS >= 0 && m.ElapsedNS != nil && m.Reason == ""
	}
	return cpuUnknownReason(m.Reason)
}

func cpuTimePointer(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	at := time.Unix(0, n.Int64).UTC()
	return &at
}

func cpuNSPointer(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	value := n.Int64
	return &value
}

func cpuStorageError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(ErrCPUFeedbackCorrupt, err)
}

const cpuFeedbackColumns = `tracking_started_ns,max_now_ns,last_begun_dispatch_ns,completed_unknown,recovered_unknown,count_saturated,token,instance,kind,root_id,job_id,job_token,job_lease_until_ns,dispatch_reserved_ns,window_started_ns,recorded_ns,status,settled_ns,cpu_time_ns,elapsed_ns,reason,backoff_ns,backoff_capped,next_allowed_ns`

func readCPUFeedback(ctx context.Context, q queryRow) (CPUFeedbackState, error) {
	state := cpuFeedbackSnapshot(true)
	var tracked, high, dispatch, reserved, started, recorded int64
	var lease, settled, cpu, elapsed, next sql.NullInt64
	var saturated, capped int64
	var typesOK bool
	var record CPUWindowRecord
	err := q.QueryRowContext(ctx, `SELECT tracking_started_ns,max_now_ns,last_begun_dispatch_ns,completed_unknown,recovered_unknown,count_saturated,
 substr(token,1,65),substr(instance,1,33),substr(kind,1,33),root_id,job_id,substr(job_token,1,33),job_lease_until_ns,
 dispatch_reserved_ns,window_started_ns,recorded_ns,substr(status,1,33),settled_ns,cpu_time_ns,elapsed_ns,substr(reason,1,65),backoff_ns,backoff_capped,next_allowed_ns,
 typeof(singleton)='integer' AND singleton=1 AND NOT EXISTS(SELECT 1 FROM worker_cpu_feedback LIMIT 1 OFFSET 1)
 AND typeof(tracking_started_ns)='integer' AND typeof(max_now_ns)='integer' AND typeof(last_begun_dispatch_ns)='integer'
 AND typeof(completed_unknown)='integer' AND typeof(recovered_unknown)='integer' AND typeof(count_saturated)='integer'
 AND typeof(token)='text' AND typeof(instance)='text' AND typeof(kind)='text' AND typeof(root_id)='integer' AND typeof(job_id)='integer' AND typeof(job_token)='text'
 AND (job_lease_until_ns IS NULL OR typeof(job_lease_until_ns)='integer') AND typeof(dispatch_reserved_ns)='integer' AND typeof(window_started_ns)='integer' AND typeof(recorded_ns)='integer' AND typeof(status)='text'
 AND (settled_ns IS NULL OR typeof(settled_ns)='integer') AND (cpu_time_ns IS NULL OR typeof(cpu_time_ns)='integer') AND (elapsed_ns IS NULL OR typeof(elapsed_ns)='integer')
 AND typeof(reason)='text' AND typeof(backoff_ns)='integer' AND typeof(backoff_capped)='integer' AND (next_allowed_ns IS NULL OR typeof(next_allowed_ns)='integer')
 FROM worker_cpu_feedback LIMIT 1`).Scan(&tracked, &high, &dispatch, &state.CompletedUnknownWindows, &state.RecoveredUnknownWindows, &saturated, &record.Token, &record.Instance, &record.Kind, &record.RootID, &record.JobID, &record.JobToken, &lease, &reserved, &started, &recorded, &record.Status, &settled, &cpu, &elapsed, &record.Reason, &record.BackoffNS, &capped, &next, &typesOK)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, cpuStorageError(ctx, err)
	}
	if !typesOK || (saturated != 0 && saturated != 1) || (capped != 0 && capped != 1) {
		return state, ErrCPUFeedbackCorrupt
	}
	state.TrackingStartedAt, state.ClockHighWater, state.LastBegunDispatchAt = cpuTimePointer(sql.NullInt64{Int64: tracked, Valid: true}), cpuTimePointer(sql.NullInt64{Int64: high, Valid: true}), cpuTimePointer(sql.NullInt64{Int64: dispatch, Valid: true})
	state.UnknownCountSaturated = saturated == 1
	state.NextAllowedAt = cpuTimePointer(next)
	record.JobLeaseUntil, record.SettledAt = cpuTimePointer(lease), cpuTimePointer(settled)
	record.CPUTimeNS, record.ElapsedNS = cpuNSPointer(cpu), cpuNSPointer(elapsed)
	record.DispatchReservedAt, record.WindowStartedAt, record.RecordedAt = time.Unix(0, reserved).UTC(), time.Unix(0, started).UTC(), time.Unix(0, recorded).UTC()
	record.BackoffCapped = capped == 1
	state.Window, state.Status = &record, record.Status
	if !validCPUFeedback(state) {
		return cpuFeedbackSnapshot(true), ErrCPUFeedbackCorrupt
	}
	return state, nil
}

func validCPUFeedback(state CPUFeedbackState) bool {
	r := state.Window
	if r == nil || state.TrackingStartedAt == nil || state.ClockHighWater == nil || state.LastBegunDispatchAt == nil || state.CompletedUnknownWindows < 0 || state.RecoveredUnknownWindows < 0 || !metadataDigest(r.Token, 64) || !metadataDigest(r.Instance, 32) || r.RootID <= 0 || r.BackoffNS < 0 || r.BackoffNS > int64(CPUFeedbackBackoffLimit) {
		return false
	}
	for _, at := range []*time.Time{state.TrackingStartedAt, state.ClockHighWater, state.LastBegunDispatchAt, &r.DispatchReservedAt, &r.WindowStartedAt, &r.RecordedAt, r.JobLeaseUntil, r.SettledAt, state.NextAllowedAt} {
		if at != nil {
			if _, err := cpuFeedbackTime(*at); err != nil {
				return false
			}
		}
	}
	if !state.LastBegunDispatchAt.Equal(r.DispatchReservedAt) || r.WindowStartedAt.After(r.RecordedAt) || r.DispatchReservedAt.After(r.RecordedAt) || state.TrackingStartedAt.After(r.RecordedAt) || state.ClockHighWater.Before(r.RecordedAt) || state.ClockHighWater.Before(*state.TrackingStartedAt) || (state.UnknownCountSaturated != (state.CompletedUnknownWindows == math.MaxInt64 || state.RecoveredUnknownWindows == math.MaxInt64)) {
		return false
	}
	switch r.Kind {
	case FairInventorySource:
		if r.JobID <= 0 || !metadataDigest(r.JobToken, 32) || r.JobLeaseUntil == nil || !r.JobLeaseUntil.After(r.RecordedAt) {
			return false
		}
	case FairInventoryMaintenance:
		if r.JobID != 0 || r.JobToken != "" || r.JobLeaseUntil != nil {
			return false
		}
	default:
		return false
	}
	if r.Status == "pending" {
		return r.SettledAt == nil && r.CPUTimeNS == nil && r.ElapsedNS == nil && r.Reason == "" && r.BackoffNS == 0 && !r.BackoffCapped && state.NextAllowedAt == nil
	}
	if r.SettledAt == nil || r.SettledAt.Before(r.RecordedAt) || state.ClockHighWater.Before(*r.SettledAt) {
		return false
	}
	switch r.Status {
	case "observed":
		if !cpuMeasurementValid(CPUWindowMeasurement{CPUTimeNS: r.CPUTimeNS, ElapsedNS: r.ElapsedNS, Reason: r.Reason}) || r.CPUTimeNS == nil {
			return false
		}
		wait, capped := CPUFeedbackWait(time.Duration(*r.CPUTimeNS), time.Duration(*r.ElapsedNS))
		if r.BackoffNS != int64(wait) || r.BackoffCapped != capped {
			return false
		}
	case "completed_unknown":
		if r.CPUTimeNS != nil || !cpuMeasurementValid(CPUWindowMeasurement{ElapsedNS: r.ElapsedNS, Reason: r.Reason}) || r.BackoffNS != 0 || r.BackoffCapped || state.CompletedUnknownWindows < 1 {
			return false
		}
	case "recovered_unknown":
		if r.CPUTimeNS != nil || r.ElapsedNS != nil || r.Reason != "interrupted_window" || r.BackoffNS != int64(CPUUnknownRecoveryDelay) || r.BackoffCapped || state.RecoveredUnknownWindows < 1 {
			return false
		}
	default:
		return false
	}
	if r.BackoffNS == 0 {
		return state.NextAllowedAt == nil
	}
	next, err := cpuFeedbackAdd(*r.SettledAt, time.Duration(r.BackoffNS))
	return err == nil && state.NextAllowedAt != nil && state.NextAllowedAt.Equal(next)
}

func (s *Store) CPUFeedback(ctx context.Context) (CPUFeedbackState, error) {
	if ctx == nil {
		return CPUFeedbackState{}, ErrCPUFeedbackInvalid
	}
	if err := ctx.Err(); err != nil {
		return CPUFeedbackState{}, err
	}
	if s.schema < 12 {
		return cpuFeedbackSnapshot(false), nil
	}
	return readCPUFeedback(ctx, s.db)
}

func (s *Store) cpuFeedbackWriter(ctx context.Context) error {
	if ctx == nil {
		return ErrCPUFeedbackInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.readOnly || s.lock == nil {
		return ErrCPUFeedbackReadOnly
	}
	if s.schema < 12 {
		return ErrCPUFeedbackUnavailable
	}
	return nil
}

func cpuNullableTime(at *time.Time) any {
	if at == nil {
		return nil
	}
	return at.UnixNano()
}

func writeCPUFeedback(ctx context.Context, tx *sql.Tx, state CPUFeedbackState) error {
	if !validCPUFeedback(state) {
		return ErrCPUFeedbackInvalid
	}
	r := state.Window
	_, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO worker_cpu_feedback(singleton,`+cpuFeedbackColumns+`) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, state.TrackingStartedAt.UnixNano(), state.ClockHighWater.UnixNano(), state.LastBegunDispatchAt.UnixNano(), state.CompletedUnknownWindows, state.RecoveredUnknownWindows, state.UnknownCountSaturated, r.Token, r.Instance, r.Kind, r.RootID, r.JobID, r.JobToken, cpuNullableTime(r.JobLeaseUntil), r.DispatchReservedAt.UnixNano(), r.WindowStartedAt.UnixNano(), r.RecordedAt.UnixNano(), r.Status, cpuNullableTime(r.SettledAt), r.CPUTimeNS, r.ElapsedNS, r.Reason, r.BackoffNS, r.BackoffCapped, cpuNullableTime(state.NextAllowedAt))
	return err
}

func (s *Store) BeginCPUWindow(ctx context.Context, claimed *FairInventoryTurn, now time.Time, start CPUWindowStart) (CPUWindowMarker, error) {
	if err := s.cpuFeedbackWriter(ctx); err != nil {
		return CPUWindowMarker{}, err
	}
	now, err := cpuFeedbackTime(now)
	started, startErr := cpuFeedbackTime(start.WindowStartedAt)
	if err != nil || startErr != nil || started.After(now) || !metadataDigest(start.Instance, 32) || claimed == nil || claimed.receipt == nil {
		return CPUWindowMarker{}, ErrCPUFeedbackInvalid
	}
	r := claimed.receipt
	if r.store != s || r.root != claimed.RootID || !bytes.Equal(r.path, claimed.RootPath) || r.kind != claimed.Kind || !sameCPUJob(r.job, claimed.Job) || now.Before(r.reservedAt) || now.Sub(r.reservedAt) >= FairInventoryClaimWindow {
		return CPUWindowMarker{}, ErrCPUFeedbackStale
	}
	if r.kind != FairInventorySource && r.kind != FairInventoryMaintenance {
		return CPUWindowMarker{}, ErrCPUFeedbackInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CPUWindowMarker{}, err
	}
	defer tx.Rollback()
	state, err := readCPUFeedback(ctx, tx)
	if err != nil {
		return CPUWindowMarker{}, err
	}
	if state.ClockHighWater != nil && now.Before(*state.ClockHighWater) {
		return CPUWindowMarker{}, ErrCPUFeedbackClockRollback
	}
	if state.Status == "pending" {
		return CPUWindowMarker{}, ErrCPUFeedbackPending
	}
	if state.NextAllowedAt != nil && now.Before(*state.NextAllowedAt) {
		return CPUWindowMarker{}, ErrCPUFeedbackDeferred
	}
	if state.LastBegunDispatchAt != nil && !r.reservedAt.After(*state.LastBegunDispatchAt) {
		return CPUWindowMarker{}, ErrCPUFeedbackStale
	}
	cursor, err := readFairInventoryCursor(ctx, tx)
	if err != nil {
		return CPUWindowMarker{}, err
	}
	var last, next, chunks int64
	var day string
	var typesOK bool
	err = tx.QueryRowContext(ctx, `SELECT last_start_ns,next_start_ns,chunks,substr(day,1,11),typeof(last_start_ns)='integer' AND typeof(next_start_ns)='integer' AND typeof(chunks)='integer' AND typeof(day)='text' FROM scan_dispatch WHERE id=1`).Scan(&last, &next, &chunks, &day, &typesOK)
	if err != nil {
		return CPUWindowMarker{}, err
	}
	if !typesOK || next <= last || next-last > int64(24*time.Hour) || chunks < 1 || chunks > 100000 || day != r.reservedAt.Format(time.DateOnly) || last != r.reservedAt.UnixNano() || cursor.root != r.root || cursor.dispatch != last {
		return CPUWindowMarker{}, ErrCPUFeedbackStale
	}
	record := CPUWindowRecord{Instance: start.Instance, Kind: r.kind, RootID: r.root, DispatchReservedAt: r.reservedAt, WindowStartedAt: started, RecordedAt: now, Status: "pending"}
	if r.kind == FairInventorySource {
		if r.job == nil || r.job.RootID != r.root || r.job.Kind != ScanKind || !r.job.LeaseUntil.After(now) {
			return CPUWindowMarker{}, ErrCPUFeedbackStale
		}
		var matches bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id=? AND root_id=? AND kind=? AND status='running' AND lease_token=? AND lease_until_ns=? AND typeof(lease_until_ns)='integer')`, r.job.ID, r.root, ScanKind, r.job.Token, r.job.LeaseUntil.UnixNano()).Scan(&matches); err != nil {
			return CPUWindowMarker{}, err
		}
		if !matches {
			return CPUWindowMarker{}, ErrCPUFeedbackStale
		}
		record.JobID, record.JobToken = r.job.ID, r.job.Token
		lease := r.job.LeaseUntil.UTC()
		record.JobLeaseUntil = &lease
	} else if r.job != nil {
		return CPUWindowMarker{}, ErrCPUFeedbackStale
	}
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		return CPUWindowMarker{}, err
	}
	record.Token = hex.EncodeToString(token[:])
	if state.TrackingStartedAt == nil {
		tracked := now
		state.TrackingStartedAt = &tracked
	}
	state.ClockHighWater, state.LastBegunDispatchAt = &now, &record.DispatchReservedAt
	state.Window, state.Status, state.NextAllowedAt = &record, record.Status, nil
	marker := CPUWindowMarker{store: s, record: record}
	if err = writeCPUFeedback(ctx, tx, state); err != nil {
		return CPUWindowMarker{}, err
	}
	if err = ctx.Err(); err != nil {
		return CPUWindowMarker{}, err
	}
	if err = tx.Commit(); err != nil {
		return marker, errors.Join(ErrCPUFeedbackPublication, err)
	}
	if err = ctx.Err(); err != nil {
		return marker, errors.Join(ErrCPUFeedbackPublication, err)
	}
	return marker, nil
}

func sameCPUWindow(a, b CPUWindowRecord) bool {
	return a.Token == b.Token && a.Instance == b.Instance && a.Kind == b.Kind && a.RootID == b.RootID && a.JobID == b.JobID && a.JobToken == b.JobToken && sameCPUTime(a.JobLeaseUntil, b.JobLeaseUntil) && a.DispatchReservedAt.Equal(b.DispatchReservedAt) && a.WindowStartedAt.Equal(b.WindowStartedAt) && a.RecordedAt.Equal(b.RecordedAt)
}

func sameCPUTime(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(*b)
}

func cloneCPUNS(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func sameCPUNS(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func incrementCPUUnknown(count *int64, saturated *bool) {
	if *count < math.MaxInt64 {
		(*count)++
	}
	if *count == math.MaxInt64 {
		*saturated = true
	}
}

func (s *Store) SettleCPUWindow(ctx context.Context, marker CPUWindowMarker, completedAt time.Time, measurement CPUWindowMeasurement) (CPUFeedbackState, error) {
	if err := s.cpuFeedbackWriter(ctx); err != nil {
		return CPUFeedbackState{}, err
	}
	measurement.CPUTimeNS = cloneCPUNS(measurement.CPUTimeNS)
	measurement.ElapsedNS = cloneCPUNS(measurement.ElapsedNS)
	completedAt, err := cpuFeedbackTime(completedAt)
	if err != nil || marker.store != s || marker.record.Status != "pending" || !cpuMeasurementValid(measurement) {
		return CPUFeedbackState{}, ErrCPUFeedbackInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CPUFeedbackState{}, err
	}
	defer tx.Rollback()
	state, err := readCPUFeedback(ctx, tx)
	if err != nil {
		return state, err
	}
	if state.Window == nil || !sameCPUWindow(marker.record, *state.Window) {
		return state, ErrCPUFeedbackStale
	}
	r := state.Window
	if r.Status != "pending" {
		if (r.Status == "observed" || r.Status == "completed_unknown") && r.SettledAt != nil && r.SettledAt.Equal(completedAt) && sameCPUNS(r.CPUTimeNS, measurement.CPUTimeNS) && sameCPUNS(r.ElapsedNS, measurement.ElapsedNS) && r.Reason == measurement.Reason {
			return state, nil
		}
		return state, ErrCPUFeedbackStale
	}
	if completedAt.Before(*state.ClockHighWater) {
		return state, ErrCPUFeedbackClockRollback
	}
	savedState := state
	record := *r
	state.Window = &record
	r = state.Window
	r.SettledAt, r.CPUTimeNS, r.ElapsedNS, r.Reason = &completedAt, measurement.CPUTimeNS, measurement.ElapsedNS, measurement.Reason
	r.Status = "completed_unknown"
	if measurement.CPUTimeNS != nil {
		r.Status = "observed"
		wait, capped := CPUFeedbackWait(time.Duration(*measurement.CPUTimeNS), time.Duration(*measurement.ElapsedNS))
		r.BackoffNS, r.BackoffCapped = int64(wait), capped
		if wait > 0 {
			next, err := cpuFeedbackAdd(completedAt, wait)
			if err != nil {
				return savedState, err
			}
			state.NextAllowedAt = &next
		}
	} else {
		incrementCPUUnknown(&state.CompletedUnknownWindows, &state.UnknownCountSaturated)
	}
	state.ClockHighWater, state.Status = &completedAt, r.Status
	if err = writeCPUFeedback(ctx, tx, state); err != nil {
		return savedState, err
	}
	if err = ctx.Err(); err != nil {
		return savedState, err
	}
	if err = tx.Commit(); err != nil {
		return savedState, errors.Join(ErrCPUFeedbackPublication, err)
	}
	if err = ctx.Err(); err != nil {
		return savedState, errors.Join(ErrCPUFeedbackPublication, err)
	}
	return state, nil
}

// RecoverCPUWindow runs only under an explicit writer-owned recovery. Opening
// stores or reading status does not perform it. A terminal result never renews.
func (s *Store) RecoverCPUWindow(ctx context.Context, now time.Time) (CPUFeedbackState, error) {
	if err := s.cpuFeedbackWriter(ctx); err != nil {
		return CPUFeedbackState{}, err
	}
	now, err := cpuFeedbackTime(now)
	if err != nil {
		return CPUFeedbackState{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CPUFeedbackState{}, err
	}
	defer tx.Rollback()
	state, err := readCPUFeedback(ctx, tx)
	if err != nil {
		return state, err
	}
	if state.ClockHighWater != nil && now.Before(*state.ClockHighWater) {
		return state, ErrCPUFeedbackClockRollback
	}
	if state.Status != "pending" {
		return state, nil
	}
	next, err := cpuFeedbackAdd(now, CPUUnknownRecoveryDelay)
	if err != nil {
		return state, err
	}
	savedState := state
	record := *state.Window
	state.Window = &record
	r := state.Window
	r.Status, r.SettledAt, r.Reason, r.BackoffNS = "recovered_unknown", &now, "interrupted_window", int64(CPUUnknownRecoveryDelay)
	state.ClockHighWater, state.NextAllowedAt, state.Status = &now, &next, r.Status
	incrementCPUUnknown(&state.RecoveredUnknownWindows, &state.UnknownCountSaturated)
	if err = writeCPUFeedback(ctx, tx, state); err != nil {
		return savedState, err
	}
	if err = ctx.Err(); err != nil {
		return savedState, err
	}
	if err = tx.Commit(); err != nil {
		return savedState, errors.Join(ErrCPUFeedbackPublication, err)
	}
	if err = ctx.Err(); err != nil {
		return savedState, errors.Join(ErrCPUFeedbackPublication, err)
	}
	return state, nil
}
