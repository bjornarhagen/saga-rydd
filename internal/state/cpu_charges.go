package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

const CPUChargesContract = "worker_self_cpu_charges_v1"
const CPUChargesScope = "one_private_state_store_run_sessions"
const CPUChargesBackoffLimit = time.Hour
const CPUChargesUnknownDelay = time.Hour

var (
	ErrCPUChargesInvalid       = errors.New("invalid SELF CPU charge input")
	ErrCPUChargesUnavailable   = errors.New("SELF CPU charge ledger is not activated")
	ErrCPUChargesCorrupt       = errors.New("invalid saved SELF CPU charge ledger")
	ErrCPUChargesReadOnly      = errors.New("SELF CPU charges require the exclusive writer")
	ErrCPUChargesStale         = errors.New("SELF CPU charge generation or observation is stale")
	ErrCPUChargesOverflow      = errors.New("SELF CPU charges exceed integer bounds")
	ErrCPUChargesClockRollback = errors.New("SELF CPU observation precedes the durable clock high-water")
	ErrCPUChargesUnknown       = errors.New("SELF CPU session closed with an unknown observation")
	ErrCPUChargesPublication   = errors.New("SELF CPU charge publication outcome uncertain")
)

// This is a frozen accounting request, not a native process-birth identity.
type CPUSessionStart struct {
	ExpectedGeneration int64     `json:"expected_generation"`
	Nonce              string    `json:"nonce"`
	Instance           string    `json:"instance"`
	ObservedAt         time.Time `json:"observed_at"`
	SelfCPUNS          *int64    `json:"self_cpu_ns"`
	Reason             string    `json:"reason"`
}

type CPUSessionSample struct {
	Ordinal    int64     `json:"ordinal"`
	ObservedAt time.Time `json:"observed_at"`
	SelfCPUNS  *int64    `json:"self_cpu_ns"`
	ElapsedNS  *int64    `json:"elapsed_ns"`
	Reason     string    `json:"reason"`
}

// A marker is bound to one open writer and immutable start request. It grants
// no work/source permission and cannot cross a writer close/reopen.
type CPUSessionMarker struct {
	store      *Store
	generation int64
	start      CPUSessionStart
}

func (m CPUSessionMarker) Generation() int64 { return m.generation }
func (m CPUSessionMarker) Nonce() string     { return m.start.Nonce }

type CPUSessionRecord struct {
	Generation            int64             `json:"generation"`
	Start                 CPUSessionStart   `json:"start"`
	Status                string            `json:"status"`
	InitialPrefixChargeNS int64             `json:"initial_prefix_charge_ns"`
	SessionChargedCPUNS   int64             `json:"session_charged_cpu_ns"`
	LastSelfCPUNS         *int64            `json:"last_self_cpu_ns"`
	LastOrdinal           int64             `json:"last_ordinal"`
	LastSample            *CPUSessionSample `json:"last_sample"`
	LastSampleFinal       bool              `json:"last_sample_final"`
	ClosedAt              *time.Time        `json:"closed_at"`
	ClosureReason         string            `json:"closure_reason"`
	LastChargeNS          int64             `json:"last_charge_ns"`
	LastBackoffNS         int64             `json:"last_backoff_ns"`
	LastBackoffCapped     bool              `json:"last_backoff_capped"`
}

// CPUChargeState is historical accounting, never current admission or a quota.
// Fresh prefixes may overlap former sessions; unknown tails are not measured.
// SELF includes every thread of an embedding process and excludes children.
type CPUChargeState struct {
	Contract                    string            `json:"contract"`
	Scope                       string            `json:"scope"`
	Available                   bool              `json:"available"`
	Status                      string            `json:"status"`
	PrefixOverlapPossible       bool              `json:"prefix_overlap_possible"`
	UniqueProcessCPUVerified    bool              `json:"unique_process_cpu_verified"`
	FullProcessLifetimeVerified bool              `json:"full_process_lifetime_verified"`
	WorkPermissionGranted       bool              `json:"work_permission_granted"`
	Generation                  int64             `json:"generation"`
	ChargedCPUNS                int64             `json:"charged_cpu_ns"`
	ClosedSessions              int64             `json:"closed_sessions"`
	UnknownTailSessions         int64             `json:"unknown_tail_sessions"`
	OpenTailUnobserved          bool              `json:"open_tail_unobserved"`
	RecoveredSessions           int64             `json:"recovered_sessions"`
	ActivatedAt                 *time.Time        `json:"activated_at"`
	ClockHighWater              *time.Time        `json:"clock_high_water"`
	NextAllowedAt               *time.Time        `json:"next_allowed_at"`
	Session                     *CPUSessionRecord `json:"session"`
}

func cpuChargesBase(available bool) CPUChargeState {
	status := "unavailable"
	if available {
		status = "untracked"
	}
	return CPUChargeState{Contract: CPUChargesContract, Scope: CPUChargesScope, Available: available, Status: status, PrefixOverlapPossible: true}
}
func cloneCPUStart(s CPUSessionStart) CPUSessionStart {
	s.SelfCPUNS = cloneCPUNS(s.SelfCPUNS)
	return s
}
func cloneCPUSample(s CPUSessionSample) CPUSessionSample {
	s.SelfCPUNS = cloneCPUNS(s.SelfCPUNS)
	s.ElapsedNS = cloneCPUNS(s.ElapsedNS)
	return s
}
func cloneCPUCharges(s CPUChargeState) CPUChargeState {
	cloneTime := func(p *time.Time) *time.Time {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	s.ActivatedAt = cloneTime(s.ActivatedAt)
	s.ClockHighWater = cloneTime(s.ClockHighWater)
	s.NextAllowedAt = cloneTime(s.NextAllowedAt)
	if s.Session != nil {
		r := *s.Session
		r.Start = cloneCPUStart(r.Start)
		r.LastSelfCPUNS = cloneCPUNS(r.LastSelfCPUNS)
		r.ClosedAt = cloneTime(r.ClosedAt)
		if r.LastSample != nil {
			q := cloneCPUSample(*r.LastSample)
			r.LastSample = &q
		}
		s.Session = &r
	}
	return s
}
func sameCPUStart(a, b CPUSessionStart) bool {
	return a.ExpectedGeneration == b.ExpectedGeneration && a.Nonce == b.Nonce && a.Instance == b.Instance && a.ObservedAt.Equal(b.ObservedAt) && sameCPUNS(a.SelfCPUNS, b.SelfCPUNS) && a.Reason == b.Reason
}
func sameCPUSample(a, b CPUSessionSample) bool {
	return a.Ordinal == b.Ordinal && a.ObservedAt.Equal(b.ObservedAt) && sameCPUNS(a.SelfCPUNS, b.SelfCPUNS) && sameCPUNS(a.ElapsedNS, b.ElapsedNS) && a.Reason == b.Reason
}
func cpuChargesReason(reason string) bool {
	return reason == "cpu_observation_unavailable" || reason == "cpu_observation_invalid"
}
func cpuChargesInputCPU(cpu *int64, reason string) bool {
	return cpu != nil && reason == "" || cpu == nil && cpuChargesReason(reason)
}
func cpuChargesTime(at time.Time) bool {
	_, err := cpuFeedbackTime(at)
	return err == nil && at.Location() == time.UTC
}
func cpuChargesStartValid(s CPUSessionStart) bool {
	return s.ExpectedGeneration >= 0 && metadataDigest(s.Nonce, 64) && metadataDigest(s.Instance, 32) && cpuChargesTime(s.ObservedAt) && cpuChargesInputCPU(s.SelfCPUNS, s.Reason)
}
func cpuChargesSampleValid(s CPUSessionSample) bool {
	return s.Ordinal > 0 && cpuChargesTime(s.ObservedAt) && cpuChargesInputCPU(s.SelfCPUNS, s.Reason)
}
func cpuChargesProblem(cpu *int64, reason string, elapsed *int64, requireElapsed bool) string {
	if cpu == nil {
		return reason
	}
	if *cpu < 0 {
		return "cpu_observation_invalid"
	}
	if requireElapsed && (elapsed == nil || *elapsed <= 0) {
		return "elapsed_window_invalid"
	}
	return ""
}

// Prefix feedback repays prospectively; no pre-Run elapsed time grants credit.
func CPUChargesPrefixWait(cpu time.Duration) (time.Duration, bool) {
	if cpu < 0 {
		return 0, false
	}
	if cpu > CPUChargesBackoffLimit/100 {
		return CPUChargesBackoffLimit, true
	}
	return cpu * 100, false
}
func cpuChargesDebt(s *CPUChargeState, at time.Time, wait time.Duration, capped bool) error {
	next, err := cpuFeedbackAdd(at, wait)
	if err != nil {
		return ErrCPUChargesOverflow
	}
	s.Session.LastBackoffNS, s.Session.LastBackoffCapped = int64(wait), capped
	if wait > 0 && (s.NextAllowedAt == nil || next.After(*s.NextAllowedAt)) {
		s.NextAllowedAt = &next
	}
	return nil
}
func cpuChargesClose(s *CPUChargeState, at time.Time, status, reason string, recovered bool) error {
	if s.ClosedSessions == math.MaxInt64 || s.UnknownTailSessions == math.MaxInt64 || recovered && s.RecoveredSessions == math.MaxInt64 {
		return ErrCPUChargesOverflow
	}
	s.OpenTailUnobserved = false
	s.ClosedSessions++
	s.UnknownTailSessions++
	if recovered {
		s.RecoveredSessions++
	}
	s.Status = status
	s.Session.Status = status
	s.Session.ClosedAt = &at
	s.Session.ClosureReason = reason
	if status == "unknown" || status == "recovered_unknown" {
		s.Session.LastChargeNS = 0
		return cpuChargesDebt(s, at, CPUChargesUnknownDelay, false)
	}
	return nil
}

func validCPUCharges(s CPUChargeState) bool {
	if s.Contract != CPUChargesContract || s.Scope != CPUChargesScope || !s.Available || !s.PrefixOverlapPossible || s.UniqueProcessCPUVerified || s.FullProcessLifetimeVerified || s.WorkPermissionGranted || s.Generation < 0 || s.ChargedCPUNS < 0 || s.ClosedSessions < 0 || s.UnknownTailSessions != s.ClosedSessions || s.RecoveredSessions < 0 || s.RecoveredSessions > s.ClosedSessions || s.ActivatedAt == nil || s.ClockHighWater == nil || !cpuChargesTime(*s.ActivatedAt) || !cpuChargesTime(*s.ClockHighWater) || s.ClockHighWater.Before(*s.ActivatedAt) {
		return false
	}
	if s.NextAllowedAt != nil && (!cpuChargesTime(*s.NextAllowedAt) || s.NextAllowedAt.Sub(*s.ClockHighWater) > CPUChargesBackoffLimit) {
		return false
	}
	if s.Generation == 0 {
		return s.Status == "untracked" && s.Session == nil && s.ChargedCPUNS == 0 && s.ClosedSessions == 0 && s.NextAllowedAt == nil && !s.OpenTailUnobserved
	}
	r := s.Session
	if r == nil || r.Generation != s.Generation || !cpuChargesStartValid(r.Start) || r.Start.ExpectedGeneration == math.MaxInt64 || r.Start.ExpectedGeneration+1 != r.Generation || r.Start.ObservedAt.Before(*s.ActivatedAt) || r.Start.ObservedAt.After(*s.ClockHighWater) || r.Status != s.Status || r.InitialPrefixChargeNS < 0 || r.SessionChargedCPUNS < r.InitialPrefixChargeNS || s.ChargedCPUNS < r.SessionChargedCPUNS || r.LastOrdinal < 0 || r.LastChargeNS < 0 || r.LastChargeNS > r.SessionChargedCPUNS || r.LastBackoffNS < 0 || r.LastBackoffNS > int64(CPUChargesBackoffLimit) {
		return false
	}
	prefix := int64(0)
	if r.Start.SelfCPUNS != nil && *r.Start.SelfCPUNS >= 0 {
		prefix = *r.Start.SelfCPUNS
	}
	if prefix != r.InitialPrefixChargeNS {
		return false
	}
	if r.LastSelfCPUNS == nil {
		if r.SessionChargedCPUNS != 0 {
			return false
		}
	} else if *r.LastSelfCPUNS < 0 || *r.LastSelfCPUNS != r.SessionChargedCPUNS {
		return false
	}
	if r.LastSample == nil {
		expected := r.Start.SelfCPUNS
		if expected != nil && *expected < 0 {
			expected = nil
		}
		if r.LastOrdinal != 0 || r.LastSampleFinal || !sameCPUNS(r.LastSelfCPUNS, expected) {
			return false
		}
	} else if !cpuChargesSampleValid(*r.LastSample) || r.LastSample.Ordinal != r.LastOrdinal || r.LastSample.ObservedAt.After(*s.ClockHighWater) {
		return false
	}
	if r.Status == "active" || r.Status == "finished" {
		anchor := r.Start.ObservedAt
		wait, capped := CPUChargesPrefixWait(time.Duration(r.InitialPrefixChargeNS))
		if r.LastSample != nil {
			q := r.LastSample
			if cpuChargesProblem(q.SelfCPUNS, q.Reason, q.ElapsedNS, true) != "" || *q.SelfCPUNS < r.InitialPrefixChargeNS || !sameCPUNS(r.LastSelfCPUNS, q.SelfCPUNS) {
				return false
			}
			anchor = q.ObservedAt
			wait, capped = CPUFeedbackWait(time.Duration(r.LastChargeNS), time.Duration(*q.ElapsedNS))
		} else if r.LastChargeNS != r.InitialPrefixChargeNS {
			return false
		}
		if r.LastBackoffNS != int64(wait) || r.LastBackoffCapped != capped || !anchor.Equal(*s.ClockHighWater) || wait > 0 && (s.NextAllowedAt == nil || s.NextAllowedAt.Before(anchor.Add(wait))) {
			return false
		}
	}
	if r.Status == "active" {
		return s.OpenTailUnobserved && s.ClosedSessions == s.Generation-1 && r.ClosedAt == nil && r.ClosureReason == "" && r.Start.SelfCPUNS != nil && *r.Start.SelfCPUNS >= 0 && !r.LastSampleFinal
	}
	if s.OpenTailUnobserved || s.ClosedSessions != s.Generation || r.ClosedAt == nil || !cpuChargesTime(*r.ClosedAt) || r.ClosedAt.Before(r.Start.ObservedAt) || r.ClosedAt.After(*s.ClockHighWater) {
		return false
	}
	switch r.Status {
	case "finished":
		return r.LastSample != nil && r.ClosedAt.Equal(r.LastSample.ObservedAt) && r.ClosureReason == "graceful_tail_unobserved" && r.LastSampleFinal && cpuChargesProblem(r.LastSample.SelfCPUNS, r.LastSample.Reason, r.LastSample.ElapsedNS, true) == "" && r.LastSelfCPUNS != nil && *r.LastSelfCPUNS == *r.LastSample.SelfCPUNS
	case "recovered_unknown":
		return r.ClosureReason == "interrupted_session" && s.RecoveredSessions > 0 && r.LastBackoffNS == int64(CPUChargesUnknownDelay) && !r.LastBackoffCapped && s.NextAllowedAt != nil && !s.NextAllowedAt.Before(r.ClosedAt.Add(CPUChargesUnknownDelay))
	case "unknown":
		validReason := false
		if r.LastSample == nil {
			validReason = cpuChargesProblem(r.Start.SelfCPUNS, r.Start.Reason, nil, false) == r.ClosureReason && r.ClosureReason != ""
		} else {
			q := r.LastSample
			problem := cpuChargesProblem(q.SelfCPUNS, q.Reason, q.ElapsedNS, true)
			validReason = problem != "" && problem == r.ClosureReason || r.ClosureReason == "cpu_clock_rollback" && q.ObservedAt.Before(*r.ClosedAt) || r.ClosureReason == "cpu_observation_regressed" && problem == "" && r.LastSelfCPUNS != nil && *q.SelfCPUNS < *r.LastSelfCPUNS
		}
		return validReason && r.LastChargeNS == 0 && r.LastBackoffNS == int64(CPUChargesUnknownDelay) && !r.LastBackoffCapped && s.NextAllowedAt != nil && !s.NextAllowedAt.Before(r.ClosedAt.Add(CPUChargesUnknownDelay))
	}
	return false
}

// Every ledger transaction checks actual version, exact table/ledger shape and
// canonical bounded payload. The Store's immutable schema is only its open-time
// lower feature bound; optional activation never races writes to that field.
func cpuChargesStorageError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(ErrCPUChargesCorrupt, err)
}

func cpuChargesSchema(ctx context.Context, tx *sql.Tx) (int, error) {
	var app, version int
	if err := tx.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return 0, err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	if app != applicationID || version < 4 || version > maxStateSchemaVersion {
		return 0, ErrCPUChargesCorrupt
	}
	ledgerSQL := strings.Split(strings.TrimSpace(migration1), ";")[0]
	var ledgerShape string
	var ledgerBounded bool
	err := tx.QueryRowContext(ctx, "SELECT CASE WHEN typeof(sql)='text' THEN CAST(substr(CAST(sql AS BLOB),1,?) AS TEXT) ELSE '' END,typeof(sql)='text' AND type='table' AND length(CAST(sql AS BLOB))<=? FROM sqlite_master WHERE name='schema_migrations'", len(ledgerSQL)+1, len(ledgerSQL)).Scan(&ledgerShape, &ledgerBounded)
	if err != nil || !ledgerBounded || ledgerShape != ledgerSQL {
		return 0, cpuChargesStorageError(ctx, err)
	}
	// Indexed raw census, including one sentinel. Extra zero/negative versions
	// must not bypass the usual positive-version lookups.
	rows, err := tx.QueryContext(ctx, `SELECT CASE WHEN typeof(version)='integer' THEN version ELSE 0 END,CASE WHEN typeof(name)='text' THEN CAST(substr(CAST(name AS BLOB),1,129) AS TEXT) ELSE '' END,typeof(version)='integer' AND typeof(name)='text' AND typeof(applied_at_ns)='integer' AND applied_at_ns>=0 FROM schema_migrations ORDER BY version LIMIT 17`)
	if err != nil {
		return 0, cpuChargesStorageError(ctx, err)
	}
	count := 0
	for rows.Next() {
		var recorded int
		var name string
		var typed bool
		err = rows.Scan(&recorded, &name, &typed)
		count++
		if err != nil || count > version || recorded != count || !typed || name != migrations[count-1].name {
			rows.Close()
			return 0, cpuChargesStorageError(ctx, err)
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil || count != version {
		return 0, cpuChargesStorageError(ctx, errors.Join(err, closeErr))
	}
	if err = cpuChargeAdmissionSchema(ctx, tx, version); err != nil {
		return 0, err
	}
	var extra bool
	var shape, kind string
	var shapeBounded bool
	err = tx.QueryRowContext(ctx, "SELECT substr(type,1,17),CASE WHEN typeof(sql)='text' THEN CAST(substr(CAST(sql AS BLOB),1,?) AS TEXT) ELSE '' END,typeof(sql)='text' AND length(CAST(sql AS BLOB))<=? FROM sqlite_master WHERE name='worker_self_cpu_charges'", len(migration15)+1, len(migration15)).Scan(&kind, &shape, &shapeBounded)
	if version < cpuChargesSchemaVersion {
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, cpuChargesStorageError(ctx, err)
		}
		return version, nil
	}
	if err != nil || !shapeBounded || kind != "table" || strings.TrimSuffix(strings.TrimSpace(shape), ";") != strings.TrimSuffix(strings.TrimSpace(migration15), ";") {
		return 0, cpuChargesStorageError(ctx, err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE tbl_name='worker_self_cpu_charges' AND name!='worker_self_cpu_charges' UNION ALL SELECT 1 FROM sqlite_temp_master WHERE tbl_name='worker_self_cpu_charges' LIMIT 1)").Scan(&extra); err != nil || extra {
		return 0, cpuChargesStorageError(ctx, err)
	}
	return version, nil
}
func readCPUCharges(ctx context.Context, tx *sql.Tx) (CPUChargeState, error) {
	v, err := cpuChargesSchema(ctx, tx)
	if err != nil {
		return cpuChargesBase(false), err
	}
	if v < cpuChargesSchemaVersion {
		return cpuChargesBase(false), nil
	}
	var raw []byte
	var gen int64
	var typed bool
	err = tx.QueryRowContext(ctx, `SELECT CASE WHEN typeof(generation)='integer' THEN generation ELSE 0 END,CASE WHEN typeof(state_json)='blob' THEN substr(state_json,1,8193) ELSE X'' END,typeof(singleton)='integer' AND singleton=1 AND typeof(generation)='integer' AND typeof(state_json)='blob' AND length(state_json)<=8192 AND NOT EXISTS(SELECT 1 FROM worker_self_cpu_charges LIMIT 1 OFFSET 1) FROM worker_self_cpu_charges LIMIT 1`).Scan(&gen, &raw, &typed)
	if err != nil || !typed || len(raw) > CPUChargesMaxJSONBytes {
		return cpuChargesBase(true), cpuChargesStorageError(ctx, err)
	}
	var s CPUChargeState
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var activation int64
	var activationTyped bool
	if err = tx.QueryRowContext(ctx, "SELECT CASE WHEN typeof(applied_at_ns)='integer' THEN applied_at_ns ELSE 0 END,typeof(applied_at_ns)='integer' FROM schema_migrations WHERE version=15").Scan(&activation, &activationTyped); err != nil {
		return cpuChargesBase(true), cpuChargesStorageError(ctx, err)
	}
	if decoder.Decode(&s) != nil || !validCPUCharges(s) || s.Generation != gen || !activationTyped || s.ActivatedAt.UnixNano() != activation {
		return cpuChargesBase(true), ErrCPUChargesCorrupt
	}
	canonical, _ := json.Marshal(s)
	if !bytes.Equal(raw, canonical) {
		return cpuChargesBase(true), ErrCPUChargesCorrupt
	}
	if v >= cpuChargeAdmissionSchemaVersion {
		if _, err = readCPUAdmission(ctx, tx, s, v); err != nil {
			return cpuChargesBase(true), err
		}
	}
	return s, nil
}
func writeCPUCharges(ctx context.Context, tx *sql.Tx, s CPUChargeState) error {
	if !validCPUCharges(s) {
		return ErrCPUChargesInvalid
	}
	raw, err := json.Marshal(s)
	if err != nil || len(raw) > CPUChargesMaxJSONBytes {
		return ErrCPUChargesOverflow
	}
	_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO worker_self_cpu_charges(singleton,generation,state_json) VALUES(1,?,?)", s.Generation, raw)
	return err
}
func (s *Store) cpuChargesWriter(ctx context.Context) error {
	if ctx == nil {
		return ErrCPUChargesInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrCPUChargesReadOnly
	}
	if s.cpuAuthorityRefused.Load() {
		return ErrCPUChargeAdmissionAuthority
	}
	if s.readOnly {
		return ErrCPUChargesReadOnly
	}
	return nil
}
func commitCPUCharges(ctx context.Context, tx *sql.Tx, prior, next CPUChargeState) (CPUChargeState, error) {
	if err := writeCPUCharges(ctx, tx, next); err != nil {
		return prior, err
	}
	if err := ctx.Err(); err != nil {
		return prior, err
	}
	if err := tx.Commit(); err != nil {
		return prior, errors.Join(ErrCPUChargesPublication, err)
	}
	if err := ctx.Err(); err != nil {
		return prior, errors.Join(ErrCPUChargesPublication, err)
	}
	return cloneCPUCharges(next), nil
}

// Completing a no-write transaction cannot create publication ambiguity. A
// final cancellation veto applies even to exact retries and terminal no-ops.
func finishCPUChargesView(ctx context.Context, tx *sql.Tx, state CPUChargeState, outcome error) (CPUChargeState, error) {
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if err := tx.Commit(); err != nil {
		return state, err
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	return state, outcome
}

func (s *Store) CPUCharges(ctx context.Context) (CPUChargeState, error) {
	if ctx == nil {
		return cpuChargesBase(false), ErrCPUChargesInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cpuChargesBase(false), err
	}
	defer tx.Rollback()
	state, err := readCPUCharges(ctx, tx)
	if err != nil {
		return state, err
	}
	return finishCPUChargesView(ctx, tx, state, nil)
}

// Activation alone publishes schema 15 and an untracked singleton. It observes
// no CPU and starts/recovers no session. Ordinary OpenWriter remains schema 14.
func (s *Store) ActivateCPUCharges(ctx context.Context, at time.Time) (CPUChargeState, error) {
	if err := s.cpuChargesWriter(ctx); err != nil {
		return cpuChargesBase(false), err
	}
	if !cpuChargesTime(at) {
		return cpuChargesBase(false), ErrCPUChargesInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cpuChargesBase(false), err
	}
	defer tx.Rollback()
	if err = s.bareCPUAuthority(ctx, tx); err != nil {
		return cpuChargesBase(false), err
	}
	prior, err := readCPUCharges(ctx, tx)
	if err != nil {
		return prior, err
	}
	if prior.Available {
		return finishCPUChargesView(ctx, tx, prior, nil)
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return prior, err
	}
	if version != schemaVersion {
		return prior, ErrCPUChargesUnavailable
	}
	if _, err = tx.ExecContext(ctx, migration15); err != nil {
		return prior, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations VALUES(?,?,?)", cpuChargesSchemaVersion, migrations[cpuChargesSchemaVersion-1].name, at.UnixNano()); err != nil {
		return prior, err
	}
	if _, err = tx.ExecContext(ctx, "PRAGMA user_version=15"); err != nil {
		return prior, err
	}
	next := cpuChargesBase(true)
	next.ActivatedAt = &at
	next.ClockHighWater = &at
	return commitCPUCharges(ctx, tx, prior, next)
}

// Exact Begin retries return the current saved outcome with a zero marker.
// They cannot reconstruct continuation authority from a saved request. Retain
// the original/qualified marker; after reopen, recover and begin a new session.
func (s *Store) BeginCPUSession(ctx context.Context, start CPUSessionStart) (CPUSessionMarker, CPUChargeState, error) {
	var empty CPUSessionMarker
	if err := s.cpuChargesWriter(ctx); err != nil {
		return empty, cpuChargesBase(false), err
	}
	start = cloneCPUStart(start)
	if !cpuChargesStartValid(start) {
		return empty, cpuChargesBase(false), ErrCPUChargesInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, cpuChargesBase(false), err
	}
	defer tx.Rollback()
	if err = s.bareCPUAuthority(ctx, tx); err != nil {
		return empty, cpuChargesBase(false), err
	}
	prior, err := readCPUCharges(ctx, tx)
	if err != nil {
		return empty, prior, err
	}
	if !prior.Available {
		return empty, prior, ErrCPUChargesUnavailable
	}
	if prior.Generation > 0 && prior.Generation-1 == start.ExpectedGeneration && sameCPUStart(prior.Session.Start, start) {
		outcome := error(nil)
		if cpuChargesProblem(start.SelfCPUNS, start.Reason, nil, false) != "" {
			outcome = ErrCPUChargesUnknown
		}
		state, err := finishCPUChargesView(ctx, tx, prior, outcome)
		return empty, state, err
	}
	if start.ExpectedGeneration != prior.Generation {
		return empty, prior, ErrCPUChargesStale
	}
	if start.ObservedAt.Before(*prior.ClockHighWater) {
		return empty, prior, ErrCPUChargesClockRollback
	}
	if prior.Generation == math.MaxInt64 {
		return empty, prior, ErrCPUChargesOverflow
	}
	next := cloneCPUCharges(prior)
	if next.Session != nil && next.Session.Status == "active" {
		if err = cpuChargesClose(&next, start.ObservedAt, "recovered_unknown", "interrupted_session", true); err != nil {
			return empty, prior, err
		}
	}
	next.Generation++
	next.ClockHighWater = &start.ObservedAt
	next.Status = "active"
	next.OpenTailUnobserved = true
	next.Session = &CPUSessionRecord{Generation: next.Generation, Start: cloneCPUStart(start), Status: "active"}
	problem := cpuChargesProblem(start.SelfCPUNS, start.Reason, nil, false)
	if problem == "" {
		charge := *start.SelfCPUNS
		if charge > math.MaxInt64-next.ChargedCPUNS {
			return empty, prior, ErrCPUChargesOverflow
		}
		next.ChargedCPUNS += charge
		next.Session.InitialPrefixChargeNS = charge
		next.Session.LastChargeNS = charge
		next.Session.SessionChargedCPUNS = charge
		next.Session.LastSelfCPUNS = cloneCPUNS(start.SelfCPUNS)
		wait, capped := CPUChargesPrefixWait(time.Duration(charge))
		if err = cpuChargesDebt(&next, start.ObservedAt, wait, capped); err != nil {
			return empty, prior, err
		}
	} else {
		if err = cpuChargesClose(&next, start.ObservedAt, "unknown", problem, false); err != nil {
			return empty, prior, err
		}
	}
	marker := CPUSessionMarker{s, next.Generation, cloneCPUStart(start)}
	state, err := commitCPUCharges(ctx, tx, prior, next)
	if err != nil {
		if errors.Is(err, ErrCPUChargesPublication) {
			return marker, state, err
		}
		return empty, state, err
	}
	if problem != "" {
		return marker, state, ErrCPUChargesUnknown
	}
	return marker, state, nil
}

func (s *Store) SampleCPUSession(ctx context.Context, marker CPUSessionMarker, sample CPUSessionSample) (CPUChargeState, error) {
	return s.sampleCPUSession(ctx, marker, sample, false)
}
func (s *Store) FinishCPUSession(ctx context.Context, marker CPUSessionMarker, sample CPUSessionSample) (CPUChargeState, error) {
	return s.sampleCPUSession(ctx, marker, sample, true)
}
func (s *Store) sampleCPUSession(ctx context.Context, marker CPUSessionMarker, sample CPUSessionSample, finish bool) (CPUChargeState, error) {
	if err := s.cpuChargesWriter(ctx); err != nil {
		return cpuChargesBase(false), err
	}
	sample = cloneCPUSample(sample)
	if marker.store != s || marker.generation <= 0 || !cpuChargesStartValid(marker.start) || !cpuChargesSampleValid(sample) {
		return cpuChargesBase(false), ErrCPUChargesStale
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cpuChargesBase(false), err
	}
	defer tx.Rollback()
	if err = s.bareCPUAuthority(ctx, tx); err != nil {
		return cpuChargesBase(false), err
	}
	prior, err := readCPUCharges(ctx, tx)
	if err != nil {
		return prior, err
	}
	if !prior.Available {
		return prior, ErrCPUChargesUnavailable
	}
	r := prior.Session
	if r == nil || marker.generation != prior.Generation || !sameCPUStart(marker.start, r.Start) {
		return prior, ErrCPUChargesStale
	}
	if r.LastSample != nil && sample.Ordinal == r.LastOrdinal && r.LastSampleFinal == finish && sameCPUSample(*r.LastSample, sample) {
		outcome := error(nil)
		if r.Status == "unknown" || r.Status == "recovered_unknown" {
			outcome = ErrCPUChargesUnknown
			if r.ClosureReason == "cpu_clock_rollback" {
				outcome = ErrCPUChargesClockRollback
			}
		}
		return finishCPUChargesView(ctx, tx, prior, outcome)
	}
	if r.LastOrdinal == math.MaxInt64 {
		return prior, ErrCPUChargesOverflow
	}
	if r.Status != "active" || sample.Ordinal != r.LastOrdinal+1 {
		return prior, ErrCPUChargesStale
	}
	next := cloneCPUCharges(prior)
	r = next.Session
	r.LastSample = &sample
	r.LastSampleFinal = finish
	r.LastOrdinal = sample.Ordinal
	problem := cpuChargesProblem(sample.SelfCPUNS, sample.Reason, sample.ElapsedNS, true)
	at := sample.ObservedAt
	if at.Before(*prior.ClockHighWater) {
		problem = "cpu_clock_rollback"
		at = *prior.ClockHighWater
	} else {
		next.ClockHighWater = &at
	}
	if problem == "" && *sample.SelfCPUNS < *r.LastSelfCPUNS {
		problem = "cpu_observation_regressed"
	}
	if problem != "" {
		if err = cpuChargesClose(&next, at, "unknown", problem, false); err != nil {
			return prior, err
		}
	} else {
		delta := *sample.SelfCPUNS - *r.LastSelfCPUNS
		if delta > math.MaxInt64-next.ChargedCPUNS {
			return prior, ErrCPUChargesOverflow
		}
		next.ChargedCPUNS += delta
		r.LastChargeNS = delta
		r.SessionChargedCPUNS = *sample.SelfCPUNS
		r.LastSelfCPUNS = cloneCPUNS(sample.SelfCPUNS)
		wait, capped := CPUFeedbackWait(time.Duration(delta), time.Duration(*sample.ElapsedNS))
		if err = cpuChargesDebt(&next, at, wait, capped); err != nil {
			return prior, err
		}
		if finish {
			if err = cpuChargesClose(&next, at, "finished", "graceful_tail_unobserved", false); err != nil {
				return prior, err
			}
		}
	}
	state, err := commitCPUCharges(ctx, tx, prior, next)
	if err != nil {
		return state, err
	}
	if problem == "cpu_clock_rollback" {
		return state, ErrCPUChargesClockRollback
	}
	if problem != "" {
		return state, ErrCPUChargesUnknown
	}
	return state, nil
}

// Recovery is an explicit mutation, never an effect of open or saved view.
func (s *Store) RecoverCPUSession(ctx context.Context, at time.Time) (CPUChargeState, error) {
	if err := s.cpuChargesWriter(ctx); err != nil {
		return cpuChargesBase(false), err
	}
	if !cpuChargesTime(at) {
		return cpuChargesBase(false), ErrCPUChargesInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cpuChargesBase(false), err
	}
	defer tx.Rollback()
	if err = s.bareCPUAuthority(ctx, tx); err != nil {
		return cpuChargesBase(false), err
	}
	prior, err := readCPUCharges(ctx, tx)
	if err != nil {
		return prior, err
	}
	if !prior.Available {
		return prior, ErrCPUChargesUnavailable
	}
	if at.Before(*prior.ClockHighWater) {
		return prior, ErrCPUChargesClockRollback
	}
	if prior.Session == nil || prior.Session.Status != "active" {
		return finishCPUChargesView(ctx, tx, prior, nil)
	}
	next := cloneCPUCharges(prior)
	next.ClockHighWater = &at
	if err = cpuChargesClose(&next, at, "recovered_unknown", "interrupted_session", true); err != nil {
		return prior, err
	}
	return commitCPUCharges(ctx, tx, prior, next)
}
