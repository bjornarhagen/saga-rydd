package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

const CPUChargeAdmissionContract = "worker_cpu_charge_admission_v1"
const CPUChargeAdmissionScope = "one_private_state_store_utc_endpoint_assigned_charges"
const cpuLimitedMarkerContract = "cpu_limited_session_marker_v1"
const cpuAdmissionWindow = 5 * time.Second

var (
	ErrCPUChargeAdmissionInvalid        = errors.New("invalid conservative CPU period admission input")
	ErrCPUChargeAdmissionUnavailable    = errors.New("conservative CPU period admission is not activated")
	ErrCPUChargeAdmissionCorrupt        = errors.New("invalid saved conservative CPU period admission")
	ErrCPUChargeAdmissionPolicyRequired = errors.New("CPU accounting requires a limited policy-bound transition")
	ErrCPUChargeAdmissionAuthority      = errors.New("CPU accounting authority is unavailable after an uncertain limited publication")
	ErrCPUChargeAdmissionPublication    = errors.New("conservative CPU period publication outcome uncertain")
)

// Limits restrict later cooperative worker admission, never recording known
// charges. Zero is an explicit tracked profile with the period gate disabled.
type CPUChargeLimits struct {
	HourNS int64 `json:"hour_ns"`
	DayNS  int64 `json:"day_ns"`
}
type CPULimitedSessionStart struct {
	Start                  CPUSessionStart `json:"start"`
	ExpectedPolicyRevision int64           `json:"expected_policy_revision"`
	Limits                 CPUChargeLimits `json:"limits"`
}
type CPUTrackingGapRequest struct {
	ExpectedPolicyRevision int64     `json:"expected_policy_revision"`
	ExpectedGeneration     int64     `json:"expected_generation"`
	Nonce                  string    `json:"nonce"`
	ObservedAt             time.Time `json:"observed_at"`
}
type CPUTrackingGapReceipt struct {
	Request         CPUTrackingGapRequest `json:"request"`
	RequestID       string                `json:"request_id"`
	AppliedRevision int64                 `json:"applied_revision"`
}

// A distinct opaque marker binds this open writer, generation and frozen policy.
// It is accounting continuation, not handler/source permission.
type CPULimitedSessionMarker struct {
	store      *Store
	contract   string
	generation int64
	revision   int64
	request    CPULimitedSessionStart
}

func (m CPULimitedSessionMarker) Generation() int64     { return m.generation }
func (m CPULimitedSessionMarker) PolicyRevision() int64 { return m.revision }
func (m CPULimitedSessionMarker) Nonce() string         { return m.request.Start.Nonce }

type CPUChargePeriod struct {
	StartNS              int64 `json:"start_ns"`
	NextBoundaryNS       int64 `json:"next_boundary_ns"`
	AssignedCPUNS        int64 `json:"assigned_cpu_ns"`
	RetiredAssignedCPUNS int64 `json:"retired_assigned_cpu_ns"`
	PartialTracking      bool  `json:"partial_tracking"`
	BlockingUnknown      bool  `json:"blocking_unknown"`
}

// This is a saved historical projection. Endpoint-assigned charges can overlap
// old prefixes and span periods; no field grants current work permission.
type CPUChargeAdmissionState struct {
	Contract                       string                  `json:"contract"`
	Scope                          string                  `json:"scope"`
	Version                        int                     `json:"version"`
	Available                      bool                    `json:"available"`
	EndpointAssignedCharges        bool                    `json:"endpoint_assigned_charges"`
	PrefixOverlapPossible          bool                    `json:"prefix_overlap_possible"`
	CurrentWorkPermissionEvaluated bool                    `json:"current_work_permission_evaluated"`
	WorkPermissionGranted          bool                    `json:"work_permission_granted"`
	PhysicalPeriodCPUVerified      bool                    `json:"physical_period_cpu_verified"`
	GlobalCPUQuotaVerified         bool                    `json:"global_cpu_quota_verified"`
	FullProcessCPUVerified         bool                    `json:"full_process_cpu_verified"`
	ActivatedAt                    *time.Time              `json:"activated_at"`
	ClockHighWater                 *time.Time              `json:"clock_high_water"`
	ActivationBaselineCPUNS        int64                   `json:"activation_baseline_cpu_ns"`
	PolicyRevision                 int64                   `json:"policy_revision"`
	Limits                         CPUChargeLimits         `json:"limits"`
	Hour                           CPUChargePeriod         `json:"hour"`
	Day                            CPUChargePeriod         `json:"day"`
	CPUGeneration                  int64                   `json:"cpu_generation"`
	CPUOrdinal                     int64                   `json:"cpu_ordinal"`
	CPUStateDigest                 string                  `json:"cpu_state_digest"`
	BeginRequest                   *CPULimitedSessionStart `json:"begin_request"`
	BeginAppliedRevision           int64                   `json:"begin_applied_revision"`
	TrackingGapOpen                bool                    `json:"tracking_gap_open"`
	GapOpenedAt                    *time.Time              `json:"gap_opened_at"`
	GapClosedAt                    *time.Time              `json:"gap_closed_at"`
	LatestGap                      *CPUTrackingGapReceipt  `json:"latest_gap"`
}

// Frozen request identity is diagnostic evidence, not confirmation of publication.
type CPUChargeAdmissionPublicationError struct {
	Operation string
	RequestID string
	Cause     error
}

func (e *CPUChargeAdmissionPublicationError) Error() string {
	return ErrCPUChargeAdmissionPublication.Error() + "; operation " + e.Operation + "; request " + e.RequestID
}
func (e *CPUChargeAdmissionPublicationError) Unwrap() error {
	return errors.Join(ErrCPUChargeAdmissionPublication, ErrCPUChargesPublication, e.Cause)
}

type cpuAdmissionHooks struct {
	beforeCommit func()
	afterCommit  func()
	noOpReleased func()
	commit       func(*sql.Tx) error
}

func cpuAdmissionBase(available bool) CPUChargeAdmissionState {
	return CPUChargeAdmissionState{Contract: CPUChargeAdmissionContract, Scope: CPUChargeAdmissionScope, Version: 1, Available: available, EndpointAssignedCharges: true, PrefixOverlapPossible: true}
}
func cloneCPULimitedStart(q CPULimitedSessionStart) CPULimitedSessionStart {
	q.Start = cloneCPUStart(q.Start)
	return q
}
func cloneCPUAdmission(a CPUChargeAdmissionState) CPUChargeAdmissionState {
	if a.ActivatedAt != nil {
		v := *a.ActivatedAt
		a.ActivatedAt = &v
	}
	if a.ClockHighWater != nil {
		v := *a.ClockHighWater
		a.ClockHighWater = &v
	}
	if a.GapOpenedAt != nil {
		v := *a.GapOpenedAt
		a.GapOpenedAt = &v
	}
	if a.GapClosedAt != nil {
		v := *a.GapClosedAt
		a.GapClosedAt = &v
	}
	if a.BeginRequest != nil {
		v := cloneCPULimitedStart(*a.BeginRequest)
		a.BeginRequest = &v
	}
	if a.LatestGap != nil {
		v := *a.LatestGap
		a.LatestGap = &v
	}
	return a
}
func cpuAdmissionLimitsValid(l CPUChargeLimits) bool {
	return l.HourNS >= 0 && l.HourNS <= int64(time.Hour) && l.DayNS >= 0 && l.DayNS <= int64(24*time.Hour)
}
func cpuAdmissionTime(at time.Time) bool {
	if !cpuChargesTime(at) {
		return false
	}
	_, _, err := cpuAdmissionSlots(at)
	return err == nil
}
func cpuAdmissionSlots(at time.Time) (CPUChargePeriod, CPUChargePeriod, error) {
	n := at.UnixNano()
	if !cpuChargesTime(at) {
		return CPUChargePeriod{}, CPUChargePeriod{}, ErrCPUChargeAdmissionInvalid
	}
	slot := func(d int64) (CPUChargePeriod, error) {
		start := n - n%d
		if start > math.MaxInt64-d {
			return CPUChargePeriod{}, ErrCPUChargesOverflow
		}
		return CPUChargePeriod{StartNS: start, NextBoundaryNS: start + d}, nil
	}
	h, e := slot(int64(time.Hour))
	if e != nil {
		return h, CPUChargePeriod{}, e
	}
	d, e := slot(int64(24 * time.Hour))
	return h, d, e
}
func cpuAdmissionID(prefix string, v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return prefix + hex.EncodeToString(h[:])
}
func cpuStateDigest(c CPUChargeState) string { return cpuAdmissionID("cpu-state-v1-", c) }

// ValidCPUTrackingGapRequestID checks only the bounded diagnostic spelling.
func ValidCPUTrackingGapRequestID(id string) bool {
	const prefix = "cpu-tracking-gap-v1-"
	return len(id) == len(prefix)+64 && strings.HasPrefix(id, prefix) && metadataDigest(id[len(prefix):], 64)
}
func validCPUGapRequest(q CPUTrackingGapRequest) bool {
	return q.ExpectedGeneration >= 0 && q.ExpectedPolicyRevision >= 0 && metadataDigest(q.Nonce, 64) && cpuAdmissionTime(q.ObservedAt)
}
func sameCPUGapRequest(a, b CPUTrackingGapRequest) bool {
	return a.ExpectedGeneration == b.ExpectedGeneration && a.ExpectedPolicyRevision == b.ExpectedPolicyRevision && a.Nonce == b.Nonce && a.ObservedAt.Equal(b.ObservedAt)
}
func sameCPULimitedStart(a, b CPULimitedSessionStart) bool {
	return a.ExpectedPolicyRevision == b.ExpectedPolicyRevision && a.Limits == b.Limits && sameCPUStart(a.Start, b.Start)
}
func validCPULimitedStart(q CPULimitedSessionStart) bool {
	return q.ExpectedPolicyRevision >= 0 && cpuAdmissionLimitsValid(q.Limits) && cpuChargesStartValid(q.Start) && cpuAdmissionTime(q.Start.ObservedAt)
}
func bindCPUAdmission(a *CPUChargeAdmissionState, c CPUChargeState) {
	a.CPUGeneration = c.Generation
	a.CPUOrdinal = 0
	if c.Session != nil {
		a.CPUOrdinal = c.Session.LastOrdinal
	}
	a.CPUStateDigest = cpuStateDigest(c)
}
func validCPUAdmission(a CPUChargeAdmissionState, c CPUChargeState) bool {
	if a.Contract != CPUChargeAdmissionContract || a.Scope != CPUChargeAdmissionScope || a.Version != 1 || !a.Available || !a.EndpointAssignedCharges || !a.PrefixOverlapPossible || a.CurrentWorkPermissionEvaluated || a.WorkPermissionGranted || a.PhysicalPeriodCPUVerified || a.GlobalCPUQuotaVerified || a.FullProcessCPUVerified || !validCPUCharges(c) || a.ActivatedAt == nil || a.ClockHighWater == nil || !cpuAdmissionTime(*a.ActivatedAt) || !cpuAdmissionTime(*a.ClockHighWater) || a.ActivatedAt.Before(*c.ActivatedAt) || a.ClockHighWater.Before(*a.ActivatedAt) || a.ClockHighWater.Before(*c.ClockHighWater) || a.ActivationBaselineCPUNS < 0 || a.ActivationBaselineCPUNS > c.ChargedCPUNS || a.PolicyRevision < 0 || !cpuAdmissionLimitsValid(a.Limits) || a.CPUGeneration != c.Generation || a.CPUStateDigest != cpuStateDigest(c) {
		return false
	}
	ordinal := int64(0)
	if c.Session != nil {
		ordinal = c.Session.LastOrdinal
	}
	if ordinal != a.CPUOrdinal {
		return false
	}
	h, d, e := cpuAdmissionSlots(*a.ClockHighWater)
	if e != nil {
		return false
	}
	for _, pair := range [][2]CPUChargePeriod{{a.Hour, h}, {a.Day, d}} {
		s, w := pair[0], pair[1]
		if s.StartNS != w.StartNS || s.NextBoundaryNS != w.NextBoundaryNS || s.AssignedCPUNS < 0 || s.RetiredAssignedCPUNS < 0 || s.RetiredAssignedCPUNS > math.MaxInt64-a.ActivationBaselineCPUNS || s.AssignedCPUNS > math.MaxInt64-a.ActivationBaselineCPUNS-s.RetiredAssignedCPUNS || a.ActivationBaselineCPUNS+s.RetiredAssignedCPUNS+s.AssignedCPUNS != c.ChargedCPUNS {
			return false
		}
	}
	if a.PolicyRevision == 0 && (a.BeginRequest != nil || a.LatestGap != nil || !a.ClockHighWater.Equal(*a.ActivatedAt) || a.ActivationBaselineCPUNS != c.ChargedCPUNS) {
		return false
	}
	activationHour, activationDay, err := cpuAdmissionSlots(*a.ActivatedAt)
	if err != nil {
		return false
	}
	for _, p := range []struct {
		slot       CPUChargePeriod
		activation CPUChargePeriod
		duration   int64
	}{{a.Hour, activationHour, int64(time.Hour)}, {a.Day, activationDay, int64(24 * time.Hour)}} {
		if p.slot.StartNS == p.activation.StartNS && !p.slot.PartialTracking {
			return false
		}
		if a.GapClosedAt != nil && a.GapClosedAt.UnixNano()/p.duration == p.slot.StartNS/p.duration && !p.slot.PartialTracking {
			return false
		}
		if a.BeginRequest != nil && c.Session != nil && (c.Status == "unknown" || c.Status == "recovered_unknown") && c.Session.ClosedAt != nil && c.Session.ClosedAt.UnixNano()/p.duration == p.slot.StartNS/p.duration && !p.slot.BlockingUnknown {
			return false
		}
	}
	if a.BeginRequest == nil {
		if a.BeginAppliedRevision != 0 || a.Limits.HourNS == 0 && a.Limits.DayNS == 0 {
			return false
		}
	} else {
		q := a.BeginRequest
		if !validCPULimitedStart(*q) || q.ExpectedPolicyRevision == math.MaxInt64 || q.ExpectedPolicyRevision+1 != a.BeginAppliedRevision || a.BeginAppliedRevision > a.PolicyRevision || q.Limits != a.Limits || c.Session == nil || !sameCPUStart(q.Start, c.Session.Start) {
			return false
		}
	}
	if a.LatestGap == nil {
		return !a.TrackingGapOpen && a.GapOpenedAt == nil && a.GapClosedAt == nil && (a.PolicyRevision == 0 || a.BeginRequest != nil && a.BeginAppliedRevision == a.PolicyRevision)
	}
	r := a.LatestGap
	if !validCPUGapRequest(r.Request) || r.Request.ExpectedPolicyRevision == math.MaxInt64 || r.AppliedRevision != r.Request.ExpectedPolicyRevision+1 || r.AppliedRevision > a.PolicyRevision || r.Request.ExpectedGeneration > c.Generation || r.RequestID != cpuAdmissionID("cpu-tracking-gap-v1-", r.Request) || a.GapOpenedAt == nil || !a.GapOpenedAt.Equal(r.Request.ObservedAt) || a.GapOpenedAt.After(*a.ClockHighWater) || a.GapOpenedAt.Before(*a.ActivatedAt) {
		return false
	}
	if a.TrackingGapOpen {
		return r.AppliedRevision == a.PolicyRevision && r.Request.ExpectedGeneration == c.Generation && a.GapClosedAt == nil && a.Hour.PartialTracking && a.Day.PartialTracking && a.Hour.BlockingUnknown && a.Day.BlockingUnknown && c.Status != "active"
	}
	return a.GapClosedAt != nil && cpuAdmissionTime(*a.GapClosedAt) && !a.GapClosedAt.Before(*a.GapOpenedAt) && !a.GapClosedAt.After(*a.ClockHighWater) && a.BeginRequest != nil && a.BeginAppliedRevision == a.PolicyRevision && !a.BeginRequest.Start.ObservedAt.Before(*a.GapClosedAt) && a.BeginAppliedRevision > r.AppliedRevision
}

// Shapes and rows returned to Go are finite. SQLite's own schema parsing and
// page examination are separate and only cooperatively deadline-bound.
func cpuChargeAdmissionSchema(ctx context.Context, tx *sql.Tx, version int) error {
	var shadow bool
	if e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_temp_master WHERE name='worker_cpu_charge_admission' OR tbl_name='worker_cpu_charge_admission')`).Scan(&shadow); e != nil || shadow {
		return errors.Join(ErrCPUChargeAdmissionCorrupt, e)
	}
	var kind, shape string
	var bounded bool
	err := tx.QueryRowContext(ctx, `SELECT substr(type,1,17),CASE WHEN typeof(sql)='text' THEN CAST(substr(CAST(sql AS BLOB),1,?) AS TEXT) ELSE '' END,typeof(sql)='text' AND length(CAST(sql AS BLOB))<=? FROM sqlite_master WHERE name='worker_cpu_charge_admission'`, len(migration16)+1, len(migration16)).Scan(&kind, &shape, &bounded)
	if version < 16 {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return errors.Join(ErrCPUChargeAdmissionCorrupt, err)
	}
	if err != nil || !bounded || kind != "table" || strings.TrimSuffix(strings.TrimSpace(shape), ";") != strings.TrimSuffix(strings.TrimSpace(migration16), ";") {
		return errors.Join(ErrCPUChargeAdmissionCorrupt, err)
	}
	var extra bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE tbl_name='worker_cpu_charge_admission' AND name!='worker_cpu_charge_admission' UNION ALL SELECT 1 FROM sqlite_temp_master WHERE tbl_name='worker_cpu_charge_admission' LIMIT 1)`).Scan(&extra); err != nil || extra {
		return errors.Join(ErrCPUChargeAdmissionCorrupt, err)
	}
	return nil
}
func readCPUAdmission(ctx context.Context, tx *sql.Tx, c CPUChargeState, version int) (CPUChargeAdmissionState, error) {
	if version < 16 {
		return cpuAdmissionBase(false), nil
	}
	var revision int64
	var raw []byte
	var typed bool
	err := tx.QueryRowContext(ctx, `SELECT CASE WHEN typeof(policy_revision)='integer' THEN policy_revision ELSE -1 END,CASE WHEN typeof(state_json)='blob' THEN substr(state_json,1,4097) ELSE X'' END,typeof(singleton)='integer' AND singleton=1 AND typeof(policy_revision)='integer' AND typeof(state_json)='blob' AND length(state_json) BETWEEN 1 AND 4096 AND NOT EXISTS(SELECT 1 FROM worker_cpu_charge_admission LIMIT 1 OFFSET 1) FROM worker_cpu_charge_admission LIMIT 1`).Scan(&revision, &raw, &typed)
	if err != nil || !typed || len(raw) > CPUChargeAdmissionMaxJSONBytes {
		return cpuAdmissionBase(true), errors.Join(ErrCPUChargeAdmissionCorrupt, err)
	}
	var a CPUChargeAdmissionState
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var at int64
	var atTyped bool
	if err = tx.QueryRowContext(ctx, `SELECT CASE WHEN typeof(applied_at_ns)='integer' THEN applied_at_ns ELSE 0 END,typeof(applied_at_ns)='integer' FROM schema_migrations WHERE version=16`).Scan(&at, &atTyped); err != nil {
		return cpuAdmissionBase(true), errors.Join(ErrCPUChargeAdmissionCorrupt, err)
	}
	if dec.Decode(&a) != nil || !validCPUAdmission(a, c) || a.PolicyRevision != revision || !atTyped || a.ActivatedAt.UnixNano() != at {
		return cpuAdmissionBase(true), ErrCPUChargeAdmissionCorrupt
	}
	canon, _ := json.Marshal(a)
	if !bytes.Equal(raw, canon) {
		return cpuAdmissionBase(true), ErrCPUChargeAdmissionCorrupt
	}
	return a, nil
}
func writeCPUAdmission(ctx context.Context, tx *sql.Tx, c CPUChargeState, a CPUChargeAdmissionState) error {
	if !validCPUAdmission(a, c) {
		return ErrCPUChargeAdmissionInvalid
	}
	b, e := json.Marshal(a)
	if e != nil || len(b) > CPUChargeAdmissionMaxJSONBytes {
		return ErrCPUChargesOverflow
	}
	_, e = tx.ExecContext(ctx, `INSERT OR REPLACE INTO worker_cpu_charge_admission(singleton,policy_revision,state_json) VALUES(1,?,?)`, a.PolicyRevision, b)
	return e
}
func rollCPUAdmission(a *CPUChargeAdmissionState, at time.Time, charge int64, unknown bool) error {
	if charge < 0 || at.Before(*a.ClockHighWater) {
		return ErrCPUChargesClockRollback
	}
	h, d, e := cpuAdmissionSlots(at)
	if e != nil {
		return e
	}
	for _, p := range []struct {
		s     *CPUChargePeriod
		fresh CPUChargePeriod
	}{{&a.Hour, h}, {&a.Day, d}} {
		if p.s.StartNS != p.fresh.StartNS {
			if p.s.AssignedCPUNS > math.MaxInt64-p.s.RetiredAssignedCPUNS {
				return ErrCPUChargesOverflow
			}
			p.fresh.RetiredAssignedCPUNS = p.s.RetiredAssignedCPUNS + p.s.AssignedCPUNS
			*p.s = p.fresh
		}
		if charge > math.MaxInt64-p.s.AssignedCPUNS {
			return ErrCPUChargesOverflow
		}
		p.s.AssignedCPUNS += charge
		if unknown {
			p.s.BlockingUnknown = true
		}
	}
	a.ClockHighWater = &at
	return nil
}
