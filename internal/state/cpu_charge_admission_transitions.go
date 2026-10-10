package state

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"
)

// Bare mutations never bypass active period coherence. Recheck after the sole
// connection was acquired: queued callers may have passed an earlier check.
func (s *Store) bareCPUAuthority(ctx context.Context, tx *sql.Tx) error {
	if s.cpuAuthorityRefused.Load() {
		return ErrCPUChargeAdmissionAuthority
	}
	var v int
	if e := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); e != nil {
		return e
	}
	if v >= cpuChargeAdmissionSchemaVersion {
		return ErrCPUChargeAdmissionPolicyRequired
	}
	return nil
}

type cpuAdmissionOperation struct {
	store  *Store
	ctx    context.Context
	cancel context.CancelFunc
	conn   *sql.Conn
	tx     *sql.Tx
	hooks  cpuAdmissionHooks
}

func (s *Store) cpuAdmissionOperation(parent context.Context, write bool, hooks cpuAdmissionHooks) (*cpuAdmissionOperation, error) {
	if parent == nil {
		return nil, ErrCPUChargeAdmissionInvalid
	}
	if e := parent.Err(); e != nil {
		return nil, e
	}
	if s == nil || s.db == nil || write && s.readOnly {
		return nil, ErrCPUChargesReadOnly
	}
	if write && s.cpuAuthorityRefused.Load() {
		return nil, ErrCPUChargeAdmissionAuthority
	}
	ctx, cancel := context.WithTimeout(parent, cpuAdmissionWindow)
	conn, e := s.db.Conn(ctx)
	if e != nil {
		cancel()
		return nil, e
	}
	tx, e := conn.BeginTx(ctx, nil)
	if e != nil {
		conn.Close()
		cancel()
		return nil, e
	}
	o := &cpuAdmissionOperation{s, ctx, cancel, conn, tx, hooks}
	if write && s.cpuAuthorityRefused.Load() {
		o.close()
		return nil, ErrCPUChargeAdmissionAuthority
	}
	return o, nil
}
func (o *cpuAdmissionOperation) close() { _ = o.tx.Rollback(); _ = o.conn.Close(); o.cancel() }
func (o *cpuAdmissionOperation) read() (CPUChargeState, CPUChargeAdmissionState, int, error) {
	c, e := readCPUCharges(o.ctx, o.tx)
	if e != nil {
		return c, cpuAdmissionBase(false), 0, e
	}
	var v int
	if e = o.tx.QueryRowContext(o.ctx, "PRAGMA user_version").Scan(&v); e != nil {
		return c, cpuAdmissionBase(false), 0, e
	}
	a, e := readCPUAdmission(o.ctx, o.tx, c, v)
	return c, a, v, e
}
func (o *cpuAdmissionOperation) view(c CPUChargeState, a CPUChargeAdmissionState, outcome error) (CPUChargeState, CPUChargeAdmissionState, error) {
	if e := o.ctx.Err(); e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	if e := o.tx.Commit(); e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	if e := o.conn.Close(); e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	if o.hooks.noOpReleased != nil {
		o.hooks.noOpReleased()
	}
	if e := o.ctx.Err(); e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	return cloneCPUCharges(c), cloneCPUAdmission(a), outcome
}
func (o *cpuAdmissionOperation) publish(prior, next CPUChargeState, pa, na CPUChargeAdmissionState, operation, id string) (CPUChargeState, CPUChargeAdmissionState, error) {
	if o.store.cpuAuthorityRefused.Load() {
		return prior, pa, ErrCPUChargeAdmissionAuthority
	}
	if e := writeCPUCharges(o.ctx, o.tx, next); e != nil {
		return prior, pa, e
	}
	if e := writeCPUAdmission(o.ctx, o.tx, next, na); e != nil {
		return prior, pa, e
	}
	if o.hooks.beforeCommit != nil {
		o.hooks.beforeCommit()
	}
	if e := o.ctx.Err(); e != nil {
		return prior, pa, e
	}
	commit := o.tx.Commit
	if o.hooks.commit != nil {
		commit = func() error { return o.hooks.commit(o.tx) }
	}
	e := commit()
	if o.hooks.afterCommit != nil {
		o.hooks.afterCommit()
	}
	if e != nil || o.ctx.Err() != nil {
		// Still holding the sole connection: no queued CPU mutation can pass its
		// transactional authority recheck before this permanent atomic latch.
		o.store.cpuAuthorityRefused.Store(true)
		return prior, pa, &CPUChargeAdmissionPublicationError{Operation: operation, RequestID: id, Cause: errors.Join(e, o.ctx.Err())}
	}
	return cloneCPUCharges(next), cloneCPUAdmission(na), nil
}
func cpuAdmissionOutcome(c CPUChargeState) error {
	if c.Status == "unknown" || c.Status == "recovered_unknown" {
		if c.Session.ClosureReason == "cpu_clock_rollback" {
			return ErrCPUChargesClockRollback
		}
		return ErrCPUChargesUnknown
	}
	return nil
}
func (s *Store) CPUChargeAdmission(ctx context.Context) (CPUChargeAdmissionState, error) {
	o, e := s.cpuAdmissionOperation(ctx, false, cpuAdmissionHooks{})
	if e != nil {
		return cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, _, e := o.read()
	if e != nil {
		return cpuAdmissionBase(false), e
	}
	_, a, e = o.view(c, a, nil)
	return a, e
}
func (s *Store) ActivateCPUChargeAdmission(ctx context.Context, at time.Time, limits CPUChargeLimits) (CPUChargeAdmissionState, error) {
	return s.activateCPUChargeAdmission(ctx, at, limits, cpuAdmissionHooks{})
}
func (s *Store) activateCPUChargeAdmission(ctx context.Context, at time.Time, limits CPUChargeLimits, hooks cpuAdmissionHooks) (CPUChargeAdmissionState, error) {
	if !cpuAdmissionLimitsValid(limits) || !cpuAdmissionTime(at) {
		return cpuAdmissionBase(false), ErrCPUChargeAdmissionInvalid
	}
	o, e := s.cpuAdmissionOperation(ctx, true, hooks)
	if e != nil {
		return cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, v, e := o.read()
	if e != nil {
		return cpuAdmissionBase(false), e
	}
	if v == 16 {
		_, a, e = o.view(c, a, nil)
		return a, e
	}
	if v != 15 || !c.Available {
		return a, ErrCPUChargeAdmissionUnavailable
	}
	if limits.HourNS == 0 && limits.DayNS == 0 {
		return a, ErrCPUChargeAdmissionInvalid
	}
	if c.Status == "active" {
		return a, ErrCPUChargesStale
	}
	if at.Before(*c.ClockHighWater) {
		return a, ErrCPUChargesClockRollback
	}
	h, d, e := cpuAdmissionSlots(at)
	if e != nil {
		return a, e
	}
	h.PartialTracking = true
	d.PartialTracking = true
	na := cpuAdmissionBase(true)
	na.ActivatedAt = &at
	na.ClockHighWater = &at
	na.ActivationBaselineCPUNS = c.ChargedCPUNS
	na.Hour = h
	na.Day = d
	na.Limits = limits
	bindCPUAdmission(&na, c)
	if _, e = o.tx.ExecContext(o.ctx, migration16); e != nil {
		return a, e
	}
	if _, e = o.tx.ExecContext(o.ctx, "INSERT INTO schema_migrations VALUES(?,?,?)", 16, migrations[15].name, at.UnixNano()); e != nil {
		return a, e
	}
	if _, e = o.tx.ExecContext(o.ctx, "PRAGMA user_version=16"); e != nil {
		return a, e
	}
	id := cpuAdmissionID("cpu-admission-activate-v1-", struct {
		At     time.Time
		Limits CPUChargeLimits
	}{at, limits})
	_, na, e = o.publish(c, c, a, na, "activate", id)
	return na, e
}
func (s *Store) BeginCPUSessionLimited(ctx context.Context, q CPULimitedSessionStart) (CPULimitedSessionMarker, CPUChargeState, CPUChargeAdmissionState, error) {
	return s.beginCPUSessionLimited(ctx, q, cpuAdmissionHooks{})
}
func (s *Store) beginCPUSessionLimited(ctx context.Context, q CPULimitedSessionStart, hooks cpuAdmissionHooks) (CPULimitedSessionMarker, CPUChargeState, CPUChargeAdmissionState, error) {
	var empty CPULimitedSessionMarker
	q = cloneCPULimitedStart(q)
	if !validCPULimitedStart(q) {
		return empty, cpuChargesBase(false), cpuAdmissionBase(false), ErrCPUChargeAdmissionInvalid
	}
	o, e := s.cpuAdmissionOperation(ctx, true, hooks)
	if e != nil {
		return empty, cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, v, e := o.read()
	if e != nil {
		return empty, c, a, e
	}
	if v != 16 {
		return empty, c, a, ErrCPUChargeAdmissionUnavailable
	}
	if a.BeginRequest != nil && sameCPULimitedStart(*a.BeginRequest, q) {
		c, a, e = o.view(c, a, cpuAdmissionOutcome(c))
		return empty, c, a, e
	}
	if q.Start.ExpectedGeneration != c.Generation || q.ExpectedPolicyRevision != a.PolicyRevision {
		return empty, c, a, ErrCPUChargesStale
	}
	if q.Start.ObservedAt.Before(*a.ClockHighWater) {
		return empty, c, a, ErrCPUChargesClockRollback
	}
	if c.Status == "active" {
		return empty, c, a, ErrCPUChargesStale
	}
	if c.Generation == math.MaxInt64 || a.PolicyRevision == math.MaxInt64 {
		return empty, c, a, ErrCPUChargesOverflow
	}
	nc, e := cpuLimitedBeginState(c, q.Start)
	if e != nil {
		return empty, c, a, e
	}
	na := cloneCPUAdmission(a)
	na.PolicyRevision++
	na.Limits = q.Limits
	na.BeginRequest = &q
	na.BeginAppliedRevision = na.PolicyRevision
	if e = rollCPUAdmission(&na, q.Start.ObservedAt, nc.ChargedCPUNS-c.ChargedCPUNS, nc.Status == "unknown"); e != nil {
		return empty, c, a, e
	}
	if na.TrackingGapOpen {
		na.TrackingGapOpen = false
		at := q.Start.ObservedAt
		na.GapClosedAt = &at
		na.Hour.PartialTracking = true
		na.Day.PartialTracking = true
	}
	bindCPUAdmission(&na, nc)
	marker := CPULimitedSessionMarker{s, cpuLimitedMarkerContract, nc.Generation, na.PolicyRevision, cloneCPULimitedStart(q)}
	nc, na, e = o.publish(c, nc, a, na, "begin", cpuAdmissionID("cpu-limited-begin-v1-", q))
	if e != nil {
		if errors.Is(e, ErrCPUChargeAdmissionPublication) {
			return marker, nc, na, e
		}
		return empty, nc, na, e
	}
	return marker, nc, na, cpuAdmissionOutcome(nc)
}
func cpuLimitedBeginState(prior CPUChargeState, start CPUSessionStart) (CPUChargeState, error) {
	next := cloneCPUCharges(prior)
	next.Generation++
	next.ClockHighWater = &start.ObservedAt
	next.Status = "active"
	next.OpenTailUnobserved = true
	next.Session = &CPUSessionRecord{Generation: next.Generation, Start: cloneCPUStart(start), Status: "active"}
	problem := cpuChargesProblem(start.SelfCPUNS, start.Reason, nil, false)
	if problem != "" {
		if e := cpuChargesClose(&next, start.ObservedAt, "unknown", problem, false); e != nil {
			return prior, e
		}
		return next, nil
	}
	charge := *start.SelfCPUNS
	if charge > math.MaxInt64-next.ChargedCPUNS {
		return prior, ErrCPUChargesOverflow
	}
	next.ChargedCPUNS += charge
	r := next.Session
	r.InitialPrefixChargeNS = charge
	r.LastChargeNS = charge
	r.SessionChargedCPUNS = charge
	r.LastSelfCPUNS = cloneCPUNS(start.SelfCPUNS)
	wait, capped := CPUChargesPrefixWait(time.Duration(charge))
	if e := cpuChargesDebt(&next, start.ObservedAt, wait, capped); e != nil {
		return prior, e
	}
	return next, nil
}
func (s *Store) SampleCPUSessionLimited(ctx context.Context, m CPULimitedSessionMarker, q CPUSessionSample) (CPUChargeState, CPUChargeAdmissionState, error) {
	return s.sampleCPUSessionLimited(ctx, m, q, false, cpuAdmissionHooks{})
}
func (s *Store) FinishCPUSessionLimited(ctx context.Context, m CPULimitedSessionMarker, q CPUSessionSample) (CPUChargeState, CPUChargeAdmissionState, error) {
	return s.sampleCPUSessionLimited(ctx, m, q, true, cpuAdmissionHooks{})
}
func (s *Store) sampleCPUSessionLimited(ctx context.Context, m CPULimitedSessionMarker, q CPUSessionSample, finish bool, hooks cpuAdmissionHooks) (CPUChargeState, CPUChargeAdmissionState, error) {
	q = cloneCPUSample(q)
	if m.store != s || m.contract != cpuLimitedMarkerContract || m.generation <= 0 || m.revision <= 0 || !validCPULimitedStart(m.request) || !cpuChargesSampleValid(q) || !cpuAdmissionTime(q.ObservedAt) {
		return cpuChargesBase(false), cpuAdmissionBase(false), ErrCPUChargesStale
	}
	o, e := s.cpuAdmissionOperation(ctx, true, hooks)
	if e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, v, e := o.read()
	if e != nil {
		return c, a, e
	}
	if v != 16 {
		return c, a, ErrCPUChargeAdmissionUnavailable
	}
	if a.TrackingGapOpen || a.PolicyRevision != m.revision || c.Generation != m.generation || a.BeginRequest == nil || !sameCPULimitedStart(*a.BeginRequest, m.request) {
		return c, a, ErrCPUChargesStale
	}
	r := c.Session
	if r.LastSample != nil && q.Ordinal == r.LastOrdinal && r.LastSampleFinal == finish && sameCPUSample(*r.LastSample, q) {
		return o.view(c, a, cpuAdmissionOutcome(c))
	}
	if r.LastOrdinal == math.MaxInt64 {
		return c, a, ErrCPUChargesOverflow
	}
	if r.Status != "active" || q.Ordinal != r.LastOrdinal+1 {
		return c, a, ErrCPUChargesStale
	}
	nc, e := cpuLimitedSampleState(c, q, finish, *a.ClockHighWater)
	if e != nil {
		return c, a, e
	}
	na := cloneCPUAdmission(a)
	at := *nc.ClockHighWater
	if e = rollCPUAdmission(&na, at, nc.ChargedCPUNS-c.ChargedCPUNS, nc.Status == "unknown"); e != nil {
		return c, a, e
	}
	bindCPUAdmission(&na, nc)
	id := cpuAdmissionID("cpu-limited-sample-v1-", struct {
		Start  CPULimitedSessionStart
		Sample CPUSessionSample
		Final  bool
	}{m.request, q, finish})
	nc, na, e = o.publish(c, nc, a, na, "sample", id)
	if e != nil {
		return nc, na, e
	}
	return nc, na, cpuAdmissionOutcome(nc)
}
func cpuLimitedSampleState(prior CPUChargeState, q CPUSessionSample, finish bool, high time.Time) (CPUChargeState, error) {
	next := cloneCPUCharges(prior)
	r := next.Session
	r.LastSample = &q
	r.LastSampleFinal = finish
	r.LastOrdinal = q.Ordinal
	problem := cpuChargesProblem(q.SelfCPUNS, q.Reason, q.ElapsedNS, true)
	at := q.ObservedAt
	if at.Before(high) {
		problem = "cpu_clock_rollback"
		at = high
	}
	next.ClockHighWater = &at
	if problem == "" && *q.SelfCPUNS < *r.LastSelfCPUNS {
		problem = "cpu_observation_regressed"
	}
	if problem != "" {
		if e := cpuChargesClose(&next, at, "unknown", problem, false); e != nil {
			return prior, e
		}
		return next, nil
	}
	delta := *q.SelfCPUNS - *r.LastSelfCPUNS
	if delta > math.MaxInt64-next.ChargedCPUNS {
		return prior, ErrCPUChargesOverflow
	}
	next.ChargedCPUNS += delta
	r.LastChargeNS = delta
	r.SessionChargedCPUNS = *q.SelfCPUNS
	r.LastSelfCPUNS = cloneCPUNS(q.SelfCPUNS)
	wait, capped := CPUFeedbackWait(time.Duration(delta), time.Duration(*q.ElapsedNS))
	if e := cpuChargesDebt(&next, at, wait, capped); e != nil {
		return prior, e
	}
	if finish {
		if e := cpuChargesClose(&next, at, "finished", "graceful_tail_unobserved", false); e != nil {
			return prior, e
		}
	}
	return next, nil
}
func (s *Store) RecoverCPUSessionLimited(ctx context.Context, at time.Time) (CPUChargeState, CPUChargeAdmissionState, error) {
	return s.recoverCPUSessionLimited(ctx, at, cpuAdmissionHooks{})
}
func (s *Store) recoverCPUSessionLimited(ctx context.Context, at time.Time, hooks cpuAdmissionHooks) (CPUChargeState, CPUChargeAdmissionState, error) {
	if !cpuAdmissionTime(at) {
		return cpuChargesBase(false), cpuAdmissionBase(false), ErrCPUChargeAdmissionInvalid
	}
	o, e := s.cpuAdmissionOperation(ctx, true, hooks)
	if e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, v, e := o.read()
	if e != nil {
		return c, a, e
	}
	if v != 16 {
		return c, a, ErrCPUChargeAdmissionUnavailable
	}
	if c.Status != "active" {
		return o.view(c, a, nil)
	}
	if at.Before(*a.ClockHighWater) {
		return c, a, ErrCPUChargesClockRollback
	}
	nc := cloneCPUCharges(c)
	nc.ClockHighWater = &at
	if e = cpuChargesClose(&nc, at, "recovered_unknown", "interrupted_session", true); e != nil {
		return c, a, e
	}
	na := cloneCPUAdmission(a)
	if e = rollCPUAdmission(&na, at, 0, true); e != nil {
		return c, a, e
	}
	bindCPUAdmission(&na, nc)
	return o.publish(c, nc, a, na, "recover", cpuAdmissionID("cpu-limited-recover-v1-", struct {
		Generation int64
		Revision   int64
		At         time.Time
	}{c.Generation, a.PolicyRevision, at}))
}
func (s *Store) OpenCPUTrackingGap(ctx context.Context, q CPUTrackingGapRequest) (CPUChargeState, CPUChargeAdmissionState, error) {
	return s.openCPUTrackingGap(ctx, q, cpuAdmissionHooks{})
}
func (s *Store) openCPUTrackingGap(ctx context.Context, q CPUTrackingGapRequest, hooks cpuAdmissionHooks) (CPUChargeState, CPUChargeAdmissionState, error) {
	if !validCPUGapRequest(q) {
		return cpuChargesBase(false), cpuAdmissionBase(false), ErrCPUChargeAdmissionInvalid
	}
	o, e := s.cpuAdmissionOperation(ctx, true, hooks)
	if e != nil {
		return cpuChargesBase(false), cpuAdmissionBase(false), e
	}
	defer o.close()
	c, a, v, e := o.read()
	if e != nil {
		return c, a, e
	}
	if v != 16 {
		return c, a, ErrCPUChargeAdmissionUnavailable
	}
	// The original expected revision is one below applied; retry identity and
	// current open-gap binding must therefore precede the ordinary stale test.
	if a.TrackingGapOpen && a.LatestGap != nil && a.PolicyRevision == a.LatestGap.AppliedRevision && sameCPUGapRequest(a.LatestGap.Request, q) {
		return o.view(c, a, nil)
	}
	if q.ExpectedGeneration != c.Generation || q.ExpectedPolicyRevision != a.PolicyRevision {
		return c, a, ErrCPUChargesStale
	}
	if q.ObservedAt.Before(*a.ClockHighWater) {
		return c, a, ErrCPUChargesClockRollback
	}
	if a.TrackingGapOpen {
		return o.view(c, a, nil)
	}
	if a.PolicyRevision == math.MaxInt64 {
		return c, a, ErrCPUChargesOverflow
	}
	nc := cloneCPUCharges(c)
	if nc.Status == "active" {
		nc.ClockHighWater = &q.ObservedAt
		if e = cpuChargesClose(&nc, q.ObservedAt, "recovered_unknown", "interrupted_session", true); e != nil {
			return c, a, e
		}
	}
	na := cloneCPUAdmission(a)
	if e = rollCPUAdmission(&na, q.ObservedAt, 0, true); e != nil {
		return c, a, e
	}
	na.PolicyRevision++
	na.TrackingGapOpen = true
	at := q.ObservedAt
	na.GapOpenedAt = &at
	na.GapClosedAt = nil
	na.Hour.PartialTracking = true
	na.Day.PartialTracking = true
	id := cpuAdmissionID("cpu-tracking-gap-v1-", q)
	na.LatestGap = &CPUTrackingGapReceipt{q, id, na.PolicyRevision}
	bindCPUAdmission(&na, nc)
	return o.publish(c, nc, a, na, "gap", id)
}
