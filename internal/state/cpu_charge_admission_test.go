package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func admissionFixture(t *testing.T) (*Store, string) {
	t.Helper()
	s, dir := chargeFixture(t)
	if _, e := s.ActivateCPUChargeAdmission(context.Background(), chargeNow(), CPUChargeLimits{HourNS: 1000, DayNS: 2000}); e != nil {
		t.Fatal(e)
	}
	return s, dir
}
func admissionStart(g, revision int64, at time.Time, cpu int64, nonce string, limits CPUChargeLimits) CPULimitedSessionStart {
	return CPULimitedSessionStart{Start: chargeStart(g, at, cpu, nonce), ExpectedPolicyRevision: revision, Limits: limits}
}
func admissionBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	a, e := s.CPUChargeAdmission(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(a)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func admissionAssertTotals(t *testing.T, c CPUChargeState, a CPUChargeAdmissionState) {
	t.Helper()
	if !validCPUAdmission(a, c) || a.ActivationBaselineCPUNS+a.Hour.RetiredAssignedCPUNS+a.Hour.AssignedCPUNS != c.ChargedCPUNS || a.ActivationBaselineCPUNS+a.Day.RetiredAssignedCPUNS+a.Day.AssignedCPUNS != c.ChargedCPUNS {
		t.Fatal("joint accounting not conserved", c, a)
	}
	if a.CurrentWorkPermissionEvaluated || a.WorkPermissionGranted || a.PhysicalPeriodCPUVerified || a.GlobalCPUQuotaVerified || a.FullProcessCPUVerified {
		t.Fatal("invented authority", a)
	}
}
func TestCPUChargeAdmissionExplicitActivationAndHistoricalViews(t *testing.T) {
	ctx := context.Background()
	s, e := OpenWriter(ctx, privateDir(t))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if a, e := s.CPUChargeAdmission(ctx); e != nil || a.Available {
		t.Fatal(a, e)
	}
	if _, e = s.ActivateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{HourNS: 1}); !errors.Is(e, ErrCPUChargeAdmissionUnavailable) {
		t.Fatal(e)
	}
	if _, e = s.ActivateCPUCharges(ctx, chargeNow()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ActivateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{}); !errors.Is(e, ErrCPUChargeAdmissionInvalid) {
		t.Fatal(e)
	}
	var v int
	if e = s.db.QueryRow("PRAGMA user_version").Scan(&v); e != nil || v != 15 {
		t.Fatal(v, e)
	}
	legacy, _, e := s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), 7, "a"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ActivateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{HourNS: 1}); !errors.Is(e, ErrCPUChargesStale) {
		t.Fatal(e)
	}
	if _, e = s.FinishCPUSession(ctx, legacy, chargeSample(1, chargeNow().Add(time.Millisecond), 9, 1)); e != nil {
		t.Fatal(e)
	}
	before := chargeBytes(t, s)
	at := chargeNow().Add(time.Second)
	a, e := s.ActivateCPUChargeAdmission(ctx, at, CPUChargeLimits{HourNS: 1, DayNS: 2})
	if e != nil || a.ActivationBaselineCPUNS != 9 || !a.Hour.PartialTracking || !a.Day.PartialTracking {
		t.Fatal(a, e)
	}
	if !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("activation changed original CPU history")
	}
	if e = s.db.QueryRow("PRAGMA user_version").Scan(&v); e != nil || v != 16 || s.schema != 14 {
		t.Fatal(v, s.schema, e)
	}
	frozen := admissionBytes(t, s)
	if _, e = s.ActivateCPUChargeAdmission(ctx, chargeNow().Add(-time.Hour), CPUChargeLimits{}); e != nil || !bytes.Equal(frozen, admissionBytes(t, s)) {
		t.Fatal("repeat activation evaluated time/changed policy", e)
	}
	if _, e = s.RecoverCPUSession(ctx, at); !errors.Is(e, ErrCPUChargeAdmissionPolicyRequired) {
		t.Fatal(e)
	}
	if _, e = s.ActivateCPUCharges(ctx, at); !errors.Is(e, ErrCPUChargeAdmissionPolicyRequired) {
		t.Fatal(e)
	}
	r, e := OpenReader(ctx, strings.TrimSuffix(s.path, "/"+Filename))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if view, e := r.CPUChargeAdmission(ctx); e != nil || view.PolicyRevision != 0 || !bytes.Equal(frozen, admissionBytes(t, r)) {
		t.Fatal(view, e)
	}
	if _, e = r.ActivateCPUChargeAdmission(ctx, at, CPUChargeLimits{}); !errors.Is(e, ErrCPUChargesReadOnly) {
		t.Fatal(e)
	}
	if _, _, e = s.RecoverCPUSessionLimited(ctx, chargeNow().Add(-time.Hour)); e != nil {
		t.Fatal("terminal recovery is historical", e)
	}
}
func TestCPUChargeAdmissionEndpointAssignmentRolloverAndOvershoot(t *testing.T) {
	ctx := context.Background()
	s, _ := admissionFixture(t)
	q := admissionStart(0, 0, chargeNow(), 100, "a", CPUChargeLimits{HourNS: 10, DayNS: 20})
	m, c, a, e := s.BeginCPUSessionLimited(ctx, q)
	if e != nil || a.Hour.AssignedCPUNS != 100 || a.Day.AssignedCPUNS != 100 {
		t.Fatal(c, a, e)
	}
	admissionAssertTotals(t, c, a)
	// The full spanning delta belongs to its later accepted endpoint; above-cap
	// evidence is kept, and rollover clears only the new slot's qualifications.
	c, a, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(1, chargeNow().Add(2*time.Second), 120, 1))
	if e != nil || a.Hour.AssignedCPUNS != 20 || a.Day.AssignedCPUNS != 20 || a.Hour.RetiredAssignedCPUNS != 100 || a.Day.RetiredAssignedCPUNS != 100 || a.Hour.PartialTracking || a.Day.PartialTracking {
		t.Fatal(c, a, e)
	}
	admissionAssertTotals(t, c, a)
	c, a, e = s.FinishCPUSessionLimited(ctx, m, chargeSample(2, chargeNow().Add(49*time.Hour), 120, 1))
	if e != nil || a.Hour.AssignedCPUNS != 0 || a.Day.AssignedCPUNS != 0 || a.Hour.RetiredAssignedCPUNS != 120 || a.Day.RetiredAssignedCPUNS != 120 || a.Hour.BlockingUnknown || a.Day.BlockingUnknown || c.UnknownTailSessions != 1 {
		t.Fatal(c, a, e)
	}
	admissionAssertTotals(t, c, a)
	frozen := admissionBytes(t, s)
	a.BeginRequest.Start.SelfCPUNS = chargeInt(999)
	a.ClockHighWater = chargeTimePointer(chargeNow())
	if !bytes.Equal(frozen, admissionBytes(t, s)) {
		t.Fatal("view aliases live evidence")
	}
}
func chargeTimePointer(at time.Time) *time.Time { return &at }
func TestCPUChargeAdmissionExactPolicyAndSampleRetries(t *testing.T) {
	ctx := context.Background()
	s, _ := admissionFixture(t)
	q := admissionStart(0, 0, chargeNow(), 0, "a", CPUChargeLimits{})
	m, c, a, e := s.BeginCPUSessionLimited(ctx, q)
	if e != nil || m.Generation() != 1 || a.PolicyRevision != 1 || a.Limits != (CPUChargeLimits{}) {
		t.Fatal(m, c, a, e)
	}
	before := admissionBytes(t, s)
	retry, _, _, e := s.BeginCPUSessionLimited(ctx, q)
	if e != nil || retry.Generation() != 0 || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("retry minted marker/changed evidence", retry, e)
	}
	changed := q
	changed.Limits.HourNS = 1
	if _, _, _, e = s.BeginCPUSessionLimited(ctx, changed); !errors.Is(e, ErrCPUChargesStale) || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal(e)
	}
	if _, _, e = s.SampleCPUSessionLimited(ctx, CPULimitedSessionMarker{}, chargeSample(1, chargeNow(), 0, 1)); !errors.Is(e, ErrCPUChargesStale) {
		t.Fatal(e)
	}
	sample := chargeSample(1, chargeNow().Add(time.Second), 0, 1)
	c, a, e = s.SampleCPUSessionLimited(ctx, m, sample)
	if e != nil {
		t.Fatal(c, a, e)
	}
	before = admissionBytes(t, s)
	if _, _, e = s.SampleCPUSessionLimited(ctx, m, sample); e != nil || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("exact sample renewed clocks", e)
	}
	if _, _, e = s.RecoverCPUSessionLimited(ctx, chargeNow().Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	before = admissionBytes(t, s)
	if _, _, e = s.SampleCPUSessionLimited(ctx, m, sample); !errors.Is(e, ErrCPUChargesUnknown) || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("recovered retry lost unknown classification", e)
	}
	c, a, e = s.RecoverCPUSessionLimited(ctx, chargeNow().Add(-time.Hour))
	if e != nil || c.RecoveredSessions != 1 || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("terminal recovery refreshed time", c, a, e)
	}
}
func TestCPUChargeAdmissionTrackingGapReplayAndReenable(t *testing.T) {
	ctx := context.Background()
	s, _ := admissionFixture(t)
	m, c, a, e := s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 123, "a", CPUChargeLimits{}))
	if e != nil {
		t.Fatal(e)
	}
	q := CPUTrackingGapRequest{a.PolicyRevision, c.Generation, strings.Repeat("b", 64), chargeNow().Add(time.Second)}
	c, a, e = s.OpenCPUTrackingGap(ctx, q)
	if e != nil || !a.TrackingGapOpen || c.RecoveredSessions != 1 || c.ChargedCPUNS != 123 || a.PolicyRevision != 2 || a.LatestGap.AppliedRevision != q.ExpectedPolicyRevision+1 {
		t.Fatal(c, a, e)
	}
	admissionAssertTotals(t, c, a)
	before := admissionBytes(t, s)
	if _, _, e = s.OpenCPUTrackingGap(ctx, q); e != nil || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("original expected revision did not replay exactly", e)
	}
	newRequest := q
	newRequest.ExpectedPolicyRevision = a.PolicyRevision
	newRequest.Nonce = strings.Repeat("c", 64)
	newRequest.ObservedAt = chargeNow().Add(48 * time.Hour)
	if _, _, e = s.OpenCPUTrackingGap(ctx, newRequest); e != nil || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal("open gap renewed coverage/receipt", e)
	}
	stale := newRequest
	stale.ExpectedGeneration++
	if _, _, e = s.OpenCPUTrackingGap(ctx, stale); !errors.Is(e, ErrCPUChargesStale) || !bytes.Equal(before, admissionBytes(t, s)) {
		t.Fatal(e)
	}
	if _, _, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(1, chargeNow().Add(2*time.Second), 124, 1)); !errors.Is(e, ErrCPUChargesStale) {
		t.Fatal("old marker survived policy revision", e)
	}
	n, c, a, e := s.BeginCPUSessionLimited(ctx, admissionStart(1, 2, chargeNow().Add(25*time.Hour), 5, "d", CPUChargeLimits{}))
	if e != nil || a.TrackingGapOpen || !a.Hour.PartialTracking || !a.Day.PartialTracking || a.Hour.AssignedCPUNS != 5 || a.Hour.BlockingUnknown || a.Day.BlockingUnknown || c.ChargedCPUNS != 128 {
		t.Fatal(c, a, e)
	}
	if _, _, e = s.OpenCPUTrackingGap(ctx, q); !errors.Is(e, ErrCPUChargesStale) {
		t.Fatal("old gap reopened", e)
	}
	c, a, e = s.FinishCPUSessionLimited(ctx, n, chargeSample(1, chargeNow().Add(25*time.Hour+time.Second), 5, 1))
	if e != nil {
		t.Fatal(c, a, e)
	}
	if _, _, _, e = s.BeginCPUSessionLimited(ctx, admissionStart(2, 3, chargeNow().Add(26*time.Hour), 0, "e", CPUChargeLimits{HourNS: 1})); e != nil {
		t.Fatal("later profile incorrectly renewed old gap closure", e)
	}
}
func TestCPUChargeAdmissionUnknownRollbackRecoveryAndWriterBinding(t *testing.T) {
	ctx := context.Background()
	s, dir := admissionFixture(t)
	m, c, a, e := s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 10, "a", CPUChargeLimits{HourNS: 100}))
	if e != nil {
		t.Fatal(e)
	}
	frozen := admissionBytes(t, s)
	bad := CPUTrackingGapRequest{99, 0, strings.Repeat("c", 64), chargeNow().Add(9 * time.Hour)}
	if _, _, e = s.OpenCPUTrackingGap(ctx, bad); !errors.Is(e, ErrCPUChargesStale) || !bytes.Equal(frozen, admissionBytes(t, s)) {
		t.Fatal("stale caller advanced clock", e)
	}
	c, a, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(1, chargeNow().Add(-time.Second), 11, 1))
	if !errors.Is(e, ErrCPUChargesClockRollback) || c.ChargedCPUNS != 10 || !a.Hour.BlockingUnknown || !a.Day.BlockingUnknown || !a.ClockHighWater.Equal(chargeNow()) {
		t.Fatal(c, a, e)
	}
	admissionAssertTotals(t, c, a)
	s.Close()
	s, e = OpenWriter(ctx, dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, _, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(2, chargeNow().Add(time.Second), 11, 1)); !errors.Is(e, ErrCPUChargesStale) {
		t.Fatal("marker crossed writer reopen", e)
	}
	if _, _, e = s.RecoverCPUSessionLimited(ctx, chargeNow().Add(10*time.Hour)); e != nil {
		t.Fatal(e)
	}
	_, c, a, e = s.BeginCPUSessionLimited(ctx, admissionStart(1, 1, chargeNow().Add(24*time.Hour+2*time.Second), 0, "b", CPUChargeLimits{DayNS: 1}))
	if e != nil || a.Hour.BlockingUnknown || a.Day.BlockingUnknown || c.UnknownTailSessions != 1 {
		t.Fatal("new full slots erased history or retained old slot uncertainty", c, a, e)
	}
}
func TestCPUChargeAdmissionBoundsCorruptionAndJointCoherence(t *testing.T) {
	for _, kind := range []string{"oversized_blob", "oversized_text", "duplicate_row", "missing", "revision_type", "json_sum", "digest", "slot_alignment", "flags", "clock_overflow", "orphan_schema"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, _ := admissionFixture(t)
			_, _, _, e := s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 10, "a", CPUChargeLimits{}))
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.db.Exec("PRAGMA ignore_check_constraints=ON"); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "oversized_blob":
				_, e = s.db.Exec("UPDATE worker_cpu_charge_admission SET state_json=zeroblob(2097152)")
			case "oversized_text":
				_, e = s.db.Exec("UPDATE worker_cpu_charge_admission SET state_json=CAST(zeroblob(2097152) AS TEXT)")
			case "duplicate_row":
				_, e = s.db.Exec("INSERT INTO worker_cpu_charge_admission SELECT 2,policy_revision,state_json FROM worker_cpu_charge_admission")
			case "missing":
				_, e = s.db.Exec("DELETE FROM worker_cpu_charge_admission")
			case "revision_type":
				_, e = s.db.Exec("UPDATE worker_cpu_charge_admission SET policy_revision=zeroblob(2097152)")
			case "orphan_schema":
				_, e = s.db.Exec("PRAGMA user_version=15;DELETE FROM schema_migrations WHERE version=16")
			default:
				a, e := s.CPUChargeAdmission(ctx)
				if e != nil {
					t.Fatal(e)
				}
				switch kind {
				case "json_sum":
					a.Hour.AssignedCPUNS++
				case "digest":
					a.CPUStateDigest = strings.Repeat("a", 64)
				case "slot_alignment":
					a.Day.StartNS++
				case "flags":
					a.WorkPermissionGranted = true
				case "clock_overflow":
					at := time.Unix(0, math.MaxInt64).UTC()
					a.ClockHighWater = &at
				}
				raw, _ := json.Marshal(a)
				_, e = s.db.Exec("UPDATE worker_cpu_charge_admission SET state_json=?", raw)
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.CPUChargeAdmission(ctx); !errors.Is(e, ErrCPUChargeAdmissionCorrupt) {
				t.Fatal("corrupt limited view accepted", e)
			}
			if _, e = s.CPUCharges(ctx); !errors.Is(e, ErrCPUChargeAdmissionCorrupt) {
				t.Fatal("bare saved CPU ignored corrupt joint state", e)
			}
		})
	}
}
func TestCPUChargeAdmissionFiniteInputAndCanceledNoWrite(t *testing.T) {
	ctx := context.Background()
	s, _ := chargeFixture(t)
	for _, limits := range []CPUChargeLimits{{HourNS: -1}, {HourNS: int64(time.Hour) + 1}, {DayNS: int64(24*time.Hour) + 1}} {
		if _, e := s.ActivateCPUChargeAdmission(ctx, chargeNow(), limits); !errors.Is(e, ErrCPUChargeAdmissionInvalid) {
			t.Fatal(e)
		}
	}
	for _, at := range []time.Time{time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(0, math.MaxInt64).UTC(), chargeNow().In(time.FixedZone("other", 0))} {
		if _, e := s.ActivateCPUChargeAdmission(ctx, at, CPUChargeLimits{HourNS: 1}); !errors.Is(e, ErrCPUChargeAdmissionInvalid) {
			t.Fatal(e)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := s.ActivateCPUChargeAdmission(canceled, chargeNow(), CPUChargeLimits{HourNS: 1}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	var v int
	if e := s.db.QueryRow("PRAGMA user_version").Scan(&v); e != nil || v != 15 {
		t.Fatal("invalid activation initialized", v, e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	if _, e := s.activateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{HourNS: 1}, cpuAdmissionHooks{beforeCommit: cancel}); !errors.Is(e, context.Canceled) || errors.Is(e, ErrCPUChargeAdmissionPublication) {
		t.Fatal("definite cancel misclassified", e)
	}
	if e := s.db.QueryRow("PRAGMA user_version").Scan(&v); e != nil || v != 15 || s.cpuAuthorityRefused.Load() {
		t.Fatal("canceled initialization/latch", v, e)
	}
}
func TestCPUChargeAdmissionUncertainLatchIncludesQueuedCPUButNotControls(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "published"}[published], func(t *testing.T) {
			ctx := context.Background()
			s, _ := chargeFixture(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			finished := make(chan error, 1)
			hooks := cpuAdmissionHooks{commit: func(tx *sql.Tx) error {
				close(entered)
				<-release
				if published {
					if e := tx.Commit(); e != nil {
						return e
					}
				} else {
					if e := tx.Rollback(); e != nil {
						return e
					}
				}
				return errors.New("generated uncertain reply")
			}}
			go func() {
				_, e := s.activateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{HourNS: 1}, hooks)
				finished <- e
			}()
			<-entered
			queued := make(chan error, 1)
			go func() { _, _, e := s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), 0, "a")); queued <- e }()
			deadline := time.After(time.Second)
			for s.db.Stats().WaitCount == 0 {
				select {
				case <-deadline:
					t.Fatal("CPU query did not queue behind sole connection")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			close(release)
			e := <-finished
			var publication *CPUChargeAdmissionPublicationError
			if !errors.As(e, &publication) || !errors.Is(e, ErrCPUChargesPublication) || publication.Operation != "activate" || !strings.HasPrefix(publication.RequestID, "cpu-admission-activate-v1-") {
				t.Fatal(e)
			}
			if e = <-queued; !errors.Is(e, ErrCPUChargeAdmissionAuthority) {
				t.Fatal("queued bare call regained CPU authority", e)
			}
			if _, e = s.ActivateCPUCharges(ctx, chargeNow()); !errors.Is(e, ErrCPUChargeAdmissionAuthority) {
				t.Fatal(e)
			}
			if _, _, _, e = s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 0, "b", CPUChargeLimits{})); !errors.Is(e, ErrCPUChargeAdmissionAuthority) {
				t.Fatal(e)
			}
			if e = s.SetPaused(ctx, true); e != nil {
				t.Fatal("CPU latch blocked control-state writer", e)
			}
			if paused, e := s.Paused(ctx); e != nil || !paused {
				t.Fatal(paused, e)
			}
			if _, e = s.CPUCharges(ctx); e != nil {
				t.Fatal("CPU latch blocked saved inspection", e)
			}
			a, e := s.CPUChargeAdmission(ctx)
			if e != nil || a.Available != published {
				t.Fatal(a, e)
			}
		})
	}
}
func TestCPUChargeAdmissionSingleMarkerConcurrentRetry(t *testing.T) {
	s, _ := admissionFixture(t)
	q := admissionStart(0, 0, chargeNow(), 0, "a", CPUChargeLimits{})
	var wg sync.WaitGroup
	markers := make(chan CPULimitedSessionMarker, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, _, _, e := s.BeginCPUSessionLimited(context.Background(), q)
			markers <- m
			errs <- e
		}()
	}
	wg.Wait()
	close(markers)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	issued := 0
	for m := range markers {
		if m.Generation() > 0 {
			issued++
		}
	}
	if issued != 1 {
		t.Fatal("continuation reminted", issued)
	}
}
func TestCPUChargeAdmissionLateCancelAndNoOpVeto(t *testing.T) {
	s, _ := admissionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	q := admissionStart(0, 0, chargeNow(), 0, "a", CPUChargeLimits{})
	_, _, _, e := s.beginCPUSessionLimited(ctx, q, cpuAdmissionHooks{afterCommit: cancel})
	if !errors.Is(e, context.Canceled) || !errors.Is(e, ErrCPUChargeAdmissionPublication) || !s.cpuAuthorityRefused.Load() {
		t.Fatal(e)
	}
	a, e := s.CPUChargeAdmission(context.Background())
	if e != nil || a.PolicyRevision != 1 {
		t.Fatal(a, e)
	}
	s2, _ := admissionFixture(t)
	q = admissionStart(0, 0, chargeNow(), 0, "b", CPUChargeLimits{})
	if _, _, _, e = s2.BeginCPUSessionLimited(context.Background(), q); e != nil {
		t.Fatal(e)
	}
	before := admissionBytes(t, s2)
	ctx, cancel = context.WithCancel(context.Background())
	m, c, a, e := s2.beginCPUSessionLimited(ctx, q, cpuAdmissionHooks{noOpReleased: cancel})
	if !errors.Is(e, context.Canceled) || m.Generation() != 0 || c.Available || a.Available || s2.cpuAuthorityRefused.Load() || !bytes.Equal(before, admissionBytes(t, s2)) {
		t.Fatal("readonly retry cancel published positive/latch", m, c, a, e)
	}
}

func TestCPUChargeAdmissionIndependentHourRolloverRetainsDayUnknown(t *testing.T) {
	ctx := context.Background()
	s, _ := admissionFixture(t)
	m, c, a, e := s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 10, "a", CPUChargeLimits{HourNS: 100, DayNS: 200}))
	if e != nil {
		t.Fatal(e)
	}
	midnight := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	c, a, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(1, midnight.Add(time.Second), 20, 1))
	if e != nil || a.Hour.AssignedCPUNS != 10 || a.Day.AssignedCPUNS != 10 || a.Hour.RetiredAssignedCPUNS != 10 || a.Day.RetiredAssignedCPUNS != 10 || a.Hour.PartialTracking || a.Day.PartialTracking {
		t.Fatal(c, a, e)
	}
	c, a, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(2, midnight.Add(time.Hour+time.Second), 30, 1))
	if e != nil || a.Hour.StartNS != midnight.Add(time.Hour).UnixNano() || a.Hour.NextBoundaryNS != midnight.Add(2*time.Hour).UnixNano() || a.Day.StartNS != midnight.UnixNano() || a.Day.NextBoundaryNS != midnight.Add(24*time.Hour).UnixNano() || a.Hour.AssignedCPUNS != 10 || a.Hour.RetiredAssignedCPUNS != 20 || a.Day.AssignedCPUNS != 20 || a.Day.RetiredAssignedCPUNS != 10 {
		t.Fatal("hour rollover changed daily attribution", c, a, e)
	}
	admissionAssertTotals(t, c, a)
	c, a, e = s.SampleCPUSessionLimited(ctx, m, chargeSample(3, midnight.Add(time.Hour+2*time.Second), 29, 1))
	if !errors.Is(e, ErrCPUChargesUnknown) || c.ChargedCPUNS != 30 || c.UnknownTailSessions != 1 || !a.Hour.BlockingUnknown || !a.Day.BlockingUnknown {
		t.Fatal("same-day unknown closure lost qualifications", c, a, e)
	}
	admissionAssertTotals(t, c, a)
	_, c, a, e = s.BeginCPUSessionLimited(ctx, admissionStart(1, 1, midnight.Add(2*time.Hour+3*time.Second), 7, "b", CPUChargeLimits{HourNS: 100, DayNS: 200}))
	if e != nil || a.Hour.StartNS != midnight.Add(2*time.Hour).UnixNano() || a.Hour.NextBoundaryNS != midnight.Add(3*time.Hour).UnixNano() || a.Day.StartNS != midnight.UnixNano() || a.Day.NextBoundaryNS != midnight.Add(24*time.Hour).UnixNano() || a.Hour.BlockingUnknown || !a.Day.BlockingUnknown || a.Hour.AssignedCPUNS != 7 || a.Hour.RetiredAssignedCPUNS != 30 || a.Day.AssignedCPUNS != 27 || a.Day.RetiredAssignedCPUNS != 10 || c.ChargedCPUNS != 37 || c.UnknownTailSessions != 1 {
		t.Fatal("next hour erased day/lifetime unknown evidence", c, a, e)
	}
	admissionAssertTotals(t, c, a)
}
