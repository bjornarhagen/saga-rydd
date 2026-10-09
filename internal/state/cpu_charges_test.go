package state

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func chargeNow() time.Time     { return time.Date(2026, 10, 9, 23, 59, 59, 0, time.UTC) }
func chargeInt(n int64) *int64 { return &n }
func chargeStart(g int64, at time.Time, cpu int64, nonce string) CPUSessionStart {
	return CPUSessionStart{ExpectedGeneration: g, Nonce: strings.Repeat(nonce, 64), Instance: strings.Repeat("a", 32), ObservedAt: at, SelfCPUNS: chargeInt(cpu)}
}
func chargeSample(n int64, at time.Time, cpu, elapsed int64) CPUSessionSample {
	return CPUSessionSample{Ordinal: n, ObservedAt: at, SelfCPUNS: chargeInt(cpu), ElapsedNS: chargeInt(elapsed)}
}
func chargeFixture(t *testing.T) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err = s.ActivateCPUCharges(context.Background(), chargeNow()); err != nil {
		t.Fatal(err)
	}
	return s, dir
}
func chargeBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	v, err := s.CPUCharges(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCPUChargesExplicitActivationCompatibility(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(ctx, []string{"/generated/cpu"}); err != nil {
		t.Fatal(err)
	}
	if err = s.EnqueueJob(ctx, 1, "fixture", []byte("x"), chargeNow()); err != nil {
		t.Fatal(err)
	}
	state, err := s.CPUCharges(ctx)
	if err != nil || state.Available || state.Status != "unavailable" {
		t.Fatal(state, err)
	}
	summary, err := s.Summary(ctx)
	if err != nil || summary.Schema != 14 || s.schema != 14 {
		t.Fatal(summary, err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.ActivateCPUCharges(ctx, chargeNow()); !errors.Is(err, ErrCPUChargesReadOnly) {
		t.Fatal(err)
	}
	activated, err := s.ActivateCPUCharges(ctx, chargeNow())
	if err != nil || !activated.Available || activated.Generation != 0 || activated.Session != nil {
		t.Fatal(activated, err)
	}
	summary, err = s.Summary(ctx)
	if err != nil || summary.Schema != 15 || summary.PendingJobs != 1 || summary.EnabledRoots != 1 || s.schema != 14 {
		t.Fatal("activation mutated ordinary inventory or open-time schema", summary, err)
	}
	before := chargeBytes(t, s)
	if _, err = s.ActivateCPUCharges(ctx, chargeNow().Add(time.Hour)); err != nil || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("activation renewed state", err)
	}
	if v, err := r.CPUCharges(ctx); err != nil || !v.Available {
		t.Fatal("existing reader cannot observe optional schema", v, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.schema != 15 || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("ordinary reopen downgraded/recovered ledger")
	}
	// The exact pre-extension identity guard rejects version 15 before mutation.
	var version int
	if err = s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version <= schemaVersion {
		t.Fatal("schema-14 binary would accept activated store", version, err)
	}
}

func TestCPUChargesLegacyReadersAndOrdinaryWriters(t *testing.T) {
	for _, version := range []int{4, 12, 13, 14} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, filepath.Join(dir, Filename), false)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := 0; i < version; i++ {
				if _, err = s.db.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec(fmt.Sprintf("PRAGMA application_id=%d;PRAGMA user_version=%d", applicationID, version)); err != nil {
				t.Fatal(err)
			}
			s.Close()
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			v, err := r.CPUCharges(ctx)
			r.Close()
			if err != nil || v.Available {
				t.Fatal(v, err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			summary, err := w.Summary(ctx)
			if err != nil || summary.Schema != 14 {
				t.Fatal("ordinary writer activated ledger", summary, err)
			}
		})
	}
}

func TestCPUChargesRepeatedRunReplayAndTerminalRetries(t *testing.T) {
	s, _ := chargeFixture(t)
	ctx := context.Background()
	at := chargeNow()
	a := chargeStart(0, at, 100, "a")
	m, v, err := s.BeginCPUSession(ctx, a)
	if err != nil || v.ChargedCPUNS != 100 || v.Session.InitialPrefixChargeNS != 100 {
		t.Fatal(v, err)
	}
	*a.SelfCPUNS = 999
	if *v.Session.Start.SelfCPUNS != 100 || m.start.SelfCPUNS == a.SelfCPUNS {
		t.Fatal("start aliases caller")
	}
	a.SelfCPUNS = chargeInt(100)
	sample := chargeSample(1, at.Add(time.Second), 120, int64(time.Second))
	v, err = s.FinishCPUSession(ctx, m, sample)
	if err != nil || v.ChargedCPUNS != 120 || v.UnknownTailSessions != 1 || v.Status != "finished" {
		t.Fatal(v, err)
	}
	before := chargeBytes(t, s)
	if _, err = s.FinishCPUSession(ctx, m, sample); err != nil || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("terminal retry charged twice", err)
	}
	if _, v, err = s.BeginCPUSession(ctx, a); err != nil || v.Status != "finished" || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("begin retry erased terminal sample", v, err)
	}
	changed := sample
	changed.SelfCPUNS = chargeInt(121)
	if _, err = s.FinishCPUSession(ctx, m, changed); !errors.Is(err, ErrCPUChargesStale) {
		t.Fatal(err)
	}
	b := chargeStart(1, at.Add(2*time.Second), 130, "b")
	bm, v, err := s.BeginCPUSession(ctx, b)
	if err != nil || v.Generation != 2 || v.ChargedCPUNS != 250 {
		t.Fatal(v, err)
	}
	if _, err = s.SampleCPUSession(ctx, bm, chargeSample(1, at.Add(3*time.Second), 140, int64(time.Second))); err != nil {
		t.Fatal(err)
	}
	before = chargeBytes(t, s)
	if _, _, err = s.BeginCPUSession(ctx, a); !errors.Is(err, ErrCPUChargesStale) || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("A-B-A replay", err)
	}
	if _, err = s.FinishCPUSession(ctx, m, sample); !errors.Is(err, ErrCPUChargesStale) {
		t.Fatal("old marker resurrected", err)
	}
	v, err = s.CPUCharges(ctx)
	if err != nil || v.ChargedCPUNS != 260 || v.UniqueProcessCPUVerified || v.FullProcessLifetimeVerified || v.WorkPermissionGranted || !v.PrefixOverlapPossible {
		t.Fatal("overlap labelled unique CPU or permission", v, err)
	}
}

func TestCPUChargesSamplesAndConcurrentExactRetries(t *testing.T) {
	s, _ := chargeFixture(t)
	ctx := context.Background()
	at := chargeNow()
	start := chargeStart(0, at, 0, "a")
	var wg sync.WaitGroup
	markers := make(chan CPUSessionMarker, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m, _, err := s.BeginCPUSession(ctx, start); markers <- m; errs <- err }()
	}
	wg.Wait()
	close(markers)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var marker CPUSessionMarker
	issued := 0
	for m := range markers {
		if m.Generation() != 0 {
			issued++
			marker = m
			if m.Generation() != 1 {
				t.Fatal(m)
			}
		} else if m.Nonce() != "" {
			t.Fatal("retry returned partial marker", m)
		}
	}
	if issued != 1 {
		t.Fatal("exact concurrent retries minted continuation markers", issued)
	}
	q := chargeSample(1, at.Add(time.Second), 20, int64(time.Second))
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.SampleCPUSession(ctx, marker, q); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	v, err := s.CPUCharges(ctx)
	if err != nil || v.ChargedCPUNS != 20 || v.Generation != 1 || v.Session.LastOrdinal != 1 {
		t.Fatal(v, err)
	}
	before := chargeBytes(t, s)
	changed := q
	changed.ElapsedNS = chargeInt(1)
	if _, err = s.SampleCPUSession(ctx, marker, changed); !errors.Is(err, ErrCPUChargesStale) || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal(err)
	}
	if _, err = s.SampleCPUSession(ctx, marker, chargeSample(2, at.Add(2*time.Second), 30, int64(time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SampleCPUSession(ctx, marker, q); !errors.Is(err, ErrCPUChargesStale) {
		t.Fatal("old ordinal replayed", err)
	}
}

func TestCPUChargesUnknownObservationsCloseOnce(t *testing.T) {
	for _, reason := range []string{"cpu_observation_unavailable", "cpu_observation_invalid", "cpu_observation_regressed", "elapsed_window_invalid", "cpu_clock_rollback"} {
		t.Run(reason, func(t *testing.T) {
			s, _ := chargeFixture(t)
			ctx := context.Background()
			at := chargeNow()
			m, _, err := s.BeginCPUSession(ctx, chargeStart(0, at, 100, "a"))
			if err != nil {
				t.Fatal(err)
			}
			q := chargeSample(1, at.Add(time.Second), 120, int64(time.Second))
			switch reason {
			case "cpu_observation_unavailable":
				q.SelfCPUNS = nil
				q.Reason = reason
			case "cpu_observation_invalid":
				q.SelfCPUNS = chargeInt(-1)
			case "cpu_observation_regressed":
				q.SelfCPUNS = chargeInt(99)
			case "elapsed_window_invalid":
				q.ElapsedNS = chargeInt(0)
			case "cpu_clock_rollback":
				q.ObservedAt = at.Add(-time.Nanosecond)
			}
			v, err := s.SampleCPUSession(ctx, m, q)
			expected := ErrCPUChargesUnknown
			if reason == "cpu_clock_rollback" {
				expected = ErrCPUChargesClockRollback
			}
			if !errors.Is(err, expected) || v.Status != "unknown" || v.UnknownTailSessions != 1 || v.Session.ClosureReason != reason || v.ChargedCPUNS != 100 || v.NextAllowedAt == nil {
				t.Fatal(v, err)
			}
			before := chargeBytes(t, s)
			if _, err = s.SampleCPUSession(ctx, m, q); !errors.Is(err, expected) || !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("unknown exact retry changed state", err)
			}
			if _, err = s.RecoverCPUSession(ctx, at.Add(time.Minute)); err != nil || !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("closed unknown recovered twice", err)
			}
			if _, err = s.SampleCPUSession(ctx, m, chargeSample(2, at.Add(2*time.Second), 130, int64(time.Second))); !errors.Is(err, ErrCPUChargesStale) {
				t.Fatal("terminal marker admitted work", err)
			}
		})
	}
}

func TestCPUChargesDebtClockMidnightRecoveryAndReopen(t *testing.T) {
	s, dir := chargeFixture(t)
	ctx := context.Background()
	at := chargeNow()
	m, v, err := s.BeginCPUSession(ctx, chargeStart(0, at, int64(time.Second), "a"))
	if err != nil || !v.NextAllowedAt.Equal(at.Add(100*time.Second)) {
		t.Fatal(v, err)
	}
	originalDeadline := *v.NextAllowedAt
	q := chargeSample(1, at.Add(2*time.Second), int64(time.Second), int64(2*time.Second))
	v, err = s.SampleCPUSession(ctx, m, q)
	if err != nil || !v.NextAllowedAt.Equal(originalDeadline) {
		t.Fatal("midnight/zero delta refunded prospective debt", v, err)
	}
	before := chargeBytes(t, s)
	if _, _, err = s.BeginCPUSession(ctx, chargeStart(1, at.Add(time.Second), 0, "b")); !errors.Is(err, ErrCPUChargesClockRollback) || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("rollback reset tracking", err)
	}
	v, err = s.RecoverCPUSession(ctx, at.Add(3*time.Second))
	if err != nil || v.RecoveredSessions != 1 || v.UnknownTailSessions != 1 || !v.NextAllowedAt.Equal(at.Add(3*time.Second+time.Hour)) {
		t.Fatal(v, err)
	}
	before = chargeBytes(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("open recovered/refunded ledger")
	}
	if _, err = s.SampleCPUSession(ctx, m, q); !errors.Is(err, ErrCPUChargesStale) {
		t.Fatal("old writer marker accepted", err)
	}
	if _, err = s.RecoverCPUSession(ctx, at.Add(time.Minute)); err != nil || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("restart renewed recovery", err)
	}
	_, v, err = s.BeginCPUSession(ctx, chargeStart(1, at.Add(4*time.Second), 0, "b"))
	if err != nil || !v.NextAllowedAt.Equal(at.Add(3*time.Second+time.Hour)) || v.RecoveredSessions != 1 {
		t.Fatal("new tracking cleared existing cooldown", v, err)
	}
	if wait, capped := CPUChargesPrefixWait(time.Hour); wait != time.Hour || !capped {
		t.Fatal(wait, capped)
	}
}

func chargeOverwrite(t *testing.T, s *Store, v CPUChargeState) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE worker_self_cpu_charges SET generation=?,state_json=?", v.Generation, raw); err != nil {
		t.Fatal(err)
	}
}
func TestCPUChargesOverflowRefusesWithoutRefund(t *testing.T) {
	for _, kind := range []string{"charge", "generation", "ordinal", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := chargeFixture(t)
			ctx := context.Background()
			at := chargeNow()
			m, v, err := s.BeginCPUSession(ctx, chargeStart(0, at, 0, "a"))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "charge":
				v.ChargedCPUNS = math.MaxInt64
				chargeOverwrite(t, s, v)
			case "generation":
				v.Generation = math.MaxInt64
				v.ClosedSessions = math.MaxInt64 - 1
				v.UnknownTailSessions = v.ClosedSessions
				v.Session.Generation = v.Generation
				v.Session.Start.ExpectedGeneration = v.Generation - 1
				chargeOverwrite(t, s, v)
			case "ordinal":
				v.Session.LastOrdinal = math.MaxInt64
				v.Session.LastSample = func() *CPUSessionSample { q := chargeSample(math.MaxInt64, at, 0, 1); return &q }()
				chargeOverwrite(t, s, v)
			case "deadline":
			}
			before := chargeBytes(t, s)
			switch kind {
			case "charge":
				_, err = s.SampleCPUSession(ctx, m, chargeSample(1, at.Add(time.Second), 1, 1))
			case "generation":
				_, _, err = s.BeginCPUSession(ctx, chargeStart(math.MaxInt64, at.Add(time.Second), 0, "b"))
			case "ordinal":
				m.generation = v.Generation
				m.start = v.Session.Start
				_, err = s.SampleCPUSession(ctx, m, chargeSample(1, at.Add(time.Second), 0, 1))
			case "deadline":
				_, err = s.SampleCPUSession(ctx, m, chargeSample(1, time.Unix(0, math.MaxInt64).UTC(), 1, 1))
			}
			expected := ErrCPUChargesOverflow

			if !errors.Is(err, expected) || !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("overflow changed/refunded row", kind, err)
			}
		})
	}
}

func TestCPUChargesCorruptionNeverAdmits(t *testing.T) {
	for _, kind := range []string{"json_duplicate", "json_unknown", "type", "missing_row", "multiple_rows", "shape", "claims", "generation_mismatch", "reason", "migration_zero", "migration_negative", "migration_applied_type", "migration_name_type", "migration_missing", "migration_extra"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := chargeFixture(t)
			ctx := context.Background()
			at := chargeNow()
			_, v, err := s.BeginCPUSession(ctx, chargeStart(0, at, 0, "a"))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "json_duplicate":
				raw := chargeBytes(t, s)
				raw = append(raw[:len(raw)-1], []byte(`,"generation":1}`)...)
				_, err = s.db.Exec("UPDATE worker_self_cpu_charges SET state_json=?", raw)
			case "json_unknown":
				raw := chargeBytes(t, s)
				raw = append(raw[:len(raw)-1], []byte(`,"unexpected":true}`)...)
				_, err = s.db.Exec("UPDATE worker_self_cpu_charges SET state_json=?", raw)
			case "type":
				_, err = s.db.Exec("PRAGMA ignore_check_constraints=ON;UPDATE worker_self_cpu_charges SET state_json='text'")
			case "missing_row":
				_, err = s.db.Exec("DELETE FROM worker_self_cpu_charges")
			case "multiple_rows":
				_, err = s.db.Exec("PRAGMA ignore_check_constraints=ON;INSERT INTO worker_self_cpu_charges SELECT 2,generation,state_json FROM worker_self_cpu_charges")
			case "shape":
				_, err = s.db.Exec("ALTER TABLE worker_self_cpu_charges ADD COLUMN extra INTEGER")
			case "claims":
				v.WorkPermissionGranted = true
				chargeOverwrite(t, s, v)
			case "generation_mismatch":
				_, err = s.db.Exec("UPDATE worker_self_cpu_charges SET generation=2")
			case "reason":
				v.Session.ClosureReason = "invented"
				chargeOverwrite(t, s, v)
			case "migration_zero":
				_, err = s.db.Exec("INSERT INTO schema_migrations VALUES(0,'extra',0)")
			case "migration_negative":
				_, err = s.db.Exec("INSERT INTO schema_migrations VALUES(-1,'extra',0)")
			case "migration_applied_type":
				_, err = s.db.Exec("UPDATE schema_migrations SET applied_at_ns='malformed' WHERE version=1")
			case "migration_name_type":
				_, err = s.db.Exec("UPDATE schema_migrations SET name=X'61' WHERE version=1")
			case "migration_missing":
				_, err = s.db.Exec("DELETE FROM schema_migrations WHERE version=1")
			case "migration_extra":
				_, err = s.db.Exec("INSERT INTO schema_migrations VALUES(16,'extra',0)")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.CPUCharges(ctx); !errors.Is(err, ErrCPUChargesCorrupt) {
				t.Fatal(kind, err)
			}
			if _, _, err = s.BeginCPUSession(ctx, chargeStart(1, at.Add(time.Second), 0, "b")); !errors.Is(err, ErrCPUChargesCorrupt) {
				t.Fatal("corrupt state admitted", kind, err)
			}
		})
	}
}

var chargeCancelOnce sync.Once
var chargeCancelErr error
var chargeCancelMu sync.Mutex
var chargeCancel context.CancelFunc

func TestCPUChargesActivationCancellationIsAtomic(t *testing.T) {
	chargeCancelOnce.Do(func() {
		chargeCancelErr = sqlite.RegisterScalarFunction("rydd_test_charge_activate_cancel", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			chargeCancelMu.Lock()
			fn := chargeCancel
			chargeCancelMu.Unlock()
			if fn != nil {
				fn()
			}
			return int64(0), nil
		})
	})
	if chargeCancelErr != nil {
		t.Fatal(chargeCancelErr)
	}
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chargeCancelMu.Lock()
	chargeCancel = cancel
	chargeCancelMu.Unlock()
	defer func() { chargeCancelMu.Lock(); chargeCancel = nil; chargeCancelMu.Unlock() }()
	if _, err = s.db.Exec("CREATE TRIGGER charge_activate_cancel BEFORE INSERT ON schema_migrations WHEN NEW.version=15 BEGIN SELECT rydd_test_charge_activate_cancel(); END"); err != nil {
		t.Fatal(err)
	}
	v, err := s.ActivateCPUCharges(ctx, chargeNow())
	if err == nil || v.Available {
		t.Fatal(v, err)
	}
	var version, count int
	if err = s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 14 {
		t.Fatal(version, err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='worker_self_cpu_charges'").Scan(&count); err != nil || count != 0 {
		t.Fatal("half activation survived", count, err)
	}
}

func TestCPUChargesPublicationAndDefiniteFailureReturnSavedState(t *testing.T) {
	for _, op := range []string{"activate", "begin", "sample", "finish", "recover"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			s, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var m CPUSessionMarker
			start := chargeStart(0, chargeNow(), 0, "a")
			q := chargeSample(1, chargeNow().Add(time.Second), 1, int64(time.Second))
			if op != "activate" {
				if _, err = s.ActivateCPUCharges(ctx, chargeNow()); err != nil {
					t.Fatal(err)
				}
			}
			if op == "sample" || op == "finish" || op == "recover" {
				m, _, err = s.BeginCPUSession(ctx, start)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := chargeBytes(t, s)
			base, cancel := context.WithCancel(ctx)
			defer cancel()
			uncertain := &cpuCommitCancelContext{Context: base, cancel: cancel, store: s}
			var returned CPUChargeState
			switch op {
			case "activate":
				returned, err = s.ActivateCPUCharges(uncertain, chargeNow())
			case "begin":
				m, returned, err = s.BeginCPUSession(uncertain, start)
			case "sample":
				returned, err = s.SampleCPUSession(uncertain, m, q)
			case "finish":
				returned, err = s.FinishCPUSession(uncertain, m, q)
			case "recover":
				returned, err = s.RecoverCPUSession(uncertain, chargeNow().Add(time.Second))
			}
			raw, _ := json.Marshal(returned)
			if !errors.Is(err, ErrCPUChargesPublication) || !errors.Is(err, context.Canceled) || !bytes.Equal(raw, before) {
				t.Fatal("publication claimed candidate as saved", returned, err)
			}
			committed := chargeBytes(t, s)
			if bytes.Equal(before, committed) {
				t.Fatal("fixture did not commit")
			}
			switch op {
			case "activate":
				_, err = s.ActivateCPUCharges(ctx, chargeNow())
			case "begin":
				_, _, err = s.BeginCPUSession(ctx, start)
			case "sample":
				_, err = s.SampleCPUSession(ctx, m, q)
			case "finish":
				_, err = s.FinishCPUSession(ctx, m, q)
			case "recover":
				_, err = s.RecoverCPUSession(ctx, chargeNow().Add(time.Minute))
			}
			if err != nil || !bytes.Equal(committed, chargeBytes(t, s)) {
				t.Fatal("uncertain retry changed saved outcome", err)
			}
		})
	}
	s, _ := chargeFixture(t)
	ctx := context.Background()
	m, prior, err := s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), 0, "a"))
	if err != nil {
		t.Fatal(err)
	}
	before := chargeBytes(t, s)
	if _, err = s.db.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	returned, err := s.SampleCPUSession(ctx, m, chargeSample(1, chargeNow().Add(time.Second), 1, 1))
	raw, _ := json.Marshal(returned)
	expected, _ := json.Marshal(prior)
	if err == nil || errors.Is(err, ErrCPUChargesPublication) || !bytes.Equal(raw, expected) || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("definite failure fabricated state", returned, err)
	}
}

func TestCPUChargesUnknownInitialPrefixAndBoundedShape(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			s, _ := chargeFixture(t)
			ctx := context.Background()
			start := chargeStart(0, chargeNow(), -1, "a")
			if !invalid {
				start.SelfCPUNS = nil
				start.Reason = "cpu_observation_unavailable"
			}
			_, v, err := s.BeginCPUSession(ctx, start)
			if !errors.Is(err, ErrCPUChargesUnknown) || v.Status != "unknown" || v.ChargedCPUNS != 0 || v.UnknownTailSessions != 1 || v.OpenTailUnobserved || v.Session.LastSelfCPUNS != nil {
				t.Fatal("invalid prefix fabricated CPU", v, err)
			}
			before := chargeBytes(t, s)
			if _, _, err = s.BeginCPUSession(ctx, start); !errors.Is(err, ErrCPUChargesUnknown) || !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("unknown Begin exact retry changed outcome", err)
			}
		})
	}
	s, _ := chargeFixture(t)
	ctx := context.Background()
	_, v, err := s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), math.MaxInt64, "a"))
	if err != nil || v.ChargedCPUNS != math.MaxInt64 || !v.Session.LastBackoffCapped || !v.OpenTailUnobserved {
		t.Fatal(v, err)
	}
	v.Generation = math.MaxInt64
	v.ClosedSessions = math.MaxInt64 - 1
	v.UnknownTailSessions = v.ClosedSessions
	v.RecoveredSessions = math.MaxInt64 - 1
	v.Session.Generation = v.Generation
	v.Session.Start.ExpectedGeneration = v.Generation - 1
	chargeOverwrite(t, s, v)
	raw := chargeBytes(t, s)
	if len(raw) > CPUChargesMaxJSONBytes {
		t.Fatal("bounded payload exceeded", len(raw))
	}
	var rows int
	if err = s.db.QueryRow("SELECT count(*) FROM worker_self_cpu_charges").Scan(&rows); err != nil || rows != 1 {
		t.Fatal(rows, err)
	}
}

func TestCPUChargesBeginRetryCannotRebindReopenedWriter(t *testing.T) {
	s, dir := chargeFixture(t)
	ctx := context.Background()
	start := chargeStart(0, chargeNow(), 100, "a")
	original, _, err := s.BeginCPUSession(ctx, start)
	if err != nil {
		t.Fatal(err)
	}
	sameWriter, state, err := s.BeginCPUSession(ctx, start)
	if err != nil || sameWriter.Generation() != 0 || sameWriter.Nonce() != "" || state.Generation != 1 {
		t.Fatal("same-writer retry minted a marker", sameWriter, state, err)
	}
	before := chargeBytes(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	retry, state, err := reopened.BeginCPUSession(ctx, start)
	if err != nil || retry.Generation() != 0 || retry.Nonce() != "" || state.Status != "active" || !bytes.Equal(before, chargeBytes(t, reopened)) {
		t.Fatal("reopened retry reconstructed or mutated active tracking", retry, state, err)
	}
	for _, marker := range []CPUSessionMarker{retry, original} {
		if _, err = reopened.SampleCPUSession(ctx, marker, chargeSample(1, chargeNow().Add(time.Second), 110, int64(time.Second))); !errors.Is(err, ErrCPUChargesStale) || !bytes.Equal(before, chargeBytes(t, reopened)) {
			t.Fatal("old session continued through reopened marker", err)
		}
	}
	state, err = reopened.RecoverCPUSession(ctx, chargeNow().Add(time.Second))
	if err != nil || state.UnknownTailSessions != 1 || state.RecoveredSessions != 1 || state.ChargedCPUNS != 100 {
		t.Fatal(state, err)
	}
	next, state, err := reopened.BeginCPUSession(ctx, chargeStart(1, chargeNow().Add(2*time.Second), 130, "b"))
	if err != nil || next.Generation() != 2 || state.ChargedCPUNS != 230 || state.UnknownTailSessions != 1 || !state.NextAllowedAt.Equal(chargeNow().Add(time.Second+time.Hour)) {
		t.Fatal("new prefix not charged after explicit recovery", state, err)
	}
}

func TestCPUChargesSampleRetryAfterRecoveryStaysUnknown(t *testing.T) {
	s, _ := chargeFixture(t)
	ctx := context.Background()
	marker, _, err := s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), 100, "a"))
	if err != nil {
		t.Fatal(err)
	}
	sample := chargeSample(1, chargeNow().Add(time.Second), 110, int64(time.Second))
	if _, err = s.SampleCPUSession(ctx, marker, sample); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RecoverCPUSession(ctx, chargeNow().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	before := chargeBytes(t, s)
	returned, err := s.SampleCPUSession(ctx, marker, sample)
	if !errors.Is(err, ErrCPUChargesUnknown) || returned.Status != "recovered_unknown" || !bytes.Equal(before, chargeBytes(t, s)) {
		t.Fatal("recovered terminal sample retry claimed success", returned, err)
	}
}

func TestCPUChargesNoopReplyCancellationPreservesHistoricalState(t *testing.T) {
	for _, op := range []string{"view", "activate", "begin", "sample", "sample_recovered", "recover_untracked", "recover_terminal"} {
		t.Run(op, func(t *testing.T) {
			s, _ := chargeFixture(t)
			ctx := context.Background()
			start := chargeStart(0, chargeNow(), 100, "a")
			sample := chargeSample(1, chargeNow().Add(time.Second), 110, int64(time.Second))
			var marker CPUSessionMarker
			var err error
			if op == "begin" || op == "sample" || op == "sample_recovered" || op == "recover_terminal" {
				marker, _, err = s.BeginCPUSession(ctx, start)
				if err != nil {
					t.Fatal(err)
				}
			}
			if op == "sample" || op == "sample_recovered" {
				if _, err = s.SampleCPUSession(ctx, marker, sample); err != nil {
					t.Fatal(err)
				}
			}
			if op == "sample_recovered" || op == "recover_terminal" {
				if _, err = s.RecoverCPUSession(ctx, chargeNow().Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			before := chargeBytes(t, s)
			base, cancel := context.WithCancel(ctx)
			defer cancel()
			canceledReply := &cpuCommitCancelContext{Context: base, cancel: cancel, store: s}
			var returned CPUChargeState
			var retried CPUSessionMarker
			switch op {
			case "view":
				returned, err = s.CPUCharges(canceledReply)
			case "activate":
				returned, err = s.ActivateCPUCharges(canceledReply, chargeNow())
			case "begin":
				retried, returned, err = s.BeginCPUSession(canceledReply, start)
			case "sample", "sample_recovered":
				returned, err = s.SampleCPUSession(canceledReply, marker, sample)
			case "recover_untracked", "recover_terminal":
				returned, err = s.RecoverCPUSession(canceledReply, chargeNow().Add(3*time.Second))
			}
			raw, _ := json.Marshal(returned)
			if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCPUChargesPublication) || retried.Generation() != 0 || !bytes.Equal(raw, before) || !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("no-op reply ignored cancellation or changed saved state", op, returned, err)
			}
		})
	}
}

func TestCPUChargesOpeningRefusesOversizedOrWrongTypeLedgerNames(t *testing.T) {
	for _, activated := range []bool{false, true} {
		for _, wrongType := range []bool{false, true} {
			t.Run(fmt.Sprintf("activated=%v/blob=%v", activated, wrongType), func(t *testing.T) {
				ctx := context.Background()
				dir := privateDir(t)
				s, err := OpenWriter(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				if activated {
					if _, err = s.ActivateCPUCharges(ctx, chargeNow()); err != nil {
						t.Fatal(err)
					}
				}
				var corrupt any = migrations[0].name + strings.Repeat("x", 128*1024)
				if wrongType {
					corrupt = []byte(migrations[0].name)
				}
				if _, err = s.db.Exec("UPDATE schema_migrations SET name=? WHERE version=1", corrupt); err != nil {
					t.Fatal(err)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				for _, reader := range []bool{true, false} {
					var opened *Store
					if reader {
						opened, err = OpenReader(ctx, dir)
					} else {
						opened, err = OpenWriter(ctx, dir)
					}
					if opened != nil {
						opened.Close()
					}
					if err == nil || !strings.Contains(err.Error(), "unexpected migration ledger") || len(err.Error()) > 256 {
						t.Fatal("opening accepted/allocated or disclosed malformed name", reader, err)
					}
				}
				raw, err := connect(ctx, filepath.Join(dir, Filename), true)
				if err != nil {
					t.Fatal(err)
				}
				defer raw.Close()
				var version, size int
				var kind string
				if err = raw.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
					t.Fatal(err)
				}
				expected := 14
				if activated {
					expected = 15
				}
				if version != expected {
					t.Fatal("failed open changed schema", version)
				}
				if err = raw.db.QueryRow("SELECT typeof(name),length(CAST(name AS BLOB)) FROM schema_migrations WHERE version=1").Scan(&kind, &size); err != nil {
					t.Fatal(err)
				}
				if wrongType && kind != "blob" || !wrongType && size <= 128*1024 {
					t.Fatal("failed opening rewrote malformed history", kind, size)
				}
			})
		}
	}
}
