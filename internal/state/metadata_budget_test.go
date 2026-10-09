package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func metadataNow() time.Time { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) }

func metadataWriterFixture(t *testing.T) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func metadataBudgetFixture(t *testing.T, s *Store, now time.Time, limit int64) MetadataBudget {
	t.Helper()
	b, err := s.MetadataBudget(context.Background(), now, limit)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func metadataScanJob(t *testing.T, s *Store, now time.Time) *Job {
	t.Helper()
	ctx := context.Background()
	if err := s.SyncRoots(ctx, []string{"/generated/fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), now); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimJob(ctx, []string{ScanKind}, now, time.Hour)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	return j
}

func TestMetadataReservationsPersistAndNeverRefund(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	s, dir := metadataWriterFixture(t)
	b := metadataBudgetFixture(t, s, now, 30)
	if !b.Available || b.Status != "untracked" || b.DayCharges != nil || b.TotalCharges != nil || b.TrackingStartedAt != nil || b.PreTrackingUsage != "unknown" {
		t.Fatal(b)
	}
	first, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, 10, 30)
	if err != nil || !metadataDigest(first.Token, 64) || !first.StartedAt.Equal(now) || !first.ClockHighWater.Equal(now) || first.Day != now.Format(time.DateOnly) {
		t.Fatal(first, err)
	}
	b = metadataBudgetFixture(t, s, now, 30)
	if b.DayCharges.Reserved != 10 || b.DayCharges.OutstandingReserved != 10 || b.DayCharges.Observed != 0 || b.DayCharges.KnownUnusedReserved != 0 || !b.TrackingStartedAt.Equal(now) {
		t.Fatal(b)
	}
	if err = s.SettleMetadata(ctx, first, 3, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	b = metadataBudgetFixture(t, s, now.Add(time.Second), 30)
	if *b.DayCharges != (MetadataCharges{Reserved: 10, Observed: 3, KnownUnusedReserved: 7}) {
		t.Fatal(b)
	}
	if err = s.SettleMetadata(ctx, first, 3, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if after := metadataBudgetFixture(t, s, now.Add(time.Second), 30); !reflect.DeepEqual(b, after) {
		t.Fatal("idempotent settlement changed accounting", after)
	}
	second, err := s.ReserveMetadata(ctx, now.Add(2*time.Second), MetadataStartup, nil, 10, 30)
	if err != nil || second.Token == first.Token {
		t.Fatal(second, err)
	}
	if err = s.SettleMetadata(ctx, first, 3, now.Add(2*time.Second)); !errors.Is(err, ErrMetadataStale) {
		t.Fatal(err)
	}
	job := metadataScanJob(t, s, now.Add(3*time.Second))
	next, err := s.ReserveMetadata(ctx, now.Add(3*time.Second), MetadataNext, job, 8, 30)
	if err != nil || next.JobID != job.ID || next.JobToken != job.Token || !next.ExpiresAt.Equal(job.LeaseUntil) {
		t.Fatal(next, err)
	}
	b = metadataBudgetFixture(t, s, now.Add(3*time.Second), 30)
	if b.DayCharges.Reserved != 28 || b.DayCharges.OutstandingReserved != 18 || b.DayCharges.KnownUnusedReserved != 7 {
		t.Fatal(b)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(4*time.Second), MetadataNext, job, 1, 30); !errors.Is(err, ErrMetadataOutstanding) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if after := metadataBudgetFixture(t, r, now.Add(3*time.Second), 30); !reflect.DeepEqual(b, after) {
		t.Fatal("opening reader changed accounting", after)
	}
	if _, err = r.RecoverMetadataReservations(ctx, now); !errors.Is(err, ErrMetadataReadOnly) {
		t.Fatal(err)
	}
	r.Close()
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if after := metadataBudgetFixture(t, s, now.Add(3*time.Second), 30); !reflect.DeepEqual(b, after) {
		t.Fatal("opening writer recovered outstanding usage", after)
	}
	n, err := s.RecoverMetadataReservations(ctx, now.Add(4*time.Second))
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	b = metadataBudgetFixture(t, s, now.Add(4*time.Second), 30)
	if *b.DayCharges != (MetadataCharges{Reserved: 28, Observed: 3, UnknownReserved: 18, KnownUnusedReserved: 7}) || *b.TotalCharges != *b.DayCharges {
		t.Fatal(b)
	}
	n, err = s.RecoverMetadataReservations(ctx, now.Add(5*time.Second))
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if err = s.SettleMetadata(ctx, next, 0, now.Add(5*time.Second)); !errors.Is(err, ErrMetadataStale) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(5*time.Second), MetadataStartup, nil, 3, 30); !errors.Is(err, ErrMetadataDailyQuota) || !errors.Is(err, ErrMetadataDeferred) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(5*time.Second), MetadataStartup, nil, 1, 10); !errors.Is(err, ErrMetadataDailyQuota) {
		t.Fatal(err)
	}
	var budgets, receipts int
	if err = s.db.QueryRow("SELECT count(*) FROM scan_metadata_budget").Scan(&budgets); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM scan_metadata_reservations").Scan(&receipts); err != nil || budgets != 1 || receipts != 2 {
		t.Fatal(budgets, receipts, err)
	}
}

func TestMetadataMidnightAndClockRollback(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 9, 23, 59, 59, 0, time.UTC)
	s, _ := metadataWriterFixture(t)
	first, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, 6, 6)
	midnight := now.Add(time.Second)
	if err != nil || !first.ExpiresAt.Equal(midnight) {
		t.Fatal(first, err)
	}
	b := metadataBudgetFixture(t, s, now, 6)
	if b.Reason != "daily_metadata_limit" || !b.NextAllowedAt.Equal(midnight) {
		t.Fatal(b)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(-time.Second), MetadataNext, nil, 1, 6); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(-time.Second), MetadataStartup, nil, 1, 6); !errors.Is(err, ErrMetadataClockRollback) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, midnight, MetadataStartup, nil, 1, 6); !errors.Is(err, ErrMetadataOutstanding) {
		t.Fatal(err)
	}
	b = metadataBudgetFixture(t, s, midnight, 6)
	if b.Reason != "settlement_required" || b.Day != now.Format(time.DateOnly) || b.DayCharges.OutstandingReserved != 6 {
		t.Fatal(b)
	}
	late := midnight.Add(time.Minute)
	if err = s.SettleMetadata(ctx, first, 2, late); err != nil {
		t.Fatal(err)
	}
	b = metadataBudgetFixture(t, s, midnight.Add(30*time.Second), 6)
	if b.Reason != "clock_rollback" || !b.ClockHighWater.Equal(late) || !b.NextAllowedAt.Equal(late) {
		t.Fatal(b)
	}
	if _, err = s.ReserveMetadata(ctx, midnight.Add(30*time.Second), MetadataStartup, nil, 1, 6); !errors.Is(err, ErrMetadataClockRollback) {
		t.Fatal(err)
	}
	second, err := s.ReserveMetadata(ctx, late, MetadataStartup, nil, 6, 6)
	if err != nil || second.Day != midnight.Format(time.DateOnly) {
		t.Fatal(second, err)
	}
	b = metadataBudgetFixture(t, s, late, 6)
	if *b.DayCharges != (MetadataCharges{Reserved: 6, OutstandingReserved: 6}) || b.TotalCharges.Reserved != 12 || b.TotalCharges.Observed != 2 || b.TotalCharges.KnownUnusedReserved != 4 {
		t.Fatal(b)
	}
	jump := late.Add(72 * time.Hour)
	if n, err := s.RecoverMetadataReservations(ctx, jump); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	b = metadataBudgetFixture(t, s, late.Add(24*time.Hour), 6)
	if b.Reason != "clock_rollback" || !b.ClockHighWater.Equal(jump) {
		t.Fatal(b)
	}
	if _, err = s.ReserveMetadata(ctx, jump.Add(-time.Second), MetadataStartup, nil, 1, 6); !errors.Is(err, ErrMetadataClockRollback) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, jump, MetadataStartup, nil, 1, 6); err != nil {
		t.Fatal(err)
	}
	b = metadataBudgetFixture(t, s, jump, 6)
	if b.DayCharges.Reserved != 1 || b.TotalCharges.Reserved != 13 || b.TotalCharges.UnknownReserved != 6 {
		t.Fatal(b)
	}
}

func TestMetadataJobBindingAndSettlementFencing(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	s, _ := metadataWriterFixture(t)
	j := metadataScanJob(t, s, now)
	bad := *j
	bad.Token = strings.Repeat("a", 32)
	if _, err := s.ReserveMetadata(ctx, now, MetadataNext, &bad, 4, 20); !errors.Is(err, ErrMetadataStale) {
		t.Fatal(err)
	}
	r, err := s.ReserveMetadata(ctx, now, MetadataNext, j, 4, 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, j.LeaseUntil, MetadataStartup, nil, 1, 20); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*MetadataReservation){
		"token":     func(r *MetadataReservation) { r.Token = strings.Repeat("a", 64) },
		"job":       func(r *MetadataReservation) { r.JobID++ },
		"job token": func(r *MetadataReservation) { r.JobToken = strings.Repeat("b", 32) },
		"allowance": func(r *MetadataReservation) { r.Allowance++ },
		"expiry":    func(r *MetadataReservation) { r.ExpiresAt = r.ExpiresAt.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := r
			change(&changed)
			if err := s.SettleMetadata(ctx, changed, 1, now); !errors.Is(err, ErrMetadataStale) {
				t.Fatal(err)
			}
		})
	}
	if err = s.SettleMetadata(ctx, r, 5, now); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal(err)
	}
	if err = s.FinishJob(ctx, *j, false, j.Cursor, now, ""); err != nil {
		t.Fatal(err)
	}
	// Job completion does not erase the reservation's usage; its exact token
	// still fences this settlement even when the job lease has been released.
	if err = s.SettleMetadata(ctx, r, 1, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleMetadata(ctx, r, 2, now.Add(2*time.Hour)); !errors.Is(err, ErrMetadataStale) {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(2*time.Hour), MetadataNext, j, 1, 20); !errors.Is(err, ErrMetadataStale) {
		t.Fatal(err)
	}
}

func TestMetadataReservationInputBoundsAndCancellation(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	s, _ := metadataWriterFixture(t)
	for _, test := range []struct {
		scope         MetadataScope
		amount, limit int64
		at            time.Time
	}{
		{MetadataStartup, 0, 10, now}, {MetadataStartup, -1, 10, now}, {MetadataStartup, MetadataAllowanceLimit + 1, MetadataDailyLimit, now},
		{MetadataStartup, 1, 0, now}, {MetadataStartup, 1, MetadataDailyLimit + 1, now}, {"other", 1, 10, now},
		{MetadataStartup, 1, 10, time.Time{}}, {MetadataStartup, 1, 10, time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if _, err := s.ReserveMetadata(ctx, test.at, test.scope, nil, test.amount, test.limit); !errors.Is(err, ErrMetadataInvalid) {
			t.Fatal(test, err)
		}
	}
	if _, err := s.ReserveMetadata(ctx, now, MetadataStartup, &Job{}, 1, 10); !errors.Is(err, ErrMetadataInvalid) {
		t.Fatal(err)
	}
	if _, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, 11, 10); !errors.Is(err, ErrMetadataDailyQuota) {
		t.Fatal(err)
	}
	if b := metadataBudgetFixture(t, s, now, 10); b.Status != "untracked" || b.DayCharges != nil {
		t.Fatal("refused reservation initialized tracking", b)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ReserveMetadata(canceled, now, MetadataStartup, nil, 1, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.MetadataBudget(canceled, now, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.RecoverMetadataReservations(canceled, now); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, MetadataAllowanceLimit, MetadataDailyLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SettleMetadata(canceled, r, 0, now); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b := metadataBudgetFixture(t, s, now, MetadataDailyLimit); b.DayCharges.OutstandingReserved != MetadataAllowanceLimit {
		t.Fatal(b)
	}
}

func TestMetadataOverflowCorruptionAndTransactionRollback(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	s, _ := metadataWriterFixture(t)
	r, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SettleMetadata(ctx, r, 0, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE scan_metadata_budget SET total_reserved=?", int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now, MetadataStartup, nil, 1, 10); !errors.Is(err, ErrMetadataOverflow) {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE scan_metadata_budget SET total_reserved=1"); err != nil {
		t.Fatal(err)
	}
	before := metadataBudgetFixture(t, s, now, 10)
	if _, err = s.db.Exec(`CREATE TRIGGER reject_metadata_reservation BEFORE INSERT ON scan_metadata_reservations
 BEGIN SELECT RAISE(ABORT,'generated private failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveMetadata(ctx, now.Add(time.Second), MetadataStartup, nil, 1, 10); !errors.Is(err, ErrMetadataUnavailable) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if after := metadataBudgetFixture(t, s, now, 10); !reflect.DeepEqual(before, after) {
		t.Fatal("failed publication partly charged a reservation", after)
	}
	if _, err = s.db.Exec("PRAGMA ignore_check_constraints=ON; UPDATE scan_metadata_budget SET observed=2"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MetadataBudget(ctx, now, 10); !errors.Is(err, ErrMetadataCorrupt) {
		t.Fatal(err)
	}
}

func metadataSchema9Fixture(t *testing.T, conflict bool) string {
	t.Helper()
	dir := privateDir(t)
	ctx := context.Background()
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := connect(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,?)", i+1, migrations[i].name, metadataNow().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=9; INSERT INTO roots(path) VALUES(X'2f66697874757265'); INSERT INTO daily_budgets VALUES('2026-10-09',42,17)"); err != nil {
		t.Fatal(err)
	}
	if conflict {
		if _, err = s.db.Exec("CREATE TABLE scan_metadata_reservations(conflict TEXT)"); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestMetadataSchema9ReadMigrationAndPreservation(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	dir := metadataSchema9Fixture(t, false)
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	b := metadataBudgetFixture(t, r, now, 10)
	if b.Available || b.Status != "unavailable" || b.Reason != "schema_unavailable" || b.DayCharges != nil || b.TotalCharges != nil || b.PreTrackingUsage != "unknown" {
		t.Fatal(b)
	}
	var inventory string
	if err = r.db.QueryRow("SELECT token FROM inventory_identity").Scan(&inventory); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Summary(ctx); err != nil {
		t.Fatal(err)
	}
	r.Close()
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var preserved string
	var bytes, ops int64
	if err = w.db.QueryRow("SELECT token FROM inventory_identity").Scan(&preserved); err != nil || preserved != inventory || w.schema != schemaVersion {
		t.Fatal(preserved, err)
	}
	if err = w.db.QueryRow("SELECT content_bytes,metadata_ops FROM daily_budgets").Scan(&bytes, &ops); err != nil || bytes != 42 || ops != 17 {
		t.Fatal(bytes, ops, err)
	}
	if b = metadataBudgetFixture(t, w, now, 10); !b.Available || b.Status != "untracked" || b.DayCharges != nil {
		t.Fatal(b)
	}
	if _, err = w.ReserveMetadata(ctx, now, MetadataStartup, nil, 1, 10); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataMigrationRollback(t *testing.T) {
	ctx := context.Background()
	dir := metadataSchema9Fixture(t, true)
	if _, err := OpenWriter(ctx, dir); err == nil {
		t.Fatal("conflicting migration succeeded")
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var version, table, ledger int
	if err = r.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 9 {
		t.Fatal(version, err)
	}
	if err = r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='scan_metadata_budget'").Scan(&table); err != nil || table != 0 {
		t.Fatal(table, err)
	}
	if err = r.db.QueryRow("SELECT count(*) FROM schema_migrations WHERE version=10").Scan(&ledger); err != nil || ledger != 0 {
		t.Fatal(ledger, err)
	}
}

func TestMetadataSQLTypeAndScopeBounds(t *testing.T) {
	s, _ := metadataWriterFixture(t)
	for _, sql := range []string{
		"INSERT INTO scan_metadata_budget VALUES(1,1,'2026-10-09',1,0.5,0,0,0,0,0)",
		"INSERT INTO scan_metadata_budget VALUES(2,1,'2026-10-09',1,0,0,0,0,0,0)",
		fmt.Sprintf("INSERT INTO scan_metadata_reservations VALUES('other','%s',0,'','2026-10-09',1,2,1,1,'reserved',NULL)", strings.Repeat("a", 64)),
	} {
		if _, err := s.db.Exec(sql); err == nil {
			t.Fatal("invalid SQL state accepted")
		}
	}
}

func TestMetadataRetainedReceiptsRejectLostCharges(t *testing.T) {
	ctx := context.Background()
	now := metadataNow()
	for _, test := range []struct {
		name, edit string
		unknown    bool
	}{
		{"observed", "UPDATE scan_metadata_budget SET observed=0,total_observed=0", false},
		{"unknown", "UPDATE scan_metadata_budget SET unknown_reserved=0,total_unknown_reserved=0", true},
		{"orphan", "DELETE FROM scan_metadata_budget", false},
		{"noninteger", "PRAGMA ignore_check_constraints=ON; UPDATE scan_metadata_budget SET reserved=4.5,total_reserved=4.5", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := metadataWriterFixture(t)
			r, err := s.ReserveMetadata(ctx, now, MetadataStartup, nil, 4, 10)
			if err != nil {
				t.Fatal(err)
			}
			if test.unknown {
				if _, err = s.RecoverMetadataReservations(ctx, now); err != nil {
					t.Fatal(err)
				}
			} else if err = s.SettleMetadata(ctx, r, 2, now); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec(test.edit); err != nil {
				t.Fatal(err)
			}
			if _, err = s.MetadataBudget(ctx, now, 10); err == nil {
				t.Fatal("inconsistent saved charge was accepted")
			}
		})
	}
}
