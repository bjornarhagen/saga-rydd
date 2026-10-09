package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

func revisitListing(t *testing.T, s *Store, root int64, at time.Time) {
	t.Helper()
	if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=? WHERE id=?", at.UnixNano(), root); err != nil {
		t.Fatal(err)
	}
}

func TestInventoryRevisitPreservesUnfinishedRootAndSchedulesHealthyRoot(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	revisitListing(t, s, 1, now.Add(-48*time.Hour))
	revisitListing(t, s, 2, now.Add(-time.Hour))
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("interrupted"), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE jobs SET cursor=?,last_error='offline',attempts=4 WHERE root_id=1", []byte{0xff, 0, 2}); err != nil {
		t.Fatal(err)
	}
	if inserted, err := s.ScheduleInventoryRevisit(ctx, 1, now, InventoryRevisitInterval); err != nil || inserted {
		t.Fatal("revisited unfinished root", inserted, err)
	}
	if inserted, err := s.ScheduleInventoryRevisit(ctx, 2, now, InventoryRevisitInterval); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	var cursor []byte
	var due, attempts int64
	var fault string
	if err := s.db.QueryRow("SELECT cursor,due_at_ns,last_error,attempts FROM jobs WHERE root_id=1").Scan(&cursor, &due, &fault, &attempts); err != nil || !bytes.Equal(cursor, []byte{0xff, 0, 2}) || due != now.Add(time.Hour).UnixNano() || fault != "offline" || attempts != 4 {
		t.Fatal(cursor, due, fault, attempts, err)
	}
	if err := s.db.QueryRow("SELECT due_at_ns FROM jobs WHERE root_id=2").Scan(&due); err != nil || due != now.Add(23*time.Hour).UnixNano() {
		t.Fatal(due, err)
	}
	// Root-listing time remains unchanged: queuing another listing proves no
	// completed descendant pass and never manufactures a new observation.
	var listing int64
	if err := s.db.QueryRow("SELECT last_scan_ns FROM roots WHERE id=2").Scan(&listing); err != nil || listing != now.Add(-time.Hour).UnixNano() {
		t.Fatal(listing, err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries") != 0 || countRows(t, s, "SELECT count(*) FROM directories") != 0 {
		t.Fatal("scheduler invented observations")
	}
}

func TestInventoryRevisitSavedDueSurvivesRestartRollbackAndNoCatchup(t *testing.T) {
	ctx := context.Background()
	s, dir := queueStore(t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	revisitListing(t, s, 1, now)
	if inserted, err := s.ScheduleInventoryRevisit(ctx, 1, now, InventoryRevisitInterval); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	due := now.Add(InventoryRevisitInterval)
	if found, err := s.InventoryRevisitPending(ctx, due, InventoryRevisitInterval); err != nil || !found {
		t.Fatal(found, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if inserted, err := w.ScheduleInventoryRevisit(ctx, 1, now.Add(-time.Hour), InventoryRevisitInterval); err != nil || inserted {
		t.Fatal("rollback changed existing due", inserted, err)
	}
	if inserted, err := w.ScheduleInventoryRevisit(ctx, 1, now.Add(5*24*time.Hour), InventoryRevisitInterval); err != nil || inserted {
		t.Fatal("catch-up duplicated listing", inserted, err)
	}
	if next, err := w.NextJobDue(ctx, []string{ScanKind}); err != nil || !next.Equal(due) {
		t.Fatal(next, err)
	}
	j, err := w.ClaimJob(ctx, []string{ScanKind}, due, time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if _, err := w.ScheduleInventoryRevisit(ctx, 1, due, InventoryRevisitInterval); err != nil {
		t.Fatal(err)
	}
	if err := w.FinishJob(ctx, *j, false, []byte("saved"), time.Unix(0, 1), ""); err != nil {
		t.Fatal(err)
	}
	if found, err := w.InventoryRevisitPending(ctx, time.Unix(0, 1), InventoryRevisitInterval); err != nil || found {
		t.Fatal("partial continuation mislabeled", found, err)
	}
	if inserted, err := w.ScheduleInventoryRevisit(ctx, 1, due, InventoryRevisitInterval); err != nil || inserted {
		t.Fatal(inserted, err)
	}
	// After all children drain, an overdue root listing yields only one job.
	j, err = w.ClaimJob(ctx, []string{ScanKind}, due, time.Minute)
	if err != nil || j == nil || string(j.Cursor) != "saved" {
		t.Fatal(j, err)
	}
	if err = w.FinishJob(ctx, *j, true, nil, due, ""); err != nil {
		t.Fatal(err)
	}
	later := now.Add(7 * 24 * time.Hour)
	if inserted, err := w.ScheduleInventoryRevisit(ctx, 1, later, InventoryRevisitInterval); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	if next, err := w.NextJobDue(ctx, []string{ScanKind}); err != nil || !next.Equal(later) {
		t.Fatal(next, err)
	}
	if countRows(t, w, "SELECT count(*) FROM jobs") != 1 {
		t.Fatal("missed intervals became catch-up credits")
	}
}

func TestInventoryRevisitRawPageBoundsDisabledBlockedAndAtomicCancel(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	roots := make([]string, 257)
	for i := range roots {
		roots[i] = fmt.Sprintf("/generated/root-%03d", i)
	}
	// Explicit generated legacy history: updated SyncRoots now refuses adding
	// more than 128 retained records, while old stores remain page-readable.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE roots SET enabled=0"); err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		if _, err = tx.ExecContext(ctx, "INSERT INTO roots(path,enabled) VALUES(?,1)", []byte(root)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Old roots consume the first two slots; every raw slot remains visible
	// to the cursor even when disabled or excluded by pending work.
	if _, err := s.db.Exec("UPDATE roots SET enabled=0 WHERE id<=128"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 129, ScanKind, []byte("child"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p, err := s.SeedInventoryRevisitPage(ctx, 0, now, InventoryRevisitInterval)
	if err != nil || p.Enqueued != 0 || p.Cursor != 128 || !p.More {
		t.Fatal("filtered rows refilled page", p, err)
	}
	p, err = s.SeedInventoryRevisitPage(ctx, p.Cursor, now, InventoryRevisitInterval)
	if err != nil || p.Enqueued != 127 || p.Cursor != 256 || !p.More {
		t.Fatal(p, err)
	}
	p, err = s.SeedInventoryRevisitPage(ctx, p.Cursor, now, InventoryRevisitInterval)
	if err != nil || p.Enqueued != 3 || p.More || p.Cursor != 0 {
		t.Fatal(p, err)
	}
	if countRows(t, s, "SELECT count(*) FROM jobs WHERE root_id=129") != 1 {
		t.Fatal("unfinished root was reseeded")
	}
	if _, err := s.db.Exec("DELETE FROM jobs WHERE root_id>=130"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	p, err = s.seedInventoryRevisitPage(canceled, 128, now, InventoryRevisitInterval, inventoryRevisitHooks{beforeCommit: cancel})
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(p, InventoryRevisitPage{}) || countRows(t, s, "SELECT count(*) FROM jobs") != 1 {
		t.Fatal("canceled page partially published", p, err)
	}
}

func TestInventoryRevisitMaintenanceModesAndManualSeedUnchanged(t *testing.T) {
	for _, maintenance := range []string{
		"INSERT INTO compact_retirement VALUES(1,X'2e',1)",
		"INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(1,X'2e',1)",
		"INSERT INTO subtree_retirement(root_id,path,scan_revision,preserve_entry) VALUES(1,X'2e',0,0)",
		"INSERT INTO allocation_cache(root_id,path) VALUES(1,X'2e')",
	} {
		t.Run(maintenance, func(t *testing.T) {
			s, _ := queueStore(t)
			if _, err := s.db.Exec(maintenance); err != nil {
				t.Fatal(err)
			}
			if inserted, err := s.ScheduleInventoryRevisit(context.Background(), 1, time.Now(), InventoryRevisitInterval); err != nil || inserted {
				t.Fatal("maintenance displaced", inserted, err)
			}
			if inserted, err := s.ScheduleInventoryRevisit(context.Background(), 2, time.Now(), InventoryRevisitInterval); err != nil || !inserted {
				t.Fatal("unrelated root blocked", inserted, err)
			}
		})
	}
	ctx := context.Background()
	s, _ := queueStore(t)
	revisitListing(t, s, 1, time.Now())
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute); err != nil || j == nil {
		t.Fatal("manual seed no longer immediate", j, err)
	}
	if _, err := s.db.Exec("DELETE FROM jobs; INSERT INTO settings(key,value) VALUES('inventory.compact',X'31')"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleInventoryRevisit(ctx, 1, time.Now(), InventoryRevisitInterval); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal(err)
	}
	if _, err := s.SeedInventoryRevisitPage(ctx, 0, time.Now(), InventoryRevisitInterval); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal(err)
	}
}

func TestInventoryRevisitInvalidEvidenceAndReadOnlyRefuseWithoutMutation(t *testing.T) {
	ctx := context.Background()
	s, dir := queueStore(t)
	now := time.Now()
	for _, interval := range []time.Duration{0, -1, 30*24*time.Hour + 1} {
		if _, err := s.ScheduleInventoryRevisit(ctx, 1, now, interval); !errors.Is(err, ErrInventoryRevisitInput) {
			t.Fatal(interval, err)
		}
	}
	for _, at := range []time.Time{time.Time{}, time.Unix(-1, 0), time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, err := s.SeedInventoryRevisitPage(ctx, 0, at, InventoryRevisitInterval); !errors.Is(err, ErrInventoryRevisitInput) {
			t.Fatal(at, err)
		}
	}
	for _, stamp := range []int64{-1, math.MaxInt64} {
		if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=? WHERE id=2", stamp); err != nil {
			t.Fatal(err)
		}
		if p, err := s.SeedInventoryRevisitPage(ctx, 0, now, InventoryRevisitInterval); !errors.Is(err, ErrInventoryRevisitCorrupt) || !reflect.DeepEqual(p, InventoryRevisitPage{}) || countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
			t.Fatal("invalid later root published earlier page", p, err)
		}
	}
	if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=NULL"); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.SeedInventoryRevisitPage(ctx, 0, now, InventoryRevisitInterval); err == nil {
		t.Fatal("reader scheduled work")
	}
	if _, err := r.ScheduleInventoryRevisit(ctx, 1, now, InventoryRevisitInterval); err == nil {
		t.Fatal("reader scheduled root")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ScheduleInventoryRevisit(canceled, 1, now, InventoryRevisitInterval); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestInventoryRevisitWaitQualificationPreservesRetryAndRawBound(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	now := time.Now()
	revisitListing(t, s, 1, now)
	due := now.Add(InventoryRevisitInterval)
	if _, err := s.ScheduleInventoryRevisit(ctx, 1, now, InventoryRevisitInterval); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"attempts=1", "cursor=X'01'", "last_error='retry'", "path=X'6368696c64'", "due_at_ns=due_at_ns+1"} {
		t.Run(mutation, func(t *testing.T) {
			if _, err := s.db.Exec("UPDATE jobs SET " + mutation + " WHERE root_id=1"); err != nil {
				t.Fatal(err)
			}
			if found, err := s.InventoryRevisitPending(ctx, due, InventoryRevisitInterval); err != nil || found {
				t.Fatal("retry/unknown became periodic wait", found, err)
			}
			if _, err := s.db.Exec("UPDATE jobs SET attempts=0,cursor=NULL,last_error='',path=X'2e',due_at_ns=? WHERE root_id=1", due.UnixNano()); err != nil {
				t.Fatal(err)
			}
		})
	}
	if found, err := s.InventoryRevisitPending(ctx, due, InventoryRevisitInterval); err != nil || !found {
		t.Fatal(found, err)
	}
	if _, err := s.db.Exec("DELETE FROM jobs"); err != nil {
		t.Fatal(err)
	}
	// 128 unrelated jobs occupy the raw proof bound; the later inventory row
	// cannot be claimed as the next qualified observation by this helper.
	for i := 0; i < 128; i++ {
		if err := s.EnqueueJob(ctx, 1, "fixture", []byte(fmt.Sprintf("other-%03d", i)), due); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ScheduleInventoryRevisit(ctx, 1, now, InventoryRevisitInterval); err != nil {
		t.Fatal(err)
	}
	if found, err := s.InventoryRevisitPending(ctx, due, InventoryRevisitInterval); err != nil || found {
		t.Fatal("raw proof limit refilled", found, err)
	}
}
