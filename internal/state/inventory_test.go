package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func scanJob(t *testing.T, s *Store) Job {
	t.Helper()
	j, err := s.ClaimJob(context.Background(), []string{ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	return *j
}
func batchFor(j Job) ScanBatch {
	return ScanBatch{Identity: "fixture-volume", Generation: 123, Directory: Entry{Path: j.Path, Kind: "directory"}, Cursor: []byte("cursor"), Entries: []Entry{{Path: []byte("child"), Kind: "directory"}, {Path: []byte("file"), Kind: "file", Size: 12}}}
}
func countRows(t *testing.T, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestInventoryBatchRollbackAndLeaseFence(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	if err := s.EnqueueJob(ctx, 1, ScanKind, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	j := scanJob(t, s)
	b := batchFor(j)
	b.Entries[1].Kind = "invalid-kind" // Fail after the root, first entry and child job writes.
	if err := s.CommitScan(ctx, j, b); err == nil {
		t.Fatal("invalid batch committed")
	}
	if countRows(t, s, "SELECT count(*) FROM entries") != 0 || countRows(t, s, "SELECT count(*) FROM jobs") != 1 || countRows(t, s, "SELECT count(*) FROM roots WHERE volume_id!=''") != 0 {
		t.Fatal("partial transaction survived")
	}
	b = batchFor(j)
	if err := s.CommitScan(ctx, j, b); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries") != 3 || countRows(t, s, "SELECT count(*) FROM jobs") != 2 {
		t.Fatal("missing observations or child job")
	}
	if err := s.CommitScan(ctx, j, b); !errors.Is(err, ErrStaleLease) {
		t.Fatal("stale batch accepted", err)
	}
	j = scanJob(t, s)
	if string(j.Path) != "." || string(j.Cursor) != "cursor" || j.RootIdentity != "fixture-volume" {
		t.Fatal(j)
	}
	b = batchFor(j)
	b.Identity = "changed"
	if err := s.CommitScan(ctx, j, b); err == nil {
		t.Fatal("changed identity accepted")
	}
	if countRows(t, s, "SELECT count(*) FROM jobs WHERE status='running'") != 1 {
		t.Fatal("lease update not rolled back")
	}
}

func TestDirectoryWatermarkAndFailureRetention(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	s.EnqueueJob(ctx, 1, ScanKind, nil, time.Now())
	j := scanJob(t, s)
	b := batchFor(j)
	b.Complete = true
	if err := s.CommitScan(ctx, j, b); err != nil {
		t.Fatal(err)
	}
	// Consume the child without altering its parent's observation generation.
	j = scanJob(t, s)
	if err := s.CommitScan(ctx, j, ScanBatch{Identity: "fixture-volume", Generation: 456, Directory: Entry{Path: j.Path, Kind: "directory"}, Complete: true}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries WHERE path=X'6368696c64' AND generation=123") != 1 {
		t.Fatal("child pass overwrote parent generation")
	}
	s.EnqueueJob(ctx, 1, ScanKind, nil, time.Now())
	j = scanJob(t, s)
	if err := s.CommitScan(ctx, j, ScanBatch{Fault: "permission denied"}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries") != 3 || countRows(t, s, "SELECT count(*) FROM directories WHERE last_error!=''") != 1 {
		t.Fatal("failed pass removed evidence")
	}
	// A successful later pass omits 'file'. Keep the old observation and use a
	// completed watermark; don't sweep the whole tree in one transaction.
	jptr, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now().Add(2*time.Hour), time.Minute)
	if err != nil || jptr == nil {
		t.Fatal(err)
	}
	b = batchFor(*jptr)
	b.Generation = 789
	b.Entries = b.Entries[:1]
	b.Complete = true
	if err := s.CommitScan(ctx, *jptr, b); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries WHERE path=X'66696c65' AND generation=123") != 1 || countRows(t, s, "SELECT count(*) FROM directories WHERE path=X'2e' AND generation=789 AND complete=1 AND last_error=''") != 1 {
		t.Fatal("reconciliation evidence wrong")
	}
}
