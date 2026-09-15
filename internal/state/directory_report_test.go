package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
)

func directoryFixture(t *testing.T) *Store {
	t.Helper()
	s, err := OpenWriter(context.Background(), privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.SyncRoots(context.Background(), []string{"/fixture"}); err != nil {
		t.Fatal(err)
	}
	put := func(path, parent, kind, ino string, size, allocated, generation int64) {
		t.Helper()
		_, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,?,?,1,1,'dev',?,?,10)`, []byte(path), []byte(parent), kind, size, allocated, ino, generation)
		if err != nil {
			t.Fatal(err)
		}
	}
	put(".", "", "directory", "1", 0, 0, 1)
	put("a", ".", "directory", "2", 0, 0, 1)
	put("a/f", "a", "file", "42", 100, 4096, 2)
	put("a/link", "a", "file", "42", 100, 4096, 2)
	put("a/sparse", "a", "file", "43", 1000, 0, 2)
	put("a2", ".", "directory", "3", 0, 0, 1)
	put("a2/other", "a2", "file", "44", 500, 4096, 3)
	for _, d := range []struct {
		path string
		gen  int
	}{{".", 1}, {"a", 2}, {"a2", 3}} {
		if _, err = s.db.Exec("INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,?,1,20)", []byte(d.path), d.gen); err != nil {
			t.Fatal(err)
		}
	}
	return s
}
func TestDirectoryScopeHardlinksAndCompleteness(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	r, err := s.MeasureDirectory(ctx, "/fixture/a")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "recorded_complete" || r.CurrentStateVerified || *r.LogicalBytes != 1200 || *r.AllocatedBytes != 4096 || r.FilePaths != 3 || r.RepeatedInodes != 1 {
		t.Fatalf("%+v", r)
	}
	if _, err = s.MeasureDirectory(ctx, "/fixture-sibling"); !errors.Is(err, ErrDirectoryScope) {
		t.Fatal(err)
	}
	missing, err := s.MeasureDirectory(ctx, "/fixture/missing")
	if err != nil || missing.Status != "unknown" || missing.LogicalBytes != nil {
		t.Fatal(missing, err)
	}
	// A completed root listing cannot make a child subtree complete.
	if _, err = s.db.Exec("UPDATE directories SET complete=0 WHERE path=?", []byte("a")); err != nil {
		t.Fatal(err)
	}
	partial, err := s.MeasureDirectory(ctx, "/fixture")
	if err != nil || partial.Status != "partial" || partial.IncompleteDirectories != 1 {
		t.Fatal(partial, err)
	}
	// A removed/replaced ancestor invalidates descendants' apparent freshness.
	if _, err = s.db.Exec("UPDATE directories SET generation=99 WHERE path=?", []byte(".")); err != nil {
		t.Fatal(err)
	}
	stale, err := s.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || stale.Status != "stale" {
		t.Fatal(stale, err)
	}
}
func TestDirectoryBoundAndOverflow(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < DirectoryEntryLimit; i++ {
		_, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'2e','file',1,0,1,1,'','',1,10)`, []byte(fmt.Sprintf("z%05d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	r, err := s.MeasureDirectory(ctx, "/fixture")
	if err != nil || r.Status != "partial" || !r.Truncated || r.EntriesExamined != DirectoryEntryLimit {
		t.Fatal(r, err)
	}
	if _, err = s.db.Exec("UPDATE entries SET size=? WHERE path IN (?,?)", int64(math.MaxInt64), []byte("a/f"), []byte("a/link")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MeasureDirectory(ctx, "/fixture/a"); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestDirectoryEmptySkippedErrorsAndConflictingInodes(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	reader, err := OpenReader(ctx, filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err = s.db.Exec("UPDATE entries SET size=101,allocated=8192 WHERE path=?", []byte("a/link")); err != nil {
		t.Fatal(err)
	}
	conflict, err := reader.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || conflict.Status != "stale" || *conflict.AllocatedBytes != 8192 {
		t.Fatal(conflict, err)
	}
	if _, err = s.db.Exec("DELETE FROM entries WHERE parent=?", []byte("a")); err != nil {
		t.Fatal(err)
	}
	empty, err := reader.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || empty.Status != "recorded_complete" || empty.LogicalBytes == nil || *empty.LogicalBytes != 0 {
		t.Fatal(empty, err)
	}
	if _, err = s.db.Exec("UPDATE entries SET skip_reason='excluded' WHERE path=?", []byte("a")); err != nil {
		t.Fatal(err)
	}
	skipped, err := reader.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || skipped.Status != "partial" || skipped.SkippedEntries != 1 {
		t.Fatal(skipped, err)
	}
	if _, err = s.db.Exec("UPDATE roots SET last_error='unavailable'"); err != nil {
		t.Fatal(err)
	}
	unavailable, err := reader.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || unavailable.Status != "stale" || unavailable.RootError != "unavailable" {
		t.Fatal(unavailable, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = reader.MeasureDirectory(canceled, "/fixture/a"); err == nil {
		t.Fatal("cancellation ignored")
	}
}
