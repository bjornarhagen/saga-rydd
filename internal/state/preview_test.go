package state

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestPreviewExactSelectionAndEligibility(t *testing.T) {
	ctx := context.Background()
	s := directoryFixture(t)
	old := time.Now().Add(-60 * 24 * time.Hour).UnixNano()
	for _, e := range []struct{ path, kind string }{{"a/node_modules", "directory"}, {"a/package.json", "file"}, {"a2/node_modules", "directory"}, {"a2/package.json", "file"}} {
		parent, gen := "a", 2
		if e.path[:2] == "a2" {
			parent, gen = "a2", 3
		}
		if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,0,0,?,1,'dev',?,?,10)`, []byte(e.path), []byte(parent), e.kind, old, e.path, gen); err != nil {
			t.Fatal(err)
		}
	}
	full, err := s.NodeModulesFindings(ctx, "", 30)
	if err != nil || len(full.Findings) != 2 {
		t.Fatal(full, err)
	}
	id := full.Findings[1].ID
	r, err := s.PreviewFindings(ctx, []string{id}, 30)
	if err != nil || len(r.Findings) != 1 || r.Findings[0].ID != id || r.EntriesExamined != 1 || r.EntryLimit != 1 || r.PageCoverage != "selected_entries_only" || r.NextCursor != "" {
		t.Fatal(r, err)
	}
	if !reflect.DeepEqual(r.Findings[0].PathBytes, full.Findings[1].PathBytes) || r.CurrentStateVerified || len(r.Findings[0].Actions) != 0 {
		t.Fatal(r)
	}
	for _, ids := range [][]string{nil, {}, {id, id}, {"bad"}, {"node-modules-v1:01:1"}, {"node-modules-v1:+1:1"}, {"node-modules-v1:0:1"}, {"node-modules-v1:1:-1"}, {"node-modules-v1:1:9223372036854775808"}, {"node-modules-v1:2:1"}, {id, "node-modules-v1:1:99999999"}} {
		if r, err := s.PreviewFindings(ctx, ids, 30); err != ErrFindingSelection || len(r.Findings) != 0 {
			t.Fatal(ids, r, err)
		}
	}
	if _, err := s.PreviewFindings(ctx, []string{id}, 90); err != ErrFindingSelection {
		t.Fatal("default age ignored", err)
	}
	if _, err := s.PreviewFindings(ctx, []string{id}, 0); err != ErrFindingAge {
		t.Fatal(err)
	}
	changes := []string{
		"UPDATE roots SET enabled=0",
		"UPDATE entries SET kind='symlink' WHERE kind='directory' AND path!=X'2e'",
		"UPDATE entries SET skip_reason='excluded' WHERE path=X'61322f6e6f64655f6d6f64756c6573'",
		"UPDATE directories SET complete=0 WHERE path=X'6132'",
		"UPDATE entries SET generation=99 WHERE path=X'61322f7061636b6167652e6a736f6e'",
		"DELETE FROM entries WHERE path=X'61322f7061636b6167652e6a736f6e'",
	}
	for _, change := range changes {
		if _, err := s.db.Exec("SAVEPOINT preview"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(change); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PreviewFindings(ctx, []string{id}, 30); err != ErrFindingSelection {
			t.Fatal(change, err)
		}
		if _, err := s.db.Exec("ROLLBACK TO preview; RELEASE preview"); err != nil {
			t.Fatal(err)
		}
	}
	// Valid selections can retain unknown measurements; never upgrade evidence.
	if r.Findings[0].Measurement.Status != "partial" {
		t.Fatal(r.Findings[0].Measurement)
	}
}

func TestPreviewSelectionBound(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	ids := []string{}
	// Put a selected candidate beyond the ordinary first page.
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1100; i++ {
		if _, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'61','file',0,0,1,1,'','',2,10)`, []byte(fmt.Sprint("f", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ path, kind string }{{"a/node_modules", "directory"}, {"a/package.json", "file"}} {
		result, err := tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'61',?,0,0,1,1,'','',2,10)`, []byte(e.path), e.kind)
		if err != nil {
			t.Fatal(err)
		}
		if e.kind == "directory" {
			id, _ := result.LastInsertId()
			ids = append(ids, fmt.Sprintf("node-modules-v1:1:%d", id))
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	r, err := s.PreviewFindings(ctx, ids, 90)
	if err != nil || r.EntriesExamined != 1 || len(r.Findings) != 1 {
		t.Fatal(r, err)
	}
	if _, err = s.PreviewFindings(ctx, make([]string, 21), 90); err != ErrFindingSelection {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.PreviewFindings(canceled, ids, 90); err == nil {
		t.Fatal("cancellation ignored")
	}
}
