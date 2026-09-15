package state

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestNodeModulesFindings(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	put := func(path, parent, kind string, mtime int64) {
		t.Helper()
		_, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,12,4096,?,1,'dev',?,2,10)`, []byte(path), []byte(parent), kind, mtime, path)
		if err != nil {
			t.Fatal(err)
		}
	}
	put("a/package.json", "a", "file", 1)
	put("a/node_modules", "a", "directory", 1)
	put("a/node_modules/edit.txt", "a/node_modules", "file", time.Now().UnixNano())
	// Fresh inner edits do not change the deliberately limited selection rule.
	// Its evidence must explicitly disclose that modifications were not inspected.
	r, err := s.NodeModulesFindings(ctx, "")
	if err != nil || len(r.Findings) != 1 {
		t.Fatal(r, err)
	}
	f := r.Findings[0]
	if f.Classification != "review_required" || f.Recognition != "manifest_filename_only" || len(f.Actions) != 0 || f.Measurement.Status != "stale" || f.Measurement.LogicalBytes == nil || *f.Measurement.LogicalBytes != 12 || r.CurrentStateVerified || f.RuleVersion != 1 {
		t.Fatalf("%+v", f)
	}
	if !strings.Contains(strings.Join(r.Notes, " "), "dependency modifications have not been inspected") {
		t.Fatal(r.Notes)
	}
	again, err := s.NodeModulesFindings(ctx, "")
	if err != nil || again.Findings[0].ID != f.ID {
		t.Fatal(again, err)
	}
	for _, change := range []string{
		"UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=X'612f7061636b6167652e6a736f6e'",
		"UPDATE entries SET kind='symlink' WHERE path=X'612f7061636b6167652e6a736f6e'",
		"UPDATE entries SET generation=99 WHERE path=X'612f7061636b6167652e6a736f6e'",
		"UPDATE entries SET skip_reason='excluded' WHERE path=X'612f6e6f64655f6d6f64756c6573'",
		"UPDATE roots SET enabled=0",
	} {
		// Roll back each mutation after checking independently.
		_, err = s.db.Exec("SAVEPOINT fixture")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec(change); err != nil {
			t.Fatal(err)
		}
		got, err := s.NodeModulesFindings(ctx, "")
		if err != nil || len(got.Findings) != 0 {
			t.Fatal(change, got, err)
		}
		if _, err = s.db.Exec("ROLLBACK TO fixture"); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("RELEASE fixture"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFindingPagesAndNestedSuppression(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < FindingEntryLimit+1; i++ {
		p := fmt.Sprintf("a/node_modules/pkg%d", i)
		for _, e := range []struct{ path, parent, kind string }{{p, "a/node_modules", "directory"}, {p + "/package.json", p, "file"}, {p + "/node_modules", p, "directory"}} {
			_, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,0,0,1,1,'','',1,10)`, []byte(e.path), []byte(e.parent), e.kind)
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err = tx.Exec(`INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,20)`, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	r, err := s.NodeModulesFindings(ctx, "")
	if err != nil || r.EntriesExamined != FindingEntryLimit || r.NextCursor == "" || len(r.Findings) != 0 {
		t.Fatal(r, err)
	}
	next, err := s.NodeModulesFindings(ctx, r.NextCursor)
	if err != nil || next.NextCursor == r.NextCursor || len(next.Findings) != 0 {
		t.Fatal(next, err)
	}
	for _, token := range []string{"bad", "nm1:-1", "nm1:0", "nm1:99999999999999999999999"} {
		if _, err = s.NodeModulesFindings(ctx, token); err != ErrReportCursor {
			t.Fatal(token, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.NodeModulesFindings(canceled, ""); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestFindingCandidateCap(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	for i := 0; i < 21; i++ {
		p := fmt.Sprintf("project%d", i)
		for _, e := range []struct{ path, parent, kind string }{{p, ".", "directory"}, {p + "/package.json", p, "file"}, {p + "/node_modules", p, "directory"}} {
			_, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,0,0,1,1,'','',1,10)`, []byte(e.path), []byte(e.parent), e.kind)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, path := range []string{p, p + "/node_modules"} {
			if _, err := s.db.Exec(`INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,20)`, []byte(path)); err != nil {
				t.Fatal(err)
			}
		}
	}
	r, err := s.NodeModulesFindings(ctx, "")
	if err != nil || len(r.Findings) != 20 || r.NextCursor == "" {
		t.Fatal(r, err)
	}
	next, err := s.NodeModulesFindings(ctx, r.NextCursor)
	if err != nil || len(next.Findings) != 1 || next.NextCursor != "" {
		t.Fatal(next, err)
	}
	if r.Findings[19].ID == next.Findings[0].ID {
		t.Fatal("candidate repeated across pages")
	}
	if next.Findings[0].Measurement.Status != "recorded_complete" || *next.Findings[0].Measurement.LogicalBytes != 0 {
		t.Fatal(next)
	}
}
