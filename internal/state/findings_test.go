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
	r, err := s.NodeModulesFindings(ctx, "", FindingAgeDays)
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
	again, err := s.NodeModulesFindings(ctx, "", FindingAgeDays)
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
		got, err := s.NodeModulesFindings(ctx, "", FindingAgeDays)
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
	r, err := s.NodeModulesFindings(ctx, "", FindingAgeDays)
	if err != nil || r.EntriesExamined != FindingEntryLimit || r.PageCoverage != "more_saved_entries" || r.NextCursor == "" || len(r.Findings) != 0 {
		t.Fatal(r, err)
	}
	next, err := s.NodeModulesFindings(ctx, r.NextCursor, FindingAgeDays)
	if err != nil || next.NextCursor == r.NextCursor || len(next.Findings) != 0 {
		t.Fatal(next, err)
	}
	for _, token := range []string{"bad", "nm1:-1", "nm1:0", "nm1:99999999999999999999999"} {
		if _, err = s.NodeModulesFindings(ctx, token, FindingAgeDays); err != ErrReportCursor {
			t.Fatal(token, err)
		}
	}
	custom, err := s.NodeModulesFindings(ctx, "", 30)
	if err != nil || custom.NextCursor == "" {
		t.Fatal(custom, err)
	}
	continued, err := s.NodeModulesFindings(ctx, custom.NextCursor, 30)
	if err != nil || continued.NextCursor == custom.NextCursor || continued.MinimumAgeDays != 30 {
		t.Fatal(continued, err)
	}
	for _, tc := range []struct {
		token string
		days  int
	}{{custom.NextCursor, 90}, {custom.NextCursor, 31}, {r.NextCursor, 30}, {"nm2:30:-1", 30}, {"nm2:30:0", 30}, {"nm2:30:1:2", 30}, {"nm2:30:99999999999999999999999", 30}} {
		if _, err := s.NodeModulesFindings(ctx, tc.token, tc.days); err != ErrReportCursor {
			t.Fatal(tc, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.NodeModulesFindings(canceled, "", FindingAgeDays); err == nil {
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
	r, err := s.NodeModulesFindings(ctx, "", FindingAgeDays)
	if err != nil || len(r.Findings) != 20 || r.PageCoverage != "more_saved_entries" || r.NextCursor == "" {
		t.Fatal(r, err)
	}
	next, err := s.NodeModulesFindings(ctx, r.NextCursor, FindingAgeDays)
	if err != nil || len(next.Findings) != 1 || next.PageCoverage != "saved_entries_exhausted" || next.NextCursor != "" {
		t.Fatal(next, err)
	}
	ids := []string{}
	for _, f := range r.Findings {
		ids = append(ids, f.ID)
	}
	preview, err := s.PreviewFindings(ctx, ids, FindingAgeDays)
	if err != nil || len(preview.Findings) != 20 || preview.EntriesExamined != 20 || preview.NextCursor != "" {
		t.Fatal(preview, err)
	}
	if _, err = s.PreviewFindings(ctx, append(ids, next.Findings[0].ID), FindingAgeDays); err != ErrFindingSelection {
		t.Fatal(err)
	}
	if r.Findings[19].ID == next.Findings[0].ID {
		t.Fatal("candidate repeated across pages")
	}
	if next.Findings[0].Measurement.Status != "recorded_complete" || *next.Findings[0].Measurement.LogicalBytes != 0 {
		t.Fatal(next)
	}
}

func TestSelectionDiagnosticReasons(t *testing.T) {
	cases := []struct{ name, change, code string }{
		{"selected", "", "selected"},
		{"kind", "UPDATE entries SET kind='symlink' WHERE path=X'612f6e6f64655f6d6f64756c6573'", "not_directory"},
		{"skip", "UPDATE entries SET skip_reason='excluded' WHERE path=X'612f6e6f64655f6d6f64756c6573'", "skipped"},
		{"manifest skip", "UPDATE entries SET skip_reason='excluded' WHERE path=X'612f7061636b6167652e6a736f6e'", "skipped"},
		{"missing manifest", "DELETE FROM entries WHERE path=X'612f7061636b6167652e6a736f6e'", "manifest_missing_or_unsupported"},
		{"unsupported manifest", "UPDATE entries SET kind='symlink' WHERE path=X'612f7061636b6167652e6a736f6e'", "manifest_missing_or_unsupported"},
		{"incomplete before age", "UPDATE directories SET complete=0 WHERE path=X'61'; UPDATE entries SET mtime_ns=9223372036854775807", "parent_incomplete_or_error"},
		{"missing parent", "DELETE FROM directories WHERE path=X'61'", "parent_incomplete_or_error"},
		{"parent error", "UPDATE directories SET last_error='unavailable' WHERE path=X'61'", "parent_incomplete_or_error"},
		{"generation", "UPDATE entries SET generation=99 WHERE path=X'612f7061636b6167652e6a736f6e'", "parent_unconfirmed"},
		{"unknown timestamp", "UPDATE entries SET mtime_ns=0 WHERE path=X'612f7061636b6167652e6a736f6e'", "timestamp_unknown"},
		{"recent directory", "UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=X'612f6e6f64655f6d6f64756c6573'", "age_not_met"},
		{"recent manifest", "UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=X'612f7061636b6167652e6a736f6e'", "age_not_met"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := directoryFixture(t)
			for _, e := range []struct{ path, kind string }{{"a/node_modules", "directory"}, {"a/package.json", "file"}} {
				_, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'61',?,0,0,1,1,'','',2,10)`, []byte(e.path), e.kind)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.change != "" {
				if _, err := s.db.Exec(tc.change); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.NodeModulesFindings(context.Background(), "", FindingAgeDays)
			if err != nil {
				t.Fatal(err)
			}
			total := 0
			matched := false
			for _, d := range r.Diagnostics {
				total += d.Count
				if d.Code == tc.code && d.Count == 1 {
					matched = true
				}
			}
			if !matched || total != r.EntriesExamined || r.PageCoverage != "saved_entries_exhausted" {
				t.Fatal(r)
			}
			if (tc.code == "selected") != (len(r.Findings) == 1) {
				t.Fatal(r)
			}
		})
	}
}

func TestFindingAgeOverride(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	old := time.Now().Add(-60 * 24 * time.Hour).UnixNano()
	for _, e := range []struct{ path, kind string }{{"a/node_modules", "directory"}, {"a/package.json", "file"}} {
		if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'61',?,0,0,?,1,'','',2,10)`, []byte(e.path), e.kind, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ days, want int }{{90, 0}, {30, 1}, {1, 1}, {MaxFindingAgeDays, 0}, {90, 0}} {
		r, err := s.NodeModulesFindings(ctx, "", tc.days)
		if err != nil || r.MinimumAgeDays != tc.days || len(r.Findings) != tc.want {
			t.Fatal(tc, r, err)
		}
		for _, f := range r.Findings {
			if f.Classification != "review_required" || len(f.Actions) != 0 || r.CurrentStateVerified {
				t.Fatal(f)
			}
		}
	}
	for _, days := range []int{-1, 0, MaxFindingAgeDays + 1, int(^uint(0) >> 1)} {
		if _, err := s.NodeModulesFindings(ctx, "", days); err != ErrFindingAge {
			t.Fatal(days, err)
		}
	}
	// Either timestamp must meet the override; unknown/future evidence is never eligible.
	for _, path := range []string{"a/node_modules", "a/package.json"} {
		for _, mt := range []int64{0, time.Now().Add(-29 * 24 * time.Hour).UnixNano(), time.Now().Add(24 * time.Hour).UnixNano()} {
			if _, err := s.db.Exec(`UPDATE entries SET mtime_ns=? WHERE path=?`, mt, []byte(path)); err != nil {
				t.Fatal(err)
			}
			r, err := s.NodeModulesFindings(ctx, "", 30)
			if err != nil || len(r.Findings) != 0 {
				t.Fatal(path, mt, r, err)
			}
		}
		if _, err := s.db.Exec(`UPDATE entries SET mtime_ns=? WHERE path=?`, old, []byte(path)); err != nil {
			t.Fatal(err)
		}
	}
}
