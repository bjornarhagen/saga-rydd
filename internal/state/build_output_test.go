package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Only generated saved metadata is used. No Cargo/project path exists or is
// opened, and no Rust toolchain is needed to exercise this recognition rule.
var buildOutputOld = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()

type buildOutputExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func buildOutputPut(t *testing.T, db buildOutputExecer, path, parent, kind, inode string, generation, size, allocated int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,?,?,?,1,'fixture-device',?,?,?)`, []byte(path), []byte(parent), kind, size, allocated, buildOutputOld, inode, generation, buildOutputOld); err != nil {
		t.Fatal(err)
	}
}

func buildOutputDirectory(t *testing.T, db buildOutputExecer, path string, generation int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,?,1,?)`, []byte(path), generation, buildOutputOld+int64(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func newBuildOutputFixture(t *testing.T) *Store {
	t.Helper()
	s, err := OpenWriter(context.Background(), privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.SyncRoots(context.Background(), []string{"/cargo-fixture"}); err != nil {
		t.Fatal(err)
	}
	buildOutputPut(t, s.db, ".", "", "directory", "root", 1, 0, 0)
	buildOutputDirectory(t, s.db, ".", 1)
	return s
}

func addBuildOutputProject(t *testing.T, s *Store, project string, profiles ...string) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	buildOutputPut(t, tx, project, filepath.Dir(project), "directory", project, 1, 0, 0)
	buildOutputDirectory(t, tx, project, 2)
	for _, manifest := range []string{"Cargo.toml", "Cargo.lock"} {
		buildOutputPut(t, tx, filepath.Join(project, manifest), project, "file", project+manifest, 2, 0, 0)
	}
	target := filepath.Join(project, "target")
	buildOutputPut(t, tx, target, project, "directory", target, 2, 0, 0)
	buildOutputDirectory(t, tx, target, 3)
	for _, profile := range profiles {
		p := filepath.Join(target, profile)
		buildOutputPut(t, tx, p, target, "directory", p, 3, 0, 0)
		buildOutputDirectory(t, tx, p, 4)
		buildOutputPut(t, tx, filepath.Join(p, ".cargo-lock"), p, "file", p+"lock", 4, 0, 0)
		for _, child := range []string{"deps", ".fingerprint"} {
			path := filepath.Join(p, child)
			buildOutputPut(t, tx, path, p, "directory", path, 4, 0, 0)
			buildOutputDirectory(t, tx, path, 5)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func buildOutputCount(r BuildOutputReport, code string) int {
	for _, d := range r.Diagnostics {
		if d.Code == code {
			return d.Count
		}
	}
	return 0
}

func assertBuildOutputQualified(t *testing.T, r BuildOutputReport) {
	t.Helper()
	if r.Contract != BuildOutputContract || r.Source != "saved_inventory" || r.ContentVerified || r.CurrentStateVerified || r.ApprovalAvailable || r.Executable || r.EstimatedReclaimableBytes != nil || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || r.EntryLimit != BuildOutputEntryLimit {
		t.Fatal("report gained authority", r)
	}
	total := 0
	for _, d := range r.Diagnostics {
		total += d.Count
	}
	if total != r.EntriesExamined || r.EntriesExamined > BuildOutputEntryLimit || len(r.Findings) > BuildOutputFindingLimit || buildOutputCount(r, "selected") != len(r.Findings) {
		t.Fatal("invalid bounded diagnostic accounting", r)
	}
	for _, f := range r.Findings {
		if f.Rule != BuildOutputRule || f.RuleVersion != 1 || f.Classification != "review_required" || f.Recognition != "saved_layout_metadata_only" || len(f.Actions) != 0 || f.ContentVerified || f.CurrentStateVerified || f.ApprovalAvailable || f.Executable || f.EstimatedReclaimableBytes != nil || f.Measurement.CurrentStateVerified || len(f.Markers) != 7 {
			t.Fatal("finding gained authority", f)
		}
	}
}

func TestCargoBuildOutputsRecognizedProfilesAndIndependentSizeEvidence(t *testing.T) {
	t.Run("release only", func(t *testing.T) {
		s := newBuildOutputFixture(t)
		addBuildOutputProject(t, s, "project", "release")
		r, err := s.CargoBuildOutputs(context.Background(), "", 90)
		if err != nil || len(r.Findings) != 1 || r.Findings[0].Profile != "release" {
			t.Fatal("release-only layout was missed", r, err)
		}
		assertBuildOutputQualified(t, r)
	})
	s := newBuildOutputFixture(t)
	addBuildOutputProject(t, s, "project", "debug", "release")
	p := "project/target/debug/deps"
	buildOutputPut(t, s.db, p+"/a", p, "file", "shared", 5, 100, 4096)
	buildOutputPut(t, s.db, p+"/b", p, "file", "shared", 5, 100, 4096)
	buildOutputPut(t, s.db, p+"/sparse", p, "file", "sparse", 5, 1000, 0)
	r, err := s.CargoBuildOutputs(context.Background(), "", 90)
	if err != nil || len(r.Findings) != 1 {
		t.Fatal(r, err)
	}
	assertBuildOutputQualified(t, r)
	f := r.Findings[0]
	if f.Profile != "debug" || f.Path != "/cargo-fixture/project/target" || f.ID != fmt.Sprintf("cargo-target-v1:1:%d", f.EntryID) || f.Measurement.Status != "recorded_complete" || f.Measurement.LogicalBytes == nil || *f.Measurement.LogicalBytes != 1200 || f.Measurement.AllocatedBytes == nil || *f.Measurement.AllocatedBytes != 4096 || f.Measurement.RepeatedInodes != 1 || f.Measurement.FilePaths != 5 {
		t.Fatal("known logical/hardlink/sparse oracle mismatch", f)
	}
	wantRoles := []string{"target", "manifest", "lockfile", "profile", "profile_lock", "dependencies", "fingerprints"}
	for i, m := range f.Markers {
		if m.Role != wantRoles[i] || m.Generation != m.ParentGeneration || m.ModifiedAt.UnixNano() != buildOutputOld || m.ObservedAt.UnixNano() != buildOutputOld || !bytes.Equal(m.PathBytes, []byte(m.Path)) {
			t.Fatal("marker evidence mismatch", m)
		}
	}
	// A recent debug marker must not hide an independently eligible release.
	if _, err = s.db.Exec("UPDATE entries SET mtime_ns=? WHERE path=?", time.Now().UnixNano(), []byte("project/target/debug/.cargo-lock")); err != nil {
		t.Fatal(err)
	}
	r, err = s.CargoBuildOutputs(context.Background(), "", 90)
	if err != nil || len(r.Findings) != 1 || r.Findings[0].Profile != "release" {
		t.Fatal("release fallback failed", r, err)
	}
	assertBuildOutputQualified(t, r)
}

func TestCargoBuildOutputsRequiredMarkerFailuresAndAge(t *testing.T) {
	for _, tc := range []struct{ name, sql, code string }{
		{"target kind", "UPDATE entries SET kind='file' WHERE path=X'70726f6a6563742f746172676574'", "not_directory"},
		{"missing manifest", "DELETE FROM entries WHERE path=X'70726f6a6563742f436172676f2e746f6d6c'", "manifest_missing_or_unsupported"},
		{"missing lock", "DELETE FROM entries WHERE path=X'70726f6a6563742f436172676f2e6c6f636b'", "manifest_missing_or_unsupported"},
		{"manifest symlink", "UPDATE entries SET kind='symlink' WHERE path=X'70726f6a6563742f436172676f2e746f6d6c'", "manifest_missing_or_unsupported"},
		{"profile lock symlink", "UPDATE entries SET kind='symlink' WHERE path=X'70726f6a6563742f7461726765742f64656275672f2e636172676f2d6c6f636b'", "profile_missing_or_unsupported"},
		{"wrong deps kind", "UPDATE entries SET kind='file' WHERE path=X'70726f6a6563742f7461726765742f64656275672f64657073'", "profile_missing_or_unsupported"},
		{"missing fingerprint", "DELETE FROM entries WHERE path=X'70726f6a6563742f7461726765742f64656275672f2e66696e6765727072696e74'", "profile_missing_or_unsupported"},
		{"excluded marker", "UPDATE entries SET skip_reason='excluded' WHERE path=X'70726f6a6563742f7461726765742f64656275672f64657073'", "skipped"},
		{"incomplete profile parent", "UPDATE directories SET complete=0 WHERE path=X'70726f6a6563742f7461726765742f6465627567'", "parent_incomplete_or_error"},
		{"missing project parent", "DELETE FROM directories WHERE path=X'70726f6a656374'", "parent_incomplete_or_error"},
		{"parent error before age", "UPDATE directories SET last_error='unavailable' WHERE path=X'70726f6a656374'; UPDATE entries SET mtime_ns=9223372036854775807", "parent_incomplete_or_error"},
		{"mixed marker generation", "UPDATE entries SET generation=99 WHERE path=X'70726f6a6563742f7461726765742f64656275672f2e66696e6765727072696e74'", "parent_unconfirmed"},
		{"wrong saved parent", "UPDATE entries SET parent=X'6f757473696465' WHERE path=X'70726f6a6563742f7461726765742f64656275672f64657073'", "unsupported_saved_path"},
		{"unknown marker date", "UPDATE entries SET mtime_ns=0 WHERE path=X'70726f6a6563742f7461726765742f64656275672f2e636172676f2d6c6f636b'", "timestamp_unknown"},
		{"future marker date", "UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=X'70726f6a6563742f7461726765742f64656275672f64657073'", "age_not_met"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newBuildOutputFixture(t)
			addBuildOutputProject(t, s, "project", "debug")
			if _, err := s.db.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			r, err := s.CargoBuildOutputs(context.Background(), "", 90)
			if err != nil || len(r.Findings) != 0 || buildOutputCount(r, tc.code) != 1 {
				t.Fatal(tc, r, err)
			}
			assertBuildOutputQualified(t, r)
		})
	}
	s := newBuildOutputFixture(t)
	addBuildOutputProject(t, s, "project", "debug")
	for _, path := range []string{"project/target", "project/Cargo.toml", "project/Cargo.lock", "project/target/debug", "project/target/debug/.cargo-lock", "project/target/debug/deps", "project/target/debug/.fingerprint"} {
		if _, err := s.db.Exec("UPDATE entries SET mtime_ns=? WHERE path=?", time.Now().Add(-60*24*time.Hour).UnixNano(), []byte(path)); err != nil {
			t.Fatal(err)
		}
		r, err := s.CargoBuildOutputs(context.Background(), "", 90)
		if err != nil || len(r.Findings) != 0 || buildOutputCount(r, "age_not_met") != 1 {
			t.Fatal(path, r, err)
		}
		r, err = s.CargoBuildOutputs(context.Background(), "", 30)
		if err != nil || len(r.Findings) != 1 || r.MinimumAgeDays != 30 {
			t.Fatal(path, r, err)
		}
		if _, err = s.db.Exec("UPDATE entries SET mtime_ns=? WHERE path=?", buildOutputOld, []byte(path)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCargoBuildOutputsHistoricalAncestorAndPartialUnknownQualifications(t *testing.T) {
	for _, tc := range []struct {
		name, change, status       string
		nullLogical, nullAllocated bool
	}{
		{"unconfirmed outer ancestor", "UPDATE directories SET generation=99 WHERE path=X'2e'", "unknown", true, true},
		{"incomplete descendant", "UPDATE directories SET complete=0 WHERE path=X'70726f6a6563742f7461726765742f64656275672f64657073'", "partial", false, false},
		{"root error", "UPDATE roots SET last_error='unavailable'", "stale", false, false},
		{"unknown content identity", "UPDATE entries SET inode='' WHERE path=X'70726f6a6563742f7461726765742f64656275672f646570732f64617461'", "recorded_complete", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newBuildOutputFixture(t)
			addBuildOutputProject(t, s, "project", "debug")
			buildOutputPut(t, s.db, "project/target/debug/deps/data", "project/target/debug/deps", "file", "data", 5, 12, 4096)
			if _, err := s.db.Exec(tc.change); err != nil {
				t.Fatal(err)
			}
			r, err := s.CargoBuildOutputs(context.Background(), "", 90)
			if err != nil || len(r.Findings) != 1 {
				t.Fatal(r, err)
			}
			assertBuildOutputQualified(t, r)
			m := r.Findings[0].Measurement
			if m.Status != tc.status || (m.LogicalBytes == nil) != tc.nullLogical || (m.AllocatedBytes == nil) != tc.nullAllocated {
				t.Fatal(tc, m)
			}
			if tc.name == "unknown content identity" && (m.UnknownInodes != 1 || !strings.Contains(strings.Join(m.Notes, " "), "counted per path")) {
				t.Fatal("unknown identities lost their existing per-path qualification", m)
			}
			if !strings.Contains(strings.Join(r.Notes, " "), "does not establish ancestor") {
				t.Fatal("ancestor limitation omitted", r.Notes)
			}
		})
	}
}

func TestCargoBuildOutputsNestedBoundariesRawPathsAndNearMisses(t *testing.T) {
	s := newBuildOutputFixture(t)
	// Names that only contain the boundary word remain separate projects.
	for _, project := range []string{"target-ish", "node_modules-ish", "quote\"雪\xff"} {
		addBuildOutputProject(t, s, project, "debug")
	}
	for _, project := range []string{"target/inner", "node_modules/inner"} {
		addBuildOutputProject(t, s, project, "debug")
	}
	addBuildOutputProject(t, s, "custom", "debug")
	if _, err := s.db.Exec("UPDATE entries SET path=CAST(REPLACE(CAST(path AS TEXT),'custom/target','custom/dist') AS BLOB),parent=CAST(REPLACE(CAST(parent AS TEXT),'custom/target','custom/dist') AS BLOB) WHERE substr(path,1,13)=X'637573746f6d2f746172676574'"); err != nil {
		t.Fatal(err)
	}
	r, err := s.CargoBuildOutputs(context.Background(), "", 90)
	if err != nil || len(r.Findings) != 3 || buildOutputCount(r, "nested_output") != 2 {
		t.Fatal("boundary or custom-layout recognition changed", r, err)
	}
	assertBuildOutputQualified(t, r)
	want := []byte("/cargo-fixture/quote\"雪\xff/target")
	if !bytes.Equal(r.Findings[2].PathBytes, want) {
		t.Fatal("raw target path changed", r.Findings[2])
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded BuildOutputReport
	if err = json.Unmarshal(encoded, &decoded); err != nil || !bytes.Equal(decoded.Findings[2].PathBytes, want) || !bytes.Equal(decoded.Findings[2].Markers[1].PathBytes, []byte("/cargo-fixture/quote\"雪\xff/Cargo.toml")) {
		t.Fatal("raw evidence lost through JSON", err)
	}
	// Mutating a returned path cannot redirect a later saved-only report.
	r.Findings[2].Markers[0].PathBytes[0] = 'x'
	again, err := s.CargoBuildOutputs(context.Background(), "", 90)
	if err != nil || !bytes.Equal(again.Findings[2].Markers[0].PathBytes, want) {
		t.Fatal(again, err)
	}
}

func TestCargoBuildOutputsRawAndFindingPaginationAndCancellation(t *testing.T) {
	s := newBuildOutputFixture(t)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < BuildOutputEntryLimit+1; i++ {
		buildOutputPut(t, tx, fmt.Sprintf("filler%04d", i), ".", "file", fmt.Sprint(i), 1, 0, 0)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	addBuildOutputProject(t, s, "last", "debug")
	r, err := s.CargoBuildOutputs(context.Background(), "", 30)
	if err != nil || r.EntriesExamined != BuildOutputEntryLimit || len(r.Findings) != 0 || r.NextCursor == "" || r.PageCoverage != "more_saved_entries" {
		t.Fatal(r, err)
	}
	assertBuildOutputQualified(t, r)
	next, err := s.CargoBuildOutputs(context.Background(), r.NextCursor, 30)
	if err != nil || len(next.Findings) != 1 || next.NextCursor != "" {
		t.Fatal(next, err)
	}
	for _, cursor := range []string{"bad", "cargo1:30:0", "cargo1:30:-1", "cargo1:30:01", "cargo1:30:999999999999999999999", r.NextCursor} {
		days := 30
		if cursor == r.NextCursor {
			days = 90
		}
		if got, e := s.CargoBuildOutputs(context.Background(), cursor, days); !errors.Is(e, ErrReportCursor) || len(got.Findings) != 0 {
			t.Fatal(cursor, got, e)
		}
	}
	for _, days := range []int{0, -1, MaxFindingAgeDays + 1} {
		if _, e := s.CargoBuildOutputs(context.Background(), "", days); !errors.Is(e, ErrFindingAge) {
			t.Fatal(days, e)
		}
	}
	capFixture := newBuildOutputFixture(t)
	for i := 0; i < BuildOutputFindingLimit+1; i++ {
		addBuildOutputProject(t, capFixture, fmt.Sprintf("p%02d", i), "debug")
	}
	first, err := capFixture.CargoBuildOutputs(context.Background(), "", 90)
	if err != nil || len(first.Findings) != 20 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	last, err := capFixture.CargoBuildOutputs(context.Background(), first.NextCursor, 90)
	if err != nil || len(last.Findings) != 1 || last.NextCursor != "" || last.Findings[0].ID == first.Findings[19].ID {
		t.Fatal(last, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, e := capFixture.CargoBuildOutputs(canceled, "", 90); !errors.Is(e, context.Canceled) || !reflect.DeepEqual(got, BuildOutputReport{}) {
		t.Fatal("early cancellation returned a report", got, e)
	}
	late, cancel := context.WithCancel(context.Background())
	if got, e := capFixture.cargoBuildOutputs(late, "", 90, buildOutputHooks{beforeCommit: cancel}); !errors.Is(e, context.Canceled) || !reflect.DeepEqual(got, BuildOutputReport{}) {
		t.Fatal("late cancellation published a partial report", got, e)
	}
}
