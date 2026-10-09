package state

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Fixtures contain generated saved metadata only. No installed cache, Go
// environment, README body or source file is consulted by report functions.
const goCacheFixtureRoot = "/generated-go-cache"

func goCachePut(t *testing.T, db buildOutputExecer, rootID int64, path, parent, kind, inode string, generation, size, allocated int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(?,?,?,?,?,?,?,11,'7',?,?,?)`, rootID, []byte(path), []byte(parent), kind, size, allocated, buildOutputOld, inode, generation, buildOutputOld); err != nil {
		t.Fatal(err)
	}
}

func goCacheDir(t *testing.T, db buildOutputExecer, rootID int64, path string, generation int64) {
	t.Helper()
	if _, err := db.Exec("INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(?,?,?,1,?)", rootID, []byte(path), generation, buildOutputOld+int64(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func goCacheLayoutFixture(t *testing.T, s *Store, rootID int64) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	goCachePut(t, tx, rootID, ".", "", "directory", "1", 1, 0, 0)
	goCacheDir(t, tx, rootID, ".", 1)
	goCachePut(t, tx, rootID, "README", ".", "file", "2", 1, 139, 4096)
	for i := 0; i < 256; i++ {
		path := fmt.Sprintf("%02x", i)
		goCachePut(t, tx, rootID, path, ".", "directory", fmt.Sprint(i+10), 1, 0, 0)
		goCacheDir(t, tx, rootID, path, 2)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func newGoCacheFixture(t *testing.T, root string) *Store {
	t.Helper()
	s, err := OpenWriter(context.Background(), privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.SyncRoots(context.Background(), []string{root}); err != nil {
		t.Fatal(err)
	}
	goCacheLayoutFixture(t, s, 1)
	return s
}

func goCacheName(index int, suffix byte) string {
	hash := fmt.Sprintf("%064x", index)
	return hash[:2] + "/" + hash + "-" + string(suffix)
}

func goCacheAdd(t *testing.T, s *Store, index int, suffix byte, inode string, size, allocated int64) string {
	t.Helper()
	path := goCacheName(index, suffix)
	goCachePut(t, s.db, 1, path, filepath.Dir(path), "file", inode, 2, size, allocated)
	return path
}

func goCacheDiagnostic(r GoCacheReport, code string) int {
	for _, d := range r.Diagnostics {
		if d.Code == code {
			return d.Count
		}
	}
	return 0
}

func assertGoCacheQualified(t *testing.T, r GoCacheReport) {
	t.Helper()
	if r.Contract != GoCacheContract || r.Rule != GoCacheRule || r.Source != "saved_inventory" || r.ContentVerified || r.CurrentStateVerified || r.RegenerationVerified || r.ApprovalAvailable || r.AutomationEligible || r.Executable || r.EstimatedReclaimableBytes != nil || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || r.EntryLimit != GoCacheEntryLimit {
		t.Fatal("report gained authority", r)
	}
	count := 0
	for _, d := range r.Diagnostics {
		count += d.Count
	}
	if count != r.EntriesExamined || r.EntriesExamined > GoCacheEntryLimit || len(r.Files) > GoCacheFindingLimit || goCacheDiagnostic(r, "selected") != len(r.Files) {
		t.Fatal("unbounded or inconsistent raw page", r)
	}
	for _, f := range r.Files {
		if f.ContentVerified || f.CurrentStateVerified || f.RegenerationVerified || f.ApprovalAvailable || f.AutomationEligible || f.Executable || f.EstimatedReclaimableBytes != nil || f.Classification != "review_required" || !bytes.Equal(f.PathBytes, []byte(f.Path)) || f.ID != fmt.Sprintf("go-cache-file-v1:%d:%d", f.RootID, f.EntryID) {
			t.Fatal("file gained authority or lost byte evidence", f)
		}
	}
}

func TestGoBuildCacheLayoutAndIndependentFileMetadata(t *testing.T) {
	s := newGoCacheFixture(t, goCacheFixtureRoot)
	goCacheAdd(t, s, 1, 'a', "700", 175, 4096)
	goCacheAdd(t, s, 2, 'd', "701", 1<<30, 0) // Independent saved sparse-size oracle.
	// Marker dates do not control entry age: a cache's root and shard can
	// change today while an individual recorded file remains old.
	if _, err := s.db.Exec("UPDATE entries SET mtime_ns=? WHERE kind='directory' OR path=X'524541444d45'", time.Now().Add(24*time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || r.Layout.Status != "layout_metadata_recognized" || r.Layout.RequiredShards != 256 || r.Layout.ConfirmedShards != 256 || len(r.Layout.Markers) != 258 || len(r.Files) != 2 || r.RootID != 1 || r.RootPath != goCacheFixtureRoot {
		t.Fatal(r, err)
	}
	if r.Files[0].EntryType != "action_index" || r.Files[1].EntryType != "data" || r.Files[0].LogicalBytes == nil || *r.Files[0].LogicalBytes != 175 || r.Files[0].AllocatedBytes == nil || *r.Files[0].AllocatedBytes != 4096 || r.Files[1].LogicalBytes == nil || *r.Files[1].LogicalBytes != 1<<30 || r.Files[1].AllocatedBytes == nil || *r.Files[1].AllocatedBytes != 0 || r.KnownObjects != 2 || r.UnknownIdentities != 0 || r.RepeatedSavedObjects != 0 {
		t.Fatal("independent file/allocation oracle differs", r)
	}
	for i, m := range r.Layout.Markers {
		if !bytes.Equal(m.RelativePathBytes, []byte(m.RelativePath)) || m.Generation != m.ParentGeneration {
			t.Fatal("marker evidence mismatch", m)
		}
		if i == 0 && (m.Role != "root" || m.RelativePath != "." || !m.ListingComplete) || i == 1 && (m.Role != "readme" || m.RelativePath != "README" || m.Kind != "file") || i >= 2 && (m.RelativePath != fmt.Sprintf("%02x", i-2) || m.Role != "shard" || !m.ListingComplete || m.ListingGeneration != 2) {
			t.Fatal("fixed layout evidence mismatch", i, m)
		}
	}
	assertGoCacheQualified(t, r)
	// Returned paths and size pointers cannot affect another saved view.
	r.Files[0].PathBytes[0] = 'x'
	r.Files[0].RelativePathBytes[0] = 'x'
	*r.Files[0].LogicalBytes = 0
	r.Layout.Markers[0].RelativePathBytes[0] = 'x'
	r.RootPathBytes[0] = 'x'
	next, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || next.RootPathBytes[0] != '/' || next.Files[0].PathBytes[0] != '/' || next.Files[0].RelativePathBytes[0] != '0' || *next.Files[0].LogicalBytes != 175 || string(next.Layout.Markers[0].RelativePathBytes) != "." {
		t.Fatal("caller mutated stored view", next, err)
	}
	assertGoCacheQualified(t, next)
}

func TestGoBuildCacheUnsupportedAndIncompleteLayouts(t *testing.T) {
	for _, tc := range []struct{ name, sql, code string }{
		{"root kind", "UPDATE entries SET kind='file' WHERE path=X'2e'", "marker_kind_or_parent"},
		{"readme missing", "DELETE FROM entries WHERE path=X'524541444d45'", "marker_missing"},
		{"readme symlink", "UPDATE entries SET kind='symlink' WHERE path=X'524541444d45'", "marker_kind_or_parent"},
		{"readme skipped", "UPDATE entries SET skip_reason='excluded' WHERE path=X'524541444d45'", "marker_skipped"},
		{"shard missing", "DELETE FROM entries WHERE path=X'6666'", "marker_missing"},
		{"shard symlink", "UPDATE entries SET kind='symlink' WHERE path=X'6666'", "marker_kind_or_parent"},
		{"shard skipped", "UPDATE entries SET skip_reason='excluded' WHERE path=X'6666'", "marker_skipped"},
		{"shard wrong parent", "UPDATE entries SET parent=X'6666' WHERE path=X'3030'", "marker_kind_or_parent"},
		{"root listing incomplete", "UPDATE directories SET complete=0 WHERE path=X'2e'", "parent_incomplete_or_error"},
		{"root listing error", "UPDATE directories SET last_error='fixture' WHERE path=X'2e'", "parent_incomplete_or_error"},
		{"readme generation", "UPDATE entries SET generation=9 WHERE path=X'524541444d45'", "parent_unconfirmed"},
		{"root generation", "UPDATE entries SET generation=9 WHERE path=X'2e'", "parent_unconfirmed"},
		{"shard listing incomplete", "UPDATE directories SET complete=0 WHERE path=X'6666'", "listing_incomplete_or_error"},
		{"shard listing missing", "DELETE FROM directories WHERE path=X'6666'", "listing_incomplete_or_error"},
		{"shard listing error", "UPDATE directories SET last_error='fixture' WHERE path=X'6666'", "listing_incomplete_or_error"},
		{"root error", "UPDATE roots SET last_error='fixture' WHERE id=1", "root_error"},
		{"disabled root", "UPDATE roots SET enabled=0 WHERE id=1", "root_not_saved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newGoCacheFixture(t, goCacheFixtureRoot)
			goCacheAdd(t, s, 1, 'd', "700", 1, 0)
			if _, err := s.db.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
			if err != nil || r.Layout.Status != "layout_unsupported" || r.Layout.Code != tc.code || len(r.Files) != 0 || r.NextCursor != "" || r.EntriesExamined != 0 || r.PageCoverage != "layout_not_recognized" {
				t.Fatal("unsupported layout implied complete empty cache", r, err)
			}
			assertGoCacheQualified(t, r)
		})
	}
	s := newGoCacheFixture(t, goCacheFixtureRoot)
	r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot+"-near-miss", "", 90)
	if err != nil || r.Layout.Code != "root_not_saved" || r.RootID != 0 {
		t.Fatal("root lookup used prefix/subtree", r, err)
	}
	for _, root := range []string{"", "relative", goCacheFixtureRoot + "/../generated-go-cache", goCacheFixtureRoot + "\x00bad", "/" + strings.Repeat("x", 4096)} {
		if r, err := s.GoBuildCache(context.Background(), root, "", 90); !errors.Is(err, ErrGoCacheRoot) || !reflect.DeepEqual(r, GoCacheReport{}) {
			t.Fatal(root, r, err)
		}
	}
}

func TestGoBuildCacheExactSupportedShapesAndAge(t *testing.T) {
	for _, tc := range []struct {
		name, path, kind, parent, skip, code string
		gen, mtime                           int64
	}{
		{"ordinary", goCacheName(1, 'a'), "file", "00", "", "selected", 2, buildOutputOld},
		{"wrong shard", "01/" + strings.Repeat("0", 64) + "-a", "file", "01", "", "outside_supported_layout", 2, buildOutputOld},
		{"uppercase", "00/" + strings.Repeat("0", 63) + "A-a", "file", "00", "", "outside_supported_layout", 2, buildOutputOld},
		{"nonhex", "00/" + strings.Repeat("0", 63) + "g-d", "file", "00", "", "outside_supported_layout", 2, buildOutputOld},
		{"bad suffix", goCacheName(1, 'x'), "file", "00", "", "outside_supported_layout", 2, buildOutputOld},
		{"wrong depth", "00/inner/" + strings.Repeat("0", 64) + "-d", "file", "00/inner", "", "outside_supported_layout", 2, buildOutputOld},
		{"fuzz", "fuzz/" + strings.Repeat("0", 64) + "-d", "file", "fuzz", "", "outside_supported_layout", 2, buildOutputOld},
		{"module", "pkg/mod/cache/download/x", "file", "pkg/mod/cache/download", "", "outside_supported_layout", 2, buildOutputOld},
		{"raw unsupported filename", "00/\xff", "file", "00", "", "outside_supported_layout", 2, buildOutputOld},
		{"auxiliary", "trim.txt", "file", ".", "", "outside_supported_layout", 1, buildOutputOld},
		{"executable directory", goCacheName(1, 'd'), "directory", "00", "", "executable_directory", 2, buildOutputOld},
		{"a directory", goCacheName(1, 'a'), "directory", "00", "", "not_regular_file", 2, buildOutputOld},
		{"symlink", goCacheName(1, 'd'), "symlink", "00", "", "not_regular_file", 2, buildOutputOld},
		{"special", goCacheName(1, 'a'), "other", "00", "", "not_regular_file", 2, buildOutputOld},
		{"excluded", goCacheName(1, 'a'), "file", "00", "excluded", "skipped", 2, buildOutputOld},
		{"wrong parent", goCacheName(1, 'a'), "file", "01", "", "unsupported_saved_path", 2, buildOutputOld},
		{"mixed generation", goCacheName(1, 'a'), "file", "00", "", "parent_unconfirmed", 3, buildOutputOld},
		{"unknown mtime", goCacheName(1, 'a'), "file", "00", "", "timestamp_unknown", 2, 0},
		{"recent", goCacheName(1, 'a'), "file", "00", "", "age_not_met", 2, time.Now().Add(-30 * 24 * time.Hour).UnixNano()},
		{"future", goCacheName(1, 'a'), "file", "00", "", "age_not_met", 2, time.Now().Add(24 * time.Hour).UnixNano()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newGoCacheFixture(t, goCacheFixtureRoot)
			goCachePut(t, s.db, 1, tc.path, tc.parent, tc.kind, "700", tc.gen, 100, 0)
			if _, err := s.db.Exec("UPDATE entries SET mtime_ns=?,skip_reason=? WHERE path=?", tc.mtime, tc.skip, []byte(tc.path)); err != nil {
				t.Fatal(err)
			}
			r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
			if err != nil || goCacheDiagnostic(r, tc.code) == 0 || len(r.Files) != map[bool]int{true: 1, false: 0}[tc.code == "selected"] {
				t.Fatal("shape/age recognition differs", r, err)
			}
			assertGoCacheQualified(t, r)
		})
	}
	// Directory entry generation belongs to its parent's pass, while selected
	// children belong to the shard's own pass. Root pass freshness cannot replace it.
	s := newGoCacheFixture(t, goCacheFixtureRoot)
	path := goCacheAdd(t, s, 1, 'd', "700", 1, 0)
	if _, err := s.db.Exec("UPDATE directories SET generation=3 WHERE path=X'3030'"); err != nil {
		t.Fatal(err)
	}
	r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || r.Layout.Status != "layout_metadata_recognized" || len(r.Files) != 0 || goCacheDiagnostic(r, "parent_unconfirmed") != 1 {
		t.Fatal("child inherited root generation", r, err)
	}
	if _, err = s.db.Exec("UPDATE entries SET mtime_ns=? WHERE path=?", time.Now().Add(-60*24*time.Hour).UnixNano(), []byte(path)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE directories SET generation=2 WHERE path=X'3030'"); err != nil {
		t.Fatal(err)
	}
	r, err = s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 30)
	if err != nil || len(r.Files) != 1 {
		t.Fatal("lower stated age missed saved file", r, err)
	}
	assertGoCacheQualified(t, r)
}

func TestGoBuildCachePageLocalAliasConflictUnknownAndRawPaths(t *testing.T) {
	root := goCacheFixtureRoot + "/quote\"雪\xff"
	s := newGoCacheFixture(t, root)
	goCacheAdd(t, s, 1, 'd', "700", 100, 4096)
	goCacheAdd(t, s, 2, 'd', "700", 100, 4096)
	goCacheAdd(t, s, 3, 'd', "701", 100, 4096)
	conflict := goCacheAdd(t, s, 4, 'd', "701", 100, 8192)
	goCacheAdd(t, s, 5, 'a', "bad", 100, 0)
	goCacheAdd(t, s, 6, 'a', "0", 100, 0)
	if _, err := s.db.Exec("UPDATE entries SET ctime_ns=12 WHERE path=?", []byte(conflict)); err != nil {
		t.Fatal(err)
	}
	r, err := s.GoBuildCache(context.Background(), root, "", 90)
	if err != nil || len(r.Files) != 6 || r.KnownObjects != 2 || r.RepeatedSavedObjects != 2 || r.ConflictingIdentities != 1 || r.UnknownIdentities != 2 {
		t.Fatal("page identity oracle differs", r, err)
	}
	for i, want := range []string{"alias", "alias", "conflicting", "conflicting", "unknown", "unknown"} {
		if r.Files[i].IdentityStatus != want {
			t.Fatal(i, r.Files[i])
		}
	}
	assertGoCacheQualified(t, r)
	blob, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GoCacheReport
	if err = json.Unmarshal(blob, &decoded); err != nil || !bytes.Equal(decoded.RootPathBytes, []byte(root)) || !bytes.Equal(decoded.Files[0].PathBytes, r.Files[0].PathBytes) {
		t.Fatal("raw path JSON lost authoritative bytes", err)
	}
	// The database schema normally guarantees nonnegative metadata. Keep
	// malformed unknown values qualified as null rather than inventing zero.
	if _, err = s.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE entries SET size=-1,allocated=-1 WHERE path=?", []byte(goCacheName(5, 'a'))); err != nil {
		t.Fatal(err)
	}
	r, err = s.GoBuildCache(context.Background(), root, "", 90)
	if err != nil || r.Files[4].LogicalBytes != nil || r.Files[4].AllocatedBytes != nil || r.Files[4].IdentityStatus != "unknown" {
		t.Fatal("unknown metadata became zero", r, err)
	}
	assertGoCacheQualified(t, r)
}

func TestGoBuildCacheRawPaginationBoundsQueryPlanAndCancellation(t *testing.T) {
	s := newGoCacheFixture(t, goCacheFixtureRoot)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1001; i++ {
		goCachePut(t, tx, 1, fmt.Sprintf("outside%d", i), ".", "file", fmt.Sprint(1000+i), 1, 1, 0)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	goCacheAdd(t, s, 1, 'd', "800", 1, 0)
	r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || r.EntriesExamined != 1000 || len(r.Files) != 0 || r.NextCursor == "" || r.PageCoverage != "more_saved_entries" {
		t.Fatal("empty raw page lost bound/continuation", r, err)
	}
	assertGoCacheQualified(t, r)
	next, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, r.NextCursor, 90)
	if err != nil || len(next.Files) != 1 || next.NextCursor != "" || next.EntriesExamined != 260 {
		t.Fatal("raw page skipped/repeated boundary", next, err)
	}
	assertGoCacheQualified(t, next)
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+goCachePageQuery, 0, GoCacheEntryLimit+1)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "MATERIALIZE selected") || !strings.Contains(plan, "SEARCH entries USING INTEGER PRIMARY KEY (rowid>?)") || !strings.Contains(plan, "SEARCH p USING") {
		t.Fatal("bundled SQLite did not retain rowid fence/indexed parent joins", plan)
	}
	for _, token := range []string{"cargo1:90:1", "gocache1:30:1", "gocache1:90:0", "gocache1:90:01", "gocache1:90:+1", "gocache1:90:1:2", "gocache1:90:9223372036854775808"} {
		if r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, token, 90); !errors.Is(err, ErrReportCursor) || !reflect.DeepEqual(r, GoCacheReport{}) {
			t.Fatal(token, r, err)
		}
	}
	for _, days := range []int{0, -1, MaxFindingAgeDays + 1} {
		if _, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", days); !errors.Is(err, ErrFindingAge) {
			t.Fatal(days, err)
		}
	}
	for _, stage := range []string{"early", "layout", "commit", "unsupported_layout"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := goCacheHooks{}
			if stage == "early" {
				cancel()
			} else if stage == "commit" {
				hooks.beforeCommit = cancel
			} else {
				hooks.afterLayout = cancel
			}
			root := goCacheFixtureRoot
			if stage == "unsupported_layout" {
				root += "-absent"
			}
			if r, err := s.goBuildCache(ctx, root, "", 90, hooks); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, GoCacheReport{}) {
				t.Fatal("cancellation returned partial positive/empty evidence", r, err)
			}
		})
	}
	q := newGoCacheFixture(t, goCacheFixtureRoot)
	for i := 0; i < 21; i++ {
		goCacheAdd(t, q, i+1, 'd', fmt.Sprint(3000+i), 1, 0)
	}
	r, err = q.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || len(r.Files) != 20 || r.EntriesExamined != 278 || r.NextCursor == "" {
		t.Fatal("selected-row bound differs", r, err)
	}
	assertGoCacheQualified(t, r)
	next, err = q.GoBuildCache(context.Background(), goCacheFixtureRoot, r.NextCursor, 90)
	if err != nil || len(next.Files) != 1 || next.Files[0].EntryID <= r.Files[19].EntryID || next.KnownObjects != 1 || next.NextCursor != "" {
		t.Fatal("selected continuation differs", next, err)
	}
	assertGoCacheQualified(t, next)
}

func TestGoBuildCacheOtherRootRawRowsCannotHideBound(t *testing.T) {
	s := newGoCacheFixture(t, goCacheFixtureRoot)
	if err := s.SyncRoots(context.Background(), []string{goCacheFixtureRoot, "/different-generated-root"}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1001; i++ {
		goCachePut(t, tx, 2, fmt.Sprintf("other%d", i), ".", "file", fmt.Sprint(1000+i), 1, 1, 0)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	goCacheAdd(t, s, 1, 'd', "5000", 1, 0)
	r, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
	if err != nil || r.EntriesExamined != 1000 || len(r.Files) != 0 || goCacheDiagnostic(r, "different_root") != 742 || r.NextCursor == "" {
		t.Fatal("root filtering bypassed raw fence", r, err)
	}
	assertGoCacheQualified(t, r)
	next, err := s.GoBuildCache(context.Background(), goCacheFixtureRoot, r.NextCursor, 90)
	if err != nil || len(next.Files) != 1 || goCacheDiagnostic(next, "different_root") != 259 || next.NextCursor != "" {
		t.Fatal("exact scope lost continuation", next, err)
	}
	assertGoCacheQualified(t, next)
}

func TestGoBuildCacheSingleSnapshotAndLegacyOfflineReaders(t *testing.T) {
	t.Run("one snapshot while writer held", func(t *testing.T) {
		s := newGoCacheFixture(t, goCacheFixtureRoot)
		path := goCacheAdd(t, s, 1, 'd', "700", 17, 4096)
		r, err := OpenReader(context.Background(), filepath.Dir(s.path))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		captured, err := r.goBuildCache(context.Background(), goCacheFixtureRoot, "", 90, goCacheHooks{afterLayout: func() {
			// Change both layout and selected-file evidence between layout and
			// row reads. One transaction must retain the entire earlier view.
			if _, err := s.db.Exec("UPDATE entries SET kind='symlink' WHERE path=X'524541444d45'"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE entries SET size=99,allocated=0 WHERE path=?", []byte(path)); err != nil {
				t.Fatal(err)
			}
		}})
		if err != nil || captured.Layout.Status != "layout_metadata_recognized" || len(captured.Files) != 1 || *captured.Files[0].LogicalBytes != 17 || *captured.Files[0].AllocatedBytes != 4096 {
			t.Fatal("mixed layout/file snapshots", captured, err)
		}
		assertGoCacheQualified(t, captured)
		next, err := r.GoBuildCache(context.Background(), goCacheFixtureRoot, "", 90)
		if err != nil || next.Layout.Code != "marker_kind_or_parent" || len(next.Files) != 0 {
			t.Fatal("next view missed committed layout change", next, err)
		}
		assertGoCacheQualified(t, next)
	})
	for version := 4; version <= schemaVersion; version++ {
		t.Run(fmt.Sprintf("schema%d", version), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			path := filepath.Join(dir, Filename)
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, path, false)
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
			if _, err = s.db.Exec(fmt.Sprintf("PRAGMA application_id=0x52594444;PRAGMA user_version=%d", version)); err != nil {
				t.Fatal(err)
			}
			s.schema = version
			root := filepath.Join(t.TempDir(), "never-created-cache")
			if err = s.SyncRoots(ctx, []string{root}); err != nil {
				t.Fatal(err)
			}
			goCacheLayoutFixture(t, s, 1)
			goCacheAdd(t, s, 1, 'a', "700", 19, 0)
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			report, err := r.GoBuildCache(ctx, root, "", 90)
			if err != nil || len(report.Files) != 1 || report.Files[0].Path != filepath.Join(root, goCacheName(1, 'a')) {
				t.Fatal(report, err)
			}
			assertGoCacheQualified(t, report)
			var gotVersion, ledger int
			if err = r.db.QueryRow("PRAGMA user_version").Scan(&gotVersion); err != nil {
				t.Fatal(err)
			}
			if err = r.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&ledger); err != nil {
				t.Fatal(err)
			}
			if gotVersion != version || ledger != version {
				t.Fatal("reader migrated legacy cache storage", gotVersion, ledger)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("reader changed saved database", err)
			}
			if _, err = os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("saved report accessed/created cache path", err)
			}
		})
	}
}

// Opt in only where an existing Go toolchain is available. This test creates
// all inputs/cache/output under disposable fixture paths; it never installs a
// toolchain, runs go env, touches an installed cache or reads cache bodies.
func TestGoBuildCacheToolProduced(t *testing.T) {
	if os.Getenv("RYDD_TEST_GO_CACHE_TOOL") != "1" {
		t.Skip("tool-produced compatibility fixture requires RYDD_TEST_GO_CACHE_TOOL=1; generated metadata tests remain separate")
	}
	tool, err := exec.LookPath("go")
	if err != nil {
		t.Fatal("tool-produced gate requested but no existing Go toolchain is available", err)
	}
	info, err := buildinfo.ReadFile(tool)
	if err != nil || info.GoVersion != "go1.27.1" {
		t.Fatalf("tool-produced gate requires the verified existing go1.27.1 executable before invocation; build info=%v error=%v", info, err)
	}
	base := t.TempDir()
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	project, cache, tmp, telemetry := filepath.Join(base, "project"), filepath.Join(base, "cache"), filepath.Join(base, "tool-tmp"), filepath.Join(base, "telemetry")
	for _, dir := range []string{project, cache, tmp, telemetry} {
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(telemetry, "mode"), []byte("off\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(project, "go.mod"), []byte("module generated.example/cachefixture\n\ngo 1.18\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(project, "main.go"), []byte("package main\nfunc main() { println(\"generated cache fixture\") }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, tool, "build", "-o", filepath.Join(base, "generated-output"), ".")
	cmd.Dir = project
	// Use guarded direct-child cancellation; compiler descendants are outside scope.
	cmd.WaitDelay = 2 * time.Second
	// Explicit tool/cache/module settings exclude persisted GOENV, a cache
	// helper and network/toolchain downloads. HOME is not repurposed.
	overrides := map[string]string{"GOENV": "off", "GOCACHE": cache, "GOCACHEPROG": "", "GOPATH": filepath.Join(base, "gopath"), "GOMODCACHE": filepath.Join(base, "module-cache"), "GOTMPDIR": tmp, "TEST_TELEMETRY_DIR": telemetry, "GO_TELEMETRY_CHILD": "", "GO_TELEMETRY_CHILD_UPLOAD": "", "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": "-mod=readonly", "GO111MODULE": "on", "GOWORK": "off", "GODEBUG": "", "GOOS": "", "GOARCH": "", "CGO_ENABLED": "0"}
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	// GOROOT may identify the existing trusted toolchain; do not inherit
	// GOEXPERIMENT, wrapper commands, user flags or other Go settings.
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		cmd.Env = append(cmd.Env, "GOROOT="+goroot)
	}
	for key, value := range overrides {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output := &goCacheToolOutput{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Run(); err != nil {
		t.Fatalf("disposable existing-tool build failed: %v (output truncated=%t)\n%s", err, output.truncated, output.String())
	}
	if output.truncated {
		t.Fatal("disposable existing-tool output exceeded fixture limit")
	}
	s, err := OpenWriter(context.Background(), privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(context.Background(), []string{cache}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type observedSize struct{ logical, allocated int64 }
	expected := map[string]observedSize{}
	imported := 0
	put := func(relative, parent string, generation int64) {
		imported++
		if imported > 8192 {
			t.Fatal("generated cache exceeded the fixture's 8192-observation import bound")
		}
		path := filepath.Join(cache, relative)
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		kind := "other"
		switch st.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			kind = "directory"
		case unix.S_IFREG:
			kind = "file"
		case unix.S_IFLNK:
			kind = "symlink"
		}
		if kind == "file" && parent != "." {
			// Age only a generated regular file, never a user cache. The
			// production report still reads solely its saved observation.
			old := time.Unix(0, buildOutputOld)
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
			if err := unix.Lstat(path, &st); err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(relative, "-a") || strings.HasSuffix(relative, "-d") {
				expected[relative] = observedSize{st.Size, st.Blocks * 512}
			}
		}
		if _, err := tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,?,?,?,?,?,?,?,?,?)`, []byte(relative), []byte(parent), kind, st.Size, st.Blocks*512, st.Mtim.Sec*int64(time.Second)+st.Mtim.Nsec, st.Ctim.Sec*int64(time.Second)+st.Ctim.Nsec, fmt.Sprint(st.Dev), fmt.Sprint(st.Ino), generation, time.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	readNames := func(path string, limit int) []string {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		names, err := f.Readdirnames(limit + 1)
		closeErr := f.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if len(names) > limit {
			t.Fatalf("generated directory exceeded fixture bound %d", limit)
		}
		return names
	}
	put(".", "", 1)
	goCacheDir(t, tx, 1, ".", 1)
	for _, name := range readNames(cache, 300) {
		put(name, ".", 1)
		if len(name) != 2 || !goCacheLowerHex(name) {
			continue
		}
		goCacheDir(t, tx, 1, name, 2)
		for _, child := range readNames(filepath.Join(cache, name), 2048) {
			put(filepath.Join(name, child), name, 2)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(expected) == 0 {
		t.Fatal("existing tool produced no regular cache-entry oracle")
	}
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 1000; page++ {
		r, err := s.GoBuildCache(context.Background(), cache, cursor, 90)
		if err != nil || r.Layout.Status != "layout_metadata_recognized" || r.Layout.ConfirmedShards != 256 {
			t.Fatal("existing Go tool layout is unsupported", r, err)
		}
		assertGoCacheQualified(t, r)
		for _, f := range r.Files {
			relative := string(f.RelativePathBytes)
			want, exists := expected[relative]
			if !exists || seen[relative] || f.LogicalBytes == nil || f.AllocatedBytes == nil || *f.LogicalBytes != want.logical || *f.AllocatedBytes != want.allocated {
				t.Fatal("tool-produced metadata oracle differs", f, want)
			}
			seen[relative] = true
		}
		if r.NextCursor == "" {
			break
		}
		if r.NextCursor == cursor || page == 999 {
			t.Fatal("tool-produced report did not complete finite pagination")
		}
		cursor = r.NextCursor
	}
	if len(seen) != len(expected) {
		t.Fatal("tool-produced supported file coverage differs", len(seen), len(expected))
	}
	t.Logf("disposable pinned go1.27.1 cache: confirmed 256 shards; all %d supported regular file observations paginated with exact independent size/allocation metadata", len(seen))
}

type goCacheToolOutput struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *goCacheToolOutput) String() string { return b.buffer.String() }

func (b *goCacheToolOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining < len(p) {
		b.truncated = true
		p = p[:remaining]
	}
	_, err := b.buffer.Write(p)
	return n, err
}

func TestGoCacheToolOutputStreamCopyBound(t *testing.T) {
	// LimitedReader hides the source's WriterTo. A promoted Buffer.ReadFrom
	// must not bypass Write when io.Copy handles a subprocess output stream.
	input := strings.Repeat("generated-output", 8192)
	output := &goCacheToolOutput{limit: 1024}
	n, err := io.Copy(output, io.LimitReader(strings.NewReader(input), int64(len(input))))
	if err != nil || n != int64(len(input)) || !output.truncated || len(output.String()) != output.limit || output.String() != input[:output.limit] {
		t.Fatal("stream copy bypassed the fixture's retained-output limit", n, err, output.truncated, len(output.String()))
	}
}
