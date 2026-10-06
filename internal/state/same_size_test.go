package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func sameSizeFixture(t *testing.T) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.SyncRoots(context.Background(), []string{"/offline-size-fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,X'2e',1,1,20)`); err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func addSameSizeFile(t *testing.T, s *Store, path string, size int64, inode string) int64 {
	t.Helper()
	result, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,'file',?,4096,1,2,'1',?,1,10)`, []byte(path), []byte(filepath.Dir(path)), size, inode)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSameSizeBandsCrossPageBoundariesWithoutLosingSingletons(t *testing.T) {
	s, _ := sameSizeFixture(t)
	for i, size := range []int64{8 << 20, 8 << 20, 8 << 20, 4 << 20, 4 << 20, 1 << 20} {
		addSameSizeFile(t, s, fmt.Sprintf("file-%d", i), size, fmt.Sprint(i+1))
	}
	first, err := s.SameSizeCandidates(context.Background(), 2, "", SameSizeMinimumBytes)
	if err != nil || first.Source != "saved_inventory" || first.ContentVerified || first.CurrentStateVerified || first.EstimatedReclaimableBytes != nil || first.MinimumBytes != 1<<20 || first.Limit != 2 || first.EntriesExamined != 2 || len(first.Bands) != 1 || first.NextCursor == "" || first.PageCoverage != "more_saved_entries" {
		t.Fatal(first, err)
	}
	band := first.Bands[0]
	if band.LogicalBytes != 8<<20 || len(band.Files) != 2 || band.Files[0].ID != 3 || band.Files[1].ID != 2 || band.ContinuesBefore || !band.ContinuesAfter || band.KnownObjects != 2 || band.RepeatedSavedObjects != 0 {
		t.Fatal(band)
	}
	second, err := s.SameSizeCandidates(context.Background(), 2, first.NextCursor, SameSizeMinimumBytes)
	if err != nil || second.EntriesExamined != 2 || len(second.Bands) != 2 || second.NextCursor == "" {
		t.Fatal(second, err)
	}
	if b := second.Bands[0]; len(b.Files) != 1 || b.Files[0].ID != 1 || !b.ContinuesBefore || b.ContinuesAfter {
		t.Fatal("tail singleton lost", b)
	}
	if b := second.Bands[1]; len(b.Files) != 1 || b.Files[0].ID != 5 || b.ContinuesBefore || !b.ContinuesAfter {
		t.Fatal("head singleton lost", b)
	}
	third, err := s.SameSizeCandidates(context.Background(), 2, second.NextCursor, SameSizeMinimumBytes)
	if err != nil || third.EntriesExamined != 2 || len(third.Bands) != 1 || third.NextCursor != "" || third.PageCoverage != "saved_entries_exhausted" {
		t.Fatal(third, err)
	}
	if b := third.Bands[0]; len(b.Files) != 1 || b.Files[0].ID != 4 || !b.ContinuesBefore || b.ContinuesAfter {
		t.Fatal("continued band tail lost or complete singleton emitted", b)
	}
	for _, report := range []SameSizeReport{first, second, third} {
		if report.Diagnostics[len(report.Diagnostics)-1].Count != report.EntriesExamined {
			t.Fatal("eligible rows not counted independently of displayed bands", report)
		}
	}
}

func TestSameSizeSavedAliasesUnknownAndConflictingEvidence(t *testing.T) {
	s, _ := sameSizeFixture(t)
	for i, inode := range []string{"10", "10", "11", "", "0", "011", "12", "12", "12"} {
		addSameSizeFile(t, s, fmt.Sprintf("file-%d", i), 2<<20, inode)
	}
	// Different parent scan generations are not platform inode generations.
	if _, err := s.db.Exec(`UPDATE entries SET generation=2 WHERE id=2;
 UPDATE entries SET ctime_ns=3 WHERE id=8;
 UPDATE entries SET allocated=8192 WHERE id=9`); err != nil {
		t.Fatal(err)
	}
	r, err := s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || len(r.Bands) != 1 {
		t.Fatal(r, err)
	}
	b := r.Bands[0]
	if len(b.Files) != 9 || b.KnownObjects != 3 || b.RepeatedSavedObjects != 3 || b.UnknownIdentities != 3 || b.ConflictingIdentities != 1 || b.ContinuesBefore || b.ContinuesAfter {
		t.Fatal(b)
	}
	if b.Files[7].ParentPass != "unconfirmed" {
		t.Fatal("parent-pass qualifier lost", b.Files)
	}
	// Same saved identity alone is still displayed, with no redundant-copy or
	// reclaimable-space claim, and counters remain page-local when it is split.
	if _, err = s.db.Exec("DELETE FROM entries WHERE id>2"); err != nil {
		t.Fatal(err)
	}
	r, err = s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || len(r.Bands) != 1 || r.Bands[0].KnownObjects != 1 || r.Bands[0].RepeatedSavedObjects != 1 || r.Bands[0].ConflictingIdentities != 0 || r.EstimatedReclaimableBytes != nil {
		t.Fatal(r, err)
	}
	page, err := s.SameSizeCandidates(context.Background(), 1, "", 1)
	if err != nil || len(page.Bands) != 1 || page.Bands[0].KnownObjects != 1 || page.Bands[0].RepeatedSavedObjects != 0 {
		t.Fatal(page, err)
	}
	for _, value := range []string{"", "0", "01", "-1", "18446744073709551616", "device"} {
		if sameSizeIdentityNumber(value, true) {
			t.Fatal("invalid canonical nonzero inode accepted", value)
		}
	}
	if !sameSizeIdentityNumber("0", false) || !sameSizeIdentityNumber("18446744073709551615", true) {
		t.Fatal("valid canonical device/inode rejected")
	}
}

func TestSameSizeFiltersAfterRawBoundAndPreservesContinuation(t *testing.T) {
	s, _ := sameSizeFixture(t)
	addSameSizeFile(t, s, "keep-a", 2<<20, "1")
	addSameSizeFile(t, s, "keep-b", 2<<20, "2")
	for i := 0; i < 5; i++ {
		addSameSizeFile(t, s, fmt.Sprintf("node_modules/pkg-%d/file", i), 2<<20, fmt.Sprint(i+3))
	}
	first, err := s.SameSizeCandidates(context.Background(), 3, "", 1)
	if err != nil || first.EntriesExamined != 3 || len(first.Bands) != 0 || first.NextCursor == "" || first.Diagnostics[2].Count != 3 {
		t.Fatal("excluded band bypassed raw-row work limit", first, err)
	}
	second, err := s.SameSizeCandidates(context.Background(), 3, first.NextCursor, 1)
	if err != nil || second.EntriesExamined != 3 || len(second.Bands) != 1 || len(second.Bands[0].Files) != 1 || second.Bands[0].Files[0].Path != "/offline-size-fixture/keep-b" || !second.Bands[0].ContinuesBefore || !second.Bands[0].ContinuesAfter || second.Diagnostics[2].Count != 2 {
		t.Fatal(second, err)
	}
	third, err := s.SameSizeCandidates(context.Background(), 3, second.NextCursor, 1)
	if err != nil || third.EntriesExamined != 1 || len(third.Bands) != 1 || len(third.Bands[0].Files) != 1 || third.NextCursor != "" {
		t.Fatal(third, err)
	}
}

func TestSameSizeGeneratedCompactSkipRootAndPathExclusions(t *testing.T) {
	s, _ := sameSizeFixture(t)
	if err := s.SyncRoots(context.Background(), []string{"/offline-size-fixture", "/offline-disabled", "/offline-generated/node_modules", "/offline-dependency/node_modules/pkg"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE roots SET enabled=0 WHERE id=2;
 INSERT INTO compact_dirs(root_id,path,generation,logical,files,unknown_inodes,skipped_files) VALUES(1,X'6361636865',1,0,0,0,0)`); err != nil {
		t.Fatal(err)
	}
	for i, path := range []string{"keep-a", "keep-b", "node_modules/pkg/file", "a/node_modules/file", "cache/file", "skipped", "../invalid", "disabled", "root-generated", "root-nested-generated", "node_modules", "node_modules_backup/file"} {
		id := addSameSizeFile(t, s, path, 2<<20, fmt.Sprint(i+1))
		switch path {
		case "skipped":
			if _, err := s.db.Exec("UPDATE entries SET skip_reason='fixture' WHERE id=?", id); err != nil {
				t.Fatal(err)
			}
		case "disabled", "root-generated", "root-nested-generated":
			root := 2
			if path == "root-generated" {
				root = 3
			} else if path == "root-nested-generated" {
				root = 4
			}
			if _, err := s.db.Exec("UPDATE entries SET root_id=? WHERE id=?", root, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	addSameSizeFile(t, s, "below-minimum", (2<<20)-1, "40")
	r, err := s.SameSizeCandidates(context.Background(), 200, "", 2<<20)
	if err != nil || r.EntriesExamined != 12 || len(r.Bands) != 1 || len(r.Bands[0].Files) != 4 || r.NextCursor != "" {
		t.Fatal(r, err)
	}
	for i, count := range []int{1, 1, 4, 1, 1, 4} {
		if r.Diagnostics[i].Count != count {
			t.Fatal("exclusion diagnostics are not first-match counts", r.Diagnostics)
		}
	}
}

func TestSameSizeMalformedIdentityAndTextPathBounds(t *testing.T) {
	s, _ := sameSizeFixture(t)
	first := addSameSizeFile(t, s, "a", 2<<20, "1")
	second := addSameSizeFile(t, s, "b", 2<<20, "2")
	// SQLite text length stops at NUL. Enforce byte bounds so malformed saved
	// identities cannot make an otherwise bounded report retain a huge value.
	malformed := "1\x00" + strings.Repeat("x", 64<<10)
	if _, err := s.db.Exec("UPDATE entries SET device=? WHERE id=?", malformed, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE entries SET inode=? WHERE id=?", malformed, second); err != nil {
		t.Fatal(err)
	}
	r, err := s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || len(r.Bands) != 1 || r.Bands[0].UnknownIdentities != 2 || r.Bands[0].Files[0].Inode != "" || r.Bands[0].Files[1].Device != "" {
		t.Fatal("identity byte bound was bypassed", r, err)
	}
	path := []byte("text-byte-\xff\n")
	if _, err = s.db.Exec("UPDATE entries SET path=CAST(? AS TEXT),parent=CAST(X'2e' AS TEXT) WHERE id=?", path, first); err != nil {
		t.Fatal(err)
	}
	r, err = s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || !bytes.Equal(r.Bands[0].Files[1].PathBytes, append([]byte("/offline-size-fixture/"), path...)) {
		t.Fatal("legacy text path lost its exact byte representation", r, err)
	}
	longPath := []byte(strings.Repeat("\xff", 4098))
	if _, err = s.db.Exec("UPDATE entries SET path=CAST(? AS TEXT) WHERE id=?", longPath, first); err != nil {
		t.Fatal(err)
	}
	r, err = s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || r.Diagnostics[4].Count != 1 || len(r.Bands) != 0 {
		t.Fatal("text path byte bound was bypassed", r, err)
	}
}

func TestSameSizeSavedOnlyBytePathsAndParentPass(t *testing.T) {
	s, dir := sameSizeFixture(t)
	path := "byte-\xff\n\x1b"
	addSameSizeFile(t, s, path, 2<<20, "1")
	addSameSizeFile(t, s, "ordinary", 2<<20, "2")
	s.Close()
	before, err := os.ReadFile(filepath.Join(dir, Filename))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	first, err := r.SameSizeCandidates(context.Background(), 20, "", SameSizeMinimumBytes)
	if err != nil || len(first.Bands) != 1 || len(first.Bands[0].Files) != 2 || !bytes.Equal(first.Bands[0].Files[1].PathBytes, []byte("/offline-size-fixture/"+path)) || first.Bands[0].Files[0].ParentPass != "observed_in_completed_parent_pass" {
		t.Fatal(first, err)
	}
	payload, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	var reopened SameSizeReport
	if err = json.Unmarshal(payload, &reopened); err != nil || !bytes.Equal(reopened.Bands[0].Files[1].PathBytes, first.Bands[0].Files[1].PathBytes) {
		t.Fatal("JSON lost authoritative byte path", err)
	}
	r.Close()
	after, err := os.ReadFile(filepath.Join(dir, Filename))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("same-size read changed database bytes", err)
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, tc := range []struct{ query, parent string }{
		{"UPDATE directories SET complete=0", "partial"},
		{"UPDATE directories SET last_error='fixture error'", "directory_error"},
		{"DELETE FROM directories", "unknown"},
	} {
		if _, err = w.db.Exec(tc.query); err != nil {
			t.Fatal(err)
		}
		report, err := w.SameSizeCandidates(context.Background(), 20, "", 1)
		if err != nil || len(report.Bands) != 1 || report.Bands[0].Files[0].ParentPass != tc.parent || report.CurrentStateVerified || report.ContentVerified {
			t.Fatal(report, err)
		}
	}
}

func TestSameSizeMinimumLimitsCursorsAndIncarnations(t *testing.T) {
	s, _ := sameSizeFixture(t)
	addSameSizeFile(t, s, "a", math.MaxInt64, "1")
	addSameSizeFile(t, s, "b", math.MaxInt64, "2")
	page, err := s.SameSizeCandidates(context.Background(), 1, "", math.MaxInt64)
	if err != nil || len(page.Bands) != 1 || page.NextCursor == "" || page.Bands[0].LogicalBytes != math.MaxInt64 {
		t.Fatal(page, err)
	}
	for _, tc := range []struct {
		limit int
		min   int64
		want  error
	}{
		{0, 1, ErrSameSizeLimit}, {201, 1, ErrSameSizeLimit}, {20, 0, ErrSameSizeMinimum}, {20, -1, ErrSameSizeMinimum},
	} {
		if _, err = s.SameSizeCandidates(context.Background(), tc.limit, "", tc.min); !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
	}
	if _, err = s.SameSizeCandidates(context.Background(), 1, page.NextCursor, 1); !errors.Is(err, ErrReportCursor) {
		t.Fatal("changed minimum accepted", err)
	}
	other, _ := sameSizeFixture(t)
	addSameSizeFile(t, other, "a", math.MaxInt64, "1")
	addSameSizeFile(t, other, "b", math.MaxInt64, "2")
	if _, err = other.SameSizeCandidates(context.Background(), 1, page.NextCursor, math.MaxInt64); !errors.Is(err, ErrReportCursor) {
		t.Fatal("reused numeric IDs accepted a different inventory", err)
	}
	data, err := base64.RawURLEncoding.DecodeString(page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(body string) string { return base64.RawURLEncoding.EncodeToString([]byte(body)) }
	for _, token := range []string{
		"bad", strings.Repeat("a", sameSizeCursorLimit+1), page.NextCursor + "=",
		encode(string(data) + " {}"), encode(strings.Replace(string(data), `"version":1`, `"version":2`, 1)),
		encode(strings.Replace(string(data), `"version":1,`, `"version":1,"version":1,`, 1)),
		encode(strings.Replace(string(data), `"version":1,`, `"unexpected":true,"version":1,`, 1)),
		encode(strings.Replace(string(data), `"id":2`, `"id":0`, 1)),
		encode(`{"size":2,"id":1}`),
	} {
		if _, err = s.SameSizeCandidates(context.Background(), 1, token, math.MaxInt64); !errors.Is(err, ErrReportCursor) {
			t.Fatal("malformed cursor accepted", token, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.SameSizeCandidates(ctx, 20, "", 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSameSizeRequiresSchemaNineWithoutMigration(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	w, err := connect(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err = w.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = w.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=8"); err != nil {
		t.Fatal(err)
	}
	w.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.SameSizeCandidates(context.Background(), 20, "", 1); !errors.Is(err, ErrSameSizeSchema) {
		t.Fatal(err)
	}
	if _, err = r.LargestFiles(context.Background(), 20, ""); err != nil {
		t.Fatal("old report compatibility changed", err)
	}
	r.Close()
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read migrated old store", err)
	}
}

func TestSameSizeSingleSnapshotAndHugeBandSeeks(t *testing.T) {
	s, dir := sameSizeFixture(t)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,X'2e','file',1048576,4096,1,2,'1',?,1,10)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10000; i++ {
		if _, err = stmt.Exec([]byte(fmt.Sprintf("file-%05d", i)), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Inspect the bundled modernc driver's actual plan, rather than inferring
	// work from a SQL LIMIT or a host SQLite version. Only the size equality
	// query can seek directly within a huge band using the existing index.
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+sameSizeContinueQuery, SameSizeMinimumBytes, 5, 3)
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
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "MATERIALIZE selected") || !strings.Contains(plan, "entries_size (size=? AND rowid<?)") {
		t.Fatal("continuation did not use the bounded raw selection and rowid seek", plan)
	}
	rows, err = s.db.Query("EXPLAIN "+sameSizeContinueQuery, SameSizeMinimumBytes, 5, 3)
	if err != nil {
		t.Fatal(err)
	}
	twoKeySeek := false
	for rows.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode string
		var p4, comment sql.NullString
		if err = rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatal(err)
		}
		if opcode == "SeekLT" && p4.String == "2" {
			twoKeySeek = true
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !twoKeySeek {
		t.Fatal("continuation did not seek both size and rowid")
	}
	var inventoryID string
	if err = s.db.QueryRow("SELECT token FROM inventory_identity").Scan(&inventoryID); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(sameSizeCursor{Version: 1, InventoryID: inventoryID, MinimumBytes: SameSizeMinimumBytes, Size: SameSizeMinimumBytes, ID: 5})
	page, err := s.SameSizeCandidates(context.Background(), 2, base64.RawURLEncoding.EncodeToString(payload), SameSizeMinimumBytes)
	if err != nil || page.EntriesExamined != 2 || len(page.Bands) != 1 || len(page.Bands[0].Files) != 2 || page.Bands[0].Files[0].ID != 4 || page.Bands[0].Files[1].ID != 3 || !page.Bands[0].ContinuesBefore || !page.Bands[0].ContinuesAfter {
		t.Fatal(page, err)
	}
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	frozen, err := r.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer frozen.Rollback()
	if err = frozen.QueryRow("SELECT token FROM inventory_identity").Scan(&inventoryID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE entries SET inode='replacement',size=2097152 WHERE id=4"); err != nil {
		t.Fatal(err)
	}
	old, err := readSameSizeRows(context.Background(), frozen, sameSizeContinueQuery, SameSizeMinimumBytes, 5, 3)
	if err != nil || len(old) != 3 || old[0].file.ID != 4 || old[0].file.Inode != "4" || old[0].file.Size != SameSizeMinimumBytes {
		t.Fatal("selection crossed inventory snapshots", old, err)
	}
	if err = frozen.Commit(); err != nil {
		t.Fatal(err)
	}
	current, err := r.SameSizeCandidates(context.Background(), 2, base64.RawURLEncoding.EncodeToString(payload), SameSizeMinimumBytes)
	if err != nil || current.Bands[0].Files[0].ID != 3 || reflect.DeepEqual(current.Bands[0].Files, page.Bands[0].Files) {
		t.Fatal("new page incorrectly treated as frozen export", current, err)
	}
}
