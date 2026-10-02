package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func compactFixture(t *testing.T, enabled bool) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.SyncRoots(context.Background(), []string{"/fixture/node_modules"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ConfigureCompact(context.Background(), &enabled); err != nil {
		t.Fatal(err)
	}
	if err = s.SeedInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, dir
}
func compactBatch(t *testing.T, s *Store, gen int64, done bool, entries ...Entry) Job {
	t.Helper()
	ctx := context.Background()
	j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	b := ScanBatch{Identity: "fixture", Generation: gen, Directory: Entry{Path: []byte("."), Kind: "directory", Device: "d", Inode: "root"}, Entries: entries, Complete: done, Cursor: []byte("saved")}
	if err = s.CommitScan(ctx, *j, b); err != nil {
		t.Fatal(err)
	}
	return *j
}
func compactFile(name, inode string, size int64) Entry {
	return Entry{Path: []byte(name), Kind: "file", Device: "d", Inode: inode, Size: size, Allocated: 4096}
}
func compactMeasure(t *testing.T, s *Store) DirectoryReport {
	t.Helper()
	r, err := s.MeasureDirectory(context.Background(), "/fixture/node_modules")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func compactMaintenanceStep(ctx context.Context, s *Store) (bool, error) {
	worked, err := s.RetireCompact(ctx)
	if err != nil || worked {
		return worked, err
	}
	worked, err = s.RetireSubtrees(ctx)
	if err != nil || worked {
		return worked, err
	}
	return s.ReduceAllocations(ctx)
}
func TestCompactLegacyReplayAndRetirement(t *testing.T) {
	ctx := context.Background()
	s, dir := compactFixture(t, false)
	compactBatch(t, s, 1, true, compactFile("old", "a", 99), compactFile("old2", "b", 99))
	on := true
	if _, err := s.ConfigureCompact(ctx, &on); err != nil {
		t.Fatal(err)
	}
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j := compactBatch(t, s, 2, false, compactFile("new", "x", 10), compactFile("link", "x", 10))
	r := compactMeasure(t, s)
	if r.LogicalBytes == nil || *r.LogicalBytes != 20 || *r.AllocatedBytes != 4096 || r.FilePaths != 2 || r.RepeatedInodes != 1 || r.Status != "partial" {
		t.Fatal(r)
	}
	report, err := s.LargestFiles(ctx, 20, "")
	if err != nil || len(report.Files) != 0 {
		t.Fatal(report, err)
	}
	// Reusing the lease must not add another contribution.
	b := ScanBatch{Identity: "fixture", Generation: 2, Directory: Entry{Path: []byte("."), Kind: "directory"}, Entries: []Entry{compactFile("bad", "bad", 999)}}
	if err = s.CommitScan(ctx, j, b); !errors.Is(err, ErrStaleLease) {
		t.Fatal(err)
	}
	off := false
	if _, err = s.ConfigureCompact(ctx, &off); err == nil {
		t.Fatal("switched pending mode")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	compactBatch(t, s, 3, true, compactFile("new", "x", 10))
	r = compactMeasure(t, s)
	if *r.LogicalBytes != 10 || r.FilePaths != 1 || r.Status != "recorded_complete" {
		t.Fatal(r)
	}
	for i := 0; i < 20; i++ {
		worked, err := compactMaintenanceStep(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
		if i == 19 {
			t.Fatal("retirement did not drain")
		}
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM entries WHERE kind='file'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM compact_inodes").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// Switch back only after retirement; new detailed files must survive cleanup.
	if _, err = s.ConfigureCompact(ctx, &off); err != nil {
		t.Fatal(err)
	}
	if err = s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 4, true, compactFile("detail", "y", 30))
	for {
		worked, err := compactMaintenanceStep(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	r = compactMeasure(t, s)
	if *r.LogicalBytes != 30 || r.CompactedDirectories != 0 {
		t.Fatal(r)
	}
}
func TestCompactReportIdentityBudgetAndUnknown(t *testing.T) {
	s, _ := compactFixture(t, true)
	for start := 0; start < DirectoryEntryLimit+1; start += MaxBatchEntries {
		var entries []Entry
		for i := start; i < min(start+MaxBatchEntries, DirectoryEntryLimit+1); i++ {
			entries = append(entries, compactFile(fmt.Sprint(i), fmt.Sprint(i), 1))
		}
		compactBatch(t, s, 1, start+MaxBatchEntries >= DirectoryEntryLimit+1, entries...)
	}
	r := compactMeasure(t, s)
	if r.LogicalBytes == nil || *r.LogicalBytes != 10001 || r.AllocatedBytes != nil || r.InodeEntriesExamined != 10000 || r.EntriesExamined != 1 || r.Status != "partial" {
		t.Fatal(r)
	}
	// Partial identity evidence must never masquerade as a measured zero.
	for {
		worked, err := compactMaintenanceStep(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	r = compactMeasure(t, s)
	if r.AllocatedBytes == nil || *r.AllocatedBytes != 4096*(DirectoryEntryLimit+1) || r.AllocatedSizeSource != "cached_reduction" || r.InodeEntriesExamined != 0 || r.Status != "recorded_complete" {
		t.Fatal("large scope did not gain a cached allocated total", r)
	}
	if err := s.SeedInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 2, true, compactFile("unknown", "", 12))
	r = compactMeasure(t, s)
	if *r.LogicalBytes != 12 || r.AllocatedBytes != nil || r.UnknownInodes != 1 {
		t.Fatal(r)
	}
}
func TestCompactMigrationKeepsV4Readable(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	// Construct the real previous schema and ledger, not a mislabeled v5 DB.
	if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := connect(ctx, filepath.Join(dir, Filename), false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=4"); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(ctx, []string{"/fixture"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// connect creates the fixture with the process umask; set private mode.
	if err := os.Chmod(filepath.Join(dir, Filename), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if summary, err := r.Summary(ctx); err != nil || summary.Schema != 4 {
		t.Fatal(summary, err)
	}
	if _, err = r.LargestFiles(ctx, 20, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = r.MeasureDirectory(ctx, "/fixture"); err != nil {
		t.Fatal(err)
	}
	r.Close()
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var root int
	if err = w.db.QueryRow("SELECT count(*) FROM roots").Scan(&root); err != nil || root != 1 {
		t.Fatal(root, err)
	}
	if w.schema != schemaVersion {
		t.Fatal(w.schema)
	}
}

func TestCompactOverflowRollsBackLeaseAndEvidence(t *testing.T) {
	s, _ := compactFixture(t, true)
	ctx := context.Background()
	j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	b := ScanBatch{Identity: "fixture", Generation: 1, Directory: Entry{Path: []byte("."), Kind: "directory"}, Complete: true,
		Entries: []Entry{compactFile("a", "a", math.MaxInt64), compactFile("b", "b", 1)}}
	if err = s.CommitScan(ctx, *j, b); err == nil {
		t.Fatal("accepted overflow")
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM compact_inodes").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if err = s.FinishJob(ctx, *j, false, nil, time.Now(), ""); err != nil {
		t.Fatal("lease was not rolled back", err)
	}
}

// Commit real leased batches so generation membership, compact evidence and
// queued children have the same relationships as scanner-produced records.
func compactTreePass(t *testing.T, s *Store, generation int64, tree map[string][]Entry, maintain ...bool) {
	t.Helper()
	ctx := context.Background()
	offsets := map[string]int{}
	for {
		j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if j == nil {
			break
		}
		entries, ok := tree[string(j.Path)]
		if !ok {
			t.Fatalf("unexpected directory job %q", j.Path)
		}
		start := offsets[string(j.Path)]
		end := min(start+MaxBatchEntries, len(entries))
		offsets[string(j.Path)] = end
		b := ScanBatch{Identity: "fixture", Generation: generation,
			Directory: Entry{Path: j.Path, Kind: "directory", Device: "d", Inode: string(j.Path)},
			Entries:   entries[start:end], Complete: end == len(entries), Cursor: []byte("fixture")}
		if err = s.CommitScan(ctx, *j, b); err != nil {
			t.Fatal(err)
		}
	}
	if len(maintain) > 0 && !maintain[0] {
		return
	}
	for {
		worked, err := compactMaintenanceStep(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
}

func TestCompactCompletedMembershipExcludesHistoricalSubtrees(t *testing.T) {
	ctx := context.Background()
	directory := func(path string) Entry {
		return Entry{Path: []byte(path), Kind: "directory", Device: "d", Inode: path}
	}
	for _, replacement := range []string{"absent", "file", "symlink"} {
		t.Run(replacement, func(t *testing.T) {
			s, dir := compactFixture(t, true)
			compactTreePass(t, s, 1, map[string][]Entry{
				".":           {directory("gone"), directory("gone2"), compactFile("keep", "shared", 5)},
				"gone":        {directory("gone/nested"), compactFile("gone/link", "shared", 5)},
				"gone/nested": {compactFile("gone/nested/old", "old", 90)},
				"gone2":       {compactFile("gone2/file", "sibling", 7)},
			})
			before := compactMeasure(t, s)
			if *before.LogicalBytes != 107 || before.RepeatedInodes != 1 {
				t.Fatal(before)
			}
			if err := s.SeedInventory(ctx); err != nil {
				t.Fatal(err)
			}
			entries := []Entry{directory("gone2"), compactFile("keep", "shared", 5)}
			wantLogical, wantAllocated, wantFiles := int64(12), int64(8192), 2
			wantExcluded := 2
			switch replacement {
			case "file":
				entries = append(entries, compactFile("gone", "new", 3))
				wantLogical, wantAllocated, wantFiles = 15, 12288, 3
			case "symlink":
				entries = append(entries, Entry{Path: []byte("gone"), Kind: "symlink", SkipReason: "symlink"})
				wantExcluded = 1 // The replacement itself is still observed.
			}
			compactTreePass(t, s, 2, map[string][]Entry{
				".": entries, "gone2": {compactFile("gone2/file", "sibling", 7)},
			}, false)
			// Offline read-only reporting also survives closing/reopening the store.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			after := compactMeasure(t, r)
			wantStatus := "recorded_complete"
			if replacement == "symlink" {
				wantStatus = "partial"
			}
			if after.Status != wantStatus || *after.LogicalBytes != wantLogical || *after.AllocatedBytes != wantAllocated ||
				after.ExcludedEntries != wantExcluded || after.FilePaths != wantFiles || after.RepeatedInodes != 0 ||
				after.InodeEntriesExamined != wantFiles || after.AllocatedSizeSource != "bounded_identity_check" || after.UnconfirmedEntries != 0 {
				t.Fatalf("%+v", after)
			}
			for _, path := range []string{"gone", "gone/nested"} {
				selected, err := r.MeasureDirectory(ctx, "/fixture/node_modules/"+path)
				if err != nil || selected.Status != "unknown" || selected.LogicalBytes != nil || selected.AllocatedBytes != nil || selected.UnknownReason == "" {
					t.Fatal(selected, err)
				}
			}
			var saved int
			if err := r.db.QueryRow("SELECT count(*) FROM compact_dirs").Scan(&saved); err != nil || saved != 4 {
				t.Fatal("reports must retain historical evidence", saved, err)
			}
		})
	}
}

func TestCompactIncompleteOrFailedMembershipRetainsEvidence(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint("failed=", failed), func(t *testing.T) {
			ctx := context.Background()
			s, _ := compactFixture(t, true)
			compactTreePass(t, s, 1, map[string][]Entry{
				".":     {{Path: []byte("child"), Kind: "directory"}},
				"child": {compactFile("child/old", "old", 50)},
			})
			if err := s.SeedInventory(ctx); err != nil {
				t.Fatal(err)
			}
			j := compactBatch(t, s, 2, false)
			if failed {
				// A failure after an incomplete batch must not reconcile children.
				pending, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute)
				if err != nil || pending == nil || pending.ID != j.ID {
					t.Fatal(pending, err)
				}
				if err = s.CommitScan(ctx, *pending, ScanBatch{Fault: "fixture read error"}); err != nil {
					t.Fatal(err)
				}
			}
			for _, path := range []string{"/fixture/node_modules", "/fixture/node_modules/child"} {
				r, err := s.MeasureDirectory(ctx, path)
				if err != nil || r.Status != "stale" || r.LogicalBytes == nil || *r.LogicalBytes != 50 ||
					r.AllocatedBytes == nil || *r.AllocatedBytes != 4096 || r.ExcludedEntries != 0 {
					t.Fatal(r, err)
				}
			}
		})
	}
}
