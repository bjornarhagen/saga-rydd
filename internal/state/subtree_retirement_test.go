package state

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func retirementDirectory(path string) Entry { return Entry{Path: []byte(path), Kind: "directory"} }

func retirementFixture(t *testing.T, sizes ...int) (*Store, string) {
	t.Helper()
	s, dir := compactFixture(t, true)
	if _, err := s.db.Exec("UPDATE roots SET path=?", []byte("/fixture")); err != nil {
		t.Fatal(err)
	}
	var files []Entry
	count := 300
	if len(sizes) > 0 {
		count = sizes[0]
	}
	for i := 0; i < count; i++ {
		files = append(files, compactFile(fmt.Sprintf("node_modules/wide/%03d", i), fmt.Sprint(i), 1))
	}
	compactTreePass(t, s, 1, map[string][]Entry{
		".":                 {retirementDirectory("node_modules"), retirementDirectory("node_modules2"), compactFile("outside", "outside", 1)},
		"node_modules":      {retirementDirectory("node_modules/wide")},
		"node_modules/wide": files,
		"node_modules2":     {compactFile("node_modules2/keep", "sibling", 7)},
	})
	return s, dir
}

func retireMissingTree(t *testing.T, s *Store) {
	t.Helper()
	if err := s.SeedInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	compactTreePass(t, s, 2, map[string][]Entry{
		".":             {retirementDirectory("node_modules2"), compactFile("outside", "outside", 1)},
		"node_modules2": {compactFile("node_modules2/keep", "sibling", 7)},
	}, false)
}

func retirementPayloadCount(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	// Control rows/cursors are separate from the 128-row payload retirement
	// budget; count every table that can hold source/cache payload here.
	for _, table := range []string{"entries", "directories", "compact_dirs", "compact_inodes", "compact_retirement", "allocation_members", "allocation_identities", "allocation_cache"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		count += n
	}
	return count
}

func advancePartialRetirement(t *testing.T, s *Store) {
	t.Helper()
	for i := 0; i < 50; i++ {
		before := retirementPayloadCount(t, s)
		worked, err := s.RetireSubtrees(context.Background())
		if err != nil || !worked {
			t.Fatal(worked, err)
		}
		after := retirementPayloadCount(t, s)
		if before-after > MaxBatchEntries {
			t.Fatal("unbounded retirement batch", before, after)
		}
		var remaining int
		if err := s.db.QueryRow("SELECT count(*) FROM compact_inodes").Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining > 0 && remaining < 300 {
			return
		}
	}
	t.Fatal("retirement did not reach partial inode deletion")
}

func assertRetiredTree(t *testing.T, s *Store) {
	t.Helper()
	for _, table := range []string{"entries", "directories", "compact_dirs", "compact_inodes", "compact_retirement", "allocation_cache", "subtree_reconcile", "subtree_retirement"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM "+table+" WHERE path=? OR (path>=? AND path<?)", []byte("node_modules"), []byte("node_modules/"), []byte("node_modules0")).Scan(&n); err != nil || n != 0 {
			t.Fatal(table, n, err)
		}
	}
	r, err := s.MeasureDirectory(context.Background(), "/fixture")
	if err != nil || r.LogicalBytes == nil || *r.LogicalBytes != 8 || r.Status != "recorded_complete" || r.ExcludedEntries != 0 {
		t.Fatal(r, err)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM entries WHERE path=?", []byte("node_modules2/keep")).Scan(&count); err != nil || count != 1 {
		t.Fatal("sibling lost", count, err)
	}
}

func TestSubtreeRetirementBoundedResumeAndFencing(t *testing.T) {
	s, dir := retirementFixture(t)
	ctx := context.Background()
	retireMissingTree(t, s)
	var scanBefore, revisionBefore int64
	if err := s.db.QueryRow("SELECT scan_revision,revision FROM allocation_revisions").Scan(&scanBefore, &revisionBefore); err != nil {
		t.Fatal(err)
	}
	advancePartialRetirement(t, s)
	var scanAfter, revisionAfter int64
	if err := s.db.QueryRow("SELECT scan_revision,revision FROM allocation_revisions").Scan(&scanAfter, &revisionAfter); err != nil || scanAfter != scanBefore || revisionAfter <= revisionBefore {
		t.Fatal(scanAfter, revisionAfter, err)
	}
	if worked, err := s.ReduceAllocations(ctx); err != nil || worked {
		t.Fatal("reduced before retirement finished", worked, err)
	}
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute); err != nil || j != nil {
		t.Fatal("seeded before retirement finished", j, err)
	}
	off := false
	if _, err := s.ConfigureCompact(ctx, &off); err == nil {
		t.Fatal("mode changed during retirement")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 100; i++ {
		before := retirementPayloadCount(t, w)
		worked, err := w.RetireSubtrees(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if before-retirementPayloadCount(t, w) > MaxBatchEntries {
			t.Fatal("unbounded resumed retirement")
		}
		if !worked {
			break
		}
		if i == 99 {
			t.Fatal("retirement did not drain")
		}
	}
	drainAllocations(t, w)
	assertRetiredTree(t, w)
}

func TestSubtreeRetirementPreservesReappearanceAndReplacements(t *testing.T) {
	ctx := context.Background()
	t.Run("reappearance", func(t *testing.T) {
		s, _ := retirementFixture(t)
		retireMissingTree(t, s)
		advancePartialRetirement(t, s)
		// Simulate newly committed observations arriving between maintenance
		// batches. The old proof must not delete any of these newer records.
		if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Now()); err != nil {
			t.Fatal(err)
		}
		compactTreePass(t, s, 3, map[string][]Entry{
			".":             {retirementDirectory("node_modules"), retirementDirectory("node_modules2"), compactFile("outside", "outside", 1)},
			"node_modules":  {compactFile("node_modules/new", "fresh", 99)},
			"node_modules2": {compactFile("node_modules2/keep", "sibling", 7)},
		})
		r, err := s.MeasureDirectory(ctx, "/fixture/node_modules")
		if err != nil || r.Status != "recorded_complete" || *r.LogicalBytes != 99 || *r.AllocatedBytes != 4096 || r.AllocatedSizeSource != "cached_reduction" {
			t.Fatal(r, err)
		}
		var fresh int
		if err := s.db.QueryRow("SELECT count(*) FROM compact_inodes WHERE inode='fresh'").Scan(&fresh); err != nil || fresh != 1 {
			t.Fatal(fresh, err)
		}
	})
	for _, kind := range []string{"file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := retirementFixture(t)
			if err := s.SeedInventory(ctx); err != nil {
				t.Fatal(err)
			}
			replacement := compactFile("node_modules", "replacement", 3)
			replacement.Kind = kind
			compactTreePass(t, s, 2, map[string][]Entry{
				".":             {replacement, retirementDirectory("node_modules2"), compactFile("outside", "outside", 1)},
				"node_modules2": {compactFile("node_modules2/keep", "sibling", 7)},
			})
			var got string
			if err := s.db.QueryRow("SELECT kind FROM entries WHERE path=?", []byte("node_modules")).Scan(&got); err != nil || got != kind {
				t.Fatal(got, err)
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM compact_inodes").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM allocation_cache WHERE path=?", []byte("node_modules")).Scan(&count); err != nil || count != 0 {
				t.Fatal("obsolete cache", count, err)
			}
		})
	}
}

func TestSubtreeRetirementRollbackAndIncompleteEvidence(t *testing.T) {
	ctx := context.Background()
	s, _ := retirementFixture(t)
	retireMissingTree(t, s)
	advancePartialRetirement(t, s)
	before := retirementPayloadCount(t, s)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_retire BEFORE UPDATE ON allocation_revisions BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireSubtrees(ctx); err == nil {
		t.Fatal("injected retirement failure accepted")
	}
	if after := retirementPayloadCount(t, s); after != before {
		t.Fatal("retirement partially committed", before, after)
	}
	if _, err := s.db.Exec("DROP TRIGGER fail_retire"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.RetireSubtrees(canceled); err == nil {
		t.Fatal("canceled retirement succeeded")
	}
	if retirementPayloadCount(t, s) != before {
		t.Fatal("cancellation removed data")
	}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Now()); err != nil {
		t.Fatal(err)
	}
	j := compactBatch(t, s, 3, false)
	if worked, err := s.RetireSubtrees(ctx); err != nil || worked {
		t.Fatal("retired during incomplete scan", worked, err)
	}
	// A failed listing retains a completed marker but cannot establish absence.
	lease, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute)
	if err != nil || lease == nil || lease.ID != j.ID {
		t.Fatal(lease, err)
	}
	if err = s.CommitScan(ctx, *lease, ScanBatch{Fault: "fixture read failure"}); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.RetireSubtrees(ctx); err != nil || worked {
		t.Fatal("retired with delayed retry", worked, err)
	}
	// Remove only the synthetic retry to exercise rejection of the saved
	// incomplete/error evidence independently from the pending-job guard.
	if _, err = s.db.Exec("DELETE FROM jobs WHERE id=?", j.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		worked, err := s.RetireSubtrees(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	if retirementPayloadCount(t, s) != before {
		t.Fatal("obsolete proof removed data after new scan")
	}
}

func TestSubtreeRetirementCrashRecovery(t *testing.T) {
	s, dir := retirementFixture(t)
	retireMissingTree(t, s)
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSubtreeRetirementCrashChild$")
	cmd.Env = append(os.Environ(), "RYDD_SUBTREE_CRASH_DIR="+dir)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	line := bufio.NewScanner(stdout)
	ready := line.Scan() && line.Text() == "ready"
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !ready {
		t.Fatal("retirement child not ready")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal(cmd.ProcessState)
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	drainAllocations(t, w)
	assertRetiredTree(t, w)
}

func TestSubtreeRetirementCrashChild(t *testing.T) {
	dir := os.Getenv("RYDD_SUBTREE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	advancePartialRetirement(t, s)
	fmt.Println("ready")
	for {
		time.Sleep(time.Hour)
	}
}

func TestSubtreeRetirementMigrationV6(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("fail=", fail), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, filepath.Join(dir, Filename), false)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 6; i++ {
				if _, err = s.db.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=6"); err != nil {
				t.Fatal(err)
			}
			if err = s.SyncRoots(ctx, []string{"/fixture"}); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("INSERT INTO allocation_revisions VALUES(1,9)"); err != nil {
				t.Fatal(err)
			}
			if fail {
				if _, err = s.db.Exec("CREATE TABLE subtree_retirement(fixture INTEGER)"); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = r.MeasureDirectory(ctx, "/fixture"); err != nil {
				t.Fatal(err)
			}
			r.Close()
			w, err := OpenWriter(ctx, dir)
			if fail {
				if err == nil {
					w.Close()
					t.Fatal("conflicting migration succeeded")
				}
				r, err = OpenReader(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				var count int
				if err = r.db.QueryRow("SELECT count(*) FROM pragma_table_info('allocation_revisions') WHERE name='scan_revision'").Scan(&count); err != nil || count != 0 {
					t.Fatal("migration partially committed", count, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				var revision, scan int
				if err = w.db.QueryRow("SELECT revision,scan_revision FROM allocation_revisions").Scan(&revision, &scan); err != nil || revision != 9 || scan != 0 || w.schema != schemaVersion {
					t.Fatal(revision, scan, err)
				}
			}
		})
	}
}

func TestSubtreeRetirementStorageFixture(t *testing.T) {
	s, _ := retirementFixture(t, 10001)
	ctx := context.Background()
	if _, err := s.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := s.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retireMissingTree(t, s)
	var peakWAL int64
	batches := 0
	for ; batches < 2000; batches++ {
		worked, err := compactMaintenanceStep(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if info, err := os.Stat(s.path + "-wal"); err == nil && info.Size() > peakWAL {
			peakWAL = info.Size()
		}
		if !worked {
			break
		}
	}
	if batches == 2000 {
		t.Fatal("fixture maintenance did not drain")
	}
	assertRetiredTree(t, s)
	if _, err := s.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := s.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var freePages int64
	if err := s.db.QueryRow("PRAGMA freelist_count").Scan(&freePages); err != nil {
		t.Fatal(err)
	}
	if freePages == 0 {
		t.Fatal("fixture retirement did not free reusable pages")
	}
	t.Logf("synthetic identities=10001 maintenance_batches=%d database_before=%d database_after=%d peak_wal_bytes=%d reusable_pages=%d", batches, before.DatabaseBytes, after.DatabaseBytes, peakWAL, freePages)
}
