package state

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func drainAllocations(t *testing.T, s *Store) {
	t.Helper()
	for steps := 0; steps < 2000; steps++ {
		worked, err := compactMaintenanceStep(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
	t.Fatal("allocation maintenance did not drain")
}

func TestAllocationScopedHardlinksAndInvalidation(t *testing.T) {
	ctx := context.Background()
	s, _ := compactFixture(t, true)
	// Use a project root so ordinary files, two outer boundaries and a nested
	// boundary share an inode. Scoped totals must never be added together.
	if _, err := s.db.Exec("UPDATE roots SET path=?", []byte("/fixture")); err != nil {
		t.Fatal(err)
	}
	directory := func(path string) Entry { return Entry{Path: []byte(path), Kind: "directory"} }
	compactTreePass(t, s, 1, map[string][]Entry{
		".":                    {directory("node_modules"), directory("project"), compactFile("outside", "shared", 5)},
		"node_modules":         {directory("node_modules/nested"), compactFile("node_modules/a", "shared", 5)},
		"node_modules/nested":  {compactFile("node_modules/nested/b", "shared", 5)},
		"project":              {directory("project/node_modules")},
		"project/node_modules": {compactFile("project/node_modules/c", "shared", 5)},
	})
	for _, fixture := range []struct {
		path     string
		repeated int
	}{
		{"/fixture", 3}, {"/fixture/node_modules", 1}, {"/fixture/project/node_modules", 0},
	} {
		r, err := s.MeasureDirectory(ctx, fixture.path)
		if err != nil || r.AllocatedSizeSource != "cached_reduction" || r.AllocatedBytes == nil || *r.AllocatedBytes != 4096 || r.RepeatedInodes != fixture.repeated {
			t.Fatal(r, err)
		}
	}
	// An arbitrary nested directory uses the bounded fallback, never a cache
	// belonging to an enclosing or sibling scope.
	r, err := s.MeasureDirectory(ctx, "/fixture/node_modules/nested")
	if err != nil || r.AllocatedSizeSource != "bounded_identity_check" || *r.AllocatedBytes != 4096 {
		t.Fatal(r, err)
	}
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 2, false, directory("node_modules"), directory("project"), compactFile("outside", "new", 8))
	// Every root scope cache is invalid immediately, even when its own compact
	// directory has not been revisited yet.
	r, err = s.MeasureDirectory(ctx, "/fixture/node_modules")
	if err != nil || r.AllocatedSizeSource == "cached_reduction" {
		t.Fatal(r, err)
	}
	if worked, err := s.ReduceAllocations(ctx); err != nil || worked {
		t.Fatal("reduced unfinished scan", worked, err)
	}
}

func TestAllocationBatchRollbackResumeAndCleanup(t *testing.T) {
	ctx := context.Background()
	s, dir := compactFixture(t, true)
	for start := 0; start < 300; start += MaxBatchEntries {
		var files []Entry
		for i := start; i < min(start+MaxBatchEntries, 300); i++ {
			files = append(files, compactFile(fmt.Sprint(i), fmt.Sprint(i), 1))
		}
		compactBatch(t, s, 1, start+MaxBatchEntries >= 300, files...)
	}
	// Stop immediately after one committed reduction batch, then inject a
	// failure at cursor commit to prove contributions and progress roll back.
	for i := 0; i < 10; i++ {
		if _, err := compactMaintenanceStep(ctx, s); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			if count != MaxBatchEntries {
				t.Fatal("unbounded reduction batch", count)
			}
			break
		}
		if i == 9 {
			t.Fatal("reduction did not start")
		}
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_reduction BEFORE UPDATE ON allocation_cache BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReduceAllocations(ctx); err == nil {
		t.Fatal("injected failure accepted")
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&count); err != nil || count != MaxBatchEntries {
		t.Fatal(count, err)
	}
	if _, err := s.db.Exec("DROP TRIGGER fail_reduction"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ReduceAllocations(canceled); err == nil {
		t.Fatal("canceled maintenance succeeded")
	}
	if err := s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&count); err != nil || count != MaxBatchEntries {
		t.Fatal("cancellation changed committed scratch", count, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now(), time.Minute); err != nil || j != nil {
		t.Fatal("revisited before reduction finished", j, err)
	}
	off := false
	if _, err := s.ConfigureCompact(ctx, &off); err == nil {
		t.Fatal("switched mode during reduction")
	}
	drainAllocations(t, s)
	r := compactMeasure(t, s)
	if r.AllocatedSizeSource != "cached_reduction" || *r.AllocatedBytes != 300*4096 || r.RepeatedInodes != 0 {
		t.Fatal(r)
	}
	for _, table := range []string{"allocation_members", "allocation_identities"} {
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("scratch not retired", table, count, err)
		}
	}
	// Re-invalidation while scratch still exists must retire the old map before
	// building the new revision. Enqueue explicitly to simulate other writers'
	// scheduled inventory work (all mutations still use CommitScan).
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 2, true, compactFile("replacement", "replacement", 2))
	for i := 0; i < 100; i++ {
		if _, err := compactMaintenanceStep(ctx, s); err != nil {
			t.Fatal(err)
		}
		var identities int
		if err := s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&identities); err != nil {
			t.Fatal(err)
		}
		if identities > 0 {
			break
		}
		if i == 99 {
			t.Fatal("did not reach allocation scratch before re-invalidation")
		}
	}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Now()); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 3, true, compactFile("newer", "newer", 3))
	drainAllocations(t, s)
	r = compactMeasure(t, s)
	if r.AllocatedSizeSource != "cached_reduction" || *r.AllocatedBytes != 4096 || *r.LogicalBytes != 3 {
		t.Fatal(r)
	}
}

func TestAllocationConflictsUnknownOverflowAndFanout(t *testing.T) {
	ctx := context.Background()
	t.Run("fanout", func(t *testing.T) {
		s, _ := compactFixture(t, true)
		var dirs []Entry
		tree := map[string][]Entry{}
		for i := 0; i < 130; i++ {
			path := fmt.Sprintf("dir%03d", i)
			dirs = append(dirs, Entry{Path: []byte(path), Kind: "directory"})
			tree[path] = []Entry{compactFile(path+"/link", "shared", 1)}
		}
		compactBatch(t, s, 1, false, dirs[:128]...)
		compactBatch(t, s, 1, true, dirs[128:]...)
		compactTreePass(t, s, 1, tree)
		r := compactMeasure(t, s)
		if r.AllocatedSizeSource != "cached_reduction" || *r.AllocatedBytes != 4096 || r.RepeatedInodes != 129 {
			t.Fatal(r)
		}
	})
	t.Run("conflicting", func(t *testing.T) {
		s, _ := compactFixture(t, true)
		a, b := compactFile("a", "same", 1), compactFile("b", "same", 2)
		b.Allocated = 8192
		compactBatch(t, s, 1, true, a, b)
		drainAllocations(t, s)
		r := compactMeasure(t, s)
		if r.Status != "stale" || r.AllocatedSizeSource != "cached_reduction" || *r.AllocatedBytes != 8192 {
			t.Fatal(r)
		}
	})
	t.Run("unknown", func(t *testing.T) {
		s, _ := compactFixture(t, true)
		compactBatch(t, s, 1, true, compactFile("a", "", 1))
		drainAllocations(t, s)
		r := compactMeasure(t, s)
		if r.AllocatedBytes != nil || r.AllocatedSizeSource != "unknown" {
			t.Fatal(r)
		}
	})
	t.Run("overflow", func(t *testing.T) {
		s, _ := compactFixture(t, true)
		a, b := compactFile("a", "a", 1), compactFile("b", "b", 1)
		a.Allocated = math.MaxInt64
		compactBatch(t, s, 1, true, a, b)
		for i := 0; i < 10; i++ {
			_, err := compactMaintenanceStep(ctx, s)
			if err != nil {
				var count int
				if e := s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&count); e != nil || count != 0 {
					t.Fatal(count, e)
				}
				return
			}
		}
		t.Fatal("overflow accepted")
	})
}

func TestAllocationCrashRecovery(t *testing.T) {
	s, dir := compactFixture(t, true)
	var entries []Entry
	for i := 0; i < MaxBatchEntries; i++ {
		entries = append(entries, compactFile(fmt.Sprint(i), fmt.Sprint(i), 1))
	}
	compactBatch(t, s, 1, true, entries...)
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestAllocationCrashChild$")
	cmd.Env = append(os.Environ(), "RYDD_ALLOCATION_CRASH_DIR="+dir)
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
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !ready {
		t.Fatal("reduction child did not reach committed batch")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("reduction child was not killed", cmd.ProcessState)
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	drainAllocations(t, w)
	r := compactMeasure(t, w)
	if r.AllocatedSizeSource != "cached_reduction" || *r.AllocatedBytes != MaxBatchEntries*4096 {
		t.Fatal(r)
	}
}

func TestAllocationCrashChild(t *testing.T) {
	dir := os.Getenv("RYDD_ALLOCATION_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err = compactMaintenanceStep(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = s.db.QueryRow("SELECT count(*) FROM allocation_identities").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == MaxBatchEntries {
			fmt.Println("ready")
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	t.Fatal("no committed reduction batch")
}

func TestAllocationMigrationV5RollbackAndReadability(t *testing.T) {
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
			for i := 0; i < 5; i++ {
				if _, err = s.db.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=5"); err != nil {
				t.Fatal(err)
			}
			if err = s.SyncRoots(ctx, []string{"/fixture/node_modules"}); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("INSERT INTO compact_dirs VALUES(1,X'2e',1,5,1,0,0)"); err != nil {
				t.Fatal(err)
			}
			if fail {
				if _, err = s.db.Exec("CREATE TABLE allocation_members(fixture INTEGER)"); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if r.schema != 5 {
				t.Fatal(r.schema)
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
				if err = r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name IN ('allocation_cache','allocation_revisions')").Scan(&count); err != nil || count != 0 {
					t.Fatal("migration partially committed", count, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				var logical int64
				if err = w.db.QueryRow("SELECT logical FROM compact_dirs").Scan(&logical); err != nil || logical != 5 {
					t.Fatal(logical, err)
				}
				if w.schema != schemaVersion {
					t.Fatal(w.schema)
				}
			}
		})
	}
}

func TestAllocationCoverageBeyondLimit(t *testing.T) {
	s := directoryFixture(t)
	ctx := context.Background()
	on := true
	if _, err := s.ConfigureCompact(ctx, &on); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < DirectoryEntryLimit; i++ {
		if _, err := tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,X'2e','file',1,0,1,1,'d',?,1,10)`, []byte(fmt.Sprintf("z%05d", i)), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec("INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO allocation_cache(root_id,path) VALUES(1,X'2e')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	drainAllocations(t, s)
	var ready bool
	var examined int
	if err := s.db.QueryRow("SELECT ready,examined FROM allocation_cache").Scan(&ready, &examined); err != nil || !ready || examined != DirectoryEntryLimit+7 {
		t.Fatal(ready, examined, err)
	}
	r, err := s.MeasureDirectory(ctx, "/fixture")
	if err != nil || r.Truncated || r.CoverageSource != "cached_reduction" || r.AllocatedSizeSource != "cached_reduction" || r.LogicalBytes == nil || *r.LogicalBytes != DirectoryEntryLimit+1700 || r.EntriesExamined != DirectoryEntryLimit+7 {
		t.Fatal(r, err)
	}
	// Evidence beyond the old synchronous cap must still qualify the full size.
	if _, err = s.db.Exec("UPDATE entries SET skip_reason='fixture' WHERE path=?", []byte(fmt.Sprintf("z%05d", DirectoryEntryLimit-1))); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE allocation_revisions SET revision=revision+1"); err != nil {
		t.Fatal(err)
	}
	drainAllocations(t, s)
	r, err = s.MeasureDirectory(ctx, "/fixture")
	if err != nil || r.Truncated || r.Status != "partial" || r.SkippedEntries != 1 || r.CoverageSource != "cached_reduction" {
		t.Fatal(r, err)
	}
}
