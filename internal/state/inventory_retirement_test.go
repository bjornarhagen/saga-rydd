package state

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"testing"
	"time"
)

func detailedRetirementFixture(t *testing.T, count int) (*Store, string) {
	t.Helper()
	s, dir := compactFixture(t, false)
	if _, err := s.db.Exec("UPDATE roots SET path=?", []byte("/fixture")); err != nil {
		t.Fatal(err)
	}
	files := make([]Entry, count)
	for i := range files {
		files[i] = compactFile(fmt.Sprintf("gone/wide/%05d", i), fmt.Sprint(i), 1)
	}
	compactTreePass(t, s, 1, map[string][]Entry{
		".":         {retirementDirectory("gone"), retirementDirectory("gone2"), compactFile("outside", "outside", 7)},
		"gone":      {retirementDirectory("gone/wide")},
		"gone/wide": files,
		"gone2":     {compactFile("gone2/keep", "keep", 9)},
	})
	return s, dir
}

func detailedMissingPass(t *testing.T, s *Store) {
	t.Helper()
	if err := s.SeedInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	compactTreePass(t, s, 2, map[string][]Entry{
		".":     {retirementDirectory("gone2"), compactFile("outside", "outside", 7)},
		"gone2": {compactFile("gone2/keep", "keep", 9)},
	}, false)
}

func detailedRemaining(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM entries WHERE root_id=1 AND (path=? OR(path>=? AND path<?))", []byte("gone"), []byte("gone/"), []byte("gone0")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func drainDetailedRoot(t *testing.T, s *Store) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		before := retirementPayloadCount(t, s)
		step, err := s.RetireSubtreesForRoot(context.Background(), 1)
		if err != nil || !step.Eligible {
			t.Fatal(step, err)
		}
		if n := before - retirementPayloadCount(t, s); n > MaxBatchEntries {
			t.Fatal("unbounded payload retirement", n)
		}
		if !step.Remaining {
			return
		}
		if !step.Worked {
			t.Fatal("pending work did not advance")
		}
	}
	t.Fatal("detailed retirement did not drain")
}

func TestDetailedInventoryRetirementBoundedRestartAndHistory(t *testing.T) {
	ctx := context.Background()
	s, dir := detailedRetirementFixture(t, 10001)
	// Future non-rebuildable tables must not be included in fixed-table purge.
	for _, name := range []string{"actions", "restore_records", "consents"} {
		if _, err := s.db.Exec("CREATE TABLE " + name + "(root_id INTEGER,path BLOB,payload BLOB); INSERT INTO " + name + " VALUES(1,X'676f6e65',X'00ff0102')"); err != nil {
			t.Fatal(err)
		}
	}
	detailedMissingPass(t, s)
	if detailedRemaining(t, s) != 10003 {
		t.Fatal("committing absence swept detailed payload")
	}
	var err error
	var scan int64
	if err = s.db.QueryRow("SELECT scan_revision FROM allocation_revisions WHERE root_id=1").Scan(&scan); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		before := retirementPayloadCount(t, s)
		step, err := s.RetireSubtreesForRoot(ctx, 1)
		if err != nil || !step.Worked || !step.Eligible {
			t.Fatal(step, err)
		}
		if n := before - retirementPayloadCount(t, s); n > MaxBatchEntries {
			t.Fatal(n)
		}
		if n := detailedRemaining(t, s); n > 0 && n < 10003 {
			break
		}
		if i == 59 {
			t.Fatal("no partial retirement")
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	drainDetailedRoot(t, w)
	afterReport, err := w.MeasureDirectory(ctx, "/fixture")
	if err != nil || afterReport.LogicalBytes == nil || *afterReport.LogicalBytes != 16 {
		t.Fatal(afterReport, err)
	}
	if detailedRemaining(t, w) != 0 || countRows(t, w, "SELECT count(*) FROM entries WHERE path=X'676f6e65322f6b656570'") != 1 {
		t.Fatal("prefix neighbor or obsolete payload wrong")
	}
	var current int64
	if err = w.db.QueryRow("SELECT scan_revision FROM allocation_revisions WHERE root_id=1").Scan(&current); err != nil || current != scan {
		t.Fatal(current, scan, err)
	}
	for _, name := range []string{"actions", "restore_records", "consents"} {
		var payload []byte
		if err = w.db.QueryRow("SELECT payload FROM " + name).Scan(&payload); err != nil || !reflect.DeepEqual(payload, []byte{0, 255, 1, 2}) {
			t.Fatal(name, payload, err)
		}
	}
	if inserted, err := w.ScheduleInventoryRevisit(ctx, 1, time.Now(), InventoryRevisitInterval); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	if _, err = w.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, w, "PRAGMA freelist_count"); n == 0 {
		t.Fatal("no reusable pages freed")
	}
}

func TestDetailedInventoryRetirementPreservesPartialErrorAndDelayedWork(t *testing.T) {
	ctx := context.Background()
	s, _ := detailedRetirementFixture(t, 300)
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j := scanJob(t, s)
	if err := s.CommitScan(ctx, j, ScanBatch{Identity: "fixture", Generation: 2, Directory: retirementDirectory("."), Complete: false, Cursor: []byte("partial")}); err != nil {
		t.Fatal(err)
	}
	before := retirementPayloadCount(t, s)
	if step, err := s.RetireSubtreesForRoot(ctx, 1); err != nil || step.Eligible || step.Worked {
		t.Fatal(step, err)
	}
	j = scanJob(t, s)
	if err := s.CommitScan(ctx, j, ScanBatch{Fault: "fixture unavailable", Cursor: j.Cursor}); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.InventoryRetirementRootPending(ctx, 1); err != nil || ready {
		t.Fatal(ready, err)
	}
	if step, err := s.RetireSubtreesForRoot(ctx, 1); err != nil || step.Eligible || step.Worked {
		t.Fatal(step, err)
	}
	if retirementPayloadCount(t, s) != before || detailedRemaining(t, s) != 302 {
		t.Fatal("incomplete evidence removed payload")
	}
	var cursor []byte
	var fault string
	if err := s.db.QueryRow("SELECT cursor,last_error FROM jobs WHERE root_id=1").Scan(&cursor, &fault); err != nil || string(cursor) != "partial" || fault != "fixture unavailable" {
		t.Fatal(cursor, fault, err)
	}
}

func TestDetailedInventoryRetirementFreshRevisionAndReplacement(t *testing.T) {
	for _, kind := range []string{"directory", "file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := detailedRetirementFixture(t, 300)
			detailedMissingPass(t, s)
			for i := 0; i < 80; i++ {
				if _, err := s.RetireSubtreesForRoot(context.Background(), 1); err != nil {
					t.Fatal(err)
				}
				if n := detailedRemaining(t, s); n > 0 && n < 302 {
					break
				}
				if i == 79 {
					t.Fatal("no partial payload deletion")
				}
			}
			// Explicitly enqueue new source work to demonstrate revision fencing
			// even when it interrupts a partially retired historical prefix.
			if err := s.EnqueueJob(context.Background(), 1, ScanKind, []byte("."), time.Now()); err != nil {
				t.Fatal(err)
			}
			replacement := compactFile("gone", "replacement", 3)
			replacement.Kind = kind
			tree := map[string][]Entry{".": {replacement, retirementDirectory("gone2"), compactFile("outside", "outside", 7)}, "gone2": {compactFile("gone2/keep", "keep", 9)}}
			if kind == "directory" {
				tree["gone"] = []Entry{compactFile("gone/fresh", "fresh", 19)}
			}
			compactTreePass(t, s, 3, tree, false)
			drainDetailedRoot(t, s)
			var got string
			if err := s.db.QueryRow("SELECT kind FROM entries WHERE path=?", []byte("gone")).Scan(&got); err != nil || got != kind {
				t.Fatal(got, err)
			}
			if kind == "directory" && countRows(t, s, "SELECT count(*) FROM entries WHERE inode='fresh'") != 1 {
				t.Fatal("fresh child removed")
			}
			if countRows(t, s, "SELECT count(*) FROM entries WHERE path>=X'676f6e652f776964652f' AND path<X'676f6e652f7769646530'") != 0 {
				t.Fatal("old descendant survived")
			}
		})
	}
}

func TestDetailedInventoryRetirementRollbackCancellationAndRawPaths(t *testing.T) {
	ctx := context.Background()
	s, _ := detailedRetirementFixture(t, 1)
	// Byte paths are compared as BLOB scopes, including prefix neighbors.
	raw := string([]byte{'x', 255})
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactTreePass(t, s, 2, map[string][]Entry{".": {retirementDirectory(raw), compactFile(raw+"2", "neighbor", 1)}, raw: {compactFile(raw+"/old", "raw", 1)}}, false)
	drainDetailedRoot(t, s)
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactTreePass(t, s, 3, map[string][]Entry{".": {compactFile(raw+"2", "neighbor", 1)}}, false)
	before := retirementPayloadCount(t, s)
	canceled, cancel := context.WithCancel(ctx)
	step, err := s.retireSubtrees(canceled, 1, inventoryRetirementHooks{beforeCommit: cancel})
	if !errors.Is(err, context.Canceled) || step != (InventoryRetirementStep{}) || retirementPayloadCount(t, s) != before {
		t.Fatal(step, err)
	}
	if _, err = s.db.Exec("CREATE TRIGGER reject_retirement BEFORE DELETE ON subtree_reconcile BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		_, err = s.RetireSubtreesForRoot(ctx, 1)
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("rollback fixture did not refuse")
	}
	if _, err = s.db.Exec("DROP TRIGGER reject_retirement"); err != nil {
		t.Fatal(err)
	}
	drainDetailedRoot(t, s)
	var n int
	if err = s.db.QueryRow("SELECT count(*) FROM entries WHERE path=?", []byte(raw+"2")).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM entries WHERE path=? OR(path>=? AND path<?)", []byte(raw), []byte(raw+"/"), []byte(raw+"0")).Scan(&n); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestInventoryRetirementPagesBoundsBusyRootsAndCorruptRefusal(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	if _, err := s.db.Exec("UPDATE roots SET enabled=0"); err != nil {
		t.Fatal(err)
	}
	for id := 3; id <= 260; id++ {
		if _, err := s.db.Exec("INSERT INTO roots(id,path,enabled) VALUES(?,?,?)", id, []byte(fmt.Sprintf("/generated/%03d", id)), id >= 256); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{256, 257, 258} {
		if _, err := s.db.Exec("INSERT INTO allocation_revisions VALUES(?,1,1)", id); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec("INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(?,X'2e',1)", id); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnqueueJob(ctx, 258, ScanKind, []byte("."), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	p, err := s.NextInventoryRetirementPage(ctx, 0)
	if err != nil || p.RootID != 0 || !p.More || p.Cursor != 128 {
		t.Fatal(p, err)
	}
	p, err = s.NextInventoryRetirementPage(ctx, p.Cursor)
	if err != nil || p.RootID != 256 || !p.More || p.Cursor != 256 {
		t.Fatal(p, err)
	}
	p, err = s.NextInventoryRetirementPage(ctx, p.Cursor)
	if err != nil || p.RootID != 257 || !p.More {
		t.Fatal(p, err)
	}
	p, err = s.NextInventoryRetirementPage(ctx, p.Cursor)
	if err != nil || p.RootID != 0 || p.More || p.Cursor != 0 {
		t.Fatal(p, err)
	}
	if step, err := s.RetireSubtreesForRoot(ctx, 258); err != nil || step.Eligible || step.Worked || !step.Remaining {
		t.Fatal(step, err)
	}
	if _, err = s.db.Exec("DELETE FROM allocation_revisions WHERE root_id=256"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.NextInventoryRetirementPage(ctx, 128); !errors.Is(err, ErrInventoryRetirementCorrupt) {
		t.Fatal(err)
	}
	if _, err = s.RetireSubtreesForRoot(ctx, 256); !errors.Is(err, ErrInventoryRetirementCorrupt) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if p, err := s.NextInventoryRetirementPage(canceled, 0); !errors.Is(err, context.Canceled) || p != (InventoryRetirementPage{}) {
		t.Fatal(p, err)
	}
	if _, err = s.NextInventoryRetirementPage(ctx, -1); !errors.Is(err, ErrInventoryRetirementInput) {
		t.Fatal(err)
	}
	if _, err = s.RetireSubtreesForRoot(ctx, 0); !errors.Is(err, ErrInventoryRetirementInput) {
		t.Fatal(err)
	}
}

func TestDetailedInventoryRetirementSIGKILLRecovery(t *testing.T) {
	s, dir := detailedRetirementFixture(t, 1001)
	detailedMissingPass(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestDetailedInventoryRetirementSIGKILLChild$")
	cmd.Env = append(os.Environ(), "RYDD_DETAILED_RETIREMENT_CHILD="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatal("child not ready")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal(cmd.ProcessState)
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	drainDetailedRoot(t, w)
	if detailedRemaining(t, w) != 0 {
		t.Fatal("obsolete payload retained after recovery")
	}
}

func TestInventoryRetirementCompletesInheritedDetailedModeCleanup(t *testing.T) {
	ctx := context.Background()
	s, _ := compactFixture(t, true)
	var old []Entry
	for i := 0; i < 300; i++ {
		old = append(old, compactFile(fmt.Sprintf("old-%03d", i), fmt.Sprint(i), 1))
	}
	compactBatch(t, s, 1, false, old[:128]...)
	compactBatch(t, s, 1, false, old[128:256]...)
	compactBatch(t, s, 1, true, old[256:]...)
	drainAllocations(t, s)
	if countRows(t, s, "SELECT count(*) FROM allocation_cache WHERE phase='done'") != 1 {
		t.Fatal("missing old complete cache")
	}
	off := false
	if _, err := s.ConfigureCompact(ctx, &off); err != nil {
		t.Fatal(err)
	}
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	compactBatch(t, s, 2, true, compactFile("current", "current", 19))
	if countRows(t, s, "SELECT count(*) FROM compact_retirement") == 0 || countRows(t, s, "SELECT count(*) FROM allocation_cache") != 1 {
		t.Fatal("no inherited cleanup work")
	}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	before := retirementPayloadCount(t, s)
	if step, err := s.RetireInventoryForRoot(ctx, 1); err != nil || step.Eligible || step.Worked || !step.Remaining || before != retirementPayloadCount(t, s) {
		t.Fatal(step, err)
	}
	// Finish only the generated blocking job; the saved payload remains guarded
	// until this transition, then every maintenance step stays bounded.
	j, err := s.ClaimJob(ctx, []string{ScanKind}, time.Now().Add(2*time.Hour), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if err = s.FinishJob(ctx, *j, true, nil, time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	p, err := s.NextInventoryRetirementPage(ctx, 0)
	if err != nil || p.RootID != 1 {
		t.Fatal(p, err)
	}
	for i := 0; i < 100; i++ {
		before := retirementPayloadCount(t, s)
		step, err := s.RetireInventoryForRoot(ctx, 1)
		if err != nil || !step.Eligible || !step.Worked {
			t.Fatal(step, err)
		}
		if removed := before - retirementPayloadCount(t, s); removed > MaxBatchEntries {
			t.Fatal("more than one bounded mutation", removed)
		}
		if !step.Remaining {
			break
		}
		if i == 99 {
			t.Fatal("inherited work did not drain")
		}
	}
	if countRows(t, s, "SELECT count(*) FROM entries WHERE inode='current'") != 1 || countRows(t, s, "SELECT count(*) FROM compact_inodes") != 0 || countRows(t, s, "SELECT count(*) FROM allocation_cache") != 0 {
		t.Fatal("current entry lost or old cache retained")
	}
	if inserted, err := s.ScheduleInventoryRevisit(ctx, 1, time.Now(), InventoryRevisitInterval); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
}

func TestDetailedInventoryRetirementSIGKILLChild(t *testing.T) {
	dir := os.Getenv("RYDD_DETAILED_RETIREMENT_CHILD")
	if dir == "" {
		t.Skip("generated child only")
	}
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 100; i++ {
		if _, err = s.RetireSubtreesForRoot(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if n := detailedRemaining(t, s); n > 0 && n < 1003 {
			fmt.Println("ready")
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	t.Fatal("child did not reach partial retirement")
}
