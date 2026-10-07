package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type hashStoreTestFixture struct {
	store                             *HashStore
	source                            *state.Store
	scanner                           *Scanner
	base, stateDir, root, inventoryID string
	expected                          []state.SameSizeFile
	data                              [][]byte
}

func hashStoreFixture(t *testing.T, contents ...[]byte) *hashStoreTestFixture {
	t.Helper()
	scanner, targets := sampleFixture(t, contents...)
	return hashStoreFixtureFromFiles(t, scanner, targets, contents)
}

func hashStoreFixtureFromFiles(t *testing.T, scanner *Scanner, targets []SavedFileTarget, contents [][]byte) *hashStoreTestFixture {
	t.Helper()
	ctx := context.Background()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base = filepath.Join(base, "hash-data")
	stateBase, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &hashStoreTestFixture{scanner: scanner, base: base, stateDir: filepath.Join(stateBase, "source-state"), root: string(targets[0].Root.PathBytes), data: contents}
	f.source, err = state.OpenWriter(ctx, f.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.source.Close() })
	if err = f.source.SyncRoots(ctx, []string{f.root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = f.source.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = f.source.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for count := 0; count < 16; count++ {
		job, e := f.source.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if job == nil {
			finished = true
			break
		}
		batch, e := scanner.Next(ctx, *job)
		if e != nil || batch.Fault != "" {
			t.Fatal(batch, e)
		}
		if e = f.source.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("synthetic scan did not finish")
	}
	db, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.QueryRow("SELECT token FROM inventory_identity WHERE singleton=1").Scan(&f.inventoryID); err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		relative, e := filepath.Rel(f.root, string(target.File.PathBytes))
		if e != nil {
			t.Fatal(e)
		}
		file := target.File
		var observed, modified int64
		err = db.QueryRow(`SELECT id,size,allocated,observed_at_ns,mtime_ns,ctime_ns,device,inode,generation FROM entries WHERE root_id=1 AND path=?`, []byte(relative)).Scan(&file.ID, &file.Size, &file.Allocated, &observed, &modified, &file.ChangedNS, &file.Device, &file.Inode, &file.Generation)
		if err != nil {
			t.Fatal(err)
		}
		file.ObservedAt, file.ModifiedAt = time.Unix(0, observed).UTC(), time.Unix(0, modified).UTC()
		file.RootID = 1
		file.ParentPass = "observed_in_completed_parent_pass"
		f.expected = append(f.expected, file)
	}
	f.store, err = OpenHashWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	f.store.now = func() time.Time { return time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC) }
	if _, err = f.store.CreateSelection(ctx, f.source, f.inventoryID, f.expected); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *hashStoreTestFixture) reopen(t *testing.T) {
	t.Helper()
	clock := f.store.now
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenHashWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.store = s
	f.store.now = clock
}
func hashStoreSnapshot(t *testing.T, s *HashStore) HashSnapshot {
	t.Helper()
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Source != "saved_hash_observations" || snapshot.Contract != FileHashContract || snapshot.ProvenanceVerified || snapshot.ContentVerified || snapshot.CurrentStateVerified || snapshot.DuplicatesVerified || snapshot.Executable || snapshot.EstimatedReclaimableBytes != nil {
		t.Fatal("stronger saved hash claim", snapshot)
	}
	return snapshot
}
func hashStoreRun(t *testing.T, f *hashStoreTestFixture, allowance, limit int64) HashRunResult {
	t.Helper()
	r, err := f.store.RunNext(context.Background(), f.source, f.scanner, allowance, limit)
	if err != nil {
		t.Fatal(r, err)
	}
	if r.Usage.ReadBytes > r.Usage.RequestedBytes || r.Usage.RequestedBytes > r.ReservedBytes {
		t.Fatal("unreserved reads", r)
	}
	requireFullHashClaims(t, r.Progress)
	return r
}

func TestHashStoreFairContinuationAcrossReopen(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129), fullHashContents(65), []byte("fixture"))
	for _, want := range []struct {
		id     string
		offset int64
	}{{"1", 64}, {"2", 64}, {"3", 7}} {
		r := hashStoreRun(t, f, 64, 4096)
		if r.WorkID != want.id || r.DurableOffset != want.offset {
			t.Fatal("round robin changed", r, want)
		}
		f.reopen(t)
	}
	for _, want := range []struct {
		id     string
		offset int64
	}{{"1", 128}, {"2", 65}, {"1", 129}} {
		r := hashStoreRun(t, f, 64, 4096)
		if r.WorkID != want.id || r.DurableOffset != want.offset {
			t.Fatal(r, want)
		}
		f.reopen(t)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	for i, w := range snapshot.Work {
		if w.Status != "complete" || w.SHA256 != fmt.Sprintf("%x", sha256.Sum256(f.data[i])) || w.DurableOffset != int64(len(f.data[i])) {
			t.Fatal("wrong resumed digest", w)
		}
	}
	if snapshot.Budget.TotalReservedBytes != 201 || snapshot.Budget.TotalRequestedBytes != 201 || snapshot.Budget.TotalReadBytes != 201 {
		t.Fatal(snapshot.Budget)
	}
	if err := f.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	saved := hashStoreSnapshot(t, reader)
	if saved.Work[0].SHA256 != snapshot.Work[0].SHA256 {
		t.Fatal("offline snapshot changed")
	}
	for _, w := range snapshot.Work {
		w.PathBytes[0] = 'x'
	}
	if !bytes.Equal(hashStoreSnapshot(t, f.store).Work[0].PathBytes, f.expected[0].PathBytes) {
		t.Fatal("snapshot paths aliased store state")
	}
}

func TestHashStoreCancellationSettlesUsageWithoutProgress(t *testing.T) {
	for _, phase := range []string{"after_reserve", "after_read", "before_publication"} {
		t.Run(phase, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(256))
			hashStoreRun(t, f, 64, 4096)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashStoreHooks{}
			switch phase {
			case "after_reserve":
				hooks.afterReserve = cancel
			case "after_read":
				hooks.file.afterRead = func(int) { cancel() }
			case "before_publication":
				hooks.beforeSettleCommit = cancel
			}
			r, err := f.store.runNext(ctx, f.source, f.scanner, 64, 4096, hooks)
			if !errors.Is(err, context.Canceled) || r.DurableOffset != 64 || r.Progress.Offset != 64 || r.Progress.SHA256 != "" || r.ReservedBytes != 64 {
				t.Fatal("canceled publication advanced", r, err)
			}
			wantRead := int64(64)
			if phase == "after_reserve" {
				wantRead = 0
			}
			if r.Usage.ReadBytes != wantRead {
				t.Fatal(r.Usage)
			}
			f.reopen(t)
			snapshot := hashStoreSnapshot(t, f.store)
			a := snapshot.Work[0].LatestAttempt
			if snapshot.Work[0].DurableOffset != 64 || a.Status != "settled" || a.RequestedBytes == nil || *a.RequestedBytes != wantRead || snapshot.Budget.TotalReservedBytes != 128 || snapshot.Budget.TotalReadBytes != 64+wantRead {
				t.Fatal(snapshot)
			}
			if r = hashStoreRun(t, f, 64, 4096); r.DurableOffset != 128 {
				t.Fatal("canceled digest state was committed", r)
			}
		})
	}
	t.Run("before_reservation_commit", func(t *testing.T) {
		f := hashStoreFixture(t, fullHashContents(128))
		ctx, cancel := context.WithCancel(context.Background())
		r, err := f.store.runNext(ctx, f.source, f.scanner, 64, 4096, hashStoreHooks{beforeReserveCommit: cancel})
		if !errors.Is(err, context.Canceled) || r.Usage.ReadBytes != 0 || r.ReservedBytes != 0 {
			t.Fatal(r, err)
		}
		snapshot := hashStoreSnapshot(t, f.store)
		if snapshot.Budget != nil || snapshot.Work[0].LatestAttempt != nil || snapshot.Work[0].Status != "pending" {
			t.Fatal("uncommitted reservation persisted", snapshot)
		}
	})
}

func TestHashStoreDurableQuantumAndAffordableTails(t *testing.T) {
	for _, cap := range []int64{1, 63} {
		t.Run(fmt.Sprint(cap), func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			for day := 0; day < 2; day++ {
				f.store.now = func() time.Time { return time.Date(2026, 10, 7+day, 12, 0, 0, 0, time.UTC) }
				r, err := f.store.RunNext(context.Background(), f.source, f.scanner, cap, cap)
				if !errors.Is(err, ErrHashDeferred) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 {
					t.Fatal(r, err)
				}
				f.reopen(t)
			}
			if hashStoreSnapshot(t, f.store).Budget != nil {
				t.Fatal("nonadvancing reads were charged")
			}
		})
	}
	t.Run("64_and_day_rollover", func(t *testing.T) {
		f := hashStoreFixture(t, fullHashContents(65))
		r := hashStoreRun(t, f, 64, 64)
		if r.DurableOffset != 64 {
			t.Fatal(r)
		}
		f.reopen(t)
		if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 64); !errors.Is(err, ErrHashDeferred) {
			t.Fatal(err)
		}
		f.store.now = func() time.Time { return time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC) }
		r = hashStoreRun(t, f, 64, 64)
		if r.DurableOffset != 65 || r.ReservedBytes != 1 || r.Budget.ReservedBytes != 1 || r.Budget.TotalReservedBytes != 65 {
			t.Fatal(r)
		}
	})
	t.Run("unaligned_terminal", func(t *testing.T) {
		f := hashStoreFixture(t, fullHashContents(65))
		r := hashStoreRun(t, f, 65, 65)
		if r.Status != "hash_observed" || r.DurableOffset != 65 || r.ReservedBytes != 65 {
			t.Fatal(r)
		}
		f.reopen(t)
		if hashStoreSnapshot(t, f.store).Work[0].SHA256 != fmt.Sprintf("%x", sha256.Sum256(f.data[0])) {
			t.Fatal("terminal digest did not survive")
		}
	})
	t.Run("small_tail_bypasses_head", func(t *testing.T) {
		f := hashStoreFixture(t, fullHashContents(129), []byte("x"))
		r := hashStoreRun(t, f, 65, 65)
		if r.ReservedBytes != 64 || r.WorkID != "1" {
			t.Fatal(r)
		}
		r = hashStoreRun(t, f, 65, 65)
		if r.WorkID != "2" || r.ReservedBytes != 1 || r.Status != "hash_observed" {
			t.Fatal("affordable tail blocked", r)
		}
	})
}

func TestHashStoreCurrentInventoryChangesRefuseSuffix(t *testing.T) {
	for _, kind := range []string{"root_revision", "ancestor", "file", "disabled", "rebuild"} {
		t.Run(kind, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			hashStoreRun(t, f, 64, 4096)
			db, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var command string
			switch kind {
			case "root_revision":
				command = "UPDATE allocation_revisions SET revision=revision+1"
			case "ancestor":
				command = "UPDATE entries SET mtime_ns=mtime_ns+1 WHERE kind='directory'"
			case "file":
				command = "UPDATE entries SET ctime_ns=ctime_ns+1 WHERE kind='file'"
			case "disabled":
				command = "UPDATE roots SET enabled=0"
			case "rebuild":
				command = "DROP TRIGGER inventory_identity_no_update; UPDATE inventory_identity SET token=lower(hex(randomblob(32)))"
			}
			if _, err = db.Exec(command); err != nil {
				t.Fatal(err)
			}
			r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096)
			if !errors.Is(err, ErrHashInventoryChanged) || r.Usage.RequestedBytes != 0 || r.ReservedBytes != 0 || r.DurableOffset != 64 {
				t.Fatal("changed saved bindings were remapped", r, err)
			}
			snapshot := hashStoreSnapshot(t, f.store)
			if snapshot.Work[0].Status != "invalidated" || snapshot.Budget.TotalReservedBytes != 64 || snapshot.Work[0].SHA256 != "" {
				t.Fatal(snapshot)
			}
		})
	}
	t.Run("change_during_read", func(t *testing.T) {
		f := hashStoreFixture(t, fullHashContents(129))
		hashStoreRun(t, f, 64, 4096)
		db, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{file: fileHashHooks{afterRead: func(int) {
			if _, e := db.Exec("UPDATE allocation_revisions SET revision=revision+1"); e != nil {
				t.Fatal(e)
			}
		}}})
		if !errors.Is(err, ErrHashInventoryChanged) || r.Usage.ReadBytes != 64 || r.DurableOffset != 64 || r.Progress.SHA256 != "" {
			t.Fatal(r, err)
		}
	})
}

func TestHashStoreUnknownSettlementRecoveryAndClock(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129), []byte("small"))
	if _, err := f.store.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 128)
	if !errors.Is(err, ErrHashRecoveryRequired) || r.Usage.ReadBytes != 64 {
		t.Fatal(r, err)
	}
	if _, err = f.store.RunNext(context.Background(), f.source, f.scanner, 64, 128); !errors.Is(err, ErrHashRecoveryRequired) {
		t.Fatal("uncertain dispatcher continued", err)
	}
	if _, err = f.store.db.Exec("DROP TRIGGER fail_checkpoint"); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	snapshot := hashStoreSnapshot(t, f.store)
	a := snapshot.Work[0].LatestAttempt
	if a.Status != "interrupted_unknown" || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil || snapshot.Work[0].DurableOffset != 0 || snapshot.Budget.UnknownReservedBytes != 64 || snapshot.Budget.TotalReservedBytes != 64 || snapshot.Budget.TotalReadBytes != 0 {
		t.Fatal("unknown read was refunded or invented", snapshot)
	}
	r = hashStoreRun(t, f, 64, 128)
	if r.WorkID != "2" || r.ReservedBytes != 5 {
		t.Fatal("crash recovery reset fair ordering", r)
	}
	f.store.now = func() time.Time { return time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC) }
	r, err = f.store.RunNext(context.Background(), f.source, f.scanner, 64, 128)
	if !errors.Is(err, ErrHashDeferred) || r.Code != "clock_rollback" || r.ReservedBytes != 0 {
		t.Fatal(r, err)
	}
	f.store.now = func() time.Time { return time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC) }
	r = hashStoreRun(t, f, 64, 64)
	if r.WorkID != "1" || r.DurableOffset != 64 || r.Budget.ReservedBytes != 64 || r.Budget.UnknownReservedBytes != 0 || r.Budget.TotalUnknownReservedBytes != 64 || r.Budget.TotalReservedBytes != 133 {
		t.Fatal("old charge moved into new day", r)
	}
}

func TestHashStoreSelectionOwnershipAndBounds(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(65))
	before := hashStoreSnapshot(t, f.store)
	again, err := f.store.CreateSelection(context.Background(), f.source, f.inventoryID, f.expected)
	if err != nil || again.SelectionID != before.SelectionID {
		t.Fatal(again, err)
	}
	if other, err := OpenHashWriter(context.Background(), f.base); !errors.Is(err, localfs.ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal(err)
	}
	for _, values := range [][2]int64{{0, 64}, {FileHashStepByteLimit + 1, 64}, {64, 0}, {64, (1 << 50) + 1}} {
		if _, err = f.store.RunNext(context.Background(), f.source, f.scanner, values[0], values[1]); err == nil {
			t.Fatal("invalid budget accepted", values)
		}
	}
	if _, err = f.store.db.Exec("UPDATE hash_meta SET next_order=?", math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.RunNext(context.Background(), f.source, f.scanner, 64, 64); !errors.Is(err, ErrHashStoreCorrupt) {
		t.Fatal("fair counter overflow", err)
	}
	if hashStoreSnapshot(t, f.store).Budget != nil {
		t.Fatal("overflow reserved bytes")
	}
	t.Run("own_base", func(t *testing.T) {
		g := hashStoreFixture(t, []byte("scope"))
		newBase := g.root
		if err := os.Chmod(newBase, 0700); err != nil {
			t.Fatal(err)
		}
		store, err := OpenHashWriter(context.Background(), newBase)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if _, err = store.CreateSelection(context.Background(), g.source, g.inventoryID, g.expected); err == nil {
			t.Fatal("own storage subtree selected")
		}
		if snapshot := hashStoreSnapshot(t, store); len(snapshot.Work) != 0 {
			t.Fatal(snapshot)
		}
	})
	t.Run("main_file_link", func(t *testing.T) {
		g := hashStoreFixture(t, []byte("scope"))
		if err := g.store.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(g.base, "hashes", hashStoreFilename)
		if err := os.Link(path, path+".alias"); err != nil {
			t.Fatal(err)
		}
		if opened, err := OpenHashReader(context.Background(), g.base); err == nil {
			opened.Close()
			t.Fatal("hardlinked database accepted")
		}
	})
	t.Run("selection_change", func(t *testing.T) {
		g := hashStoreFixture(t, []byte("one"), []byte("two"))
		if _, err := g.store.CreateSelection(context.Background(), g.source, g.inventoryID, g.expected[:1]); !errors.Is(err, ErrHashSelectionConflict) {
			t.Fatal(err)
		}
	})
	t.Run("oversized_checkpoint", func(t *testing.T) {
		g := hashStoreFixture(t, []byte("x"))
		if _, err := g.store.db.Exec("PRAGMA ignore_check_constraints=1; UPDATE hash_work SET checkpoint=?", bytes.Repeat([]byte{'x'}, 8193)); err != nil {
			t.Fatal(err)
		}
		if _, err := g.store.Snapshot(context.Background()); !errors.Is(err, ErrHashStoreCorrupt) {
			t.Fatal(err)
		}
	})
}

func TestHashStoreAuthoritativeUnixPaths(t *testing.T) {
	for _, name := range []string{"line\nquote\"雪", string([]byte{'r', 0xff, 'w'})} {
		t.Run(fmt.Sprintf("%x", name), func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("abc"))
			path := filepath.Join(filepath.Dir(string(targets[0].File.PathBytes)), name)
			err := os.Rename(string(targets[0].File.PathBytes), path)
			requireSampleFixtureFilename(t, name, err)
			targets = captureSampleTargets(t, s, string(targets[0].Root.PathBytes), []string{path})
			f := hashStoreFixtureFromFiles(t, s, targets, [][]byte{[]byte("abc")})
			r := hashStoreRun(t, f, 64, 64)
			if !bytes.Equal(r.Progress.PathBytes, []byte(path)) || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("abc"))) {
				t.Fatal("lossy authoritative path", r)
			}
			f.reopen(t)
			if !bytes.Equal(hashStoreSnapshot(t, f.store).Work[0].PathBytes, []byte(path)) {
				t.Fatal("path bytes changed on restore")
			}
		})
	}
}

func TestHashStorePendingQueryUsesIndex(t *testing.T) {
	f := hashStoreFixture(t, []byte("a"), []byte("b"))
	rows, err := f.store.db.Query("EXPLAIN QUERY PLAN SELECT id FROM hash_work INDEXED BY hash_work_pending WHERE status='pending' ORDER BY ready_order,id LIMIT 21")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	matched := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "hash_work_pending") {
			matched = true
		}
		if strings.Contains(detail, "TEMP B-TREE") {
			t.Fatal("round robin sorts unbounded work", detail)
		}
	}
	if !matched {
		t.Fatal("round robin missed pending index")
	}
}

func TestHashStoreLargerThanDayAndStepContinuesWithSmallPeer(t *testing.T) {
	data := fullHashContents(int(FileHashStepByteLimit) + 129)
	f := hashStoreFixture(t, data, []byte("small"))
	r := hashStoreRun(t, f, FileHashStepByteLimit, FileHashStepByteLimit)
	if r.WorkID != "1" || r.DurableOffset != FileHashStepByteLimit {
		t.Fatal(r)
	}
	f.reopen(t)
	if r, err := f.store.RunNext(context.Background(), f.source, f.scanner, FileHashStepByteLimit, FileHashStepByteLimit); !errors.Is(err, ErrHashDeferred) || r.ReservedBytes != 0 {
		t.Fatal(r, err)
	}
	f.store.now = func() time.Time { return time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC) }
	r = hashStoreRun(t, f, FileHashStepByteLimit, FileHashStepByteLimit)
	if r.WorkID != "2" || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("small"))) {
		t.Fatal("large continuation starved peer", r)
	}
	f.reopen(t)
	r = hashStoreRun(t, f, FileHashStepByteLimit, FileHashStepByteLimit)
	if r.WorkID != "1" || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || r.DurableOffset != int64(len(data)) || r.Budget.TotalReservedBytes != int64(len(data)+5) || r.Budget.TotalReadBytes != int64(len(data)+5) {
		t.Fatal("large prefix restarted or digest differed", r)
	}
}

func TestHashStoreRestoredLiveEvidenceInvalidation(t *testing.T) {
	for _, kind := range []string{"edit", "restored_mtime", "file_replace", "parent_replace", "root_symlink", "excluded", "closed_scanner"} {
		t.Run(kind, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			hashStoreRun(t, f, 64, 4096)
			f.reopen(t)
			if kind == "excluded" {
				f.scanner.excludes = append(f.scanner.excludes, string(f.expected[0].PathBytes))
			} else if kind == "closed_scanner" {
				f.scanner.Close()
			} else {
				mutateFullHashFixture(t, kind, string(f.expected[0].PathBytes), f.root, f.expected[0].ModifiedAt)
			}
			r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096)
			if err == nil || r.Usage.RequestedBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("restored stale evidence read source", r, err)
			}
			snapshot := hashStoreSnapshot(t, f.store)
			if snapshot.Work[0].DurableOffset != 64 || snapshot.Work[0].SHA256 != "" {
				t.Fatal(snapshot)
			}
		})
	}
}

func TestHashStoreCorruptAccountingFailsBeforeRead(t *testing.T) {
	for _, kind := range []string{"missing_meta", "missing_budget", "missing_attempt", "large_status", "large_day", "wrong_integer"} {
		t.Run(kind, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			hashStoreRun(t, f, 64, 4096)
			var query string
			var args []any
			switch kind {
			case "missing_meta":
				query = "DELETE FROM hash_meta"
			case "missing_budget":
				query = "DELETE FROM hash_budget"
			case "missing_attempt":
				query = "DELETE FROM hash_attempt"
			case "large_status":
				query = "UPDATE hash_work SET status=?"
				args = []any{strings.Repeat("x", 1<<20)}
			case "large_day":
				query = "UPDATE hash_budget SET day=?"
				args = []any{strings.Repeat("x", 1<<20)}
			case "wrong_integer":
				query = "UPDATE hash_budget SET reserved_bytes=?"
				args = []any{bytes.Repeat([]byte{'x'}, 1<<20)}
			}
			if _, err := f.store.db.Exec("PRAGMA ignore_check_constraints=1"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.db.Exec(query, args...); err != nil {
				t.Fatal(err)
			}
			called := false
			r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{file: fileHashHooks{afterOpen: func() { called = true }}})
			if !errors.Is(err, ErrHashStoreCorrupt) || called || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 {
				t.Fatal("damaged accounting dispatched bytes", kind, r, err)
			}
		})
	}
}

func TestHashStoreSequenceOverflowFailsBeforeRead(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	snapshot, record, err := f.store.readHashSnapshot(context.Background(), f.store.db)
	if err != nil {
		t.Fatal(err)
	}
	w, err := readHashWork(context.Background(), f.store.db, record, snapshot.SelectionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hashTargetDigest(record.Targets[0])
	if err != nil {
		t.Fatal(err)
	}
	blob, err := encodeHashCheckpoint(hashCheckpointBinding{StoreID: record.StoreID, SelectionID: snapshot.SelectionID, WorkID: "1", Sequence: math.MaxInt64, TargetDigest: digest}, record.Targets[0], w.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.db.Exec("UPDATE hash_work SET sequence=?,checkpoint=?", math.MaxInt64, blob); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.db.Exec("INSERT INTO hash_attempt VALUES(1,?,?,0,64,'2026-10-07','settled',0,0,0)", strings.Repeat("a", 64), int64(math.MaxInt64-1)); err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := f.store.now()
	if err = writeHashBudget(context.Background(), tx, HashBudget{Day: "2026-10-07", MaxNow: now, ReservedBytes: 64, TotalReservedBytes: 64}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	called := false
	r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{file: fileHashHooks{afterOpen: func() { called = true }}})
	if !errors.Is(err, ErrHashStoreCorrupt) || called || r.ReservedBytes != 0 {
		t.Fatal(r, err)
	}
	if hashStoreSnapshot(t, f.store).Budget.TotalReservedBytes != 64 {
		t.Fatal("overflow charged work")
	}
}

func TestHashStorePrivateAliasAfterOpenRefusesReads(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	alias := filepath.Join(f.root, "project", "private-alias")
	if err := os.Link(filepath.Join(f.base, "hashes", hashStoreFilename), alias); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(alias)
	// The source scanner was constructed without privatePaths, so the store's
	// independent ownership/alias guard must protect its own database.
	if err := f.source.EnqueueJob(context.Background(), 1, state.ScanKind, []byte("."), time.Now()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		job, err := f.source.ClaimJob(context.Background(), []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			break
		}
		batch, err := f.scanner.Next(context.Background(), *job)
		if err != nil || batch.Fault != "" {
			t.Fatal(batch, err)
		}
		if err = f.source.CommitScan(context.Background(), *job, batch); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.CreateSelection(context.Background(), f.source, f.inventoryID, f.expected); err == nil {
		t.Fatal("hardlinked private file accepted during capture")
	}
	called := false
	r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{file: fileHashHooks{afterOpen: func() { called = true }}})
	if err == nil || called || r.Usage.RequestedBytes != 0 || r.ReservedBytes != 0 {
		t.Fatal("private storage alias dispatched", r, err)
	}
}

func TestHashStorePrivateAliasAfterReservationSettlesZeroUsage(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	alias := filepath.Join(f.root, "project", "late-private-alias")
	defer os.Remove(alias)
	opened := false
	r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{afterReserve: func() {
		if e := os.Link(filepath.Join(f.base, "hashes", hashStoreFilename), alias); e != nil {
			t.Fatal(e)
		}
	}, file: fileHashHooks{afterOpen: func() { opened = true }}})
	if err == nil || opened || r.Usage.ReadBytes != 0 || r.Usage.RequestedBytes != 0 || r.ReservedBytes != 64 || r.DurableOffset != 0 || r.Code != "hash_storage_changed" || r.Progress.SHA256 != "" {
		t.Fatal("late private alias was read", r, err)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	a := snapshot.Work[0].LatestAttempt
	if snapshot.Work[0].Status != "invalidated" || a.RequestedBytes == nil || *a.RequestedBytes != 0 || a.ReadBytes == nil || *a.ReadBytes != 0 || snapshot.Budget.TotalReservedBytes != 64 || snapshot.Budget.TotalReadBytes != 0 {
		t.Fatal("late storage guard lost its charged allowance", snapshot)
	}
}

func TestHashStoreFailedCompletionDoesNotPublishDigest(t *testing.T) {
	f := hashStoreFixture(t, []byte("abc"))
	if _, err := f.store.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 64)
	if !errors.Is(err, ErrHashRecoveryRequired) || r.Progress.SHA256 != "" || r.Progress.Offset != 0 || r.DurableOffset != 0 || r.Usage.ReadBytes != 3 || r.ReservedBytes != 3 {
		t.Fatal("failed publication returned tentative digest", r, err)
	}
}

type hashInterleaveQuery struct {
	*sql.Tx
	once       bool
	interleave func()
}

func (q *hashInterleaveQuery) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if !q.once && strings.Contains(query, " FROM hash_attempt ") {
		q.once = true
		q.interleave()
	}
	return q.Tx.QueryRowContext(ctx, query, args...)
}
func TestHashStoreReaderUsesOnePublicationSnapshot(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(256))
	hashStoreRun(t, f, 64, 4096)
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	tx, err := reader.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := &hashInterleaveQuery{Tx: tx, interleave: func() {
		r := hashStoreRun(t, f, 64, 4096)
		if r.DurableOffset != 128 {
			t.Fatal(r)
		}
	}}
	snapshot, _, err := reader.readHashSnapshot(context.Background(), query)
	if err != nil || !query.once || snapshot.Work[0].DurableOffset != 64 || snapshot.Work[0].Sequence != 1 {
		t.Fatal("mixed snapshot during settlement", snapshot, err)
	}
	if hashStoreSnapshot(t, f.store).Work[0].DurableOffset != 128 {
		t.Fatal("interleave did not commit new publication")
	}
}
