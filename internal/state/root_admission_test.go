package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func rootAdmissionStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func rootAdmissionPaths(count int) []string {
	paths := make([]string, count)
	for i := range paths {
		paths[i] = fmt.Sprintf("/generated/root-%03d", i)
	}
	return paths
}

// The oracle observes all retained root fields plus genuine queue state. It is
// limited to generated test stores and does not query or open source objects.
func rootAdmissionEvidence(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT id,hex(path),enabled,volume_id,coalesce(last_scan_ns,-1),last_error FROM roots ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id, enabled, scanned int64
		var path, volume, failure string
		if err = rows.Scan(&id, &path, &enabled, &volume, &scanned, &failure); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%d:%s:%d:%s:%d:%s", id, path, enabled, volume, scanned, failure))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRootAdmissionCapacityIncludesDisabledAndIsAtomic(t *testing.T) {
	s, _ := rootAdmissionStore(t)
	paths := rootAdmissionPaths(126)
	if err := s.SyncRoots(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncRoots(context.Background(), []string{paths[0], "/generated/final-a", "/generated/final-b"}); err != nil {
		t.Fatal(err)
	}
	before := rootAdmissionEvidence(t, s)
	if len(before) != RootAdmissionLimit {
		t.Fatal(len(before))
	}
	for _, selection := range [][]string{{"/generated/overflow"}, {paths[1], "/generated/overflow-a", "/generated/overflow-b"}} {
		if err := s.SyncRoots(context.Background(), selection); !errors.Is(err, ErrRootAdmissionCapacity) {
			t.Fatal(err)
		}
		if after := rootAdmissionEvidence(t, s); !reflect.DeepEqual(before, after) {
			t.Fatal("refused set changed root membership", before, after)
		}
	}
	// Reactivation consumes no slot and keeps the existing root identity.
	if err := s.SyncRoots(context.Background(), []string{paths[1]}); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := s.db.QueryRow("SELECT id FROM roots WHERE path=? AND enabled=1", []byte(paths[1])).Scan(&id); err != nil || id != 2 {
		t.Fatal(id, err)
	}
}

func TestRootAdmissionLegacyHistoryAndQueueEvidenceRemain(t *testing.T) {
	s, dir := rootAdmissionStore(t)
	paths := rootAdmissionPaths(200)
	// Explicit legacy fixture: older writers/external SQL could exceed the new
	// updated-writer admission limit. No existing history is deleted or repaired.
	for _, path := range paths {
		if _, err := s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte(path)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec("UPDATE roots SET volume_id='volume',last_scan_ns=123,last_error='saved failure' WHERE id=150"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(context.Background(), 150, ScanKind, []byte("child"), time.Unix(1700000000, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE jobs SET cursor=X'ff',attempts=3,last_error='retry',inventory_claimed=1 WHERE root_id=150"); err != nil {
		t.Fatal(err)
	}
	before := rootAdmissionEvidence(t, s)
	if err := s.SyncRoots(context.Background(), []string{paths[149]}); err != nil {
		t.Fatal(err)
	}
	var id, scanned, attempts, due int64
	var volume, failure, cursor, jobFailure string
	if err := s.db.QueryRow("SELECT id,volume_id,last_scan_ns,last_error FROM roots WHERE path=?", []byte(paths[149])).Scan(&id, &volume, &scanned, &failure); err != nil {
		t.Fatal(err)
	}
	if id != 150 || scanned != 123 || volume != "volume" || failure != "saved failure" {
		t.Fatal(id, scanned, volume, failure)
	}
	if err := s.db.QueryRow("SELECT hex(cursor),attempts,due_at_ns,last_error FROM jobs WHERE root_id=150").Scan(&cursor, &attempts, &due, &jobFailure); err != nil || cursor != "FF" || attempts != 3 || due != time.Unix(1700000000, 0).UnixNano() || jobFailure != "retry" {
		t.Fatal(cursor, attempts, due, jobFailure, err)
	}
	if len(rootAdmissionEvidence(t, s)) != len(before) {
		t.Fatal("legacy records removed")
	}
	current := rootAdmissionEvidence(t, s)
	if err := s.SyncRoots(context.Background(), []string{paths[149], "/generated/new"}); !errors.Is(err, ErrRootAdmissionCapacity) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, rootAdmissionEvidence(t, s)) {
		t.Fatal("legacy refusal changed evidence")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if summary, err := r.Summary(context.Background()); err != nil || summary.EnabledRoots != 1 || summary.PendingJobs != 1 {
		t.Fatal(summary, err)
	}
}

func TestRootAdmissionExactNoOpDoesNotRewriteAndCancellationWins(t *testing.T) {
	s, dir := rootAdmissionStore(t)
	paths := []string{"/generated/a", "/generated/b"}
	if err := s.SyncRoots(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	// An aborting update trigger independently proves the no-op avoids even a
	// harmless membership rewrite. Snapshot bytes/stamps cover DB and WAL.
	if _, err := s.db.Exec("CREATE TRIGGER no_root_rewrite BEFORE UPDATE ON roots BEGIN SELECT RAISE(ABORT,'unexpected rewrite'); END"); err != nil {
		t.Fatal(err)
	}
	before := rootAdmissionStorage(t, dir)
	if err := s.SyncRoots(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	if after := rootAdmissionStorage(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatal("no-op rewrote SQLite storage")
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := s.syncRootsAdmitted(ctx, paths, rootAdmissionHooks{noOpReleased: cancel})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrRootAdmissionPublication) {
		t.Fatal(err)
	}
	if after := rootAdmissionStorage(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatal("canceled no-op rewrote storage")
	}
}

type rootStorageEvidence struct {
	Data     []byte
	Size     int64
	Modified time.Time
}

func rootAdmissionStorage(t *testing.T, dir string) map[string]rootStorageEvidence {
	t.Helper()
	result := make(map[string]rootStorageEvidence)
	for _, name := range []string{Filename, Filename + "-wal"} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		result[name] = rootStorageEvidence{data, info.Size(), info.ModTime()}
	}
	return result
}

func TestRootAdmissionInputPreflightRawPathsAndFrozenIdentity(t *testing.T) {
	s, _ := rootAdmissionStore(t)
	ctx := context.Background()
	for _, paths := range [][]string{nil, rootAdmissionPaths(129), {"relative"}, {"/generated/a/../b"}, {"/generated/./b"}, {"/generated/b/"}, {"/generated//b"}, {"/generated/a", "/generated/a"}, {"/generated/" + strings.Repeat("b", 4096)}, {"/" + strings.Repeat("b", 2<<20)}, {"/generated/\x00"}} {
		if err := s.SyncRoots(ctx, paths); !errors.Is(err, ErrRootAdmissionInput) {
			t.Fatal(paths, err)
		}
	}
	if countRows(t, s, "SELECT count(*) FROM roots") != 0 {
		t.Fatal("invalid input reached mutation")
	}
	raw := "/generated/" + string([]byte{0xff, 0xfe})
	paths := []string{raw, "/"}
	_, expectedID, err := rootAdmissionRequest(ctx, paths)
	if err != nil || !ValidRootAdmissionRequestID(expectedID) {
		t.Fatal(expectedID, err)
	}
	_, reversedID, _ := rootAdmissionRequest(ctx, []string{"/", raw})
	if reversedID == expectedID || ValidRootAdmissionRequestID(strings.ToUpper(expectedID)) || ValidRootAdmissionRequestID(expectedID+"0") {
		t.Fatal("request identity lost exact order/format")
	}
	err = s.syncRootsAdmitted(ctx, paths, rootAdmissionHooks{beforeCommit: func() { paths[0] = "/generated/caller-changed" }})
	if err != nil {
		t.Fatal(err)
	}
	var path []byte
	if err = s.db.QueryRow("SELECT path FROM roots WHERE id=1").Scan(&path); err != nil || !bytes.Equal(path, []byte(raw)) {
		t.Fatal(path, err)
	}
	_, actualID, _ := rootAdmissionRequest(ctx, []string{raw, "/"})
	if actualID != expectedID {
		t.Fatal("raw path identity changed")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = s.SyncRoots(canceled, []string{"/generated/new"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRootAdmissionCorruptScalarAndAliasRefuseWithoutRepair(t *testing.T) {
	for _, corrupt := range []string{"text twin", "oversize blob", "oversize text", "enabled blob", "enabled real", "negative id", "noncanonical", "root-dot", "root-dotdot", "nul"} {
		t.Run(corrupt, func(t *testing.T) {
			s, _ := rootAdmissionStore(t)
			if err := s.SyncRoots(context.Background(), []string{"/generated/existing"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			var err error
			switch corrupt {
			case "text twin":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", "/generated/existing")
			case "oversize blob":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte("/"+strings.Repeat("x", 2<<20)))
			case "oversize text":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", strings.Repeat("x", 2<<20))
			case "enabled blob":
				_, err = s.db.Exec("UPDATE roots SET enabled=?", bytes.Repeat([]byte{'x'}, 2<<20))
			case "enabled real":
				_, err = s.db.Exec("UPDATE roots SET enabled=0.5")
			case "negative id":
				_, err = s.db.Exec("INSERT INTO roots(id,path,enabled) VALUES(-1,X'2f626164',0)")
			case "noncanonical":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte("/generated/a/../b"))
			case "root-dot":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte("/."))
			case "root-dotdot":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte("/.."))
			case "nul":
				_, err = s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte("/bad\x00"))
			}
			if err != nil {
				t.Fatal(err)
			}
			var enabledType, pathHex string
			var n int64
			if err = s.db.QueryRow("SELECT count(*),typeof(enabled),hex(path) FROM roots WHERE id=1").Scan(&n, &enabledType, &pathHex); err != nil {
				t.Fatal(err)
			}
			if err = s.SyncRoots(context.Background(), []string{"/generated/new"}); !errors.Is(err, ErrRootAdmissionCorrupt) {
				t.Fatal(err)
			}
			var afterType, afterPath string
			var afterN int64
			if err = s.db.QueryRow("SELECT count(*),typeof(enabled),hex(path) FROM roots WHERE id=1").Scan(&afterN, &afterType, &afterPath); err != nil || n != afterN || enabledType != afterType || pathHex != afterPath {
				t.Fatal("damage repaired or membership rewritten", err)
			}
			if countRows(t, s, "SELECT count(*) FROM roots WHERE path=X'2f67656e6572617465642f6e6577'") != 0 {
				t.Fatal("corruption admitted new scope")
			}
		})
	}
}

func TestRootAdmissionConcurrentWritersCannotOvershoot(t *testing.T) {
	s, dir := rootAdmissionStore(t)
	if err := s.SyncRoots(context.Background(), rootAdmissionPaths(127)); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenWriter(context.Background(), dir); !errors.Is(err, localfs.ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal(err)
	}
	// Deliberately bypass only the app lock, modelling a cooperating legacy
	// SQLite client. Both counts and writes still share their own single TX.
	b, err := connect(context.Background(), filepath.Join(dir, Filename), false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, 2)
	for i, writer := range []*Store{s, b} {
		wg.Add(1)
		go func(index int, w *Store) {
			defer wg.Done()
			<-start
			results <- w.SyncRoots(context.Background(), []string{fmt.Sprintf("/generated/last-%d", index)})
		}(i, writer)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrRootAdmissionCapacity) && !errors.Is(err, ErrRootAdmissionUnavailable) && !errors.Is(err, ErrRootAdmissionPublication) {
			t.Fatal(err)
		}
	}
	if success != 1 || countRows(t, s, "SELECT count(*) FROM roots") != 128 || countRows(t, s, "SELECT count(*) FROM roots WHERE enabled=1") != 1 {
		t.Fatal("last-slot race overshot or lost both admissions", success)
	}
}

func TestRootAdmissionCommitCancellationAndUncertaintyCloseAuthority(t *testing.T) {
	for _, mode := range []string{"before", "after", "commit-error", "committed-error"} {
		t.Run(mode, func(t *testing.T) {
			s, dir := rootAdmissionStore(t)
			if err := s.SyncRoots(context.Background(), []string{"/generated/old"}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			paths := []string{"/generated/new"}
			_, id, _ := rootAdmissionRequest(ctx, paths)
			hooks := rootAdmissionHooks{}
			switch mode {
			case "before":
				hooks.beforeCommit = cancel
			case "after":
				hooks.afterCommit = cancel
			case "commit-error":
				hooks.commit = func(*sql.Tx) error { return errors.New("private SQL diagnostic") }
			case "committed-error":
				hooks.commit = func(tx *sql.Tx) error {
					if err := tx.Commit(); err != nil {
						return err
					}
					return errors.New("private SQL diagnostic")
				}
			}
			err := s.syncRootsAdmitted(ctx, paths, hooks)
			if mode == "before" {
				if !errors.Is(err, context.Canceled) || errors.Is(err, ErrRootAdmissionPublication) || countRows(t, s, "SELECT count(*) FROM roots") != 1 {
					t.Fatal(err)
				}
				return
			}
			var uncertain *RootAdmissionPublicationError
			if !errors.As(err, &uncertain) || uncertain.RequestID != id || !ValidRootAdmissionRequestID(uncertain.RequestID) || strings.Contains(err.Error(), "private SQL") {
				t.Fatal(err)
			}
			if mode == "after" && !errors.Is(err, context.Canceled) {
				t.Fatal("lost canceled identity", err)
			}
			if _, err = s.Summary(context.Background()); err == nil {
				t.Fatal("uncertain handle retained query authority")
			}
			r, err := OpenWriter(context.Background(), dir)
			if err != nil {
				t.Fatal("uncertainty did not release held lock", err)
			}
			defer r.Close()
			want := 1
			if mode != "commit-error" {
				want = 2
			}
			if countRows(t, r, "SELECT count(*) FROM roots") != want {
				t.Fatal("unknown outcome falsely reset history")
			}
			if err := r.SyncRoots(context.Background(), paths); err != nil {
				t.Fatal(err)
			}
			if countRows(t, r, "SELECT count(*) FROM roots") != 2 {
				t.Fatal("exact intentional retry duplicated root")
			}
		})
	}
}

func TestRootAdmissionSchemaAndCPUHistoryRemainUnchanged(t *testing.T) {
	for _, cpu := range []bool{false, true} {
		t.Run(fmt.Sprint(cpu), func(t *testing.T) {
			s, dir := rootAdmissionStore(t)
			if cpu {
				if _, err := s.ActivateCPUCharges(context.Background(), chargeNow()); err != nil {
					t.Fatal(err)
				}
			}
			before := chargeBytes(t, s)
			var schema int
			if err := s.db.QueryRow("PRAGMA user_version").Scan(&schema); err != nil {
				t.Fatal(err)
			}
			if err := s.SyncRoots(context.Background(), []string{"/generated/a"}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, chargeBytes(t, s)) {
				t.Fatal("root membership changed CPU history")
			}
			var after int
			if err := s.db.QueryRow("PRAGMA user_version").Scan(&after); err != nil || schema != after {
				t.Fatal(schema, after, err)
			}
			r, err := OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err := r.SyncRoots(context.Background(), []string{"/generated/b"}); !errors.Is(err, ErrRootAdmissionUnavailable) {
				t.Fatal(err)
			}
		})
	}
}

func TestRootAdmissionFiniteConnectionWaitAndEarlierDeadline(t *testing.T) {
	for _, callerDeadline := range []bool{false, true} {
		t.Run(fmt.Sprint(callerDeadline), func(t *testing.T) {
			s, _ := rootAdmissionStore(t)
			if err := s.SyncRoots(context.Background(), []string{"/generated/old"}); err != nil {
				t.Fatal(err)
			}
			conn, err := s.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx := context.Background()
			if callerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			done := make(chan error, 1)
			go func() { done <- s.SyncRoots(ctx, []string{"/generated/new"}) }()
			defer func() { _ = conn.Close() }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrRootAdmissionPublication) {
					t.Fatal(err)
				}
			case <-time.After(7 * time.Second):
				_ = conn.Close()
				<-done
				t.Fatal("root admission did not enforce its own finite wait")
			}
			if err = conn.Close(); err != nil {
				t.Fatal(err)
			}
			if countRows(t, s, "SELECT count(*) FROM roots") != 1 {
				t.Fatal("connection denial admitted a new root")
			}
		})
	}
}

func TestRootAdmissionUncertainCommitVetoesQueuedQuery(t *testing.T) {
	s, dir := rootAdmissionStore(t)
	if err := s.SyncRoots(context.Background(), []string{"/generated/old"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	beforeWaits := s.db.Stats().WaitCount
	err := s.syncRootsAdmitted(ctx, []string{"/generated/new"}, rootAdmissionHooks{afterCommit: func() {
		go func() {
			query, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, err := s.Summary(query)
			done <- err
		}()
		limit := time.Now().Add(time.Second)
		for s.db.Stats().WaitCount == beforeWaits && time.Now().Before(limit) {
			time.Sleep(time.Millisecond)
		}
		if s.db.Stats().WaitCount == beforeWaits {
			t.Error("query did not queue behind held publication connection")
		}
		cancel()
	}})
	if !errors.Is(err, ErrRootAdmissionPublication) || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("queued query escaped uncertain handle revocation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued query was not released after closure")
	}
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if countRows(t, r, "SELECT count(*) FROM roots") != 2 {
		t.Fatal("post-commit cancellation falsely claimed rollback")
	}
}

func TestRootAdmissionUnavailableKeepsCausePrivateAndRollsBack(t *testing.T) {
	s, _ := rootAdmissionStore(t)
	if err := s.SyncRoots(context.Background(), []string{"/generated/old"}); err != nil {
		t.Fatal(err)
	}
	before := rootAdmissionEvidence(t, s)
	if _, err := s.db.Exec("CREATE TRIGGER root_insert_fault BEFORE INSERT ON roots BEGIN SELECT RAISE(ABORT,'private SQL payload'); END"); err != nil {
		t.Fatal(err)
	}
	err := s.SyncRoots(context.Background(), []string{"/generated/new"})
	if !errors.Is(err, ErrRootAdmissionUnavailable) || errors.Is(err, ErrRootAdmissionPublication) || strings.Contains(err.Error(), "private SQL") {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rootAdmissionEvidence(t, s)) {
		t.Fatal("failed insert kept the prior disable")
	}
}

func TestRootAdmissionLegacyDamageBeyondReturnedCensusRefuses(t *testing.T) {
	s, _ := rootAdmissionStore(t)
	for _, path := range rootAdmissionPaths(200) {
		if _, err := s.db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte(path)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON; UPDATE roots SET enabled=zeroblob(2097152) WHERE id=200"); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncRoots(context.Background(), []string{"/generated/root-000"}); !errors.Is(err, ErrRootAdmissionCorrupt) {
		t.Fatal("damage beyond the bounded returned census was silently adopted", err)
	}
	if countRows(t, s, "SELECT count(*) FROM roots WHERE enabled=1") != 0 {
		t.Fatal("corrupt legacy history reactivated selected scope")
	}
	var kind string
	var size int64
	if err := s.db.QueryRow("SELECT typeof(enabled),length(enabled) FROM roots WHERE id=200").Scan(&kind, &size); err != nil || kind != "blob" || size != 2<<20 {
		t.Fatal("damage was repaired", kind, size, err)
	}
}
