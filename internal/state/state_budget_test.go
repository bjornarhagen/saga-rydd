//go:build darwin || linux

package state

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func stateBudgetNamedFixture(t *testing.T, database, wal int64) *Store {
	t.Helper()
	dir := privateDir(t)
	s := &Store{path: filepath.Join(dir, Filename)}
	for i, length := range []int64{database, wal} {
		if length < 0 {
			continue
		}
		path := s.path
		if i == 1 {
			path += "-wal"
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(length); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestInventoryStateBudgetNamedLengthsAndExactThreshold(t *testing.T) {
	s := stateBudgetNamedFixture(t, MinInventoryStateBytes-9, 9)
	// No SQLite connection exists. Only the two names' metadata may be sampled.
	var paths []string
	hooks := inventoryStateBudgetHooks{lstat: func(path string, st *unix.Stat_t) error { paths = append(paths, path); return unix.Lstat(path, st) }}
	report, err := s.inventoryStateBudget(context.Background(), MinInventoryStateBytes, hooks)
	if err != nil || !report.Available || report.Status != "limit_reached" || report.Reason != "inventory_state_limit" || report.DatabaseBytes == nil || *report.DatabaseBytes != MinInventoryStateBytes-9 || report.WALBytes == nil || *report.WALBytes != 9 || report.TotalBytes == nil || *report.TotalBytes != MinInventoryStateBytes {
		t.Fatal(report, err)
	}
	if !reflect.DeepEqual(paths, []string{s.path, s.path + "-wal", s.path, s.path + "-wal"}) {
		t.Fatal("unexpected metadata scope", paths)
	}
	if report.Contract != InventoryStateBudgetContract || report.Scope != "configured_inventory_database_and_wal" || !report.SequentialObservations || report.HardLimitEnforced || report.PhysicalAllocationVerified || report.NamespaceAuthenticated || report.OtherStoresIncluded || report.SampleStartedAt.IsZero() || report.SampleFinishedAt.Before(report.SampleStartedAt) {
		t.Fatal(report)
	}
	*report.TotalBytes = 0
	below, err := s.InventoryStateBudget(context.Background(), MinInventoryStateBytes+1)
	if err != nil || !below.Available || below.Status != "below_limit" || below.Reason != "" || *below.TotalBytes != MinInventoryStateBytes {
		t.Fatal("returned pointer changed later observation", below, err)
	}
	encoded, err := json.Marshal(below)
	if err != nil || strings.Contains(string(encoded), s.path) || strings.Contains(string(encoded), "hard_limit_enforced\":true") {
		t.Fatal("scope/authority projection", string(encoded), err)
	}
}

func TestInventoryStateBudgetMissingWALAndUnavailableFiles(t *testing.T) {
	s := stateBudgetNamedFixture(t, 24, -1)
	report, err := s.InventoryStateBudget(context.Background(), MinInventoryStateBytes)
	if err != nil || !report.Available || report.WALBytes == nil || *report.WALBytes != 0 || *report.TotalBytes != 24 {
		t.Fatal("absent WAL was not known zero", report, err)
	}
	for _, mode := range []string{"missing_database", "database_directory", "database_symlink", "wal_symlink", "wal_directory", "wal_fifo", "shared_database", "hardlinked_database", "stat_error"} {
		t.Run(mode, func(t *testing.T) {
			s := stateBudgetNamedFixture(t, 24, -1)
			source := filepath.Join(filepath.Dir(s.path), "unopened-generated-body")
			if err := os.WriteFile(source, []byte("generated secret body"), 0600); err != nil {
				t.Fatal(err)
			}
			hooks := inventoryStateBudgetHooks{}
			switch mode {
			case "missing_database", "database_directory", "database_symlink":
				if err := os.Remove(s.path); err != nil {
					t.Fatal(err)
				}
				if mode == "database_directory" {
					if err := os.Mkdir(s.path, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "database_symlink" {
					if err := os.Symlink(source, s.path); err != nil {
						t.Fatal(err)
					}
				}
			case "wal_symlink":
				if err := os.Symlink(source, s.path+"-wal"); err != nil {
					t.Fatal(err)
				}
			case "wal_directory":
				if err := os.Mkdir(s.path+"-wal", 0700); err != nil {
					t.Fatal(err)
				}
			case "wal_fifo":
				if err := unix.Mkfifo(s.path+"-wal", 0600); err != nil {
					t.Fatal(err)
				}
			case "shared_database":
				if err := os.Chmod(s.path, 0644); err != nil {
					t.Fatal(err)
				}
			case "hardlinked_database":
				if err := os.Link(s.path, filepath.Join(filepath.Dir(s.path), "database-alias")); err != nil {
					t.Fatal(err)
				}
			case "stat_error":
				hooks.lstat = func(string, *unix.Stat_t) error { return unix.EIO }
			}
			report, err := s.inventoryStateBudget(context.Background(), MinInventoryStateBytes, hooks)
			if err != nil || report.Available || report.Status != "unavailable" || report.TotalBytes != nil || report.DatabaseBytes != nil || report.WALBytes != nil {
				t.Fatal("unsafe/missing file admitted", report, err)
			}
			if body, err := os.ReadFile(source); err != nil || string(body) != "generated secret body" {
				t.Fatal("metadata helper changed source body", err)
			}
		})
	}
}

func TestInventoryStateBudgetReplacementAndGrowthDuringSample(t *testing.T) {
	for _, mode := range []string{"database_replaced", "wal_created", "database_grown", "same_length_edit"} {
		t.Run(mode, func(t *testing.T) {
			s := stateBudgetNamedFixture(t, 24, -1)
			hooks := inventoryStateBudgetHooks{afterStat: func(call int) {
				if call != 1 {
					return
				}
				switch mode {
				case "database_replaced":
					if err := os.Rename(s.path, s.path+"-parked"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(s.path, make([]byte, 24), 0600); err != nil {
						t.Fatal(err)
					}
				case "wal_created":
					if err := os.WriteFile(s.path+"-wal", []byte("wal"), 0600); err != nil {
						t.Fatal(err)
					}
				case "database_grown":
					if err := os.Truncate(s.path, 25); err != nil {
						t.Fatal(err)
					}
				case "same_length_edit":
					info, err := os.Stat(s.path)
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Millisecond)
					if err := os.WriteFile(s.path, []byte(strings.Repeat("x", 24)), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Chtimes(s.path, info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
				}
			}}
			report, err := s.inventoryStateBudget(context.Background(), MinInventoryStateBytes, hooks)
			if err != nil || report.Available || report.TotalBytes != nil || report.Reason != "inventory_state_changed" {
				t.Fatal("changed named evidence admitted", report, err)
			}
		})
	}
}

func TestInventoryStateBudgetUnsafeMetadataAndOverflow(t *testing.T) {
	for _, mode := range []string{"wrong_owner", "zero_links", "negative_size", "setuid", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			s := &Store{path: "/generated-only-database-name"}
			calls := 0
			hooks := inventoryStateBudgetHooks{lstat: func(path string, st *unix.Stat_t) error {
				calls++
				*st = unix.Stat_t{Mode: unix.S_IFREG | 0600, Uid: uint32(os.Geteuid()), Nlink: 1, Size: 24}
				switch mode {
				case "wrong_owner":
					st.Uid++
				case "zero_links":
					st.Nlink = 0
				case "negative_size":
					st.Size = -1
				case "setuid":
					st.Mode |= unix.S_ISUID
				case "overflow":
					if path == s.path {
						st.Size = math.MaxInt64
					} else {
						st.Size = 1
					}
				}
				return nil
			}}
			report, err := s.inventoryStateBudget(context.Background(), MinInventoryStateBytes, hooks)
			if err != nil || report.Available || report.TotalBytes != nil || calls > 4 {
				t.Fatal(report, err, calls)
			}
			if mode == "overflow" && (report.Reason != "inventory_state_overflow" || report.DatabaseBytes == nil || *report.DatabaseBytes != math.MaxInt64) {
				t.Fatal("overflow lost known components", report)
			}
		})
	}
}

func TestInventoryStateBudgetCancellationAndTwoClockFinalAdmission(t *testing.T) {
	s := stateBudgetNamedFixture(t, 24, -1)
	if _, err := s.InventoryStateBudget(nil, MinInventoryStateBytes); !errors.Is(err, ErrInventoryStateBudgetInput) {
		t.Fatal(err)
	}
	for _, limit := range []int64{0, MinInventoryStateBytes - 1, MaxInventoryStateBytes + 1} {
		if _, err := s.InventoryStateBudget(context.Background(), limit); !errors.Is(err, ErrInventoryStateBudgetInput) {
			t.Fatal(err)
		}
	}
	for _, cancelAt := range []int{-1, 0, 3} {
		ctx, cancel := context.WithCancel(context.Background())
		if cancelAt == -1 {
			cancel()
		}
		calls := 0
		hooks := inventoryStateBudgetHooks{lstat: func(path string, st *unix.Stat_t) error { calls++; return unix.Lstat(path, st) }, afterStat: func(call int) {
			if call == cancelAt {
				cancel()
			}
		}}
		report, err := s.inventoryStateBudget(ctx, MinInventoryStateBytes, hooks)
		cancel()
		if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(report, InventoryStateBudget{}) || (cancelAt == -1 && calls != 0) {
			t.Fatal("cancellation published partial evidence", cancelAt, report, err, calls)
		}
	}
	for _, mode := range []string{"wall_gap", "elapsed_gap", "wall_rollback", "elapsed_rollback", "final_wall_gap", "unavailable_final_rollback"} {
		t.Run(mode, func(t *testing.T) {
			start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			wall, elapsed := start, start
			clockCalls, statCalls := 0, 0
			hooks := inventoryStateBudgetHooks{wallNow: func() time.Time {
				clockCalls++
				if mode == "final_wall_gap" && clockCalls == 6 {
					return start.Add(5 * time.Second)
				}
				if mode == "unavailable_final_rollback" && clockCalls == 3 {
					return start.Add(-time.Second)
				}
				return wall
			}, elapsedNow: func() time.Time { return elapsed }, lstat: func(path string, st *unix.Stat_t) error {
				statCalls++
				if mode == "unavailable_final_rollback" {
					return unix.ENOENT
				}
				return unix.Lstat(path, st)
			}, afterStat: func(call int) {
				if call != 0 {
					return
				}
				switch mode {
				case "wall_gap":
					wall = start.Add(5 * time.Second)
				case "elapsed_gap":
					elapsed = start.Add(5 * time.Second)
				case "wall_rollback":
					wall = start.Add(-time.Second)
				case "elapsed_rollback":
					elapsed = start.Add(-time.Second)
				}
			}}
			report, err := s.inventoryStateBudget(context.Background(), MinInventoryStateBytes, hooks)
			want := error(context.DeadlineExceeded)
			if strings.Contains(mode, "rollback") {
				want = ErrInventoryStateBudgetClock
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(report, InventoryStateBudget{}) {
				t.Fatal("refused clock published evidence", report, err)
			}
			if mode == "final_wall_gap" && statCalls != 4 {
				t.Fatal("final admission fixture missed full sample", statCalls)
			}
		})
	}
}

func TestInventoryStateBudgetRealSQLiteRetainsReusableLengthsAndHistory(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, []string{"/generated-offline-root"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.Exec("INSERT INTO settings VALUES('owner-history-fixture',X'01'); CREATE TABLE fixture_padding(body BLOB); INSERT INTO fixture_padding VALUES(zeroblob(2097152))"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	before, err := w.InventoryStateBudget(ctx, MinInventoryStateBytes)
	if err != nil || !before.Available || before.Status != "limit_reached" {
		t.Fatal(before, err)
	}
	if _, err := w.db.Exec("DELETE FROM fixture_padding; PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		t.Fatal(err)
	}
	var reusable int64
	if err := w.db.QueryRow("PRAGMA freelist_count").Scan(&reusable); err != nil || reusable < 1 {
		t.Fatal("fixture did not free pages", reusable, err)
	}
	reader, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	after, err := reader.InventoryStateBudget(ctx, MinInventoryStateBytes)
	if err != nil || !after.Available || after.Status != "limit_reached" || *after.DatabaseBytes < *before.DatabaseBytes {
		t.Fatal("free pages were called physical shrink", before, after, err)
	}
	var count, version int
	if err := w.db.QueryRow("SELECT count(*) FROM settings WHERE key='owner-history-fixture'").Scan(&count); err != nil || count != 1 {
		t.Fatal("observation changed owner history", count, err)
	}
	if err := w.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != schemaVersion {
		t.Fatal("observation changed schema", version, err)
	}
}
