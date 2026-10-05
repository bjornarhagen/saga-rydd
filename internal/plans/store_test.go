package plans

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func selection() state.SelectionSnapshot {
	path := []byte("/fixture/資料\xff/node_modules")
	return state.SelectionSnapshot{
		InventoryID: strings.Repeat("a", 64),
		Roots:       []state.RootBinding{{ID: 1, PathBytes: []byte("/fixture"), Fingerprint: "root-device-inode"}},
		Targets:     []state.TargetBinding{{FindingID: "node-modules-v1:1:2", Target: state.EntryBinding{Device: "d", Inode: "i"}}},
		Evidence: state.FindingReport{Source: "saved_inventory", PageCoverage: "selected_entries_only", MinimumAgeDays: 90, EntriesExamined: 1, Findings: []state.Finding{{
			ID: "node-modules-v1:1:2", RootID: 1, EntryID: 2, Device: "d", Inode: "i", Path: string(path), PathBytes: path,
			Measurement: state.DirectoryReport{Status: "partial", Truncated: true},
		}}},
	}
}

func TestSavedPlanRoundTripAndImmutability(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	r, err := Save(ctx, base, selection())
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(ctx, base, r.ID)
	if err != nil || !reflect.DeepEqual(loaded.Record.Selection.Evidence.Findings[0].PathBytes, selection().Evidence.Findings[0].PathBytes) || loaded.Record.Executable || loaded.Record.ApprovalAvailable || loaded.Record.Status != "unapproved" || loaded.Record.Selection.Evidence.Findings[0].Measurement.Status != "partial" {
		t.Fatal(loaded, err)
	}
	// JSON's display path may normalize invalid UTF-8; authoritative bytes survive.
	again, err := Load(ctx, base, r.ID)
	if err != nil || !reflect.DeepEqual(again, loaded) {
		t.Fatal(again, err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	for _, q := range []string{"UPDATE plans SET payload=X'00'", "DELETE FROM plans"} {
		if _, err = db.Exec(q); err == nil {
			t.Fatal("immutable record changed", q)
		}
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err = privateFile(filepath.Join(base, "plans", filename) + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	var sync int
	if err = db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatal(sync, err)
	}
	if _, err = Save(ctx, base, selection()); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal(err)
	}
	if _, err = Load(ctx, base, r.ID); err != nil {
		t.Fatal("reader blocked by idle plan writer", err)
	}
	// A damaged stored payload must fail even if its ID is still present.
	if _, err = db.Exec("DROP TRIGGER plans_no_update; UPDATE plans SET payload=X'00'"); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(ctx, base, r.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestSavedPlanBoundsAndReadOnlyErrors(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "not-created")
	if _, err := Load(ctx, base, "../escape"); !errors.Is(err, ErrID) {
		t.Fatal(err)
	}
	if _, err := Load(ctx, base, "plan-v1-"+strings.Repeat("0", 64)); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatal("show created state", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Save(canceled, base, selection()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	oversized := selection()
	oversized.Evidence.Notes = []string{strings.Repeat("x", MaxRecordBytes)}
	if _, err := Save(ctx, base, oversized); err == nil {
		t.Fatal("oversized plan accepted")
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatal("failed save created storage", err)
	}
	r, err := Save(ctx, base, selection())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Load(ctx, filepath.Join(t.TempDir(), "different-state"), r.ID); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	for _, mutate := range []func(*Record){func(r *Record) { r.Version = 99 }, func(r *Record) { r.Executable = true }, func(r *Record) { r.Status = "approved" }} {
		bad := r.Record
		mutate(&bad)
		payload, _ := json.Marshal(bad)
		id := fmt.Sprintf("plan-v1-%x", sha256.Sum256(payload))
		if _, err = db.Exec("INSERT INTO plans VALUES(?,?)", id, payload); err != nil {
			t.Fatal(err)
		}
		if _, err = Load(ctx, base, id); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}

func TestSavedPlanPrivateStorage(t *testing.T) {
	for _, kind := range []string{"shared", "symlink", "hardlink", "foreign", "newer"} {
		t.Run(kind, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			dir := filepath.Join(base, "plans")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, filename)
			switch kind {
			case "shared":
				if err := os.Chmod(dir, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink", "hardlink":
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				if kind == "symlink" {
					err = os.Symlink(target, path)
				} else {
					err = os.Link(target, path)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "foreign", "newer":
				db, closeDB, err := open(context.Background(), base, true)
				if err != nil {
					t.Fatal(err)
				}
				q := "PRAGMA application_id=123"
				if kind == "newer" {
					q = "PRAGMA user_version=99"
				}
				if _, err = db.Exec(q); err != nil {
					t.Fatal(err)
				}
				closeDB()
			}
			if _, err := Save(context.Background(), base, selection()); err == nil {
				t.Fatal("unsafe storage accepted")
			}
		})
	}
}

func TestSavedPlanCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_PLAN_CRASH_FIXTURE")
	if base == "" {
		return
	}
	db, closeDB, err := open(context.Background(), base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Force more than the 1 MiB page cache to exercise uncommitted WAL spills.
	for i := 0; i < 8; i++ {
		if _, err = tx.Exec("INSERT INTO plans VALUES(?,?)", fmt.Sprint("unfinished-", i), []byte(strings.Repeat("x", MaxRecordBytes))); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("READY")
	time.Sleep(time.Minute)
}

func TestSavedPlanCrashRecovery(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	ctx := context.Background()
	r, err := Save(ctx, base, selection())
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSavedPlanCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_PLAN_CRASH_FIXTURE="+base)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() { scan := bufio.NewScanner(stdout); ready <- scan.Scan() && scan.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("crash helper not ready")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("crash helper timed out")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("expected killed helper")
	}
	if _, err = Load(ctx, base, r.ID); err != nil {
		t.Fatal("committed plan lost after crash", err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var n int
	if err = db.QueryRow("SELECT count(*) FROM plans").Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
