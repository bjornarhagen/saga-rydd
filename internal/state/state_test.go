package state

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCancellationAndBoundedWriterContention(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	a, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := a.SyncRoots(ctx, []string{"/old"}); err != nil {
		t.Fatal(err)
	}
	tx, err := a.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE roots SET last_error='pending'"); err != nil {
		t.Fatal(err)
	}
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.SyncRoots(timeout, []string{"/new"}); err == nil {
		t.Fatal("competing write should time out")
	}
	if timeout.Err() != nil {
		t.Fatal("busy timeout did not bound lock waiting")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := b.SyncRoots(ctx, []string{"/new"}); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := b.Summary(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not propagated: %v", err)
	}
}

func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPathBytesAndForeignKeys(t *testing.T) {
	w, err := OpenWriter(context.Background(), privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(context.Background(), []string{"/fixture"}); err != nil {
		t.Fatal(err)
	}
	path := []byte{'f', 0xff, 'x'}
	if _, err := w.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?, 'file',1,4096,0,0,'1','2',1,0)`, path, []byte{}); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := w.db.QueryRow("SELECT path FROM entries").Scan(&stored); err != nil || !bytes.Equal(stored, path) {
		t.Fatal(stored, err)
	}
	if _, err := w.db.Exec("INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(999,'directory',X'00',0)"); err == nil {
		t.Fatal("foreign key not enforced")
	}
}

func TestMigrationReopenAndReadOnly(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SyncRoots(ctx, []string{"/fixture/dev"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.Exec("INSERT INTO settings VALUES('preserve', X'01')"); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.Exec("DELETE FROM roots"); err == nil {
		t.Fatal("reader allowed writes")
	}
	summary, err := r.Summary(ctx)
	if err != nil || summary.EnabledRoots != 1 || summary.Entries != 0 {
		t.Fatalf("%+v %v", summary, err)
	}
	r.Close()
	w.Close()
	w, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var n int
	if err := w.db.QueryRow("SELECT count(*) FROM settings WHERE key='preserve'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("lost existing state: %d %v", n, err)
	}
	if err := w.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("duplicate migration: %d %v", n, err)
	}
	w.db.SetMaxIdleConns(0)
	for name, expected := range map[string]int{"foreign_keys": 1, "synchronous": 2, "busy_timeout": 1000, "cache_size": -4096, "wal_autocheckpoint": 1000} {
		if err := w.db.QueryRow("PRAGMA " + name).Scan(&n); err != nil || n != expected {
			t.Fatalf("%s=%d want %d: %v", name, n, expected, err)
		}
	}
}

func TestMissingReaderAndPrivatePaths(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(privateDir(t), "absent")
	if _, err := OpenReader(ctx, dir); err == nil {
		t.Fatal("missing state accepted")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("reader created directory")
	}
	target := filepath.Join(privateDir(t), "outside")
	if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	linkDir := privateDir(t)
	if err := os.Symlink(target, filepath.Join(linkDir, Filename)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWriter(ctx, linkDir); err == nil {
		t.Fatal("symlink database accepted")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "untouched" {
		t.Fatal("symlink target changed")
	}
	shared := filepath.Join(privateDir(t), "shared")
	if err := os.Mkdir(shared, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWriter(ctx, shared); err == nil {
		t.Fatal("shared directory accepted")
	}
}

func TestRejectForeignAndFutureDatabase(t *testing.T) {
	ctx := context.Background()
	for _, future := range []bool{false, true} {
		dir := privateDir(t)
		path := filepath.Join(dir, Filename)
		if future {
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.db.Exec("PRAGMA user_version=99"); err != nil {
				t.Fatal(err)
			}
			w.Close()
		} else {
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('keep')"); err != nil {
				t.Fatal(err)
			}
			db.Close()
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenWriter(ctx, dir); err == nil {
			t.Fatal("unsupported database accepted")
		}
		after, _ := os.ReadFile(path)
		if string(before) != string(after) {
			t.Fatal("unsupported database modified")
		}
	}
}

func TestRootSyncRollbackAndPreservation(t *testing.T) {
	ctx := context.Background()
	w, err := OpenWriter(ctx, privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, []string{"/a", "/b"}); err != nil {
		t.Fatal(err)
	}
	if err := w.SyncRoots(ctx, []string{"/b"}); err != nil {
		t.Fatal(err)
	}
	var total, enabled int
	if err := w.db.QueryRow("SELECT count(*),sum(enabled) FROM roots").Scan(&total, &enabled); err != nil || total != 2 || enabled != 1 {
		t.Fatalf("%d %d %v", total, enabled, err)
	}
	if _, err := w.db.Exec("CREATE TRIGGER reject_new BEFORE INSERT ON roots BEGIN SELECT RAISE(ABORT,'simulated failure'); END"); err != nil {
		t.Fatal(err)
	}
	if err := w.SyncRoots(ctx, []string{"/new"}); err == nil {
		t.Fatal("expected simulated failure")
	}
	if err := w.db.QueryRow("SELECT sum(enabled) FROM roots").Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("root changes partially committed: %d %v", enabled, err)
	}
}

func TestWALReaderSnapshotAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, []string{"/a"}); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	snapshot, err := r.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback()
	var n int
	if err := snapshot.QueryRow("SELECT count(*) FROM roots").Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err := w.SyncRoots(ctx, []string{"/a", "/b"}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.QueryRow("SELECT count(*) FROM roots").Scan(&n); err != nil || n != 1 {
		t.Fatal("snapshot changed", n, err)
	}
	if _, err := w.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRow("SELECT count(*) FROM roots").Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	result, err := w.Checkpoint(ctx)
	if err != nil || result.CheckpointedPages != result.LogPages {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := privateDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestStateCrashChild$")
	cmd.Env = append(os.Environ(), "RYDD_STATE_CRASH_DIR="+dir)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scanner := bufio.NewScanner(stdout)
	ready := scanner.Scan() && scanner.Text() == "ready"
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if !ready {
		t.Fatal("crash helper not ready")
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var committed, pending int
	if err := w.db.QueryRow("SELECT count(*) FROM settings WHERE key='committed'").Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRow("SELECT count(*) FROM settings WHERE key LIKE 'pending%'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if committed != 1 || pending != 0 {
		t.Fatalf("committed=%d pending=%d", committed, pending)
	}
	var integrity string
	if err := w.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal(integrity, err)
	}
}

func TestStateCrashChild(t *testing.T) {
	dir := os.Getenv("RYDD_STATE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	w, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.Exec("INSERT INTO settings VALUES('committed',X'01')"); err != nil {
		t.Fatal(err)
	}
	tx, err := w.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if _, err := tx.Exec("INSERT INTO settings VALUES(?,zeroblob(1048576))", fmt.Sprintf("pending%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("ready")
	time.Sleep(time.Minute)
	t.Fatal("parent should kill this process")
}
