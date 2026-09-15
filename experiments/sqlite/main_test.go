package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderSnapshotAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite3")
	writer, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	reader, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	snapshot, err := reader.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback()
	var n int
	if err := snapshot.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil || n != 1 {
		t.Fatalf("initial snapshot %d: %v", n, err)
	}
	if _, err := writer.Exec("INSERT INTO records VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil || n != 1 {
		t.Fatalf("snapshot changed %d: %v", n, err)
	}
	if err := snapshot.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil || n != 2 {
		t.Fatalf("new snapshot %d: %v", n, err)
	}
	tx, err := writer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO records VALUES(3)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow("SELECT count(*) FROM records").Scan(&n); err != nil || n != 2 {
		t.Fatalf("rollback %d: %v", n, err)
	}
}

func TestProcessCrashRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.sqlite3")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(), "RYDD_CRASH_TEST="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scanner := bufio.NewScanner(stdout)
	ready := scanner.Scan() && scanner.Text() == "ready"
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if !ready {
		t.Fatal("child did not reach its uncommitted transaction")
	}
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var committed, pending int
	if err := db.QueryRow("SELECT count(*) FROM records WHERE kind='committed'").Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM records WHERE kind='pending'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if committed != 1 || pending != 0 {
		t.Fatalf("after kill: committed=%d pending=%d", committed, pending)
	}
	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity %s: %v", integrity, err)
	}
}

func TestCrashChild(t *testing.T) {
	path := os.Getenv("RYDD_CRASH_TEST")
	if path == "" {
		t.Skip("subprocess only")
	}
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE records(kind TEXT, data BLOB); INSERT INTO records VALUES('committed', X'01')"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if _, err := tx.Exec("INSERT INTO records VALUES('pending', zeroblob(1048576))"); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("ready")
	time.Sleep(time.Minute)
	t.Fatal("parent should kill this process")
}
