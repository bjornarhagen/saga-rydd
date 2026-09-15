package localfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExclusiveLockAndStableInode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := EnsurePrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	first, err := AcquireLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	before, _ := os.Lstat(filepath.Join(dir, "writer.lock"))
	if other, err := AcquireLock(dir); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second lock: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Lstat(filepath.Join(dir, "writer.lock"))
	if !os.SameFile(before, after) {
		t.Fatal("lock file replaced")
	}
	if other, err := AcquireLock(dir); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal("closing old lock released new lock", err)
	}
}

func TestRejectLockSymlinkAndHardlink(t *testing.T) {
	for _, symlink := range []bool{true, false} {
		dir := filepath.Join(t.TempDir(), "private")
		if err := EnsurePrivateDir(dir); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "untouched")
		if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "writer.lock")
		var err error
		if symlink {
			err = os.Symlink(target, path)
		} else {
			err = os.Link(target, path)
		}
		if err != nil {
			t.Fatal(err)
		}
		if lock, err := AcquireLock(dir); err == nil {
			lock.Close()
			t.Fatal("linked lock accepted")
		}
		data, _ := os.ReadFile(target)
		if string(data) != "keep" {
			t.Fatal("target modified")
		}
	}
}
