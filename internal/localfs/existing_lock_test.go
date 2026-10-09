package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestExistingLockMissingBusyAndStableIdentity(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "private")
	if lock, err := AcquireExistingLock(ctx, dir); !errors.Is(err, os.ErrNotExist) || lock != nil {
		t.Fatal("missing state was initialized", lock, err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing directory was created", err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if lock, err := AcquireExistingLock(ctx, dir); !errors.Is(err, os.ErrNotExist) || lock != nil {
		t.Fatal("missing lock was initialized", lock, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "writer.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing lock name was created", err)
	}
	w, err := AcquireLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if lock, err := AcquireExistingLock(ctx, dir); !errors.Is(err, ErrLocked) || lock != nil {
		t.Fatal("held writer was not excluded", lock, err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(filepath.Join(dir, "writer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	l, err := AcquireExistingLock(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.Check(ctx); err != nil || l.Identity().Device == "" || l.Identity().Inode == "" {
		t.Fatal("newly acquired existing lock was not stable", err)
	}
	if lock, err := AcquireLock(dir); !errors.Is(err, ErrLocked) || lock != nil {
		t.Fatal("existing-only lock did not coordinate with state writer", lock, err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Lstat(filepath.Join(dir, "writer.lock"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("existing lock was replaced or removed", err)
	}
}

func TestExistingLockRejectsUnsafeNamesAndChangedObjects(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "shared", "replacement", "parent_replacement"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "private")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "writer.lock")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "replacement" || kind == "parent_replacement" {
				lock, err := AcquireExistingLock(context.Background(), dir)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
				if kind == "replacement" {
					if err = os.Rename(path, path+".parked"); err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(path, nil, 0600); err != nil {
						t.Fatal(err)
					}
				} else {
					if err = os.Rename(dir, dir+".parked"); err != nil {
						t.Fatal(err)
					}
					if err = os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if err = lock.Check(context.Background()); err == nil {
					t.Fatal("changed lock or parent was accepted")
				}
				return
			}
			if err := os.Rename(path, path+".parked"); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(path+".parked", path)
			case "hardlink":
				err = os.Link(path+".parked", path)
			case "fifo":
				err = unix.Mkfifo(path, 0600)
			case "shared":
				err = os.WriteFile(path, nil, 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
			if lock, err := AcquireExistingLock(context.Background(), dir); err == nil {
				lock.Close()
				t.Fatal("unsafe lock accepted")
			}
		})
	}
}

func TestExistingLockCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "missing")
	if lock, err := AcquireExistingLock(ctx, dir); !errors.Is(err, context.Canceled) || lock != nil {
		t.Fatal("cancellation did not precede filesystem work", lock, err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled acquisition created state", err)
	}
}
