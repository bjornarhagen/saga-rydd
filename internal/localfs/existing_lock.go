//go:build darwin || linux

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// ExistingLock coordinates an edit with an already initialized state writer.
// It never creates a directory or lock and never unlinks the stable lock name.
type ExistingLock struct {
	file, parent *os.File
	path         string
	stamp        unix.Stat_t
	parentStamp  unix.Stat_t
	once         sync.Once
	closeErr     error
}

func AcquireExistingLock(ctx context.Context, dir string) (*ExistingLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	parent := os.NewFile(uintptr(fd), dir)
	failParent := func(err error) (*ExistingLock, error) { _ = parent.Close(); return nil, err }
	var before unix.Stat_t
	if err = unix.Fstat(fd, &before); err != nil {
		return failParent(err)
	}
	if !privateExistingLockDir(before) {
		return failParent(fmt.Errorf("%q must be a private, owned directory", dir))
	}
	lockFD, err := unix.Openat(fd, "writer.lock", unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return failParent(err)
	}
	file := os.NewFile(uintptr(lockFD), filepath.Join(dir, "writer.lock"))
	fail := func(err error) (*ExistingLock, error) { _ = file.Close(); return failParent(err) }
	var stamp unix.Stat_t
	if err = unix.Fstat(lockFD, &stamp); err != nil {
		return fail(err)
	}
	if !privateExistingLockFile(stamp) {
		return fail(fmt.Errorf("%q must be a private, owned regular lock file with one link", file.Name()))
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	if err = unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			err = ErrLocked
		}
		return fail(err)
	}
	l := &ExistingLock{file: file, parent: parent, path: dir, stamp: stamp, parentStamp: before}
	if err = l.Check(ctx); err != nil {
		return fail(err)
	}
	return l, nil
}

func privateExistingLockDir(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid())
}

func privateExistingLockFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1
}

// Check detects changed held/named lock and parent objects. This coordinates
// cooperating processes; it does not authenticate a mutable same-user namespace.
func (l *ExistingLock) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil || l.file == nil || l.parent == nil {
		return errors.New("existing writer lock is unavailable")
	}
	var held, named, parent, namedParent unix.Stat_t
	if err := unix.Fstat(int(l.file.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(l.parent.Fd()), "writer.lock", &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if err := unix.Fstat(int(l.parent.Fd()), &parent); err != nil {
		return err
	}
	if err := unix.Lstat(l.path, &namedParent); err != nil {
		return err
	}
	if !privateExistingLockFile(held) || !privateExistingLockFile(named) || !sameExistingLockStamp(l.stamp, held) || !sameExistingLockStamp(held, named) || !privateExistingLockDir(parent) || !privateExistingLockDir(namedParent) || !sameExistingLockDir(l.parentStamp, parent) || !sameExistingLockDir(parent, namedParent) {
		return errors.New("existing writer lock or directory changed")
	}
	return ctx.Err()
}

func sameExistingLockStamp(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func sameExistingLockDir(a, b unix.Stat_t) bool {
	// Creating a same-directory config temporary legitimately changes mtime.
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid
}

func (l *ExistingLock) Identity() ObjectIdentity {
	return ObjectIdentity{Device: fmt.Sprint(l.stamp.Dev), Inode: fmt.Sprint(l.stamp.Ino)}
}

func (l *ExistingLock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { l.closeErr = errors.Join(l.file.Close(), l.parent.Close()) })
	return l.closeErr
}
