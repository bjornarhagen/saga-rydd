//go:build darwin || linux

package localfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrLocked = errors.New("another Rydd worker or state writer is already running")

type Lock struct {
	file *os.File
	once sync.Once
	err  error
}

// AcquireLock uses a stable inode: never unlink a lock file, even on shutdown.
// The kernel releases the lock on process death; PID files are not used as locks.
func AcquireLock(dir string) (*Lock, error) {
	if err := CheckPrivateDir(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "writer.lock")
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	fail := func(err error) (*Lock, error) { f.Close(); return nil, err }
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 {
		return fail(fmt.Errorf("%q must be a private, owned regular lock file with one link", path))
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fail(ErrLocked)
		}
		return fail(err)
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return fail(errors.New("writer lock path changed while acquiring lock"))
	}
	return &Lock{file: f}, nil
}

func (l *Lock) Close() error {
	l.once.Do(func() { l.err = l.file.Close() })
	return l.err
}

// CheckOwnedDir supplements mode checks for IPC directories under /tmp.
func CheckOwnedDir(path string) error {
	if err := CheckPrivateDir(path); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%q belongs to another user", path)
	}
	return nil
}
