package plans

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"golang.org/x/sys/unix"
)

// Undo must not recreate storage or a lock removed after its reader preflight.
// SQLite mode=rw and the no-create lock descriptor preserve that boundary.
func openExistingDismissalWriter(ctx context.Context, base string) (*sql.DB, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !filepath.IsAbs(base) {
		return nil, nil, ErrDismissalRequest
	}
	dir := filepath.Join(base, "plans")
	for _, path := range []string{base, dir} {
		if err := localfs.CheckOwnedDir(path); err != nil {
			return nil, nil, err
		}
	}
	lockPath := filepath.Join(dir, "writer.lock")
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	var db *sql.DB
	closeDB := func() {
		if db != nil {
			_ = db.Close()
		}
		_ = lock.Close()
	}
	fail := func(err error) (*sql.DB, func(), error) { closeDB(); return nil, nil, err }
	info, err := lock.Stat()
	if err != nil {
		return fail(err)
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 {
		return fail(ErrDismissalCorrupt)
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			err = localfs.ErrLocked
		}
		return fail(err)
	}
	named, err := os.Lstat(lockPath)
	if err != nil {
		return fail(err)
	}
	if !os.SameFile(info, named) {
		return fail(ErrDismissalCorrupt)
	}
	path := filepath.Join(dir, filename)
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		if err = privateFile(path + suffix); err != nil && !(suffix != "" && errors.Is(err, os.ErrNotExist)) {
			return fail(err)
		}
	}
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(1000)", "cache_size(-1024)", "mmap_size(0)", "synchronous(FULL)", "wal_autocheckpoint(256)", "foreign_keys(1)"}}.Encode()
	db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return fail(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var app, version int
	if err = db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return fail(err)
	}
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fail(err)
	}
	if app != applicationID || version != 5 {
		return fail(ErrDismissalCorrupt)
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return db, closeDB, nil
}
