// Package state manages the local inventory database. It does not scan or delete files.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	_ "modernc.org/sqlite"
)

const Filename = "state.sqlite3"
const WALBackpressureBytes int64 = 32 << 20

type Store struct {
	db        *sql.DB
	path      string
	readOnly  bool
	schema    int
	lock      *localfs.Lock
	closeOnce sync.Once
	closeErr  error
}

// OpenWriter creates/migrates only a private Rydd-owned database. FULL durability
// applies to all commits initially; weakening inventory durability is a later,
// measured optimization and must not weaken action/restore records.
func OpenWriter(ctx context.Context, dir string) (*Store, error) {
	if err := localfs.EnsurePrivateDir(dir); err != nil {
		return nil, err
	}
	lock, err := localfs.AcquireLock(dir)
	if err != nil {
		return nil, err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			_ = lock.Close()
		}
	}()
	path := filepath.Join(dir, Filename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		err = f.Close()
	}
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if err := checkFiles(path); err != nil {
		return nil, err
	}
	s, err := connect(ctx, path, false)
	if err != nil {
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		s.Close()
		return nil, err
	}
	s.schema = schemaVersion
	s.lock = lock
	keepLock = true
	return s, nil
}

// OpenReader never initializes/migrates the database or creates its parent.
func OpenReader(ctx context.Context, dir string) (*Store, error) {
	if err := localfs.CheckPrivateDir(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, Filename)
	if err := checkFiles(path); err != nil {
		return nil, err
	}
	s, err := connect(ctx, path, true)
	if err != nil {
		return nil, err
	}
	version, err := s.identity(ctx, false)
	if err != nil {
		s.Close()
		return nil, err
	}
	if version != schemaVersion && version != 4 {
		s.Close()
		return nil, fmt.Errorf("state schema %d requires migration; run rydd state init", version)
	}
	s.schema = version
	if err := s.checkLedger(ctx, version); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func checkFiles(path string) error {
	if err := localfs.CheckPrivateFile(path); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := localfs.CheckPrivateFile(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func connect(ctx context.Context, path string, readOnly bool) (*Store, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := url.Values{}
	q.Set("mode", "rw")
	for _, value := range []string{"busy_timeout(1000)", "foreign_keys(1)", "cache_size(-4096)", "mmap_size(0)"} {
		q.Add("_pragma", value)
	}
	if readOnly {
		q.Set("mode", "ro")
		q.Add("_pragma", "query_only(1)")
	} else {
		q.Add("_pragma", "synchronous(FULL)")
		q.Add("_pragma", "wal_autocheckpoint(1000)")
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, path: absolute, readOnly: readOnly}, nil
}

func (s *Store) identity(ctx context.Context, allowEmpty bool) (int, error) {
	var app, version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	if app == 0 && version == 0 && allowEmpty {
		var tables int
		if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").Scan(&tables); err != nil {
			return 0, err
		}
		if tables == 0 {
			return 0, nil
		}
	}
	if app != applicationID {
		return 0, errors.New("refusing to use a database that is not identified as Rydd state")
	}
	if version < 1 || version > schemaVersion {
		return 0, fmt.Errorf("unsupported state schema %d (supported: %d); database left intact", version, schemaVersion)
	}
	return version, nil
}

func (s *Store) checkLedger(ctx context.Context, version int) error {
	for i := 0; i < version; i++ {
		var name string
		if err := s.db.QueryRowContext(ctx, "SELECT name FROM schema_migrations WHERE version=?", i+1).Scan(&name); err != nil {
			return fmt.Errorf("invalid migration ledger: %w", err)
		}
		if name != migrations[i].name {
			return errors.New("unexpected migration ledger; database left intact")
		}
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	version, err := s.identity(ctx, true)
	if err != nil {
		return err
	}
	if version > 0 {
		if err := s.checkLedger(ctx, version); err != nil {
			return err
		}
	}
	// Validate ownership/version before changing journaling on an existing file.
	var journal string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return err
	}
	if journal != "wal" {
		return fmt.Errorf("WAL mode unavailable (%s); local filesystem required", journal)
	}
	if version == schemaVersion {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := version; i < schemaVersion; i++ {
		if _, err := tx.ExecContext(ctx, migrations[i].sql); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations VALUES(?,?,?)", i+1, migrations[i].name, time.Now().UnixNano()); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id=0x52594444; PRAGMA user_version=%d", schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

// SyncRoots disables removed roots but retains their inventory. Re-adding a root
// preserves its ID; availability/physical identity remain the scanner's concern.
func (s *Store) SyncRoots(ctx context.Context, roots []string) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	if len(roots) == 0 {
		return errors.New("at least one root is required")
	}
	for _, root := range roots {
		if !filepath.IsAbs(root) {
			return fmt.Errorf("root %q must be absolute", root)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE roots SET enabled=0"); err != nil {
		return err
	}
	for _, root := range roots {
		if _, err := tx.ExecContext(ctx, "INSERT INTO roots(path,enabled) VALUES(?,1) ON CONFLICT(path) DO UPDATE SET enabled=1", []byte(filepath.Clean(root))); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type Summary struct {
	Schema              int    `json:"schema"`
	SQLiteVersion       string `json:"sqlite_version"`
	EnabledRoots        int64  `json:"enabled_roots"`
	Entries             int64  `json:"entries"`
	PendingJobs         int64  `json:"pending_jobs"`
	RunningJobs         int64  `json:"running_jobs"`
	CompleteDirectories int64  `json:"complete_directories"`
	DirectoryErrors     int64  `json:"directory_errors"`
	SkippedEntries      int64  `json:"skipped_entries"`
	DatabaseBytes       int64  `json:"database_bytes"`
	WALBytes            int64  `json:"wal_bytes"`
	NeedsBackpressure   bool   `json:"needs_backpressure"`
}

func (s *Store) Summary(ctx context.Context) (Summary, error) {
	result := Summary{Schema: s.schema}
	// One statement supplies a consistent snapshot without retaining a reader lock.
	err := s.db.QueryRowContext(ctx, `SELECT sqlite_version(),
 (SELECT count(*) FROM roots WHERE enabled=1), (SELECT count(*) FROM entries),
 (SELECT count(*) FROM jobs WHERE status='pending'), (SELECT count(*) FROM jobs WHERE status='running'),
 (SELECT count(*) FROM directories WHERE complete=1 AND last_error=''),
 (SELECT count(*) FROM directories WHERE last_error!=''),
 (SELECT count(*) FROM entries WHERE skip_reason!='')`).Scan(&result.SQLiteVersion, &result.EnabledRoots, &result.Entries, &result.PendingJobs, &result.RunningJobs, &result.CompleteDirectories, &result.DirectoryErrors, &result.SkippedEntries)
	if err != nil {
		return result, err
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return result, err
	}
	result.DatabaseBytes = info.Size()
	if info, err := os.Stat(s.path + "-wal"); err == nil {
		result.WALBytes = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	result.NeedsBackpressure = result.WALBytes > WALBackpressureBytes
	return result, nil
}

type Checkpoint struct{ Busy, LogPages, CheckpointedPages int }

// Checkpoint is passive: readers can keep using their snapshots. The scheduler
// must apply backpressure when a reader prevents WAL recycling; do not busy-loop
// or run VACUUM in a scan loop. Automatic checkpoints also run every 1000 pages.
func (s *Store) Checkpoint(ctx context.Context) (Checkpoint, error) {
	var result Checkpoint
	if s.readOnly {
		return result, errors.New("state is read-only")
	}
	err := s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&result.Busy, &result.LogPages, &result.CheckpointedPages)
	return result, err
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
		if s.lock != nil {
			s.closeErr = errors.Join(s.closeErr, s.lock.Close())
		}
	})
	return s.closeErr
}
