package state

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"strings"
)

func generatedDirectory(root, path []byte) bool {
	if filepath.Base(string(root)) == "node_modules" {
		return true
	}
	for _, part := range strings.Split(string(path), string(filepath.Separator)) {
		if part == "node_modules" {
			return true
		}
	}
	return false
}

// ConfigureCompact pins the selected storage mode until both inventory and
// retirement work have finished. Nil keeps the saved mode (default detailed).
func (s *Store) ConfigureCompact(ctx context.Context, requested *bool) (bool, error) {
	var value []byte
	err := s.db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key='inventory.compact'").Scan(&value)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if len(value) > 0 && string(value) != "0" && string(value) != "1" {
		return false, errors.New("invalid saved compact inventory mode")
	}
	enabled := string(value) == "1"
	if requested == nil || *requested == enabled {
		return enabled, nil
	}
	var pending bool
	if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE kind=?) OR EXISTS(SELECT 1 FROM compact_retirement)`, ScanKind).Scan(&pending); err != nil {
		return false, err
	}
	if pending {
		return enabled, errors.New("finish the pending scan before changing --compact/--detailed mode; rerun scan without a mode flag to resume")
	}
	if s.readOnly {
		return enabled, errors.New("state is read-only")
	}
	value = []byte("0")
	if *requested {
		value = []byte("1")
	}
	_, err = s.db.ExecContext(ctx, "INSERT INTO settings(key,value) VALUES('inventory.compact',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", value)
	return *requested, err
}

func addCompact(total *int64, n int64) error {
	if n < 0 || *total > math.MaxInt64-n {
		return errors.New("compact inventory total exceeds supported range")
	}
	*total += n
	return nil
}

// commitCompact shares CommitScan's transaction and lease fence. No file paths
// are retained here. A new enumeration generation replaces this directory's
// totals immediately; old inode generations are ignored and retired separately.
func commitCompact(ctx context.Context, tx *sql.Tx, j Job, b ScanBatch) (bool, error) {
	var enabled bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key='inventory.compact' AND value=X'31')`).Scan(&enabled); err != nil {
		return false, err
	}
	compact := enabled && generatedDirectory(j.RootPath, j.Path)
	if !compact {
		var oldGeneration int64
		err := tx.QueryRowContext(ctx, "DELETE FROM compact_dirs WHERE root_id=? AND path=? RETURNING generation", j.RootID, j.Path).Scan(&oldGeneration)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO compact_retirement VALUES(?,?,?) ON CONFLICT DO NOTHING", j.RootID, j.Path, oldGeneration)
		return false, err
	}
	var generation, logical, files, unknown, skipped int64
	err := tx.QueryRowContext(ctx, `SELECT generation,logical,files,unknown_inodes,skipped_files FROM compact_dirs WHERE root_id=? AND path=?`, j.RootID, j.Path).Scan(&generation, &logical, &files, &unknown, &skipped)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if generation != b.Generation {
		logical, files, unknown, skipped = 0, 0, 0, 0
	}
	for _, e := range b.Entries {
		if e.Kind != "file" {
			continue
		}
		if err = addCompact(&logical, e.Size); err != nil {
			return false, err
		}
		if err = addCompact(&files, 1); err != nil {
			return false, err
		}
		if e.SkipReason != "" {
			skipped++
		}
		if e.Device == "" || e.Inode == "" {
			unknown++
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO compact_inodes VALUES(?,?,?,?,?,?,?,1,0)
 ON CONFLICT(root_id,path,generation,device,inode) DO UPDATE SET
 paths=paths+1,allocated=max(allocated,excluded.allocated),
 conflicting=conflicting OR logical!=excluded.logical OR allocated!=excluded.allocated`, j.RootID, j.Path, b.Generation, e.Device, e.Inode, e.Allocated, e.Size)
		if err != nil {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO compact_dirs VALUES(?,?,?,?,?,?,?) ON CONFLICT(root_id,path) DO UPDATE SET
 generation=excluded.generation,logical=excluded.logical,files=excluded.files,
 unknown_inodes=excluded.unknown_inodes,skipped_files=excluded.skipped_files`, j.RootID, j.Path, b.Generation, logical, files, unknown, skipped)
	if err != nil {
		return false, err
	}
	if generation != b.Generation {
		_, err = tx.ExecContext(ctx, "INSERT INTO compact_retirement VALUES(?,?,?) ON CONFLICT DO NOTHING", j.RootID, j.Path, generation)
	}
	return true, err
}

func (s *Store) HasCompactRetirement(ctx context.Context) (bool, error) {
	var pending bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM compact_retirement)").Scan(&pending)
	return pending, err
}

// RetireCompact removes at most 128 rebuildable records per call. Current
// generation inode evidence and newly detailed files are never removed.
func (s *Store) RetireCompact(ctx context.Context) (bool, error) {
	if s.readOnly {
		return false, errors.New("state is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var root, generation int64
	var path []byte
	err = tx.QueryRowContext(ctx, "SELECT root_id,path,generation FROM compact_retirement ORDER BY root_id,path,generation LIMIT 1").Scan(&root, &path, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE id IN (
 SELECT e.id FROM entries e JOIN compact_dirs c ON c.root_id=e.root_id AND c.path=e.parent
 WHERE e.root_id=? AND e.parent=? AND e.kind='file' LIMIT ?)`, root, path, MaxBatchEntries)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		res, err = tx.ExecContext(ctx, `DELETE FROM compact_inodes WHERE (root_id,path,generation,device,inode) IN (
 SELECT i.root_id,i.path,i.generation,i.device,i.inode FROM compact_inodes i
 WHERE i.root_id=? AND i.path=? AND i.generation=? AND NOT EXISTS (
 SELECT 1 FROM compact_dirs c WHERE c.root_id=i.root_id AND c.path=i.path AND c.generation=i.generation) LIMIT ?)`, root, path, generation, MaxBatchEntries)
		if err != nil {
			return false, err
		}
		n, err = res.RowsAffected()
		if err != nil {
			return false, err
		}
		if n == 0 {
			if _, err = tx.ExecContext(ctx, "DELETE FROM compact_retirement WHERE root_id=? AND path=? AND generation=?", root, path, generation); err != nil {
				return false, err
			}
		}
	}
	return true, tx.Commit()
}
