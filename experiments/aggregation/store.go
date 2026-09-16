// Package aggregation is a storage experiment, not the production scanner.
// It deliberately has no filesystem traversal or application migration hooks.
package aggregation

import (
	"context"
	"database/sql"
	"errors"
	"math"
)

const BatchLimit = 128

// Keep per-directory generations separate. Re-enumerating one directory must
// replace its contribution rather than add it again or erase other directories.
const Schema = `
CREATE TABLE aggregate_dirs (
 id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, next_batch INTEGER NOT NULL,
 files INTEGER NOT NULL, logical INTEGER NOT NULL, complete INTEGER NOT NULL
);
CREATE TABLE aggregate_inodes (
 directory INTEGER NOT NULL, generation INTEGER NOT NULL,
 device TEXT NOT NULL, inode TEXT NOT NULL,
 paths INTEGER NOT NULL, logical INTEGER NOT NULL, allocated INTEGER NOT NULL,
 conflicting INTEGER NOT NULL,
 PRIMARY KEY(directory,generation,device,inode)
) WITHOUT ROWID;
`

type File struct {
	Device, Inode      string
	Logical, Allocated int64
}

type Batch struct {
	Directory, Generation, Ordinal int64
	Complete                       bool
	Files                          []File
}

// Commit is called in the same transaction as the future job cursor update.
// The caller must roll back on any error, including a later lease-fence failure.
// Ordinals reject replay of an already committed batch. A restarted directory
// begins with a fresh generation and ordinal zero. Old generations are inert.
func Commit(ctx context.Context, tx *sql.Tx, b Batch) error {
	if b.Directory <= 0 || b.Generation <= 0 || b.Ordinal < 0 || len(b.Files) > BatchLimit {
		return errors.New("invalid aggregate batch")
	}
	for _, f := range b.Files {
		if f.Device == "" || f.Inode == "" || f.Logical < 0 || f.Allocated < 0 {
			return errors.New("unsupported file identity or size")
		}
	}
	var generation, next, files, logical int64
	var complete bool
	err := tx.QueryRowContext(ctx, `SELECT generation,next_batch,files,logical,complete FROM aggregate_dirs WHERE id=?`, b.Directory).Scan(&generation, &next, &files, &logical, &complete)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if generation != b.Generation {
		if b.Ordinal != 0 {
			return errors.New("new generation must start at batch zero")
		}
		// The production caller must additionally fence the directory generation
		// with its current job lease; this experiment does not implement leases.
		next, files, logical, complete = 0, 0, 0, false
	}
	if complete || next != b.Ordinal || next == math.MaxInt64 {
		return errors.New("stale or repeated aggregate batch")
	}
	if files > math.MaxInt64-int64(len(b.Files)) {
		return errors.New("file count overflow")
	}
	files += int64(len(b.Files))
	for _, f := range b.Files {
		if logical > math.MaxInt64-f.Logical {
			return errors.New("logical size overflow")
		}
		logical += f.Logical
		_, err = tx.ExecContext(ctx, `INSERT INTO aggregate_inodes VALUES(?,?,?,?,1,?,?,0)
 ON CONFLICT(directory,generation,device,inode) DO UPDATE SET
 paths=paths+1, allocated=max(allocated,excluded.allocated),
 conflicting=conflicting OR logical!=excluded.logical OR allocated!=excluded.allocated`, b.Directory, b.Generation, f.Device, f.Inode, f.Logical, f.Allocated)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO aggregate_dirs VALUES(?,?,?,?,?,?)
 ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,next_batch=excluded.next_batch,
 files=excluded.files,logical=excluded.logical,complete=excluded.complete`, b.Directory, b.Generation, b.Ordinal+1, files, logical, b.Complete)
	return err
}

// Collect retires at most one batch of inode rows from one obsolete generation.
// It never touches the current generation. Production needs a durable retirement
// queue so even directories that shrink or disappear eventually release state.
func Collect(ctx context.Context, tx *sql.Tx, directory, generation int64) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM aggregate_inodes WHERE (directory,generation,device,inode) IN (
 SELECT i.directory,i.generation,i.device,i.inode FROM aggregate_inodes i
 JOIN aggregate_dirs d ON d.id=i.directory
 WHERE i.directory=? AND i.generation=? AND i.generation!=d.generation LIMIT ?)`, directory, generation, BatchLimit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

type Totals struct {
	Files, Logical, Allocated, UniqueInodes int64
	Complete, Conflicting                   bool
}

// Measure is an experimental oracle for the storage comparison, not a bounded
// product report. Its GROUP BY can inspect the full ledger. Production must
// reduce inode contributions incrementally under the normal resource budget.
func Measure(ctx context.Context, db *sql.DB) (Totals, error) {
	var r Totals
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(files),0),COALESCE(sum(logical),0),count(*)>0 AND min(complete)=1 FROM aggregate_dirs`).Scan(&r.Files, &r.Logical, &r.Complete)
	if err != nil {
		return r, err
	}
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(allocated),0),count(*),COALESCE(max(conflict),0) FROM (
 SELECT max(i.allocated) AS allocated,
 max(i.conflicting) OR min(i.logical)!=max(i.logical) OR min(i.allocated)!=max(i.allocated) AS conflict
 FROM aggregate_inodes i JOIN aggregate_dirs d ON d.id=i.directory AND d.generation=i.generation
 GROUP BY i.device,i.inode)`).Scan(&r.Allocated, &r.UniqueInodes, &r.Conflicting)
	if err != nil {
		return r, err
	}
	return r, tx.Commit()
}
