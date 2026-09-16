package state

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"
)

const ScanKind = "inventory"
const MaxBatchEntries = 128

// Entry paths are bytes so non-UTF-8 Unix names round-trip through SQLite.
type Entry struct {
	Path                              []byte
	Kind, Device, Inode, SkipReason   string
	Size, Allocated, MtimeNS, CtimeNS int64
}

// A directory's complete marker is a reconciliation watermark. An entry with
// another generation is unconfirmed in that pass, not an authorized deletion.
// No bulk descendant deletion or unbounded sweep is performed at commit time.
type ScanBatch struct {
	Identity   string
	Generation int64
	Directory  Entry
	Entries    []Entry
	Complete   bool
	Cursor     []byte
	Fault      string
}

func validRelative(p []byte) bool {
	s := string(p)
	return len(s) > 0 && len(s) <= 4096 && !strings.ContainsRune(s, 0) && !filepath.IsAbs(s) && filepath.Clean(s) == s && s != ".." && !strings.HasPrefix(s, "../")
}

// SeedInventory starts a new pass only for roots without unfinished inventory
// work, including running jobs and delayed retries. The exclusive writer lock
// serializes this decision with job commits.
func (s *Store) SeedInventory(ctx context.Context) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs(root_id,kind,path,due_at_ns)
 SELECT r.id,?,X'2e',? FROM roots r WHERE r.enabled=1
 AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.root_id=r.id AND j.kind=?)
 AND NOT EXISTS (SELECT 1 FROM compact_retirement c WHERE c.root_id=r.id)
 ON CONFLICT(root_id,kind,path) DO NOTHING`, ScanKind, time.Now().UnixNano(), ScanKind)
	return err
}

// CommitScan atomically fences the lease, upserts the observed batch, enqueues
// children, and saves enumeration progress. A crash cannot commit one without
// the others. Call only from the worker's owning event loop.
func (s *Store) CommitScan(ctx context.Context, j Job, b ScanBatch) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	if j.Kind != ScanKind || !validRelative(j.Path) || len(b.Entries) > MaxBatchEntries || len(b.Cursor) > MaxCursorBytes || len(b.Fault) > 2048 {
		return errors.New("invalid inventory batch")
	}
	if b.Fault == "" {
		if b.Identity == "" || len(b.Identity) > 4096 || b.Generation <= 0 || string(b.Directory.Path) != string(j.Path) || b.Directory.Kind != "directory" {
			return errors.New("invalid directory observation")
		}
		for _, e := range b.Entries {
			if !validRelative(e.Path) || filepath.Dir(string(e.Path)) != string(j.Path) || string(e.Path) == string(j.Path) || e.Size < 0 || e.Allocated < 0 || len(e.SkipReason) > 256 {
				return errors.New("invalid child observation")
			}
		}
	} else if len(b.Entries) > 0 || b.Complete {
		return errors.New("failed enumeration cannot reconcile entries")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now()
	due := time.Unix(0, 1) // Continue this open directory before starting a child.
	if b.Fault != "" {
		due = now.Add(time.Hour)
	}
	if err := finishJob(ctx, tx, j, b.Complete, b.Cursor, due, b.Fault); err != nil {
		return err
	}
	if b.Fault != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO directories(root_id,path,last_error) VALUES(?,?,?)
 ON CONFLICT(root_id,path) DO UPDATE SET last_error=excluded.last_error`, j.RootID, j.Path, b.Fault)
		if err != nil {
			return err
		}
		if string(j.Path) == "." {
			if _, err = tx.ExecContext(ctx, "UPDATE roots SET last_error=? WHERE id=?", b.Fault, j.RootID); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	result, err := tx.ExecContext(ctx, "UPDATE roots SET volume_id=? WHERE id=? AND enabled=1 AND (volume_id='' OR volume_id=?)", b.Identity, j.RootID, b.Identity)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("root identity changed; batch rejected")
	}
	put := func(e Entry, parent []byte, generation int64) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns,skip_reason)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(root_id,path) DO UPDATE SET
 parent=excluded.parent,kind=excluded.kind,size=excluded.size,allocated=excluded.allocated,
 mtime_ns=excluded.mtime_ns,ctime_ns=excluded.ctime_ns,device=excluded.device,inode=excluded.inode,
 generation=excluded.generation,observed_at_ns=excluded.observed_at_ns,skip_reason=excluded.skip_reason`,
			j.RootID, e.Path, parent, e.Kind, e.Size, e.Allocated, e.MtimeNS, e.CtimeNS, e.Device, e.Inode, generation, now.UnixNano(), e.SkipReason)
		return err
	}
	// Only the root is its own observation. A child directory's generation
	// belongs to its parent's enumeration and must not be overwritten here.
	if string(j.Path) == "." {
		if err := put(b.Directory, []byte(""), b.Generation); err != nil {
			return err
		}
	}
	compact, err := commitCompact(ctx, tx, j, b)
	if err != nil {
		return err
	}
	for _, e := range b.Entries {
		if compact && e.Kind == "file" {
			continue
		}
		if err := put(e, j.Path, b.Generation); err != nil {
			return err
		}
		if e.Kind == "directory" && e.SkipReason == "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(?,?,?,?) ON CONFLICT(root_id,kind,path) DO NOTHING`, j.RootID, ScanKind, e.Path, now.UnixNano()); err != nil {
				return err
			}
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO directories(root_id,path,generation,complete,checked_at_ns,last_error) VALUES(?,?,?,?,?,'')
 ON CONFLICT(root_id,path) DO UPDATE SET generation=excluded.generation,complete=excluded.complete,checked_at_ns=excluded.checked_at_ns,last_error=''`, j.RootID, j.Path, b.Generation, b.Complete, now.UnixNano())
	if err != nil {
		return err
	}
	if string(j.Path) == "." && b.Complete {
		if _, err = tx.ExecContext(ctx, "UPDATE roots SET last_scan_ns=?,last_error='' WHERE id=?", now.UnixNano(), j.RootID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
