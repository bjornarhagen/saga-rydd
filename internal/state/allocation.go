package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
)

// Root-wide revisions deliberately invalidate every scope together: hardlinks
// and ancestor membership can cross directory boundaries. The revision shares
// the scanner's transaction/lease fence, including failures and detailed passes.
func invalidateAllocations(ctx context.Context, tx *sql.Tx, j Job) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO allocation_revisions(root_id,revision,scan_revision) VALUES(?,1,1)
 ON CONFLICT(root_id) DO UPDATE SET revision=revision+1,scan_revision=scan_revision+1`, j.RootID); err != nil {
		return err
	}
	var compact bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key='inventory.compact' AND value=X'31')`).Scan(&compact); err != nil {
		return err
	}
	if !compact {
		return nil
	}
	paths := []string{"."}
	if filepath.Base(string(j.RootPath)) != "node_modules" {
		parts := strings.Split(string(j.Path), string(filepath.Separator))
		for i, part := range parts {
			if part == "node_modules" {
				paths = append(paths, filepath.Join(parts[:i+1]...))
				break
			}
		}
	}
	for _, path := range paths {
		if _, err := tx.ExecContext(ctx, `INSERT INTO allocation_cache(root_id,path) VALUES(?,?) ON CONFLICT DO NOTHING`, j.RootID, []byte(path)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) HasAllocationWork(ctx context.Context) (bool, error) {
	var pending bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM allocation_cache c
 JOIN allocation_revisions v ON v.root_id=c.root_id WHERE c.revision!=v.revision OR c.phase!='done')`).Scan(&pending)
	return pending, err
}

type allocationWork struct {
	root, revision, examined, allocated, repeated int64
	path, cursor                                  []byte
	phase, device, inode                          string
	unknown, conflicting, ready                   bool
	coverage                                      scopeCoverage
}

// Seek the exact pending prefix rather than walking completed scope members.
// The partial index omits excluded, ordinary-file and noncompact members.
const allocationPendingMemberQuery = `SELECT path,generation FROM allocation_members INDEXED BY allocation_members_pending
 WHERE root_id=? AND scope=? AND done=0 AND excluded=0 AND kind='directory' AND generation>0 ORDER BY path LIMIT 1`

// ReduceAllocations advances one saved scope in a short transaction. A step
// processes at most 128 inventory rows, 128 inode contributions, or 128 scratch
// rows for cleanup. Even one inode linked across many directories is resumable.
// Reports never call this writer. Retained caches are scalars; deduplication
// scratch is retired durably after publication or invalidation.
func (s *Store) ReduceAllocations(ctx context.Context) (bool, error) {
	if s.readOnly {
		return false, errors.New("state is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var w allocationWork
	var current int64
	var coverage []byte
	err = tx.QueryRowContext(ctx, `SELECT c.root_id,c.path,c.revision,c.phase,c.entry_cursor,c.inode_device,c.inode_number,
 c.examined,c.allocated,c.repeated,c.unknown,c.conflicting,c.ready,v.revision,c.coverage
 FROM allocation_cache c JOIN allocation_revisions v ON v.root_id=c.root_id JOIN roots r ON r.id=c.root_id
 WHERE r.enabled=1 AND (c.revision!=v.revision OR c.phase!='done')
 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.root_id=c.root_id AND j.kind=?)
 AND NOT EXISTS(SELECT 1 FROM subtree_reconcile t WHERE t.root_id=c.root_id)
 AND NOT EXISTS(SELECT 1 FROM subtree_retirement t WHERE t.root_id=c.root_id)
 ORDER BY c.root_id,c.path LIMIT 1`, ScanKind).Scan(&w.root, &w.path, &w.revision, &w.phase, &w.cursor, &w.device, &w.inode,
		&w.examined, &w.allocated, &w.repeated, &w.unknown, &w.conflicting, &w.ready, &current, &coverage)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(coverage) > 0 {
		if err := json.Unmarshal(coverage, &w.coverage); err != nil {
			return false, err
		}
	}
	if w.revision != current {
		w = allocationWork{root: w.root, path: w.path, revision: current, phase: "reset", cursor: []byte{}}
	}
	// SQLite's driver may scan an empty BLOB as a nil slice. Keep the lower
	// keyset bound an empty BLOB rather than binding SQL NULL on restart.
	if w.cursor == nil {
		w.cursor = []byte{}
	}
	var compact bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key='inventory.compact' AND value=X'31')`).Scan(&compact); err != nil {
		return false, err
	}
	if !compact {
		w.phase, w.ready = "cleanup", false
	}
	switch w.phase {
	case "reset", "cleanup":
		var removed int64
		// Delete only rebuildable scratch, never source inventory or action data.
		for _, table := range []string{"allocation_members", "allocation_identities"} {
			key := "path"
			if table == "allocation_identities" {
				key = "device,inode"
			}
			res, e := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE root_id=? AND scope=? AND (`+key+`) IN
 (SELECT `+key+` FROM `+table+` WHERE root_id=? AND scope=? LIMIT ?)`, w.root, w.path, w.root, w.path, MaxBatchEntries-removed)
			if e != nil {
				return false, e
			}
			n, e := res.RowsAffected()
			if e != nil {
				return false, e
			}
			removed += n
		}
		if removed == 0 {
			if w.phase == "reset" {
				w.phase = "entries"
			} else {
				w.phase = "done"
			}
		}
	case "entries":
		err = w.entries(ctx, tx)
	case "inodes":
		err = w.inodes(ctx, tx)
	}
	if err != nil {
		return false, err
	}
	if !compact && w.phase == "done" {
		if _, err = tx.ExecContext(ctx, "DELETE FROM allocation_cache WHERE root_id=? AND path=?", w.root, w.path); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	coverage, err = json.Marshal(w.coverage)
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE allocation_cache SET revision=?,phase=?,entry_cursor=?,inode_device=?,inode_number=?,
 examined=?,allocated=?,repeated=?,unknown=?,conflicting=?,ready=?,coverage=? WHERE root_id=? AND path=?`,
		w.revision, w.phase, w.cursor, w.device, w.inode, w.examined, w.allocated, w.repeated, w.unknown, w.conflicting, w.ready, coverage, w.root, w.path)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (w *allocationWork) entries(ctx context.Context, tx *sql.Tx) error {
	// Keep selection and membership equivalent to MeasureDirectory, including
	// legacy overlap suppression. Total coverage is durable; each batch stays bounded.
	q := `SELECT e.path,e.parent,e.kind,e.generation,e.device,e.inode,e.size,e.allocated,
 COALESCE(p.generation,0),COALESCE(p.complete,0),COALESCE(p.last_error,''),COALESCE(c.generation,0),COALESCE(c.unknown_inodes,0),
 e.observed_at_ns,e.skip_reason,COALESCE(d.complete,0),COALESCE(d.checked_at_ns,0),COALESCE(d.last_error,''),
 COALESCE(c.logical,0),COALESCE(c.files,0),COALESCE(c.skipped_files,0)
 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent
 LEFT JOIN compact_dirs c ON c.root_id=e.root_id AND c.path=e.path
 LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.path
 WHERE e.root_id=? AND e.path>? AND (e.kind!='file' OR NOT EXISTS
 (SELECT 1 FROM compact_dirs c WHERE c.root_id=e.root_id AND c.path=e.parent))`
	args := []any{w.root, w.cursor}
	if string(w.path) != "." {
		q += ` AND (e.path=? OR (e.path>=? AND e.path<?))`
		args = append(args, w.path, []byte(string(w.path)+"/"), []byte(string(w.path)+"0"))
	}
	q += ` ORDER BY e.path LIMIT ?`
	args = append(args, MaxBatchEntries)
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	var batch []coverageEntry
	for rows.Next() {
		var e coverageEntry
		if err = rows.Scan(&e.path, &e.parent, &e.kind, &e.generation, &e.dev, &e.ino, &e.size, &e.allocated,
			&e.parentGeneration, &e.parentComplete, &e.parentError, &e.compactGeneration, &e.unknown,
			&e.observed, &e.skip, &e.dirComplete, &e.checked, &e.dirError, &e.compactLogical, &e.compactFiles, &e.compactSkipped); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		w.phase = "inodes"
	}
	for _, e := range batch {
		if err := addCompact(&w.examined, 1); err != nil {
			return err
		}
		w.cursor = e.path
		excluded := false
		confirmed := true
		if string(e.path) != string(w.path) {
			var parentExcluded, parentConfirmed bool
			var parentKind string
			err := tx.QueryRowContext(ctx, `SELECT excluded,kind,confirmed FROM allocation_members WHERE root_id=? AND scope=? AND path=?`,
				w.root, w.path, e.parent).Scan(&parentExcluded, &parentKind, &parentConfirmed)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			confirmed = err == nil && parentConfirmed && e.generation == e.parentGeneration
			excluded = (err == nil && (parentExcluded || parentKind != "directory")) ||
				(e.parentGeneration > 0 && e.parentComplete && e.parentError == "" && e.generation != e.parentGeneration)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO allocation_members(root_id,scope,path,excluded,kind,generation,confirmed) VALUES(?,?,?,?,?,?,?)`,
			w.root, w.path, e.path, excluded, e.kind, e.compactGeneration, confirmed && e.skip == "" && e.kind == "directory"); err != nil {
			return err
		}
		if err := w.coverage.add(e, string(e.path) == string(w.path), excluded, confirmed); err != nil {
			return err
		}
		if excluded {
			continue
		}
		if e.kind == "directory" && e.compactGeneration > 0 && e.unknown > 0 {
			w.unknown = true
		}
		if e.kind == "file" {
			if err := w.contribute(ctx, tx, e.dev, e.ino, e.allocated, e.size, 1, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *allocationWork) inodes(ctx context.Context, tx *sql.Tx) error {
	var path []byte
	var generation int64
	err := tx.QueryRowContext(ctx, allocationPendingMemberQuery,
		w.root, w.path).Scan(&path, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		w.ready, w.phase = true, "cleanup"
		return nil
	}
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT device,inode,allocated,logical,paths,conflicting FROM compact_inodes
 WHERE root_id=? AND path=? AND generation=? AND (device,inode)>(?,?) ORDER BY device,inode LIMIT ?`,
		w.root, path, generation, w.device, w.inode, MaxBatchEntries)
	if err != nil {
		return err
	}
	type contribution struct {
		dev, ino                  string
		allocated, logical, paths int64
		conflict                  bool
	}
	var batch []contribution
	for rows.Next() {
		var c contribution
		if err = rows.Scan(&c.dev, &c.ino, &c.allocated, &c.logical, &c.paths, &c.conflict); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, c := range batch {
		if err := w.contribute(ctx, tx, c.dev, c.ino, c.allocated, c.logical, c.paths, c.conflict); err != nil {
			return err
		}
		w.device, w.inode = c.dev, c.ino
	}
	if len(batch) < MaxBatchEntries {
		_, err = tx.ExecContext(ctx, `UPDATE allocation_members SET done=1 WHERE root_id=? AND scope=? AND path=?`, w.root, w.path, path)
		w.device, w.inode = "", ""
	}
	return err
}

func (w *allocationWork) contribute(ctx context.Context, tx *sql.Tx, dev, ino string, allocated, logical, paths int64, conflict bool) error {
	if paths < 1 {
		return errors.New("invalid allocated contribution path count")
	}
	w.conflicting = w.conflicting || conflict
	if dev == "" || ino == "" {
		// Ordinary unknown identities retain the report's per-path accounting.
		// Compact unknown evidence is flagged separately at its directory row.
		return addCompact(&w.allocated, allocated)
	}
	var priorAllocated, priorLogical int64
	err := tx.QueryRowContext(ctx, `SELECT allocated,logical FROM allocation_identities
 WHERE root_id=? AND scope=? AND device=? AND inode=?`, w.root, w.path, dev, ino).Scan(&priorAllocated, &priorLogical)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	repeated := paths - 1
	if err == nil {
		repeated++
		w.conflicting = w.conflicting || priorAllocated != allocated || priorLogical != logical
	}
	if err := addCompact(&w.repeated, repeated); err != nil {
		return err
	}
	if allocated > priorAllocated {
		if err := addCompact(&w.allocated, allocated-priorAllocated); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO allocation_identities VALUES(?,?,?,?,?,?)
 ON CONFLICT(root_id,scope,device,inode) DO UPDATE SET allocated=max(allocated,excluded.allocated)`,
		w.root, w.path, dev, ino, allocated, logical)
	return err
}
