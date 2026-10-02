package state

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) HasSubtreeRetirement(ctx context.Context) (bool, error) {
	var pending bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM subtree_reconcile) OR EXISTS(SELECT 1 FROM subtree_retirement)`).Scan(&pending)
	return pending, err
}

// RetireSubtrees only touches rebuildable database records. Discovery examines
// at most 128 saved children; a purge removes at most 128 rows from one table.
// All inventory jobs (including delayed retries) must finish before maintenance.
// A separate scan revision fences new observations without confusing our own
// allocated-cache invalidations with filesystem scan commits.
func (s *Store) RetireSubtrees(ctx context.Context) (bool, error) {
	if s.readOnly {
		return false, errors.New("state is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var root, revision, current int64
	var path []byte
	var preserve bool
	var phase int
	err = tx.QueryRowContext(ctx, `SELECT t.root_id,t.path,t.scan_revision,t.preserve_entry,t.phase,v.scan_revision
 FROM subtree_retirement t JOIN allocation_revisions v ON v.root_id=t.root_id JOIN roots r ON r.id=t.root_id
 WHERE r.enabled=1 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.root_id=t.root_id AND j.kind=?)
 ORDER BY t.root_id,t.path LIMIT 1`, ScanKind).Scan(&root, &path, &revision, &preserve, &phase, &current)
	if err == nil {
		if revision != current {
			// New scans own any reappearing entries. Their completed parent passes
			// enqueue fresh reconciliation; this old proof must not remove them.
			_, err = tx.ExecContext(ctx, "DELETE FROM subtree_retirement WHERE root_id=? AND path=?", root, path)
		} else {
			err = purgeSubtreeStep(ctx, tx, root, path, preserve, phase)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		worked, e := reconcileSubtreeStep(ctx, tx)
		if e != nil || !worked {
			return false, e
		}
		err = nil
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func reconcileSubtreeStep(ctx context.Context, tx *sql.Tx) (bool, error) {
	var root, generation, savedGeneration, revision int64
	var path, cursor []byte
	var complete bool
	var fault string
	err := tx.QueryRowContext(ctx, `SELECT t.root_id,t.path,t.generation,t.cursor,
 COALESCE(d.generation,0),COALESCE(d.complete,0),COALESCE(d.last_error,''),v.scan_revision
 FROM subtree_reconcile t JOIN roots r ON r.id=t.root_id JOIN allocation_revisions v ON v.root_id=t.root_id
 LEFT JOIN directories d ON d.root_id=t.root_id AND d.path=t.path
 WHERE r.enabled=1 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.root_id=t.root_id AND j.kind=?)
 ORDER BY t.root_id,t.path LIMIT 1`, ScanKind).Scan(&root, &path, &generation, &cursor, &savedGeneration, &complete, &fault, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	finish := func() (bool, error) {
		_, err := tx.ExecContext(ctx, "DELETE FROM subtree_reconcile WHERE root_id=? AND path=?", root, path)
		return true, err
	}
	if generation != savedGeneration || !complete || fault != "" {
		return finish()
	}
	if cursor == nil {
		cursor = []byte{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.path,e.generation,e.kind,
 EXISTS(SELECT 1 FROM directories d WHERE d.root_id=e.root_id AND d.path=e.path)
 OR EXISTS(SELECT 1 FROM compact_dirs c WHERE c.root_id=e.root_id AND c.path=e.path)
 OR EXISTS(SELECT 1 FROM allocation_cache c WHERE c.root_id=e.root_id AND c.path=e.path)
 FROM entries e WHERE e.root_id=? AND e.parent=? AND e.path>? ORDER BY e.path LIMIT ?`, root, path, cursor, MaxBatchEntries)
	if err != nil {
		return false, err
	}
	type child struct {
		path           []byte
		generation     int64
		kind           string
		directoryState bool
	}
	var children []child
	for rows.Next() {
		var c child
		if err = rows.Scan(&c.path, &c.generation, &c.kind, &c.directoryState); err != nil {
			rows.Close()
			return false, err
		}
		children = append(children, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	if len(children) == 0 {
		return finish()
	}
	for _, c := range children {
		absent := c.generation != generation
		if absent || (c.kind != "directory" && c.directoryState) {
			_, err = tx.ExecContext(ctx, `INSERT INTO subtree_retirement(root_id,path,scan_revision,preserve_entry) VALUES(?,?,?,?)
 ON CONFLICT(root_id,path) DO UPDATE SET scan_revision=excluded.scan_revision,preserve_entry=excluded.preserve_entry,phase=0`, root, c.path, revision, !absent)
			if err != nil {
				return false, err
			}
		}
		cursor = c.path
	}
	_, err = tx.ExecContext(ctx, "UPDATE subtree_reconcile SET cursor=? WHERE root_id=? AND path=?", cursor, root, path)
	return true, err
}

// Keep the anchor entry until its payload and descendants are retired. If a
// scan interrupts maintenance, the remaining anchor can be reconciled again.
// Replacement file/symlink entries always survive; only their old directory
// payload is retired. Table/key names below are fixed, never supplied by paths.
func purgeSubtreeStep(ctx context.Context, tx *sql.Tx, root int64, path []byte, preserve bool, phase int) error {
	if string(path) == "." || !validRelative(path) {
		return errors.New("invalid subtree retirement scope")
	}
	tables := []struct{ table, pathColumn, key string }{
		{"allocation_members", "scope", "scope,path"},
		{"allocation_identities", "scope", "scope,device,inode"},
		{"allocation_cache", "path", "path"},
		{"compact_inodes", "path", "path,generation,device,inode"},
		{"compact_retirement", "path", "path,generation"},
		{"compact_dirs", "path", "path"},
		{"directories", "path", "path"},
		{"subtree_reconcile", "path", "path"},
		{"entries", "path", "id"},
		{"subtree_retirement", "path", "path"},
	}
	if phase < 0 || phase > len(tables) {
		return errors.New("invalid subtree retirement phase")
	}
	if phase == len(tables) {
		_, err := tx.ExecContext(ctx, "DELETE FROM subtree_retirement WHERE root_id=? AND path=?", root, path)
		return err
	}
	table := tables[phase]
	prefix, upper := []byte(string(path)+"/"), []byte(string(path)+"0")
	remove := func(predicate string, bounds ...any) (int64, error) {
		args := append([]any{root, root}, bounds...)
		args = append(args, MaxBatchEntries)
		res, err := tx.ExecContext(ctx, "DELETE FROM "+table.table+" WHERE root_id=? AND ("+table.key+") IN (SELECT "+table.key+
			" FROM "+table.table+" WHERE root_id=? AND "+predicate+" ORDER BY "+table.pathColumn+" DESC LIMIT ?)", args...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	// Separate contiguous descendant and exact-anchor seeks keep the LIMIT
	// index-backed without sorting/filtering a whole OR-selected subtree.
	n, err := remove(table.pathColumn+">=? AND "+table.pathColumn+"<?", prefix, upper)
	if err == nil && n == 0 && !(table.table == "entries" && preserve) && table.table != "subtree_retirement" {
		n, err = remove(table.pathColumn+"=?", path)
	}
	if err != nil {
		return err
	}
	if n > 0 {
		// Coverage can change even when previously excluded rows contributed no
		// bytes. Invalidate caches, but retain the separate scan proof revision.
		_, err = tx.ExecContext(ctx, "UPDATE allocation_revisions SET revision=revision+1 WHERE root_id=?", root)
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE subtree_retirement SET phase=phase+1 WHERE root_id=? AND path=?", root, path)
	return err
}
