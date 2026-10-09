package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"time"
)

const backgroundInventoryEpochKey = "inventory.background.mode_epoch_v1"

var (
	ErrBackgroundInventoryInput       = errors.New("invalid configured inventory storage scope")
	ErrBackgroundInventoryCorrupt     = errors.New("invalid saved configured inventory mode evidence")
	ErrBackgroundInventoryModePending = errors.New("finish the current inventory pass and saved maintenance before changing compact_inventory")
)

// BackgroundInventoryScope binds the selected storage mode and at most 32
// exact configured roots to this open store. It conveys no source authority.
type BackgroundInventoryScope struct {
	roots   FairInventoryRoots
	compact bool
	epoch   int64
}

func (scope BackgroundInventoryScope) Compact() bool { return scope.compact }
func (scope BackgroundInventoryScope) RootIDs() []int64 {
	ids := make([]int64, len(scope.roots.roots))
	for i, r := range scope.roots.roots {
		ids[i] = r.id
	}
	return ids
}

func readBackgroundInventoryMode(ctx context.Context, tx *sql.Tx) (bool, error) {
	var mode []byte
	var kind string
	err := tx.QueryRowContext(ctx, "SELECT substr(value,1,2),typeof(value) FROM settings WHERE key='inventory.compact'").Scan(&mode, &kind)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if kind != "blob" || (string(mode) != "0" && string(mode) != "1") {
		return false, ErrBackgroundInventoryCorrupt
	}
	return string(mode) == "1", nil
}
func readBackgroundInventoryEpoch(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw []byte
	var kind string
	err := tx.QueryRowContext(ctx, "SELECT substr(value,1,20),typeof(value) FROM settings WHERE key=?", backgroundInventoryEpochKey).Scan(&raw, &kind)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, e := strconv.ParseInt(string(raw), 10, 64)
	if e != nil || kind != "blob" || n < 1 || strconv.FormatInt(n, 10) != string(raw) {
		return 0, ErrBackgroundInventoryCorrupt
	}
	return n, nil
}
func advanceBackgroundInventoryEpoch(ctx context.Context, tx *sql.Tx) (int64, error) {
	n, err := readBackgroundInventoryEpoch(ctx, tx)
	if err != nil {
		return 0, err
	}
	if n == math.MaxInt64 {
		return 0, ErrBackgroundInventoryCorrupt
	}
	n++
	_, err = tx.ExecContext(ctx, "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", backgroundInventoryEpochKey, []byte(strconv.FormatInt(n, 10)))
	return n, err
}
func inventoryClaim(ctx context.Context, tx *sql.Tx, j Job) error {
	if j.Kind != ScanKind {
		return nil
	}
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET inventory_claimed=1 WHERE id=? AND root_id=? AND kind=?
 AND typeof(inventory_claimed)='integer' AND inventory_claimed IN(0,1)`, j.ID, j.RootID, j.Kind)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrBackgroundInventoryCorrupt
	}
	return nil
}

// ConfigureBackgroundInventoryMode preserves existing jobs. A mode change
// admits only exact root listings whose durable provenance proves no claim.
// Old/unknown, interrupted, child, retry or out-of-scope work blocks conversion.
// Same-mode adoption resumes that work without weakening its provenance.
func (s *Store) ConfigureBackgroundInventoryMode(ctx context.Context, roots FairInventoryRoots, compact bool, now time.Time) (BackgroundInventoryScope, error) {
	n, err := fairInventoryNow(ctx, now)
	if err != nil || s.readOnly || s.schema < 14 || !s.validFairScope(roots) {
		return BackgroundInventoryScope{}, errors.Join(ErrBackgroundInventoryInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BackgroundInventoryScope{}, err
	}
	defer tx.Rollback()
	for _, root := range roots.roots {
		if err = checkBackgroundInventoryRoot(ctx, tx, root); err != nil {
			return BackgroundInventoryScope{}, err
		}
	}
	saved, err := readBackgroundInventoryMode(ctx, tx)
	if err != nil {
		return BackgroundInventoryScope{}, err
	}
	if saved != compact {
		if err = backgroundModeTransition(ctx, tx, roots); err != nil {
			return BackgroundInventoryScope{}, err
		}
		// A mode change resets learning without rewriting work. The subsequent
		// disabled adaptive configuration owns any exact weekly-to-daily cleanup.
		for _, root := range roots.roots {
			row, e := readAdaptiveRevisit(ctx, tx, root.id)
			if errors.Is(e, sql.ErrNoRows) {
				continue
			}
			if e != nil {
				return BackgroundInventoryScope{}, e
			}
			if n < row.maxNow {
				return BackgroundInventoryScope{}, ErrAdaptiveRevisitClock
			}
			row.unknown = true
			row.streak = 0
			row.maxNow = n
			if e = writeAdaptiveRevisit(ctx, tx, row); e != nil {
				return BackgroundInventoryScope{}, e
			}
		}
		value := []byte("0")
		if compact {
			value = []byte("1")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO settings(key,value) VALUES('inventory.compact',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", value); err != nil {
			return BackgroundInventoryScope{}, err
		}
	}
	epoch, err := advanceBackgroundInventoryEpoch(ctx, tx)
	if err != nil {
		return BackgroundInventoryScope{}, err
	}
	scope := BackgroundInventoryScope{roots: roots, compact: compact, epoch: epoch}
	if err = ctx.Err(); err != nil {
		return BackgroundInventoryScope{}, err
	}
	if err = tx.Commit(); err != nil {
		return BackgroundInventoryScope{}, err
	}
	if err = ctx.Err(); err != nil {
		return BackgroundInventoryScope{}, err
	}
	return scope, nil
}

func backgroundModeTransition(ctx context.Context, tx *sql.Tx, roots FairInventoryRoots) error {
	var maintenance bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM compact_retirement)
 OR EXISTS(SELECT 1 FROM subtree_reconcile) OR EXISTS(SELECT 1 FROM subtree_retirement)
 OR EXISTS(SELECT 1 FROM allocation_cache c LEFT JOIN allocation_revisions v ON v.root_id=c.root_id
 WHERE v.revision IS NULL OR c.revision!=v.revision OR c.phase!='done')`).Scan(&maintenance); err != nil {
		return err
	}
	if maintenance {
		return ErrBackgroundInventoryModePending
	}
	rows, err := tx.QueryContext(ctx, `SELECT
 CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,
 CASE WHEN typeof(root_id)='integer' THEN root_id ELSE NULL END,
 CASE WHEN typeof(path)='blob' THEN substr(path,1,4097) ELSE NULL END,
 substr(CAST(status AS BLOB),1,17),CASE WHEN typeof(due_at_ns)='integer' THEN due_at_ns ELSE NULL END,
 substr(cursor,1,1),CASE WHEN typeof(attempts)='integer' THEN attempts ELSE NULL END,
 substr(CAST(last_error AS BLOB),1,2049),substr(CAST(lease_token AS BLOB),1,33),
 CASE WHEN typeof(lease_until_ns)='integer' THEN lease_until_ns ELSE NULL END,
 CASE WHEN typeof(inventory_claimed)='integer' THEN inventory_claimed ELSE NULL END,
 typeof(kind)='text' AND typeof(status)='text' AND typeof(last_error)='text' AND typeof(lease_token)='text'
 AND typeof(cursor) IN('null','blob') AND length(CAST(last_error AS BLOB))<=2048 AND length(CAST(lease_token AS BLOB))<=32 FROM jobs WHERE kind=? ORDER BY id LIMIT ?`, ScanKind, MaxFairInventoryRoots+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, root, due, attempts, until, claimed int64
		var path, cursor []byte
		var status, fault, token []byte
		var types bool
		if err = rows.Scan(&id, &root, &path, &status, &due, &cursor, &attempts, &fault, &token, &until, &claimed, &types); err != nil {
			return errors.Join(ErrBackgroundInventoryCorrupt, err)
		}
		count++
		if !types || id <= 0 || root <= 0 || due < 0 || claimed < 0 || claimed > 1 || attempts < 0 || until < 0 || len(path) > 4096 || len(fault) > 2048 || len(token) > 32 || (string(status) != "pending" && string(status) != "running") {
			return ErrBackgroundInventoryCorrupt
		}
		admitted := false
		for _, r := range roots.roots {
			if r.id == root {
				admitted = true
				break
			}
		}
		if count > MaxFairInventoryRoots || !admitted || claimed != 0 || string(path) != "." || string(status) != "pending" || len(cursor) != 0 || attempts != 0 || len(fault) != 0 || len(token) != 0 || until != 0 {
			return ErrBackgroundInventoryModePending
		}
	}
	return rows.Err()
}

func (s *Store) checkBackgroundInventoryScope(ctx context.Context, tx *sql.Tx, scope *BackgroundInventoryScope, rootID int64) error {
	if scope == nil {
		return nil
	}
	if s.schema < 14 || !s.validFairScope(scope.roots) || scope.epoch < 1 {
		return ErrBackgroundInventoryInput
	}
	mode, err := readBackgroundInventoryMode(ctx, tx)
	if err != nil {
		return err
	}
	epoch, err := readBackgroundInventoryEpoch(ctx, tx)
	if err != nil {
		return err
	}
	if mode != scope.compact || epoch != scope.epoch {
		return ErrBackgroundInventoryInput
	}
	found := rootID == 0
	for _, root := range scope.roots.roots {
		if rootID != 0 && root.id != rootID {
			continue
		}
		if err = checkBackgroundInventoryRoot(ctx, tx, root); err != nil {
			return err
		}
		found = true
	}
	if !found {
		return ErrBackgroundInventoryInput
	}
	return ctx.Err()
}

func backgroundRootListing(ctx context.Context, tx *sql.Tx, rootID int64) (inventoryRevisitRoot, error) {
	r := inventoryRevisitRoot{id: rootID}
	var types bool
	err := tx.QueryRowContext(ctx, `SELECT CASE WHEN typeof(enabled)='integer' THEN enabled ELSE NULL END,CASE WHEN typeof(last_scan_ns) IN('null','integer') THEN last_scan_ns ELSE NULL END,typeof(enabled)='integer' AND typeof(last_scan_ns) IN('null','integer') FROM roots WHERE id=?`, rootID).Scan(&r.enabled, &r.lastListing, &types)
	if err != nil {
		return r, errors.Join(ErrBackgroundInventoryCorrupt, err)
	}
	if !types {
		return r, ErrBackgroundInventoryCorrupt
	}
	return r, nil
}

// ScheduleBackgroundInventoryRevisit is fixed-interval root-listing scheduling
// after exact-root saved work drains. It retains all jobs and their due times.
func (s *Store) ScheduleBackgroundInventoryRevisit(ctx context.Context, scope BackgroundInventoryScope, rootID int64, now time.Time, interval time.Duration) (bool, error) {
	return s.scheduleInventoryRevisit(ctx, rootID, now, interval, inventoryRevisitHooks{}, scope)
}

// SeedBackgroundInventoryRevisitPage visits only indexed admitted roots, up to
// 32 in this scope. Historical roots outside the scope are not queried/refilled.
func (s *Store) SeedBackgroundInventoryRevisitPage(ctx context.Context, scope BackgroundInventoryScope, afterRootID int64, now time.Time, interval time.Duration) (InventoryRevisitPage, error) {
	return s.seedInventoryRevisitPage(ctx, afterRootID, now, interval, inventoryRevisitHooks{}, scope)
}

func (s *Store) BackgroundInventoryRevisitPending(ctx context.Context, scope BackgroundInventoryScope, due time.Time, interval time.Duration) (bool, error) {
	return s.inventoryRevisitPending(ctx, due, interval, &scope)
}

// RetireBackgroundInventoryForRoot shares the existing one-worked-transaction
// engines. Each selected mutation rechecks this exact saved mode/root binding.
func (s *Store) RetireBackgroundInventoryForRoot(ctx context.Context, scope BackgroundInventoryScope, rootID int64) (InventoryRetirementStep, error) {
	return s.retireInventoryForRoot(ctx, rootID, &scope)
}

func checkBackgroundInventoryRoot(ctx context.Context, tx *sql.Tx, root fairInventoryRoot) error {
	var path []byte
	var enabled int64
	var types bool
	if err := tx.QueryRowContext(ctx, `SELECT substr(CAST(path AS BLOB),1,4097),CASE WHEN typeof(enabled)='integer' THEN enabled ELSE NULL END,typeof(path)='blob' AND typeof(enabled)='integer' FROM roots WHERE id=?`, root.id).Scan(&path, &enabled, &types); err != nil {
		return errors.Join(ErrBackgroundInventoryInput, err)
	}
	if !types || enabled != 1 || !bytes.Equal(path, root.path) {
		return ErrBackgroundInventoryInput
	}
	return ctx.Err()
}
