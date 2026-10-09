package state

import (
	"context"
	"database/sql"
	"errors"
)

var (
	ErrInventoryRetirementInput   = errors.New("invalid inventory retirement request")
	ErrInventoryRetirementCorrupt = errors.New("invalid saved inventory retirement evidence")
)

// InventoryRetirementStep describes one saved-state-only transaction. Eligible
// means the enabled root had no inventory job, including delayed or future work.
// Remaining describes its saved control rows, not source availability or size.
type InventoryRetirementStep struct {
	Worked    bool
	Remaining bool
	Eligible  bool
}

// InventoryRetirementPage examines at most 128 raw roots and one continuation
// row. A selected root retains the cursor at its own ID, so later ready roots
// are not skipped. Disabled and busy roots consume slots; pages never refill.
type InventoryRetirementPage struct {
	RootID int64
	Cursor int64
	More   bool
}

type inventoryRetirementHooks struct{ beforeCommit func() }

func inventoryRetirementRemaining(ctx context.Context, tx *sql.Tx, rootID int64, remaining *bool) error {
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM subtree_reconcile WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM subtree_retirement WHERE root_id=?)`, rootID, rootID).Scan(remaining); err != nil {
		return err
	}
	if !*remaining {
		return nil
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, "SELECT scan_revision FROM allocation_revisions WHERE root_id=?", rootID).Scan(&revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInventoryRetirementCorrupt
		}
		return err
	}
	if revision < 0 {
		return ErrInventoryRetirementCorrupt
	}
	return nil
}

func inventoryMaintenanceRemaining(ctx context.Context, tx *sql.Tx, rootID int64, remaining *bool) error {
	var proofs, compact, allocation bool
	if err := inventoryRetirementRemaining(ctx, tx, rootID, &proofs); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM compact_retirement WHERE root_id=?),
 EXISTS(SELECT 1 FROM allocation_cache c LEFT JOIN allocation_revisions v ON v.root_id=c.root_id
 WHERE c.root_id=? AND(v.revision IS NULL OR c.revision!=v.revision OR c.phase!='done'))`, rootID, rootID).Scan(&compact, &allocation); err != nil {
		return err
	}
	if allocation {
		var revision int64
		if err := tx.QueryRowContext(ctx, "SELECT revision FROM allocation_revisions WHERE root_id=?", rootID).Scan(&revision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrInventoryRetirementCorrupt
			}
			return err
		}
		if revision < 0 {
			return ErrInventoryRetirementCorrupt
		}
	}
	*remaining = proofs || compact || allocation
	return nil
}

// RetireInventoryForRoot advances at most one worked maintenance transaction:
// obsolete compact records, completed-parent retirement, then obsolete detailed
// allocation scratch/cache. The exact enabled-root/no-inventory-job gate is
// rechecked in each mutation transaction. Completed current caches and source
// observations are never treated as stale merely because the worker restarted.
func (s *Store) RetireInventoryForRoot(ctx context.Context, rootID int64) (InventoryRetirementStep, error) {
	if ctx == nil || rootID <= 0 {
		return InventoryRetirementStep{}, ErrInventoryRetirementInput
	}
	if err := ctx.Err(); err != nil {
		return InventoryRetirementStep{}, err
	}
	if s.readOnly {
		return InventoryRetirementStep{}, errors.New("state is read-only")
	}
	eligible, remaining, err := s.inventoryMaintenanceState(ctx, rootID)
	if err != nil {
		return InventoryRetirementStep{}, err
	}
	step := InventoryRetirementStep{Eligible: eligible, Remaining: remaining}
	if !eligible || !remaining {
		return step, ctx.Err()
	}
	step.Worked, err = s.retireCompactForRoot(ctx, rootID)
	if err == nil && !step.Worked {
		var subtree InventoryRetirementStep
		subtree, err = s.RetireSubtreesForRoot(ctx, rootID)
		step.Worked = subtree.Worked
	}
	if err == nil && !step.Worked {
		step.Worked, err = s.reduceAllocationsForRoot(ctx, rootID)
	}
	if err != nil {
		return InventoryRetirementStep{}, err
	}
	step.Eligible, step.Remaining, err = s.inventoryMaintenanceState(ctx, rootID)
	if err != nil {
		return InventoryRetirementStep{}, err
	}
	if err = ctx.Err(); err != nil {
		return InventoryRetirementStep{}, err
	}
	return step, nil
}

func (s *Store) inventoryMaintenanceState(ctx context.Context, rootID int64) (bool, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	var enabled int64
	var busy, remaining bool
	if err = tx.QueryRowContext(ctx, `SELECT enabled,EXISTS(SELECT 1 FROM jobs WHERE root_id=roots.id AND kind=?) FROM roots WHERE id=?`, ScanKind, rootID).Scan(&enabled, &busy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrInventoryRetirementInput
		}
		return false, false, err
	}
	if enabled != 0 && enabled != 1 {
		return false, false, ErrInventoryRetirementCorrupt
	}
	if err = inventoryMaintenanceRemaining(ctx, tx, rootID, &remaining); err != nil {
		return false, false, err
	}
	if err = ctx.Err(); err != nil {
		return false, false, err
	}
	if err = tx.Commit(); err != nil {
		return false, false, err
	}
	if err = ctx.Err(); err != nil {
		return false, false, err
	}
	return enabled == 1 && !busy, remaining, nil
}

// RetireSubtreesForRoot advances only the selected root's existing completed-
// parent proofs. It makes no source calls. The same per-root scan gate, scan
// revision fence and 128-row fixed-table bound apply as in RetireSubtrees.
// Rebuildable inventory/cache rows are retired; saved user decisions and action
// or restore history are not part of these tables.
func (s *Store) RetireSubtreesForRoot(ctx context.Context, rootID int64) (InventoryRetirementStep, error) {
	if rootID <= 0 {
		return InventoryRetirementStep{}, ErrInventoryRetirementInput
	}
	return s.retireSubtrees(ctx, rootID, inventoryRetirementHooks{})
}

// InventoryRetirementRootPending is an exact-root saved-state readiness check.
// A future revisit or delayed retry counts as unfinished inventory work.
func (s *Store) InventoryRetirementRootPending(ctx context.Context, rootID int64) (bool, error) {
	if ctx == nil || rootID <= 0 {
		return false, ErrInventoryRetirementInput
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var pending bool
	if err = tx.QueryRowContext(ctx, `SELECT r.enabled=1
 AND NOT EXISTS(SELECT 1 FROM jobs WHERE root_id=r.id AND kind=?)
 AND (EXISTS(SELECT 1 FROM subtree_reconcile WHERE root_id=r.id)
 OR EXISTS(SELECT 1 FROM subtree_retirement WHERE root_id=r.id)) FROM roots r WHERE r.id=?`, ScanKind, rootID).Scan(&pending); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrInventoryRetirementInput
		}
		return false, err
	}
	if pending {
		if err = inventoryRetirementRemaining(ctx, tx, rootID, &pending); err != nil {
			return false, err
		}
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	return pending, nil
}

// NextInventoryRetirementPage discovers saved work left between scan completion,
// retirement and revisit scheduling, including after interruption. It reads one
// bounded snapshot and returns no partial positive result on cancellation.
func (s *Store) NextInventoryRetirementPage(ctx context.Context, afterRootID int64) (InventoryRetirementPage, error) {
	if ctx == nil || afterRootID < 0 {
		return InventoryRetirementPage{}, ErrInventoryRetirementInput
	}
	if err := ctx.Err(); err != nil {
		return InventoryRetirementPage{}, err
	}
	if s.schema < 7 {
		return InventoryRetirementPage{}, ErrInventoryRetirementInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InventoryRetirementPage{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT r.id,r.enabled,
 EXISTS(SELECT 1 FROM jobs WHERE root_id=r.id AND kind=?),
 EXISTS(SELECT 1 FROM subtree_reconcile WHERE root_id=r.id)
 OR EXISTS(SELECT 1 FROM subtree_retirement WHERE root_id=r.id),
 EXISTS(SELECT 1 FROM compact_retirement WHERE root_id=r.id),
 EXISTS(SELECT 1 FROM allocation_cache c LEFT JOIN allocation_revisions v ON v.root_id=c.root_id
 WHERE c.root_id=r.id AND(v.revision IS NULL OR c.revision!=v.revision OR c.phase!='done')),
 (SELECT scan_revision FROM allocation_revisions WHERE root_id=r.id)
 FROM roots r WHERE r.id>? ORDER BY r.id LIMIT ?`, ScanKind, afterRootID, InventoryRevisitPageSize+1)
	if err != nil {
		return InventoryRetirementPage{}, err
	}
	type root struct {
		id, enabled                        int64
		busy, pending, compact, allocation bool
		revision                           sql.NullInt64
	}
	roots := make([]root, 0, InventoryRevisitPageSize+1)
	for rows.Next() {
		var r root
		if err = rows.Scan(&r.id, &r.enabled, &r.busy, &r.pending, &r.compact, &r.allocation, &r.revision); err != nil {
			rows.Close()
			return InventoryRetirementPage{}, err
		}
		roots = append(roots, r)
	}
	err, closeErr := rows.Err(), rows.Close()
	if err != nil || closeErr != nil {
		return InventoryRetirementPage{}, errors.Join(err, closeErr)
	}
	page := InventoryRetirementPage{More: len(roots) > InventoryRevisitPageSize}
	if page.More {
		roots = roots[:InventoryRevisitPageSize]
		page.Cursor = roots[len(roots)-1].id
	}
	for _, r := range roots {
		if err = ctx.Err(); err != nil {
			return InventoryRetirementPage{}, err
		}
		if r.id <= afterRootID || (r.enabled != 0 && r.enabled != 1) {
			return InventoryRetirementPage{}, ErrInventoryRetirementCorrupt
		}
		if r.enabled != 1 || r.busy || !(r.pending || r.compact || r.allocation) {
			continue
		}
		if (r.pending || r.allocation) && (!r.revision.Valid || r.revision.Int64 < 0) {
			return InventoryRetirementPage{}, ErrInventoryRetirementCorrupt
		}
		page.RootID, page.Cursor, page.More = r.id, r.id, true
		break
	}
	if err = ctx.Err(); err != nil {
		return InventoryRetirementPage{}, err
	}
	if err = tx.Commit(); err != nil {
		return InventoryRetirementPage{}, err
	}
	if err = ctx.Err(); err != nil {
		return InventoryRetirementPage{}, err
	}
	return page, nil
}
