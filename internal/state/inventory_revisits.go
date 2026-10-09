package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// InventoryRevisitInterval is the worker's fixed periodic root-listing policy.
// A completed root listing does not establish completed descendant coverage.
const InventoryRevisitInterval = 24 * time.Hour
const InventoryRevisitPageSize = 128

var (
	ErrInventoryRevisitInput   = errors.New("invalid inventory revisit request")
	ErrInventoryRevisitCorrupt = errors.New("invalid saved inventory revisit evidence")
	ErrInventoryRevisitMode    = errors.New("periodic inventory revisits require detailed inventory")
)

// InventoryRevisitPage advances through raw root rows. Disabled roots and roots
// with unfinished work consume page slots. Cursor is the last examined root ID
// when More is true; no source metadata or tree-completion claim is returned.
type InventoryRevisitPage struct {
	Enqueued int64
	Cursor   int64
	More     bool
}

type inventoryRevisitRoot struct {
	id, enabled int64
	lastListing sql.NullInt64
}

type inventoryRevisitHooks struct{ beforeCommit func() }

func validateInventoryRevisit(ctx context.Context, now time.Time, interval time.Duration) (int64, error) {
	if ctx == nil || interval <= 0 || interval > 30*24*time.Hour {
		return 0, ErrInventoryRevisitInput
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := now.UnixNano()
	if n <= 0 || !time.Unix(0, n).Equal(now) {
		return 0, ErrInventoryRevisitInput
	}
	return n, nil
}

func inventoryRevisitMode(ctx context.Context, tx *sql.Tx) error {
	var value []byte
	err := tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE key='inventory.compact'").Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	switch string(value) {
	case "0":
		return nil
	case "1":
		return ErrInventoryRevisitMode
	default:
		return ErrInventoryRevisitCorrupt
	}
}

func scheduleInventoryRevisit(ctx context.Context, tx *sql.Tx, r inventoryRevisitRoot, now int64, interval time.Duration) (bool, error) {
	if r.id <= 0 || (r.enabled != 0 && r.enabled != 1) || (r.lastListing.Valid && (r.lastListing.Int64 < 0 || r.lastListing.Int64 > math.MaxInt64-int64(interval))) {
		return false, ErrInventoryRevisitCorrupt
	}
	if r.enabled == 0 {
		return false, nil
	}
	// Exact root lookups keep running work, delayed retries and saved derived
	// calculations ahead of another root listing. Other roots remain eligible.
	var unfinished bool
	err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM jobs WHERE root_id=? AND kind=?)
 OR EXISTS(SELECT 1 FROM compact_retirement WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM subtree_reconcile WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM subtree_retirement WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM allocation_cache c LEFT JOIN allocation_revisions v ON v.root_id=c.root_id
 WHERE c.root_id=? AND (v.revision IS NULL OR c.revision!=v.revision OR c.phase!='done'))`,
		r.id, ScanKind, r.id, r.id, r.id, r.id).Scan(&unfinished)
	if err != nil || unfinished {
		return false, err
	}
	due := now
	if r.lastListing.Valid && r.lastListing.Int64 > 0 {
		due = max(due, r.lastListing.Int64+int64(interval))
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed) VALUES(?,?,X'2e',?,0)
 ON CONFLICT(root_id,kind,path) DO NOTHING`, r.id, ScanKind, due)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// ScheduleInventoryRevisit enqueues at most one exact root listing after that
// root's saved inventory/maintenance queue drains. Its due time is the later of
// now and the last completed root listing plus interval. Existing jobs retain
// their due time, lease, cursor and retry evidence. The interval is a bounded
// library policy input; the production worker always uses 24 hours.
func (s *Store) ScheduleInventoryRevisit(ctx context.Context, rootID int64, now time.Time, interval time.Duration) (bool, error) {
	return s.scheduleInventoryRevisit(ctx, rootID, now, interval, inventoryRevisitHooks{})
}

func (s *Store) scheduleInventoryRevisit(ctx context.Context, rootID int64, now time.Time, interval time.Duration, hooks inventoryRevisitHooks, background ...BackgroundInventoryScope) (bool, error) {
	n, err := validateInventoryRevisit(ctx, now, interval)
	if err != nil {
		return false, err
	}
	if rootID <= 0 {
		return false, ErrInventoryRevisitInput
	}
	if s.readOnly {
		return false, errors.New("state is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if len(background) == 0 {
		err = inventoryRevisitMode(ctx, tx)
	} else {
		err = s.checkBackgroundInventoryScope(ctx, tx, &background[0], rootID)
	}
	if err != nil {
		return false, err
	}
	r := inventoryRevisitRoot{id: rootID}
	if len(background) > 0 {
		r, err = backgroundRootListing(ctx, tx, rootID)
	} else {
		err = tx.QueryRowContext(ctx, "SELECT enabled,last_scan_ns FROM roots WHERE id=?", rootID).Scan(&r.enabled, &r.lastListing)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("%w: root is unavailable", ErrInventoryRevisitInput)
		}
		return false, err
	}
	inserted, err := scheduleInventoryRevisit(ctx, tx, r, n, interval)
	if err != nil {
		return false, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
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
	return inserted, nil
}

// SeedInventoryRevisitPage examines at most 128 raw roots plus one continuation
// row in one transaction. It initializes periodic scheduling without an
// unbounded enabled-root query. Callers yield to controls between pages. A
// restart can safely begin at zero because existing queued work is preserved.
func (s *Store) SeedInventoryRevisitPage(ctx context.Context, afterRootID int64, now time.Time, interval time.Duration) (InventoryRevisitPage, error) {
	return s.seedInventoryRevisitPage(ctx, afterRootID, now, interval, inventoryRevisitHooks{})
}

func (s *Store) seedInventoryRevisitPage(ctx context.Context, afterRootID int64, now time.Time, interval time.Duration, hooks inventoryRevisitHooks, background ...BackgroundInventoryScope) (InventoryRevisitPage, error) {
	n, err := validateInventoryRevisit(ctx, now, interval)
	if err != nil {
		return InventoryRevisitPage{}, err
	}
	if afterRootID < 0 {
		return InventoryRevisitPage{}, ErrInventoryRevisitInput
	}
	if s.readOnly {
		return InventoryRevisitPage{}, errors.New("state is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InventoryRevisitPage{}, err
	}
	defer tx.Rollback()
	if len(background) == 0 {
		err = inventoryRevisitMode(ctx, tx)
	} else {
		err = s.checkBackgroundInventoryScope(ctx, tx, &background[0], 0)
	}
	if err != nil {
		return InventoryRevisitPage{}, err
	}
	columns := "id,enabled,last_scan_ns"
	if len(background) > 0 {
		columns = "CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(enabled)='integer' THEN enabled ELSE NULL END,CASE WHEN typeof(last_scan_ns) IN('null','integer') THEN last_scan_ns ELSE NULL END"
	}
	query := "SELECT " + columns + " FROM roots WHERE id>?"
	args := []any{afterRootID}
	if len(background) > 0 {
		ids := background[0].RootIDs()
		marks := make([]string, len(ids))
		for i, id := range ids {
			marks[i] = "?"
			args = append(args, id)
		}
		query += " AND id IN(" + strings.Join(marks, ",") + ")"
	}
	query += " ORDER BY id LIMIT ?"
	args = append(args, InventoryRevisitPageSize+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return InventoryRevisitPage{}, err
	}
	roots := make([]inventoryRevisitRoot, 0, InventoryRevisitPageSize+1)
	for rows.Next() {
		var r inventoryRevisitRoot
		if err = rows.Scan(&r.id, &r.enabled, &r.lastListing); err != nil {
			rows.Close()
			return InventoryRevisitPage{}, err
		}
		roots = append(roots, r)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil {
		return InventoryRevisitPage{}, errors.Join(err, closeErr)
	}
	page := InventoryRevisitPage{More: len(roots) > InventoryRevisitPageSize}
	if page.More {
		roots = roots[:InventoryRevisitPageSize]
		page.Cursor = roots[len(roots)-1].id
	}
	for _, r := range roots {
		if len(background) > 0 {
			r, err = backgroundRootListing(ctx, tx, r.id)
			if err != nil {
				return InventoryRevisitPage{}, err
			}
		}
		if err = ctx.Err(); err != nil {
			return InventoryRevisitPage{}, err
		}
		inserted, e := scheduleInventoryRevisit(ctx, tx, r, n, interval)
		if e != nil {
			return InventoryRevisitPage{}, e
		}
		if inserted {
			page.Enqueued++
		}
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return InventoryRevisitPage{}, err
	}
	if err = tx.Commit(); err != nil {
		return InventoryRevisitPage{}, err
	}
	if err = ctx.Err(); err != nil {
		return InventoryRevisitPage{}, err
	}
	return page, nil
}

// InventoryRevisitPending qualifies the next enabled inventory job at an
// already obtained due time. It examines at most 129 raw jobs at that exact
// indexed due time; an unexamined/unknown candidate keeps the generic wait
// classification. It reads saved state only and evaluates no source freshness.
func (s *Store) InventoryRevisitPending(ctx context.Context, due time.Time, interval time.Duration) (bool, error) {
	return s.inventoryRevisitPending(ctx, due, interval, nil)
}

func (s *Store) inventoryRevisitPending(ctx context.Context, due time.Time, interval time.Duration, background *BackgroundInventoryScope) (bool, error) {
	n, err := validateInventoryRevisit(ctx, due, interval)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = s.checkBackgroundInventoryScope(ctx, tx, background, 0); err != nil {
		return false, err
	}
	type candidate struct {
		root, attempts int64
		kind           string
		path, cursor   []byte
		lastError      []byte
		claimed        int64
		types          bool
	}
	columns := "root_id,kind,path,cursor,last_error,attempts"
	if background != nil {
		columns = "CASE WHEN typeof(root_id)='integer' THEN root_id ELSE NULL END,substr(CAST(kind AS BLOB),1,33),substr(CAST(path AS BLOB),1,4097),substr(cursor,1,1),substr(CAST(last_error AS BLOB),1,2049),CASE WHEN typeof(attempts)='integer' THEN attempts ELSE NULL END,CASE WHEN typeof(inventory_claimed)='integer' THEN inventory_claimed ELSE NULL END,typeof(root_id)='integer' AND typeof(kind)='text' AND typeof(path)='blob' AND typeof(cursor) IN('null','blob') AND typeof(last_error)='text' AND typeof(attempts)='integer'"
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+columns+" FROM jobs WHERE status='pending' AND due_at_ns=? ORDER BY id LIMIT ?", n, InventoryRevisitPageSize+1)
	if err != nil {
		return false, err
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		fields := []any{&c.root, &c.kind, &c.path, &c.cursor, &c.lastError, &c.attempts}
		if background != nil {
			fields = append(fields, &c.claimed, &c.types)
		}
		if err = rows.Scan(fields...); err != nil {
			rows.Close()
			if background != nil && ctx.Err() == nil {
				return false, errors.Join(ErrBackgroundInventoryCorrupt, err)
			}
			return false, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil {
		return false, errors.Join(err, closeErr)
	}
	if len(candidates) > InventoryRevisitPageSize {
		candidates = candidates[:InventoryRevisitPageSize]
	}
	matched := false
	for _, c := range candidates {
		if background != nil {
			if !c.types || len(c.path) > 4096 || len(c.lastError) > 2048 || len(c.kind) > 32 || c.root <= 0 || c.attempts < 0 || c.claimed < 0 || c.claimed > 1 {
				return false, ErrBackgroundInventoryCorrupt
			}
			admitted := false
			for _, root := range background.roots.roots {
				if root.id == c.root {
					admitted = true
					break
				}
			}
			if !admitted {
				continue
			}
		}
		if c.kind != ScanKind {
			continue
		}
		var enabled int64
		var listing sql.NullInt64
		if background != nil {
			r, e := backgroundRootListing(ctx, tx, c.root)
			if e != nil {
				return false, e
			}
			enabled, listing = r.enabled, r.lastListing
		} else if err = tx.QueryRowContext(ctx, "SELECT enabled,last_scan_ns FROM roots WHERE id=?", c.root).Scan(&enabled, &listing); err != nil {
			return false, err
		}
		if enabled == 0 {
			continue
		}
		matched = (background == nil || c.claimed == 0) && enabled == 1 && string(c.path) == "." && len(c.cursor) == 0 && len(c.lastError) == 0 && c.attempts == 0 && listing.Valid && listing.Int64 > 0 && listing.Int64 <= math.MaxInt64-int64(interval) && listing.Int64+int64(interval) == n
		break
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
	return matched, nil
}
