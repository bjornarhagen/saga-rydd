package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MaxFairInventoryRoots = 32
const FairInventorySource = "source"
const FairInventoryMaintenance = "maintenance"
const fairInventoryCursorKey = "worker.inventory.root_turn_v1"
const FairInventoryClaimWindow = 2 * time.Second

var ErrFairInventoryInput = errors.New("invalid bounded inventory root turn")
var ErrFairInventoryCorrupt = errors.New("invalid saved inventory root turn evidence")
var ErrFairInventoryReceipt = errors.New("inventory root turn needs an unused current dispatch reservation")

type fairInventoryRoot struct {
	id   int64
	path []byte
}

// FairInventoryRoots binds at most 32 exact configured paths to one open store.
// Resolve again after reopening the store. Callers cannot add roots to it.
type FairInventoryRoots struct {
	store *Store
	roots []fairInventoryRoot
}

type FairInventoryTurn struct {
	RootID   int64
	RootPath []byte
	Kind     string
	Job      *Job
	receipt  *fairInventoryReceipt
}

// NextSourceDue includes blocked due source work. The owner combines it with
// metadata readiness before sleeping, rather than polling a past due time.
// DormantRoots have no inventory job, or are disabled; closing their parked
// streams loses no unfinished source continuation.
type FairInventorySchedule struct {
	Turn          *FairInventoryTurn
	NextSourceDue time.Time
	SourceBlocked bool
	DormantRoots  [][]byte
}

type fairInventoryCursor struct {
	root, dispatch int64
}

// ValidateFairInventoryPaths has no side effect. The experimental worker calls
// it before SyncRoots, so an oversized selection cannot change saved roots.
func ValidateFairInventoryPaths(paths []string) error {
	if len(paths) < 1 || len(paths) > MaxFairInventoryRoots {
		return ErrFairInventoryInput
	}
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		if len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || seen[path] {
			return ErrFairInventoryInput
		}
		seen[path] = true
	}
	return nil
}

func (s *Store) ResolveFairInventoryRoots(ctx context.Context, paths []string) (FairInventoryRoots, error) {
	if ctx == nil || s.schema < 11 || ValidateFairInventoryPaths(paths) != nil {
		return FairInventoryRoots{}, ErrFairInventoryInput
	}
	paths = append([]string(nil), paths...)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FairInventoryRoots{}, err
	}
	defer tx.Rollback()
	scope := FairInventoryRoots{store: s, roots: make([]fairInventoryRoot, 0, len(paths))}
	for _, path := range paths {
		var root fairInventoryRoot
		var enabled int64
		var pathType, enabledType string
		if err = tx.QueryRowContext(ctx, "SELECT id,path,enabled,typeof(path),typeof(enabled) FROM roots WHERE path=?", []byte(path)).Scan(&root.id, &root.path, &enabled, &pathType, &enabledType); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				err = ErrFairInventoryInput
			}
			return FairInventoryRoots{}, err
		}
		if root.id <= 0 || enabled != 1 || pathType != "blob" || enabledType != "integer" || !bytes.Equal(root.path, []byte(path)) {
			return FairInventoryRoots{}, ErrFairInventoryCorrupt
		}
		scope.roots = append(scope.roots, root)
	}
	sort.Slice(scope.roots, func(i, j int) bool { return scope.roots[i].id < scope.roots[j].id })
	if err = ctx.Err(); err != nil {
		return FairInventoryRoots{}, err
	}
	if err = tx.Commit(); err != nil {
		return FairInventoryRoots{}, err
	}
	if err = ctx.Err(); err != nil {
		return FairInventoryRoots{}, err
	}
	return scope, nil
}

func (s *Store) validFairScope(scope FairInventoryRoots) bool {
	if scope.store != s || s.schema < 11 || len(scope.roots) < 1 || len(scope.roots) > MaxFairInventoryRoots {
		return false
	}
	previous := int64(0)
	for _, root := range scope.roots {
		if root.id <= previous || ValidateFairInventoryPaths([]string{string(root.path)}) != nil {
			return false
		}
		previous = root.id
	}
	return true
}

func fairInventoryNow(ctx context.Context, now time.Time) (int64, error) {
	if ctx == nil {
		return 0, ErrFairInventoryInput
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := now.UnixNano()
	if n <= 0 || !time.Unix(0, n).Equal(now) {
		return 0, ErrFairInventoryInput
	}
	return n, nil
}

func readFairInventoryCursor(ctx context.Context, tx *sql.Tx) (fairInventoryCursor, error) {
	var raw []byte
	var kind string
	err := tx.QueryRowContext(ctx, "SELECT typeof(value),value FROM settings WHERE key=?", fairInventoryCursorKey).Scan(&kind, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return fairInventoryCursor{}, nil
	}
	if err != nil {
		return fairInventoryCursor{}, err
	}
	if kind != "blob" || len(raw) > 43 {
		return fairInventoryCursor{}, ErrFairInventoryCorrupt
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[0] != "1" {
		return fairInventoryCursor{}, ErrFairInventoryCorrupt
	}
	values := make([]int64, 2)
	for i, part := range parts[1:] {
		values[i], err = strconv.ParseInt(part, 10, 64)
		if err != nil || values[i] < 0 || strconv.FormatInt(values[i], 10) != part {
			return fairInventoryCursor{}, ErrFairInventoryCorrupt
		}
	}
	if (values[0] == 0) != (values[1] == 0) {
		return fairInventoryCursor{}, ErrFairInventoryCorrupt
	}
	return fairInventoryCursor{root: values[0], dispatch: values[1]}, nil
}

func writeFairInventoryCursor(ctx context.Context, tx *sql.Tx, root, dispatch int64) error {
	raw := []byte("1:" + strconv.FormatInt(root, 10) + ":" + strconv.FormatInt(dispatch, 10))
	_, err := tx.ExecContext(ctx, "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", fairInventoryCursorKey, raw)
	return err
}

// These indexed existence probes preserve d1's pending-cache semantics while
// avoiding a join/filter over every completed cache scope during selection.
// Retirement mutations retain their existing cooperative deadlines and fences.
func fairInventoryMaintenanceRemaining(ctx context.Context, tx *sql.Tx, rootID int64) (bool, error) {
	var proofs bool
	if err := inventoryRetirementRemaining(ctx, tx, rootID, &proofs); err != nil {
		return false, err
	}
	var compact, pending, older, newer, orphan bool
	err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM compact_retirement WHERE root_id=?),
 EXISTS(SELECT 1 FROM allocation_cache INDEXED BY allocation_cache_pending_root WHERE root_id=? AND phase!='done'),
 EXISTS(SELECT 1 FROM allocation_cache INDEXED BY allocation_cache_root_revision WHERE root_id=? AND revision<(SELECT revision FROM allocation_revisions WHERE root_id=?)),
 EXISTS(SELECT 1 FROM allocation_cache INDEXED BY allocation_cache_root_revision WHERE root_id=? AND revision>(SELECT revision FROM allocation_revisions WHERE root_id=?)),
 EXISTS(SELECT 1 FROM allocation_cache WHERE root_id=? AND NOT EXISTS(SELECT 1 FROM allocation_revisions WHERE root_id=?))`, rootID, rootID, rootID, rootID, rootID, rootID, rootID, rootID).Scan(&compact, &pending, &older, &newer, &orphan)
	if err != nil {
		return false, err
	}
	if pending || older || newer || orphan {
		var revision int64
		if err = tx.QueryRowContext(ctx, "SELECT revision FROM allocation_revisions WHERE root_id=?", rootID).Scan(&revision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				err = ErrInventoryRetirementCorrupt
			}
			return false, err
		}
		if revision < 0 {
			return false, ErrInventoryRetirementCorrupt
		}
	}
	return proofs || compact || pending || older || newer || orphan, nil
}

func selectFairInventoryTurn(ctx context.Context, tx *sql.Tx, scope FairInventoryRoots, now int64, allowSource bool, cursor fairInventoryCursor) (FairInventorySchedule, int64, error) {
	schedule := FairInventorySchedule{}
	jobID := int64(0)
	start := sort.Search(len(scope.roots), func(i int) bool { return scope.roots[i].id > cursor.root })
	for i := 0; i < len(scope.roots); i++ {
		root := scope.roots[(start+i)%len(scope.roots)]
		var enabled int64
		var path []byte
		var pathType, enabledType string
		if err := tx.QueryRowContext(ctx, "SELECT enabled,path,typeof(path),typeof(enabled) FROM roots WHERE id=?", root.id).Scan(&enabled, &path, &pathType, &enabledType); err != nil {
			return FairInventorySchedule{}, 0, err
		}
		if (enabled != 0 && enabled != 1) || pathType != "blob" || enabledType != "integer" || !bytes.Equal(path, root.path) {
			return FairInventorySchedule{}, 0, ErrFairInventoryCorrupt
		}
		if enabled == 0 {
			schedule.DormantRoots = append(schedule.DormantRoots, bytes.Clone(path))
			continue
		}
		var id, due int64
		var dueType string
		err := tx.QueryRowContext(ctx, `SELECT id,due_at_ns,typeof(due_at_ns) FROM jobs INDEXED BY jobs_inventory_root_due
 WHERE root_id=? AND kind='inventory' AND status='pending' ORDER BY due_at_ns,id LIMIT 1`, root.id).Scan(&id, &due, &dueType)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return FairInventorySchedule{}, 0, err
		}
		if err == nil {
			if id <= 0 || due < 0 || dueType != "integer" {
				return FairInventorySchedule{}, 0, ErrFairInventoryCorrupt
			}
			when := time.Unix(0, due)
			if schedule.NextSourceDue.IsZero() || when.Before(schedule.NextSourceDue) {
				schedule.NextSourceDue = when
			}
			if due <= now {
				if !allowSource {
					schedule.SourceBlocked = true
				} else if schedule.Turn == nil {
					schedule.Turn = &FairInventoryTurn{RootID: root.id, RootPath: bytes.Clone(path), Kind: FairInventorySource}
					jobID = id
				}
			}
		}
		var busy bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE root_id=? AND kind=?)", root.id, ScanKind).Scan(&busy); err != nil {
			return FairInventorySchedule{}, 0, err
		}
		if busy {
			continue
		}
		schedule.DormantRoots = append(schedule.DormantRoots, bytes.Clone(path))
		remaining, err := fairInventoryMaintenanceRemaining(ctx, tx, root.id)
		if err != nil {
			return FairInventorySchedule{}, 0, err
		}
		if remaining && schedule.Turn == nil {
			schedule.Turn = &FairInventoryTurn{RootID: root.id, RootPath: bytes.Clone(path), Kind: FairInventoryMaintenance}
		}
	}
	if err := ctx.Err(); err != nil {
		return FairInventorySchedule{}, 0, err
	}
	return schedule, jobID, nil
}

// NextFairInventoryTurn reads one bounded saved snapshot; it writes no cursor,
// lease or budget. allowSource is readiness, not source-read authorization.
func (s *Store) NextFairInventoryTurn(ctx context.Context, scope FairInventoryRoots, now time.Time, allowSource bool) (FairInventorySchedule, error) {
	n, err := fairInventoryNow(ctx, now)
	if err != nil {
		return FairInventorySchedule{}, err
	}
	if !s.validFairScope(scope) {
		return FairInventorySchedule{}, ErrFairInventoryInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FairInventorySchedule{}, err
	}
	defer tx.Rollback()
	cursor, err := readFairInventoryCursor(ctx, tx)
	if err != nil {
		return FairInventorySchedule{}, err
	}
	schedule, _, err := selectFairInventoryTurn(ctx, tx, scope, n, allowSource, cursor)
	if err != nil {
		return FairInventorySchedule{}, err
	}
	if err = tx.Commit(); err != nil {
		return FairInventorySchedule{}, err
	}
	if err = ctx.Err(); err != nil {
		return FairInventorySchedule{}, err
	}
	return schedule, nil
}

// ClaimFairInventoryTurn requires the latest unused durable dispatch receipt.
// Source claims and the root rotation commit together. A maintenance turn
// consumes the same receipt before the existing exact-root mutation API runs.
// A lost reply never refunds a receipt or resets rotation. Sources still need
// their independent metadata reservations before construction or Next.
func (s *Store) ClaimFairInventoryTurn(ctx context.Context, scope FairInventoryRoots, now time.Time, lease time.Duration, allowSource bool, reservedAt time.Time) (*FairInventoryTurn, error) {
	n, err := fairInventoryNow(ctx, now)
	if err != nil {
		return nil, err
	}
	reserved, err := fairInventoryNow(ctx, reservedAt)
	if err != nil || reserved > n || lease <= 0 || lease > 48*time.Hour || n > math.MaxInt64-int64(lease) || s.readOnly || s.lock == nil || !s.validFairScope(scope) {
		return nil, ErrFairInventoryInput
	}
	if n-reserved >= int64(min(lease, FairInventoryClaimWindow)) {
		return nil, ErrFairInventoryReceipt
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cursor, err := readFairInventoryCursor(ctx, tx)
	if err != nil {
		return nil, err
	}
	var last, next int64
	var count int
	var day string
	if err = tx.QueryRowContext(ctx, "SELECT day,chunks,last_start_ns,next_start_ns FROM scan_dispatch WHERE id=1").Scan(&day, &count, &last, &next); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrFairInventoryReceipt
		}
		return nil, err
	}
	if last != reserved || cursor.dispatch >= reserved || count < 1 || next <= last || day != reservedAt.UTC().Format(time.DateOnly) {
		return nil, ErrFairInventoryReceipt
	}
	schedule, jobID, err := selectFairInventoryTurn(ctx, tx, scope, n, allowSource, cursor)
	if err != nil || schedule.Turn == nil {
		return nil, err
	}
	turn := schedule.Turn
	if turn.Kind == FairInventorySource {
		var token [16]byte
		if _, err = rand.Read(token[:]); err != nil {
			return nil, err
		}
		var job Job
		var until int64
		err = tx.QueryRowContext(ctx, `UPDATE jobs SET status='running',lease_token=?,lease_until_ns=?,attempts=attempts+1
 WHERE id=? AND root_id=? AND kind='inventory' AND status='pending' AND due_at_ns<=?
 RETURNING id,root_id,kind,path,cursor,attempts,lease_token,lease_until_ns,
 (SELECT path FROM roots WHERE roots.id=jobs.root_id),
 (SELECT volume_id FROM roots WHERE roots.id=jobs.root_id)`, hex.EncodeToString(token[:]), n+int64(lease), jobID, turn.RootID, n).Scan(&job.ID, &job.RootID, &job.Kind, &job.Path, &job.Cursor, &job.Attempts, &job.Token, &until, &job.RootPath, &job.RootIdentity)
		if err != nil {
			return nil, err
		}
		if !validRelative(job.Path) || len(job.Cursor) > MaxCursorBytes || job.Attempts < 1 || !bytes.Equal(job.RootPath, turn.RootPath) || len(job.RootIdentity) > 4096 {
			return nil, ErrFairInventoryCorrupt
		}
		job.LeaseUntil = time.Unix(0, until)
		turn.Job = &job
	}
	if err = writeFairInventoryCursor(ctx, tx, turn.RootID, reserved); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	turn.receipt = newFairInventoryReceipt(s, turn, reservedAt)
	return turn, nil
}
