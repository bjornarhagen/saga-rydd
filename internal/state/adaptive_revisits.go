package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

const AdaptiveRevisitContract = "adaptive_historical_metadata_v1"
const AdaptiveStableRevisitInterval = 7 * 24 * time.Hour

var (
	ErrAdaptiveRevisitInput   = errors.New("invalid adaptive inventory revisit request")
	ErrAdaptiveRevisitCorrupt = errors.New("invalid saved adaptive inventory revisit evidence")
	ErrAdaptiveRevisitClock   = errors.New("adaptive inventory revisit clock moved backwards or exceeded its range")
)

// AdaptiveRevisitScope is exact, open-store-bound and cannot be extended by a
// caller. Digest binds its caller's canonical configuration/exclusion profile;
// the library additionally binds the fixed profile and exact admitted roots.
type AdaptiveRevisitScope struct {
	roots            FairInventoryRoots
	digest, instance string
	enabled          bool
}

// RootIDs returns a clone of at most 32 admitted IDs for control-responsive
// startup bookkeeping. It does not let callers extend this scope.
func (scope AdaptiveRevisitScope) RootIDs() []int64 {
	ids := make([]int64, len(scope.roots.roots))
	for i, root := range scope.roots.roots {
		ids[i] = root.id
	}
	return ids
}

// AdaptiveRevisitResult describes historical metadata scheduling, not content
// equality, inactivity, current tree coverage or cleanup authority.
type AdaptiveRevisitResult struct {
	Scheduled        bool
	Initialized      bool
	Due              time.Time
	Interval         time.Duration
	Epoch            int64
	UnchangedStreak  int
	Changed, Unknown bool
}

// AdaptiveRevisitStatus is a bounded aggregate over the admitted roots only.
// No clock is evaluated and no source path or per-root list is returned.
type AdaptiveRevisitStatus struct {
	Contract               string `json:"contract"`
	TrackedRoots           int    `json:"tracked_roots"`
	ActiveEpochs           int    `json:"active_epochs"`
	ChangedEpochs          int    `json:"changed_epochs"`
	UnknownEpochs          int    `json:"unknown_epochs"`
	StableRoots            int    `json:"stable_roots"`
	DailyRoots             int    `json:"daily_roots"`
	WeeklyRoots            int    `json:"weekly_roots"`
	HistoricalMetadataOnly bool   `json:"historical_metadata_only"`
	CurrentContentVerified bool   `json:"current_content_verified"`
	CleanupApproved        bool   `json:"cleanup_approved"`
}

type adaptiveRevisitRow struct {
	lastInitialized, lastChanged, lastUnknown             bool
	lastStreak                                            int
	root                                                  int64
	path                                                  []byte
	digest, instance, identity                            string
	epoch, job, generation                                int64
	enabled, claimed, started, complete, changed, unknown bool
	streak                                                int
	maxNow, finalized, finished, due, interval            int64
	outcome                                               string
}

const adaptiveColumns = `root_id,root_path,scope_digest,enabled,instance,root_identity,epoch,root_job_id,root_generation,
 claimed,started,root_complete,changed,unknown,unchanged_streak,max_now_ns,finalized_epoch,finished_ns,scheduled_due_ns,interval_ns,last_initialized,last_changed,last_unknown,last_streak,last_outcome`

func adaptiveHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func readAdaptiveRevisit(ctx context.Context, tx *sql.Tx, root int64) (adaptiveRevisitRow, error) {
	r := adaptiveRevisitRow{}
	// Bound decoded strings even if a damaged database bypassed its CHECKs.
	columns := strings.Split(strings.ReplaceAll(adaptiveColumns, "\n", ""), ",")
	for i, column := range columns {
		column = strings.TrimSpace(column)
		switch column {
		case "root_path":
			columns[i] = "CASE WHEN typeof(root_path)='blob' THEN substr(root_path,1,4097) ELSE NULL END"
		case "root_identity":
			columns[i] = "CASE WHEN typeof(root_identity)='text' THEN substr(root_identity,1,4097) ELSE NULL END"
		case "scope_digest":
			columns[i] = "CASE WHEN typeof(scope_digest)='text' THEN substr(scope_digest,1,65) ELSE NULL END"
		case "instance":
			columns[i] = "CASE WHEN typeof(instance)='text' THEN substr(instance,1,33) ELSE NULL END"
		case "last_outcome":
			columns[i] = "CASE WHEN typeof(last_outcome)='text' THEN substr(last_outcome,1,16) ELSE NULL END"
		default:
			columns[i] = "CASE WHEN typeof(" + column + ")='integer' THEN " + column + " ELSE NULL END"
		}
	}
	err := tx.QueryRowContext(ctx, "SELECT "+strings.Join(columns, ",")+" FROM adaptive_inventory_revisits WHERE root_id=?", root).Scan(
		&r.root, &r.path, &r.digest, &r.enabled, &r.instance, &r.identity, &r.epoch, &r.job, &r.generation, &r.claimed, &r.started, &r.complete, &r.changed, &r.unknown,
		&r.streak, &r.maxNow, &r.finalized, &r.finished, &r.due, &r.interval, &r.lastInitialized, &r.lastChanged, &r.lastUnknown, &r.lastStreak, &r.outcome)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || ctx.Err() != nil {
			return r, err
		}
		return adaptiveRevisitRow{}, fmt.Errorf("%w: scalar decoding", ErrAdaptiveRevisitCorrupt)
	}
	if r.root <= 0 || ValidateFairInventoryPaths([]string{string(r.path)}) != nil || !adaptiveHex(r.digest, 64) || !adaptiveHex(r.instance, 32) ||
		len(r.identity) > 4096 || !utf8.ValidString(r.identity) || r.epoch < 0 || r.job < 0 || r.generation < 0 || r.streak < 0 || r.streak > 2 || r.lastStreak < 0 || r.lastStreak > 2 ||
		r.maxNow <= 0 || r.finalized < 0 || r.finalized > r.epoch || r.finished < 0 || r.finished > r.maxNow || r.due < 0 ||
		(r.interval != int64(InventoryRevisitInterval) && r.interval != int64(AdaptiveStableRevisitInterval)) ||
		(r.outcome != "not_recorded" && r.outcome != "changed" && r.outcome != "unknown" && r.outcome != "unchanged") ||
		(r.complete && (!r.started || !r.claimed || r.generation <= 0)) || (!r.started && r.generation != 0) ||
		(r.epoch == 0 && (r.job != 0 || r.generation != 0 || r.started || r.complete || r.finalized != 0)) ||
		(r.finished > 0 && (r.finished > math.MaxInt64-r.interval || r.due != r.finished+r.interval)) {
		return adaptiveRevisitRow{}, ErrAdaptiveRevisitCorrupt
	}
	return r, nil
}

func writeAdaptiveRevisit(ctx context.Context, tx *sql.Tx, r adaptiveRevisitRow) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO adaptive_inventory_revisits(`+adaptiveColumns+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(root_id) DO UPDATE SET root_path=excluded.root_path,scope_digest=excluded.scope_digest,enabled=excluded.enabled,instance=excluded.instance,
 root_identity=excluded.root_identity,epoch=excluded.epoch,root_job_id=excluded.root_job_id,root_generation=excluded.root_generation,
 claimed=excluded.claimed,started=excluded.started,root_complete=excluded.root_complete,changed=excluded.changed,unknown=excluded.unknown,
 unchanged_streak=excluded.unchanged_streak,max_now_ns=excluded.max_now_ns,finalized_epoch=excluded.finalized_epoch,
 finished_ns=excluded.finished_ns,scheduled_due_ns=excluded.scheduled_due_ns,interval_ns=excluded.interval_ns,last_initialized=excluded.last_initialized,last_changed=excluded.last_changed,last_unknown=excluded.last_unknown,last_streak=excluded.last_streak,last_outcome=excluded.last_outcome`,
		r.root, r.path, r.digest, r.enabled, r.instance, r.identity, r.epoch, r.job, r.generation, r.claimed, r.started, r.complete, r.changed, r.unknown, r.streak,
		r.maxNow, r.finalized, r.finished, r.due, r.interval, r.lastInitialized, r.lastChanged, r.lastUnknown, r.lastStreak, r.outcome)
	return err
}

func adaptiveNow(ctx context.Context, now time.Time) (int64, error) {
	n, err := fairInventoryNow(ctx, now)
	if errors.Is(err, ErrFairInventoryInput) {
		err = ErrAdaptiveRevisitInput
	}
	return n, err
}

func (s *Store) validAdaptiveScope(scope AdaptiveRevisitScope) bool {
	return s.schema >= 13 && s.validFairScope(scope.roots) && adaptiveHex(scope.digest, 64) && adaptiveHex(scope.instance, 32)
}

func adaptiveRoot(ctx context.Context, tx *sql.Tx, root fairInventoryRoot) (string, error) {
	var path []byte
	var identity string
	var enabled int
	err := tx.QueryRowContext(ctx, "SELECT substr(path,1,4097),enabled,substr(volume_id,1,4097) FROM roots WHERE id=?", root.id).Scan(&path, &enabled, &identity)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrAdaptiveRevisitInput
	}
	if err != nil {
		return "", err
	}
	if enabled != 1 || !bytes.Equal(path, root.path) || len(identity) > 4096 || !utf8.ValidString(identity) {
		return "", ErrAdaptiveRevisitInput
	}
	return identity, nil
}

// ConfigureAdaptiveRevisits examines at most 32 indexed admitted roots. A first
// or changed scope and a previously started unfinished epoch cannot earn quiet
// credit. Only an exact never-claimed adaptive future job may be shortened
// to its saved daily baseline after a policy change; all started work is retained.
// Call once at worker startup; old scope handles are invalidated atomically.
func (s *Store) ConfigureAdaptiveRevisits(ctx context.Context, roots FairInventoryRoots, enabled bool, digest string, now time.Time) (AdaptiveRevisitScope, error) {
	n, err := adaptiveNow(ctx, now)
	if err != nil {
		return AdaptiveRevisitScope{}, err
	}
	if s.readOnly || s.schema < 13 || !s.validFairScope(roots) || !adaptiveHex(digest, 64) {
		return AdaptiveRevisitScope{}, ErrAdaptiveRevisitInput
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		return AdaptiveRevisitScope{}, err
	}
	scope := AdaptiveRevisitScope{roots: roots, digest: digest, enabled: enabled, instance: hex.EncodeToString(token[:])}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdaptiveRevisitScope{}, err
	}
	defer tx.Rollback()
	if enabled {
		if err = adaptiveDetailedMode(ctx, tx); err != nil {
			return AdaptiveRevisitScope{}, err
		}
	} else if _, err = readBackgroundInventoryMode(ctx, tx); err != nil {
		return AdaptiveRevisitScope{}, err
	}
	for _, root := range roots.roots {
		identity, e := adaptiveRoot(ctx, tx, root)
		if e != nil {
			return AdaptiveRevisitScope{}, e
		}
		r, e := readAdaptiveRevisit(ctx, tx, root.id)
		fresh := errors.Is(e, sql.ErrNoRows)
		if e != nil && !fresh {
			return AdaptiveRevisitScope{}, e
		}
		if fresh {
			r = adaptiveRevisitRow{root: root.id, path: bytes.Clone(root.path), identity: identity, digest: digest, enabled: enabled, maxNow: n, unknown: true, interval: int64(InventoryRevisitInterval), outcome: "not_recorded"}
			var id int64
			e = tx.QueryRowContext(ctx, "SELECT id FROM jobs WHERE root_id=? AND kind=? AND path=X'2e'", root.id, ScanKind).Scan(&id)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return AdaptiveRevisitScope{}, e
			}
			var busy bool
			if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE root_id=? AND kind=?)", root.id, ScanKind).Scan(&busy); e != nil {
				return AdaptiveRevisitScope{}, e
			}
			if busy {
				r.epoch = 1
				r.job = id
				r.claimed = true
				r.started = true
			}
		} else {
			if n < r.maxNow {
				return AdaptiveRevisitScope{}, ErrAdaptiveRevisitClock
			}
			policyChanged := !bytes.Equal(r.path, root.path) || r.digest != digest || r.enabled != enabled || (r.identity != "" && r.identity != identity)
			if policyChanged || r.started || r.claimed {
				r.unknown = true
				r.streak = 0
			}
			// Future scheduling provenance remains historical even when a scope
			// change resets learning. The narrow never-claimed daily exception
			// does not reset cursors, errors, tokens or started work.
			if policyChanged {
				if e = shortenUntouchedAdaptive(ctx, tx, &r); e != nil {
					return AdaptiveRevisitScope{}, e
				}
			}
			r.path = bytes.Clone(root.path)
			r.digest = digest
			r.enabled = enabled
			r.identity = identity
			r.maxNow = n
		}
		r.instance = scope.instance
		if err = writeAdaptiveRevisit(ctx, tx, r); err != nil {
			return AdaptiveRevisitScope{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitScope{}, err
	}
	if err = tx.Commit(); err != nil {
		return AdaptiveRevisitScope{}, err
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitScope{}, err
	}
	return scope, nil
}

func adaptiveDetailedMode(ctx context.Context, tx *sql.Tx) error {
	var mode []byte
	var modeType string
	err := tx.QueryRowContext(ctx, "SELECT substr(value,1,2),typeof(value) FROM settings WHERE key='inventory.compact'").Scan(&mode, &modeType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if modeType != "blob" || (string(mode) != "0" && string(mode) != "1") {
		return ErrAdaptiveRevisitCorrupt
	}
	if string(mode) == "1" {
		return ErrInventoryRevisitMode
	}
	return nil
}

func (s *Store) adaptiveBoundRow(ctx context.Context, tx *sql.Tx, scope AdaptiveRevisitScope, rootID int64) (adaptiveRevisitRow, error) {
	if !s.validAdaptiveScope(scope) {
		return adaptiveRevisitRow{}, ErrAdaptiveRevisitInput
	}
	if err := adaptiveDetailedMode(ctx, tx); err != nil {
		return adaptiveRevisitRow{}, err
	}
	for _, root := range scope.roots.roots {
		if root.id != rootID {
			continue
		}
		identity, err := adaptiveRoot(ctx, tx, root)
		if err != nil {
			return adaptiveRevisitRow{}, err
		}
		r, err := readAdaptiveRevisit(ctx, tx, root.id)
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrAdaptiveRevisitInput
		}
		if err != nil {
			return adaptiveRevisitRow{}, err
		}
		if r.digest != scope.digest || r.enabled != scope.enabled || r.instance != scope.instance || !bytes.Equal(r.path, root.path) || (r.identity != "" && r.identity != identity) {
			return adaptiveRevisitRow{}, ErrAdaptiveRevisitInput
		}
		return r, nil
	}
	return adaptiveRevisitRow{}, ErrAdaptiveRevisitInput
}

// FinalizeAdaptiveRevisit learns at most once, and only when all this root's
// inventory jobs and saved maintenance have drained. It atomically schedules
// the next exact listing. Its completion time is a scheduling anchor, not a
// simultaneous observation of the tree. It never rewrites existing work.
func (s *Store) FinalizeAdaptiveRevisit(ctx context.Context, scope AdaptiveRevisitScope, rootID int64, now time.Time) (AdaptiveRevisitResult, error) {
	return s.finalizeAdaptiveRevisit(ctx, scope, rootID, now, inventoryRevisitHooks{})
}

func (s *Store) finalizeAdaptiveRevisit(ctx context.Context, scope AdaptiveRevisitScope, rootID int64, now time.Time, hooks inventoryRevisitHooks) (AdaptiveRevisitResult, error) {
	n, err := adaptiveNow(ctx, now)
	if err != nil {
		return AdaptiveRevisitResult{}, err
	}
	if s.readOnly || !scope.enabled {
		return AdaptiveRevisitResult{}, ErrAdaptiveRevisitInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdaptiveRevisitResult{}, err
	}
	defer tx.Rollback()
	r, err := s.adaptiveBoundRow(ctx, tx, scope, rootID)
	if err != nil {
		return AdaptiveRevisitResult{}, err
	}
	if n < r.maxNow {
		return AdaptiveRevisitResult{}, ErrAdaptiveRevisitClock
	}
	result := AdaptiveRevisitResult{Epoch: r.epoch, Interval: time.Duration(r.interval), UnchangedStreak: r.streak, Changed: r.changed, Unknown: r.unknown}
	if r.due > 0 {
		result.Due = time.Unix(0, r.due)
	}
	var busy bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE root_id=? AND kind=?)", rootID, ScanKind).Scan(&busy); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	remaining, e := fairInventoryMaintenanceRemaining(ctx, tx, rootID)
	if e != nil {
		return AdaptiveRevisitResult{}, e
	}
	if busy || remaining {
		if busy && !r.started && !r.claimed && r.due > 0 {
			result.Initialized = r.lastInitialized
			result.Epoch = r.finalized
			result.UnchangedStreak = r.lastStreak
			result.Changed = r.lastChanged
			result.Unknown = r.lastUnknown
		}
		return result, ctx.Err()
	}
	initial := r.epoch == 0
	initializing := initial || !r.started || !r.complete
	if !initial {
		if !r.started || !r.complete {
			r.unknown = true
		}
		if r.changed || r.unknown {
			r.streak = 0
		} else {
			r.streak = min(2, r.streak+1)
		}
		if r.unknown {
			r.outcome = "unknown"
		} else if r.changed {
			r.outcome = "changed"
		} else {
			r.outcome = "unchanged"
		}
	}
	interval := int64(InventoryRevisitInterval)
	if r.streak == 2 {
		interval = int64(AdaptiveStableRevisitInterval)
	}
	if r.epoch == math.MaxInt64 || n > math.MaxInt64-interval {
		return AdaptiveRevisitResult{}, ErrAdaptiveRevisitClock
	}
	due := n + interval
	if initial {
		due = n
	}
	var job int64
	if err = tx.QueryRowContext(ctx, `INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed) VALUES(?,?,X'2e',?,0) RETURNING id`, rootID, ScanKind, due).Scan(&job); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	result = AdaptiveRevisitResult{Scheduled: true, Initialized: initializing, Due: time.Unix(0, due), Interval: time.Duration(interval), Epoch: r.epoch, UnchangedStreak: r.streak, Changed: r.changed, Unknown: r.unknown}
	r.lastInitialized = initializing
	r.lastChanged = r.changed
	r.lastUnknown = r.unknown
	r.lastStreak = r.streak
	r.finalized = r.epoch
	r.epoch++
	r.job = job
	r.generation = 0
	r.claimed = false
	r.started = false
	r.complete = false
	r.changed = false
	r.unknown = false
	if initial {
		r.unknown = true
	}
	r.finished = n
	if initial {
		r.finished = 0
	}
	r.due = due
	r.interval = interval
	r.maxNow = n
	if err = writeAdaptiveRevisit(ctx, tx, r); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitResult{}, err
	}
	return result, nil
}

func (s *Store) AdaptiveRevisitStatus(ctx context.Context, scope AdaptiveRevisitScope) (AdaptiveRevisitStatus, error) {
	if ctx == nil || !s.validAdaptiveScope(scope) {
		return AdaptiveRevisitStatus{}, ErrAdaptiveRevisitInput
	}
	if err := ctx.Err(); err != nil {
		return AdaptiveRevisitStatus{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdaptiveRevisitStatus{}, err
	}
	defer tx.Rollback()
	status := AdaptiveRevisitStatus{Contract: AdaptiveRevisitContract, HistoricalMetadataOnly: true}
	for _, root := range scope.roots.roots {
		r, e := s.adaptiveBoundRow(ctx, tx, scope, root.id)
		if e != nil {
			return AdaptiveRevisitStatus{}, e
		}
		status.TrackedRoots++
		if r.started {
			status.ActiveEpochs++
		}
		if r.changed {
			status.ChangedEpochs++
		}
		if r.unknown {
			status.UnknownEpochs++
		}
		if r.streak == 2 {
			status.StableRoots++
		}
		if r.interval == int64(AdaptiveStableRevisitInterval) {
			status.WeeklyRoots++
		} else {
			status.DailyRoots++
		}
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitStatus{}, err
	}
	if err = tx.Commit(); err != nil {
		return AdaptiveRevisitStatus{}, err
	}
	if err = ctx.Err(); err != nil {
		return AdaptiveRevisitStatus{}, err
	}
	return status, nil
}

// AdaptiveRevisitPending qualifies the next raw enabled job at an exact due
// time with at most 128 raw jobs plus one continuation row. Disabled rows
// consume slots. Unknown/nonadaptive work stays generic.
func (s *Store) AdaptiveRevisitPending(ctx context.Context, scope AdaptiveRevisitScope, due time.Time) (bool, error) {
	n, err := adaptiveNow(ctx, due)
	if err != nil {
		return false, err
	}
	if !s.validAdaptiveScope(scope) || !scope.enabled {
		return false, ErrAdaptiveRevisitInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	bound := make(map[int64]adaptiveRevisitRow, len(scope.roots.roots))
	for _, root := range scope.roots.roots {
		r, e := s.adaptiveBoundRow(ctx, tx, scope, root.id)
		if e != nil {
			return false, e
		}
		bound[root.id] = r
	}
	type candidate struct {
		root, id, attempts, enabled int64
		kind, fault                 string
		path, cursor                []byte
	}
	rows, err := tx.QueryContext(ctx, `SELECT j.root_id,j.id,substr(j.kind,1,33),substr(j.path,1,4097),substr(j.cursor,1,65537),j.attempts,substr(j.last_error,1,2049),
 (SELECT enabled FROM roots WHERE id=j.root_id) FROM jobs j
 WHERE j.status='pending' AND j.due_at_ns=? ORDER BY j.id LIMIT ?`, n, InventoryRevisitPageSize+1)
	if err != nil {
		return false, err
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.root, &c.id, &c.kind, &c.path, &c.cursor, &c.attempts, &c.fault, &c.enabled); err != nil {
			rows.Close()
			return false, err
		}
		candidates = append(candidates, c)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return false, err
	}
	if len(candidates) > InventoryRevisitPageSize {
		candidates = candidates[:InventoryRevisitPageSize]
	}
	matched := false
	for _, c := range candidates {
		if c.enabled == 0 {
			continue
		}
		if c.enabled != 1 {
			return false, ErrAdaptiveRevisitCorrupt
		}
		r, ok := bound[c.root]
		matched = ok && c.kind == ScanKind && bytes.Equal(c.path, []byte(".")) && len(c.cursor) == 0 && c.attempts == 0 && c.fault == "" &&
			r.job == c.id && r.due == n && !r.claimed && !r.started && r.finalized == r.epoch-1 && r.finished > 0
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
