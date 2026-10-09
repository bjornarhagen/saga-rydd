package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// AdaptiveRevisitScopeDigest performs no filesystem access. It binds effective
// lexical roots, exclusions, protected paths and the fixed detailed/skip policy.
// Path order/duplicates do not change that scope. Byte paths remain exact.
func AdaptiveRevisitScopeDigest(roots, excludes, protected []string) (string, error) {
	if ValidateFairInventoryPaths(roots) != nil || len(excludes)+len(protected) > 65536 {
		return "", ErrAdaptiveRevisitInput
	}
	h := sha256.New()
	h.Write([]byte(AdaptiveRevisitContract + ":detailed:skip_unknown_v1\x00"))
	total := 0
	for _, paths := range [][]string{roots, excludes, protected} {
		copyPaths := append([]string(nil), paths...)
		for _, path := range copyPaths {
			if len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
				return "", ErrAdaptiveRevisitInput
			}
			total += len(path)
			if total > 4<<20 {
				return "", ErrAdaptiveRevisitInput
			}
		}
		sort.Strings(copyPaths)
		var width [8]byte
		previous := ""
		for _, path := range copyPaths {
			if path == previous {
				continue
			}
			previous = path
			binary.BigEndian.PutUint64(width[:], uint64(len(path)))
			h.Write(width[:])
			h.Write([]byte(path))
		}
		binary.BigEndian.PutUint64(width[:], 0)
		h.Write(width[:])
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func shortenUntouchedAdaptive(ctx context.Context, tx *sql.Tx, r *adaptiveRevisitRow) error {
	if r.claimed || r.started || r.job == 0 || r.finished <= 0 || r.interval != int64(AdaptiveStableRevisitInterval) {
		return nil
	}
	base := r.finished + int64(InventoryRevisitInterval)
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET due_at_ns=? WHERE id=? AND root_id=? AND kind=? AND path=X'2e'
 AND status='pending' AND due_at_ns=? AND (cursor IS NULL OR length(cursor)=0) AND attempts=0 AND last_error='' AND lease_token='' AND lease_until_ns=0`,
		base, r.job, r.root, ScanKind, r.due)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		r.due = base
		r.interval = int64(InventoryRevisitInterval)
	}
	return nil
}

// Claim provenance is sticky even when FinishJob resets attempts to zero. It
// is recorded with the lease before any callback or source operation begins.
func adaptiveClaim(ctx context.Context, tx *sql.Tx, j Job) error {
	if j.Kind != ScanKind {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE adaptive_inventory_revisits SET claimed=1,
 unknown=CASE WHEN root_job_id!=? AND started=0 THEN 1 ELSE unknown END,
 unchanged_streak=CASE WHEN root_job_id!=? AND started=0 THEN 0 ELSE unchanged_streak END WHERE root_id=?`, j.ID, j.ID, j.RootID)
	return err
}

func adaptiveUncertain(ctx context.Context, tx *sql.Tx, rootID int64) error {
	_, err := tx.ExecContext(ctx, "UPDATE adaptive_inventory_revisits SET unknown=1,unchanged_streak=0 WHERE root_id=?", rootID)
	return err
}

func adaptiveChanged(ctx context.Context, tx *sql.Tx, rootID int64) error {
	_, err := tx.ExecContext(ctx, "UPDATE adaptive_inventory_revisits SET changed=1 WHERE root_id=?", rootID)
	return err
}

func adaptiveComparable(e Entry) bool {
	device, deviceErr := strconv.ParseUint(e.Device, 10, 64)
	inode, inodeErr := strconv.ParseUint(e.Inode, 10, 64)
	return (e.Kind == "file" || e.Kind == "directory" || e.Kind == "symlink" || e.Kind == "other") && e.Size >= 0 && e.Allocated >= 0 &&
		deviceErr == nil && inodeErr == nil && device > 0 && inode > 0 && len(e.Device) <= 20 && len(e.Inode) <= 20 &&
		strconv.FormatUint(device, 10) == e.Device && strconv.FormatUint(inode, 10) == e.Inode &&
		(e.SkipReason == "" || e.SkipReason == "excluded or protected")
}

func adaptiveCompareEntry(ctx context.Context, tx *sql.Tx, root int64, e Entry) (bool, bool, error) {
	var old Entry
	var validTypes bool
	err := tx.QueryRowContext(ctx, `SELECT substr(kind,1,17),
 CASE WHEN typeof(size)='integer' THEN size ELSE -1 END,
 CASE WHEN typeof(allocated)='integer' THEN allocated ELSE -1 END,
 CASE WHEN typeof(mtime_ns)='integer' THEN mtime_ns ELSE 0 END,
 CASE WHEN typeof(ctime_ns)='integer' THEN ctime_ns ELSE 0 END,
 substr(device,1,21),substr(inode,1,21),substr(skip_reason,1,257),
 typeof(kind)='text' AND typeof(size)='integer' AND typeof(allocated)='integer' AND typeof(mtime_ns)='integer' AND typeof(ctime_ns)='integer'
 AND typeof(device)='text' AND typeof(inode)='text' AND typeof(skip_reason)='text' FROM entries WHERE root_id=? AND path=?`, root, e.Path).Scan(
		&old.Kind, &old.Size, &old.Allocated, &old.MtimeNS, &old.CtimeNS, &old.Device, &old.Inode, &old.SkipReason, &validTypes)
	if errors.Is(err, sql.ErrNoRows) {
		return true, !adaptiveComparable(e), nil
	}
	if err != nil {
		return false, false, err
	}
	unknown := !validTypes || !adaptiveComparable(old) || !adaptiveComparable(e)
	changed := old.Kind != e.Kind || old.Size != e.Size || old.Allocated != e.Allocated || old.MtimeNS != e.MtimeNS || old.CtimeNS != e.CtimeNS || old.Device != e.Device || old.Inode != e.Inode || old.SkipReason != e.SkipReason
	return changed, unknown, nil
}

// CommitAdaptiveScan uses the same lease, comparison, entry upserts, child jobs
// and cursor transaction as CommitScan. Ordinary partial chunks retain a single
// epoch; failures and untracked calls cannot earn quiet metadata credit.
func (s *Store) CommitAdaptiveScan(ctx context.Context, scope AdaptiveRevisitScope, j Job, b ScanBatch, now time.Time) error {
	if _, err := adaptiveNow(ctx, now); err != nil {
		return err
	}
	if !scope.enabled || !s.validAdaptiveScope(scope) {
		return ErrAdaptiveRevisitInput
	}
	if len(b.Entries) > MaxBatchEntries {
		return ErrAdaptiveRevisitInput
	}
	if b.Fault != "" && now.UnixNano() > math.MaxInt64-int64(time.Hour) {
		return ErrAdaptiveRevisitClock
	}
	if b.Fault == "" && (!utf8.ValidString(b.Identity) || len(b.Identity) > 4096 || len(b.Directory.Path) > 4096 || len(b.Directory.SkipReason) > 256) {
		return ErrAdaptiveRevisitInput
	}
	if len(b.Directory.Device) > 128 || len(b.Directory.Inode) > 128 {
		return ErrAdaptiveRevisitInput
	}
	for _, e := range b.Entries {
		if len(e.Device) > 128 || len(e.Inode) > 128 {
			return ErrAdaptiveRevisitInput
		}
	}
	return s.commitScan(ctx, j, b, &scope, now)
}

func (s *Store) adaptiveScan(ctx context.Context, tx *sql.Tx, scope *AdaptiveRevisitScope, j Job, b ScanBatch, now time.Time) error {
	if s.schema < 13 {
		return nil
	}
	if scope == nil {
		return adaptiveUncertain(ctx, tx, j.RootID)
	}
	r, err := s.adaptiveBoundRow(ctx, tx, *scope, j.RootID)
	if err != nil {
		return err
	}
	n := now.UnixNano()
	if n < r.maxNow {
		return ErrAdaptiveRevisitClock
	}
	r.maxNow = n
	if r.epoch == 0 {
		return ErrAdaptiveRevisitInput
	}
	if b.Fault != "" {
		r.unknown = true
		r.streak = 0
		return writeAdaptiveRevisit(ctx, tx, r)
	}
	if r.epoch == 0 || r.job == 0 {
		r.unknown = true
	}
	if bytes.Equal(j.Path, []byte(".")) {
		if r.job != j.ID || (r.generation != 0 && r.generation != b.Generation) || r.complete {
			r.unknown = true
		}
		r.started = true
		r.claimed = true
		r.generation = b.Generation
		if b.Complete {
			r.complete = true
		}
		if r.identity == "" {
			r.identity = b.Identity
		} else if r.identity != b.Identity {
			r.unknown = true
		}
	} else if !r.started {
		r.unknown = true
	}
	// A child directory is observed again when it opens. Compare that metadata
	// without overwriting its parent's membership generation.
	changed, unknown, e := adaptiveCompareEntry(ctx, tx, j.RootID, b.Directory)
	if e != nil {
		return e
	}
	r.changed = r.changed || changed
	r.unknown = r.unknown || unknown
	seen := make(map[string]bool, len(b.Entries))
	for _, entry := range b.Entries {
		if seen[string(entry.Path)] {
			r.unknown = true
		}
		seen[string(entry.Path)] = true
		changed, unknown, e := adaptiveCompareEntry(ctx, tx, j.RootID, entry)
		if e != nil {
			return e
		}
		r.changed = r.changed || changed
		r.unknown = r.unknown || unknown
	}
	if r.unknown {
		r.streak = 0
	}
	return writeAdaptiveRevisit(ctx, tx, r)
}
