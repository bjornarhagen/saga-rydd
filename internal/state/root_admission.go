package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"time"
)

const RootAdmissionContract = "inventory_root_admission_v1"
const RootAdmissionLimit = 128
const RootAdmissionInputLimit = 128
const RootAdmissionPathBytes = 4096

const rootAdmissionWindow = 5 * time.Second
const rootAdmissionIDPrefix = "root-sync-v1-"

var (
	ErrRootAdmissionInput       = errors.New("inventory root selection must contain 1–128 unique canonical absolute paths of at most 4096 bytes")
	ErrRootAdmissionCapacity    = errors.New("retained inventory root capacity reached; existing roots and history remain available")
	ErrRootAdmissionCorrupt     = errors.New("saved inventory root evidence is malformed; existing records were not repaired")
	ErrRootAdmissionUnavailable = errors.New("inventory root synchronization is unavailable; inspect the existing state")
	ErrRootAdmissionPublication = errors.New("inventory root synchronization outcome is uncertain; inspect the saved enabled roots")
)

// RootAdmissionPublicationError retains the frozen request identity, not proof
// of membership publication. Its public text never includes the SQL cause.
type RootAdmissionPublicationError struct {
	RequestID string
	Cause     error
}

func (e *RootAdmissionPublicationError) Error() string {
	return ErrRootAdmissionPublication.Error() + "; request " + e.RequestID
}

func (e *RootAdmissionPublicationError) Unwrap() error {
	return errors.Join(ErrRootAdmissionPublication, e.Cause)
}

func ValidRootAdmissionRequestID(id string) bool {
	if len(id) != len(rootAdmissionIDPrefix)+64 || !strings.HasPrefix(id, rootAdmissionIDPrefix) {
		return false
	}
	for _, c := range id[len(rootAdmissionIDPrefix):] {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}

type rootAdmissionOperationError struct{ cause error }

func (e rootAdmissionOperationError) Error() string { return ErrRootAdmissionUnavailable.Error() }
func (e rootAdmissionOperationError) Unwrap() error {
	return errors.Join(ErrRootAdmissionUnavailable, e.cause)
}

type rootAdmissionHooks struct {
	beforeCommit func()
	afterCommit  func()
	noOpReleased func()
	commit       func(*sql.Tx) error
}

type admittedRoot struct {
	id      int64
	path    []byte
	enabled int64
}

func canonicalAdmissionPath(path []byte) bool {
	return len(path) > 0 && len(path) <= RootAdmissionPathBytes && !bytes.ContainsRune(path, 0) &&
		filepath.IsAbs(string(path)) && filepath.Clean(string(path)) == string(path)
}

func rootAdmissionRequest(ctx context.Context, roots []string) ([]string, string, error) {
	if ctx == nil {
		return nil, "", ErrRootAdmissionInput
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if len(roots) < 1 || len(roots) > RootAdmissionInputLimit {
		return nil, "", ErrRootAdmissionInput
	}
	// Copy before validation or callbacks. Authoritative paths are bytes, so
	// invalid UTF-8 in supported manual paths never collides through JSON.
	paths := append([]string(nil), roots...)
	seen := make(map[string]bool, len(paths))
	digest := sha256.New()
	_, _ = digest.Write([]byte(RootAdmissionContract))
	var frame [4]byte
	binary.BigEndian.PutUint32(frame[:], uint32(len(paths)))
	_, _ = digest.Write(frame[:])
	for _, path := range paths {
		// Reject oversized caller strings before any byte conversion. Bounded
		// preflight must not depend on the compiler eliminating that copy.
		if len(path) < 1 || len(path) > RootAdmissionPathBytes {
			return nil, "", ErrRootAdmissionInput
		}
		if !canonicalAdmissionPath([]byte(path)) || seen[path] {
			return nil, "", ErrRootAdmissionInput
		}
		seen[path] = true
		binary.BigEndian.PutUint32(frame[:], uint32(len(path)))
		_, _ = digest.Write(frame[:])
		_, _ = digest.Write([]byte(path))
	}
	return paths, rootAdmissionIDPrefix + hex.EncodeToString(digest.Sum(nil)), nil
}

// CASE projects at most 4096 path bytes and fixed scalars before Scan. The
// separate shape flag prevents NULL/fallback values from validating damage.
const rootAdmissionProjection = `CASE WHEN typeof(id)='integer' THEN id ELSE 0 END,
CASE WHEN typeof(path)='blob' THEN CASE WHEN length(path) BETWEEN 1 AND 4096 THEN substr(path,1,4097) ELSE NULL END ELSE NULL END,
CASE WHEN typeof(enabled)='integer' THEN enabled ELSE NULL END,
typeof(id)='integer' AND id>0 AND typeof(path)='blob' AND length(path) BETWEEN 1 AND 4096 AND typeof(enabled)='integer' AND enabled IN (0,1)`

// Only Boolean data crosses this legacy-wide query. Nested CASE establishes
// lazy evaluation before bounded byte tests. Engine/page examination is not
// bounded by the returned-row limit and remains cooperatively cancelable.
const rootAdmissionBadShape = `SELECT EXISTS(SELECT 1 FROM roots WHERE
CASE WHEN typeof(id)='integer' THEN id>0 ELSE 0 END != 1 OR
CASE WHEN typeof(enabled)='integer' THEN enabled IN(0,1) ELSE 0 END != 1 OR
CASE WHEN typeof(path)='blob' THEN CASE WHEN length(path) BETWEEN 1 AND 4096 THEN
 substr(path,1,1)=X'2f' AND instr(path,X'00')=0 AND instr(path,X'2f2f')=0 AND
 instr(path,X'2f2e2f')=0 AND instr(path,X'2f2e2e2f')=0 AND
 substr(path,-2)!=X'2f2e' AND substr(path,-3)!=X'2f2e2e' AND
 (path=X'2f' OR substr(path,-1)!=X'2f')
ELSE 0 END ELSE 0 END != 1)`

func rootAdmissionScan(rows *sql.Rows) (admittedRoot, error) {
	var r admittedRoot
	var enabled sql.NullInt64
	var shaped bool
	if err := rows.Scan(&r.id, &r.path, &enabled, &shaped); err != nil {
		return admittedRoot{}, ErrRootAdmissionCorrupt
	}
	if !shaped || r.id <= 0 || !enabled.Valid || (enabled.Int64 != 0 && enabled.Int64 != 1) || !canonicalAdmissionPath(r.path) {
		return admittedRoot{}, ErrRootAdmissionCorrupt
	}
	r.enabled = enabled.Int64
	return r, nil
}

func rootAdmissionFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	for _, definite := range []error{ErrRootAdmissionInput, ErrRootAdmissionCorrupt, ErrRootAdmissionCapacity} {
		if errors.Is(err, definite) {
			return definite
		}
	}
	return rootAdmissionOperationError{cause: err}
}

// syncRootsAdmitted limits new retained rows for updated writers only. Existing
// schema-14/15 readers and optional CPU activation remain unchanged. Older
// binaries and external SQL can bypass this admission; it is no hard size cap.
func (s *Store) syncRootsAdmitted(parent context.Context, roots []string, hooks rootAdmissionHooks) error {
	paths, requestID, err := rootAdmissionRequest(parent, roots)
	if err != nil {
		return err
	}
	if s == nil || s.db == nil || s.readOnly {
		return rootAdmissionOperationError{cause: errors.New("root synchronization requires an open writer")}
	}
	ctx, cancel := context.WithTimeout(parent, rootAdmissionWindow)
	defer cancel()
	// Keep the sole connection held through outcome checks. On uncertainty,
	// closing the DB before releasing it vetoes other queued Store operations.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	defer tx.Rollback()
	var malformed bool
	if err = tx.QueryRowContext(ctx, rootAdmissionBadShape).Scan(&malformed); err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	if malformed {
		return rootAdmissionFailure(ctx, ErrRootAdmissionCorrupt)
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+rootAdmissionProjection+" FROM roots ORDER BY id LIMIT 129")
	if err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	retained, enabled := 0, 0
	seen := make(map[string]bool, RootAdmissionLimit+1)
	for rows.Next() {
		r, scanErr := rootAdmissionScan(rows)
		if scanErr != nil || seen[string(r.path)] {
			_ = rows.Close()
			return rootAdmissionFailure(ctx, ErrRootAdmissionCorrupt)
		}
		seen[string(r.path)] = true
		retained++
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	missing := 0
	for _, path := range paths {
		// This CAST-based alias probe is not an indexed lookup. Its returned
		// Boolean is finite; SQLite engine work is separately deadline-bound.
		var alias bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM roots WHERE typeof(path)='text' AND CAST(path AS BLOB)=?)", []byte(path)).Scan(&alias); err != nil {
			return rootAdmissionFailure(ctx, err)
		}
		if alias {
			return rootAdmissionFailure(ctx, ErrRootAdmissionCorrupt)
		}
		rows, err = tx.QueryContext(ctx, "SELECT "+rootAdmissionProjection+" FROM roots WHERE path=? LIMIT 2", []byte(path))
		if err != nil {
			return rootAdmissionFailure(ctx, err)
		}
		found := 0
		for rows.Next() {
			r, scanErr := rootAdmissionScan(rows)
			if scanErr != nil || !bytes.Equal(r.path, []byte(path)) {
				_ = rows.Close()
				return rootAdmissionFailure(ctx, ErrRootAdmissionCorrupt)
			}
			found++
			enabled += int(r.enabled)
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return rootAdmissionFailure(ctx, err)
		}
		if found > 1 {
			return rootAdmissionFailure(ctx, ErrRootAdmissionCorrupt)
		}
		if found == 0 {
			missing++
		}
	}
	if missing > 0 && (retained > RootAdmissionLimit || missing > RootAdmissionLimit-retained) {
		return rootAdmissionFailure(ctx, ErrRootAdmissionCapacity)
	}
	var extraEnabled bool
	// Finite Boolean projection, not an all-table examination bound. Exact
	// selected paths are supplied BLOBs; no saved payload is returned here.
	args := make([]any, len(paths))
	for i, path := range paths {
		args[i] = []byte(path)
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(paths)), ",")
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM roots WHERE enabled=1 AND path NOT IN ("+marks+"))", args...).Scan(&extraEnabled); err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	if missing == 0 && enabled == len(paths) && !extraEnabled {
		if err = tx.Rollback(); err != nil {
			return rootAdmissionFailure(ctx, err)
		}
		if err = conn.Close(); err != nil {
			return rootAdmissionFailure(ctx, err)
		}
		if hooks.noOpReleased != nil {
			hooks.noOpReleased()
		}
		return ctx.Err()
	}
	if _, err = tx.ExecContext(ctx, "UPDATE roots SET enabled=0 WHERE enabled=1"); err != nil {
		return rootAdmissionFailure(ctx, err)
	}
	for _, path := range paths {
		if _, err = tx.ExecContext(ctx, "INSERT INTO roots(path,enabled) VALUES(?,1) ON CONFLICT(path) DO UPDATE SET enabled=1", []byte(path)); err != nil {
			return rootAdmissionFailure(ctx, err)
		}
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	commit := tx.Commit
	if hooks.commit != nil {
		commit = func() error { return hooks.commit(tx) }
	}
	err = commit()
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	if err != nil || ctx.Err() != nil {
		cause := errors.Join(err, ctx.Err())
		_ = tx.Rollback()
		_ = s.db.Close()
		_ = conn.Close()
		_ = s.Close()
		return &RootAdmissionPublicationError{RequestID: requestID, Cause: cause}
	}
	return nil
}
