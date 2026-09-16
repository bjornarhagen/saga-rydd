package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

var ErrStaleLease = errors.New("job lease no longer belongs to this attempt")

const MaxCursorBytes = 64 << 10

type Job struct {
	ID, RootID   int64
	Kind         string
	Path, Cursor []byte
	Attempts     int
	Token        string
	LeaseUntil   time.Time
	RootPath     []byte
	RootIdentity string
}

func validKind(kind string) bool {
	if len(kind) == 0 || len(kind) > 32 {
		return false
	}
	for _, r := range kind {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// EnqueueJob is idempotent and never resets a running attempt or saved cursor.
func (s *Store) EnqueueJob(ctx context.Context, rootID int64, kind string, path []byte, due time.Time) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	p := string(path)
	if p == "" {
		p = "."
	}
	p = filepath.Clean(p)
	if rootID <= 0 || !validKind(kind) || len(p) > 4096 || strings.ContainsRune(p, 0) || filepath.IsAbs(p) || p == ".." || strings.HasPrefix(p, ".."+string(filepath.Separator)) {
		return errors.New("invalid job root, kind or relative path")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(?,?,?,?)
 ON CONFLICT(root_id,kind,path) DO NOTHING`, rootID, kind, []byte(p), due.UnixNano())
	return err
}

func kindsClause(kinds []string) (string, []any, error) {
	if len(kinds) > 64 {
		return "", nil, errors.New("too many job handlers")
	}
	args := make([]any, len(kinds))
	marks := make([]string, len(kinds))
	for i, kind := range kinds {
		if !validKind(kind) {
			return "", nil, fmt.Errorf("invalid kind %q", kind)
		}
		args[i] = kind
		marks[i] = "?"
	}
	return strings.Join(marks, ","), args, nil
}

func (s *Store) NextJobDue(ctx context.Context, kinds []string) (time.Time, error) {
	if len(kinds) == 0 {
		return time.Time{}, nil
	}
	clause, args, err := kindsClause(kinds)
	if err != nil {
		return time.Time{}, err
	}
	var due sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT min(j.due_at_ns) FROM jobs j JOIN roots r ON r.id=j.root_id
 WHERE j.status='pending' AND r.enabled=1 AND j.kind IN (`+clause+`)`, args...).Scan(&due)
	if err != nil || !due.Valid {
		return time.Time{}, err
	}
	return time.Unix(0, due.Int64), nil
}

func (s *Store) ClaimJob(ctx context.Context, kinds []string, now time.Time, lease time.Duration) (*Job, error) {
	if s.readOnly {
		return nil, errors.New("state is read-only")
	}
	if len(kinds) == 0 {
		return nil, nil
	}
	if lease <= 0 || lease > 48*time.Hour {
		return nil, errors.New("invalid lease duration")
	}
	clause, args, err := kindsClause(kinds)
	if err != nil {
		return nil, err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	params := []any{hex.EncodeToString(token[:]), now.Add(lease).UnixNano(), now.UnixNano()}
	params = append(params, args...)
	var j Job
	var until int64
	err = s.db.QueryRowContext(ctx, `UPDATE jobs SET status='running',lease_token=?,lease_until_ns=?,attempts=attempts+1
 WHERE id=(SELECT j.id FROM jobs j JOIN roots r ON r.id=j.root_id WHERE j.status='pending' AND j.due_at_ns<=?
 AND r.enabled=1 AND j.kind IN (`+clause+`) ORDER BY j.due_at_ns,j.id LIMIT 1)
 RETURNING id,root_id,kind,path,cursor,attempts,lease_token,lease_until_ns,
 (SELECT path FROM roots WHERE roots.id=jobs.root_id),
 (SELECT volume_id FROM roots WHERE roots.id=jobs.root_id)`, params...).Scan(&j.ID, &j.RootID, &j.Kind, &j.Path, &j.Cursor, &j.Attempts, &j.Token, &until, &j.RootPath, &j.RootIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j.LeaseUntil = time.Unix(0, until)
	return &j, nil
}

// FinishJob commits a cursor or completion only for the current attempt.
func (s *Store) FinishJob(ctx context.Context, j Job, done bool, cursor []byte, due time.Time, lastError string) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	return finishJob(ctx, s.db, j, done, cursor, due, lastError)
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func finishJob(ctx context.Context, db executor, j Job, done bool, cursor []byte, due time.Time, lastError string) error {
	if j.Token == "" {
		return ErrStaleLease
	}
	if len(cursor) > MaxCursorBytes {
		return errors.New("job cursor exceeds 64 KiB")
	}
	if len(lastError) > 2048 {
		lastError = lastError[:2048]
	}
	var result sql.Result
	var err error
	if done {
		result, err = db.ExecContext(ctx, "DELETE FROM jobs WHERE id=? AND status='running' AND lease_token=?", j.ID, j.Token)
	} else {
		result, err = db.ExecContext(ctx, `UPDATE jobs SET status='pending',cursor=?,due_at_ns=?,last_error=?,lease_token='',lease_until_ns=0,
 attempts=CASE WHEN ?='' THEN 0 ELSE attempts END WHERE id=? AND status='running' AND lease_token=?`, cursor, due.UnixNano(), lastError, lastError, j.ID, j.Token)
	}
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrStaleLease
	}
	return nil
}

// RecoverJobs is called only after taking the exclusive writer lock. Recover all
// running jobs even if a wall-clock lease has not expired: the former owner is
// gone. Inventory keeps its existing priority so a partially enumerated parent
// stays ahead of children it already queued. Never steal an in-process task
// merely because the machine slept.
func (s *Store) RecoverJobs(ctx context.Context, now time.Time) (int64, error) {
	if s.readOnly || s.lock == nil {
		return 0, errors.New("recovery requires the exclusive writer lock")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='pending',
 due_at_ns=CASE WHEN kind=? THEN due_at_ns ELSE ? END,lease_token='',lease_until_ns=0
 WHERE status='running'`, ScanKind, now.UnixNano())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) Paused(ctx context.Context) (bool, error) {
	var value []byte
	err := s.db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key='worker.paused'").Scan(&value)
	if err != nil {
		return false, err
	}
	switch string(value) {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, errors.New("invalid saved pause setting; refusing to run")
	}
}

func (s *Store) SetPaused(ctx context.Context, paused bool) error {
	if s.readOnly {
		return errors.New("state is read-only")
	}
	value := []byte("0")
	if paused {
		value = []byte("1")
	}
	result, err := s.db.ExecContext(ctx, "UPDATE settings SET value=? WHERE key='worker.paused'", value)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("missing pause setting")
	}
	return nil
}
