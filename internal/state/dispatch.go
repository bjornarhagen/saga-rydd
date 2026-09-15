package state

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"
)

var ErrDispatchDeferred = errors.New("scan dispatch deferred")

// DispatchBudget accounts for reserved batches, not metadata operations or
// bytes. A reservation is never refunded after a crash or cancellation.
type DispatchBudget struct {
	Day         string    `json:"day_utc"`
	Used        int       `json:"reserved_chunks"`
	Limit       int       `json:"limit"`
	NextAllowed time.Time `json:"next_allowed_at"`
	Reason      string    `json:"wait_reason"`
}
type queryRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func dispatchBudget(ctx context.Context, db queryRow, now time.Time, limit int) (DispatchBudget, error) {
	if limit < 1 || limit > 100000 {
		return DispatchBudget{}, errors.New("invalid scan chunk limit")
	}
	b := DispatchBudget{Day: now.UTC().Format(time.DateOnly), Limit: limit, NextAllowed: now.UTC()}
	var day string
	var used int
	var last, next int64
	err := db.QueryRowContext(ctx, "SELECT day,chunks,last_start_ns,next_start_ns FROM scan_dispatch WHERE id=1").Scan(&day, &used, &last, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	parsed, err := time.Parse(time.DateOnly, day)
	if err != nil {
		return b, errors.New("invalid dispatch budget day")
	}
	if day >= b.Day {
		b.Day = day
		b.Used = used
	}
	if now.UnixNano() < next {
		b.NextAllowed = time.Unix(0, next).UTC()
		b.Reason = "cadence"
	}
	if b.Used >= limit {
		midnight := parsed.AddDate(0, 0, 1)
		if midnight.After(b.NextAllowed) {
			b.NextAllowed = midnight
		}
		b.Reason = "daily_chunk_limit"
	}
	if now.UnixNano() < last {
		b.Reason = "clock_rollback"
	}
	return b, nil
}

func (s *Store) DispatchBudget(ctx context.Context, now time.Time, limit int) (DispatchBudget, error) {
	return dispatchBudget(ctx, s.db, now, limit)
}

func (s *Store) ReserveScanChunk(ctx context.Context, now time.Time, interval time.Duration, limit int) (DispatchBudget, error) {
	if s.readOnly {
		return DispatchBudget{}, errors.New("state is read-only")
	}
	if interval <= 0 || interval > 24*time.Hour {
		return DispatchBudget{}, errors.New("invalid dispatch interval")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DispatchBudget{}, err
	}
	defer tx.Rollback()
	b, err := dispatchBudget(ctx, tx, now, limit)
	if err != nil {
		return b, err
	}
	if b.Reason != "" {
		return b, ErrDispatchDeferred
	}
	b.Used++
	b.NextAllowed = now.Add(interval).UTC()
	b.Reason = "cadence"
	_, err = tx.ExecContext(ctx, `INSERT INTO scan_dispatch VALUES(1,?,?,?,?)
 ON CONFLICT(id) DO UPDATE SET day=excluded.day,chunks=excluded.chunks,last_start_ns=excluded.last_start_ns,next_start_ns=excluded.next_start_ns`, b.Day, b.Used, now.UnixNano(), b.NextAllowed.UnixNano())
	if err != nil {
		return b, err
	}
	return b, tx.Commit()
}

// WALBlocked uses file size only as a trigger for a passive checkpoint. A large
// fully checkpointed WAL can be reused without truncation and must not deadlock
// scanning merely because its allocated file remains large.
func (s *Store) WALBlocked(ctx context.Context, threshold int64) (bool, error) {
	if s.readOnly {
		return false, errors.New("state is read-only")
	}
	if threshold < 0 {
		return false, errors.New("invalid WAL threshold")
	}
	info, err := os.Stat(s.path + "-wal")
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Size() <= threshold {
		return false, nil
	}
	checkpoint, err := s.Checkpoint(ctx)
	if err != nil {
		return false, err
	}
	return checkpoint.Busy != 0 || checkpoint.LogPages > checkpoint.CheckpointedPages, nil
}
