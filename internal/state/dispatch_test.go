package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDispatchSurvivesRestartAndClockChanges(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	if _, err = s.ReserveScanChunk(ctx, now, time.Minute, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveScanChunk(ctx, now.Add(time.Second), time.Minute, 2); !errors.Is(err, ErrDispatchDeferred) {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, err := s.DispatchBudget(ctx, now.Add(time.Second), 2)
	if err != nil || b.Used != 1 || b.Reason != "cadence" {
		t.Fatal(b, err)
	}
	if _, err = s.ReserveScanChunk(ctx, now.Add(time.Minute), time.Minute, 2); err != nil {
		t.Fatal(err)
	}
	b, err = s.DispatchBudget(ctx, now.Add(time.Hour), 2)
	if err != nil || b.Reason != "daily_chunk_limit" {
		t.Fatal(b, err)
	}
	b, err = s.DispatchBudget(ctx, now.Add(-24*time.Hour), 2)
	if err != nil || b.Used != 2 || b.Reason != "clock_rollback" {
		t.Fatal(b, err)
	}
	b, err = s.ReserveScanChunk(ctx, now.Add(24*time.Hour), time.Minute, 2)
	if err != nil || b.Used != 1 {
		t.Fatal(b, err)
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM scan_dispatch").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func TestWALGateWaitsForPinnedReader(t *testing.T) {
	ctx := context.Background()
	s, err := OpenWriter(ctx, privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(ctx, []string{"/fixture"}); err != nil {
		t.Fatal(err)
	}
	r, err := connect(ctx, s.path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRow("SELECT count(*) FROM roots").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(ctx, []string{"/fixture", "/second"}); err != nil {
		t.Fatal(err)
	}
	blocked, err := s.WALBlocked(ctx, 0)
	if err != nil || !blocked {
		t.Fatal(blocked, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	blocked, err = s.WALBlocked(ctx, 0)
	if err != nil || blocked {
		t.Fatal(blocked, err)
	}
}
