package inventory

import (
	"context"
	"errors"
	"fmt"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestEntryPacingCancellationAndNoCatchUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Scanner{}
		if err := WithEntryRate(10)(s); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := s.paceEntry(ctx); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := s.paceEntry(ctx); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Millisecond {
			t.Fatal(elapsed)
		}
		// Sleeping for hours does not earn a burst of inspections.
		time.Sleep(time.Hour)
		if err := s.paceEntry(ctx); err != nil {
			t.Fatal(err)
		}
		cancelCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- s.paceEntry(cancelCtx) }()
		synctest.Wait()
		if !s.Metrics().Throttled {
			t.Fatal("wait not visible")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		start = time.Now()
		if err := s.paceEntry(ctx); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 100*time.Millisecond {
			t.Fatal("cancel granted a free entry")
		}
		if s.Metrics().Throttled || s.Metrics().ThrottleWaitNS != uint64(200*time.Millisecond) {
			t.Fatal(s.Metrics())
		}
	})
}

func TestSlowEntryPacingPreservesPartialBatch(t *testing.T) {
	root := t.TempDir()
	s, err := New([]string{root}, nil, nil, WithEntryRate(20))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 7; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("file-%d", i)))
	}
	j := state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Kind: state.ScanKind}
	seen := map[string]bool{}
	var generation int64
	complete := false
	partials := 0
	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		batch, err := s.Next(ctx, j)
		cancel()
		if err != nil || batch.Fault != "" {
			t.Fatal(batch, err)
		}
		if generation == 0 {
			generation = batch.Generation
		}
		if generation != batch.Generation {
			t.Fatal("throttle wait restarted enumeration")
		}
		for _, entry := range batch.Entries {
			path := string(entry.Path)
			if seen[path] {
				t.Fatal("replayed a committed entry", path)
			}
			seen[path] = true
		}
		if batch.Complete {
			complete = true
			break
		}
		partials++
		j.Cursor = batch.Cursor
	}
	if !complete || len(seen) != 7 || partials < 2 || s.Metrics().EntryInspections != 7 || s.Metrics().ThrottleWaitNS == 0 {
		t.Fatal(complete, seen, partials, s.Metrics())
	}
}

func TestMinimumEntryRateCompletesWithOneSecondWindows(t *testing.T) {
	root := t.TempDir()
	s, err := New([]string{root}, nil, nil, WithEntryRate(1))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 3; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("file-%d", i)))
	}
	j := state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Kind: state.ScanKind}
	start := time.Now()
	entries := 0
	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		batch, err := s.Next(ctx, j)
		cancel()
		if err != nil || batch.Fault != "" {
			t.Fatal(batch, err)
		}
		entries += len(batch.Entries)
		if batch.Complete {
			if entries != 3 || s.Metrics().EntryInspections != 3 || time.Since(start) < 2*time.Second {
				t.Fatal(entries, s.Metrics(), time.Since(start))
			}
			return
		}
		j.Cursor = batch.Cursor
	}
	t.Fatal("minimum-rate scan did not complete", entries, s.Metrics())
}

func TestPacedPendingNamesAreDiscardedAfterDirectoryMutation(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 3; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("old-%d", i)))
	}
	s, err := New([]string{root}, nil, nil, WithEntryRate(10))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	j := state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Kind: state.ScanKind}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	first, err := s.Next(ctx, j)
	cancel()
	if err != nil || first.Fault != "" || s.current == nil || len(s.current.pending) == 0 {
		t.Fatal(first, err)
	}
	for i := 0; i < 3; i++ {
		if err := os.Remove(filepath.Join(root, fmt.Sprintf("old-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(root, "new"))
	stamp := time.Now().Add(time.Hour)
	if err := os.Chtimes(root, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	j.Cursor = first.Cursor
	second := next(t, s, j)
	if second.Generation == first.Generation || len(second.Entries) != 1 || string(second.Entries[0].Path) != "new" {
		t.Fatal(second)
	}
}

func TestExactEntryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Scanner{}
		if err := WithEntryDelay(1500 * time.Millisecond)(s); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := s.paceEntry(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.paceEntry(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 1500*time.Millisecond {
			t.Fatal(time.Since(start))
		}
		if err := WithEntryDelay(0)(s); err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		if err := s.paceEntry(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 0 {
			t.Fatal("--now still paced")
		}
	})
}
