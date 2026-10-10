package storeexport

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestGuardRollbackDeadlineAndCancellationStayTerminal(t *testing.T) {
	for _, kind := range []string{"wall-rollback", "elapsed-rollback-above-start", "wall-expiry", "elapsed-expiry", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			elapsed := 17 * time.Second
			calls := 0
			g := guardWithClocks(ctx, func() time.Time { calls++; return wall }, func() time.Duration { calls++; return elapsed })
			wall = wall.Add(2 * time.Second)
			elapsed += time.Second
			if err := g.check(); err != nil {
				t.Fatal(err)
			}
			oldWall, oldElapsed := g.wallHigh, g.elapsedHigh
			switch kind {
			case "wall-rollback":
				wall = wall.Add(-time.Nanosecond)
				elapsed += time.Second
			case "elapsed-rollback-above-start":
				elapsed -= time.Nanosecond
				wall = wall.Add(time.Second)
			case "wall-expiry":
				wall = g.wallEnd
			case "elapsed-expiry":
				elapsed = g.elapsedEnd
			case "cancel":
				wall = wall.Add(time.Second)
				elapsed += time.Second
				cancel()
			}
			first := g.check()
			if first == nil {
				t.Fatal("missing terminal refusal")
			}
			if kind == "cancel" {
				if !errors.Is(first, context.Canceled) {
					t.Fatal(first)
				}
			} else if !errors.Is(first, ErrClock) {
				t.Fatal(first)
			}
			if g.wallHigh.Before(oldWall) || g.elapsedHigh < oldElapsed {
				t.Fatal("high-water mark decreased")
			}
			if kind == "elapsed-rollback-above-start" && (elapsed <= g.elapsedStart || g.elapsedHigh != oldElapsed || g.wallHigh != wall) {
				t.Fatal("elapsed rollback fixture or independent highs are wrong")
			}
			if kind == "wall-rollback" && (g.wallHigh != oldWall || g.elapsedHigh != elapsed) {
				t.Fatal("wall rollback changed independent highs")
			}
			stoppedCalls := calls
			wall = wall.Add(time.Hour)
			elapsed += time.Hour
			if g.check() != first || calls != stoppedCalls {
				t.Fatal("terminal refusal changed or resampled clocks")
			}
		})
	}
}

func TestGuardExactCooperativeBoundaryAndEarlierCaller(t *testing.T) {
	for _, domain := range []string{"wall", "elapsed"} {
		t.Run(domain, func(t *testing.T) {
			wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			elapsed := 11 * time.Second
			g := guardWithClocks(context.Background(), func() time.Time { return wall }, func() time.Duration { return elapsed })
			if domain == "wall" {
				wall = g.wallEnd.Add(-time.Nanosecond)
			} else {
				elapsed = g.elapsedEnd - time.Nanosecond
			}
			if err := g.check(); err != nil {
				t.Fatal(err)
			}
			if domain == "wall" {
				wall = g.wallEnd
			} else {
				elapsed = g.elapsedEnd
			}
			if !errors.Is(g.check(), ErrClock) {
				t.Fatal("independent 120s boundary not refused")
			}
		})
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if !errors.Is(newGuard(ctx).check(), context.DeadlineExceeded) {
		t.Fatal("earlier caller deadline lost")
	}
}

func TestGuardInvalidInitialAndObservedTimesStayDenied(t *testing.T) {
	valid := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"zero-wall", "wall-before-year-one", "wall-after-year-9999", "wall-end-overflow", "negative-elapsed", "elapsed-end-overflow"} {
		t.Run("initial-"+kind, func(t *testing.T) {
			wall, elapsed := valid, time.Duration(0)
			calls := 0
			switch kind {
			case "zero-wall":
				wall = time.Time{}
			case "wall-before-year-one":
				wall = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
			case "wall-after-year-9999":
				wall = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "wall-end-overflow":
				wall = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
			case "negative-elapsed":
				elapsed = -1
			case "elapsed-end-overflow":
				elapsed = time.Duration(math.MaxInt64) - wholeLimit + 1
			}
			g := guardWithClocks(context.Background(), func() time.Time { calls++; return wall }, func() time.Duration { calls++; return elapsed })
			wall, elapsed = valid, 0
			if !errors.Is(g.check(), ErrClock) || calls != 2 {
				t.Fatal("invalid constructor was not sticky")
			}
		})
	}
	for _, kind := range []string{"zero-wall", "invalid-wall", "negative-elapsed", "maximum-elapsed"} {
		t.Run("observed-"+kind, func(t *testing.T) {
			wall, elapsed := valid, time.Duration(0)
			g := guardWithClocks(context.Background(), func() time.Time { return wall }, func() time.Duration { return elapsed })
			wall = wall.Add(time.Second)
			elapsed = time.Second
			if err := g.check(); err != nil {
				t.Fatal(err)
			}
			oldWall, oldElapsed := g.wallHigh, g.elapsedHigh
			switch kind {
			case "zero-wall":
				wall = time.Time{}
			case "invalid-wall":
				wall = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "negative-elapsed":
				elapsed = -1
			case "maximum-elapsed":
				elapsed = time.Duration(math.MaxInt64)
			}
			if !errors.Is(g.check(), ErrClock) {
				t.Fatal("invalid observation accepted")
			}
			if g.wallHigh.Before(oldWall) || g.elapsedHigh < oldElapsed {
				t.Fatal("invalid observation reduced high-water")
			}
			if (kind == "zero-wall" || kind == "invalid-wall") && g.wallHigh != oldWall {
				t.Fatal("invalid wall time retained as high-water")
			}
		})
	}
	wall := valid
	elapsed := time.Duration(math.MaxInt64) - wholeLimit
	g := guardWithClocks(context.Background(), func() time.Time { return wall }, func() time.Duration { return elapsed })
	if err := g.check(); err != nil {
		t.Fatal("representable exact elapsed endpoint refused", err)
	}
	elapsed = time.Duration(math.MaxInt64)
	if !errors.Is(g.check(), ErrClock) {
		t.Fatal("exact maximum endpoint accepted")
	}
}

func TestStoragePathAndScopeArePureAndExact(t *testing.T) {
	for _, path := range []string{"", "relative", "/tmp/../x", "/tmp/x/", "/tmp/\x00x", "/tmp/\xff", "/" + strings.Repeat("a", maxPathBytes)} {
		if cleanPath(path) == nil {
			t.Fatalf("accepted invalid path %q", path)
		}
	}
	if err := cleanPath("/generated/$literal`path`"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := outputPath("/generated/base", "/generated/base/plans/out"); err == nil {
		t.Fatal("source scope output accepted")
	}
	if _, _, err := outputPath("/generated/base", "/generated/output"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExportPlanStore(ctx, "/absent-generated-input", "/absent-generated-output"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
