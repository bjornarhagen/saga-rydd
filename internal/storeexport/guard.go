package storeexport

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// A guard is cooperative. Entered filesystem and driver calls can outlast it.
// It never starts a replacement goroutine to escape a slow call.
type guard struct {
	ctx                                   context.Context
	wallNow                               func() time.Time
	elapsedNow                            func() time.Duration
	wallStart, wallHigh, wallEnd          time.Time
	elapsedStart, elapsedHigh, elapsedEnd time.Duration
	failed                                error
}

func newGuard(ctx context.Context) *guard {
	origin := time.Now()
	return guardWithClocks(ctx, time.Now, func() time.Duration { return time.Since(origin) })
}

func validWall(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }

// The clock domains have separate starts, high-water marks and endpoints.
// Elapsed readings use a monotonic origin in production. No comparison or
// subtraction converts between an elapsed reading and a wall timestamp.
func guardWithClocks(ctx context.Context, wallNow func() time.Time, elapsedNow func() time.Duration) *guard {
	g := &guard{ctx: ctx, wallNow: wallNow, elapsedNow: elapsedNow}
	w, e := wallNow().UTC().Round(0), elapsedNow()
	if validWall(w) {
		g.wallStart, g.wallHigh, g.wallEnd = w, w, w.Add(wholeLimit)
	} else {
		g.failed = ErrClock
	}
	if e >= 0 && e <= time.Duration(math.MaxInt64)-wholeLimit {
		g.elapsedStart, g.elapsedHigh, g.elapsedEnd = e, e, e+wholeLimit
	} else {
		g.failed = ErrClock
	}
	if !validWall(g.wallEnd) {
		g.failed = ErrClock
	}
	if g.failed == nil {
		g.failed = ctx.Err()
	}
	return g
}

func (g *guard) check() error {
	if g.failed != nil {
		return g.failed
	}
	w, e := g.wallNow().UTC().Round(0), g.elapsedNow()
	wallOK, elapsedOK := validWall(w), e >= 0
	wallBack, elapsedBack := wallOK && w.Before(g.wallHigh), elapsedOK && e < g.elapsedHigh
	// Keep the largest valid observations even when this check denies work.
	if wallOK && w.After(g.wallHigh) {
		g.wallHigh = w
	}
	if elapsedOK && e > g.elapsedHigh {
		g.elapsedHigh = e
	}
	if err := g.ctx.Err(); err != nil {
		g.failed = err
	} else if !wallOK || !elapsedOK || wallBack || elapsedBack || !w.Before(g.wallEnd) || e >= g.elapsedEnd {
		g.failed = ErrClock
	}
	return g.failed
}

func cleanPath(p string) error {
	if len(p) == 0 || len(p) > maxPathBytes || !utf8.ValidString(p) || strings.IndexByte(p, 0) >= 0 || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return ErrBounds
	}
	return nil
}

func beneath(parent, path string) bool {
	r, err := filepath.Rel(parent, path)
	return err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator))
}

func outputPath(base, destination string) (string, string, error) {
	if err := cleanPath(base); err != nil {
		return "", "", err
	}
	if err := cleanPath(destination); err != nil {
		return "", "", err
	}
	parent, name := filepath.Dir(destination), filepath.Base(destination)
	if destination == string(filepath.Separator) || len(name) > 255 || name == "." || name == ".." || beneath(base, parent) || len(parent)+80 > maxPathBytes {
		return "", "", ErrProfile
	}
	return parent, name, nil
}

func joined(current *error, next error) {
	if next != nil {
		*current = errors.Join(*current, next)
	}
}
