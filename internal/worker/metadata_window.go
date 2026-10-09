package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var (
	errMetadataExpired   = errors.New("scanner metadata allowance expired")
	errMetadataRollback  = errors.New("scanner metadata clock moved backwards")
	errMetadataExhausted = errors.New("scanner metadata allowance exhausted")
)

// A window admits scanner API attempts only. It holds no database connection
// and neither returns unused credits nor claims to count physical I/O. Separate
// clocks allow tests to move wall time without extending elapsed-time expiry.
type metadataWindow struct {
	reservation         state.MetadataReservation
	wallNow, elapsedNow func() time.Time
	deadline            time.Time
	wallDeadline        time.Time
	mu                  sync.Mutex
	highWater           time.Time
	observed            int64
}

func newMetadataWindow(reservation state.MetadataReservation, work time.Duration) *metadataWindow {
	return metadataWindowWithClocks(reservation, work, time.Now, time.Now)
}

func metadataWindowWithClocks(reservation state.MetadataReservation, work time.Duration, wallNow, elapsedNow func() time.Time) *metadataWindow {
	wall := wallNow().UTC()
	// Anchor wall work to the charged reservation, not delayed construction.
	// Go's elapsed clock can stop during suspend; a forward wall gap must not
	// extend this source window. Neither clock can grant catch-up allowance.
	wallDeadline := minTime(reservation.StartedAt.Add(work), reservation.ExpiresAt)
	remaining := min(wallDeadline.Sub(wall), work)
	return &metadataWindow{reservation: reservation, wallNow: wallNow, elapsedNow: elapsedNow,
		deadline: elapsedNow().Add(remaining), wallDeadline: wallDeadline, highWater: reservation.ClockHighWater}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (w *metadataWindow) permit(ctx context.Context, kind inventory.APICallKind) error {
	return w.permitAt(ctx, kind, w.wallNow().UTC(), w.elapsedNow())
}

func (w *metadataWindow) permitAt(ctx context.Context, kind inventory.APICallKind, wall, elapsed time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ctx == nil {
		return state.ErrMetadataInvalid
	}
	if err := context.Cause(ctx); err != nil {
		return errors.Join(ctx.Err(), err)
	}
	switch kind {
	case inventory.APIStat, inventory.APIDirectoryOpen, inventory.APIDirectoryRead,
		inventory.APIFilesystemStat, inventory.APIMountIdentity, inventory.APIPathResolution:
	default:
		return state.ErrMetadataInvalid
	}
	if _, err := w.fenceLocked(ctx, wall, elapsed); err != nil {
		return err
	}
	if w.observed >= w.reservation.Allowance {
		return errMetadataExhausted
	}
	w.observed++ // Count admission immediately before the scanner API attempt.
	return nil
}

// A pacing wait or capacity check observes clocks without admitting an API.
// Retain its highest wall observation for settlement, including refused waits.
func (w *metadataWindow) fenceAt(ctx context.Context, wall, elapsed time.Time) (time.Duration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fenceLocked(ctx, wall, elapsed)
}

func (w *metadataWindow) fenceLocked(ctx context.Context, wall, elapsed time.Time) (time.Duration, error) {
	if ctx == nil || w.reservation.Allowance < 1 || w.reservation.Allowance > state.MetadataAllowanceLimit {
		return 0, state.ErrMetadataInvalid
	}
	wall = wall.Round(0).UTC()
	rollback := wall.Before(w.highWater)
	if wall.After(w.highWater) {
		w.highWater = wall
	}
	if err := context.Cause(ctx); err != nil {
		return 0, errors.Join(ctx.Err(), err)
	}
	if rollback {
		return 0, errMetadataRollback
	}
	remaining := min(w.wallDeadline.Sub(wall), w.reservation.ExpiresAt.Sub(wall), w.deadline.Sub(elapsed))
	if deadline, ok := ctx.Deadline(); ok {
		remaining = min(remaining, time.Until(deadline))
	}
	if remaining <= 0 {
		return 0, errMetadataExpired
	}
	return remaining, nil
}

func (w *metadataWindow) usage() (int64, time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.observed, w.highWater
}

// Sequential operations carry the highest observed wall time and earliest
// work deadlines across their permit windows. The Next reservation predates
// construction's calls; it cannot extend the preceding source work window.
func (w *metadataWindow) inheritHighWater(previous *metadataWindow) {
	previous.mu.Lock()
	highWater, deadline, wallDeadline := previous.highWater, previous.deadline, previous.wallDeadline
	previous.mu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if highWater.After(w.highWater) {
		w.highWater = highWater
	}
	w.deadline = minTime(w.deadline, deadline)
	w.wallDeadline = minTime(w.wallDeadline, wallDeadline)
}

// Publish and Close can overlap when construction is interrupted. Either
// ordering closes the published scanner, without blocking status on source I/O.
type scannerHolder struct {
	value  atomic.Pointer[inventory.Scanner]
	closed atomic.Bool
}

func (s *scannerHolder) publish(scanner *inventory.Scanner) {
	s.value.Store(scanner)
	if s.closed.Load() {
		scanner.Close()
	}
}

func (s *scannerHolder) close() {
	s.closed.Store(true)
	if scanner := s.value.Load(); scanner != nil {
		scanner.Close()
	}
}

// Readiness includes all allowances needed for one operation. A positive but
// insufficient remainder must not claim jobs or burn dispatch reservations.
func metadataReadiness(b state.MetadataBudget, now time.Time, startup int64) (time.Time, string, error) {
	if !b.Available {
		return time.Time{}, "", state.ErrMetadataUnavailable
	}
	if b.Reason != "" {
		if b.NextAllowedAt == nil {
			return time.Time{}, "", state.ErrMetadataOutstanding
		}
		return *b.NextAllowedAt, b.Reason, nil
	}
	used := int64(0)
	if b.DayCharges != nil {
		used = b.DayCharges.Reserved
	}
	needed := inventory.MaxAPIAttemptAllowance + startup
	if startup < 0 || startup > inventory.MaxAPIAttemptAllowance || b.Limit < 1 || used < 0 {
		return time.Time{}, "", state.ErrMetadataInvalid
	}
	if used > b.Limit || needed > b.Limit-used {
		utc := now.UTC()
		midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
		return midnight, "daily_metadata_limit", nil
	}
	return time.Time{}, "", nil
}

func metadataInterruption(err error) bool {
	return errors.Is(err, inventory.ErrAPIPermitDenied) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func settleMetadata(ctx context.Context, store *state.Store, windows []*metadataWindow, unknown bool, now time.Time) error {
	if len(windows) == 0 {
		return nil
	}
	// Persist the highest wall time seen by any permit, even on a denied call.
	// This closes a restart loophole after a forward jump followed by rollback.
	for _, window := range windows {
		_, highWater := window.usage()
		if highWater.After(now) {
			now = highWater
		}
	}
	if unknown {
		_, err := store.RecoverMetadataReservations(ctx, now)
		return err
	}
	for _, window := range windows {
		observed, _ := window.usage()
		if err := store.SettleMetadata(ctx, window.reservation, observed, now); err != nil {
			return err
		}
	}
	return nil
}
