package inventory

import (
	"context"
	"errors"
	"time"
)

// Option configures one scanner before it starts. Library fixtures can omit
// pacing; the worker always supplies its configured rate.
type Option func(*Scanner) error

func WithEntryRate(rate int) Option {
	return func(s *Scanner) error {
		if rate < 1 || rate > 100000 {
			return errors.New("entry rate must be 1–100000")
		}
		s.entryRate = rate
		// Round up so integer truncation never permits a faster rate.
		s.entrySpacing = (time.Second + time.Duration(rate) - 1) / time.Duration(rate)
		return nil
	}
}

// paceEntry permits one inspection immediately, then spaces later starts.
// Idle time never accumulates burst credits. State survives partial batches
// and scanner resets, but not worker restarts (dispatch cadence is durable).
// The caller holds s.mu; status reads only the atomic diagnostics.
func (s *Scanner) paceEntry(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.entrySpacing == 0 {
		return nil
	}
	if delay := time.Until(s.nextEntry); delay > 0 {
		started := time.Now()
		s.metrics.throttled.Store(true)
		timer := time.NewTimer(delay)
		defer func() {
			timer.Stop()
			s.metrics.waitNS.Add(uint64(time.Since(started)))
			s.metrics.throttled.Store(false)
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.nextEntry = time.Now().Add(s.entrySpacing)
	return nil
}

// WithEntryDelay sets exact spacing for explicitly invoked foreground scans.
func WithEntryDelay(delay time.Duration) Option {
	return func(s *Scanner) error {
		if delay < 0 || delay > time.Minute {
			return errors.New("entry delay must be between zero and one minute")
		}
		s.entrySpacing = delay
		s.entryRate = 0
		if delay > 0 {
			s.entryRate = int(time.Second / delay)
		}
		return nil
	}
}
