package inventory

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var (
	ErrHashReadPacingInput    = errors.New("hash read pacing requires 1–1073741824 requested bytes per second, a 0–64 byte minimum, and a remaining window of at most five seconds")
	ErrHashReadPacingCapacity = errors.New("the hash read pacing rate cannot fit the minimum durable read quantum in the remaining work window")
	ErrHashReadReservationDay = errors.New("the hash read reservation UTC day ended before another read could be admitted")
)

const HashReadPacingContract = "requested_read_spacing_v1"

// HashReadPacingObservation describes only this explicit operation. Each read
// prepays its requested bytes, including the first, short and failed requests.
// It is not a rolling, process-wide or physical-device throughput guarantee.
// No pacing state or rate is saved in a consent, checkpoint or budget record.
type HashReadPacingObservation struct {
	Contract                string         `json:"contract"`
	RequestedBytesPerSecond int64          `json:"requested_bytes_per_second"`
	ObservedWait            *time.Duration `json:"observed_wait_ns"`
	Yielded                 bool           `json:"yielded"`
}

// CheckHashReadPacingCapacity is a pure conservative preflight, not an
// authorization or reusable execution token. A positive minimum must fit in
// one quarter of the remaining work window, leaving time for held-file and
// final checks. Callers still need the exact selected-work check at dispatch.
// An empty file or idle queue can pass a zero minimum without a read wait.
func CheckHashReadPacingCapacity(rate, minBytes int64, workRemaining time.Duration) error {
	if rate < 1 || rate > 1<<30 || minBytes < 0 || minBytes > 64 || workRemaining < 0 || workRemaining > fileHashStepTimeout {
		return ErrHashReadPacingInput
	}
	if minBytes == 0 {
		return nil
	}
	if hashReadPacingCost(rate, minBytes) > workRemaining/4 {
		return ErrHashReadPacingCapacity
	}
	return nil
}

func hashReadPacingCost(rate, bytes int64) time.Duration {
	// Internal requests are <=32 KiB, so both products and the ceiling fit.
	return time.Duration((bytes*int64(time.Second) + rate - 1) / rate)
}

// RunConsentedPaced keeps the exact immutable scope, consent and full durable
// reservation of RunConsented. It performs one step with an operation-local
// requested-byte spacing ceiling. Other stores and unpaced APIs are outside
// this ceiling; no automatic continuation or retry is introduced.
func (s *HashStore) RunConsentedPaced(ctx context.Context, id string, source *state.Store, scanner *Scanner, rate int64) (HashRunResult, error) {
	if err := CheckHashReadPacingCapacity(rate, 0, fileHashStepTimeout); err != nil {
		return HashRunResult{}, err
	}
	if s == nil || s.now == nil {
		return HashRunResult{}, errors.New("hash store is unavailable")
	}
	p := newHashReadPacer(rate, s.now, time.Now, nil)
	return s.runConsented(ctx, id, source, scanner, hashStoreHooks{pacing: p})
}

// RunFreshConsentedPaced adds the same finite spacing ceiling to one exact
// fresh job step. The fresh controller still owns its inventory and historical
// stamp/mount checks, and no saved job, approval or continuation is rewritten.
func (s *HashStore) RunFreshConsentedPaced(ctx context.Context, id string, scanner *Scanner, rate int64) (HashFreshRunResult, error) {
	if err := CheckHashReadPacingCapacity(rate, 0, fileHashStepTimeout); err != nil {
		return HashFreshRunResult{}, err
	}
	if s == nil || s.now == nil {
		return HashFreshRunResult{}, errors.New("hash store is unavailable")
	}
	p := newHashReadPacer(rate, s.now, time.Now, nil)
	return s.runFreshConsented(ctx, id, scanner, hashFreshRunHooks{pacing: p})
}

type hashReadPacer struct {
	rate                          int64
	wall, elapsed                 func() time.Time
	wait                          func(context.Context, time.Duration) error
	wallDeadline, elapsedDeadline time.Time
	phaseWall, phaseElapsed       time.Time
	maxWall, lastElapsed          time.Time
	expiry                        time.Time
	day                           string
	cancel                        context.CancelCauseFunc
	refusal                       error
	waited                        time.Duration
	waitKnown                     bool
	yielded                       bool
}

func newHashReadPacer(rate int64, wall, elapsed func() time.Time, wait func(context.Context, time.Duration) error) *hashReadPacer {
	if wait == nil {
		wait = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-timer.C:
				return nil
			}
		}
	}
	w, e := wall().UTC(), elapsed()
	return &hashReadPacer{rate: rate, wall: wall, elapsed: elapsed, wait: wait, waitKnown: true, maxWall: w, lastElapsed: e, wallDeadline: w.Add(fileHashStepTimeout), elapsedDeadline: e.Add(fileHashStepTimeout)}
}

func (p *hashReadPacer) observation() *HashReadPacingObservation {
	if p == nil {
		return nil
	}
	waited := p.waited
	var observed *time.Duration
	if p.waitKnown {
		observed = &waited
	}
	return &HashReadPacingObservation{Contract: HashReadPacingContract, RequestedBytesPerSecond: p.rate, ObservedWait: observed, Yielded: p.yielded}
}

// Sampling is synchronous and operation-local. Retain every valid observed
// wall high-water BEFORE any cancellation/expiry/window refusal, including
// observations made by denied waits. Refusals are sticky through catch-up.
func (p *hashReadPacer) observe(ctx context.Context) (time.Time, time.Time, error) {
	return p.observeAt(ctx, p.wall().UTC(), p.elapsed())
}

func (p *hashReadPacer) observeAt(ctx context.Context, w, e time.Time) (time.Time, time.Time, error) {
	priorWall, priorElapsed := p.maxWall, p.lastElapsed
	if e.Before(priorElapsed) {
		p.waitKnown = false
	}
	if validHashReadClock(w) && w.After(p.maxWall) {
		p.maxWall = w
	}
	if e.After(p.lastElapsed) {
		p.lastElapsed = e
	}
	var err error
	switch {
	case p.refusal != nil:
		err = p.refusal
	case !validHashReadClock(w) || w.Before(priorWall) || e.Before(priorElapsed):
		err = ErrHashReadClockRollback
	case !p.expiry.IsZero() && !w.Before(p.expiry):
		err = ErrHashReadExpired
	case p.day != "" && w.Format(time.DateOnly) != p.day:
		err = ErrHashReadReservationDay
	case !w.Before(p.wallDeadline) || !e.Before(p.elapsedDeadline):
		err = context.DeadlineExceeded
	case ctx.Err() != nil:
		err = context.Cause(ctx)
	}
	if err != nil {
		if p.refusal == nil {
			p.refusal = err
		}
		if p.cancel != nil {
			p.cancel(p.refusal)
		}
		return w, e, p.refusal
	}
	return w, e, nil
}

func (p *hashReadPacer) bind(expiry, high time.Time, day string, cancel context.CancelCauseFunc) {
	p.expiry, p.day, p.cancel = expiry, day, cancel
	if high.After(p.maxWall) {
		p.maxWall = high
	}
	if p.refusal != nil && cancel != nil {
		cancel(p.refusal)
	}
}

func (p *hashReadPacer) preflight(ctx context.Context, left int64) error {
	w, e, err := p.observe(ctx)
	if err != nil {
		return err
	}
	minimum := min(left, int64(64))
	remaining := min(p.wallDeadline.Sub(w), p.elapsedDeadline.Sub(e), fileHashStepTimeout)
	if deadline, ok := ctx.Deadline(); ok {
		remaining = min(remaining, max(time.Until(deadline), 0))
	}
	return CheckHashReadPacingCapacity(p.rate, minimum, max(remaining, 0))
}

func (p *hashReadPacer) beginPhase(ctx context.Context, window time.Duration) error {
	w, e, err := p.observe(ctx)
	if err != nil {
		return err
	}
	window = min(window, max(p.wallDeadline.Sub(w)/2, 0), max(p.elapsedDeadline.Sub(e)/2, 0))
	p.phaseWall, p.phaseElapsed = w.Add(window), e.Add(window)
	return nil
}

// request returns zero for an ordinary soft yield. That yield still runs the
// existing held-file and path checks; it never consumes bytes or invalidates a
// tentative batch. A refusal cancels the controller's actual read context so
// settlement retains the old checkpoint rather than invalidating source.
func (p *hashReadPacer) request(ctx context.Context, want, left int64) (int64, error) {
	w, e, err := p.observe(ctx)
	if err != nil {
		return 0, err
	}
	remaining := min(p.phaseWall.Sub(w), p.phaseElapsed.Sub(e))
	if remaining <= 0 {
		p.yielded = true
		return 0, nil
	}
	// Reserving half the remaining phase avoids paying to its exact boundary.
	affordable := int64(remaining/2) * p.rate / int64(time.Second)
	want = min(want, affordable)
	if want < left {
		want -= want % 64
	}
	if want <= 0 {
		p.yielded = true
		return 0, nil
	}
	cost := hashReadPacingCost(p.rate, want)
	targetWall, targetElapsed := w.Add(cost), e.Add(cost)
	for {
		w, e, err = p.observe(ctx)
		if err != nil {
			return 0, err
		}
		if !w.Before(p.phaseWall) || !e.Before(p.phaseElapsed) {
			p.yielded = true
			return 0, nil
		}
		delay := max(targetWall.Sub(w), targetElapsed.Sub(e), 0)
		if delay == 0 {
			return want, nil
		}
		before := e
		waitErr := p.wait(ctx, min(delay, 25*time.Millisecond))
		w, e, err = p.observe(ctx)
		if spent := e.Sub(before); spent < 0 {
			p.waitKnown = false
		} else if spent > 0 {
			if spent > time.Duration(math.MaxInt64)-p.waited {
				p.waitKnown = false
			} else {
				p.waited += spent
			}
		}
		if err != nil {
			return 0, err
		}
		if waitErr != nil {
			p.refusal = waitErr
			if p.cancel != nil {
				p.cancel(waitErr)
			}
			return 0, waitErr
		}
	}
}

func hashReadPacingCode(err error) string {
	switch {
	case errors.Is(err, ErrHashReadPacingCapacity):
		return "read_pacing_capacity"
	case errors.Is(err, ErrHashReadReservationDay):
		return "reservation_day_ended"
	default:
		return hashReadErrorCode(err)
	}
}

// Keep observed pacing clock evidence in the existing consent settlement TX.
// Observe the actual current clock first; taking a maximum alone must never
// hide a rollback. A latched refusal still cancels the read context and vetoes
// tentative progress even when the saved high-water update itself is valid.
func hashReadPacingSettlementTime(p *hashReadPacer, ctx context.Context, now time.Time) time.Time {
	if p == nil {
		return now
	}
	_, _, _ = p.observeAt(ctx, now, p.elapsed())
	return p.maxWall
}

// Feed existing lifecycle/reservation captures into the same admission guard;
// a newer sampled time must not disappear before a subsequent paced check.
func hashReadPacingClock(p *hashReadPacer, ctx context.Context, now time.Time) time.Time {
	if p != nil {
		_, _, _ = p.observeAt(ctx, now, p.elapsed())
	}
	return now
}

func hashReadPacingRefusalState(err error) (string, string) {
	if errors.Is(err, ErrHashReadExpired) || errors.Is(err, ErrHashReadClockRollback) || errors.Is(err, ErrHashReadReservationDay) {
		return "refused", hashReadPacingCode(err)
	}
	return "canceled", "request_canceled"
}
