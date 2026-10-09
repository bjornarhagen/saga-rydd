package worker

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// APIPacingSnapshot is process-local spacing evidence for the experimental
// source scanner. It is not a syscall, byte, physical-I/O or global quota.
type APIPacingSnapshot struct {
	RatePerSecond int    `json:"rate_per_second"`
	CapacityReady bool   `json:"capacity_ready"`
	WaitNS        uint64 `json:"wait_ns"`
	Waiting       bool   `json:"waiting"`
}

type sourceAPIPacer struct {
	rate                      int
	spacing, work, entrySpace time.Duration
	bounds                    inventory.APIPacingBounds
	wallNow, elapsedNow       func() time.Time
	mu                        sync.Mutex
	lastWall, lastElapsed     time.Time
	highWall, highElapsed     time.Time
	waitNS                    atomic.Uint64
	waiting                   atomic.Bool
	ready                     atomic.Bool
	profileBlocked            atomic.Bool
}

func newSourceAPIPacer(rate int, work time.Duration, entryRate int, bounds inventory.APIPacingBounds, wall, elapsed func() time.Time) (*sourceAPIPacer, error) {
	if rate < 0 || rate > 100000 || work <= 0 || entryRate < 1 || entryRate > 100000 {
		return nil, inventory.ErrAPIPacingInput
	}
	if rate == 0 {
		return nil, nil
	}
	if bounds.Startup < 1 || bounds.Startup > inventory.MaxAPIAttemptAllowance || bounds.MinimumNext < 1 || bounds.MinimumNext > inventory.MaxAPIAttemptAllowance || bounds.FinalValidation < 1 || bounds.FinalValidation > inventory.MaxAPIAttemptAllowance || bounds.Entry < 1 || bounds.Entry > inventory.MaxAPIAttemptAllowance || wall == nil || elapsed == nil {
		return nil, inventory.ErrAPIPacingInput
	}
	p := &sourceAPIPacer{rate: rate, work: work, spacing: (time.Second + time.Duration(rate) - 1) / time.Duration(rate), entrySpace: (time.Second + time.Duration(entryRate) - 1) / time.Duration(entryRate), bounds: bounds, wallNow: wall, elapsedNow: elapsed}
	p.capacity(true)
	return p, nil
}

// capacity is pure admission planning. No source observation or pacing credit
// is created. Reserve half the work window for revalidation and publication.
func (p *sourceAPIPacer) capacity(construction bool) bool {
	if p == nil {
		return true
	}
	count := p.bounds.MinimumNext
	if construction {
		count += p.bounds.Startup
	}
	ready := !p.profileBlocked.Load() && count <= int64((p.work/2-p.entrySpace)/p.spacing) && p.work/2 >= p.entrySpace
	p.ready.Store(ready)
	return ready
}

func (p *sourceAPIPacer) snapshot() *APIPacingSnapshot {
	if p == nil {
		return nil
	}
	return &APIPacingSnapshot{RatePerSecond: p.rate, CapacityReady: p.ready.Load(), WaitNS: p.waitNS.Load(), Waiting: p.waiting.Load()}
}

func (p *sourceAPIPacer) blockProfile() { p.profileBlocked.Store(true); p.ready.Store(false) }

func (p *sourceAPIPacer) reason() string {
	if p == nil {
		return ""
	}
	if p.profileBlocked.Load() {
		return "api_profile_blocked"
	}
	return "api_capacity_blocked"
}

func (p *sourceAPIPacer) blockedReason(ready bool) string {
	if ready {
		return ""
	}
	return p.reason()
}

func (p *sourceAPIPacer) delay(wall, elapsed time.Time) (time.Duration, error) {
	wall = wall.Round(0).UTC()
	rollback := wall.Before(p.highWall) || elapsed.Before(p.highElapsed)
	if wall.After(p.highWall) {
		p.highWall = wall
	}
	if elapsed.After(p.highElapsed) {
		p.highElapsed = elapsed
	}
	if rollback {
		return 0, errMetadataRollback
	}
	if p.lastWall.IsZero() {
		return 0, nil
	}
	return max(time.Duration(0), p.lastWall.Add(p.spacing).Sub(wall), p.lastElapsed.Add(p.spacing).Sub(elapsed)), nil
}

func (p *sourceAPIPacer) addWait(wait time.Duration) {
	if wait <= 0 {
		return
	}
	for {
		before := p.waitNS.Load()
		after := before + uint64(wait)
		if uint64(wait) > math.MaxUint64-before {
			after = math.MaxUint64
		}
		if p.waitNS.CompareAndSwap(before, after) {
			return
		}
	}
}

// Retain every captured pair in the lifetime pacer, including a window denial.
// The window still records its durable wall evidence and owns error priority.
// Callers hold p.mu; neither check admits an API.
func (p *sourceAPIPacer) fence(window *metadataWindow, ctx context.Context, wall, elapsed time.Time) (time.Duration, time.Duration, error) {
	delay, paceErr := p.delay(wall, elapsed)
	remaining, windowErr := window.fenceAt(ctx, wall, elapsed)
	if windowErr != nil {
		return 0, 0, windowErr
	}
	return remaining, delay, paceErr
}

func (p *sourceAPIPacer) permit(window *metadataWindow) inventory.APIPermit {
	return func(ctx context.Context, kind inventory.APICallKind) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		for {
			wall, elapsed := p.wallNow().Round(0).UTC(), p.elapsedNow()
			remaining, delay, err := p.fence(window, ctx, wall, elapsed)
			if err != nil {
				return err
			}
			if delay == 0 {
				if err := window.permitAt(ctx, kind, wall, elapsed); err != nil {
					return err
				}
				p.lastWall, p.lastElapsed = wall, elapsed
				return nil
			}
			started := time.Now()
			p.waiting.Store(true)
			timer := time.NewTimer(min(delay, remaining))
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
			p.addWait(time.Since(started))
			p.waiting.Store(false)
		}
	}
}

func (p *sourceAPIPacer) entryCapacity(window *metadataWindow) inventory.APIEntryCapacity {
	return func(ctx context.Context, calls int64) (bool, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		wall, elapsed := p.wallNow().Round(0).UTC(), p.elapsedNow()
		remaining, delay, err := p.fence(window, ctx, wall, elapsed)
		if err != nil {
			return false, err
		}
		if calls < 1 || calls > inventory.MaxAPIAttemptAllowance {
			return false, inventory.ErrAPIPacingInput
		}
		return delay <= remaining && calls <= int64((remaining-delay)/p.spacing), nil
	}
}
