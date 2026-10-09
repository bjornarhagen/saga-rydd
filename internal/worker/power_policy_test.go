package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
)

type powerFixtureClock struct {
	mu            sync.Mutex
	wall, elapsed time.Time
}

func newPowerFixtureClock() *powerFixtureClock {
	start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	return &powerFixtureClock{wall: start, elapsed: start}
}
func (c *powerFixtureClock) wallNow() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.wall }
func (c *powerFixtureClock) elapsedNow() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.elapsed
}
func (c *powerFixtureClock) advance(wall, elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = c.wall.Add(wall)
	c.elapsed = c.elapsed.Add(elapsed)
}
func (c *powerFixtureClock) set(wall, elapsed time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall, c.elapsed = wall, elapsed
}

func fixturePowerObservation(clock *powerFixtureClock, witness bool) powerinfo.Observation {
	stamp := clock.wallNow()
	o := powerinfo.Observation{Contract: powerinfo.Contract, Platform: "linux", Profile: powerinfo.LinuxProfile, Status: "unknown", Reason: "no_system_battery_discharge_witness", StartedAt: stamp, FinishedAt: stamp, SequentialObservations: true, CoverageComplete: true}
	if witness {
		value, entries := true, 1
		o.SystemBatteryDischargingObserved = &value
		o.SupplyEntriesObserved = &entries
		o.ProvidersProcessed = 1
		o.ConfirmedSystemBatteries = 1
		o.DischargingSystemBatteries = 1
		o.Status = "system_battery_discharging_observed"
	}
	return o
}

func waitPowerTicket(t *testing.T, ticket *powerTicket) powerCompletion {
	t.Helper()
	if ticket == nil {
		t.Fatal("missing ticket")
	}
	select {
	case <-ticket.done:
	case <-time.After(time.Second):
		t.Fatal("power callback did not finish")
	}
	result, ready := ticket.completed()
	if !ready {
		t.Fatal("closed ticket missing result")
	}
	return result
}

func waitPowerEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("power fixture callback not entered")
	}
}

func releasePowerFixture(t *testing.T, ticket *powerTicket, release chan struct{}) func() {
	t.Helper()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); waitPowerTicket(t, ticket) })
	return unblock
}

func TestPowerCoordinatorOneTicketMultipleReadersAndClones(t *testing.T) {
	clock := newPowerFixtureClock()
	var calls atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		calls.Add(1)
		return fixturePowerObservation(clock, true), nil
	}, clock.wallNow, clock.elapsedNow)
	first := c.sourcePolicy(context.Background())
	if !first.Started || !first.Sampling || !first.SourceBackoff || first.Wait != 5*time.Second || first.NextSampleWait != 5*time.Minute {
		t.Fatalf("initial admission: %+v", first)
	}
	result := waitPowerTicket(t, first.Ticket)
	if result.Err != nil || result.Observation == nil {
		t.Fatalf("first result: %+v", result)
	}
	var readers sync.WaitGroup
	for i := 0; i < 32; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			clone, ready := first.Ticket.completed()
			if !ready || clone.ID != first.Ticket.ID || clone.Observation == nil {
				t.Error("ticket result stolen")
				return
			}
			*clone.Observation.SystemBatteryDischargingObserved = false
			*clone.Observation.SupplyEntriesObserved = 99
		}()
	}
	readers.Wait()
	again, _ := first.Ticket.completed()
	if !*again.Observation.SystemBatteryDischargingObserved || *again.Observation.SupplyEntriesObserved != 1 || calls.Load() != 1 {
		t.Fatalf("mutable clone/stolen reply: %+v calls=%d", again, calls.Load())
	}
	clock.advance(time.Second, time.Second)
	decision := c.sourcePolicy(context.Background())
	if decision.Started || decision.Sampling || !decision.SourceBackoff || decision.Status != "system_battery_discharging_observed" || decision.Wait != powerSampleInterval-time.Second || decision.Ticket != first.Ticket {
		t.Fatalf("positive decision: %+v", decision)
	}
	*decision.Observation.SystemBatteryDischargingObserved = false
	if next := c.sourcePolicy(context.Background()); next.Observation == nil || !*next.Observation.SystemBatteryDischargingObserved {
		t.Fatal("decision clone changed retained evidence")
	}
}

func TestPowerCoordinatorUnknownUsesFixedPacingAndNoUnrequestedProbe(t *testing.T) {
	for _, mode := range []string{"unknown", "unsupported", "error"} {
		t.Run(mode, func(t *testing.T) {
			clock := newPowerFixtureClock()
			var calls atomic.Int64
			c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				calls.Add(1)
				o := fixturePowerObservation(clock, false)
				if mode == "unsupported" {
					o.Platform = "darwin"
					o.Profile = "unsupported"
					o.Reason = "unsupported_platform"
				}
				if mode == "error" {
					return o, errors.New("private provider detail")
				}
				return o, nil
			}, clock.wallNow, clock.elapsedNow)
			if calls.Load() != 0 || c.ticket != nil {
				t.Fatal("constructor probed without due source")
			}
			first := c.sourcePolicy(context.Background())
			waitPowerTicket(t, first.Ticket)
			for i := 0; i < 20; i++ {
				clock.advance(time.Second, time.Second)
				d := c.sourcePolicy(context.Background())
				if d.Started || d.SourceBackoff || d.Wait != 0 || d.Status != "unknown" {
					t.Fatalf("unknown changed source pacing: %+v", d)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("unknown refilled before minimum interval: %d", calls.Load())
			}
			// Reading the cached ticket alone neither probes nor changes anchors.
			wall, elapsed := c.lastWall, c.lastElapsed
			for i := 0; i < 20; i++ {
				first.Ticket.completed()
			}
			if !c.lastWall.Equal(wall) || !c.lastElapsed.Equal(elapsed) || calls.Load() != 1 {
				t.Fatal("cached reads reanchored/probed")
			}
		})
	}
}

func TestPowerCoordinatorIndependentStartIntervalsAndFreshness(t *testing.T) {
	clock := newPowerFixtureClock()
	start := clock.wallNow()
	var calls atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		calls.Add(1)
		return fixturePowerObservation(clock, true), nil
	}, clock.wallNow, clock.elapsedNow)
	first := c.sourcePolicy(context.Background())
	waitPowerTicket(t, first.Ticket)
	clock.set(start.Add(5*time.Minute), start.Add(4*time.Minute))
	d := c.sourcePolicy(context.Background())
	if d.Started || d.SourceBackoff || d.Observation != nil || d.Status != "unknown" || d.NextSampleWait != time.Minute || calls.Load() != 1 {
		t.Fatalf("forward wall jump bypassed elapsed or kept witness: %+v", d)
	}
	clock.set(start.Add(time.Minute), start.Add(4*time.Minute))
	d = c.sourcePolicy(context.Background())
	if d.Started || d.SourceBackoff || d.Reason != "power_clock_rollback" || d.NextSampleWait != 4*time.Minute || calls.Load() != 1 {
		t.Fatalf("rollback restored expired witness/credits: %+v", d)
	}
	clock.set(start.Add(5*time.Minute), start.Add(5*time.Minute))
	d = c.sourcePolicy(context.Background())
	if !d.Started || d.Ticket == first.Ticket || d.Ticket.ID == first.Ticket.ID {
		t.Fatalf("both intervals did not admit fresh ticket: %+v", d)
	}
	waitPowerTicket(t, d.Ticket)
	if calls.Load() != 2 {
		t.Fatalf("unexpected starts: %d", calls.Load())
	}
	// The old ticket remains immutable and readable after slot reuse.
	old, _ := first.Ticket.completed()
	if !old.LaunchWall.Equal(start) || !old.CompletedWall.Equal(start) {
		t.Fatalf("old ticket reanchored: %+v", old)
	}
}

func TestPowerCoordinatorCompletionAgesNeverReanchor(t *testing.T) {
	clock := newPowerFixtureClock()
	start := clock.wallNow()
	entered, release := make(chan struct{}), make(chan struct{})
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		o := fixturePowerObservation(clock, true)
		close(entered)
		<-release
		clock.advance(4*time.Second, 4*time.Second)
		o.FinishedAt = clock.wallNow()
		return o, nil
	}, clock.wallNow, clock.elapsedNow)
	d := c.sourcePolicy(context.Background())
	unblock := releasePowerFixture(t, d.Ticket, release)
	waitPowerEntered(t, entered)
	unblock()
	result := waitPowerTicket(t, d.Ticket)
	if result.Err != nil || !result.CompletedWall.Equal(start.Add(4*time.Second)) {
		t.Fatalf("terminal capture: %+v", result)
	}
	// Completion is still younger than five minutes, but launch is exactly
	// five minutes old. Separate elapsed admission remains blocked.
	clock.set(start.Add(5*time.Minute), start.Add(4*time.Minute))
	decision := c.sourcePolicy(context.Background())
	if decision.Observation != nil || decision.SourceBackoff || decision.Started || decision.NextSampleWait != time.Minute {
		t.Fatalf("completion reanchored launch freshness: %+v", decision)
	}
	clock.set(start.Add(3*time.Second), start.Add(3*time.Second))
	decision = c.sourcePolicy(context.Background())
	if decision.SourceBackoff || decision.Observation != nil || decision.Reason != "power_clock_rollback" {
		t.Fatalf("negative completion age retained witness: %+v", decision)
	}
}

func TestPowerCoordinatorBlockedCallbackSurvivesCancelAndRunRestart(t *testing.T) {
	clock := newPowerFixtureClock()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls, active, maxActive atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		n := active.Add(1)
		for {
			old := maxActive.Load()
			if old >= n || maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return fixturePowerObservation(clock, true), nil
	}, clock.wallNow, clock.elapsedNow)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := c.sourcePolicy(ctx)
	unblock := releasePowerFixture(t, first.Ticket, release)
	waitPowerEntered(t, entered)
	cancel()
	// A new Run's lifetime context cannot replace the old occupied slot.
	for _, advance := range []time.Duration{time.Second, 6 * time.Minute} {
		clock.advance(advance, advance)
		d := c.sourcePolicy(context.Background())
		if d.Started || d.SourceBackoff || !d.Sampling || d.Ticket != first.Ticket || d.Wait != 0 || calls.Load() != 1 {
			t.Fatalf("canceled blocked callback replaced/waited: %+v", d)
		}
	}
	select {
	case <-first.Ticket.done:
		t.Fatal("slot freed before actual callback return")
	default:
	}
	unblock()
	result := waitPowerTicket(t, first.Ticket)
	if result.Observation != nil || !errors.Is(result.Err, context.Canceled) || active.Load() != 0 {
		t.Fatalf("late canceled callback published/leaked: %+v active=%d", result, active.Load())
	}
	next := c.sourcePolicy(context.Background())
	if !next.Started || next.Ticket == first.Ticket {
		t.Fatalf("returned slot not reusable: %+v", next)
	}
	waitPowerTicket(t, next.Ticket)
	if calls.Load() != 2 || active.Load() != 0 || maxActive.Load() != 1 {
		t.Fatalf("observer concurrency/resource bound: calls=%d active=%d max=%d", calls.Load(), active.Load(), maxActive.Load())
	}
}

func TestPowerCoordinatorAdmissionExpiryAndRollbackNeverReplaceStall(t *testing.T) {
	for _, mode := range []string{"wall", "elapsed", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			clock := newPowerFixtureClock()
			start := clock.wallNow()
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				calls.Add(1)
				close(entered)
				<-release
				return fixturePowerObservation(clock, true), nil
			}, clock.wallNow, clock.elapsedNow)
			first := c.sourcePolicy(context.Background())
			unblock := releasePowerFixture(t, first.Ticket, release)
			waitPowerEntered(t, entered)
			switch mode {
			case "wall":
				clock.advance(5*time.Second, 0)
			case "elapsed":
				clock.advance(0, 5*time.Second)
			case "rollback":
				clock.advance(-time.Second, 0)
			}
			d := c.sourcePolicy(context.Background())
			if d.Started || d.SourceBackoff || !d.Sampling || d.Wait != 0 || d.Ticket != first.Ticket || calls.Load() != 1 {
				t.Fatalf("expired/rollback slot not unknown: %+v", d)
			}
			clock.set(start, start) // Restoring clocks cannot renew the ticket.
			d = c.sourcePolicy(context.Background())
			if d.SourceBackoff || d.Started || d.Wait != 0 {
				t.Fatalf("restoration renewed canceled ticket: %+v", d)
			}
			unblock()
			result := waitPowerTicket(t, first.Ticket)
			if result.Observation != nil || result.Err == nil {
				t.Fatalf("invalidated completion yielded witness: %+v", result)
			}
		})
	}
}

func TestPowerCoordinatorQueuedTimeCountsBeforeObserverInvocation(t *testing.T) {
	for _, mode := range []string{"wall", "elapsed", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			clock := newPowerFixtureClock()
			var calls atomic.Int64
			var queued func()
			c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				calls.Add(1)
				return fixturePowerObservation(clock, true), nil
			}, clock.wallNow, clock.elapsedNow)
			c.launch = func(run func()) { queued = run }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := c.sourcePolicy(ctx)
			if queued == nil || !first.Started || calls.Load() != 0 {
				t.Fatal("launch seam failed")
			}
			switch mode {
			case "wall":
				clock.advance(5*time.Second, 0)
			case "elapsed":
				clock.advance(0, 5*time.Second)
			case "cancel":
				cancel()
			}
			queued()
			result := waitPowerTicket(t, first.Ticket)
			if calls.Load() != 0 || result.Observation != nil || result.Err == nil {
				t.Fatalf("queued window renewed at callback: %+v calls=%d", result, calls.Load())
			}
		})
	}
}

func TestPowerCoordinatorLateAndInvalidResultsNeverBackoff(t *testing.T) {
	for _, mode := range []string{"late_wall", "late_elapsed", "rollback", "false_witness", "future_finish", "wrong_contract"} {
		t.Run(mode, func(t *testing.T) {
			clock := newPowerFixtureClock()
			c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
				o := fixturePowerObservation(clock, true)
				switch mode {
				case "late_wall":
					clock.advance(5*time.Second, 0)
				case "late_elapsed":
					clock.advance(0, 5*time.Second)
				case "rollback":
					clock.advance(-time.Second, 0)
				case "false_witness":
					*o.SystemBatteryDischargingObserved = false
				case "future_finish":
					o.FinishedAt = o.FinishedAt.Add(time.Second)
				case "wrong_contract":
					o.Contract = "foreign"
				}
				return o, nil
			}, clock.wallNow, clock.elapsedNow)
			first := c.sourcePolicy(context.Background())
			result := waitPowerTicket(t, first.Ticket)
			if result.Observation != nil || result.Err == nil {
				t.Fatalf("invalid completion retained evidence: %+v", result)
			}
			d := c.sourcePolicy(context.Background())
			if d.SourceBackoff || d.Observation != nil || d.Started {
				t.Fatalf("invalid completion influenced source policy: %+v", d)
			}
		})
	}
}

func TestPowerCoordinatorCanceledAskDoesNotLaunch(t *testing.T) {
	var calls atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		calls.Add(1)
		return powerinfo.Observation{}, nil
	}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{nil, ctx} {
		d := c.sourcePolicy(ctx)
		if d.Started || d.Ticket != nil || calls.Load() != 0 {
			t.Fatalf("canceled ask launched: %+v", d)
		}
	}
}

func TestPowerCoordinatorLaterClockSampleDoesNotInvalidateFrozenCompletion(t *testing.T) {
	clock := newPowerFixtureClock()
	start := clock.wallNow()
	captured, release := make(chan struct{}), make(chan struct{})
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) { return fixturePowerObservation(clock, true), nil }, clock.wallNow, clock.elapsedNow)
	c.afterCompletionCapture = func() { close(captured); <-release }
	first := c.sourcePolicy(context.Background())
	unblock := releasePowerFixture(t, first.Ticket, release)
	waitPowerEntered(t, captured)
	clock.advance(time.Second, time.Second)
	d := c.sourcePolicy(context.Background())
	if d.Started || !d.Sampling || !d.SourceBackoff {
		t.Fatalf("pending publication state: %+v", d)
	}
	unblock()
	result := waitPowerTicket(t, first.Ticket)
	if result.Err != nil || result.Observation == nil || !result.CompletedWall.Equal(start) || !result.CompletedElapsed.Equal(start) {
		t.Fatalf("newer clock sample mislabeled captured completion: %+v", result)
	}
	d = c.sourcePolicy(context.Background())
	if d.Observation == nil || !d.SourceBackoff || d.Status != "system_battery_discharging_observed" {
		t.Fatalf("normal chronology lost witness: %+v", d)
	}
}

func TestPowerCoordinatorExactCancellationCannotAffectAnotherTicket(t *testing.T) {
	clock := newPowerFixtureClock()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return fixturePowerObservation(clock, true), nil
	}, clock.wallNow, clock.elapsedNow)
	first := c.sourcePolicy(context.Background())
	waitPowerTicket(t, first.Ticket)
	if c.cancelTicket(first.Ticket) {
		t.Fatal("terminal ticket was mutable")
	}
	clock.advance(powerSampleInterval, powerSampleInterval)
	next := c.sourcePolicy(context.Background())
	unblock := releasePowerFixture(t, next.Ticket, release)
	waitPowerEntered(t, entered)
	forged := &powerTicket{ID: next.Ticket.ID}
	if c.cancelTicket(first.Ticket) || c.cancelTicket(forged) || context.Cause(next.Ticket.ctx) != nil {
		t.Fatal("old/forged ticket canceled current slot")
	}
	if !c.cancelTicket(next.Ticket) || !errors.Is(context.Cause(next.Ticket.ctx), context.Canceled) {
		t.Fatal("exact occupied ticket cancellation refused")
	}
	select {
	case <-next.Ticket.done:
		t.Fatal("cancellation freed occupied slot")
	default:
	}
	unblock()
	result := waitPowerTicket(t, next.Ticket)
	if result.Observation != nil || !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("canceled ticket published evidence: %+v", result)
	}
}

func TestPowerCoordinatorChargedRecheckNeverStartsAnotherProbe(t *testing.T) {
	clock := newPowerFixtureClock()
	var calls atomic.Int64
	c := newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
		calls.Add(1)
		return fixturePowerObservation(clock, false), nil
	}, clock.wallNow, clock.elapsedNow)
	before := c.recheckSourcePolicy(context.Background())
	if before.Started || before.Ticket != nil || !before.SourceBackoff || before.Wait != 0 || before.Status != "sample_required" || calls.Load() != 0 || !c.lastWall.IsZero() {
		t.Fatalf("unscreened recheck started/allowed: %+v", before)
	}
	first := c.sourcePolicy(context.Background())
	waitPowerTicket(t, first.Ticket)
	clock.advance(time.Second, time.Second)
	within := c.recheckSourcePolicy(context.Background())
	if within.Started || within.SourceBackoff || within.Wait != 0 || within.Status != "unknown" || within.Ticket != first.Ticket || calls.Load() != 1 {
		t.Fatalf("known unknown policy not reused: %+v", within)
	}
	clock.advance(powerSampleInterval, powerSampleInterval)
	for i := 0; i < 3; i++ {
		d := c.recheckSourcePolicy(context.Background())
		if d.Started || !d.SourceBackoff || d.Wait != 0 || d.Status != "sample_required" || d.Observation != nil || d.Ticket != first.Ticket || calls.Load() != 1 {
			t.Fatalf("charged recheck launched/allowed stale policy: %+v", d)
		}
	}
	next := c.sourcePolicy(context.Background())
	if !next.Started || next.Ticket == first.Ticket {
		t.Fatalf("fresh uncharged pass did not sample: %+v", next)
	}
	waitPowerTicket(t, next.Ticket)
	if calls.Load() != 2 {
		t.Fatalf("unexpected callback count: %d", calls.Load())
	}
}
