package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestInventoryStateTurnPrefixCancelAndTransferAreOnceOnly(t *testing.T) {
	for _, transfer := range []bool{false, true} {
		t.Run(map[bool]string{false: "abandon", true: "transfer"}[transfer], func(t *testing.T) {
			clock := newInventoryStateAsyncClock()
			c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
			scope := asyncInventoryScope(t, c, asyncInventorySampler(t), nil)
			turn, err := newInventoryStateTurn(scope, cpuWindow{}, clock.wallNow())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			turn.turnCtx, turn.cancel = ctx, cancel
			var finishes int
			finish := func(cpuWindow, *state.CPUWindowMarker) error { finishes++; return nil }
			if transfer {
				inherited, release := turn.transfer()
				if inherited != ctx || ctx.Err() != nil || release == nil {
					t.Fatal("transfer renewed or canceled the original context")
				}
				release()
			}
			turn.abandon(finish)
			turn.abandon(finish)
			want := 1
			if transfer {
				want = 0
			}
			if finishes != want || ctx.Err() != context.Canceled || !turn.intent.abandoned || turn.prefixOwned {
				t.Fatal(finishes, ctx.Err(), turn)
			}
		})
	}
}

func TestInventoryStateTurnOriginalReceiptAndEarliestExpiry(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		return asyncInventoryReport(clock, limit), nil
	})
	turn, err := newInventoryStateTurn(scope, cpuWindow{}, clock.wallNow())
	if err != nil {
		t.Fatal(err)
	}
	d := c.startEligible(context.Background(), turn.request)
	turn.ticket = d.Ticket
	waitAsyncInventory(t, d.Ticket)
	if _, ok := c.takeCompletion(context.Background(), turn.request, d.Ticket); !ok {
		t.Fatal("pre refused")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	original, _ := ctx.Deadline()
	wall, elapsed := clock.wallNow(), clock.elapsedNow()
	d, err = turn.startPost(ctx, cancel, state.DispatchBudget{Day: wall.Format(time.DateOnly), Used: 1}, wall, elapsed, fairInventoryPlan{allowSource: true}, true)
	if err != nil || !d.Started || turn.turnCtx != ctx || !turn.receipt.deadline.Equal(original) {
		t.Fatal(d, err, turn)
	}
	waitAsyncInventory(t, d.Ticket)
	if _, ok := c.takeCompletion(ctx, turn.request, d.Ticket); !ok {
		t.Fatal("post refused")
	}
	if turn.receipt.used != 1 || turn.request.receipt != turn.receipt {
		t.Fatal(turn.receipt)
	}
	// Expiry uses the original elapsed and actual deadline, not a renewed child.
	clock.advance(0, state.FairInventoryClaimWindow)
	if inventoryStateReceiptCurrent(ctx, wall, elapsed, clock.wallNow(), clock.elapsedNow()) {
		t.Fatal("elapsed equality accepted")
	}
	turn.abandon(func(cpuWindow, *state.CPUWindowMarker) error { return nil })
	if !turn.receipt.deadline.Equal(original) {
		t.Fatal("abandon reanchored receipt")
	}
}

func TestInventoryStateTurnMaintenanceRefusalClassesAndCachedClocks(t *testing.T) {
	t.Run("returned_refusal_wakes_without_refund_or_replacement", func(t *testing.T) {
		clock := newInventoryStateAsyncClock()
		c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
		var calls atomic.Int64
		scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			calls.Add(1)
			return fixtureInventoryStateReport(limit, limit, clock.wallNow()), nil
		})
		turn, err := newInventoryStateTurn(scope, cpuWindow{}, clock.wallNow())
		if err != nil {
			t.Fatal(err)
		}
		d := c.startEligible(context.Background(), turn.request)
		if !d.Started || d.Ticket == nil {
			t.Fatal(d)
		}
		turn.ticket = d.Ticket
		waitAsyncInventory(t, d.Ticket)
		result, returned := d.Ticket.completed()
		if !returned || result.Reason != "inventory_state_limit" || result.Usable || calls.Load() != 1 {
			t.Fatal(result, returned, calls.Load())
		}
		c.mu.Lock()
		refusal, retry := d.Ticket.refusal, c.retry
		c.mu.Unlock()
		if refusal != result.Reason || !retry.wall.Equal(result.LaunchWall.Add(inventoryStateRetryInterval)) || !retry.elapsed.Equal(result.LaunchElapsed.Add(inventoryStateRetryInterval)) {
			t.Fatal("producer refusal lost its original retry", refusal, retry, result)
		}
		if delay, wake := turn.expiryWait(clock.wallNow(), clock.elapsedNow()); delay != 0 || !wake {
			t.Fatal("returned refusal lost its owning-loop completion wake", delay, wake)
		}
		if _, allowed := c.takeCompletion(context.Background(), turn.request, d.Ticket); allowed {
			t.Fatal("completion wake admitted a refused SOURCE")
		}
		finishes := 0
		finish := func(cpuWindow, *state.CPUWindowMarker) error { finishes++; return nil }
		turn.abandon(finish)
		turn.abandon(finish)
		next, err := newInventoryStateTurn(scope, cpuWindow{}, clock.wallNow())
		if err != nil {
			t.Fatal(err)
		}
		nextDecision := c.startEligible(context.Background(), next.request)
		if nextDecision.Started || nextDecision.Wait != inventoryStateRetryInterval || calls.Load() != 1 || finishes != 1 {
			t.Fatal("completion consumption replaced or refunded the attempt", nextDecision, calls.Load(), finishes)
		}
		c.mu.Lock()
		unchanged := c.retry == retry
		c.mu.Unlock()
		if !unchanged {
			t.Fatal("consumption moved the frozen retry anchor")
		}
		next.abandon(func(cpuWindow, *state.CPUWindowMarker) error { return nil })
	})
	for _, reason := range []string{"inventory_state_limit", "inventory_state_unavailable", "inventory_state_changed", "inventory_state_invalid", "inventory_state_overflow"} {
		if !inventoryStateMaintenanceResult(inventoryStateCompletion{Reason: reason}) {
			t.Fatal(reason)
		}
	}
	for _, reason := range []string{"", "inventory_state_timeout", "inventory_state_canceled", "inventory_state_clock"} {
		if inventoryStateMaintenanceResult(inventoryStateCompletion{Reason: reason}) {
			t.Fatal(reason)
		}
	}
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), nil)
	wall, elapsed := clock.wallNow(), clock.elapsedNow()
	c.domain.capture()
	reads := clock.reads.Load()
	if !inventoryStateClocksCurrent(scope, wall, elapsed) || inventoryStateClocksCurrent(scope, wall.Add(-time.Nanosecond), elapsed) {
		t.Fatal("clock fence")
	}
	inventoryStateScheduling(scope, wall, elapsed)
	if clock.reads.Load() != reads {
		t.Fatal("cached scheduling sampled clocks")
	}
}
