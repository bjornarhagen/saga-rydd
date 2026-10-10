package worker

import (
	"context"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// One owning-loop continuation retains the original unmarked CPU prefix and,
// after reservation, the exact charged receipt context. The producer has none
// of these values and cannot close, renew or transfer them.
type inventoryStateTurn struct {
	scope                         *inventoryStateRunScope
	intent                        *inventoryStateIntent
	request                       inventoryStateRequest
	ticket                        *inventoryStateTicket
	window                        cpuWindow
	windowWall                    time.Time
	prefixOwned                   bool
	turnCtx                       context.Context
	cancel                        context.CancelFunc
	receipt                       *inventoryStateReceipt
	reservedWall, reservedElapsed time.Time
	plan                          fairInventoryPlan
	sourceScreened                bool
	sourceRevoked                 bool
}

func newInventoryStateTurn(scope *inventoryStateRunScope, window cpuWindow, wall time.Time) (*inventoryStateTurn, error) {
	if scope == nil || scope.coordinator == nil {
		return nil, errInventoryStateAsyncInput
	}
	intent, err := scope.coordinator.newIntent(scope)
	if err != nil {
		return nil, err
	}
	return &inventoryStateTurn{scope: scope, intent: intent, request: inventoryStateRequest{scope: scope, intent: intent, phase: "before_dispatch"}, window: window, windowWall: wall, prefixOwned: true}, nil
}

func (p *inventoryStateTurn) abandon(finish func(cpuWindow, *state.CPUWindowMarker) error) {
	if p == nil {
		return
	}
	p.scope.coordinator.abandon(p.scope, p.intent)
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	// The Run closure returns nil for an unmarked prefix before any SQL or
	// settlement hook. Durable session/marked-window failures retain their own
	// refusal/error path; abandonment cannot discard a publication failure.
	if p.prefixOwned {
		p.prefixOwned = false
		_ = finish(p.window, nil)
	}
}

func (p *inventoryStateTurn) transfer() (context.Context, context.CancelFunc) {
	// Abandon the sample authority before the admitted writer interval. The
	// charged receipt itself is verified/consumed by ClaimFairInventoryTurn.
	p.scope.coordinator.abandon(p.scope, p.intent)
	p.prefixOwned = false
	ctx, cancel := p.turnCtx, p.cancel
	p.cancel = nil // The admitted writer unit now owns the original cancellation.
	return ctx, cancel
}

func (p *inventoryStateTurn) startPost(ctx context.Context, cancel context.CancelFunc, budget state.DispatchBudget, wall, elapsed time.Time, plan fairInventoryPlan, screened bool) (inventoryStateAsyncDecision, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		cancel()
		return inventoryStateAsyncDecision{}, errInventoryStateAsyncInput
	}
	receipt, err := p.scope.coordinator.bindReceipt(p.intent, budget, wall, elapsed, deadline)
	if err != nil {
		cancel()
		return inventoryStateAsyncDecision{}, err
	}
	p.turnCtx, p.cancel, p.receipt = ctx, cancel, receipt
	p.reservedWall, p.reservedElapsed, p.plan, p.sourceScreened = wall, elapsed, plan, screened
	p.request = inventoryStateRequest{scope: p.scope, intent: p.intent, phase: "after_receipt", receipt: receipt}
	d := p.scope.coordinator.startEligible(ctx, p.request)
	p.ticket = d.Ticket
	return d, nil
}

// This read does not sample clocks or metadata and never renews any anchor.
// It supplies only scheduling predicates for source-disabled planning. A real
// eligible ask/consumption performs the coordinator's serialized clock fence.
func inventoryStateScheduling(scope *inventoryStateRunScope, wall, elapsed time.Time) (time.Duration, bool) {
	if scope == nil {
		return 0, false
	}
	c := scope.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	c.domain.mu.Lock()
	defer c.domain.mu.Unlock()
	wait := max(time.Duration(0), c.retry.wall.Sub(wall), c.retry.elapsed.Sub(elapsed), c.domain.high.wall.Sub(wall), c.domain.high.elapsed.Sub(elapsed))
	occupied := false
	if c.ticket != nil {
		_, returned := c.ticket.completed()
		occupied = !returned
	}
	return wait, occupied
}

func (p *inventoryStateTurn) expiryWait(wall, elapsed time.Time) (time.Duration, bool) {
	if p == nil || p.ticket == nil {
		return 0, false
	}
	c := p.scope.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, complete := p.ticket.completed(); complete {
		return 0, true
	}
	if p.ticket.refusal != "" {
		return 0, false
	}
	remaining := min(time.Until(p.ticket.deadline), p.ticket.launch.wall.Add(inventoryStateSampleWindow).Sub(wall), p.ticket.launch.elapsed.Add(inventoryStateSampleWindow).Sub(elapsed))
	if p.receipt != nil {
		remaining = min(remaining, time.Until(p.receipt.deadline), p.reservedWall.Add(state.FairInventoryClaimWindow).Sub(wall), p.reservedElapsed.Add(state.FairInventoryClaimWindow).Sub(elapsed))
	}
	return max(time.Duration(0), remaining), true
}

func inventoryStateMaintenanceResult(r inventoryStateCompletion) bool {
	switch r.Reason {
	case "inventory_state_limit", "inventory_state_unavailable", "inventory_state_changed", "inventory_state_overflow", "inventory_state_invalid":
		return true
	default:
		return false
	}
}

func inventoryStateClocksCurrent(scope *inventoryStateRunScope, wall, elapsed time.Time) bool {
	if scope == nil {
		return false
	}
	d := scope.domain
	d.mu.Lock()
	defer d.mu.Unlock()
	return validInventoryStateTime(wall) && validInventoryStateTime(elapsed) && !wall.Before(d.high.wall) && !elapsed.Before(d.high.elapsed)
}
