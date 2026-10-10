package worker

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var errInventoryStateAsyncInput = errors.New("invalid inventory state sample scope")

// One production domain and slot survive successive in-process Run calls.
// Merely injecting an observer or clock must never select a substitute slot.
var processInventoryStateCoordinator = newInventoryStateCoordinator(time.Now, time.Now)

type inventoryStateObserver func(context.Context, state.InventoryStateBudgetSampler, int64) (state.InventoryStateBudget, error)

type inventoryStateClockPair struct{ wall, elapsed time.Time }

// Clock sampling and high-water updates have one serialized chronology.
// The callback never executes while this mutex or the coordinator mutex is held.
type inventoryStateClockDomain struct {
	mu                  sync.Mutex
	wallNow, elapsedNow func() time.Time
	high                inventoryStateClockPair
}

func validInventoryStateTime(t time.Time) bool {
	n := t.UnixNano()
	return n > 0 && time.Unix(0, n).Equal(t)
}

func inventoryStateTimeAdd(t time.Time, d time.Duration) (time.Time, bool) {
	added := t.Add(d)
	return added, validInventoryStateTime(t) && validInventoryStateTime(added) && added.After(t)
}

func (d *inventoryStateClockDomain) capture() (inventoryStateClockPair, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.captureLocked()
}

func (d *inventoryStateClockDomain) captureLocked() (inventoryStateClockPair, string) {
	p := inventoryStateClockPair{wall: d.wallNow().Round(0).UTC(), elapsed: d.elapsedNow()}
	reason := ""
	if !validInventoryStateTime(p.wall) || !validInventoryStateTime(p.elapsed) {
		reason = "inventory_state_clock"
	} else if p.wall.Before(d.high.wall) || p.elapsed.Before(d.high.elapsed) {
		reason = "inventory_state_clock"
	}
	// Even a refused forward observation retains its high-water. The domains
	// remain independent; there is no max or subtraction between them.
	if validInventoryStateTime(p.wall) && p.wall.After(d.high.wall) {
		d.high.wall = p.wall
	}
	if validInventoryStateTime(p.elapsed) && p.elapsed.After(d.high.elapsed) {
		d.high.elapsed = p.elapsed
	}
	return p, reason
}

func (d *inventoryStateClockDomain) remaining(p inventoryStateClockPair) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return max(time.Duration(0), d.high.wall.Sub(p.wall), d.high.elapsed.Sub(p.elapsed))
}

// An explicit private fixture owner substitutes a complete process lifetime.
// Callers retain this same value across restart/store/threshold changes. There
// is no registry, implicit detection from function pointers, or per-Run reset.
type inventoryStateFixtureOwner struct{ coordinator *inventoryStateCoordinator }

func newInventoryStateFixtureOwner(wallNow, elapsedNow func() time.Time) (*inventoryStateFixtureOwner, error) {
	if wallNow == nil || elapsedNow == nil {
		return nil, errInventoryStateAsyncInput
	}
	return &inventoryStateFixtureOwner{coordinator: newInventoryStateCoordinator(wallNow, elapsedNow)}, nil
}

func selectedInventoryStateCoordinator(owner *inventoryStateFixtureOwner) *inventoryStateCoordinator {
	if owner == nil {
		return processInventoryStateCoordinator
	}
	return owner.coordinator
}

type inventoryStateCoordinator struct {
	mu                     sync.Mutex
	domain                 *inventoryStateClockDomain
	ticket                 *inventoryStateTicket
	nextID                 uint64
	retry                  inventoryStateClockPair
	launch                 func(func())
	afterCompletionCapture func() // private generated ordering seam
	beforePendingRefusal   func() // private generated cancel/return ordering seam
}

func newInventoryStateCoordinator(wallNow, elapsedNow func() time.Time) *inventoryStateCoordinator {
	return &inventoryStateCoordinator{domain: &inventoryStateClockDomain{wallNow: wallNow, elapsedNow: elapsedNow}, launch: func(run func()) { go run() }}
}

// The owning loop binds this nonzero-size token to its exact resolved fair
// scope. Keeping the token avoids retaining FairInventoryRoots' Store pointer
// in an observer. A replacement resolution must use a new token and Run scope.
type inventoryStateFairScopeIdentity struct{ value byte }

func newInventoryStateFairScopeIdentity() *inventoryStateFairScopeIdentity {
	return &inventoryStateFairScopeIdentity{value: 1}
}

type inventoryStateRunScope struct {
	coordinator *inventoryStateCoordinator
	domain      *inventoryStateClockDomain
	sampler     state.InventoryStateBudgetSampler
	instance    string
	limit       int64
	fair        *inventoryStateFairScopeIdentity
	observer    inventoryStateObserver
	nextIntent  uint64 // admission fields below are guarded by coordinator.mu
	current     *inventoryStateIntent
}

func newInventoryStateRunScope(c *inventoryStateCoordinator, sampler state.InventoryStateBudgetSampler, instance string, limit int64, fair *inventoryStateFairScopeIdentity, observer inventoryStateObserver) (*inventoryStateRunScope, error) {
	if c == nil || c.domain == nil || !sampler.SameScope(sampler) || len(instance) == 0 || len(instance) > 128 || !utf8.ValidString(instance) || strings.ContainsRune(instance, 0) || limit < state.MinInventoryStateBytes || limit > state.MaxInventoryStateBytes || fair == nil {
		return nil, errInventoryStateAsyncInput
	}
	if observer == nil {
		observer = func(ctx context.Context, sampler state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
			return sampler.Observe(ctx, limit)
		}
	}
	return &inventoryStateRunScope{coordinator: c, domain: c.domain, sampler: sampler, instance: instance, limit: limit, fair: fair, observer: observer}, nil
}

type inventoryStateIntent struct {
	scope       *inventoryStateRunScope
	generation  uint64
	abandoned   bool
	preUsed     bool
	preStarted  bool
	postStarted bool
	receipt     *inventoryStateReceipt
}

type inventoryStateReceipt struct {
	intent   *inventoryStateIntent
	reserved inventoryStateClockPair
	deadline time.Time // original actual claim deadline, never renewed
	day      string
	used     int
}

type inventoryStateRequest struct {
	scope   *inventoryStateRunScope
	intent  *inventoryStateIntent
	phase   string
	receipt *inventoryStateReceipt
}

func (c *inventoryStateCoordinator) newIntent(scope *inventoryStateRunScope) (*inventoryStateIntent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validScope(scope) || scope.nextIntent == math.MaxUint64 {
		return nil, errInventoryStateAsyncInput
	}
	if scope.current != nil {
		c.abandonLocked(scope, scope.current)
	}
	scope.nextIntent++
	intent := &inventoryStateIntent{scope: scope, generation: scope.nextIntent}
	scope.current = intent
	return intent, nil
}

// Bind only a successfully returned exact receipt. This helper does not reserve
// or validate durable state; the owner must retain the original claim context.
func (c *inventoryStateCoordinator) bindReceipt(intent *inventoryStateIntent, budget state.DispatchBudget, wall, elapsed, deadline time.Time) (*inventoryStateReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if intent == nil || !c.validScope(intent.scope) || intent.scope.current != intent || intent.abandoned || !intent.preUsed || intent.receipt != nil || budget.Used < 1 || budget.Used > 100000 || budget.Day != wall.UTC().Format(time.DateOnly) || !validInventoryStateTime(wall) || !validInventoryStateTime(elapsed) || !validInventoryStateTime(deadline) {
		return nil, errInventoryStateAsyncInput
	}
	if _, ok := inventoryStateTimeAdd(wall, state.FairInventoryClaimWindow); !ok {
		return nil, errInventoryStateAsyncInput
	}
	if _, ok := inventoryStateTimeAdd(elapsed, state.FairInventoryClaimWindow); !ok {
		return nil, errInventoryStateAsyncInput
	}
	r := &inventoryStateReceipt{intent: intent, reserved: inventoryStateClockPair{wall.Round(0).UTC(), elapsed}, deadline: deadline, day: budget.Day, used: budget.Used}
	intent.receipt = r
	return r, nil
}

func (c *inventoryStateCoordinator) validScope(scope *inventoryStateRunScope) bool {
	return scope != nil && scope.coordinator == c && scope.domain == c.domain && scope.fair != nil && scope.sampler.SameScope(scope.sampler) && scope.observer != nil && scope.limit >= state.MinInventoryStateBytes && scope.limit <= state.MaxInventoryStateBytes
}

func (c *inventoryStateCoordinator) validRequest(r inventoryStateRequest) bool {
	if !c.validScope(r.scope) || r.intent == nil || r.intent.scope != r.scope || r.intent.generation == 0 || r.scope.current != r.intent || r.intent.abandoned {
		return false
	}
	switch r.phase {
	case "before_dispatch":
		return r.receipt == nil && !r.intent.preUsed
	case "after_receipt":
		return r.intent.preUsed && r.receipt != nil && r.receipt == r.intent.receipt && r.receipt.intent == r.intent
	default:
		return false
	}
}

type inventoryStateTicket struct {
	ID         uint64
	request    inventoryStateRequest
	launch     inventoryStateClockPair
	deadline   time.Time
	parent     context.Context
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	refusal    string // guarded by coordinator.mu; first denial is sticky
	used       bool   // one phase consumption, guarded by coordinator.mu
	completion inventoryStateCompletion
	returned   atomic.Pointer[inventoryStateReturnCapture]
}

type inventoryStateReturnCapture struct {
	completion inventoryStateCompletion
}

type inventoryStateCompletion struct {
	ID                              uint64
	Phase                           string
	LaunchWall, LaunchElapsed       time.Time
	CompletedWall, CompletedElapsed time.Time
	Usable                          bool
	Reason                          string
	Observation                     *state.InventoryStateBudget
}

type inventoryStateAsyncDecision struct {
	Ticket              *inventoryStateTicket
	Status, Reason      string
	SamplingState       string
	CallbackReturned    bool
	ProcessSlotOccupied bool
	Started             bool
	Wait                time.Duration
}

func (t *inventoryStateTicket) completed() (inventoryStateCompletion, bool) {
	if t == nil {
		return inventoryStateCompletion{}, false
	}
	select {
	case <-t.done:
		r := t.completion
		r.Observation = cloneInventoryStateBudget(r.Observation)
		return r, true
	default:
		return inventoryStateCompletion{}, false
	}
}

func inventoryStateContextReason(ctx context.Context) string {
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		return "inventory_state_timeout"
	}
	if context.Cause(ctx) != nil {
		return "inventory_state_canceled"
	}
	return ""
}

func inventoryStateWindowReason(start, current inventoryStateClockPair, window time.Duration) string {
	if current.wall.Before(start.wall) || current.elapsed.Before(start.elapsed) {
		return "inventory_state_clock"
	}
	if current.wall.Sub(start.wall) >= window || current.elapsed.Sub(start.elapsed) >= window {
		return "inventory_state_timeout"
	}
	return ""
}

func (c *inventoryStateCoordinator) windowReason(t *inventoryStateTicket, p inventoryStateClockPair) string {
	if reason := inventoryStateWindowReason(t.launch, p, inventoryStateSampleWindow); reason != "" {
		return reason
	}
	if !time.Now().Before(t.deadline) {
		return "inventory_state_timeout"
	}
	if t.request.receipt != nil {
		if !time.Now().Before(t.request.receipt.deadline) {
			return "inventory_state_timeout"
		}
		return inventoryStateWindowReason(t.request.receipt.reserved, p, state.FairInventoryClaimWindow)
	}
	return ""
}

func (c *inventoryStateCoordinator) recordFailureLocked(t *inventoryStateTicket, reason string) {
	if t.refusal != "" || reason == "" {
		return
	}
	t.refusal = reason
	c.retry.wall, _ = inventoryStateTimeAdd(t.launch.wall, inventoryStateRetryInterval)
	c.retry.elapsed, _ = inventoryStateTimeAdd(t.launch.elapsed, inventoryStateRetryInterval)
	t.cancel()
}

// Serialize the nil-return test and first pending refusal against terminal
// capture. A callback that has returned cannot acquire manufactured failure
// debt while this owner is waiting for the clock guard.
func (c *inventoryStateCoordinator) refuseLocked(t *inventoryStateTicket, reason string) bool {
	if reason == "" {
		return false
	}
	if c.beforePendingRefusal != nil {
		c.beforePendingRefusal()
	}
	c.domain.mu.Lock()
	defer c.domain.mu.Unlock()
	if t.returned.Load() != nil {
		return false
	}
	c.recordFailureLocked(t, reason)
	return true
}

func (c *inventoryStateCoordinator) pendingRefusalLocked(t *inventoryStateTicket, p inventoryStateClockPair, clockReason string) {
	if t.returned.Load() != nil {
		return
	}
	reason := t.refusal
	if reason == "" {
		reason = inventoryStateContextReason(t.ctx)
	}
	if reason == "" {
		reason = clockReason
	}
	if reason == "" {
		reason = c.windowReason(t, p)
	}
	c.refuseLocked(t, reason)
}

func (c *inventoryStateCoordinator) startEligible(parent context.Context, request inventoryStateRequest) inventoryStateAsyncDecision {
	decision := inventoryStateAsyncDecision{Status: "invalid_scope", Reason: "inventory_state_invalid", SamplingState: "refused"}
	if parent == nil {
		return decision
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validRequest(request) {
		return decision
	}
	if reason := inventoryStateContextReason(parent); reason != "" {
		decision.Status, decision.Reason = "canceled", reason
		return decision
	}
	p, clockReason := c.domain.capture()
	if c.ticket != nil {
		c.pendingRefusalLocked(c.ticket, p, clockReason)
		if c.ticket.request == request {
			decision.Ticket = c.ticket
			result, returned := c.ticket.completed()
			decision.CallbackReturned, decision.ProcessSlotOccupied = returned, !returned
			decision.Status, decision.SamplingState = "process_slot_occupied", "pending"
			if returned {
				decision.Status, decision.SamplingState = "returned", "returned"
				decision.Reason = result.Reason
			}
			if captured := c.ticket.returned.Load(); captured != nil && !returned {
				decision.CallbackReturned, decision.SamplingState = true, "returned"
				decision.Reason = captured.completion.Reason
				if decision.Reason != "" {
					decision.SamplingState = "refused"
				}
			}
			if c.ticket.refusal != "" {
				decision.Reason, decision.SamplingState = c.ticket.refusal, "refused"
			}
			decision.Wait = max(c.domain.remaining(p), c.retry.wall.Sub(p.wall), c.retry.elapsed.Sub(p.elapsed))
			return decision
		}
		if _, returned := c.ticket.completed(); !returned {
			decision.Ticket, decision.Status, decision.ProcessSlotOccupied = c.ticket, "process_slot_occupied", true
			decision.Reason, decision.SamplingState = "inventory_state_process_slot", "foreign_slot"
			decision.CallbackReturned = c.ticket.returned.Load() != nil
			if c.ticket.request.scope == request.scope && c.ticket.request.intent == request.intent {
				decision.SamplingState = "pending"
				decision.Reason = c.ticket.refusal
				if decision.Reason != "" {
					decision.SamplingState = "refused"
				}
			}
			return decision
		}
	}
	if request.phase == "before_dispatch" && request.intent.preStarted || request.phase == "after_receipt" && request.intent.postStarted {
		return decision
	}
	decision.Wait = max(c.domain.remaining(p), c.retry.wall.Sub(p.wall), c.retry.elapsed.Sub(p.elapsed))
	if clockReason != "" {
		decision.Status, decision.Reason = "clock_refused", clockReason
		return decision
	}
	if decision.Wait > 0 {
		decision.Status, decision.Reason = "retry_wait", "inventory_state_retry"
		return decision
	}
	if c.nextID == math.MaxUint64 {
		return decision
	}
	if _, ok := inventoryStateTimeAdd(p.wall, inventoryStateRetryInterval); !ok {
		return decision
	}
	if _, ok := inventoryStateTimeAdd(p.elapsed, inventoryStateRetryInterval); !ok {
		return decision
	}
	if request.receipt != nil {
		if !time.Now().Before(request.receipt.deadline) || inventoryStateWindowReason(request.receipt.reserved, p, state.FairInventoryClaimWindow) != "" {
			decision.Status, decision.Reason = "canceled", "inventory_state_timeout"
			return decision
		}
	}
	ctx, cancel := context.WithTimeout(parent, inventoryStateSampleWindow)
	deadline, _ := ctx.Deadline()
	c.nextID++
	t := &inventoryStateTicket{ID: c.nextID, request: request, launch: p, deadline: deadline, parent: parent, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	if request.phase == "before_dispatch" {
		request.intent.preStarted = true
	} else {
		request.intent.postStarted = true
	}
	c.ticket, c.retry = t, inventoryStateClockPair{}
	decision.Ticket, decision.Started, decision.ProcessSlotOccupied = t, true, true
	decision.Status, decision.Reason, decision.SamplingState = "started", "", "pending"
	c.launch(func() { c.observe(t) })
	return decision
}

func (c *inventoryStateCoordinator) observe(t *inventoryStateTicket) {
	var report state.InventoryStateBudget
	callbackReason := ""
	c.mu.Lock()
	p, clockReason := c.domain.capture()
	c.pendingRefusalLocked(t, p, clockReason)
	callbackReason = t.refusal
	c.mu.Unlock()
	if callbackReason == "" {
		// A panic supplies no report and never escapes as raw callback text.
		func() {
			defer func() {
				if recover() != nil {
					callbackReason = "inventory_state_unavailable"
				}
			}()
			var err error
			report, err = t.request.scope.observer(t.ctx, t.request.scope.sampler, t.request.scope.limit)
			switch {
			case errors.Is(err, state.ErrInventoryStateBudgetClock):
				callbackReason = "inventory_state_clock"
			case errors.Is(err, context.DeadlineExceeded):
				callbackReason = "inventory_state_timeout"
			case errors.Is(err, context.Canceled):
				callbackReason = "inventory_state_canceled"
			case err != nil:
				callbackReason = "inventory_state_unavailable"
			}
		}()
	}
	// Freeze both actual-return chronology and the verdict before releasing the
	// clock guard. Later owner clocks/cancellation cannot invent a failed call
	// while this producer is waiting to publish done. Late consumers still veto.
	c.domain.mu.Lock()
	finished, finishClockReason := c.domain.captureLocked()
	reason := ""
	for _, candidate := range []string{inventoryStateContextReason(t.ctx), finishClockReason, c.windowReason(t, finished), callbackReason} {
		if reason == "" {
			reason = candidate
		}
	}
	result := inventoryStateCompletion{ID: t.ID, Phase: t.request.phase, LaunchWall: t.launch.wall, LaunchElapsed: t.launch.elapsed, CompletedWall: finished.wall, CompletedElapsed: finished.elapsed}
	if reason == "" {
		if !validInventoryStateObservation(report, t.request.scope.limit, t.launch.wall, finished.wall) {
			reason = "inventory_state_invalid"
		} else {
			result.Observation = cloneInventoryStateBudget(&report)
			switch {
			case report.Available && report.Status == "below_limit":
				result.Usable = true
			case report.Status == "limit_reached":
				reason = "inventory_state_limit"
			case report.Reason == "inventory_state_changed" || report.Reason == "inventory_state_overflow":
				reason = report.Reason
			default:
				reason = "inventory_state_unavailable"
			}
		}
	}
	result.Reason = reason
	t.returned.Store(&inventoryStateReturnCapture{completion: result})
	c.domain.mu.Unlock()
	if c.afterCompletionCapture != nil {
		c.afterCompletionCapture()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.refusal != "" {
		reason = t.refusal
	}
	if reason != "" {
		// Only the frozen actual-return verdict (or earlier sticky denial)
		// records callback failure after the returned marker was published.
		c.recordFailureLocked(t, reason)
		result.Reason = t.refusal
		result.Usable = false
	}
	t.completion = result
	close(t.done)
	// Producer timer cleanup cannot poison the frozen successful verdict.
	t.cancel()
}

func (c *inventoryStateCoordinator) takeCompletion(ctx context.Context, request inventoryStateRequest, t *inventoryStateTicket) (inventoryStateCompletion, bool) {
	if ctx == nil || t == nil {
		return inventoryStateCompletion{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validRequest(request) || c.ticket != t || t.request != request || t.used || inventoryStateContextReason(ctx) != "" || inventoryStateContextReason(t.parent) != "" {
		return inventoryStateCompletion{}, false
	}
	result, returned := t.completed()
	if !returned || !result.Usable || t.refusal != "" {
		return result, false
	}
	p, clockReason := c.domain.capture()
	if clockReason != "" || p.wall.Before(result.CompletedWall) || p.elapsed.Before(result.CompletedElapsed) {
		// The callback already returned successfully. Discard its permit and
		// retain domain high-waters without inventing a failed metadata call.
		t.used = true
		return result, false
	}
	if c.windowReason(t, p) != "" {
		// Expired consumption of a timely result is not a new failed sample.
		t.used = true
		return result, false
	}
	if inventoryStateContextReason(ctx) != "" || inventoryStateContextReason(t.parent) != "" {
		return result, false
	}
	t.used = true
	if request.phase == "before_dispatch" {
		request.intent.preUsed = true
	}
	return result, true
}

func (c *inventoryStateCoordinator) cancelOwned(scope *inventoryStateRunScope, t *inventoryStateTicket) bool {
	if t == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validScope(scope) || c.ticket != t || t.request.scope != scope {
		return false
	}
	if t.returned.Load() != nil {
		return false
	}
	return c.refuseLocked(t, "inventory_state_canceled")
}

func (c *inventoryStateCoordinator) abandonLocked(scope *inventoryStateRunScope, intent *inventoryStateIntent) {
	intent.abandoned = true
	if c.ticket != nil && c.ticket.request.scope == scope && c.ticket.request.intent == intent {
		if c.ticket.returned.Load() == nil {
			c.refuseLocked(c.ticket, "inventory_state_canceled")
		}
	}
}

func (c *inventoryStateCoordinator) abandon(scope *inventoryStateRunScope, intent *inventoryStateIntent) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validScope(scope) || intent == nil || intent.scope != scope || scope.current != intent {
		return false
	}
	c.abandonLocked(scope, intent)
	return true
}

// cachedStatus reads no clocks, metadata, SQL or observer. A foreign ticket
// supplies capacity/wake evidence only, never its report or cancel authority.
func (c *inventoryStateCoordinator) cachedStatus(scope *inventoryStateRunScope) inventoryStateAsyncDecision {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := inventoryStateAsyncDecision{SamplingState: "not_requested"}
	t := c.ticket
	if t == nil {
		return d
	}
	d.Ticket = t
	_, published := t.completed()
	captured := t.returned.Load()
	d.CallbackReturned = captured != nil
	d.ProcessSlotOccupied = !published
	if t.request.scope != scope {
		d.SamplingState, d.Reason = "foreign_slot", "inventory_state_process_slot"
		return d
	}
	d.SamplingState = "pending"
	if d.CallbackReturned {
		d.SamplingState = "returned"
		if captured.completion.Reason != "" {
			d.SamplingState, d.Reason = "refused", captured.completion.Reason
		}
	}
	if t.refusal != "" {
		d.SamplingState, d.Reason = "refused", t.refusal
	}
	return d
}
