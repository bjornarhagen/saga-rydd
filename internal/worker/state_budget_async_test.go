package worker

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type inventoryStateAsyncClock struct {
	mu            sync.Mutex
	wall, elapsed time.Time
	reads         atomic.Int64
}

func newInventoryStateAsyncClock() *inventoryStateAsyncClock {
	stamp := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	return &inventoryStateAsyncClock{wall: stamp, elapsed: stamp}
}

func (c *inventoryStateAsyncClock) wallNow() time.Time {
	c.reads.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

func (c *inventoryStateAsyncClock) elapsedNow() time.Time {
	c.reads.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.elapsed
}

func (c *inventoryStateAsyncClock) advance(wall, elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall, c.elapsed = c.wall.Add(wall), c.elapsed.Add(elapsed)
}

func asyncInventorySampler(t *testing.T) state.InventoryStateBudgetSampler {
	t.Helper()
	w, err := state.OpenWriter(context.Background(), filepath.Join(t.TempDir(), "private"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sampler, err := w.InventoryStateBudgetSampler()
	if err != nil {
		t.Fatal(err)
	}
	return sampler
}

func asyncInventoryReport(clock *inventoryStateAsyncClock, limit int64) state.InventoryStateBudget {
	stamp := clock.wallNow()
	database, wal, total := int64(31), int64(0), int64(31)
	return state.InventoryStateBudget{Contract: state.InventoryStateBudgetContract, Scope: "configured_inventory_database_and_wal", Available: true, Status: "below_limit", Limit: limit, DatabaseBytes: &database, WALBytes: &wal, TotalBytes: &total, SampleStartedAt: stamp, SampleFinishedAt: stamp, SequentialObservations: true}
}

func asyncInventoryScope(t *testing.T, c *inventoryStateCoordinator, sampler state.InventoryStateBudgetSampler, observer inventoryStateObserver) *inventoryStateRunScope {
	t.Helper()
	scope, err := newInventoryStateRunScope(c, sampler, "generated-instance", state.MinInventoryStateBytes, newInventoryStateFairScopeIdentity(), observer)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func asyncInventoryRequest(t *testing.T, c *inventoryStateCoordinator, scope *inventoryStateRunScope) inventoryStateRequest {
	t.Helper()
	intent, err := c.newIntent(scope)
	if err != nil {
		t.Fatal(err)
	}
	return inventoryStateRequest{scope: scope, intent: intent, phase: "before_dispatch"}
}

func waitAsyncInventory(t *testing.T, ticket *inventoryStateTicket) inventoryStateCompletion {
	t.Helper()
	if ticket == nil {
		t.Fatal("missing inventory sample ticket")
	}
	select {
	case <-ticket.done:
	case <-time.After(5 * time.Second):
		t.Fatal("generated inventory callback did not return")
	}
	result, returned := ticket.completed()
	if !returned {
		t.Fatal("completion missing after publication")
	}
	return result
}

func waitAsyncInventoryEvent(t *testing.T, event <-chan struct{}) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(5 * time.Second):
		t.Fatal("generated inventory event did not occur")
	}
}

func TestInventoryStateAsyncOneSlotAcrossRunStoreAndThreshold(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var first *inventoryStateTicket
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if first != nil {
			waitAsyncInventory(t, first)
		}
	})
	var calls atomic.Int64
	a := asyncInventoryScope(t, c, asyncInventorySampler(t), func(context.Context, state.InventoryStateBudgetSampler, int64) (state.InventoryStateBudget, error) {
		calls.Add(1)
		close(entered)
		<-release // deliberately noncooperative; cleanup releases and joins it
		return asyncInventoryReport(clock, state.MinInventoryStateBytes), nil
	})
	requestA := asyncInventoryRequest(t, c, a)
	started := c.startEligible(context.Background(), requestA)
	first = started.Ticket
	if !started.Started || !started.ProcessSlotOccupied {
		t.Fatal(started)
	}
	waitAsyncInventoryEvent(t, entered)
	b, err := newInventoryStateRunScope(c, asyncInventorySampler(t), "second-generated-instance", state.MinInventoryStateBytes+1, newInventoryStateFairScopeIdentity(), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		calls.Add(1)
		return asyncInventoryReport(clock, limit), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	requestB := asyncInventoryRequest(t, c, b)
	var competing sync.WaitGroup
	var wrong atomic.Bool
	for i := 0; i < 20; i++ {
		competing.Add(1)
		go func() {
			defer competing.Done()
			blocked := c.startEligible(context.Background(), requestB)
			if blocked.Started || !blocked.ProcessSlotOccupied || blocked.SamplingState != "foreign_slot" || blocked.Ticket != first {
				wrong.Store(true)
			}
		}()
	}
	competing.Wait()
	if wrong.Load() || calls.Load() != 1 {
		t.Fatal("simultaneous foreign asks adopted or replaced held callback", calls.Load())
	}
	if c.cancelOwned(b, first) || !c.cancelOwned(a, first) {
		t.Fatal("cancellation did not bind the exact Run")
	}
	clock.advance(inventoryStateRetryInterval, inventoryStateRetryInterval)
	if next := c.startEligible(context.Background(), requestB); next.Started || !next.ProcessSlotOccupied || calls.Load() != 1 {
		t.Fatal("expired retry replaced an unfinished callback", next, calls.Load())
	}
	releaseOnce.Do(func() { close(release) })
	if result := waitAsyncInventory(t, first); result.Usable || result.Reason != "inventory_state_canceled" {
		t.Fatal("late canceled result became usable", result)
	}
	second := c.startEligible(context.Background(), requestB)
	if !second.Started || second.Ticket == first || c.cancelOwned(a, second.Ticket) {
		t.Fatal("new scope failed after actual return", second)
	}
	if result := waitAsyncInventory(t, second.Ticket); !result.Usable || calls.Load() != 2 {
		t.Fatal(result, calls.Load())
	}
}

func TestInventoryStateAsyncExactRequestMultiReaderAndSingleUse(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	var calls atomic.Int64
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		calls.Add(1)
		return asyncInventoryReport(clock, limit), nil
	})
	r := asyncInventoryRequest(t, c, scope)
	first := c.startEligible(context.Background(), r)
	result := waitAsyncInventory(t, first.Ticket)
	if !result.Usable || first.Ticket.ctx.Err() == nil {
		t.Fatal("producer cleanup or valid report mismatch", result)
	}
	var group sync.WaitGroup
	var wrong atomic.Bool
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			out := c.startEligible(context.Background(), r)
			if out.Started || out.Ticket != first.Ticket || !out.CallbackReturned {
				wrong.Store(true)
			}
			copy, returned := first.Ticket.completed()
			if !returned || copy.Observation == nil {
				wrong.Store(true)
				return
			}
			*copy.Observation.TotalBytes = 999
		}()
	}
	group.Wait()
	if wrong.Load() || calls.Load() != 1 {
		t.Fatal("exact asks relaunched or completion was stolen", calls.Load())
	}
	copy, _ := first.Ticket.completed()
	if *copy.Observation.TotalBytes != 31 {
		t.Fatal("a reader mutated retained completion", copy)
	}
	if _, permitted := c.takeCompletion(context.Background(), r, first.Ticket); !permitted {
		t.Fatal("timer cleanup poisoned frozen success")
	}
	if _, permitted := c.takeCompletion(context.Background(), r, first.Ticket); permitted {
		t.Fatal("phase consumed twice")
	}
}

func TestInventoryStateAsyncQueuedDeadlineAndIndependentRetry(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	var queued func()
	c.launch = func(run func()) { queued = run }
	var calls int
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		calls++
		return asyncInventoryReport(clock, limit), nil
	})
	r := asyncInventoryRequest(t, c, scope)
	first := c.startEligible(context.Background(), r)
	clock.advance(inventoryStateSampleWindow, 0)
	queued()
	if out := waitAsyncInventory(t, first.Ticket); out.Usable || out.Reason != "inventory_state_timeout" || calls != 0 {
		t.Fatal("queued expired callback entered observer", out, calls)
	}
	r = asyncInventoryRequest(t, c, scope)
	clock.advance(inventoryStateRetryInterval-inventoryStateSampleWindow, 0)
	if out := c.startEligible(context.Background(), r); out.Started || out.Status != "retry_wait" || out.Wait != inventoryStateRetryInterval {
		t.Fatal("wall-only advance erased elapsed retry", out)
	}
	clock.advance(0, inventoryStateRetryInterval)
	second := c.startEligible(context.Background(), r)
	if !second.Started {
		t.Fatal("both fixed retry anchors did not allow a new request", second)
	}
	queued()
	if out := waitAsyncInventory(t, second.Ticket); !out.Usable || calls != 1 {
		t.Fatal(out, calls)
	}
}

func TestInventoryStateAsyncReturnCaptureBeforeOwnerPublication(t *testing.T) {
	for _, action := range []string{"normal_clock", "expired_consumer", "canceled_parent", "abandoned_intent"} {
		t.Run(action, func(t *testing.T) {
			clock := newInventoryStateAsyncClock()
			c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
			captured, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var ticket *inventoryStateTicket
			t.Cleanup(func() {
				once.Do(func() { close(release) })
				if ticket != nil {
					waitAsyncInventory(t, ticket)
				}
			})
			c.afterCompletionCapture = func() { close(captured); <-release }
			observer := func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
				return asyncInventoryReport(clock, limit), nil
			}
			sampler := asyncInventorySampler(t)
			scope := asyncInventoryScope(t, c, sampler, observer)
			r := asyncInventoryRequest(t, c, scope)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ticket = c.startEligible(ctx, r).Ticket
			waitAsyncInventoryEvent(t, captured)
			switch action {
			case "normal_clock":
				clock.advance(time.Millisecond, time.Millisecond)
			case "expired_consumer":
				clock.advance(inventoryStateSampleWindow, inventoryStateSampleWindow)
			case "canceled_parent":
				cancel()
			case "abandoned_intent":
				if !c.abandon(scope, r.intent) {
					t.Fatal("could not abandon exact intent")
				}
			}
			foreign := asyncInventoryScope(t, c, sampler, observer)
			blocked := c.startEligible(context.Background(), asyncInventoryRequest(t, c, foreign))
			if blocked.Started || !blocked.ProcessSlotOccupied || !blocked.CallbackReturned || c.cancelOwned(scope, ticket) {
				t.Fatal("return capture was treated as an unfinished metadata call", blocked)
			}
			once.Do(func() { close(release) })
			result := waitAsyncInventory(t, ticket)
			if !result.Usable || result.Reason != "" || !c.retry.wall.IsZero() || !c.retry.elapsed.IsZero() {
				t.Fatal("later owner manufactured callback failure/retry", result, c.retry)
			}
			_, permitted := c.takeCompletion(ctx, r, ticket)
			if permitted != (action == "normal_clock") {
				t.Fatal("owner freshness/context verdict incorrect", action, permitted)
			}
		})
	}
}

func TestInventoryStateAsyncConsumptionRollbackIsNotFailedCall(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		return asyncInventoryReport(clock, limit), nil
	})
	r := asyncInventoryRequest(t, c, scope)
	ticket := c.startEligible(context.Background(), r).Ticket
	if result := waitAsyncInventory(t, ticket); !result.Usable {
		t.Fatal(result)
	}
	clock.advance(-time.Millisecond, 0)
	if _, permitted := c.takeCompletion(context.Background(), r, ticket); permitted {
		t.Fatal("rollback consumer admitted")
	}
	clock.advance(time.Millisecond, 0)
	if _, permitted := c.takeCompletion(context.Background(), r, ticket); permitted {
		t.Fatal("discarded permit resurrected")
	}
	if !c.retry.wall.IsZero() || !c.retry.elapsed.IsZero() {
		t.Fatal("successful call acquired a manufactured failure retry")
	}
	r = asyncInventoryRequest(t, c, scope)
	next := c.startEligible(context.Background(), r)
	if !next.Started {
		t.Fatal("regained high-water did not admit fresh sample", next)
	}
	if result := waitAsyncInventory(t, next.Ticket); !result.Usable {
		t.Fatal(result)
	}
}

func TestInventoryStateAsyncReturnWinsPendingCancelCheck(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	entered, releaseObserver := make(chan struct{}), make(chan struct{})
	checked, continueCancel := make(chan struct{}), make(chan struct{})
	captured, continuePublish := make(chan struct{}), make(chan struct{})
	var observerOnce, cancelOnce, publishOnce sync.Once
	var ticket *inventoryStateTicket
	var cancelResult <-chan bool
	t.Cleanup(func() {
		observerOnce.Do(func() { close(releaseObserver) })
		cancelOnce.Do(func() { close(continueCancel) })
		publishOnce.Do(func() { close(continuePublish) })
		if ticket != nil {
			waitAsyncInventory(t, ticket)
		}
		if cancelResult != nil {
			select {
			case <-cancelResult:
			case <-time.After(5 * time.Second):
				t.Error("generated cancellation caller did not return")
			}
		}
	})
	c.beforePendingRefusal = func() { close(checked); <-continueCancel }
	c.afterCompletionCapture = func() { close(captured); <-continuePublish }
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		close(entered)
		<-releaseObserver
		return asyncInventoryReport(clock, limit), nil
	})
	r := asyncInventoryRequest(t, c, scope)
	ticket = c.startEligible(context.Background(), r).Ticket
	waitAsyncInventoryEvent(t, entered)
	result := make(chan bool, 1)
	cancelResult = result
	go func() { result <- c.cancelOwned(scope, ticket); close(result) }()
	waitAsyncInventoryEvent(t, checked)
	// The cancellation caller holds coordinator.mu after its initial nil check.
	// The producer freezes return under the separate domain guard first.
	observerOnce.Do(func() { close(releaseObserver) })
	waitAsyncInventoryEvent(t, captured)
	cancelOnce.Do(func() { close(continueCancel) })
	select {
	case canceled := <-result:
		if canceled {
			t.Fatal("pending nil check canceled an already-returned callback")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation/domain ordering deadlocked")
	}
	publishOnce.Do(func() { close(continuePublish) })
	completion := waitAsyncInventory(t, ticket)
	if !completion.Usable || completion.Reason != "" || !c.retry.wall.IsZero() || !c.retry.elapsed.IsZero() {
		t.Fatal("successful return acquired false callback failure", completion, c.retry)
	}
	if _, admitted := c.takeCompletion(context.Background(), r, ticket); !admitted {
		t.Fatal("return-first cancellation poisoned permit")
	}
}

func TestInventoryStateAsyncActualReturnDeadlineRollbackAndCancellation(t *testing.T) {
	for _, action := range []string{"wall_equal", "elapsed_equal", "wall_rollback", "elapsed_rollback", "parent_cancel"} {
		t.Run(action, func(t *testing.T) {
			clock := newInventoryStateAsyncClock()
			c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
				report := asyncInventoryReport(clock, limit)
				switch action {
				case "wall_equal":
					clock.advance(inventoryStateSampleWindow, 0)
				case "elapsed_equal":
					clock.advance(0, inventoryStateSampleWindow)
				case "wall_rollback":
					clock.advance(-time.Millisecond, 0)
				case "elapsed_rollback":
					clock.advance(0, -time.Millisecond)
				case "parent_cancel":
					cancel()
				}
				return report, nil
			})
			r := asyncInventoryRequest(t, c, scope)
			ticket := c.startEligible(ctx, r).Ticket
			result := waitAsyncInventory(t, ticket)
			expected := "inventory_state_timeout"
			if strings.Contains(action, "rollback") {
				expected = "inventory_state_clock"
			}
			if action == "parent_cancel" {
				expected = "inventory_state_canceled"
			}
			if result.Usable || result.Reason != expected || c.retry.wall != ticket.launch.wall.Add(inventoryStateRetryInterval) || c.retry.elapsed != ticket.launch.elapsed.Add(inventoryStateRetryInterval) {
				t.Fatal("actual return refusal did not retain fixed retry", result, c.retry)
			}
		})
	}
}

func TestInventoryStateAsyncDistinctReceiptAndOriginalWindow(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(map[bool]string{false: "usable", true: "expired"}[expire], func(t *testing.T) {
			clock := newInventoryStateAsyncClock()
			c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
			var calls atomic.Int64
			scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
				calls.Add(1)
				return asyncInventoryReport(clock, limit), nil
			})
			pre := asyncInventoryRequest(t, c, scope)
			preTicket := c.startEligible(context.Background(), pre).Ticket
			waitAsyncInventory(t, preTicket)
			if _, ok := c.takeCompletion(context.Background(), pre, preTicket); !ok {
				t.Fatal("pre refused")
			}
			wall, elapsed := clock.wallNow(), clock.elapsedNow()
			receipt, err := c.bindReceipt(pre.intent, state.DispatchBudget{Day: wall.Format(time.DateOnly), Used: 1}, wall, elapsed, time.Now().Add(state.FairInventoryClaimWindow))
			if err != nil {
				t.Fatal(err)
			}
			post := inventoryStateRequest{scope: scope, intent: pre.intent, phase: "after_receipt", receipt: receipt}
			copy := *receipt
			forged := post
			forged.receipt = &copy
			if out := c.startEligible(context.Background(), forged); out.Started || out.Status != "invalid_scope" {
				t.Fatal("equal but foreign receipt accepted", out)
			}
			clock.advance(time.Millisecond, time.Millisecond)
			postTicket := c.startEligible(context.Background(), post).Ticket
			if postTicket == preTicket {
				t.Fatal("post reused pre sample")
			}
			if result := waitAsyncInventory(t, postTicket); !result.Usable || calls.Load() != 2 {
				t.Fatal(result, calls.Load())
			}
			if expire {
				clock.advance(state.FairInventoryClaimWindow-time.Millisecond, 0)
			}
			if _, ok := c.takeCompletion(context.Background(), post, postTicket); ok == expire {
				t.Fatal("receipt equality/window verdict", expire, ok)
			}
			if _, ok := c.takeCompletion(context.Background(), post, postTicket); ok {
				t.Fatal("post replayed")
			}
			if !c.retry.wall.IsZero() {
				t.Fatal("late receipt invented metadata failure")
			}
		})
	}
}

func TestInventoryStateAsyncInvalidReportsPanicAndPrivateErrors(t *testing.T) {
	for _, kind := range []string{"limit", "missing", "changed", "overflow", "invalid_total", "wrong_limit", "authority", "error", "panic"} {
		t.Run(kind, func(t *testing.T) {
			clock := newInventoryStateAsyncClock()
			c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
			scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
				r := asyncInventoryReport(clock, limit)
				switch kind {
				case "limit":
					*r.DatabaseBytes, *r.TotalBytes = limit, limit
					r.Status, r.Reason = "limit_reached", "inventory_state_limit"
				case "missing", "changed":
					r.Available, r.Status, r.Reason = false, "unavailable", "inventory_state_unavailable"
					r.DatabaseBytes, r.WALBytes, r.TotalBytes = nil, nil, nil
					if kind == "changed" {
						r.Reason = "inventory_state_changed"
					}
				case "overflow":
					*r.DatabaseBytes, *r.WALBytes = math.MaxInt64, 1
					r.TotalBytes = nil
					r.Available, r.Status, r.Reason = false, "unavailable", "inventory_state_overflow"
				case "invalid_total":
					*r.TotalBytes++
				case "wrong_limit":
					r.Limit++
				case "authority":
					r.NamespaceAuthenticated = true
				case "error":
					return r, errors.New("unpublished-private-path-and-secret")
				case "panic":
					panic("unpublished-private-path-and-secret")
				}
				return r, nil
			})
			result := waitAsyncInventory(t, c.startEligible(context.Background(), asyncInventoryRequest(t, c, scope)).Ticket)
			encoded, err := json.Marshal(result)
			if err != nil || result.Usable || result.Reason == "" || strings.Contains(string(encoded), "private-path") || c.retry.wall.IsZero() {
				t.Fatal("invalid observation/error escaped", string(encoded), err)
			}
		})
	}
}

func TestInventoryStateAsyncScopeGenerationAndCanceledPreflight(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	sampler := asyncInventorySampler(t)
	var calls atomic.Int64
	observer := func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		calls.Add(1)
		return asyncInventoryReport(clock, limit), nil
	}
	scope := asyncInventoryScope(t, c, sampler, observer)
	r := asyncInventoryRequest(t, c, scope)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, parent := range []context.Context{nil, ctx} {
		if out := c.startEligible(parent, r); out.Started || c.nextID != 0 {
			t.Fatal("bad context started a callback", out)
		}
	}
	bad := r
	bad.phase = "after_receipt"
	if out := c.startEligible(context.Background(), bad); out.Started || out.Status != "invalid_scope" {
		t.Fatal(out)
	}
	other := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	if out := other.startEligible(context.Background(), r); out.Started || out.Status != "invalid_scope" {
		t.Fatal("clock/coordinator scope crossed", out)
	}
	if _, err := newInventoryStateRunScope(c, state.InventoryStateBudgetSampler{}, "x", state.MinInventoryStateBytes, newInventoryStateFairScopeIdentity(), observer); !errors.Is(err, errInventoryStateAsyncInput) {
		t.Fatal("zero sampler scope accepted", err)
	}
	c.nextID = math.MaxUint64
	if out := c.startEligible(context.Background(), r); out.Started || calls.Load() != 0 {
		t.Fatal("ticket generation wrapped", out)
	}
	scope.nextIntent = math.MaxUint64
	if _, err := c.newIntent(scope); !errors.Is(err, errInventoryStateAsyncInput) {
		t.Fatal("intent generation wrapped", err)
	}
	if !c.abandon(scope, r.intent) {
		t.Fatal("exact intent abandonment failed")
	}
	if out := c.startEligible(context.Background(), r); out.Started || out.Status != "invalid_scope" {
		t.Fatal("abandoned request adopted", out)
	}
}

func TestInventoryStateAsyncCachedStatusAndExplicitFixtureDomain(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	owner, err := newInventoryStateFixtureOwner(clock.wallNow, clock.elapsedNow)
	if err != nil {
		t.Fatal(err)
	}
	if selectedInventoryStateCoordinator(nil) != processInventoryStateCoordinator || selectedInventoryStateCoordinator(nil) != selectedInventoryStateCoordinator(nil) || selectedInventoryStateCoordinator(owner) != owner.coordinator {
		t.Fatal("production selection resets singleton")
	}
	if _, err := newInventoryStateFixtureOwner(nil, clock.elapsedNow); !errors.Is(err, errInventoryStateAsyncInput) {
		t.Fatal("unpaired fixture domain accepted")
	}
	c := owner.coordinator
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		return asyncInventoryReport(clock, limit), nil
	})
	if out := c.cachedStatus(scope); out.SamplingState != "not_requested" {
		t.Fatal(out)
	}
	ticket := c.startEligible(context.Background(), asyncInventoryRequest(t, c, scope)).Ticket
	waitAsyncInventory(t, ticket)
	reads := clock.reads.Load()
	for i := 0; i < 100; i++ {
		out := c.cachedStatus(scope)
		if out.SamplingState != "returned" || !out.CallbackReturned || out.ProcessSlotOccupied {
			t.Fatal(out)
		}
	}
	if clock.reads.Load() != reads {
		t.Fatal("cached status read clocks")
	}
	foreign := asyncInventoryScope(t, c, asyncInventorySampler(t), nil)
	if out := c.cachedStatus(foreign); out.SamplingState != "foreign_slot" || !out.CallbackReturned || out.ProcessSlotOccupied {
		t.Fatal("foreign historical completion adopted", out)
	}
}

func TestInventoryStateAsyncClockAdditionOverflowRefusesBeforeCallback(t *testing.T) {
	clock := newInventoryStateAsyncClock()
	clock.wall, clock.elapsed = time.Unix(0, math.MaxInt64-time.Minute.Nanoseconds()), time.Unix(0, math.MaxInt64-time.Minute.Nanoseconds())
	c := newInventoryStateCoordinator(clock.wallNow, clock.elapsedNow)
	var calls atomic.Int64
	scope := asyncInventoryScope(t, c, asyncInventorySampler(t), func(_ context.Context, _ state.InventoryStateBudgetSampler, limit int64) (state.InventoryStateBudget, error) {
		calls.Add(1)
		return asyncInventoryReport(clock, limit), nil
	})
	if out := c.startEligible(context.Background(), asyncInventoryRequest(t, c, scope)); out.Started || c.nextID != 0 || calls.Load() != 0 {
		t.Fatal("overflowing deadline launched callback", out)
	}
}
