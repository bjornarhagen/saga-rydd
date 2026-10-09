package worker

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func fixtureInventoryStateReport(limit, total int64, now time.Time) state.InventoryStateBudget {
	database, wal := total, int64(0)
	r := state.InventoryStateBudget{Contract: state.InventoryStateBudgetContract, Scope: "configured_inventory_database_and_wal", Available: true, Status: "below_limit", Limit: limit, DatabaseBytes: &database, WALBytes: &wal, TotalBytes: &total, SampleStartedAt: now.Round(0).UTC(), SampleFinishedAt: now.Round(0).UTC(), SequentialObservations: true}
	if total >= limit {
		r.Status, r.Reason = "limit_reached", "inventory_state_limit"
	}
	return r
}

func TestInventoryStatePolicyExactAdmissionAndUnknownEvidence(t *testing.T) {
	start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"below", "equal", "above", "unavailable", "unavailable_total", "unavailable_negative", "unavailable_reason", "known_overflow", "error", "deadline", "canceled", "clock", "wall_rollback", "elapsed_rollback", "wall_gap", "elapsed_gap", "contract", "scope", "limit", "nil_total", "negative", "overflow", "sum", "authority", "zero_time", "future_time"} {
		t.Run(mode, func(t *testing.T) {
			p := sourceInventoryState{enabled: true, limit: state.MinInventoryStateBytes}
			before := p.snapshot()
			if before.LastDecisionStatus != "not_evaluated" || before.LastAttemptStartedAt != nil || before.Observation != nil || before.LastSourceBackoff != nil {
				t.Fatal(before)
			}
			r := fixtureInventoryStateReport(p.limit, p.limit-1, start)
			wall, elapsed := start.Add(time.Millisecond), start.Add(time.Millisecond)
			var err error
			switch mode {
			case "equal":
				r = fixtureInventoryStateReport(p.limit, p.limit, start)
			case "above":
				r = fixtureInventoryStateReport(p.limit, p.limit+1, start)
			case "unavailable":
				r.Available, r.Status, r.Reason, r.DatabaseBytes, r.WALBytes, r.TotalBytes = false, "unavailable", "inventory_state_changed", nil, nil, nil
			case "unavailable_total":
				r.Available, r.Status, r.Reason = false, "unavailable", "inventory_state_unavailable"
			case "unavailable_negative":
				r.Available, r.Status, r.Reason, r.TotalBytes = false, "unavailable", "inventory_state_overflow", nil
				*r.DatabaseBytes = -1
			case "unavailable_reason":
				r.Available, r.Status, r.Reason, r.DatabaseBytes, r.WALBytes, r.TotalBytes = false, "unavailable", "unexpected private text", nil, nil, nil
			case "known_overflow":
				r.Available, r.Status, r.Reason, r.TotalBytes = false, "unavailable", "inventory_state_overflow", nil
				*r.DatabaseBytes, *r.WALBytes = math.MaxInt64, 1
			case "error":
				err = errors.New("private path must not reach status")
			case "deadline":
				err = context.DeadlineExceeded
			case "canceled":
				err = context.Canceled
			case "clock":
				err = state.ErrInventoryStateBudgetClock
			case "wall_rollback":
				wall = start.Add(-time.Nanosecond)
			case "elapsed_rollback":
				elapsed = start.Add(-time.Nanosecond)
			case "wall_gap":
				wall = start.Add(2 * time.Second)
			case "elapsed_gap":
				elapsed = start.Add(2 * time.Second)
			case "contract":
				r.Contract = "other"
			case "scope":
				r.Scope = "other"
			case "limit":
				r.Limit++
			case "nil_total":
				r.TotalBytes = nil
			case "negative":
				*r.WALBytes = -1
			case "overflow":
				*r.DatabaseBytes, *r.WALBytes = math.MaxInt64, 1
			case "sum":
				*r.TotalBytes--
			case "authority":
				r.HardLimitEnforced = true
			case "zero_time":
				r.SampleStartedAt = time.Time{}
			case "future_time":
				r.SampleFinishedAt = start.Add(time.Hour)
			}
			allowed := p.record("before_dispatch", r, err, start, start, wall, elapsed)
			snapshot := p.snapshot()
			if allowed != (mode == "below") || snapshot.LastSourceBackoff == nil || *snapshot.LastSourceBackoff == allowed || snapshot.SourceOnly != true || snapshot.Persistent || snapshot.Limit != p.limit || snapshot.RetryIntervalNS != int64(5*time.Minute) {
				t.Fatal(mode, snapshot, allowed)
			}
			if allowed {
				if snapshot.Observation == nil || snapshot.Observation.WALBytes == nil || *snapshot.Observation.WALBytes != 0 || snapshot.RetryAfterAt != nil {
					t.Fatal(snapshot)
				}
				*r.DatabaseBytes = 0
				*snapshot.Observation.TotalBytes = 0
				if *p.snapshot().Observation.TotalBytes != p.limit-1 || *p.snapshot().Observation.DatabaseBytes != p.limit-1 {
					t.Fatal("snapshot or source pointer aliases cached observation")
				}
			} else if snapshot.RetryStartedAt == nil || !snapshot.RetryStartedAt.Equal(start) || snapshot.RetryAfterAt == nil || !snapshot.RetryAfterAt.Equal(start.Add(5*time.Minute)) {
				t.Fatal(snapshot)
			}
			if mode == "error" && (snapshot.Observation != nil || snapshot.LastDecisionReason != "inventory_state_unavailable") {
				t.Fatal("raw error or old evidence escaped", snapshot)
			}
			if mode == "unavailable_total" || mode == "unavailable_negative" || mode == "unavailable_reason" {
				if snapshot.Observation != nil || snapshot.LastDecisionReason != "inventory_state_invalid" {
					t.Fatal("malformed unknown evidence escaped", snapshot)
				}
			}
		})
	}
}

func TestInventoryStatePolicyIndependentRetryClocksAndNoReanchor(t *testing.T) {
	start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	p := sourceInventoryState{enabled: true, limit: state.MinInventoryStateBytes}
	r := fixtureInventoryStateReport(p.limit, p.limit, start)
	if p.record("after_receipt", r, nil, start, start, start, start) {
		t.Fatal("equality admitted")
	}
	before := p.snapshot()
	if got := p.remaining(start.Add(time.Hour), start); got != 5*time.Minute {
		t.Fatal("wall jump erased elapsed wait", got)
	}
	if got := p.remaining(start.Add(time.Hour), start.Add(5*time.Minute)); got != 0 {
		t.Fatal(got)
	}
	if got := p.remaining(start.Add(5*time.Minute), start.Add(5*time.Minute)); got != 55*time.Minute {
		t.Fatal("rollback ignored saved high-water", got)
	}
	if !reflect.DeepEqual(before, p.snapshot()) {
		t.Fatal("gate or snapshot renewed historical retry")
	}
	q := sourceInventoryState{enabled: true, limit: p.limit}
	q.record("before_dispatch", r, nil, start, start, start, start)
	if got := q.remaining(start, start.Add(time.Hour)); got != 5*time.Minute {
		t.Fatal("elapsed advance erased wall wait", got)
	}
	if got := q.remaining(start.Add(5*time.Minute), start.Add(time.Hour)); got != 0 {
		t.Fatal(got)
	}
	if got := q.remaining(start.Add(5*time.Minute), start.Add(time.Minute)); got != 59*time.Minute {
		t.Fatal("elapsed rollback ignored high-water", got)
	}
}

func TestInventoryStatePolicyReadyThenStartRollbackDoesNotObserve(t *testing.T) {
	for _, domain := range []string{"wall", "elapsed"} {
		t.Run(domain, func(t *testing.T) {
			start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			p := sourceInventoryState{enabled: true, limit: state.MinInventoryStateBytes}
			p.record("after_receipt", fixtureInventoryStateReport(p.limit, p.limit, start), nil, start, start, start, start)
			readyWall, readyElapsed := start.Add(10*time.Minute), start.Add(10*time.Minute)
			if wait := p.remaining(readyWall, readyElapsed); wait != 0 {
				t.Fatal("retry did not become ready", wait)
			}
			before, beforeSnapshot := p, p.snapshot()
			// Both captured clocks are beyond the original five-minute due.
			// One now falls below its high-water from the readiness check.
			startedWall, startedElapsed := readyWall, readyElapsed
			if domain == "wall" {
				startedWall = start.Add(6 * time.Minute)
			} else {
				startedElapsed = start.Add(6 * time.Minute)
			}
			calls := 0
			allowed := p.observe(context.Background(), "before_dispatch", func() time.Time { return startedWall }, func() time.Time { return startedElapsed }, func(context.Context) (state.InventoryStateBudget, error) {
				calls++
				return fixtureInventoryStateReport(p.limit, p.limit-1, startedWall), nil
			})
			if allowed || calls != 0 || !reflect.DeepEqual(before, p) || !reflect.DeepEqual(beforeSnapshot, p.snapshot()) {
				t.Fatal("start rollback sampled or changed retry evidence", allowed, calls, beforeSnapshot, p.snapshot())
			}
		})
	}
}

func TestInventoryStatePolicyFailureReplacesEarlierEvidenceAndReceiptAge(t *testing.T) {
	start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	p := sourceInventoryState{enabled: true, limit: state.MinInventoryStateBytes}
	r := fixtureInventoryStateReport(p.limit, 7, start)
	p.record("before_dispatch", r, nil, start, start, start, start)
	p.record("after_receipt", state.InventoryStateBudget{}, context.DeadlineExceeded, start, start, start, start)
	if p.snapshot().Observation != nil || p.snapshot().LastDecisionReason != "inventory_state_timeout" {
		t.Fatal(p.snapshot())
	}
	for _, mode := range []string{"current", "wall_expired", "elapsed_expired", "wall_rollback", "elapsed_rollback", "canceled"} {
		ctx, cancel := context.WithCancel(context.Background())
		wall, elapsed := start, start
		switch mode {
		case "wall_expired":
			wall = start.Add(state.FairInventoryClaimWindow)
		case "elapsed_expired":
			elapsed = start.Add(state.FairInventoryClaimWindow)
		case "wall_rollback":
			wall = start.Add(-time.Nanosecond)
		case "elapsed_rollback":
			elapsed = start.Add(-time.Nanosecond)
		case "canceled":
			cancel()
		}
		if current := inventoryStateReceiptCurrent(ctx, start, start, wall, elapsed); current != (mode == "current") {
			t.Fatal(mode, current)
		}
		cancel()
	}
}
