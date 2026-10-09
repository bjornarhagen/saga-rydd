package worker

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const (
	InventoryStatePolicyContract = "worker_inventory_state_admission_v1"
	inventoryStateRetryInterval  = 5 * time.Minute
	inventoryStateSampleWindow   = 2 * time.Second
)

// InventoryStatePolicySnapshot is cached admission evidence, not a current
// measurement or a physical storage quota. Reading it performs no sample.
type InventoryStatePolicySnapshot struct {
	Contract              string                      `json:"contract"`
	PolicyEnabled         bool                        `json:"policy_enabled"`
	SourceOnly            bool                        `json:"source_only"`
	Persistent            bool                        `json:"persistent"`
	Limit                 int64                       `json:"limit"`
	RetryIntervalNS       int64                       `json:"retry_interval_ns"`
	LastAttemptPhase      string                      `json:"last_attempt_phase"`
	LastAttemptStartedAt  *time.Time                  `json:"last_attempt_started_at"`
	LastAttemptFinishedAt *time.Time                  `json:"last_attempt_finished_at"`
	LastDecisionStatus    string                      `json:"last_decision_status"`
	LastDecisionReason    string                      `json:"last_decision_reason"`
	LastSourceBackoff     *bool                       `json:"last_source_backoff"`
	RetryStartedAt        *time.Time                  `json:"retry_started_at"`
	RetryAfterAt          *time.Time                  `json:"retry_after_at"`
	Observation           *state.InventoryStateBudget `json:"observation"`
}

type sourceInventoryState struct {
	enabled                   bool
	limit                     int64
	phase, status, reason     string
	startedWall, finishedWall time.Time
	backoff                   bool
	observation               *state.InventoryStateBudget
	retryWall, retryElapsed   time.Time
	highWall, highElapsed     time.Time
}

func cloneInventoryStateBudget(source *state.InventoryStateBudget) *state.InventoryStateBudget {
	if source == nil {
		return nil
	}
	copy := *source
	clone := func(source *int64) *int64 {
		if source == nil {
			return nil
		}
		value := *source
		return &value
	}
	copy.DatabaseBytes, copy.WALBytes, copy.TotalBytes = clone(source.DatabaseBytes), clone(source.WALBytes), clone(source.TotalBytes)
	return &copy
}

// remaining compares each clock domain independently. Status and controls do
// not renew either anchor, and a forward wall adjustment cannot erase elapsed
// pacing. A rollback must also regain the previously observed high-water.
func (p *sourceInventoryState) remaining(wall, elapsed time.Time) time.Duration {
	if !p.backoff {
		return 0
	}
	wall = wall.Round(0).UTC()
	remaining := max(time.Duration(0), p.retryWall.Sub(wall), p.retryElapsed.Sub(elapsed), p.highWall.Sub(wall), p.highElapsed.Sub(elapsed))
	if wall.After(p.highWall) {
		p.highWall = wall
	}
	if elapsed.After(p.highElapsed) {
		p.highElapsed = elapsed
	}
	return remaining
}

// Recheck the captured start clocks at the call boundary. A clock can move
// backwards after an earlier readiness check, even beyond the original due
// time. A refused start keeps the earlier evidence and fixed retry anchors.
func (p *sourceInventoryState) observe(ctx context.Context, phase string, wallNow, elapsedNow func() time.Time, observer func(context.Context) (state.InventoryStateBudget, error)) bool {
	startedWall, startedElapsed := wallNow(), elapsedNow()
	if p.remaining(startedWall, startedElapsed) > 0 {
		return false
	}
	report, err := observer(ctx)
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return p.record(phase, report, err, startedWall, startedElapsed, wallNow(), elapsedNow())
}

func validInventoryStateObservation(r state.InventoryStateBudget, limit int64, started, finished time.Time) bool {
	if r.Contract != state.InventoryStateBudgetContract || r.Scope != "configured_inventory_database_and_wal" || r.Limit != limit || !r.SequentialObservations || r.HardLimitEnforced || r.PhysicalAllocationVerified || r.NamespaceAuthenticated || r.OtherStoresIncluded || r.SampleStartedAt.IsZero() || r.SampleFinishedAt.Before(r.SampleStartedAt) || r.SampleStartedAt.Before(started) || r.SampleFinishedAt.After(finished) {
		return false
	}
	if !r.Available {
		if r.Status != "unavailable" || r.TotalBytes != nil {
			return false
		}
		switch r.Reason {
		case "inventory_state_unavailable", "inventory_state_changed":
			return r.DatabaseBytes == nil && r.WALBytes == nil
		case "inventory_state_overflow":
			return r.DatabaseBytes != nil && r.WALBytes != nil && *r.DatabaseBytes >= 0 && *r.WALBytes >= 0 && *r.DatabaseBytes > math.MaxInt64-*r.WALBytes
		default:
			return false
		}
	}
	if r.DatabaseBytes == nil || r.WALBytes == nil || r.TotalBytes == nil || *r.DatabaseBytes < 0 || *r.WALBytes < 0 || *r.DatabaseBytes > math.MaxInt64-*r.WALBytes || *r.TotalBytes != *r.DatabaseBytes+*r.WALBytes {
		return false
	}
	if *r.TotalBytes < limit {
		return r.Status == "below_limit" && r.Reason == ""
	}
	return r.Status == "limit_reached" && r.Reason == "inventory_state_limit"
}

// record always replaces the previous observation. A failed attempt must not
// leave an earlier below-limit sample looking like its result.
func (p *sourceInventoryState) record(phase string, report state.InventoryStateBudget, err error, startedWall, startedElapsed, finishedWall, finishedElapsed time.Time) bool {
	p.phase, p.startedWall, p.finishedWall = phase, startedWall.Round(0).UTC(), time.Time{}
	p.status, p.reason, p.backoff, p.observation = "source_deferred", "inventory_state_unavailable", true, nil
	startedWall, finishedWall = startedWall.Round(0).UTC(), finishedWall.Round(0).UTC()
	chronology := !finishedWall.Before(startedWall) && !finishedElapsed.Before(startedElapsed)
	if chronology {
		p.finishedWall = finishedWall
	}
	switch {
	case !chronology || errors.Is(err, state.ErrInventoryStateBudgetClock):
		p.reason = "inventory_state_clock"
	case errors.Is(err, context.DeadlineExceeded) || finishedWall.Sub(startedWall) >= inventoryStateSampleWindow || finishedElapsed.Sub(startedElapsed) >= inventoryStateSampleWindow:
		p.reason = "inventory_state_timeout"
	case errors.Is(err, context.Canceled):
		p.reason = "inventory_state_canceled"
	case err != nil:
		p.reason = "inventory_state_unavailable"
	case !validInventoryStateObservation(report, p.limit, startedWall, finishedWall):
		p.reason = "inventory_state_invalid"
	default:
		p.observation = cloneInventoryStateBudget(&report)
		if report.Available && report.Status == "below_limit" {
			p.status, p.reason, p.backoff = "source_admitted", "", false
		} else if report.Status == "limit_reached" {
			p.reason = "inventory_state_limit"
		} else if report.Reason == "inventory_state_changed" || report.Reason == "inventory_state_overflow" {
			p.reason = report.Reason
		}
	}
	if p.backoff {
		p.retryWall, p.retryElapsed = startedWall.Add(inventoryStateRetryInterval), startedElapsed.Add(inventoryStateRetryInterval)
		p.highWall, p.highElapsed = maxTime(startedWall, finishedWall), maxTime(startedElapsed, finishedElapsed)
	} else {
		p.retryWall, p.retryElapsed, p.highWall, p.highElapsed = time.Time{}, time.Time{}, time.Time{}, time.Time{}
	}
	return !p.backoff
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (p *sourceInventoryState) snapshot() *InventoryStatePolicySnapshot {
	s := &InventoryStatePolicySnapshot{Contract: InventoryStatePolicyContract, PolicyEnabled: p.enabled, SourceOnly: true, Limit: p.limit, RetryIntervalNS: int64(inventoryStateRetryInterval), LastDecisionStatus: "not_evaluated"}
	if p.startedWall.IsZero() {
		return s
	}
	started, backoff := p.startedWall, p.backoff
	s.LastAttemptPhase, s.LastAttemptStartedAt, s.LastSourceBackoff = p.phase, &started, &backoff
	s.LastDecisionStatus, s.LastDecisionReason = p.status, p.reason
	if !p.finishedWall.IsZero() {
		finished := p.finishedWall
		s.LastAttemptFinishedAt = &finished
	}
	if p.backoff {
		retry := p.retryWall
		s.RetryStartedAt, s.RetryAfterAt = &started, &retry
	}
	s.Observation = cloneInventoryStateBudget(p.observation)
	return s
}

func inventoryStateReceiptCurrent(ctx context.Context, reservedWall, reservedElapsed, wall, elapsed time.Time) bool {
	return ctx.Err() == nil && !wall.Before(reservedWall) && !elapsed.Before(reservedElapsed) && wall.Sub(reservedWall) < state.FairInventoryClaimWindow && elapsed.Sub(reservedElapsed) < state.FairInventoryClaimWindow
}
