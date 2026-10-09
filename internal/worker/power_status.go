package worker

import (
	"context"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
)

const PowerPolicyContract = "worker_source_power_policy_v1"

// PowerPolicySnapshot contains the last source admission decision and separately
// observed callback completion. Reading it does not evaluate or start a probe.
type PowerPolicySnapshot struct {
	Contract              string                 `json:"contract"`
	LastDecisionStatus    string                 `json:"last_decision_status"`
	LastDecisionReason    string                 `json:"last_decision_reason"`
	LastDecisionAt        *time.Time             `json:"last_decision_at"`
	LastSourceBackoff     *bool                  `json:"last_source_backoff"`
	LastWaitNS            *int64                 `json:"last_wait_ns"`
	LastNextSampleWaitNS  *int64                 `json:"last_next_sample_wait_ns"`
	TicketID              *uint64                `json:"ticket_id"`
	SampleLaunchAt        *time.Time             `json:"sample_launch_at"`
	SampleCompletedAt     *time.Time             `json:"sample_completed_at"`
	CallbackReturned      *bool                  `json:"callback_returned"`
	SampleStatus          string                 `json:"sample_status"`
	SampleReason          string                 `json:"sample_reason"`
	Observation           *powerinfo.Observation `json:"observation"`
	PolicyEnabled         bool                   `json:"policy_enabled"`
	SourceOnly            bool                   `json:"source_only"`
	Persistent            bool                   `json:"persistent"`
	PhysicalPowerVerified bool                   `json:"physical_power_verified"`
}

type sourcePowerState struct {
	enabled  bool
	decision powerPolicyDecision
	at       time.Time
	ticket   *powerTicket
	wake     <-chan struct{}
	returned bool
	result   powerCompletion
}

// Each Run owns its observation parent, while the coordinator owns the global
// slot. Cancel only a ticket actually started by this Run, never a borrowed one.
type sourcePowerLifetime struct {
	parent      context.Context
	ctx         context.Context
	cancel      context.CancelFunc
	coordinator *powerCoordinator
	owned       *powerTicket
}

func newSourcePowerLifetime(parent context.Context, coordinator *powerCoordinator) *sourcePowerLifetime {
	ctx, cancel := context.WithCancel(parent)
	return &sourcePowerLifetime{parent: parent, ctx: ctx, cancel: cancel, coordinator: coordinator}
}

func (l *sourcePowerLifetime) cancelCurrent() {
	l.cancel()
	if l.owned != nil {
		l.coordinator.cancelTicket(l.owned)
	}
}

func (l *sourcePowerLifetime) resume() {
	if l.ctx.Err() != nil {
		l.cancel()
		l.ctx, l.cancel = context.WithCancel(l.parent)
	}
}

func (p *sourcePowerState) update(decision powerPolicyDecision, wall time.Time) {
	p.decision, p.at = decision, wall.UTC()
	if p.ticket != decision.Ticket {
		p.ticket, p.returned, p.result = decision.Ticket, false, powerCompletion{}
		p.wake = nil
		if p.ticket != nil {
			p.wake = p.ticket.done
		}
	}
	p.refreshCompletion()
}

func (p *sourcePowerState) refreshCompletion() {
	if p.ticket == nil || p.returned {
		return
	}
	if result, returned := p.ticket.completed(); returned {
		p.returned, p.result = true, result
	}
}

// remaining uses fixed launch anchors. Controls and status never renew a wait.
// The observing window expires in either clock domain; sparse resampling must
// satisfy both independent domains before another callback can start.
func (p *sourcePowerState) remaining(wall, elapsed time.Time) time.Duration {
	if !p.decision.SourceBackoff || p.ticket == nil {
		return 0
	}
	if p.decision.Status == "observing" {
		if p.returned || powerWindowError(p.ticket, wall.UTC(), elapsed) != nil || (p.ticket.ctx != nil && context.Cause(p.ticket.ctx) != nil) {
			return 0 // Replan; a completed sample is not itself a new decision.
		}
		return max(0, min(p.ticket.LaunchWall.Add(powerinfo.ObservationTimeout).Sub(wall.UTC()), p.ticket.LaunchElapsed.Add(powerinfo.ObservationTimeout).Sub(elapsed)))
	}
	if !p.returned || !freshPowerCompletion(p.result, wall.UTC(), elapsed) {
		return 0 // Stale/rolled-back evidence supplies no source delay.
	}
	return max(0, max(p.ticket.LaunchWall.Add(powerSampleInterval).Sub(wall.UTC()), p.ticket.LaunchElapsed.Add(powerSampleInterval).Sub(elapsed)))
}

func (p *sourcePowerState) snapshot() *PowerPolicySnapshot {
	p.refreshCompletion()
	s := &PowerPolicySnapshot{Contract: PowerPolicyContract, LastDecisionStatus: "not_evaluated", SampleStatus: "not_recorded", SourceOnly: true, PolicyEnabled: p.enabled}
	if !p.enabled {
		s.LastDecisionReason = "power_policy_disabled"
	}
	if !p.at.IsZero() {
		at, backoff := p.at, p.decision.SourceBackoff
		wait, next := int64(p.decision.Wait), int64(p.decision.NextSampleWait)
		s.LastDecisionAt, s.LastSourceBackoff, s.LastWaitNS, s.LastNextSampleWaitNS = &at, &backoff, &wait, &next
		s.LastDecisionStatus, s.LastDecisionReason = p.decision.Status, p.decision.Reason
	}
	if p.ticket != nil {
		id, launch, returned := p.ticket.ID, p.ticket.LaunchWall.UTC(), p.returned
		s.TicketID, s.SampleLaunchAt, s.CallbackReturned = &id, &launch, &returned
		s.SampleStatus = "callback_not_returned"
		if p.returned {
			completed := p.result.CompletedWall.UTC()
			s.SampleCompletedAt, s.Observation = &completed, clonePowerObservation(p.result.Observation)
			s.SampleStatus, s.SampleReason = "completed", "power_sample_unknown"
			if p.result.Err != nil {
				s.SampleStatus, s.SampleReason = "unknown", powerCompletionReason(p.result, nil)
			} else if p.result.Observation != nil {
				s.SampleStatus, s.SampleReason = p.result.Observation.Status, p.result.Observation.Reason
			}
		}
	}
	return s
}
