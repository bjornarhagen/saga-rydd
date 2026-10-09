package worker

import (
	"context"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type fairInventoryPlan struct {
	schedule    state.FairInventorySchedule
	due         time.Time
	waitReason  string
	allowSource bool
	generic     bool
}

func nextFairInventoryPlan(ctx context.Context, w *state.Store, roots state.FairInventoryRoots, genericKinds []string, now time.Time, metadata state.MetadataBudget, startup int64, sourceEnabled bool) (fairInventoryPlan, error) {
	allowed, reason, err := metadataReadiness(metadata, now, startup)
	if err != nil {
		return fairInventoryPlan{}, err
	}
	plan := fairInventoryPlan{allowSource: reason == "" && sourceEnabled}
	plan.schedule, err = w.NextFairInventoryTurn(ctx, roots, now, plan.allowSource)
	if err != nil {
		return fairInventoryPlan{}, err
	}
	if plan.schedule.Turn != nil {
		plan.due = now
		if plan.schedule.Turn.Kind == state.FairInventoryMaintenance {
			plan.waitReason = "inventory_maintenance"
		}
	} else if !plan.schedule.NextSourceDue.IsZero() {
		plan.due = plan.schedule.NextSourceDue
		if plan.due.After(now) {
			plan.waitReason = "job_retry"
		}
		if reason != "" && !allowed.Before(plan.due) {
			plan.due, plan.waitReason = allowed, reason
		}
		if plan.due.Before(now) {
			plan.due = now
		}
	}
	if len(genericKinds) > 0 {
		due, err := w.NextJobDue(ctx, genericKinds)
		if err != nil {
			return fairInventoryPlan{}, err
		}
		if !due.IsZero() && (plan.due.IsZero() || due.Before(plan.due) || (!plan.allowSource && plan.schedule.Turn == nil && !due.After(now))) {
			plan.due, plan.generic = due, true
			plan.waitReason = ""
			if due.After(now) {
				plan.waitReason = "job_retry"
			} else {
				plan.due = now
			}
		}
	}
	return plan, nil
}

func releaseDormantRootStreams(source *scannerHolder, schedule state.FairInventorySchedule) {
	if scanner := source.value.Load(); scanner != nil {
		for _, root := range schedule.DormantRoots {
			scanner.ReleaseRoot(string(root))
		}
	}
}
