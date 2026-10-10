package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerInventoryState(out io.Writer, policy *worker.InventoryStatePolicySnapshot) {
	if policy == nil {
		return
	}
	fmt.Fprintf(out, "Last inventory-state decision: %s\nInventory source threshold: %d bytes\n", inventoryStateDecisionText(policy.LastDecisionStatus), policy.Limit)
	if policy.SamplingState != "" {
		fmt.Fprintf(out, "Recorded state-check status: %s\n", inventoryStateSamplingText(policy.SamplingState))
	}
	if policy.LastDecisionReason != "" {
		fmt.Fprintln(out, inventoryStateReasonText(policy.LastDecisionReason))
	}
	if policy.LastAttemptStartedAt != nil {
		fmt.Fprintf(out, "State check started at: %s\nState check boundary: %s\n", policy.LastAttemptStartedAt.UTC().Format(time.RFC3339Nano), inventoryStatePhaseText(policy.LastAttemptPhase))
	}
	if policy.LastAttemptFinishedAt != nil {
		fmt.Fprintf(out, "State check finished at: %s\n", policy.LastAttemptFinishedAt.UTC().Format(time.RFC3339Nano))
	}
	if policy.LastSourceBackoff != nil {
		fmt.Fprintf(out, "Source deferred by that decision: %t\n", *policy.LastSourceBackoff)
	}
	if report := policy.Observation; report != nil {
		fmt.Fprintf(out, "Cached state-length sample: %s\n", inventoryStateSampleText(report.Status))
		for _, item := range []struct {
			label string
			size  *int64
		}{{"Database logical length", report.DatabaseBytes}, {"WAL logical length", report.WALBytes}, {"Combined logical length", report.TotalBytes}} {
			if item.size == nil {
				fmt.Fprintf(out, "%s: UNKNOWN\n", item.label)
			} else {
				fmt.Fprintf(out, "%s: %d bytes\n", item.label, *item.size)
			}
		}
	} else {
		fmt.Fprintln(out, "State-length sample: NOT RECORDED")
	}
	if policy.RetryAfterAt != nil {
		fmt.Fprintf(out, "Recorded retry time (wall clock): %s\n", policy.RetryAfterAt.UTC().Format(time.RFC3339Nano))
	}
	printWrapped(out, "This is cached source admission evidence. Status does not sample state files. A refused observation keeps its original five-minute retry in both independent clock domains. A pending or occupied slot supplies no size decision; controls and eligible saved-data maintenance keep their existing gates.", "")
	printWrapped(out, "The threshold covers only the configured inventory database and WAL logical lengths. Startup bookkeeping and admitted work can exceed it. The selected metadata callback runs outside the control loop and can outlast cancellation. Other entered filesystem or SQLite work can still delay controls.", "")
	printWrapped(out, "Pending work and owner history are retained. No records are purged and no shrink or automatic resumption is guaranteed. Physical allocation and other stores are not measured. Retry timing is process-local. Starting another Run or changing stores does not reset failed retry or release an unfinished callback. A new process requires a new observation.", "")
}

func inventoryStateSamplingText(status string) string {
	switch status {
	case "not_requested":
		return "No observation requested"
	case "pending":
		return "Observation pending"
	case "returned":
		return "State check returned; see the recorded decision"
	case "refused":
		return "Observation refused"
	case "foreign_slot":
		return "Waiting for an earlier state check to finish"
	default:
		return "Unknown"
	}
}

func inventoryStateDecisionText(status string) string {
	switch status {
	case "not_evaluated":
		return "Not checked"
	case "source_admitted":
		return "Below the threshold in this sample"
	case "source_deferred":
		return "Source work deferred"
	default:
		return "Unknown"
	}
}

func inventoryStateSampleText(status string) string {
	switch status {
	case "below_limit":
		return "Below the threshold"
	case "limit_reached":
		return "At or above the threshold"
	case "unavailable":
		return "Unavailable"
	default:
		return "Unknown"
	}
}

func inventoryStatePhaseText(phase string) string {
	switch phase {
	case "before_dispatch":
		return "Before dispatch reservation"
	case "after_receipt":
		return "After dispatch reservation"
	default:
		return "Not recorded"
	}
}

func inventoryStateReasonText(reason string) string {
	switch reason {
	case "inventory_state_pending":
		return "The observation is pending. No source permission follows."
	case "inventory_state_process_slot":
		return "An earlier state check is still finishing. This worker cannot use its result."
	case "inventory_state_limit":
		return "The combined database and WAL length reached the threshold."
	case "inventory_state_unavailable":
		return "State lengths were unavailable."
	case "inventory_state_changed":
		return "State files changed during the check."
	case "inventory_state_overflow":
		return "The combined lengths could not be represented safely."
	case "inventory_state_timeout":
		return "The state check reached its time limit."
	case "inventory_state_clock":
		return "The check's clocks moved backwards."
	case "inventory_state_canceled":
		return "The state check was canceled."
	case "inventory_state_invalid":
		return "The check did not return usable evidence."
	default:
		return "The check did not return a supported result."
	}
}
