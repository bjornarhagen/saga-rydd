package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerPower(out io.Writer, power *worker.PowerPolicySnapshot) {
	if power == nil {
		return
	}
	fmt.Fprintf(out, "Source power policy enabled: %t\nLast source power decision: %s\n", power.PolicyEnabled, power.LastDecisionStatus)
	if power.LastDecisionReason != "" {
		fmt.Fprintf(out, "Last decision reason: %s\n", power.LastDecisionReason)
	}
	if power.LastDecisionAt != nil {
		fmt.Fprintf(out, "Decision recorded at: %s\n", power.LastDecisionAt.UTC().Format(time.RFC3339Nano))
	}
	if power.LastSourceBackoff != nil {
		fmt.Fprintf(out, "Source deferred by that decision: %t\n", *power.LastSourceBackoff)
	}
	if power.TicketID != nil {
		fmt.Fprintf(out, "Power check reference: %d\nSample result: %s\n", *power.TicketID, power.SampleStatus)
		if power.CallbackReturned != nil {
			fmt.Fprintf(out, "Power check finished: %t\n", *power.CallbackReturned)
		}
		if power.SampleLaunchAt != nil {
			fmt.Fprintf(out, "Power check started at: %s\n", power.SampleLaunchAt.UTC().Format(time.RFC3339Nano))
		}
		if power.SampleCompletedAt != nil {
			fmt.Fprintf(out, "Power check finished at: %s\n", power.SampleCompletedAt.UTC().Format(time.RFC3339Nano))
		}
		if power.SampleReason != "" {
			fmt.Fprintf(out, "Sample reason: %s\n", power.SampleReason)
		}
	}
	if observation := power.Observation; observation != nil {
		fmt.Fprintf(out, "Observation profile: %s\n", observation.Profile)
		if observation.SystemBatteryDischargingObserved != nil && *observation.SystemBatteryDischargingObserved {
			fmt.Fprintln(out, "System-battery discharge: OBSERVED IN THIS SAMPLE")
		} else {
			fmt.Fprintln(out, "System-battery discharge: UNKNOWN")
		}
	} else {
		fmt.Fprintln(out, "System-battery discharge: NOT RECORDED")
	}
	printWrapped(out, "These are cached sample results and the last source admission decision. Status does not start a probe or evaluate current battery state. A power check can finish before the next source decision.", "")
	printWrapped(out, "Power observations can delay source work. Eligible saved-data maintenance keeps its existing admission gates. Unknown or stalled observations use fixed pacing; an unfinished power check remains the only active check.", "")
	printWrapped(out, "External power, physical power use and sleep are not verified. Power calls are separate from scanner metadata allowances; this is not a power quota.", "")
}
