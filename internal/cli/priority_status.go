package cli

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerPriority(out io.Writer, observation *worker.ThreadPriorityObservation) {
	if observation == nil {
		return
	}
	fmt.Fprintf(out, "Source-thread scheduling observation: %s at %s\n", observation.Platform, observation.CheckedAt.Format(time.RFC3339Nano))
	fmt.Fprintf(out, "  %-22s %-20s %s\n", "Setting", "Request reply", "Observed value")
	for _, row := range []struct {
		name  string
		value *worker.PrioritySetting
	}{{"CPU nice", observation.CPU}, {"I/O priority (raw)", observation.IO}, {"Background (0/1)", observation.Background}} {
		if row.value == nil {
			continue
		}
		reply := "Not requested"
		if row.value.Attempted {
			reply = "Unknown"
			if row.value.Accepted != nil {
				if *row.value.Accepted {
					reply = "Accepted"
				} else {
					reply = "Refused"
				}
			}
		}
		value := "Unknown"
		if row.value.Observed != nil {
			value = strconv.Itoa(*row.value.Observed)
		}
		fmt.Fprintf(out, "  %-22s %-20s %s\n", row.name, reply, value)
	}
	fmt.Fprintln(out, "These are sequential observations for one experimental source-handler thread.\nThe changed thread stays locked until the handler exits. Newly created threads\ncan inherit settings. Other threads, children and current process-wide settings\nare unverified. Request acceptance does not prove effective scheduling, physical\nI/O or power savings. Independent cadence and resource budgets still apply.")
}
