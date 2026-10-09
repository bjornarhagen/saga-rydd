package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerAPIPacing(out io.Writer, pacing *worker.APIPacingSnapshot) {
	if pacing == nil {
		return
	}
	capacity := "READY"
	if !pacing.CapacityReady {
		capacity = "SOURCE DEFERRED"
	}
	wait := "NO"
	if pacing.Waiting {
		wait = "YES"
	}
	fmt.Fprintf(out, "Scanner API pacing: %d attempts/s\nWork-window capacity: %s\nPacing wait at last status sample: %s\nAccumulated pacing wait: %s\n", pacing.RatePerSecond, capacity, wait, time.Duration(min(pacing.WaitNS, uint64(1<<63-1))))
	if pacing.WaitNS > uint64(1<<63-1) {
		fmt.Fprintln(out, "Accumulated wait exceeds the displayed duration range.")
	}
	printWrapped(out, "This is process-local spacing for all six source-scanner API kinds. It is separate from entry pacing. Status does not inspect source folders. An unsupported path or insufficient work-window capacity defers source work for this worker run; eligible saved-data work and controls remain available.", "")
	printWrapped(out, "API attempts can perform multiple system calls. This is not a global, byte or physical I/O limit. Manual scans, hash reads and database work are outside this pacing scope. Entered filesystem calls remain cooperative and can outlast cancellation.", "")
}
