package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// The saved rows are a conservative preflight only. Queue order can change
// before the writer opens; the core rechecks its actual selection before it
// reserves any new bytes. No saved row is an authorization token.
func originalHashReadMinimum(work []inventory.SavedHashWork) int64 {
	minimum, found := int64(64), false
	for _, member := range work {
		if member.Status != "pending" && member.Status != "running" {
			continue
		}
		remaining := member.LogicalBytes - member.DurableOffset
		minimum = min(minimum, remaining)
		found = true
	}
	if !found {
		return 0
	}
	return minimum
}

func freshHashReadMinimum(job inventory.SavedFreshJob) int64 {
	if job.Progress != nil {
		minimum, found := int64(64), false
		for _, member := range job.Progress {
			if member.Status != "pending" && member.Status != "running" {
				continue
			}
			minimum = min(minimum, member.LogicalBytes-member.DurableOffset)
			found = true
		}
		if !found {
			return 0
		}
		return minimum
	}
	minimum, found := int64(64), false
	for _, member := range job.Work {
		if member.Status != "pending" {
			continue
		}
		if member.Ordinal < 1 || member.Ordinal > len(job.Record.Request.Targets) {
			return -1
		}
		remaining := job.Record.Request.Targets[member.Ordinal-1].Target.File.Size - member.CheckedOffset
		minimum = min(minimum, remaining)
		found = true
	}
	if !found {
		return 0
	}
	return minimum
}

func checkCLIHashReadPacing(ctx context.Context, rate, minimum int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining = min(remaining, max(time.Until(deadline), time.Duration(0)))
	}
	if err := inventory.CheckHashReadPacingCapacity(rate, minimum, remaining); err != nil {
		return fmt.Errorf("configured content-read pacing refused this step before source setup or a new byte reservation; inspect saved progress before another explicit run: %w", err)
	}
	return nil
}

func printHashReadPacing(out io.Writer, pacing *inventory.HashReadPacingObservation) {
	if pacing == nil {
		return
	}
	printField(out, "Configured read rate", fmt.Sprintf("%d requested bytes per second", pacing.RequestedBytesPerSecond))
	wait := "NOT RECORDED"
	if pacing.ObservedWait != nil {
		wait = pacing.ObservedWait.String()
	}
	printField(out, "Read pacing wait", wait)
	printField(out, "Stopped at a pacing limit", pacing.Yielded)
	printWrapped(out, "Each body request waits for its requested bytes before reading, including the first, short and failed requests. This ceiling applies to this explicit step only. It is not a global or physical I/O limit. Charged reservations are not refunded, and no automatic continuation occurs.", "")
}

func printHashPacingZeroProgress(out io.Writer, code string) {
	if code == "pacing_window_exhausted" {
		printWrapped(out, "The pacing window ended before another durable prefix was saved. The full byte reservation stays charged. Inspect saved progress before deciding whether to run another explicit step.", "")
	}
}
