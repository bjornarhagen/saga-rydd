package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func printCPUFeedback(out io.Writer, feedback *state.CPUFeedbackState) {
	if feedback == nil {
		return
	}
	fmt.Fprintf(out, "Saved CPU feedback: %s\n", feedback.Status)
	if record := feedback.Window; record != nil {
		fmt.Fprintf(out, "Saved CPU window: %s\nWorker instance: %s\nSaved turn: %s; root ID: %d\n", record.Token, record.Instance, record.Kind, record.RootID)
		if record.CPUTimeNS != nil && record.ElapsedNS != nil {
			fmt.Fprintf(out, "Saved process CPU: %s; elapsed: %s; added wait: %s; capped: %t\n", time.Duration(*record.CPUTimeNS), time.Duration(*record.ElapsedNS), time.Duration(record.BackoffNS), record.BackoffCapped)
		} else {
			fmt.Fprintln(out, "Saved CPU measurement: UNKNOWN")
		}
		if feedback.Status == "pending" {
			printWrapped(out, "This window is not settled. Writer recovery is required before another tracked turn.", "")
		}
		if feedback.Status == "recovered_unknown" {
			printWrapped(out, fmt.Sprintf("Interrupted window: %s conservative delay from its first saved recovery. This delay is not a measured CPU charge.", time.Duration(feedback.UnknownRecoveryBackoffNS)), "")
		}
	}
	if feedback.NextAllowedAt != nil {
		fmt.Fprintf(out, "Saved CPU not-before time: %s\n", feedback.NextAllowedAt.UTC().Format(time.RFC3339Nano))
	}
	if feedback.Window != nil {
		fmt.Fprintf(out, "Saved unknown windows: completed=%d; recovered=%d; count saturated: %t\n", feedback.CompletedUnknownWindows, feedback.RecoveredUnknownWindows, feedback.UnknownCountSaturated)
	} else {
		fmt.Fprintln(out, "Saved window counts: NOT RECORDED")
	}
	printWrapped(out, "Saved feedback covers experimental source and saved-inventory maintenance turns. Earlier usage, interrupted measurements and work outside these windows are not measured.", "")
	printWrapped(out, "Setup work before marker publication and the final feedback write are outside durable crash coverage. Saved views do not recover windows or evaluate current permission.", "")
	printWrapped(out, "This is CPU pacing, not a daily or hourly CPU quota. Battery and physical sleep behavior are not evaluated; existing fixed pacing remains.", "")
}
