package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func printScannerMetadata(out io.Writer, budget *state.MetadataBudget) {
	if budget == nil {
		return
	}
	fmt.Fprintf(out, "Saved scanner API allowances: %s; daily cap: %d attempts\n", budget.Status, budget.Limit)
	if budget.TrackingStartedAt != nil {
		fmt.Fprintf(out, "Tracking started: %s; usage before tracking: %s\n", budget.TrackingStartedAt.UTC().Format(time.RFC3339Nano), budget.PreTrackingUsage)
	} else {
		fmt.Fprintln(out, "Tracking has not started or is unavailable. Earlier usage is unknown.")
	}
	if charges := budget.DayCharges; charges != nil {
		fmt.Fprintf(out, "Reserved for saved day (%s UTC): %d; observed: %d; unknown charge: %d; outstanding: %d; known unused charge: %d\n", budget.Day, charges.Reserved, charges.Observed, charges.UnknownReserved, charges.OutstandingReserved, charges.KnownUnusedReserved)
	} else {
		fmt.Fprintln(out, "Daily scanner charges: Unknown")
	}
	if charges := budget.TotalCharges; charges != nil {
		fmt.Fprintf(out, "Reserved since tracking began: %d; observed: %d; unknown charge: %d; outstanding: %d; known unused charge: %d\n", charges.Reserved, charges.Observed, charges.UnknownReserved, charges.OutstandingReserved, charges.KnownUnusedReserved)
	}
	if budget.Reason != "" {
		fmt.Fprintf(out, "Saved allowance wait: %s\n", budget.Reason)
	}
	if budget.NextAllowedAt != nil {
		fmt.Fprintf(out, "Saved next allowance time: %s\n", budget.NextAllowedAt.UTC().Format(time.RFC3339Nano))
	}
	fmt.Fprintln(out, "These are full charged reservations for experimental background scanner APIs. Unused allowances are not refunded. Unknown charges include recovered interruptions; outstanding usage has not been settled. Counts exclude manual scans, explicit reads, configuration, SQLite and runtime bookkeeping. They do not measure syscalls or physical I/O. Saved views do not recover work or grant source access.")
}
