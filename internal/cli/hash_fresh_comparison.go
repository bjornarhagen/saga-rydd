package cli

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshComparisonLabel(status string) string {
	switch status {
	case "historical_hashes_match":
		return "Historical hashes match"
	case "historical_hashes_differ":
		return "Historical hashes differ"
	case "blocked":
		return "Blocked"
	default:
		return "Incomplete"
	}
}

func printHashFreshComparisonMember(out io.Writer, member inventory.HashFreshJobComparisonMember) {
	fmt.Fprintf(out, "Fresh work %d (historical work %s, selected %s)\n  %q\n", member.Ordinal, member.HistoricalWorkID, member.Role, string(member.PathBytes))
	if member.Observation == nil {
		printField(out, "Fresh observation", "Not started")
		return
	}
	observation := member.Observation
	printField(out, "Saved fresh state", observation.Status)
	printField(out, "Fresh sequence", observation.Sequence)
	if !observation.CheckedAt.IsZero() {
		printField(out, "Observed at", observation.CheckedAt.UTC().Format(time.RFC3339Nano))
	}
	if observation.LatestAttempt != nil {
		printField(out, "Latest fresh attempt", observation.LatestAttempt.Status)
	}
	if observation.Code != "" {
		printField(out, "Saved work code", observation.Code)
	}
}

func printHashFreshComparison(out io.Writer, report inventory.HashFreshJobComparisonReport) {
	printResultBanner(out, "SAVED FRESH KEEPER/COPY COMPARISON - HISTORICAL")
	printField(out, "Comparison", hashFreshComparisonLabel(report.Status))
	printField(out, "Copy results", fmt.Sprintf("%d matching, %d differing, %d incomplete, %d blocked", report.MatchingCopies, report.DifferingCopies, report.IncompleteCopies, report.BlockedCopies))
	printHashFreshComparisonMember(out, report.Keeper)
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "Copy work\tHistorical comparison\tSaved copy state")
	for _, pair := range report.Copies {
		state := "Not started"
		if pair.Copy.Observation != nil {
			state = pair.Copy.Observation.Status
		}
		fmt.Fprintf(table, "%d\t%s\t%s\n", pair.Copy.Ordinal, hashFreshComparisonLabel(pair.Relation), state)
	}
	_ = table.Flush()
	for _, pair := range report.Copies {
		printHashFreshComparisonMember(out, pair.Copy)
	}
	printWrapped(out, "These comparisons use saved observations made at separate times. A matching historical hash proves no current equality or safe cleanup. Incomplete or blocked work supplies no inferred hash. Viewing this report starts no read, evaluates no permission and recovers no work. Reclaimable space remains unknown.", "")
}
