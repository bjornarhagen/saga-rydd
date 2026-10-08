package cli

import (
	"fmt"
	"io"
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
	role := "Selected copy for review"
	if member.Role == "keeper" {
		role = "Selected keeper for review"
	}
	fmt.Fprintf(out, "\n%s\n  Fresh work %d; historical work %s; saved file %d; saved root %d\n  %q\n", role, member.Ordinal, member.HistoricalWorkID, member.FileID, member.RootID, string(member.PathBytes))
	if member.Observation == nil {
		printField(out, "Fresh observation", "NOT RECORDED")
		return
	}
	observation := member.Observation
	printField(out, "Saved fresh state", observation.Status)
	printField(out, "Saved logical size", fmt.Sprintf("%d bytes", observation.LogicalBytes))
	printField(out, "Saved fresh prefix", fmt.Sprintf("%d bytes", observation.DurableOffset))
	printField(out, "Fresh sequence", observation.Sequence)
	if !observation.CheckedAt.IsZero() {
		printField(out, "Observed at", observation.CheckedAt.UTC().Format(time.RFC3339Nano))
	}
	if observation.SHA256 != "" {
		fmt.Fprintf(out, "Historical fresh SHA-256: %s\n", observation.SHA256)
	}
	if observation.Code != "" {
		printField(out, "Saved work code", observation.Code)
	}
	printHashAttempt(out, observation.LatestAttempt)
}

func printHashFreshComparisonSummary(out io.Writer, report inventory.HashFreshJobComparisonReport) {
	printField(out, "Comparison", hashFreshComparisonLabel(report.Status))
	printField(out, "Copy results", fmt.Sprintf("%d matching, %d differing, %d incomplete, %d blocked", report.MatchingCopies, report.DifferingCopies, report.IncompleteCopies, report.BlockedCopies))
}

func printHashFreshComparisonMembers(out io.Writer, report inventory.HashFreshJobComparisonReport) {
	printHashFreshComparisonMember(out, report.Keeper)
	for _, pair := range report.Copies {
		printHashFreshComparisonMember(out, pair.Copy)
		printField(out, "Comparison with keeper", hashFreshComparisonLabel(pair.Relation))
	}
}

func printHashFreshComparison(out io.Writer, report inventory.HashFreshJobComparisonReport) {
	printResultBanner(out, "SAVED FRESH KEEPER/COPY COMPARISON - HISTORICAL")
	printHashFreshComparisonSummary(out, report)
	printHashFreshComparisonMembers(out, report)
	printHashFreshComparisonQualification(out)
}

func printHashFreshComparisonQualification(out io.Writer) {
	printWrapped(out, "These comparisons use saved observations made at separate times. They prove no current equality or safe cleanup. Incomplete or blocked work supplies no inferred hash. Saved running state does not prove a process is active. This saved view starts no read, evaluates no current read permission and recovers no work. It grants no cleanup permission. Reclaimable space remains unknown.", "")
}
