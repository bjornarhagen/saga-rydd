package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func printHashKeeperPreview(out io.Writer, r inventory.HashKeeperPreview) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: possible keeper and copies", "")
	printResultBanner(guard, "HISTORICAL KEEPER PREVIEW - CURRENT FILES NOT CHECKED")
	printWrapped(guard, "These possible roles use only the saved full hashes you selected. The observations need not be simultaneous. They do not prove current duplicate files or safe cleanup. No decision was saved.", "")
	fmt.Fprintf(guard, "Store: %s\nSelection: %s\n", r.StoreID, r.SelectionID)
	printField(guard, "Logical size per file", humanBytes(r.LogicalBytes))
	printField(guard, "Historical SHA-256", r.SHA256)
	printHashPreviewMember(guard, "Possible keeper", r.Keeper)
	for _, member := range r.Copies {
		printHashPreviewMember(guard, "Possible copy for review", member)
	}
	printWrapped(guard, "Only these explicitly selected paths have possible roles. Other matching paths are not selected. Repeated or conflicting saved identities are refused, but saved identities do not prove inode continuity or independent storage.", "")
	printResultBanner(guard, "WHOLE SAVED SELECTION")
	printField(guard, "Selected work", r.SelectedWork)
	printField(guard, "Completed observations", r.CompletedObservations)
	printField(guard, "Without a full observation", r.UnfinishedWork)
	printField(guard, "Completed without a match", r.UnmatchedCompletedObservations)
	printWrapped(guard, "Coverage and charges include the whole saved selection. Work without a full observation includes pending, running and invalidated records; saved running state does not prove a process is active.", "")
	printField(guard, "Approval", "Unavailable")
	printField(guard, "Reclaimable space", "Unknown")
	printHashBudget(guard, r.Budget)
	printHashReadConsent(guard, r.ReadConsent)
	printWrapped(guard, "This command reads saved hash records only. It does not check current files, recover or resume work, or evaluate current read permission. No source files or saved records were changed.", "")
	if guard.err != nil {
		return fmt.Errorf("write historical keeper preview: %w", guard.err)
	}
	return nil
}

func printHashPreviewMember(out io.Writer, role string, member inventory.SavedHashPreviewMember) {
	fmt.Fprintf(out, "\n%s\n  Work %s; saved file %d; saved root %d\n  %q\n", role, member.WorkID, member.FileID, member.RootID, string(member.PathBytes))
	printField(out, "Observation sequence", member.Sequence)
	printField(out, "Historical check time", member.CheckedAt.UTC().Format(time.RFC3339Nano))
	printField(out, "Saved device / inode", member.SavedDevice+" / "+member.SavedInode)
}
