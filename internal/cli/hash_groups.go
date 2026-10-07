package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func printHashGroups(out io.Writer, r inventory.HashGroupsReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: matching historical hashes", "")
	printResultBanner(guard, "HISTORICAL HASH MATCHES - CURRENT FILES NOT CHECKED")
	printWrapped(guard, "These groups contain saved full SHA-256 observations with the same logical file size and digest. The observations need not be simultaneous. They do not prove current duplicate files or safe cleanup.", "")
	fmt.Fprintf(guard, "Store: %s\n", r.StoreID)
	if r.SelectionID != "" {
		fmt.Fprintf(guard, "Selection: %s\n", r.SelectionID)
	}
	printResultBanner(guard, "WHOLE SAVED SELECTION")
	printField(guard, "Selected work", r.SelectedWork)
	printField(guard, "Completed observations", r.CompletedObservations)
	printField(guard, "Without a full observation", r.UnfinishedWork)
	printField(guard, "Completed without a match", r.UnmatchedCompletedObservations)
	printField(guard, "Matching groups", len(r.Groups))
	if r.SelectionID == "" {
		printWrapped(guard, "No selection is saved in this hash store. This command does not create or start hashing work.", "")
	} else if len(r.Groups) == 0 {
		printWrapped(guard, "No matching completed observations are saved in this selection. This does not prove that there are no duplicate files.", "")
	}
	printWrapped(guard, "Coverage is limited to this saved selection. Work without a full observation includes pending, running and invalidated records. Saved running state does not prove a process is active.", "")
	for i, group := range r.Groups {
		fmt.Fprintf(guard, "\nGroup %d\n", i+1)
		printField(guard, "Logical size per file", humanBytes(group.LogicalBytes))
		printField(guard, "Historical SHA-256", group.SHA256)
		printField(guard, "Recorded paths", len(group.Members))
		printField(guard, "Saved identities", group.SavedIdentities)
		printField(guard, "Repeated paths in group", group.RepeatedSavedPaths)
		printField(guard, "Conflicting identities", group.ConflictingSavedIdentities)
		if group.SavedIdentities == 1 {
			printWrapped(guard, "These paths share one recorded identity. This does not establish current hardlinks or independent copies.", "")
		}
		for _, member := range group.Members {
			fmt.Fprintf(guard, "\n  Work %s; saved file %d; saved root %d\n  %q\n", member.WorkID, member.FileID, member.RootID, string(member.PathBytes))
			printField(guard, "Historical check time", member.CheckedAt.UTC().Format(time.RFC3339Nano))
			printField(guard, "Saved device / inode", member.SavedDevice+" / "+member.SavedInode)
			if member.RepeatedSavedIdentity {
				printWrapped(guard, "This recorded identity appears in another selected path.", "  ")
			}
			if member.SavedIdentityConflict {
				printWrapped(guard, "Conflicting saved evidence makes this identity uncertain.", "  ")
			}
		}
	}
	printWrapped(guard, "Member identity flags cover the whole selection, including paths outside the displayed group. Saved identities do not establish inode continuity or independent storage. File sizes are per file; no keeper or reclaimable-space estimate is provided.", "")
	printHashBudget(guard, r.Budget)
	printHashReadConsent(guard, r.ReadConsent)
	printWrapped(guard, "This command reads saved hash records only. It does not check current files, recover or resume work, or evaluate current read permission. No source files or saved records were changed.", "")
	if guard.err != nil {
		return fmt.Errorf("write historical hash groups: %w", guard.err)
	}
	return nil
}
