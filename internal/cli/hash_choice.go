package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func validateHashChoiceCLIRequest(selection, keeper string, copies []string) error {
	if !validHashSelectionID(selection) || !validSavedHashWorkID(keeper) || len(copies) < 1 || len(copies) >= inventory.FileSampleTargetLimit {
		return usageError{errors.New("hash --save-choice requires one full lowercase selection ID, --keeper WORK_ID and 1–19 distinct canonical copy IDs from 1 to 20; options must precede IDs")}
	}
	seen := map[string]bool{keeper: true}
	for _, id := range copies {
		if !validSavedHashWorkID(id) || seen[id] {
			return usageError{errors.New("keeper and copy work IDs must be distinct canonical saved work IDs from 1 to 20")}
		}
		seen[id] = true
	}
	return nil
}

func hashSaveKeeperChoice(ctx context.Context, paths config.Paths, selection, keeper string, copies []string) (inventory.SavedHashKeeperChoice, error) {
	preview, err := loadHashReviewPreview(ctx, paths, selection, keeper, copies)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage is required for --save-choice; no storage was initialized: %w", err)}
		}
		return inventory.SavedHashKeeperChoice{}, err
	}
	return saveHashKeeperChoice(ctx, paths, preview)
}

// A finite command captures its expected preview first. Guided review supplies
// the exact preview already displayed. The core rechecks selected evidence in
// the publishing transaction; reusable work ordinals never replace it.
func saveHashKeeperChoice(ctx context.Context, paths config.Paths, expected inventory.HashKeeperPreview) (inventory.SavedHashKeeperChoice, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return inventory.SavedHashKeeperChoice{}, err
	}
	writer, err := inventory.OpenHashChoiceWriter(opCtx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage became unavailable; no replacement was initialized: %w", err)}
		}
		return inventory.SavedHashKeeperChoice{}, err
	}
	saved, saveErr := writer.SaveKeeperChoice(opCtx, expected)
	closeErr := writer.Close()
	if saveErr != nil {
		if saved.ID != "" {
			return saved, fmt.Errorf("choice publication or its reply is uncertain; inspect hashes --choice %s before retrying: %w", saved.ID, saveErr)
		}
		return saved, saveErr
	}
	if closeErr != nil {
		return saved, fmt.Errorf("choice %s was saved, but closing its writer failed; reopen with hashes --choice %s: %w", saved.ID, saved.ID, closeErr)
	}
	if err := opCtx.Err(); err != nil {
		return saved, fmt.Errorf("choice %s was saved, but its reply was canceled; reopen with hashes --choice %s: %w", saved.ID, saved.ID, err)
	}
	return saved, nil
}

func printHashKeeperChoice(out io.Writer, saved inventory.SavedHashKeeperChoice) error {
	guard := &reviewOutput{writer: out}
	r := saved.Record.Evidence
	printWrapped(guard, "Saga — Rydd: saved historical keeper and copies", "")
	printResultBanner(guard, "HISTORICAL CHOICE SAVED - CLEANUP UNAVAILABLE")
	fmt.Fprintf(guard, "Saved choice: %s\nStore: %s\nSelection: %s\nInventory: %s\n", saved.ID, r.StoreID, r.SelectionID, r.InventoryID)
	printField(guard, "Saved at", saved.Record.CreatedAt.UTC().Format(time.RFC3339Nano))
	printField(guard, "Choice status", saved.Record.Status)
	printWrapped(guard, "These roles preserve the exact historical observations you selected. Current files have not been checked, and observations need not be simultaneous. This choice is unapproved; it is not a keep policy, content-read consent or cleanup permission.", "")
	printField(guard, "Logical size per file", humanBytes(r.LogicalBytes))
	printField(guard, "Historical SHA-256", r.SHA256)
	printHashPreviewMember(guard, "Selected keeper for review", r.Keeper)
	for _, member := range r.Copies {
		printHashPreviewMember(guard, "Selected copy for review", member)
	}
	printWrapped(guard, "Only these explicitly selected paths have saved roles. Other matching paths are not selected. Saved identities do not prove current hardlinks, inode continuity or independent storage. Saving this choice does not hide paths from later reports.", "")
	printResultBanner(guard, "WHOLE-SELECTION CONTEXT SAVED WITH THIS CHOICE")
	printField(guard, "Selected work", r.SelectedWork)
	printField(guard, "Completed observations", r.CompletedObservations)
	printField(guard, "Without a full observation", r.UnfinishedWork)
	printField(guard, "Completed without a match", r.UnmatchedCompletedObservations)
	printWrapped(guard, "Coverage, charges and read-consent records below are historical context from when this choice was first saved. They do not describe later hashing work or evaluate current read permission.", "")
	printHashBudget(guard, r.Budget)
	printHashReadConsent(guard, r.ReadConsent)
	printField(guard, "Approval", "Unavailable")
	printField(guard, "Reclaimable space", "Unknown")
	printWrapped(guard, "No source files were read, moved or deleted by this operation. No cleanup consent was recorded.", "")
	if guard.err != nil {
		return fmt.Errorf("choice %s is saved, but its output failed; reopen with hashes --choice %s: %w", saved.ID, saved.ID, guard.err)
	}
	return nil
}
