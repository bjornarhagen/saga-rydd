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

func hashCheckKeeperChoiceMetadata(ctx context.Context, paths config.Paths, id string) (inventory.HashKeeperChoiceMetadataReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage is required for --check-choice; no storage was initialized: %w", err)}
		}
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	request, prepareErr := reader.PrepareKeeperChoiceMetadata(ctx, id)
	closeErr := reader.Close()
	if prepareErr != nil {
		if errors.Is(prepareErr, os.ErrNotExist) {
			prepareErr = missingHashError{fmt.Errorf("saved historical choice %s is unavailable: %w", id, prepareErr)}
		}
		return inventory.HashKeeperChoiceMetadataReport{}, prepareErr
	}
	if closeErr != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, closeErr
	}
	if err := ctx.Err(); err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	// The preparation reader is closed before guarded configuration access.
	// All original proposal identities protect configuration, including files
	// with no keeper/copy role. The request retains its own immutable scope.
	cfg, err := loadHashRunConfig(ctx, paths, request.Proposal())
	if err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	locator := request.SourceLocator()
	scanner, err := inventory.New([]string{string(locator.RootPathBytes)}, cfg.Excludes, []string{paths.StateDir, paths.ConfigFile})
	if err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	defer scanner.Close()
	// Check owns the exact derived inventory reader. No writer, replacement
	// inventory, scan, recovery or consent evaluation is part of this command.
	report, err := request.Check(ctx, scanner)
	if err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return inventory.HashKeeperChoiceMetadataReport{}, err
	}
	return report, nil
}

func printHashKeeperChoiceMetadata(out io.Writer, report inventory.HashKeeperChoiceMetadataReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved-choice metadata screen", "")
	if report.Status == "metadata_matches" {
		printResultBanner(guard, "SELECTED METADATA MATCHED AT INDIVIDUAL CHECK TIMES")
	} else {
		printResultBanner(guard, "METADATA SCREEN BLOCKED - REVIEW REQUIRED")
	}
	fmt.Fprintf(guard, "Choice: %s\nStore: %s\nSelection: %s\nInventory: %s\n", report.ChoiceID, report.StoreID, report.SelectionID, report.InventoryID)
	inventoryStatus := "Not checked"
	switch report.InventoryStatus {
	case "matches_saved_inventory":
		inventoryStatus = "Matches frozen evidence"
	case "changed":
		inventoryStatus = "Changed"
	case "unavailable":
		inventoryStatus = "Unavailable"
	}
	printField(guard, "Saved inventory", inventoryStatus)
	printField(guard, "Screen finished", report.CheckedAt.UTC().Format(time.RFC3339Nano))
	printWrapped(guard, report.Message, "")
	for _, target := range report.Targets {
		role := "Selected copy for review"
		if target.Role == "keeper" {
			role = "Selected keeper for review"
		}
		printHashPreviewMember(guard, role, target.Observation)
		status := "Blocked"
		if target.Status == "metadata_matches" {
			status = "Metadata matched"
		}
		printField(guard, "Metadata result", status)
		checkedAt := "Not checked"
		if !target.MetadataCheckedAt.IsZero() {
			checkedAt = target.MetadataCheckedAt.UTC().Format(time.RFC3339Nano)
		}
		printField(guard, "Metadata check time", checkedAt)
		if target.Code != "" {
			printField(guard, "Reason code", target.Code)
		}
		printWrapped(guard, target.Message, "  ")
	}
	fmt.Fprintln(guard)
	printField(guard, "Selected file-body bytes requested", fmt.Sprintf("%d bytes", report.SelectedContentRequestedBytes))
	printField(guard, "Selected file-body bytes read", fmt.Sprintf("%d bytes", report.SelectedContentReadBytes))
	printField(guard, "Approval", "Unavailable")
	printField(guard, "Reclaimable space", "Unknown")
	printWrapped(guard, "Metadata checks apply to one selected file at a time. Selected file bodies were not read. Matching metadata does not prove current content equality or permit another content read or cleanup. Configuration bytes and saved SQLite pages can be read by this command. No screen result was saved; source contents and saved records were not changed.", "")
	if guard.err != nil {
		return fmt.Errorf("write choice %s metadata screen; no screen result was saved: %w", report.ChoiceID, guard.err)
	}
	return nil
}
