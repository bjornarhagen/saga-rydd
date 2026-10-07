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

// This finite review opens existing hash storage only. It does not load current
// configuration or inventory, evaluate consent, or prepare source descriptors.
func hashRequestKeeperChoice(ctx context.Context, paths config.Paths, id string) (inventory.HashKeeperChoiceFreshRequestReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return inventory.HashKeeperChoiceFreshRequestReport{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage is required for --request-choice; no storage was initialized: %w", err)}
		}
		return inventory.HashKeeperChoiceFreshRequestReport{}, err
	}
	request, prepareErr := reader.PrepareKeeperChoiceFreshRequest(ctx, id)
	closeErr := reader.Close()
	if prepareErr != nil {
		if errors.Is(prepareErr, os.ErrNotExist) {
			prepareErr = missingHashError{fmt.Errorf("saved historical choice %s is unavailable: %w", id, prepareErr)}
		}
		return inventory.HashKeeperChoiceFreshRequestReport{}, prepareErr
	}
	if closeErr != nil {
		return inventory.HashKeeperChoiceFreshRequestReport{}, closeErr
	}
	report := request.Report()
	if err := ctx.Err(); err != nil {
		return inventory.HashKeeperChoiceFreshRequestReport{}, err
	}
	return report, nil
}

func printHashKeeperChoiceFreshRequest(out io.Writer, report inventory.HashKeeperChoiceFreshRequestReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: proposed fresh full-file read", "")
	printResultBanner(guard, "UNAPPROVED FRESH-READ REQUEST - NOTHING SAVED")
	fmt.Fprintf(guard, "Request: %s\nChoice: %s\nOriginal store: %s\nOriginal selection: %s\nInventory: %s\nManual root: %q\n", report.RequestID, report.ChoiceID, report.StoreID, report.SelectionID, report.InventoryID, string(report.SourceLocator.RootPathBytes))
	printWrapped(guard, "This request preserves the exact saved keeper and ordered copies. It proposes reading every selected file from the beginning in a separate future job. No job or read consent was created, and current files were not checked.", "")
	for _, target := range report.Targets {
		role := "Selected copy for review"
		if target.Role == "keeper" {
			role = "Selected keeper for review"
		}
		printHashPreviewMember(guard, role, target.Observation)
		printField(guard, "Frozen root", fmt.Sprintf("%q", string(target.Target.Root.PathBytes)))
		printField(guard, "Frozen ancestor records", len(target.Target.Ancestors))
	}
	fmt.Fprintln(guard)
	printResultBanner(guard, "HISTORICAL CONTEXT FROM THE SAVED CHOICE")
	printField(guard, "Choice saved at", report.HistoricalChoice.Record.CreatedAt.UTC().Format(time.RFC3339Nano))
	printWrapped(guard, "The following charges and consent were saved with the original choice. They are historical context, and give no permission or allowance for a fresh read.", "")
	printHashBudget(guard, report.HistoricalChoice.Record.Evidence.Budget)
	printHashReadConsent(guard, report.HistoricalChoice.Record.Evidence.ReadConsent)
	printField(guard, "New read approval", "Unavailable")
	printField(guard, "Reclaimable space", "Unknown")
	printWrapped(guard, "Only existing saved hash records were read. No source files, inventory or configuration were opened. No request was saved, no previous approval was renewed, and no hashing charges were changed. The request ID is not a job, read approval or cleanup token.", "")
	if guard.err != nil {
		return fmt.Errorf("write choice %s fresh-read request; no request was saved: %w", report.ChoiceID, guard.err)
	}
	return nil
}
