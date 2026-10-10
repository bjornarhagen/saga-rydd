package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// A failed view retains the requested canonical ID without claiming that its
// record exists or that a partial result was successfully returned.
func hashChoiceSizesViewError(id string, err error) error {
	return fmt.Errorf("requested historical choice %s could not be shown; inspect hashes --choice %s: %w", id, id, err)
}

func completeHashChoiceSizesView(ctx context.Context, id string, report inventory.HashKeeperChoiceSizesReport, readErr, closeErr error) (any, error) {
	if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
		return nil, hashChoiceSizesViewError(id, err)
	}
	return report, nil
}

func printHashKeeperChoiceSizes(out io.Writer, report inventory.HashKeeperChoiceSizesReport) error {
	if err := printHashKeeperChoice(out, report.SavedHashKeeperChoice); err != nil {
		return err
	}
	guard := &reviewOutput{writer: out}
	sizes := report.HistoricalCopySizes
	printResultBanner(guard, "HISTORICAL SIZES OF SELECTED POSSIBLE COPIES")
	printField(guard, "Selected possible copy paths", sizes.SelectedCopyPaths)
	printField(guard, "Known saved objects among copies", sizes.KnownSavedObjects)
	printField(guard, "Historical logical total across selected copy paths", historicalCopySizeText(sizes.LogicalBytes, sizes.LogicalBytesStatus))
	printField(guard, "Historical allocated total across selected copy paths", historicalCopySizeText(sizes.SavedAllocatedBytes, sizes.SavedAllocatedBytesStatus))
	printWrapped(guard, "These totals exclude the keeper and every unselected path. Saved device/inode objects are historical identities, not proof of current continuity or independent physical storage. Recorded allocation may overlap shared extents; observations need not be simultaneous. Current allocation and reclaimable space remain unknown.", "")
	if guard.err != nil {
		return fmt.Errorf("historical choice %s size output failed; reopen with hashes --choice %s: %w", report.ID, report.ID, guard.err)
	}
	return nil
}

func historicalCopySizeText(value *int64, status string) string {
	if status == "known" && value != nil {
		return humanBytes(*value)
	}
	if status == "overflow" && value == nil {
		return "Unknown (overflow)"
	}
	return "Unknown"
}
