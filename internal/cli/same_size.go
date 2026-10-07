package cli

import (
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func printSameSizeReport(out io.Writer, r state.SameSizeReport, command string) {
	printResultBanner(out, "SAME-SIZE FILES - CONTENT NOT CHECKED")
	printWrapped(out, "Saved metadata only. Equal sizes do not prove equal contents. Current files and ancestor freshness have not been checked. No keeper or cleanup action is selected.", "")
	printField(out, "Minimum file size", humanBytes(r.MinimumBytes))
	printField(out, "Saved file rows checked", humanCount(r.EntriesExamined))
	printWrapped(out, "Counts cover this page only. Known device/inode aliases may be paths to one object; unknown or conflicting identities cannot establish independent copies. There is no estimate of space you can free.", "")
	for i, band := range r.Bands {
		fmt.Fprintf(out, "\n%d. File size: %s\n", i+1, humanBytes(band.LogicalBytes))
		printField(out, "Known saved objects", humanCount(band.KnownObjects))
		printField(out, "Repeated saved objects", humanCount(band.RepeatedSavedObjects))
		printField(out, "Unknown identities", humanCount(band.UnknownIdentities))
		printField(out, "Conflicting identities", humanCount(band.ConflictingIdentities))
		if band.ContinuesBefore || band.ContinuesAfter {
			printWrapped(out, "This size band crosses a page boundary. Other saved rows can have this size; this is not a complete group.", "  ")
		}
		for _, file := range band.Files {
			fmt.Fprintf(out, "\n  %q\n", string(file.PathBytes))
			printField(out, "Saved file ID", file.ID)
			printField(out, "Allocated on disk", humanBytes(file.Allocated))
			printField(out, "Parent folder listing", parentLabel(file.ParentPass))
		}
	}
	if len(r.Bands) == 0 {
		printWrapped(out, "No same-size bands on this page. Excluded or unique-size rows can still leave a continuation. This does not prove there are no duplicates.", "")
	}
	labels := map[string]string{
		"disabled_root":        "Disabled root rows",
		"skipped":              "Saved skipped rows",
		"generated_dependency": "Dependency tree rows",
		"compact_parent":       "Compact overlap rows",
		"invalid_saved_path":   "Invalid saved paths",
		"eligible":             "Eligible rows on page",
	}
	for _, diagnostic := range r.Diagnostics {
		if label, ok := labels[diagnostic.Code]; ok && diagnostic.Count != 0 {
			printField(out, label, humanCount(diagnostic.Count))
		}
	}
	if r.NextCursor != "" {
		fmt.Fprintf(out, "\nRead the next saved page:\n  %s --cursor %s\n", command, shellQuote(r.NextCursor))
	} else {
		printWrapped(out, "End of saved file rows above the minimum. This does not prove complete scan coverage.", "")
	}
	printWrapped(out, "Dependency trees named node_modules and saved skipped entries are excluded. Other generated categories remain outside this first filter. Hashing requires a separate exact selection and explicit full-file read consent. This report does not read file contents.", "")
}
