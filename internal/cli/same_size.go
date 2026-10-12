package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Only this selector is canonical and once-only. Absence or explicit false
// keeps the original reader, V1 envelope, cursor and human output.
type cargoSameSizeMode struct {
	value bool
	set   bool
}

func (v *cargoSameSizeMode) String() string   { return fmt.Sprint(v.value) }
func (v *cargoSameSizeMode) IsBoolFlag() bool { return true }
func (v *cargoSameSizeMode) Set(value string) error {
	if v.set {
		return errors.New("--exclude-cargo-target cannot be repeated")
	}
	if value != "true" && value != "false" {
		return errors.New("--exclude-cargo-target accepts only true or false")
	}
	v.value, v.set = value == "true", true
	return nil
}

// This local seam permits generated read/close failures without production
// hooks, extra inventory reads or source access.
type cargoSameSizePageReader interface {
	SameSizeCandidatesWithOptions(context.Context, int, string, int64, state.SameSizeOptions) (state.SameSizeReport, error)
	Close() error
}

func readCargoSameSizePage(ctx context.Context, s cargoSameSizePageReader, limit int, cursor string, minimumBytes int64, command string) (reportResult, error) {
	r, readErr := s.SameSizeCandidatesWithOptions(ctx, limit, cursor, minimumBytes, state.SameSizeOptions{ExcludeCargoDefaultTarget: true})
	closeErr := s.Close()
	if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		return reportResult{}, err
	}
	if r.CargoTargetExclusion == nil || r.CargoTargetExclusion.Policy != state.SameSizeCargoPolicy || r.CargoTargetExclusion.Recognition != "saved_layout_metadata_only" {
		return reportResult{}, errors.New("saved Cargo filter summary is missing or invalid")
	}
	return reportResult{FileReport: state.FileReport{SameSize: &r, GeneratedAt: r.GeneratedAt, Source: r.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: r.Notes}, sameSizeCommand: command}, nil
}

func selectedCargoSameSizeReport(r reportResult) bool {
	return r.SameSize != nil && r.SameSize.CargoTargetExclusion != nil
}

func printSameSizeReport(out io.Writer, r state.SameSizeReport, command string) {
	printResultBanner(out, "SAME-SIZE FILES - CONTENT NOT CHECKED")
	printWrapped(out, "Saved metadata only. Equal sizes do not prove equal contents. Current files and ancestor freshness have not been checked. No keeper or cleanup action is selected.", "")
	printField(out, "Minimum file size", humanBytes(r.MinimumBytes))
	printField(out, "Saved file rows checked", humanCount(r.EntriesExamined))
	printWrapped(out, "Counts cover this page only. Known device/inode aliases may be paths to one object; unknown or conflicting identities cannot establish independent copies. There is no estimate of space you can free.", "")
	if c := r.CargoTargetExclusion; c != nil {
		fmt.Fprintln(out, "\nCARGO FILTER - SAVED LAYOUT ONLY")
		printField(out, "Hidden Cargo target rows", humanCount(c.ExcludedRows))
		printField(out, "Retained unrecognized rows", humanCount(c.RetainedUnrecognizedRows))
		printField(out, "Retained unknown rows", humanCount(c.RetainedUnknownRows))
		printField(out, "Retained ancestor-limit rows", humanCount(c.RetainedAncestorLimitRows))
		printWrapped(out, "These counts cover this saved page only. The filter has no age cutoff. A supported layout can contain unique or manually edited files. Hidden rows do not prove generated-only content, inactivity, safe regeneration or safe cleanup.", "")
	}
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
	if r.CargoTargetExclusion != nil {
		printWrapped(out, "Dependency trees named node_modules, saved skips and supported saved Cargo target layouts are excluded. Missing or uncertain layout evidence does not exclude a row through the Cargo filter. Current sources, file contents and effective Cargo settings were not checked. This filtered V2 report is not accepted by hash --select. No cleanup is authorized.", "")
	} else {
		printWrapped(out, "Dependency trees named node_modules and saved skipped entries are excluded. Other generated categories remain outside this first filter. Hashing requires a separate exact selection and explicit full-file read consent. This report does not read file contents.", "")
	}
}
