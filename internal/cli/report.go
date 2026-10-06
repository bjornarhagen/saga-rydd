package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Presentation context stays outside the serialized report contract.
type reportResult struct {
	state.FileReport
	candidateCommand string
	fileCommand      string
	sameSizeCommand  string
}

func report(ctx context.Context, args []string, paths config.Paths) (reportResult, error) {
	scanPaths := paths
	f := flag.NewFlagSet("report", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	limit := f.Int("limit", 20, "files per page (1–200)")
	cursor := f.String("cursor", "", "next page cursor")
	minimumAge := f.Int("min-age-days", state.FindingAgeDays, "minimum candidate age in days (1–36500)")
	candidates := f.Bool("candidates", false, "review old node_modules observations")
	sameSize := f.Bool("same-size", false, "read bounded saved same-size file bands; contents unchecked")
	minimumBytes := f.Int64("min-size-bytes", state.SameSizeMinimumBytes, "minimum same-size file bytes")
	directory := f.String("directory", "", "measure a saved directory subtree")
	f.StringVar(directory, "d", "", "directory alias")
	if err := f.Parse(args); err != nil {
		return reportResult{}, usageError{err}
	}
	if f.NArg() != 0 || *limit < 1 || *limit > 200 {
		return reportResult{}, usageError{errors.New("report accepts --limit 1–200 and --cursor TOKEN, or --directory ABSOLUTE_PATH")}
	}
	directorySet, pageSet, limitSet, ageSet, sizeSet, candidatesSet, sameSizeSet := false, false, false, false, false, false, false
	directoryFlags := 0
	f.Visit(func(v *flag.Flag) {
		if v.Name == "candidates" {
			candidatesSet = true
		}
		if v.Name == "same-size" {
			sameSizeSet = true
		}
		if v.Name == "min-age-days" {
			ageSet = true
		}
		if v.Name == "min-size-bytes" {
			sizeSet = true
		}
		if v.Name == "limit" {
			limitSet = true
		}
		if v.Name == "directory" || v.Name == "d" {
			directorySet = true
			directoryFlags++
		}
		if v.Name == "cursor" || v.Name == "limit" {
			pageSet = true
		}
	})
	if ageSet && !*candidates {
		return reportResult{}, usageError{errors.New("--min-age-days requires --candidates")}
	}
	if sameSizeSet && candidatesSet || sizeSet && !*sameSize || *minimumBytes < 1 {
		return reportResult{}, usageError{errors.New("--same-size accepts --min-size-bytes greater than zero and cannot be combined with --candidates; --min-size-bytes requires --same-size")}
	}
	if *minimumAge < 1 || *minimumAge > state.MaxFindingAgeDays {
		return reportResult{}, usageError{state.ErrFindingAge}
	}
	if directoryFlags > 1 {
		return reportResult{}, usageError{errors.New("use only one of -d and --directory")}
	}
	if *candidates && limitSet {
		return reportResult{}, usageError{errors.New("--candidates accepts --cursor and an optional manual-scan directory, but not --limit")}
	}
	if directorySet && pageSet && !*candidates && !*sameSize {
		return reportResult{}, usageError{errors.New("directory size reports cannot be combined with --limit or --cursor")}
	}
	if directorySet {
		normalized, err := directoryPath(*directory)
		if err != nil {
			return reportResult{}, err
		}
		*directory = normalized
		manual := manualState(paths, normalized)
		if _, err := os.Lstat(filepath.Join(manual, state.Filename)); err == nil {
			paths.StateDir = manual
		} else if !errors.Is(err, os.ErrNotExist) {
			return reportResult{}, err
		} else if *candidates {
			return reportResult{}, usageError{errors.New("scoped candidates require a manual scan of this exact directory; run scan -d PATH first")}
		} else if *sameSize {
			return reportResult{}, missingScanMessage(normalized, scanPaths, os.ErrNotExist)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		if directorySet && errors.Is(err, os.ErrNotExist) {
			return reportResult{}, missingScanMessage(*directory, scanPaths, err)
		}
		return reportResult{}, err
	}
	defer s.Close()
	if *sameSize {
		r, err := s.SameSizeCandidates(ctx, *limit, *cursor, *minimumBytes)
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		command := fmt.Sprintf("%s report --same-size --min-size-bytes %d --limit %d", commandPrefix(scanPaths), *minimumBytes, *limit)
		if directorySet {
			command += " -d " + shellQuote(*directory)
		}
		return reportResult{FileReport: state.FileReport{SameSize: &r, GeneratedAt: r.GeneratedAt, Source: r.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: r.Notes}, sameSizeCommand: command}, err
	}
	if *candidates {
		c, err := s.NodeModulesFindings(ctx, *cursor, *minimumAge)
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		return reportResult{FileReport: state.FileReport{Candidates: &c, GeneratedAt: c.GeneratedAt, Source: c.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: c.Notes}, candidateCommand: candidateReportCommand(scanPaths, *directory, *minimumAge)}, err
	}
	if directorySet {
		d, err := s.MeasureDirectory(ctx, filepath.Clean(*directory))
		if errors.Is(err, state.ErrDirectoryScope) {
			err = missingScanMessage(*directory, scanPaths, err)
		}
		return reportResult{FileReport: state.FileReport{Directory: &d, GeneratedAt: d.GeneratedAt, Source: d.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: d.Notes}}, err
	}
	r, err := s.LargestFiles(ctx, *limit, *cursor)
	if errors.Is(err, state.ErrReportCursor) {
		return reportResult{FileReport: r}, usageError{err}
	}
	return reportResult{FileReport: r, fileCommand: fmt.Sprintf("%s report --limit %d", commandPrefix(scanPaths), *limit)}, err
}
func printReport(out io.Writer, r reportResult) {
	if r.SameSize != nil {
		printSameSizeReport(out, *r.SameSize, r.sameSizeCommand)
		return
	}
	if r.Candidates != nil {
		printFindingReport(out, *r.Candidates, r.candidateCommand)
		return
	}
	if r.Directory != nil {
		printDirectoryReport(out, *r.Directory)
		return
	}
	printWrapped(out, "Saga — Rydd: largest observed files", "")
	printWrapped(out, "Saved scan only. Current contents have not been checked. These sizes are not estimates of space you can free.", "")
	for i, f := range r.Files {
		fmt.Fprintf(out, "\n%d. %q\n", i+1, string(f.PathBytes))
		printField(out, "File size", humanBytes(f.Size))
		printField(out, "Allocated on disk", humanBytes(f.Allocated))
		printField(out, "Observed", f.ObservedAt.Format(time.RFC3339))
		printField(out, "Modified", f.ModifiedAt.Format(time.RFC3339))
		printField(out, "Parent folder listing", parentLabel(f.ParentPass))
		if f.SkipReason != "" {
			printField(out, "Skip reason", fmt.Sprintf("%q", f.SkipReason))
		}
	}
	if len(r.Files) == 0 {
		printWrapped(out, "No observed files on this page. Scanning may not have started or may be incomplete.", "")
	}
	fmt.Fprintln(out, "\nSCAN COVERAGE")
	for _, root := range r.Roots {
		last := "never recorded"
		if root.LastRootPass != nil {
			last = root.LastRootPass.Format(time.RFC3339)
		}
		fmt.Fprintf(out, "\n%q\n", string(root.PathBytes))
		printField(out, "Pending scan jobs", humanCount(root.PendingJobs))
		printField(out, "Running scan jobs", humanCount(root.RunningJobs))
		printField(out, "Directory errors", humanCount(root.DirectoryErrors))
		printField(out, "Root folder last listed", last)
		if root.LastError != "" {
			fmt.Fprintf(out, "  Last root error: %q\n", root.LastError)
		}
	}
	for _, note := range r.Notes {
		printWrapped(out, note, "  ")
	}
	if r.RootsTruncated {
		fmt.Fprintln(out, "Additional roots omitted from diagnostics.")
	}
	if r.NextCursor != "" {
		fmt.Fprintf(out, "\nNext page:\n  %s --cursor %s\n", r.fileCommand, shellQuote(r.NextCursor))
	}
}
func humanBytes(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	v := float64(n)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

func parentLabel(value string) string {
	switch value {
	case "observed_in_completed_parent_pass":
		return "seen in last completed listing"
	case "partial":
		return "listing incomplete"
	case "unconfirmed":
		return "not confirmed in latest listing"
	case "directory_error":
		return "last directory scan reported an error"
	default:
		return "unknown"
	}
}

func printDirectoryReport(out io.Writer, r state.DirectoryReport) {
	printWrapped(out, "Saga — Rydd: saved directory size", "")
	fmt.Fprintf(out, "%q\n", string(r.PathBytes))
	fmt.Fprintln(out, "\nSIZE")
	row := func(label string, value any) { printField(out, label, value) }
	printSavedMeasurement(out, r)
	printWrapped(out, "Current contents have not been checked.", "  ")
	if r.AllocatedBytes != nil && r.AllocatedSizeSource == "cached_reduction" {
		row("Allocation evidence", "Completed saved calculation")
	}
	fmt.Fprintln(out, "\nSCAN COVERAGE")
	if r.CoverageSource == "cached_reduction" {
		row("Entries covered", humanCount(r.EntriesExamined))
		row("Coverage evidence", "completed saved calculation")
	} else {
		row("Entries examined", fmt.Sprintf("%s (limit %s)", humanCount(r.EntriesExamined), humanCount(r.EntryLimit)))
	}
	row("Files observed", humanCount(r.FilePaths))
	if r.CompactedDirectories > 0 {
		row("Compact file records", humanCount(r.CompactedFiles))
	}
	row("Incomplete directories", humanCount(r.IncompleteDirectories))
	row("Skipped entries", humanCount(r.SkippedEntries))
	row("Directory errors", humanCount(r.DirectoryErrors))
	if r.UnconfirmedEntries > 0 {
		row("Unconfirmed entries", humanCount(r.UnconfirmedEntries))
	}
	if r.ExcludedEntries > 0 {
		row("Historical rows excluded", humanCount(r.ExcludedEntries))
	}
	if r.RepeatedInodes > 0 {
		row("Repeated file identities", humanCount(r.RepeatedInodes))
	}
	if r.UnknownInodes > 0 {
		row("Unknown file identities", humanCount(r.UnknownInodes))
	}
	if r.OldestObservation != nil || r.RootError != "" {
		fmt.Fprintln(out, "\nFRESHNESS")
		if r.OldestObservation != nil {
			row("First observation", r.OldestObservation.Format("2006-01-02 15:04:05 MST"))
		}
		if r.NewestObservation != nil {
			row("Last observation", r.NewestObservation.Format("2006-01-02 15:04:05 MST"))
		}
		if r.RootError != "" {
			row("Last root error", fmt.Sprintf("%q", r.RootError))
		}
	}
	fmt.Fprintln(out, "\nABOUT THESE SIZES")
	for _, note := range r.Notes {
		// Condense the generic caveats for the terminal; JSON retains full evidence.
		switch {
		case strings.HasPrefix(note, "Saved observations only;"):
			continue // Already stated beside the measurement.
		case strings.HasPrefix(note, "Logical bytes sum regular-file paths;"):
			note = "Regular files only. File size counts each path; allocation counts each known file identity once in the measured portion."
		case strings.HasPrefix(note, "Hardlinks outside this folder,"):
			note = "These sizes are not estimates of space you can free. Hardlinks, clones and snapshots can retain storage. Do not add overlapping folder sizes."
		case strings.HasPrefix(note, "Stale or partial inventories"):
			if r.Status == "partial" || r.Status == "stale" || r.Status == "recorded_complete" {
				continue
			}
			note = "Partial or stale records can overstate or understate size; truncated results cover only part of the saved folder."
		}
		printWrapped(out, note, "  - ")
	}
}

func humanCount[T ~int | ~int64](n T) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func commandPrefix(paths config.Paths) string {
	command := "rydd"
	defaults, err := config.ResolvePaths("")
	if err != nil || paths.StateDir != defaults.StateDir {
		command += " --data-dir " + shellQuote(paths.StateDir)
	}
	return command
}

func candidateReportCommand(paths config.Paths, directory string, minimumAge int) string {
	command := commandPrefix(paths)
	command += " report --candidates"
	if minimumAge != state.FindingAgeDays {
		command += fmt.Sprintf(" --min-age-days %d", minimumAge)
	}
	if directory != "" {
		command += " -d " + shellQuote(directory)
	}
	return command
}

func printFindingReport(out io.Writer, r state.FindingReport, command string) {
	printWrapped(out, "Saga — Rydd: node_modules review candidates", "")
	if len(r.Findings) == 0 {
		printResultBanner(out, "NO CANDIDATES ON THIS PAGE")
	} else {
		noun := "candidates"
		if len(r.Findings) == 1 {
			noun = "candidate"
		}
		printWrapped(out, fmt.Sprintf("%d %s on this page - review required.", len(r.Findings), noun), "")
	}
	printWrapped(out, "Saved scan only. Current contents have not been checked. Project activity is unconfirmed. Cleanup is not yet supported.", "")
	printWrapped(out, "These sizes are not estimates of space you can free.", "")
	for i, f := range r.Findings {
		printFinding(out, i+1, f)
	}
	printFindingPageSummary(out, r)

	if r.NextCursor != "" {
		fmt.Fprintln(out, "\nMORE RESULTS")
		if len(r.Findings) == 0 {
			printWrapped(out, "This page is empty, but more saved entries remain.", "  ")
		} else {
			fmt.Fprintln(out, "  More saved entries remain.")
		}
		fmt.Fprintf(out, "  Next page:\n    %s --cursor %s\n", command, shellQuote(r.NextCursor))
	} else {
		fmt.Fprintln(out)
		printWrapped(out, "End of saved entries. This does not prove the scan is complete.", "")
	}
	fmt.Fprintln(out, "\nABOUT THESE RESULTS")
	printWrapped(out, fmt.Sprintf("Age filter: both the folder and package.json modification dates must be at least %d days old. Age alone does not establish inactivity or safe deletion.", r.MinimumAgeDays), "  ")
	printWrapped(out, "Recognition uses the saved package.json filename. Its contents, lockfiles and project activity have not been checked. Do not add overlapping folder sizes.", "  ")
	if len(r.Findings) > 0 {
		printWrapped(out, "Removing dependencies can break builds or lose local edits. Reinstalling may need the right tools, lockfile, credentials and available packages.", "  ")
	}

}

func printFindingPageSummary(out io.Writer, r state.FindingReport) {
	fmt.Fprintln(out, "\nPAGE SUMMARY")
	printField(out, "Saved entries checked", humanCount(r.EntriesExamined))
	labels := map[string]string{
		"not_node_modules":                "Other entries",
		"nested_dependency":               "Nested dependencies",
		"not_directory":                   "Not a directory",
		"skipped":                         "Excluded or skipped",
		"manifest_missing_or_unsupported": "No usable package.json",
		"parent_incomplete_or_error":      "Incomplete parent listing",
		"parent_unconfirmed":              "Unconfirmed observations",
		"timestamp_unknown":               "Unknown modification dates",
		"age_not_met":                     "Too recent / future-dated",
		"selected":                        "Selected for review",
	}
	for _, d := range r.Diagnostics {
		if d.Count == 0 {
			continue
		}
		label, ok := labels[d.Code]
		if !ok {
			label = d.Explanation
		}
		printField(out, label, humanCount(d.Count))
	}

}

type missingScanError struct {
	cause   error
	message string
}

func (e missingScanError) Error() string { return e.message }
func (e missingScanError) Unwrap() error { return e.cause }

func missingScanMessage(directory string, paths config.Paths, cause error) error {
	command := commandPrefix(paths)
	command += " scan -d " + shellQuote(directory)
	return missingScanError{cause: cause, message: fmt.Sprintf("No saved scan covers folder %q in this state location.\nScan it first:\n  %s\nThen run the report again. Reports use saved results; they do not start a scan.", directory, command)}
}
