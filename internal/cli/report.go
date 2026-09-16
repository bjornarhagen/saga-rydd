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
}

func report(ctx context.Context, args []string, paths config.Paths) (reportResult, error) {
	scanPaths := paths
	f := flag.NewFlagSet("report", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	limit := f.Int("limit", 20, "files per page (1–200)")
	cursor := f.String("cursor", "", "next page cursor")
	candidates := f.Bool("candidates", false, "review old node_modules observations")
	directory := f.String("directory", "", "measure a saved directory subtree")
	f.StringVar(directory, "d", "", "directory alias")
	if err := f.Parse(args); err != nil {
		return reportResult{}, usageError{err}
	}
	if f.NArg() != 0 || *limit < 1 || *limit > 200 {
		return reportResult{}, usageError{errors.New("report accepts --limit 1–200 and --cursor TOKEN, or --directory ABSOLUTE_PATH")}
	}
	directorySet, pageSet, limitSet := false, false, false
	directoryFlags := 0
	f.Visit(func(v *flag.Flag) {
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
	if directoryFlags > 1 {
		return reportResult{}, usageError{errors.New("use only one of -d and --directory")}
	}
	if *candidates && limitSet {
		return reportResult{}, usageError{errors.New("--candidates accepts --cursor and an optional manual-scan directory, but not --limit")}
	}
	if directorySet && pageSet && !*candidates {
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
	if *candidates {
		c, err := s.NodeModulesFindings(ctx, *cursor)
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		return reportResult{FileReport: state.FileReport{Candidates: &c, GeneratedAt: c.GeneratedAt, Source: c.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: c.Notes}, candidateCommand: candidateReportCommand(scanPaths, *directory)}, err
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
	return reportResult{FileReport: r}, err
}
func printReport(out io.Writer, r reportResult) {
	if r.Candidates != nil {
		printFindingReport(out, *r.Candidates, r.candidateCommand)
		return
	}
	if r.Directory != nil {
		printDirectoryReport(out, *r.Directory)
		return
	}
	fmt.Fprintln(out, "Saga — Rydd: largest observed files")
	fmt.Fprintln(out, "Saved inventory only; current filesystem state has not been verified.")
	for _, f := range r.Files {
		fmt.Fprintf(out, "%s logical; %s allocated  %q\n  Observed %s; modified %s; directory listing: %s", humanBytes(f.Size), humanBytes(f.Allocated), string(f.PathBytes), f.ObservedAt.Format(time.RFC3339), f.ModifiedAt.Format(time.RFC3339), parentLabel(f.ParentPass))
		if f.SkipReason != "" {
			fmt.Fprintf(out, "; skip: %q", f.SkipReason)
		}
		fmt.Fprintln(out)
	}
	if len(r.Files) == 0 {
		fmt.Fprintln(out, "No observed files on this page. Scanning may not have started or may be incomplete.")
	}
	fmt.Fprintln(out, "\nSaved root diagnostics:")
	for _, root := range r.Roots {
		last := "never recorded"
		if root.LastRootPass != nil {
			last = root.LastRootPass.Format(time.RFC3339)
		}
		fmt.Fprintf(out, "%q: pending=%d running=%d directory errors=%d; root directory last listed=%s\n", string(root.PathBytes), root.PendingJobs, root.RunningJobs, root.DirectoryErrors, last)
		if root.LastError != "" {
			fmt.Fprintf(out, "  Last root error: %q\n", root.LastError)
		}
	}
	for _, note := range r.Notes {
		fmt.Fprintln(out, note)
	}
	if r.RootsTruncated {
		fmt.Fprintln(out, "Additional roots omitted from diagnostics.")
	}
	if r.NextCursor != "" {
		fmt.Fprintf(out, "Next page: report --limit %d --cursor %s (keep the same --data-dir, if set)\n", r.Limit, r.NextCursor)
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
	fmt.Fprintf(out, "Saga — Rydd: saved directory size\n%q\n", string(r.PathBytes))
	status := strings.ToUpper(r.Status)
	if r.Status == "recorded_complete" {
		status = "COMPLETE (saved)"
	}
	fmt.Fprintf(out, "\nSIZE  ·  %s\n", status)
	row := func(label string, value any) { fmt.Fprintf(out, "  %-23s %v\n", label, value) }
	if r.LogicalBytes != nil {
		row("Observed file size", humanBytes(*r.LogicalBytes))
		if r.AllocatedBytes != nil {
			row("Allocated on disk", humanBytes(*r.AllocatedBytes))
		} else {
			row("Allocated on disk", "unknown")
		}
	} else {
		row("Observed file size", "Unknown")
		if r.UnknownReason != "" {
			printWrapped(out, r.UnknownReason, "  ")
		}
	}
	switch r.Status {
	case "partial":
		fmt.Fprintln(out, "  Incomplete measurement — the full folder may be larger or smaller.")
	case "stale":
		fmt.Fprintln(out, "  Saved records are stale — these sizes may no longer match the folder.")
	case "recorded_complete":
		fmt.Fprintln(out, "  Complete in saved inventory; current contents are not verified.")
	}
	fmt.Fprintln(out, "\nSCAN COVERAGE")
	row("Entries measured", fmt.Sprintf("%s (limit %s)", humanCount(r.EntriesExamined), humanCount(r.EntryLimit)))
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
	if r.RepeatedInodes > 0 {
		row("Repeated file identities", humanCount(r.RepeatedInodes))
	}
	if r.UnknownInodes > 0 {
		row("Unknown file identities", humanCount(r.UnknownInodes))
	}
	if r.Truncated {
		fmt.Fprintln(out, "  Entry limit reached; sizes cover only the measured portion.")
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
			if r.Status == "recorded_complete" {
				continue
			}
			note = "Saved observations only; current disk contents are unverified."
		case strings.HasPrefix(note, "Logical bytes sum regular-file paths;"):
			note = "Regular files only. File size counts each path; allocation counts each known file identity once in the measured portion."
		case strings.HasPrefix(note, "Hardlinks outside this folder,"):
			note = "Not a reclaimable-space estimate: hardlinks, clones and snapshots can retain storage. Do not add overlapping folder sizes."
		case strings.HasPrefix(note, "Stale or partial inventories"):
			if r.Status == "partial" || r.Status == "stale" || r.Status == "recorded_complete" {
				continue
			}
			note = "Partial or stale records can overstate or understate size; truncated results cover only part of the saved folder."
		}
		printWrapped(out, note, "  • ")
	}
}

func humanCount(n int) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func printWrapped(out io.Writer, text, prefix string) {
	const width = 78
	line := prefix
	continuation := "    "
	for _, word := range strings.Fields(text) {
		if len([]rune(line))+1+len([]rune(word)) > width && strings.TrimSpace(line) != strings.TrimSpace(prefix) {
			fmt.Fprintln(out, line)
			line = continuation + word
		} else {
			if line != prefix {
				line += " "
			}
			line += word
		}
	}
	fmt.Fprintln(out, line)
}

func directoryStatusLabel(status string) string {
	switch status {
	case "recorded_complete":
		return "complete in saved inventory (current disk state unverified)"
	case "partial":
		return "partial"
	case "stale":
		return "stale saved records"
	default:
		return "unknown"
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func candidateReportCommand(paths config.Paths, directory string) string {
	command := "rydd"
	defaults, err := config.ResolvePaths("")
	if err != nil || paths.StateDir != defaults.StateDir {
		command += " --data-dir " + shellQuote(paths.StateDir)
	}
	command += " report --candidates"
	if directory != "" {
		command += " -d " + shellQuote(directory)
	}
	return command
}

func printFindingReport(out io.Writer, r state.FindingReport, command string) {
	fmt.Fprintln(out, "Saga — Rydd: node_modules review candidates")
	if len(r.Findings) == 0 {
		fmt.Fprintln(out, "\nNo candidates on this page.")
	} else {
		fmt.Fprintf(out, "\n%d candidate(s) on this page — review required.\n", len(r.Findings))
	}
	fmt.Fprintf(out, "\nPAGE SUMMARY\n  Saved entries checked     %s\n", humanCount(r.EntriesExamined))
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
		fmt.Fprintf(out, "  %-25s %s\n", label, humanCount(d.Count))
	}
	for i, f := range r.Findings {
		m := f.Measurement
		logical, allocated := "unknown", "unknown"
		if m.LogicalBytes != nil {
			logical = humanBytes(*m.LogicalBytes)
		}
		if m.AllocatedBytes != nil {
			allocated = humanBytes(*m.AllocatedBytes)
		}
		fmt.Fprintf(out, "\n%d. %q\n", i+1, string(f.PathBytes))
		fmt.Fprintf(out, "   Size       %s logical; %s allocated (%s)\n", logical, allocated, strings.ReplaceAll(m.Status, "_", " "))
		fmt.Fprintf(out, "   Modified   folder %s; package.json %s\n", f.DirectoryModifiedAt.Format("2006-01-02"), f.ManifestModifiedAt.Format("2006-01-02"))
		fmt.Fprintf(out, "   Reference  %s\n", f.ID)
	}
	if r.NextCursor != "" {
		fmt.Fprintln(out, "\nMORE RESULTS")
		if len(r.Findings) == 0 {
			fmt.Fprintln(out, "  This page is empty, but more saved entries remain.")
		} else {
			fmt.Fprintln(out, "  More saved entries remain.")
		}
		fmt.Fprintf(out, "  Next page:\n    %s --cursor %s\n", command, shellQuote(r.NextCursor))
	} else {
		fmt.Fprintln(out, "\nEnd of saved entries. This does not prove the scan is complete.")
	}
	fmt.Fprintln(out, "\nABOUT THESE RESULTS")
	printWrapped(out, fmt.Sprintf("Age filter: both the folder and package.json modification dates must be at least %d days old. Age alone does not establish inactivity or safe deletion.", r.MinimumAgeDays), "  ")
	printWrapped(out, "Saved metadata only; sizes may be partial or stale and are not reclaimable-space estimates. Project contents and activity have not been checked.", "  ")
	if len(r.Findings) > 0 {
		printWrapped(out, "Removing dependencies can break builds or lose local edits. Reinstalling may need the right tools, lockfile, credentials and available packages. Cleanup is not yet supported.", "  ")
	}
	fmt.Fprintln(out, "  Full evidence and caveats: add --json.")
}

type missingScanError struct {
	cause   error
	message string
}

func (e missingScanError) Error() string { return e.message }
func (e missingScanError) Unwrap() error { return e.cause }

func missingScanMessage(directory string, paths config.Paths, cause error) error {
	// Single-quote shell arguments so suggested commands preserve literal paths.
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	command := "rydd"
	defaults, err := config.ResolvePaths("")
	if err != nil || defaults.StateDir != paths.StateDir {
		command += " --data-dir " + quote(paths.StateDir)
	}
	command += " scan -d " + quote(directory)
	return missingScanError{cause: cause, message: fmt.Sprintf("No saved scan covers folder %q in this state location.\nScan it first:\n  %s\nThen run the report again. Reports use saved results; they do not start a scan.", directory, command)}
}
