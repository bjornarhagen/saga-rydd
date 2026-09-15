package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func report(ctx context.Context, args []string, paths config.Paths) (state.FileReport, error) {
	f := flag.NewFlagSet("report", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	limit := f.Int("limit", 20, "files per page (1–200)")
	cursor := f.String("cursor", "", "next page cursor")
	candidates := f.Bool("candidates", false, "review old node_modules observations")
	directory := f.String("directory", "", "measure a saved directory subtree")
	f.StringVar(directory, "d", "", "directory alias")
	if err := f.Parse(args); err != nil {
		return state.FileReport{}, usageError{err}
	}
	if f.NArg() != 0 || *limit < 1 || *limit > 200 {
		return state.FileReport{}, usageError{errors.New("report accepts --limit 1–200 and --cursor TOKEN, or --directory ABSOLUTE_PATH")}
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
		return state.FileReport{}, usageError{errors.New("use only one of -d and --directory")}
	}
	if *candidates && limitSet {
		return state.FileReport{}, usageError{errors.New("--candidates accepts --cursor and an optional manual-scan directory, but not --limit")}
	}
	if directorySet && pageSet && !*candidates {
		return state.FileReport{}, usageError{errors.New("directory size reports cannot be combined with --limit or --cursor")}
	}
	if directorySet {
		normalized, err := directoryPath(*directory)
		if err != nil {
			return state.FileReport{}, err
		}
		*directory = normalized
		manual := manualState(paths, normalized)
		if _, err := os.Lstat(filepath.Join(manual, state.Filename)); err == nil {
			paths.StateDir = manual
		} else if !errors.Is(err, os.ErrNotExist) {
			return state.FileReport{}, err
		} else if *candidates {
			return state.FileReport{}, usageError{errors.New("scoped candidates require a manual scan of this exact directory; run scan -d PATH first")}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return state.FileReport{}, err
	}
	defer s.Close()
	if *candidates {
		c, err := s.NodeModulesFindings(ctx, *cursor)
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		return state.FileReport{Candidates: &c, GeneratedAt: c.GeneratedAt, Source: c.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: c.Notes}, err
	}
	if directorySet {
		d, err := s.MeasureDirectory(ctx, filepath.Clean(*directory))
		return state.FileReport{Directory: &d, GeneratedAt: d.GeneratedAt, Source: d.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: d.Notes}, err
	}
	r, err := s.LargestFiles(ctx, *limit, *cursor)
	if errors.Is(err, state.ErrReportCursor) {
		return r, usageError{err}
	}
	return r, err
}
func printReport(out io.Writer, r state.FileReport) {
	if r.Candidates != nil {
		printFindingReport(out, *r.Candidates)
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
	fmt.Fprintf(out, "Saga — Rydd: saved directory size\n%q\nMeasurement: %s\n", string(r.PathBytes), directoryStatusLabel(r.Status))
	if r.LogicalBytes != nil && r.AllocatedBytes != nil {
		fmt.Fprintf(out, "Regular files: %s logical; %s allocated (known duplicate inodes counted once)\n", humanBytes(*r.LogicalBytes), humanBytes(*r.AllocatedBytes))
	} else {
		fmt.Fprintf(out, "Size unknown: %s\n", r.UnknownReason)
	}
	fmt.Fprintf(out, "Examined %d/%d saved entries; file paths=%d; repeated inodes=%d; unknown inodes=%d\n", r.EntriesExamined, r.EntryLimit, r.FilePaths, r.RepeatedInodes, r.UnknownInodes)
	fmt.Fprintf(out, "Unconfirmed entries=%d; incomplete directories=%d; directory errors=%d; skipped=%d\n", r.UnconfirmedEntries, r.IncompleteDirectories, r.DirectoryErrors, r.SkippedEntries)
	if r.Truncated {
		fmt.Fprintln(out, "Measurement truncated at the entry limit; sizes cover only the examined portion.")
	}
	if r.OldestObservation != nil {
		fmt.Fprintf(out, "Observation range: %s to %s\n", r.OldestObservation.Format(time.RFC3339), r.NewestObservation.Format(time.RFC3339))
	}
	if r.RootError != "" {
		fmt.Fprintf(out, "Last root error: %q\n", r.RootError)
	}
	for _, note := range r.Notes {
		fmt.Fprintln(out, note)
	}
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

func printFindingReport(out io.Writer, r state.FindingReport) {
	fmt.Fprintln(out, "Saga — Rydd: node_modules review candidates")
	fmt.Fprintf(out, "Selection: directory and package.json recorded modification times at least %d days old. Examined %d/%d inventory entries.\n", r.MinimumAgeDays, r.EntriesExamined, r.EntryLimit)
	fmt.Fprintln(out, "Selection outcomes on this page (first matching reason per entry):")
	for _, d := range r.Diagnostics {
		if d.Count > 0 {
			fmt.Fprintf(out, "  %d: %s [%s]\n", d.Count, d.Explanation, d.Code)
		}
	}
	if r.PageCoverage == "more_saved_entries" {
		fmt.Fprintln(out, "More saved entries remain; follow the next cursor even if this page has no candidates.")
	} else {
		fmt.Fprintln(out, "End of saved entries reached; this does not mean filesystem scanning is complete.")
	}
	for _, f := range r.Findings {
		fmt.Fprintf(out, "\n%s — review required\n%q\nRule: %s v%d; recognition: %s\nManifest: %q\nModified: directory %s; manifest %s\nObserved: directory %s; manifest %s\n", f.ID, string(f.PathBytes), f.Rule, f.RuleVersion, f.Recognition, string(f.ManifestPathBytes), f.DirectoryModifiedAt.Format(time.RFC3339), f.ManifestModifiedAt.Format(time.RFC3339), f.DirectoryObservedAt.Format(time.RFC3339), f.ManifestObservedAt.Format(time.RFC3339))
		printDirectoryReport(out, f.Measurement)
	}
	if len(r.Findings) == 0 {
		fmt.Fprintln(out, "No candidates on this page. Selection outcomes above explain the examined records.")
	}
	for _, note := range r.Notes {
		fmt.Fprintln(out, note)
	}
	if r.NextCursor != "" {
		fmt.Fprintf(out, "Next page: report --candidates --cursor %s (keep the same --data-dir, if set)\n", r.NextCursor)
	}
}
