package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	directory := f.String("directory", "", "measure a saved directory subtree")
	if err := f.Parse(args); err != nil {
		return state.FileReport{}, usageError{err}
	}
	if f.NArg() != 0 || *limit < 1 || *limit > 200 {
		return state.FileReport{}, usageError{errors.New("report accepts --limit 1–200 and --cursor TOKEN, or --directory ABSOLUTE_PATH")}
	}
	directorySet, pageSet := false, false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "directory" {
			directorySet = true
		}
		if v.Name == "cursor" || v.Name == "limit" {
			pageSet = true
		}
	})
	if directorySet && (pageSet || !filepath.IsAbs(*directory)) {
		return state.FileReport{}, usageError{errors.New("--directory requires an absolute path and cannot be combined with --limit or --cursor")}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return state.FileReport{}, err
	}
	defer s.Close()
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
