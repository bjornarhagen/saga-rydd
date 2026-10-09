package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Presentation fields do not extend the JSON report contract. Existing report
// modes retain their parser, stored evidence and rendering.
type dispatchedReport struct {
	reportResult
	buildOutputCommand string
	buildOutputCursor  string
}

func dispatchReport(ctx context.Context, args []string, paths config.Paths) (dispatchedReport, error) {
	if !hasBuildOutputFlag(args) {
		r, err := report(ctx, args, paths)
		return dispatchedReport{reportResult: r}, err
	}
	return buildOutput(ctx, args, paths)
}

// Flag values and tokens after -- are data, even when they spell a mode flag.
func hasBuildOutputFlag(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || !strings.HasPrefix(a, "-") || a == "-" {
			return false
		}
		name, _, assigned := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-"), "=")
		if name == "build-output" {
			return true
		}
		switch name {
		case "d", "directory", "cursor", "limit", "min-age-days", "min-size-bytes":
			if !assigned {
				i++
			}
		}
	}
	return false
}

type buildOutputValue struct {
	value string
	set   bool
}

func (v *buildOutputValue) String() string { return v.value }
func (v *buildOutputValue) Set(value string) error {
	if v.set {
		return errors.New("build-output options cannot be repeated or combined with an alias")
	}
	v.value, v.set = value, true
	return nil
}

type buildOutputMode struct {
	value bool
	set   bool
}

func (v *buildOutputMode) String() string   { return strconv.FormatBool(v.value) }
func (v *buildOutputMode) IsBoolFlag() bool { return true }
func (v *buildOutputMode) Set(value string) error {
	if v.set {
		return errors.New("--build-output cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

func buildOutput(ctx context.Context, args []string, paths config.Paths) (dispatchedReport, error) {
	var mode buildOutputMode
	var directory, age, cursor buildOutputValue
	f := flag.NewFlagSet("report --build-output", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&mode, "build-output", "review saved default Cargo target layouts")
	f.Var(&directory, "d", "exact existing manual scan root")
	f.Var(&directory, "directory", "exact existing manual scan root")
	f.Var(&age, "min-age-days", "minimum saved marker age (1–36500; default 90)")
	f.Var(&cursor, "cursor", "next Cargo report page with the same age filter")
	if err := f.Parse(args); err != nil {
		return dispatchedReport{}, usageError{err}
	}
	if !mode.set || !mode.value || !directory.set || directory.value == "" || f.NArg() != 0 {
		return dispatchedReport{}, usageError{errors.New("report --build-output requires -d ROOT and accepts only --min-age-days and --cursor; modes, aliases and options cannot be repeated")}
	}
	days := state.FindingAgeDays
	if age.set {
		parsed, err := strconv.Atoi(age.value)
		if err != nil || parsed < 1 || parsed > state.MaxFindingAgeDays {
			return dispatchedReport{}, usageError{state.ErrFindingAge}
		}
		days = parsed
	}
	if cursor.value != "" && !validBuildOutputCursor(cursor.value, days) {
		return dispatchedReport{}, usageError{state.ErrReportCursor}
	}
	root, err := directoryPath(directory.value)
	if err != nil {
		return dispatchedReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return dispatchedReport{}, err
	}
	s, err := state.OpenReader(ctx, manualState(paths, root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return dispatchedReport{}, missingScanMessage(root, paths, err)
		}
		return dispatchedReport{}, err
	}
	r, readErr := s.CargoBuildOutputs(ctx, cursor.value, days)
	closeErr := s.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		if errors.Is(err, state.ErrReportCursor) || errors.Is(err, state.ErrFindingAge) {
			err = usageError{err}
		}
		return dispatchedReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return dispatchedReport{}, err
	}
	return dispatchedReport{
		reportResult:       reportResult{FileReport: state.FileReport{BuildOutput: &r, GeneratedAt: r.GeneratedAt, Source: r.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: r.Notes}},
		buildOutputCommand: fmt.Sprintf("%s report --build-output -d %s --min-age-days %d", commandPrefix(paths), shellQuote(root), days),
		buildOutputCursor:  cursor.value,
	}, nil
}

// Match the state's canonical age-bound cursor grammar before opening a reader.
func validBuildOutputCursor(token string, days int) bool {
	prefix := fmt.Sprintf("cargo1:%d:", days)
	if !strings.HasPrefix(token, prefix) {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(token, prefix), 10, 64)
	return err == nil && n > 0 && token == prefix+strconv.FormatInt(n, 10)
}

func printDispatchedReport(out io.Writer, r dispatchedReport) {
	if r.BuildOutput != nil {
		printBuildOutputReport(out, *r.BuildOutput, r.buildOutputCommand, r.buildOutputCursor)
		return
	}
	printReport(out, r.reportResult)
}

func printBuildOutputReport(out io.Writer, r state.BuildOutputReport, command, cursor string) {
	noun := "candidates"
	if len(r.Findings) == 1 {
		noun = "candidate"
	}
	printWrapped(out, fmt.Sprintf("%d Cargo build-output %s on this page - review required.", len(r.Findings), noun), "")
	printWrapped(out, "Saved layout metadata only. Current contents, project activity and safe regeneration have not been checked. Approval and cleanup are unavailable.", "")
	printWrapped(out, "Logical and allocated sizes are not estimates of space you can free. Do not add overlapping folder sizes.", "")
	for i, f := range r.Findings {
		fmt.Fprintf(out, "\n%d. %q\n", i+1, string(f.PathBytes))
		printField(out, "Known layout", fmt.Sprintf("target/%s with .cargo-lock, deps and .fingerprint; Cargo.toml and Cargo.lock beside target", f.Profile))
		printSavedMeasurement(out, f.Measurement)
		printField(out, "Incomplete directories", humanCount(f.Measurement.IncompleteDirectories))
		printField(out, "Skipped entries", humanCount(f.Measurement.SkippedEntries))
		printField(out, "Directory errors", humanCount(f.Measurement.DirectoryErrors))
		if f.Measurement.UnconfirmedEntries != 0 {
			printField(out, "Unconfirmed entries", humanCount(f.Measurement.UnconfirmedEntries))
		}
		if f.Measurement.UnknownInodes != 0 {
			printField(out, "Unknown file identities", humanCount(f.Measurement.UnknownInodes))
		}
		var latest time.Time
		for _, m := range f.Markers {
			if m.ModifiedAt.After(latest) {
				latest = m.ModifiedAt
			}
		}
		if !latest.IsZero() {
			printField(out, "Newest required marker date", latest.Format("2006-01-02"))
		}
		printField(out, "Reference", f.ID)
	}
	fmt.Fprintln(out, "\nPAGE SUMMARY")
	printField(out, "Saved entries checked", fmt.Sprintf("%s (limit %s)", humanCount(r.EntriesExamined), humanCount(r.EntryLimit)))
	labels := map[string]string{
		"not_target": "Other entries", "nested_output": "Overlapping output suppressed", "unsupported_saved_path": "Unsupported saved scope",
		"not_directory": "target is not a directory", "skipped": "Excluded or skipped markers", "manifest_missing_or_unsupported": "No usable Cargo.toml / Cargo.lock",
		"profile_missing_or_unsupported": "No supported debug / release layout", "parent_incomplete_or_error": "Incomplete parent listing",
		"parent_unconfirmed": "Unconfirmed marker observations", "timestamp_unknown": "Unknown marker dates", "age_not_met": "Too recent / future-dated", "selected": "Selected for review",
	}
	for _, d := range r.Diagnostics {
		if d.Count != 0 {
			label := labels[d.Code]
			if label == "" {
				label = d.Explanation
			}
			printField(out, label, humanCount(d.Count))
		}
	}
	if r.NextCursor != "" {
		fmt.Fprintln(out, "\nMORE RESULTS")
		if len(r.Findings) == 0 {
			printWrapped(out, "This page is empty, but more saved entries remain.", "  ")
		}
		fmt.Fprintf(out, "  Next page:\n    %s --cursor %s\n", command, shellQuote(r.NextCursor))
	} else {
		fmt.Fprintln(out)
		printWrapped(out, "End of saved entries. This does not prove the scan is complete.", "")
	}
	fmt.Fprintln(out, "\nABOUT THESE RESULTS")
	printWrapped(out, fmt.Sprintf("Age filter: every required marker's saved modification date must be at least %d days old. Age does not prove inactivity, continuous stability or safe removal.", r.MinimumAgeDays), "  ")
	printWrapped(out, "Only the default outermost target layout is recognized. Custom locations, cross-target layouts and newer internal layouts are unsupported. Direct parent confirmation does not prove ancestor or current-filesystem freshness.", "  ")
	printWrapped(out, "Cargo inputs and artifact contents were not read or validated. Rebuilding can need the correct toolchain, build scripts, dependencies, credentials and network access; local edits and unique artifacts remain unverified.", "  ")
	printWrapped(out, "No source bodies, configuration or Cargo commands were read or invoked. These references are not accepted by node_modules plans or dismissals. No records or source files were changed.", "  ")
	if cursor != "" {
		command += " --cursor " + shellQuote(cursor)
	}
	fmt.Fprintf(out, "\nFull saved marker and size evidence (JSON):\n  %s --json\n", command)
}
