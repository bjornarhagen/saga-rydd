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

// Earlier modes retain their existing parser and rendering. The new wrapper
// adds only private presentation context and an omitted optional JSON field.
type savedReportDispatch struct {
	dispatchedReport
	goCacheCommand string
	goCacheCursor  string
}

func dispatchSavedReport(ctx context.Context, args []string, paths config.Paths) (savedReportDispatch, error) {
	if hasGoCacheFlag(args) {
		return goCache(ctx, args, paths)
	}
	r, err := dispatchReport(ctx, args, paths)
	return savedReportDispatch{dispatchedReport: r}, err
}

func hasGoCacheFlag(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || !strings.HasPrefix(a, "-") || a == "-" {
			return false
		}
		name, _, assigned := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-"), "=")
		if name == "go-cache" {
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

type goCacheValue struct {
	value string
	set   bool
}

func (v *goCacheValue) String() string { return v.value }
func (v *goCacheValue) Set(value string) error {
	if v.set {
		return errors.New("Go cache options cannot be repeated or combined with an alias")
	}
	v.value, v.set = value, true
	return nil
}

type goCacheMode struct {
	value bool
	set   bool
}

func (v *goCacheMode) String() string   { return strconv.FormatBool(v.value) }
func (v *goCacheMode) IsBoolFlag() bool { return true }
func (v *goCacheMode) Set(value string) error {
	if v.set {
		return errors.New("--go-cache cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

func goCache(ctx context.Context, args []string, paths config.Paths) (savedReportDispatch, error) {
	var mode goCacheMode
	var directory, age, cursor goCacheValue
	f := flag.NewFlagSet("report --go-cache", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&mode, "go-cache", "review saved Go local build-cache file metadata")
	f.Var(&directory, "d", "exact manual scan root of the proposed cache")
	f.Var(&directory, "directory", "exact manual scan root of the proposed cache")
	f.Var(&age, "min-age-days", "minimum saved file age (1–36500; default 90)")
	f.Var(&cursor, "cursor", "next Go cache page with the same age filter")
	if err := f.Parse(args); err != nil {
		return savedReportDispatch{}, usageError{err}
	}
	if !mode.set || !mode.value || !directory.set || directory.value == "" || f.NArg() != 0 {
		return savedReportDispatch{}, usageError{errors.New("report --go-cache requires -d ROOT and accepts only --min-age-days and --cursor; modes, aliases and options cannot be repeated")}
	}
	days := state.FindingAgeDays
	if age.set {
		parsed, err := strconv.Atoi(age.value)
		if err != nil || parsed < 1 || parsed > state.MaxFindingAgeDays {
			return savedReportDispatch{}, usageError{state.ErrFindingAge}
		}
		days = parsed
	}
	if cursor.value != "" && !validGoCacheCursor(cursor.value, days) {
		return savedReportDispatch{}, usageError{state.ErrReportCursor}
	}
	root, err := directoryPath(directory.value)
	if err != nil {
		return savedReportDispatch{}, err
	}
	if len(root) > 4096 || strings.ContainsRune(root, 0) {
		return savedReportDispatch{}, usageError{state.ErrGoCacheRoot}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return savedReportDispatch{}, err
	}
	s, err := state.OpenReader(ctx, manualState(paths, root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return savedReportDispatch{}, missingScanMessage(root, paths, err)
		}
		return savedReportDispatch{}, err
	}
	r, readErr := s.GoBuildCache(ctx, root, cursor.value, days)
	closeErr := s.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		if errors.Is(err, state.ErrReportCursor) || errors.Is(err, state.ErrFindingAge) || errors.Is(err, state.ErrGoCacheRoot) {
			err = usageError{err}
		}
		return savedReportDispatch{}, err
	}
	if err := ctx.Err(); err != nil {
		return savedReportDispatch{}, err
	}
	return savedReportDispatch{
		dispatchedReport: dispatchedReport{reportResult: reportResult{FileReport: state.FileReport{GoCache: &r, GeneratedAt: r.GeneratedAt, Source: r.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: r.Notes}}},
		goCacheCommand:   fmt.Sprintf("%s report --go-cache -d %s --min-age-days %d", commandPrefix(paths), shellQuote(root), days),
		goCacheCursor:    cursor.value,
	}, nil
}

func validGoCacheCursor(token string, days int) bool {
	prefix := fmt.Sprintf("gocache1:%d:", days)
	if !strings.HasPrefix(token, prefix) {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(token, prefix), 10, 64)
	return err == nil && n > 0 && token == prefix+strconv.FormatInt(n, 10)
}

func printSavedReport(out io.Writer, r savedReportDispatch) {
	if r.GoCache != nil {
		printGoCacheReport(out, *r.GoCache, r.goCacheCommand, r.goCacheCursor)
		return
	}
	printDispatchedReport(out, r.dispatchedReport)
}

func printGoCacheReport(out io.Writer, r state.GoCacheReport, command, cursor string) {
	noun := "files"
	if len(r.Files) == 1 {
		noun = "file"
	}
	printWrapped(out, fmt.Sprintf("%d Go build-cache %s on this page - historical review only.", len(r.Files), noun), "")
	fmt.Fprintf(out, "Proposed cache root: %q\n", string(r.RootPathBytes))
	layout := "Unsupported or incomplete saved layout"
	if r.Layout.Status == "layout_metadata_recognized" {
		layout = "Recognized from saved metadata only"
	}
	printField(out, "Saved layout", layout)
	printField(out, "Confirmed shards", fmt.Sprintf("%s of %s", humanCount(r.Layout.ConfirmedShards), humanCount(r.Layout.RequiredShards)))
	if r.Layout.Message != "" {
		printWrapped(out, r.Layout.Message, "  ")
	}
	printWrapped(out, "Layout filenames do not prove that Go produced these files or uses this cache. Contents, effective Go settings, current use and regeneration have not been checked. Approval, automation and cleanup are unavailable.", "")
	for i, file := range r.Files {
		fmt.Fprintf(out, "\n%d. %q\n", i+1, string(file.PathBytes))
		printField(out, "Saved entry type", file.EntryType)
		printField(out, "Logical size observed", savedBytes(file.LogicalBytes))
		printField(out, "Allocated size observed", savedBytes(file.AllocatedBytes))
		printField(out, "Saved file identity", file.IdentityStatus)
		printField(out, "Modified", file.ModifiedAt.UTC().Format(time.RFC3339Nano))
		observed := "NOT RECORDED"
		if !file.ObservedAt.IsZero() {
			observed = file.ObservedAt.UTC().Format(time.RFC3339Nano)
		}
		printField(out, "Observed", observed)
		printField(out, "Reference", file.ID)
	}
	fmt.Fprintln(out, "\nPAGE SUMMARY")
	printField(out, "Raw saved entries checked", fmt.Sprintf("%s (limit %s)", humanCount(r.EntriesExamined), humanCount(r.EntryLimit)))
	printField(out, "Known saved objects", humanCount(r.KnownObjects))
	printField(out, "Repeated saved objects", humanCount(r.RepeatedSavedObjects))
	printField(out, "Unknown identities", humanCount(r.UnknownIdentities))
	printField(out, "Conflicting identities", humanCount(r.ConflictingIdentities))
	labels := map[string]string{
		"different_root": "Outside requested root", "unsupported_saved_path": "Unsupported saved scope",
		"outside_supported_layout": "Other layout entries", "executable_directory": "Executable directories unsupported",
		"not_regular_file": "Not regular files", "skipped": "Excluded or skipped", "parent_incomplete_or_error": "Incomplete shard listing",
		"parent_unconfirmed": "Unconfirmed shard membership", "timestamp_unknown": "Unknown file dates", "age_not_met": "Too recent / future-dated", "selected": "Selected for review",
	}
	for _, diagnostic := range r.Diagnostics {
		if diagnostic.Count != 0 {
			label := labels[diagnostic.Code]
			if label == "" {
				label = diagnostic.Code
			}
			printField(out, label, humanCount(diagnostic.Count))
		}
	}
	if r.NextCursor != "" {
		if len(r.Files) == 0 {
			printWrapped(out, "This page is empty, but more saved entries remain.", "")
		}
		fmt.Fprintf(out, "\nNext page:\n  %s --cursor %s\n", command, shellQuote(r.NextCursor))
	} else if r.PageCoverage == "saved_entries_exhausted" {
		printWrapped(out, "End of saved entries. This does not prove the scan is complete or describe the whole cache.", "")
	} else {
		printWrapped(out, "The saved layout was not recognized. An empty result does not prove this root is empty or dispensable.", "")
	}
	printWrapped(out, fmt.Sprintf("Age filter: each selected file's saved modification date must be at least %d days old. Root and shard dates are not this filter. Old dates do not prove inactivity or safe removal. Go performs its own cache trimming.", r.MinimumAgeDays), "")
	printWrapped(out, "Sizes are nullable per-file observations, not whole-cache totals or space you can free. Do not sum paths or pages: hardlinks, clones and snapshots can share storage. Unknown, repeated and conflicting identities retain their qualifications.", "")
	printWrapped(out, "Fuzz, module/download caches, external managers and executable directories are outside this category. Rebuilding can need original inputs, toolchains and external prerequisites. No source paths, configuration or Go commands were read or invoked. No records changed, and node_modules plans or dismissals do not accept these references.", "")
	if cursor != "" {
		command += " --cursor " + shellQuote(cursor)
	}
	fmt.Fprintf(out, "\nFull saved layout and file evidence (JSON):\n  %s --json\n", command)
}
