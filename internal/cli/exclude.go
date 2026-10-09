package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
)

type excludeValue struct {
	value string
	set   bool
}

func (v *excludeValue) String() string { return v.value }
func (v *excludeValue) Set(value string) error {
	if v.set {
		return errors.New("exclude options cannot be repeated or combined with an alias")
	}
	v.value, v.set = value, true
	return nil
}

type excludeList struct {
	value bool
	set   bool
}

func (v *excludeList) String() string   { return strconv.FormatBool(v.value) }
func (v *excludeList) IsBoolFlag() bool { return true }
func (v *excludeList) Set(value string) error {
	if v.set {
		return errors.New("exclude modes cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

// Config bytes and paths describe a saved configuration, not checked source
// objects or live permission. Qualifications stay outside the editor API.
type ExcludeReport struct {
	config.ExclusionResult
	EffectiveOn               string `json:"effective_on"`
	ActiveInvocationsReloaded bool   `json:"active_invocations_reloaded"`
	SavedHistoryChanged       bool   `json:"saved_history_changed"`
	SourceFilesRead           bool   `json:"source_files_read"`
	SourceFilesChanged        bool   `json:"source_files_changed"`
	CurrentStateVerified      bool   `json:"current_state_verified"`
	ApprovalAvailable         bool   `json:"approval_available"`
	Executable                bool   `json:"executable"`
	EstimatedReclaimableBytes *int64 `json:"estimated_reclaimable_bytes"`
}

type excludeUnavailableError struct{ error }

func (e excludeUnavailableError) Unwrap() error { return e.error }

func exclude(ctx context.Context, args []string, paths config.Paths, home string) (ExcludeReport, error) {
	var list excludeList
	var add, remove excludeValue
	f := flag.NewFlagSet("exclude", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&list, "list", "list exclusions from existing valid configuration")
	f.Var(&add, "add", "add one absolute subtree path for later invocations")
	f.Var(&remove, "remove", "remove every exact occurrence of one absolute subtree path")
	if err := f.Parse(args); err != nil {
		return ExcludeReport{}, usageError{err}
	}
	modes := 0
	for _, set := range []bool{list.set, add.set, remove.set} {
		if set {
			modes++
		}
	}
	if modes != 1 || f.NArg() != 0 || list.set && !list.value {
		return ExcludeReport{}, usageError{errors.New("exclude requires exactly one of --list, --add ABSOLUTE_PATH or --remove ABSOLUTE_PATH; repeated options and positional arguments are unavailable")}
	}
	action, path := "list", ""
	if add.set {
		action, path = "add", add.value
	}
	if remove.set {
		action, path = "remove", remove.value
	}
	if action != "list" {
		if !filepath.IsAbs(path) || len(path) > 4096 || strings.ContainsRune(path, 0) || !utf8.ValidString(path) {
			return ExcludeReport{}, usageError{errors.New("exclude paths must be absolute UTF-8 paths of at most 4096 bytes without NUL; ~/ and relative paths are unavailable")}
		}
		path = filepath.Clean(path)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return ExcludeReport{}, err
	}
	var result config.ExclusionResult
	var err error
	if action == "list" {
		result, err = config.ListExclusions(ctx, paths, home)
	} else {
		result, err = config.EditExclusion(ctx, paths, home, action, path)
	}
	report := ExcludeReport{ExclusionResult: result, EffectiveOn: "later_invocations"}
	return report, excludeResultError(report, paths, action, path, err)
}

func excludeResultError(report ExcludeReport, paths config.Paths, action, path string, err error) error {
	if err != nil {
		if errors.Is(err, config.ErrExclusionPublication) {
			return fmt.Errorf("configuration publication or reply did not finish for %s %q in %q, candidate SHA-256 %s; inspect with %s exclude --list before retrying the exact edit: %w", action, path, paths.ConfigFile, report.ConfigSHA256, commandPrefix(paths), err)
		}
		if errors.Is(err, os.ErrNotExist) {
			return excludeUnavailableError{fmt.Errorf("exclude requires existing valid configuration and edits require its existing configured-state lock; no configuration or state was initialized: %w", err)}
		}
	}
	return err
}

func excludeReplyMessage(result ExcludeReport, paths config.Paths) string {
	if result.Action == "list" {
		return fmt.Sprintf("Exclusion list reply did not finish; no configuration was changed. Inspect with %s exclude --list", commandPrefix(paths))
	}
	if result.Publication == "saved" {
		return fmt.Sprintf("The %s exclusion edit for %q was saved in %q. Inspect with %s exclude --list before retrying the exact edit", result.Action, string(result.PathBytes), string(result.ConfigFileBytes), commandPrefix(paths))
	}
	return fmt.Sprintf("The %s exclusion request for %q needed no configuration change. Inspect with %s exclude --list before retrying the exact edit", result.Action, string(result.PathBytes), commandPrefix(paths))
}

func printExcludeResult(out io.Writer, result ExcludeReport, paths config.Paths) error {
	guard := &reviewOutput{writer: out}
	title := "CONFIGURED EXCLUSIONS - NO CHANGES"
	if result.Publication == "saved" {
		if result.Action == "add" {
			title = "EXCLUSION ADDED - APPLIES TO LATER INVOCATIONS"
		} else {
			title = "EXCLUSION REMOVED - APPLIES TO LATER INVOCATIONS"
		}
	} else if result.Action != "list" {
		title = "EXCLUSIONS UNCHANGED - EXACT REQUEST NEEDED NO EDIT"
	}
	printResultBanner(guard, title)
	fmt.Fprintf(guard, "Configuration: %q\n", string(result.ConfigFileBytes))
	if result.Action != "list" {
		fmt.Fprintf(guard, "Requested path: %q\n", string(result.PathBytes))
	}
	printField(guard, "Configuration digest", result.ConfigSHA256)
	printField(guard, "Publication", result.Publication)
	printField(guard, "Configured exclusions", len(result.ExclusionPathBytes))
	if len(result.ExclusionPathBytes) == 0 {
		fmt.Fprintln(guard, "  No subtree exclusions are configured.")
	} else {
		for i, path := range result.ExclusionPathBytes {
			fmt.Fprintf(guard, "  %d. %q\n", i+1, string(path))
		}
	}
	printWrapped(guard, "Exclusions apply to later invocations. Active manual scans and hash steps retain their captured settings. Saved reports, plans, dismissals and hash history remain unchanged and historical. Paths name subtrees; they are not glob patterns. Removing one entry can leave its path covered by another exclusion. Configuration bytes were read; no configured source paths were opened or changed. No state was initialized and no worker was started, stopped or reloaded. This grants no cleanup permission and estimates no reclaimed space.", "")
	fmt.Fprintf(guard, "\nInspect this configuration with the same data directory:\n  %s exclude --list\n", commandPrefix(paths))
	if guard.err != nil {
		return fmt.Errorf("%s: %w", excludeReplyMessage(result, paths), guard.err)
	}
	return nil
}
