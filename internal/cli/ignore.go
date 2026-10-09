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
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type ignoreValue struct {
	value string
	set   bool
}

func (v *ignoreValue) String() string { return v.value }
func (v *ignoreValue) Set(value string) error {
	if v.set {
		return errors.New("ignore options cannot be repeated or combined with an alias")
	}
	v.value, v.set = value, true
	return nil
}

type ignoreMode struct {
	value bool
	set   bool
}

func (v *ignoreMode) String() string   { return strconv.FormatBool(v.value) }
func (v *ignoreMode) IsBoolFlag() bool { return true }
func (v *ignoreMode) Set(value string) error {
	if v.set {
		return errors.New("ignore modes cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

type ignoreSavedResult struct {
	Mode  string
	Saved plans.SavedDismissal
}

type ignoreUnavailableError struct{ error }

func (e ignoreUnavailableError) Unwrap() error { return e.error }

func ignoreMissing(err error, message string) error {
	if errors.Is(err, os.ErrNotExist) {
		return ignoreUnavailableError{fmt.Errorf("%s; no storage was initialized: %w", message, err)}
	}
	return err
}

func validIgnoreFindingID(id string) bool {
	parts := strings.Split(id, ":")
	if len(parts) != 3 || parts[0] != "node-modules-v1" {
		return false
	}
	root, rootErr := strconv.ParseInt(parts[1], 10, 64)
	entry, entryErr := strconv.ParseInt(parts[2], 10, 64)
	return rootErr == nil && entryErr == nil && root > 0 && entry > 0 && id == fmt.Sprintf("node-modules-v1:%d:%d", root, entry)
}

func ignore(ctx context.Context, args []string, paths config.Paths) (any, error) {
	var preview, save ignoreMode
	var directory, days, from, show, undo ignoreValue
	f := flag.NewFlagSet("ignore", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&preview, "preview", "preview dismissal of one exact saved finding; nothing is saved")
	f.Var(&save, "save", "save one exact historical dismissal request")
	f.Var(&directory, "d", "exact manual scan root, with --preview only")
	f.Var(&directory, "directory", "exact manual scan root, with --preview only")
	f.Var(&days, "min-age-days", "with --preview, minimum saved candidate age (1–36500; default 90)")
	f.Var(&from, "from", "with --save, one stable ignore --preview JSON request file of at most 1 MiB")
	f.Var(&show, "show", "show one existing historical dismissal without inventory access")
	f.Var(&undo, "undo", "undo one historical dismissal without inventory access")
	if err := f.Parse(args); err != nil {
		return nil, usageError{err}
	}
	modes := 0
	for _, set := range []bool{preview.set, save.set, show.set, undo.set} {
		if set {
			modes++
		}
	}
	if modes != 1 || preview.set && !preview.value || save.set && !save.value {
		return nil, usageError{errors.New("ignore requires exactly one of --preview, --save, --show or --undo; put options before the finding ID")}
	}
	minimumAge := state.FindingAgeDays
	if preview.set {
		if !directory.set || directory.value == "" || from.set || f.NArg() != 1 || !validIgnoreFindingID(f.Arg(0)) {
			return nil, usageError{errors.New("ignore --preview requires -d ROOT and one exact finding ID; no request file is accepted")}
		}
		if days.set {
			parsed, err := strconv.Atoi(days.value)
			if err != nil || parsed < 1 || parsed > state.MaxFindingAgeDays {
				return nil, usageError{state.ErrFindingAge}
			}
			minimumAge = parsed
		}
	} else if save.set {
		if !from.set || from.value == "" || from.value == "-" || directory.set || days.set || f.NArg() != 0 {
			return nil, usageError{errors.New("ignore --save requires only --from REQUEST_JSON; directory, age, finding overrides and stdin are unavailable")}
		}
	} else {
		id := show.value
		if undo.set {
			id = undo.value
		}
		if directory.set || days.set || from.set || f.NArg() != 0 || !plans.ValidDismissalID(id) {
			return nil, usageError{errors.New("ignore --show or --undo requires one full dismissal ID and no directory, age, finding or request options")}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if preview.set {
		root, err := directoryPath(directory.value)
		if err != nil {
			return nil, err
		}
		reader, err := state.OpenReader(ctx, manualState(paths, root))
		if err != nil {
			return nil, ignoreMissing(err, "the exact existing manual inventory is required; run scan -d ROOT first")
		}
		selection, readErr := reader.SnapshotSelection(ctx, []string{f.Arg(0)}, minimumAge)
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		request, err := plans.NewDismissalRequest([]byte(root), selection)
		if err != nil {
			return nil, fmt.Errorf("the saved finding cannot support an exact dismissal; no dismissal was saved: %w", state.ErrDismissalSelection)
		}
		return request, nil
	}
	if save.set {
		request, err := readIgnoreRequest(ctx, from.value)
		if err != nil {
			return nil, err
		}
		if err = plans.CheckDismissalStorage(ctx, paths.StateDir, []state.SelectionSnapshot{request.Selection}); err != nil {
			return nil, err
		}
		// An exact retry retains the first record and its undo status offline.
		// Missing or legacy dismissal storage is not an error for a new save.
		saved, err := plans.FindDismissal(ctx, paths.StateDir, request.ID)
		if err == nil {
			return ignoreSavedResult{Mode: "save", Saved: saved}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		root := string(request.ManualRootBytes)
		inventory := manualState(paths, root)
		if err = state.CheckDismissalInventoryStorage(ctx, inventory, request.Selection); err != nil {
			return nil, err
		}
		reader, err := state.OpenReader(ctx, inventory)
		if err != nil {
			return nil, ignoreMissing(err, "the request's existing manual inventory is required for a new dismissal")
		}
		checkErr := reader.CheckDismissalSelection(ctx, request.Selection)
		closeErr := reader.Close()
		if checkErr != nil {
			return nil, checkErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		saved, err = plans.SaveDismissal(ctx, paths.StateDir, request)
		return ignorePublicationResult("save", saved, err, paths)
	}
	if show.set {
		saved, err := plans.ShowDismissal(ctx, paths.StateDir, show.value)
		return ignoreSavedResult{Mode: "show", Saved: saved}, ignoreMissing(err, "the exact historical dismissal is unavailable")
	}
	saved, err := plans.UndoDismissal(ctx, paths.StateDir, undo.value)
	return ignorePublicationResult("undo", saved, ignoreMissing(err, "the exact existing dismissal is required to undo it"), paths)
}

// Publication errors can retain a candidate ID without confirming a commit.
// Preserve that evidence and the original error, but do not print saved status.
func ignorePublicationResult(mode string, saved plans.SavedDismissal, err error, paths config.Paths) (ignoreSavedResult, error) {
	result := ignoreSavedResult{Mode: mode, Saved: saved}
	if err == nil || saved.ID == "" {
		return result, err
	}
	identity := saved.ID
	if saved.Undo != nil {
		identity += " and undo " + saved.Undo.ID
	}
	return result, fmt.Errorf("dismissal publication or reply did not finish for candidate %s; inspect it with %s ignore --show %s before retrying the exact operation: %w", identity, commandPrefix(paths), saved.ID, err)
}

func ignoreReplyMessage(result any) string {
	if saved, ok := result.(ignoreSavedResult); ok {
		if saved.Saved.Undo != nil {
			return fmt.Sprintf("Dismissal %s has saved status %s and undo %s. Inspect it with ignore --show %s before retrying the exact request", saved.Saved.ID, saved.Saved.Status, saved.Saved.Undo.ID, saved.Saved.ID)
		}
		return fmt.Sprintf("Dismissal %s has saved status %s. Inspect it with ignore --show %s before retrying the exact request", saved.Saved.ID, saved.Saved.Status, saved.Saved.ID)
	}
	return "Dismissal preview reply failed; no dismissal was saved"
}

func printIgnoreResult(out io.Writer, result any, paths config.Paths) error {
	guard := &reviewOutput{writer: out}
	var request plans.DismissalRequest
	switch r := result.(type) {
	case plans.DismissalRequest:
		request = r
		printResultBanner(guard, "HISTORICAL FINDING DISMISSAL PREVIEW - NOTHING SAVED")
	case ignoreSavedResult:
		request = r.Saved.Record.Request
		printResultBanner(guard, "SAVED HISTORICAL FINDING DISMISSAL - NO CLEANUP")
		printField(guard, "Saved status", r.Saved.Status)
		fmt.Fprintf(guard, "Dismissal: %s\nStore: %s\n", r.Saved.ID, r.Saved.Record.StoreID)
		printField(guard, "First saved at", r.Saved.Record.CreatedAt.UTC().Format(time.RFC3339Nano))
		if r.Saved.Undo != nil {
			fmt.Fprintf(guard, "Undo: %s\n", r.Saved.Undo.ID)
			printField(guard, "Undo saved at", r.Saved.Undo.Record.CreatedAt.UTC().Format(time.RFC3339Nano))
		}
	default:
		return errors.New("unsupported ignore result")
	}
	finding := request.Selection.Evidence.Findings[0]
	fmt.Fprintf(guard, "Request: %s\nManual root: %q\nFinding: %s\nTarget: %q\nManifest: %q\n", request.ID, string(request.ManualRootBytes), finding.ID, string(finding.PathBytes), string(finding.ManifestPathBytes))
	printField(guard, "Inventory", request.Selection.InventoryID)
	printField(guard, "Saved age threshold", fmt.Sprintf("%d days", request.Selection.Evidence.MinimumAgeDays))
	printField(guard, "Saved measurement status", finding.Measurement.Status)
	printField(guard, "Saved coverage source", finding.Measurement.CoverageSource)
	printField(guard, "Saved logical size", savedBytes(finding.Measurement.LogicalBytes))
	printField(guard, "Saved allocated size", savedBytes(finding.Measurement.AllocatedBytes))
	printField(guard, "Current applicability", "Not evaluated by this historical view")
	printField(guard, "Reclaimable space", "Unknown")
	printWrapped(guard, "A dismissal hides only this exact saved finding evidence. A rescan, changed observation or different age filter can make the finding visible again. Undo retains the historical record. This is not a persistent exclusion, keep policy, current-file verification, read consent or cleanup permission. No source files or configuration were read or changed.", "")
	if r, ok := result.(ignoreSavedResult); ok {
		fmt.Fprintf(guard, "\nInspect this exact historical record:\n  %s ignore --show %s\n", commandPrefix(paths), r.Saved.ID)
		if r.Saved.Status == "dismissed" {
			fmt.Fprintf(guard, "\nUndo this exact dismissal:\n  %s ignore --undo %s\n", commandPrefix(paths), r.Saved.ID)
		}
	} else {
		printWrapped(guard, "To save this exact preview, first write its --json output to a private regular file, then explicitly use:", "")
		fmt.Fprintf(guard, "  %s ignore --save --from REQUEST_JSON\n", commandPrefix(paths))
	}
	if guard.err != nil {
		return fmt.Errorf("%s: %w", ignoreReplyMessage(result), guard.err)
	}
	return nil
}
