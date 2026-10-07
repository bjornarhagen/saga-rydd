package cli

import (
	"bytes"
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
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type hashOption struct {
	value string
	set   bool
}

func (v *hashOption) String() string { return v.value }
func (v *hashOption) Set(value string) error {
	if v.set {
		return errors.New("hash options cannot be repeated or combined with an alias")
	}
	v.value, v.set = value, true
	return nil
}

type hashSelectOption struct {
	value bool
	set   bool
}

func (v *hashSelectOption) String() string   { return strconv.FormatBool(v.value) }
func (v *hashSelectOption) IsBoolFlag() bool { return true }
func (v *hashSelectOption) Set(value string) error {
	if v.set {
		return errors.New("hash boolean options cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

func validHashSelectionID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, b := range value {
		if b < '0' || b > '9' {
			if b < 'a' || b > 'f' {
				return false
			}
		}
	}
	return true
}

func hash(ctx context.Context, args []string, paths config.Paths) (any, error) {
	var selectMode, confirmRead, newJobKey hashSelectOption
	var show, directory, from, approve, run, revoke, day, total, saveChoice, checkChoice, requestChoice, keeper, saveChoiceJob, showJob, jobKey hashOption
	f := flag.NewFlagSet("hash", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&selectMode, "select", "save an exact unapproved metadata selection")
	f.Var(&show, "show", "show one full saved selection ID without source reads")
	f.Var(&saveChoice, "save-choice", "save explicitly selected historical keeper/copy roles; no cleanup approval")
	f.Var(&checkChoice, "check-choice", "screen one saved choice's exact live metadata; no file-body reads or approval")
	f.Var(&requestChoice, "request-choice", "review an unapproved fresh-read request from one saved choice; nothing is saved or read from sources")
	f.Var(&newJobKey, "new-job-key", "generate an explicit fresh job retry key without saving or reading storage")
	f.Var(&saveChoiceJob, "save-choice-job", "save a separate unapproved fresh job from one exact saved choice")
	f.Var(&jobKey, "job-key", "with --save-choice-job, one explicit stable job retry key")
	f.Var(&showJob, "show-job", "show one existing fresh job without source access")
	f.Var(&keeper, "keeper", "with --save-choice, one explicit keeper work ID")
	f.Var(&directory, "d", "exact manual scan root")
	f.Var(&directory, "directory", "exact manual scan root")
	f.Var(&from, "from", "one existing same-size JSON report file (at most 1 MiB)")
	f.Var(&approve, "approve", "record full-file read consent for an exact selection")
	f.Var(&run, "run", "perform one guarded step for an exact read consent ID")
	f.Var(&revoke, "revoke", "revoke exact read consent without source access")
	f.Var(&confirmRead, "confirm-content-read", "explicitly confirm full-file reads of the frozen selection")
	f.Var(&day, "max-day-bytes", "approved reservation-day byte cap (1–1125899906842624)")
	f.Var(&total, "max-total-bytes", "absolute lifetime byte cap including prior charges (1–1125899906842624)")
	if err := f.Parse(args); err != nil {
		return inventory.HashProposal{}, usageError{err}
	}
	modes := 0
	for _, set := range []bool{selectMode.set, show.set, approve.set, run.set, revoke.set, saveChoice.set, checkChoice.set, requestChoice.set, newJobKey.set, saveChoiceJob.set, showJob.set} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return nil, usageError{errors.New("hash requires exactly one of --select, --show, --save-choice, --check-choice, --request-choice, --new-job-key, --save-choice-job, --show-job, --approve, --run or --revoke")}
	}
	if newJobKey.set || saveChoiceJob.set || showJob.set {
		if directory.set || from.set || keeper.set || confirmRead.set || day.set || total.set || f.NArg() != 0 {
			return nil, usageError{errors.New("fresh-job modes accept no root, report, target, keeper, read confirmation or budget options")}
		}
		if newJobKey.set {
			if !newJobKey.value || jobKey.set {
				return nil, usageError{errors.New("hash --new-job-key takes no key or target; use the generated key explicitly when saving a job")}
			}
			return hashNewFreshJobKey(ctx)
		}
		if showJob.set {
			if jobKey.set || !inventory.ValidHashFreshJobID(showJob.value) {
				return nil, usageError{errors.New("hash --show-job requires one full saved hash-choice-job-v1 ID and no key or other options")}
			}
			return hashShowFreshChoiceJob(ctx, paths, showJob.value)
		}
		if !jobKey.set || !inventory.ValidHashKeeperChoiceID(saveChoiceJob.value) || !inventory.ValidHashFreshJobKey(jobKey.value) {
			return nil, usageError{errors.New("hash --save-choice-job requires one full saved choice ID and --job-key KEY from --new-job-key; no key is generated implicitly")}
		}
		return hashSaveFreshChoiceJob(ctx, paths, saveChoiceJob.value, jobKey.value)
	}
	if jobKey.set {
		return nil, usageError{errors.New("--job-key requires hash --save-choice-job")}
	}
	if requestChoice.set {
		if directory.set || from.set || keeper.set || confirmRead.set || day.set || total.set || f.NArg() != 0 || !inventory.ValidHashKeeperChoiceID(requestChoice.value) {
			return nil, usageError{errors.New("hash --request-choice requires one full saved hash-choice-v1 ID and no root, report, target, read confirmation or budget options")}
		}
		return hashRequestKeeperChoice(ctx, paths, requestChoice.value)
	}
	if checkChoice.set {
		if directory.set || from.set || keeper.set || confirmRead.set || day.set || total.set || f.NArg() != 0 || !inventory.ValidHashKeeperChoiceID(checkChoice.value) {
			return nil, usageError{errors.New("hash --check-choice requires one full saved hash-choice-v1 ID and no root, report, target, read confirmation or budget options")}
		}
		return hashCheckKeeperChoiceMetadata(ctx, paths, checkChoice.value)
	}
	if saveChoice.set {
		if directory.set || from.set || confirmRead.set || day.set || total.set || !keeper.set {
			return nil, usageError{errors.New("hash --save-choice requires --keeper WORK_ID and explicit copy IDs; no source, read confirmation or budget options are accepted")}
		}
		if err := validateHashChoiceCLIRequest(saveChoice.value, keeper.value, f.Args()); err != nil {
			return nil, err
		}
		return hashSaveKeeperChoice(ctx, paths, saveChoice.value, keeper.value, f.Args())
	}
	if keeper.set {
		return nil, usageError{errors.New("--keeper requires hash --save-choice")}
	}
	if approve.set || run.set || revoke.set {
		if directory.set || from.set || f.NArg() != 0 {
			return nil, usageError{errors.New("hash read-consent modes do not accept root, report, file ID or target overrides")}
		}
		mode, id := "approve", approve.value
		if run.set {
			mode, id = "run", run.value
		}
		if revoke.set {
			mode, id = "revoke", revoke.value
		}
		if !validHashSelectionID(id) {
			return nil, usageError{errors.New("hash read-consent modes require one full lowercase 64-character selection or read consent ID")}
		}
		var dayCap, totalCap int64
		if approve.set {
			if !confirmRead.set || !confirmRead.value || !day.set || !total.set {
				return nil, usageError{errors.New("hash --approve requires --confirm-content-read, --max-day-bytes N and --max-total-bytes N; there are no default caps")}
			}
			var err error
			dayCap, err = parseHashReservationCap(day.value, "--max-day-bytes")
			if err != nil {
				return nil, err
			}
			totalCap, err = parseHashReservationCap(total.value, "--max-total-bytes")
			if err != nil {
				return nil, err
			}
		} else if confirmRead.set || day.set || total.set {
			return nil, usageError{errors.New("hash --run and --revoke do not accept confirmation, allowance or cap changes")}
		}
		return hashReadCommand(ctx, mode, id, dayCap, totalCap, paths)
	}
	if confirmRead.set || day.set || total.set {
		return nil, usageError{errors.New("read confirmation and caps require hash --approve")}
	}
	var ids []int64
	var root string
	if show.set {
		if selectMode.set || directory.set || from.set || f.NArg() != 0 || !validHashSelectionID(show.value) {
			return inventory.HashProposal{}, usageError{errors.New("hash --show requires one full lowercase selection ID and no other options")}
		}
	} else {
		if !selectMode.set || !selectMode.value || !directory.set || !from.set || from.value == "" || f.NArg() < 1 || f.NArg() > state.FileSampleTargetLimit {
			return inventory.HashProposal{}, usageError{errors.New("hash requires --select -d ROOT --from REPORT_JSON and 1–20 unique saved file IDs, or --show SELECTION_ID; options must precede IDs")}
		}
		if len(from.value) > 4096 || strings.ContainsRune(from.value, 0) {
			return inventory.HashProposal{}, usageError{errors.New("--from requires a report path of at most 4096 bytes without NUL")}
		}
		seen := make(map[int64]bool, f.NArg())
		for _, text := range f.Args() {
			id, err := strconv.ParseInt(text, 10, 64)
			if err != nil || id <= 0 || strconv.FormatInt(id, 10) != text || seen[id] {
				return inventory.HashProposal{}, usageError{errors.New("saved file IDs must be distinct canonical positive decimal integers")}
			}
			seen[id] = true
			ids = append(ids, id)
		}
		var err error
		root, err = directoryPath(directory.value)
		if err != nil {
			return inventory.HashProposal{}, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return inventory.HashProposal{}, err
	}
	if show.set {
		reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				err = missingHashError{fmt.Errorf("saved hash proposal storage is unavailable; --show does not initialize it: %w", err)}
			}
			return inventory.HashProposal{}, err
		}
		proposal, err := reader.Proposal(ctx, show.value)
		closeErr := reader.Close()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				err = missingHashError{fmt.Errorf("saved hash selection %s is unavailable: %w", show.value, err)}
			}
			return inventory.HashProposal{}, err
		}
		if closeErr != nil {
			return inventory.HashProposal{}, closeErr
		}
		if err := ctx.Err(); err != nil {
			return inventory.HashProposal{}, err
		}
		return proposal, nil
	}
	page, err := readHashReport(ctx, from.value)
	if err != nil {
		return inventory.HashProposal{}, err
	}
	expected, err := selectHashReportFiles(page, ids)
	if err != nil {
		return inventory.HashProposal{}, err
	}
	source, err := state.OpenReader(ctx, manualState(paths, root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingScanMessage(root, paths, err)
		}
		return inventory.HashProposal{}, err
	}
	defer source.Close()
	// Check the complete displayed rows and exact manual root before creating
	// any hash storage. The core repeats this capture in its own transaction.
	targets, err := source.PrepareFileSampleSelection(ctx, page.InventoryID, expected)
	if err != nil {
		return inventory.HashProposal{}, err
	}
	for _, target := range targets {
		if !bytes.Equal(target.Root.PathBytes, []byte(root)) {
			return inventory.HashProposal{}, state.ErrFileSampleEvidence
		}
	}
	writer, err := inventory.OpenHashSelectionWriter(ctx, paths.StateDir)
	if err != nil {
		return inventory.HashProposal{}, err
	}
	proposal, err := writer.CreateManualSelection(ctx, source, page.InventoryID, expected, []byte(root))
	closeErr := writer.Close()
	if err != nil {
		return inventory.HashProposal{}, fmt.Errorf("save hash proposal (publication may have completed; inspect hashes before retrying): %w", err)
	}
	if closeErr != nil {
		return inventory.HashProposal{}, fmt.Errorf("close saved hash proposal (publication may have completed; inspect hashes before retrying): %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return inventory.HashProposal{}, fmt.Errorf("hash proposal reply canceled (publication may have completed; inspect hashes before retrying): %w", err)
	}
	return proposal, nil
}

func printHashProposal(out io.Writer, proposal inventory.HashProposal) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved hash proposal", "")
	if proposal.ReadConsent == nil {
		printResultBanner(guard, "UNAPPROVED HASH SELECTION - NO SELECTED FILE CONTENTS READ")
		printWrapped(guard, "This shows saved metadata for exact selected files. It does not approve or start full-file reads. Current source files have not been checked. No duplicate, keeper or cleanup action is selected.", "")
	} else {
		printResultBanner(guard, "SAVED HASH SELECTION - READ CONSENT RECORDED")
		printWrapped(guard, "This shows saved metadata and a read consent record for exact selected files. Current read permission and source files have not been checked. This command does not start reads. No duplicate, keeper or cleanup action is selected.", "")
	}
	fmt.Fprintf(guard, "Store: %s\nSelection: %s\nInventory: %s\n", proposal.StoreID, proposal.SelectionID, proposal.InventoryID)
	if proposal.SourceLocator == nil {
		printWrapped(guard, "This historical selection has no manual inventory locator. It cannot authorize later source reads.", "")
	} else {
		fmt.Fprintf(guard, "Manual root: %q\n", string(proposal.SourceLocator.RootPathBytes))
	}
	printHashReadConsent(guard, proposal.ReadConsent)
	for i, target := range proposal.Targets {
		file := target.File
		fmt.Fprintf(guard, "\nSelected file %d\n  %q\n", i+1, string(file.PathBytes))
		printField(guard, "Saved file ID", file.ID)
		printField(guard, "Logical file size", fmt.Sprintf("%d bytes", file.Size))
		allocated := "Unknown"
		if file.Allocated >= 0 {
			allocated = fmt.Sprintf("%d bytes", file.Allocated)
		}
		printField(guard, "Allocated on disk", allocated)
		printField(guard, "Observed", file.ObservedAt.UTC().Format(time.RFC3339Nano))
		printField(guard, "Modified", file.ModifiedAt.UTC().Format(time.RFC3339Nano))
		printField(guard, "Parent folder listing", parentLabel(file.ParentPass))
		fmt.Fprintf(guard, "Root baseline: %q\n", string(target.Root.PathBytes))
		for j, ancestor := range target.Ancestors {
			fmt.Fprintf(guard, "Ancestor %d (relative path): %q\n", j+1, string(ancestor.Path))
		}
	}
	printWrapped(guard, "Root and ancestor stamps are the baseline captured when this proposal was saved. They were not part of the earlier report. Add --json to hash --show for complete saved evidence. Logical and allocated sizes are not estimates of space you can free. No selected-source bytes were read and no cleanup was performed by this command.", "")
	if guard.err != nil {
		return fmt.Errorf("write hash proposal (saved selection %s remains available with hash --show): %w", proposal.SelectionID, guard.err)
	}
	return nil
}
