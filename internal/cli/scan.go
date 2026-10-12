package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func directoryPath(path string) (string, error) {
	if path == "" || strings.ContainsRune(path, 0) {
		return "", usageError{errors.New("a directory path is required")}
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if len(path) > 4096 {
		return "", usageError{errors.New("directory path exceeds 4096 bytes")}
	}
	return filepath.Clean(path), nil
}

func manualState(paths config.Paths, root string) string {
	return filepath.Join(paths.StateDir, "manual", fmt.Sprintf("%x", sha256.Sum256([]byte(root))))
}

type ScanReport struct {
	Directory                string        `json:"directory"`
	DirectoryBytes           []byte        `json:"directory_bytes"`
	StateDir                 string        `json:"state_dir"`
	Compact                  bool          `json:"compact"`
	SleepMS                  int           `json:"sleep_ms"`
	Mode                     string        `json:"mode"`
	Outcome                  string        `json:"outcome"`
	RetirementBatches        int           `json:"retirement_batches"`
	AllocationBatches        int           `json:"allocation_batches"`
	SubtreeRetirementBatches int           `json:"subtree_retirement_batches"`
	Batches                  int           `json:"batches"`
	Inventory                state.Summary `json:"inventory"`
}

func scan(ctx context.Context, args []string, paths config.Paths, progress io.Writer) (ScanReport, error) {
	r := ScanReport{}
	f := flag.NewFlagSet("scan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var path string
	var delay int
	f.StringVar(&path, "d", "", "directory to scan")
	f.StringVar(&path, "directory", "", "directory to scan")
	f.IntVar(&delay, "s", 10, "milliseconds between entry inspections")
	f.IntVar(&delay, "sleep", 10, "milliseconds between entry inspections")
	compactFlag := f.Bool("compact", false, "store node_modules file metadata compactly")
	detailedFlag := f.Bool("detailed", false, "store ordinary per-file inventory")
	now := f.Bool("now", false, "disable deliberate entry delays")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	seen := map[string]bool{}
	f.Visit(func(v *flag.Flag) { seen[v.Name] = true })
	if (seen["compact"] && seen["detailed"]) || f.NArg() != 0 || delay < 0 || delay > 60000 || (*now && (seen["s"] || seen["sleep"])) || (seen["d"] && seen["directory"]) || (seen["s"] && seen["sleep"]) {
		return r, usageError{errors.New("scan accepts -d PATH and -s 0–60000 milliseconds or --now; do not combine aliases")}
	}
	root, err := directoryPath(path)
	if err != nil {
		return r, err
	}
	if *now {
		delay = 0
	}
	info, err := os.Stat(root)
	if err != nil {
		return r, err
	}
	if !info.IsDir() {
		return r, usageError{errors.New("scan target must be a directory")}
	}
	r.Directory = root
	r.DirectoryBytes = []byte(root)
	r.StateDir = manualState(paths, root)
	r.SleepMS = delay
	var excludes []string
	home, err := os.UserHomeDir()
	if err != nil {
		return r, err
	}
	cfg, err := config.Load(paths.ConfigFile, home)
	if err == nil {
		excludes = cfg.Excludes
	} else if !errors.Is(err, os.ErrNotExist) {
		return r, err
	}
	// Protect the whole app state, including inventories of other selected roots.
	// Check each app-owned parent before creating a nested store.
	for _, dir := range []string{paths.StateDir, filepath.Join(paths.StateDir, "manual")} {
		if err = localfs.EnsurePrivateDir(dir); err != nil {
			return r, err
		}
	}
	w, err := state.OpenWriter(ctx, r.StateDir)
	if err != nil {
		return r, err
	}
	defer w.Close()
	var requested *bool
	if seen["compact"] {
		requested = compactFlag
	}
	if seen["detailed"] {
		value := !*detailedFlag
		requested = &value
	}
	r.Compact, err = w.ConfigureCompact(ctx, requested)
	if err != nil {
		return r, err
	}
	if err = w.SyncRoots(ctx, []string{root}); err != nil {
		return r, err
	}
	scanner, err := inventory.New([]string{root}, excludes, []string{paths.StateDir, paths.ConfigFile}, inventory.WithEntryDelay(time.Duration(delay)*time.Millisecond))
	if err != nil {
		return r, err
	}
	defer scanner.Close()
	if _, err = w.RecoverJobs(ctx, time.Now()); err != nil {
		return r, err
	}
	due, err := w.NextJobDue(ctx, []string{state.ScanKind})
	if err != nil {
		return r, err
	}
	retiring, err := w.HasCompactRetirement(ctx)
	if err != nil {
		return r, err
	}
	reducing, err := w.HasAllocationWork(ctx)
	if err != nil {
		return r, err
	}
	reconciling, err := w.HasSubtreeRetirement(ctx)
	if err != nil {
		return r, err
	}
	r.Mode = "new_pass"
	if !due.IsZero() || retiring || reducing || reconciling {
		r.Mode = "resume"
	}
	if err = w.SeedInventory(ctx); err != nil {
		return r, err
	}
	fmt.Fprintf(progress, "Scanning folder:\n%q\n", root)
	printWrapped(progress, fmt.Sprintf("Entry delay: %d ms. Press Ctrl+C to stop. Progress already saved is kept.", delay), "")
	if r.Mode == "resume" {
		printWrapped(progress, "Resuming saved work before revisiting completed folders.", "")
	} else {
		fmt.Fprintln(progress, "Starting a new pass.")
	}
	if r.Compact {
		printWrapped(progress, "Compact node_modules inventory enabled. File contents are not read.", "")
	}
	lastProgress := time.Now()
	for {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		blocked, e := w.WALBlocked(ctx, state.WALBackpressureBytes)
		if e != nil {
			return r, e
		}
		if blocked {
			r.Outcome = "wal_backpressure"
			break
		}
		j, e := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			return r, e
		}
		if j == nil {
			worked, cleanupErr := w.RetireCompact(ctx)
			if cleanupErr != nil {
				return r, cleanupErr
			}
			if worked {
				r.RetirementBatches++
			} else {
				worked, cleanupErr = w.RetireSubtrees(ctx)
				if cleanupErr != nil {
					return r, cleanupErr
				}
				if worked {
					r.SubtreeRetirementBatches++
				}
			}
			if !worked {
				worked, cleanupErr = w.ReduceAllocations(ctx)
				if cleanupErr != nil {
					return r, cleanupErr
				}
				if worked {
					r.AllocationBatches++
				}
			}
			if worked {
				if time.Since(lastProgress) >= time.Second {
					printWrapped(progress, fmt.Sprintf("Saved size calculations: %d batches. Removing outdated database records: %d batches.", r.AllocationBatches, r.RetirementBatches+r.SubtreeRetirementBatches), "")
					lastProgress = time.Now()
				}
				timer := time.NewTimer(time.Duration(max(delay, 1)) * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return r, ctx.Err()
				case <-timer.C:
				}
				continue
			}
			r.Outcome = "queue_drained"
			break
		}
		chunk, cancel := context.WithTimeout(ctx, 10*time.Second)
		type result struct {
			batch state.ScanBatch
			err   error
		}
		done := make(chan result, 1)
		go func() { b, e := scanner.Next(chunk, *j); done <- result{b, e} }()
		var batch state.ScanBatch
		select {
		case next := <-done:
			batch, e = next.batch, next.err
		case <-chunk.Done():
			e = chunk.Err()
		}
		cancel()
		if e != nil {
			// Preserve the queue for the next invocation, even on interrupt.
			cleanup, done := context.WithTimeout(context.Background(), time.Second)
			finishErr := w.FinishJob(cleanup, *j, false, j.Cursor, time.Unix(0, 1), e.Error())
			done()
			return r, errors.Join(e, finishErr)
		}
		if err = w.CommitScan(ctx, *j, batch); err != nil {
			return r, err
		}
		r.Batches++
		if time.Since(lastProgress) >= time.Second {
			fmt.Fprintf(progress, "Saved %d batches.\n", r.Batches)
			lastProgress = time.Now()
		}
	}
	r.Inventory, err = w.Summary(ctx)
	if err != nil {
		return r, err
	}
	if r.Outcome == "queue_drained" && (r.Inventory.PendingJobs > 0 || r.Inventory.RunningJobs > 0) {
		r.Outcome = "pending_retry"
	}
	if _, err = w.Checkpoint(ctx); err != nil {
		return r, err
	}
	return r, nil
}

func printScanReport(out io.Writer, r ScanReport) {
	switch r.Outcome {
	case "queue_drained":
		printWrapped(out, "Scan pass finished. Results are saved.", "")
	case "pending_retry":
		printWrapped(out, "Scan paused. Some folders still need another attempt.", "")
	case "wal_backpressure":
		printWrapped(out, "Scan paused. The saved database needs to finish writing pending changes before scanning can continue.", "")
	default:
		printWrapped(out, "Scan stopped. Check the saved results before continuing.", "")
	}
	fmt.Fprintf(out, "%q\n\n", r.Directory)
	printField(out, "Saved entries", humanCount(r.Inventory.Entries))
	printField(out, "Pending scan jobs", humanCount(r.Inventory.PendingJobs))
	printField(out, "Running scan jobs", humanCount(r.Inventory.RunningJobs))
	printField(out, "Directory errors", humanCount(r.Inventory.DirectoryErrors))
	printField(out, "Skipped entries", humanCount(r.Inventory.SkippedEntries))
	printWrapped(out, "Saved entries do not prove complete coverage or current contents. The report shows size and coverage limits.", "")
	// StateDir is the per-root store; commands need its configured parent.
	base := filepath.Dir(filepath.Dir(r.StateDir))
	if r.Outcome == "pending_retry" || r.Outcome == "wal_backpressure" {
		fmt.Fprintf(out, "\nProgress is saved. Retry later:\n  rydd --data-dir %s scan -d %s -s %d\n", shellQuote(base), shellQuote(r.Directory), r.SleepMS)
	}
	fmt.Fprintf(out, "\nView the saved report:\n  rydd --data-dir %s report -d %s\n", shellQuote(base), shellQuote(r.Directory))
	fmt.Fprintf(out, "\nReview saved node_modules candidates:\n  rydd --data-dir %s review -d %s\n", shellQuote(base), shellQuote(r.Directory))
}

// Refresh is selected before any storage or source operation. Flag operands
// remain operands, even when their spelling is --refresh or --json.
func scanRefreshArgs(args []string) ([]string, bool, error) {
	filtered := make([]string, 0, len(args))
	seen, selected, positional := false, false, false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			filtered = append(filtered, args[i:]...)
			break
		}
		if a == "-d" || a == "--d" || a == "-directory" || a == "--directory" || a == "-s" || a == "--s" || a == "-sleep" || a == "--sleep" {
			filtered = append(filtered, a)
			if i+1 < len(args) {
				i++
				filtered = append(filtered, args[i])
			}
			continue
		}
		if a == "--refresh" || strings.HasPrefix(a, "--refresh=") {
			if seen || positional {
				return nil, false, usageError{errors.New("scan accepts --refresh once, before operands")}
			}
			seen = true
			selected = true
			if a != "--refresh" {
				value := strings.TrimPrefix(a, "--refresh=")
				if value != "true" && value != "false" {
					return nil, false, usageError{errors.New("--refresh accepts only true or false")}
				}
				selected = value == "true"
			}
			continue
		}
		if a == "" || a[0] != '-' {
			positional = true
		}
		filtered = append(filtered, a)
	}
	return filtered, selected, nil
}

type scanRefreshOptions struct {
	path      string
	delay     int
	requested *bool
}

func parseScanRefreshOptions(args []string) (scanRefreshOptions, error) {
	var o scanRefreshOptions
	f := flag.NewFlagSet("scan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.StringVar(&o.path, "d", "", "")
	f.StringVar(&o.path, "directory", "", "")
	f.IntVar(&o.delay, "s", 10, "")
	f.IntVar(&o.delay, "sleep", 10, "")
	compact := f.Bool("compact", false, "")
	detailed := f.Bool("detailed", false, "")
	now := f.Bool("now", false, "")
	if err := f.Parse(args); err != nil {
		return o, usageError{err}
	}
	seen := map[string]bool{}
	f.Visit(func(v *flag.Flag) { seen[v.Name] = true })
	if (seen["compact"] && seen["detailed"]) || f.NArg() != 0 || o.path == "" || o.delay < 0 || o.delay > 60000 || (*now && (seen["s"] || seen["sleep"])) || (seen["d"] && seen["directory"]) || (seen["s"] && seen["sleep"]) {
		return o, usageError{errors.New("scan accepts --refresh, -d PATH and -s 0–60000 milliseconds or --now; do not combine aliases")}
	}
	if *now {
		o.delay = 0
	}
	if seen["compact"] {
		o.requested = compact
	}
	if seen["detailed"] {
		value := !*detailed
		o.requested = &value
	}
	return o, nil
}

func validateScanRefreshArgs(args []string) error {
	filtered, selected, err := scanRefreshArgs(args)
	if err != nil || !selected {
		return err
	}
	_, err = parseScanRefreshOptions(filtered)
	return err
}

// Test seams are private, nil by default, and called only by the selected route.
// They do not add work or change the original scan API.
type scanRefreshHooks struct {
	afterActivation func(*state.Store, *state.ManualRefreshActivation, *state.ManualRefreshReceipt) error
	afterCommit     func(*state.ManualRefreshReceipt) error
	closeStore      func(*state.Store) error
}

func scanWithRefresh(ctx context.Context, args []string, paths config.Paths, progress io.Writer) (ScanReport, *state.ManualRefreshReceipt, error) {
	filtered, selected, err := scanRefreshArgs(args)
	if err != nil {
		return ScanReport{}, nil, err
	}
	if !selected {
		r, err := scan(ctx, filtered, paths, progress)
		return r, nil, err
	}
	return scanSelectedRefresh(ctx, filtered, paths, progress, scanRefreshHooks{})
}

func scanSelectedRefresh(ctx context.Context, args []string, paths config.Paths, progress io.Writer, hooks scanRefreshHooks) (r ScanReport, receipt *state.ManualRefreshReceipt, retErr error) {
	o, err := parseScanRefreshOptions(args)
	if err != nil {
		return r, nil, err
	}
	root, err := directoryPath(o.path)
	if err != nil {
		return r, nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return r, nil, err
	}
	if !info.IsDir() {
		return r, nil, usageError{errors.New("scan target must be a directory")}
	}
	r.Directory, r.DirectoryBytes, r.StateDir, r.SleepMS = root, []byte(root), manualState(paths, root), o.delay
	r.Mode = "resume"
	home, err := os.UserHomeDir()
	if err != nil {
		return r, nil, err
	}
	var excludes []string
	cfg, err := config.Load(paths.ConfigFile, home)
	if err == nil {
		excludes = cfg.Excludes
	} else if !errors.Is(err, os.ErrNotExist) {
		return r, nil, err
	}
	for _, dir := range []string{paths.StateDir, filepath.Join(paths.StateDir, "manual")} {
		if err = localfs.EnsurePrivateDir(dir); err != nil {
			return r, nil, err
		}
	}
	w, err := state.OpenWriter(ctx, r.StateDir)
	if err != nil {
		return r, nil, err
	}
	defer func() {
		var closeErr error
		if hooks.closeStore != nil {
			closeErr = hooks.closeStore(w)
		} else {
			closeErr = w.Close()
		}
		retErr = errors.Join(retErr, closeErr)
		if ctx.Err() != nil {
			retErr = errors.Join(retErr, ctx.Err())
		}
		if receipt != nil {
			retErr = errors.Join(retErr, scanRefreshReceiptBound(receipt))
		}
	}()
	session, known, err := w.PreflightManualRefresh(ctx, []byte(root), o.requested)
	receipt = known
	if err != nil {
		return r, receipt, err
	}
	r.Compact, err = w.ConfigureCompact(ctx, o.requested)
	if err != nil {
		return r, receipt, err
	}
	if err = w.SyncRoots(ctx, []string{root}); err != nil {
		return r, receipt, err
	}
	nextReceipt, err := w.RequestManualRefresh(ctx, session)
	receipt = keepScanRefreshReceipt(receipt, nextReceipt)
	if err != nil {
		return r, receipt, err
	}
	if receipt == nil {
		return r, nil, errors.New("manual refresh request returned no receipt")
	}
	display := &reviewOutput{writer: progress}
	fmt.Fprintf(display, "Scanning folder:\n%q\n", root)
	printWrapped(display, fmt.Sprintf("Entry delay: %d ms. Press Ctrl+C to stop. Progress already saved is kept.", o.delay), "")
	printWrapped(display, "Refresh requested. Saved work finishes before one additional metadata pass.", "")
	if display.err != nil {
		return r, receipt, display.err
	}
	scanner, err := inventory.New([]string{root}, excludes, []string{paths.StateDir, paths.ConfigFile}, inventory.WithEntryDelay(time.Duration(o.delay)*time.Millisecond))
	if err != nil {
		return r, receipt, err
	}
	defer scanner.Close()
	if _, err = w.RecoverJobs(ctx, time.Now()); err != nil {
		return r, receipt, err
	}
	var activation *state.ManualRefreshActivation
	lastProgress := time.Now()
	for {
		if err = ctx.Err(); err != nil {
			return r, receipt, err
		}
		if receipt.Phase == "pending" {
			nextReceipt, nextActivation, err := w.TryActivateManualRefresh(ctx, session, time.Now())
			receipt = keepScanRefreshReceipt(receipt, nextReceipt)
			activation = nextActivation
			if err != nil {
				return r, receipt, err
			}
			if activation != nil {
				r.Mode = "new_pass"
				if hooks.afterActivation != nil {
					if err = hooks.afterActivation(w, activation, receipt); err != nil {
						return r, receipt, err
					}
				}
				printWrapped(display, "Starting the selected refresh pass.", "")
				if display.err != nil {
					return r, receipt, display.err
				}
			}
		}
		blocked, err := w.WALBlocked(ctx, state.WALBackpressureBytes)
		if err != nil {
			return r, receipt, err
		}
		if blocked {
			r.Outcome = "wal_backpressure"
			break
		}
		j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			return r, receipt, err
		}
		if j == nil {
			worked, err := w.RetireCompact(ctx)
			if err != nil {
				return r, receipt, err
			}
			if worked {
				r.RetirementBatches++
			} else {
				worked, err = w.RetireSubtrees(ctx)
				if err != nil {
					return r, receipt, err
				}
				if worked {
					r.SubtreeRetirementBatches++
				} else {
					worked, err = w.ReduceAllocations(ctx)
					if err != nil {
						return r, receipt, err
					}
					if worked {
						r.AllocationBatches++
					}
				}
			}
			if worked {
				timer := time.NewTimer(time.Duration(max(o.delay, 1)) * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return r, receipt, ctx.Err()
				case <-timer.C:
				}
				continue
			}
			nextReceipt, err := w.FinalizeManualRefresh(ctx, session)
			receipt = keepScanRefreshReceipt(receipt, nextReceipt)
			if err != nil {
				return r, receipt, err
			}
			if receipt.Phase == "pending" {
				// Maintenance just drained. Admission, not broad seeding, decides
				// whether the one selected pass can start now.
				nextReceipt, nextActivation, err := w.TryActivateManualRefresh(ctx, session, time.Now())
				receipt = keepScanRefreshReceipt(receipt, nextReceipt)
				activation = nextActivation
				if err != nil {
					return r, receipt, err
				}
				if activation != nil {
					r.Mode = "new_pass"
					if hooks.afterActivation != nil {
						if err = hooks.afterActivation(w, activation, receipt); err != nil {
							return r, receipt, err
						}
					}
					continue
				}
			}
			r.Outcome = "pending_retry"
			if receipt.Phase == "completed" {
				r.Outcome = "queue_drained"
			}
			break
		}
		chunk, cancel := context.WithTimeout(ctx, 10*time.Second)
		type nextResult struct {
			batch state.ScanBatch
			err   error
		}
		done := make(chan nextResult, 1)
		go func() { b, e := scanner.Next(chunk, *j); done <- nextResult{b, e} }()
		var batch state.ScanBatch
		var nextErr error
		select {
		case next := <-done:
			batch, nextErr = next.batch, next.err
		case <-chunk.Done():
			nextErr = chunk.Err()
		}
		cancel()
		if nextErr != nil {
			cleanup, finish := context.WithTimeout(context.Background(), time.Second)
			finishErr := w.FinishJob(cleanup, *j, false, j.Cursor, time.Unix(0, 1), nextErr.Error())
			finish()
			return r, receipt, errors.Join(nextErr, finishErr)
		}
		if activation != nil && j.ID == receipt.ActivatedRootJobID && string(j.Path) == "." {
			nextReceipt, commitErr := w.CommitManualRefreshScan(ctx, activation, *j, batch)
			receipt = keepScanRefreshReceipt(receipt, nextReceipt)
			err = commitErr
		} else {
			err = w.CommitScan(ctx, *j, batch)
		}
		if err != nil {
			return r, receipt, err
		}
		if receipt.RootListingEOFWitness {
			activation = nil
		}
		r.Batches++
		if hooks.afterCommit != nil {
			if err = hooks.afterCommit(receipt); err != nil {
				return r, receipt, err
			}
		}
		if time.Since(lastProgress) >= time.Second {
			fmt.Fprintf(display, "Saved %d batches.\n", r.Batches)
			lastProgress = time.Now()
			if display.err != nil {
				return r, receipt, display.err
			}
		}
	}
	r.Inventory, err = w.Summary(ctx)
	if err != nil {
		return r, receipt, err
	}
	if _, err = w.Checkpoint(ctx); err != nil {
		return r, receipt, err
	}
	return r, receipt, nil
}

func keepScanRefreshReceipt(known, next *state.ManualRefreshReceipt) *state.ManualRefreshReceipt {
	if next != nil {
		return next
	}
	return known
}

func scanRefreshReceiptBound(receipt *state.ManualRefreshReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil || len(data) > 1024 {
		return errors.New("manual refresh receipt exceeds its 1024-byte reply bound")
	}
	return nil
}

func scanRefreshReplyMessage(receipt *state.ManualRefreshReceipt) string {
	return fmt.Sprintf("Manual refresh request %d for inventory %s, root %d: %s. Reply failure does not undo saved evidence. Keep this identity; a later explicit refresh can create another request.", receipt.RequestGeneration, receipt.InventoryIncarnationID, receipt.RootID, receipt.Phase)
}

func printScanRefreshReceipt(out io.Writer, receipt *state.ManualRefreshReceipt) {
	fmt.Fprintf(out, "\nManual refresh: %s\n", receipt.Phase)
	printField(out, "Request generation", fmt.Sprint(receipt.RequestGeneration))
	printField(out, "Inventory incarnation", receipt.InventoryIncarnationID)
	printField(out, "Root ID", fmt.Sprint(receipt.RootID))
	printWrapped(out, "Completion describes a historical metadata pass. It does not prove current coverage, inactivity or safe cleanup.", "")
}
