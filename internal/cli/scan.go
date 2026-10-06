package cli

import (
	"context"
	"crypto/sha256"
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
