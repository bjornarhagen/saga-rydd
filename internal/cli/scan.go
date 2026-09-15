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
	Directory      string        `json:"directory"`
	DirectoryBytes []byte        `json:"directory_bytes"`
	StateDir       string        `json:"state_dir"`
	SleepMS        int           `json:"sleep_ms"`
	Outcome        string        `json:"outcome"`
	Batches        int           `json:"batches"`
	Inventory      state.Summary `json:"inventory"`
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
	now := f.Bool("now", false, "disable deliberate entry delays")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	seen := map[string]bool{}
	f.Visit(func(v *flag.Flag) { seen[v.Name] = true })
	if f.NArg() != 0 || delay < 0 || delay > 60000 || (*now && (seen["s"] || seen["sleep"])) || (seen["d"] && seen["directory"]) || (seen["s"] && seen["sleep"]) {
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
	if err = w.SeedInventory(ctx); err != nil {
		return r, err
	}
	fmt.Fprintf(progress, "Scanning %q; entry delay %d ms. Ctrl+C stops; committed batches remain saved.\n", root, delay)
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
			finishErr := w.FinishJob(cleanup, *j, false, j.Cursor, time.Now(), e.Error())
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
	fmt.Fprintf(out, "Scan stopped: %s. Saved observations: %d; pending jobs: %d; directory errors: %d; skipped: %d.\nState: %q\nView: rydd --data-dir %q report -d %q\nSaved observations are not verified current contents; use the report for size completeness.\n", r.Outcome, r.Inventory.Entries, r.Inventory.PendingJobs, r.Inventory.DirectoryErrors, r.Inventory.SkippedEntries, r.StateDir, r.StateDir, r.Directory)
}
