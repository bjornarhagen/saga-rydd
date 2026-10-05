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

type MeasureReport struct {
	Directory string `json:"directory"`
	StateDir  string `json:"state_dir"`
	Batches   int    `json:"batches"`
	Complete  bool   `json:"complete"`
	Outcome   string `json:"outcome"`
}

// measure writes only derived state for an existing exact manual compact root.
// It never creates a scan job or accesses the selected folder's contents.
func measure(ctx context.Context, args []string, paths config.Paths) (MeasureReport, error) {
	r := MeasureReport{}
	f := flag.NewFlagSet("measure", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	directory := f.String("directory", "", "exact saved manual compact scan root")
	f.StringVar(directory, "d", "", "directory alias")
	batches := f.Int("batches", 128, "maximum calculation batches (1–1000)")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	aliases := 0
	f.Visit(func(v *flag.Flag) {
		if v.Name == "directory" || v.Name == "d" {
			aliases++
		}
	})
	if f.NArg() != 0 || aliases != 1 || *batches < 1 || *batches > 1000 {
		return r, usageError{errors.New("measure requires -d PATH and accepts --batches 1–1000")}
	}
	root, err := directoryPath(*directory)
	if err != nil {
		return r, err
	}
	r.Directory = root
	r.StateDir = manualState(paths, root)
	// A reader verifies an existing owned store before a writer can migrate it.
	reader, err := state.OpenReader(ctx, r.StateDir)
	if err != nil {
		return r, err
	}
	compact, err := reader.ConfigureCompact(ctx, nil)
	reader.Close()
	if err != nil {
		return r, err
	}
	if !compact {
		return r, usageError{errors.New("measure requires an existing compact manual inventory")}
	}
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	w, err := state.OpenWriter(budget, r.StateDir)
	if err != nil {
		return r, err
	}
	defer w.Close()
	// Check again under the writer lock; another writer may have changed mode.
	compact, err = w.ConfigureCompact(budget, nil)
	if err != nil {
		return r, err
	}
	if !compact {
		return r, errors.New("saved inventory is no longer compact")
	}
	due, err := w.NextJobDue(budget, []string{state.ScanKind})
	if err != nil {
		return r, err
	}
	retiring, err := w.HasCompactRetirement(budget)
	if err != nil {
		return r, err
	}
	subtrees, err := w.HasSubtreeRetirement(budget)
	if err != nil {
		return r, err
	}
	if !due.IsZero() || retiring || subtrees {
		return r, errors.New("finish the pending manual scan with scan -d PATH before measuring saved inventory")
	}
	r.Outcome = "batch_limit"
	for r.Batches < *batches {
		blocked, e := w.WALBlocked(budget, state.WALBackpressureBytes)
		if e != nil {
			err = e
			break
		}
		if blocked {
			r.Outcome = "wal_backpressure"
			return r, nil
		}
		worked, e := w.ReduceAllocations(budget)
		if e != nil {
			err = e
			break
		}
		if !worked {
			r.Complete = true
			r.Outcome = "complete"
			return r, nil
		}
		r.Batches++
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			r.Outcome = "time_limit"
			return r, nil
		}
		return r, err
	}
	pending, err := w.HasAllocationWork(budget)
	if err != nil {
		return r, err
	}
	if !pending {
		r.Complete = true
		r.Outcome = "complete"
	}
	return r, nil
}

func printMeasureReport(out io.Writer, r MeasureReport) {
	if r.Complete {
		printWrapped(out, "Saved size calculations are complete.", "")
	} else {
		printWrapped(out, "More size calculations remain. Progress is saved.", "")
		switch r.Outcome {
		case "batch_limit":
			printWrapped(out, "This run reached its work limit.", "")
		case "time_limit":
			printWrapped(out, "This run reached its time limit.", "")
		case "wal_backpressure":
			printWrapped(out, "The saved database needs to finish writing pending changes. Retry later.", "")
		}
	}
	fmt.Fprintf(out, "%q\n", r.Directory)
	printWrapped(out, "Only saved scan data was used. Current contents have not been checked. No files were deleted.", "")
	base := filepath.Dir(filepath.Dir(r.StateDir))
	if !r.Complete {
		fmt.Fprintf(out, "\nContinue the calculation:\n  rydd --data-dir %s measure -d %s\n", shellQuote(base), shellQuote(r.Directory))
	}
	fmt.Fprintf(out, "\nView the saved report:\n  rydd --data-dir %s report -d %s\n", shellQuote(base), shellQuote(r.Directory))
}
