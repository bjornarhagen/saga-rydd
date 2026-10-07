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
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// HashReport may select one work item, but its budget always describes the
// entire saved selection. No result is a current source observation.
type HashReport struct {
	inventory.HashSnapshot
	SelectedWorkID string `json:"selected_work_id,omitempty"`
	BudgetScope    string `json:"budget_scope"`
}

type missingHashError struct{ error }

func (e missingHashError) Unwrap() error { return e.error }

func hashes(ctx context.Context, args []string, paths config.Paths) (HashReport, error) {
	f := flag.NewFlagSet("hashes", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	workID := f.String("work", "", "show one saved work ID (1–20); whole-selection budget remains visible")
	if err := f.Parse(args); err != nil {
		return HashReport{}, usageError{err}
	}
	workFlags := 0
	for _, arg := range args {
		if arg == "--work" || arg == "-work" || strings.HasPrefix(arg, "--work=") || strings.HasPrefix(arg, "-work=") {
			workFlags++
		}
	}
	if f.NArg() != 0 || workFlags > 1 {
		return HashReport{}, usageError{errors.New("hashes accepts only one optional --work WORK_ID; no directory or source options")}
	}
	if workFlags != 0 {
		ordinal, err := strconv.Atoi(*workID)
		if err != nil || ordinal < 1 || ordinal > inventory.FileSampleTargetLimit || strconv.Itoa(ordinal) != *workID {
			return HashReport{}, usageError{errors.New("--work requires a canonical saved work ID from 1 to 20")}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashReport{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("saved hash storage is unavailable; hashes only reads existing observations: %w", err)}
		}
		return HashReport{}, err
	}
	snapshot, err := reader.Snapshot(ctx)
	closeErr := reader.Close()
	if err != nil {
		return HashReport{}, err
	}
	if closeErr != nil {
		return HashReport{}, closeErr
	}
	if err := ctx.Err(); err != nil {
		return HashReport{}, err
	}
	r := HashReport{HashSnapshot: snapshot, SelectedWorkID: *workID, BudgetScope: "whole_saved_selection"}
	if *workID != "" {
		for _, work := range snapshot.Work {
			if work.ID == *workID {
				r.Work = []inventory.SavedHashWork{work}
				return r, nil
			}
		}
		return HashReport{}, missingHashError{fmt.Errorf("saved hash work %s is absent from this selection: %w", *workID, os.ErrNotExist)}
	}
	return r, nil
}

func printHashes(out io.Writer, r HashReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved hash observations", "")
	printResultBanner(guard, "SAVED HASH OBSERVATIONS - CURRENT FILES NOT CHECKED")
	printWrapped(guard, "This reads saved records only. A completed digest is a historical full-read observation. It does not prove current contents, duplicate files or safe cleanup.", "")
	fmt.Fprintf(guard, "Store: %s\n", r.StoreID)
	if r.SelectionID == "" {
		printWrapped(guard, "No selection is saved in this hash store. This command does not create or start hashing work.", "")
	} else {
		fmt.Fprintf(guard, "Selection: %s\n", r.SelectionID)
		if r.SelectedWorkID != "" {
			printField(guard, "Selected work", r.SelectedWorkID)
		}
		printField(guard, "Saved work shown", len(r.Work))
		for _, work := range r.Work {
			fmt.Fprintf(guard, "\nWork %s\n  %q\n", work.ID, string(work.PathBytes))
			printField(guard, "Saved state", savedHashStateLabel(work.Status))
			printField(guard, "Logical file size", humanBytes(work.LogicalBytes))
			printField(guard, "Saved prefix", humanBytes(work.DurableOffset))
			if work.CheckedAt.IsZero() {
				printField(guard, "Checked at", "No checked prefix recorded")
			} else {
				printField(guard, "Checked at", work.CheckedAt.UTC().Format(time.RFC3339Nano))
			}
			if work.SHA256 != "" {
				printField(guard, "Historical SHA-256", work.SHA256)
			}
			if work.Code != "" {
				printField(guard, "Recorded reason", work.Code)
			}
			printHashAttempt(guard, work.LatestAttempt)
		}
	}
	printHashBudget(guard, r.Budget)
	printHashReadConsent(guard, r.ReadConsent)
	printWrapped(guard, "Saved running state does not prove a process is active. This command does not recover, resume or start work. No source files or saved records were changed.", "")
	if guard.err != nil {
		return fmt.Errorf("write hash report: %w", guard.err)
	}
	return nil
}

func savedHashStateLabel(status string) string {
	switch status {
	case "pending":
		return "Pending in saved queue"
	case "running":
		return "Running in saved records; active process unknown"
	case "complete":
		return "Full read recorded"
	case "invalidated":
		return "Invalidated; saved prefix cannot establish current contents"
	default:
		return "Unknown"
	}
}

func printHashAttempt(out io.Writer, attempt *inventory.HashAttempt) {
	if attempt == nil {
		printField(out, "Latest attempt", "No attempt recorded")
		return
	}
	label := "Reserved; usage not settled"
	if attempt.Status == "settled" {
		label = "Settled; usage recorded"
	} else if attempt.Status == "interrupted_unknown" {
		label = "Interrupted; usage unknown"
	}
	printField(out, "Latest attempt", label)
	printField(out, "Reservation day (UTC)", attempt.ReservationDay)
	printField(out, "Reserved allowance", humanBytes(attempt.ReservedBytes))
	printField(out, "Observed requested", savedBytes(attempt.RequestedBytes))
	printField(out, "Observed read", savedBytes(attempt.ReadBytes))
	if attempt.ElapsedNS == nil {
		printField(out, "Observed elapsed", "Unknown")
	} else {
		printField(out, "Observed elapsed", time.Duration(*attempt.ElapsedNS).String())
	}
}

func printHashBudget(out io.Writer, budget *inventory.HashBudget) {
	printResultBanner(out, "WHOLE-SELECTION RESERVATION BUDGET")
	if budget == nil {
		printWrapped(out, "No reservation is recorded for this selection.", "")
		return
	}
	printField(out, "Saved day (UTC)", budget.Day)
	printField(out, "Latest reservation time", budget.MaxNow.UTC().Format(time.RFC3339Nano))
	for _, item := range []struct {
		label string
		value int64
	}{
		{"Day reserved", budget.ReservedBytes},
		{"Day requested, known", budget.RequestedBytes},
		{"Day read, known", budget.ReadBytes},
		{"Day interrupted charge", budget.UnknownReservedBytes},
		{"Total reserved", budget.TotalReservedBytes},
		{"Total requested, known", budget.TotalRequestedBytes},
		{"Total read, known", budget.TotalReadBytes},
		{"Total interrupted charge", budget.TotalUnknownReservedBytes},
	} {
		printField(out, item.label, humanBytes(item.value))
	}
	printWrapped(out, "These counters cover the whole saved selection, including work not shown. Reservations are full charged allowances and are never refunded. Interrupted charges count recovered attempts with unknown usage. Unsettled reservations remain charged but are excluded from the interrupted counters. Known counters omit attempts with unknown usage. They do not measure physical I/O or reads per wall-clock day. The budget itself does not store a limit or calculate remaining quota.", "")
}
