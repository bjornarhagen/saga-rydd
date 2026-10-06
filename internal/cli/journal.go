package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

func journal(ctx context.Context, args []string, paths config.Paths) (plans.JournalSnapshot, error) {
	f := flag.NewFlagSet("journal", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	id := f.String("show", "", "read a saved preparation and its recorded history; no source operations")
	if err := f.Parse(args); err != nil {
		return plans.JournalSnapshot{}, usageError{err}
	}
	showCount := 0
	for _, arg := range args {
		if arg == "--show" || arg == "-show" || strings.HasPrefix(arg, "--show=") || strings.HasPrefix(arg, "-show=") {
			showCount++
		}
	}
	if showCount != 1 || f.NArg() != 0 || !plans.ValidJournalID(*id) {
		return plans.JournalSnapshot{}, usageError{errors.New("journal requires --show with one full journal intent ID and no directory or selection options")}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return plans.LoadJournal(ctx, paths.StateDir, *id)
}

func printJournal(out io.Writer, r plans.JournalSnapshot) {
	printWrapped(out, "Saga — Rydd: saved recovery record", "")
	switch r.State {
	case "prepared":
		printResultBanner(out, "PREPARATION RECORD - NO OPERATION ENABLED")
	case "recorded_at_source":
		printResultBanner(out, "RECORDED OBSERVATION: SOURCE LOCATION")
	case "recorded_at_destination":
		printResultBanner(out, "RECORDED OBSERVATION: DESTINATION LOCATION")
	case "recorded_conflict":
		printResultBanner(out, "RECORDED LOCATIONS CONFLICT - REVIEW REQUIRED")
	default:
		printResultBanner(out, "RECORDED OUTCOME UNKNOWN - REVIEW REQUIRED")
	}
	intent := r.Intent.Record
	request := intent.Request
	fmt.Fprintf(out, "Journal: %s\n", r.Intent.ID)
	fmt.Fprintf(out, "Plan: %s\n", request.PlanID)
	printField(out, "Finding", request.FindingID)
	printField(out, "Preparation", request.Kind)
	printField(out, "Recorded", intent.CreatedAt.Format(time.RFC3339))
	printField(out, "State", r.State)
	printField(out, "Recorded events", len(r.Events))
	printField(out, "Source", strconv.Quote(string(request.SourcePathBytes)))
	printField(out, "Destination", strconv.Quote(string(request.DestinationPathBytes)))
	printWrapped(out, "This shows saved preparation and caller-supplied observation records. It does not inspect either location or establish the current filesystem outcome.", "")
	printWrapped(out, "The preparation contract cannot authorize an operation. Missing results do not prove failure or permit retry. No files or saved records were changed by this command.", "")
}
