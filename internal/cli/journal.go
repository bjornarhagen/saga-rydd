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
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

type ObservedJournal struct {
	IntentID      string                   `json:"intent_id"`
	SavedState    string                   `json:"saved_state"`
	ReferenceKind string                   `json:"reference_kind"`
	ReferenceID   string                   `json:"reference_id"`
	Observation   inventory.RecoveryReport `json:"observation"`
}

func journal(ctx context.Context, args []string, paths config.Paths) (any, error) {
	f := flag.NewFlagSet("journal", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	id := f.String("show", "", "read a saved preparation and its recorded history; no source operations")
	observe := f.String("observe", "", "observe metadata at recorded recovery locations; no operations or saved changes")
	if err := f.Parse(args); err != nil {
		return nil, usageError{err}
	}
	modeCount := 0
	for _, arg := range args {
		for _, mode := range []string{"show", "observe"} {
			if arg == "--"+mode || arg == "-"+mode || strings.HasPrefix(arg, "--"+mode+"=") || strings.HasPrefix(arg, "-"+mode+"=") {
				modeCount++
			}
		}
	}
	selected := *id
	if *observe != "" {
		selected = *observe
	}
	if modeCount != 1 || f.NArg() != 0 || !plans.ValidJournalID(selected) {
		return nil, usageError{errors.New("journal requires --show or --observe with one full journal intent ID and no directory or selection options")}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	snapshot, err := plans.LoadJournal(ctx, paths.StateDir, selected)
	if err != nil || *observe == "" {
		return snapshot, err
	}
	r := ObservedJournal{IntentID: snapshot.Intent.ID, SavedState: snapshot.State, ReferenceKind: "preparation", ReferenceID: snapshot.Intent.ID}
	request := snapshot.Intent.Record.Request
	object := request.SourceObject
	for _, event := range snapshot.Events {
		if event.Record.Request.ObjectIdentity != nil {
			object = *event.Record.Request.ObjectIdentity
			r.ReferenceKind, r.ReferenceID = "recorded_event", event.ID
		}
	}
	identity := func(v plans.JournalIdentity) inventory.RecoveryIdentity {
		return inventory.RecoveryIdentity{Device: v.Device, Inode: v.Inode, ChangedNS: v.ChangedNS, Generation: v.Generation}
	}
	r.Observation, err = inventory.ObserveRecovery(ctx, inventory.RecoveryRequest{SourcePathBytes: request.SourcePathBytes, DestinationPathBytes: request.DestinationPathBytes, SourceParent: identity(request.SourceParent), SourceObject: identity(object), DestinationParent: identity(request.DestinationParent)})
	return r, err
}

func printJournal(out io.Writer, result any) {
	if r, ok := result.(ObservedJournal); ok {
		printObservedJournal(out, r)
		return
	}
	r := result.(plans.JournalSnapshot)
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

func printObservedJournal(out io.Writer, r ObservedJournal) {
	printWrapped(out, "Saga — Rydd: recovery location observations", "")
	printResultBanner(out, "LOCATIONS OBSERVED - OUTCOME REMAINS UNKNOWN")
	fmt.Fprintf(out, "Journal: %s\n", r.IntentID)
	printField(out, "Saved state", r.SavedState)
	printField(out, "Reference", r.ReferenceKind)
	printField(out, "Observed", r.Observation.CheckedAt.Format(time.RFC3339))
	for _, location := range []struct {
		name string
		item inventory.RecoveryLocation
	}{{"Source", r.Observation.SourceLocation}, {"Destination", r.Observation.DestinationLocation}} {
		printField(out, location.name+" path", strconv.Quote(string(location.item.PathBytes)))
		printField(out, location.name, location.item.Message)
	}
	printWrapped(out, "These are current metadata observations compared with supplied history. They do not prove which object moved or establish the current cleanup outcome.", "")
	printWrapped(out, "No retry or restore is enabled. No files or saved records were changed by this command.", "")
}
