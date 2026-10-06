package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

func observedJournalCLIResult(t *testing.T, f reviewCLI, id string) ObservedJournal {
	t.Helper()
	code, raw := f.run("journal", "--observe", id, "--json")
	var result struct{ Journal ObservedJournal }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	return result.Journal
}

func TestJournalObserveLocationsAndUnchangedHistory(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	hashes := observationCLIHashes(t, f.base)
	observed := observedJournalCLIResult(t, f, snapshot.Intent.ID)
	r := observed.Observation
	if observed.IntentID != snapshot.Intent.ID || observed.SavedState != "prepared" || observed.ReferenceKind != "preparation" || observed.ReferenceID != snapshot.Intent.ID || r.Status != "outcome_unknown" || r.CurrentStateVerified || r.Executable || r.HistoricalMountVerified || r.ScopeVerified || r.Source != "live_recovery_metadata" || r.CheckedAt.IsZero() || r.SourceLocation.Status != "observed_present" || r.DestinationLocation.Status != "observed_absent" || r.SourceLocation.IdentityRelation != "metadata_matches_reference" || !r.SourceLocation.ParentCtimeChanged {
		t.Fatalf("unexpected recovery observation: %+v", observed)
	}
	code, human := f.run("journal", "--observe", snapshot.Intent.ID)
	plain := strings.Join(strings.Fields(human), " ")
	if code != 0 || !strings.Contains(human, "LOCATIONS OBSERVED - OUTCOME REMAINS UNKNOWN") || !strings.Contains(plain, "No retry or restore is enabled") || !strings.Contains(plain, "No files or saved records were changed") {
		t.Fatal(code, human)
	}
	if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("observation wrote saved records")
	}
	request := snapshot.Intent.Record.Request
	if err := os.Rename(string(request.SourcePathBytes), string(request.DestinationPathBytes)); err != nil {
		t.Fatal(err)
	}
	// This is a disposable fixture move, not a production operation. The
	// journal must not infer or record success from the two live locations.
	observed = observedJournalCLIResult(t, f, snapshot.Intent.ID)
	if observed.SavedState != "prepared" || observed.Observation.Status != "outcome_unknown" || observed.Observation.SourceLocation.Status != "observed_absent" || observed.Observation.DestinationLocation.Status != "observed_present" || observed.Observation.Executable || observed.Observation.CurrentStateVerified {
		t.Fatal(observed)
	}
	saved, err := plans.LoadJournal(context.Background(), f.base, snapshot.Intent.ID)
	if err != nil || !reflect.DeepEqual(snapshot, saved) || !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("observation changed journal", saved, err)
	}
	body, err := os.ReadFile(filepath.Join(string(request.DestinationPathBytes), "fixture-dep", "bin.js"))
	if err != nil || string(body) != string(treeDependencyContents) {
		t.Fatal("fixture contents changed", err)
	}
}

func TestJournalObserveLatestRecordedReferenceAndReviewIndependence(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	if code, raw := f.run(f.approveArgs()...); code != 0 {
		t.Fatal(code, raw)
	}
	if code, raw := f.run("plan", "--revoke", f.saved.ID, "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	attempt := plans.EventRequest{RequestKey: strings.Repeat("b", 64), ExpectedSequence: 1, PredecessorID: snapshot.Intent.ID, Kind: "attempt_recorded", Outcome: "not_observed", SourceState: "unknown", DestinationState: "unknown"}
	snapshot, err := plans.AppendJournalEvent(context.Background(), f.base, snapshot.Intent.ID, attempt)
	if err != nil {
		t.Fatal(err)
	}
	object := snapshot.Intent.Record.Request.SourceObject
	object.ChangedNS++
	result := plans.EventRequest{RequestKey: strings.Repeat("c", 64), ExpectedSequence: 2, PredecessorID: snapshot.Events[0].ID, Kind: "result_recorded", Outcome: "recorded_at_destination", SourceState: "absent", DestinationState: "present", ObjectIdentity: &object}
	snapshot, err = plans.AppendJournalEvent(context.Background(), f.base, snapshot.Intent.ID, result)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	inventory := manualState(paths, f.root)
	if err := os.Rename(inventory, inventory+".offline"); err != nil {
		t.Fatal(err)
	}
	hashes := observationCLIHashes(t, f.base)
	observed := observedJournalCLIResult(t, f, snapshot.Intent.ID)
	if observed.SavedState != "recorded_at_destination" || observed.ReferenceKind != "recorded_event" || observed.ReferenceID != snapshot.Events[1].ID || observed.Observation.SourceLocation.Status != "observed_present" || observed.Observation.DestinationLocation.Status != "observed_absent" || observed.Observation.Status != "outcome_unknown" {
		t.Fatal("caller-supplied outcome was mistaken for current state", observed)
	}
	if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("observation changed offline inventory or journal")
	}
	if _, err := os.Stat(inventory); !os.IsNotExist(err) {
		t.Fatal("observation initialized missing inventory", err)
	}
}

func TestJournalObserveMissingAncestorsAndArguments(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	destinationParent := filepath.Dir(string(snapshot.Intent.Record.Request.DestinationPathBytes))
	if err := os.Rename(destinationParent, destinationParent+".offline"); err != nil {
		t.Fatal(err)
	}
	observed := observedJournalCLIResult(t, f, snapshot.Intent.ID)
	for _, status := range []string{observed.Observation.SourceLocation.Status, observed.Observation.DestinationLocation.Status} {
		if status != "unavailable" && status != "blocked" {
			t.Fatal("missing ancestor was treated as object absence", observed)
		}
	}
	for _, args := range [][]string{{"--observe"}, {"--observe", "invalid"}, {"--observe", snapshot.Intent.ID, "--observe", snapshot.Intent.ID}, {"--observe", snapshot.Intent.ID, "--show", snapshot.Intent.ID}, {"--observe", snapshot.Intent.ID, "-d", f.root}, {"--observe", snapshot.Intent.ID, "extra"}, {"--observe", snapshot.Intent.ID, "--execute"}} {
		args = append([]string{"--json", "journal"}, args...)
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	code, raw := f.run("capabilities", "--json")
	var capabilities struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || !capabilities.Features["journal_location_observations"] || capabilities.Features["cleanup"] || !strings.Contains(raw, "--observe INTENT_ID") {
		t.Fatal(code, raw, err)
	}
}

func TestJournalObserveJSONLookingFlagValue(t *testing.T) {
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"journal", "--observe", "--json"}, &out, &stderr)
	if code != 2 || out.Len() != 0 || !strings.Contains(stderr.String(), "one full journal intent ID") {
		t.Fatal(code, out.String(), stderr.String())
	}
}

func TestJournalObserveLinkedRestoreUsesOwnLocationsAndHistory(t *testing.T) {
	f, original := journalCLIFixture(t)
	q := original.Intent.Record.Request
	if err := os.Rename(string(q.SourcePathBytes), string(q.DestinationPathBytes)); err != nil {
		t.Fatal(err)
	}
	appendEvent := func(snapshot plans.JournalSnapshot, key, kind string, object *plans.JournalIdentity) plans.JournalSnapshot {
		t.Helper()
		predecessor := snapshot.Intent.ID
		if len(snapshot.Events) > 0 {
			predecessor = snapshot.Events[len(snapshot.Events)-1].ID
		}
		request := plans.EventRequest{RequestKey: strings.Repeat(key, 64), ExpectedSequence: len(snapshot.Events) + 1, PredecessorID: predecessor, Kind: kind, Outcome: "not_observed", SourceState: "unknown", DestinationState: "unknown"}
		if object != nil {
			request.Outcome, request.SourceState, request.DestinationState, request.ObjectIdentity = "recorded_at_destination", "absent", "present", object
		}
		result, err := plans.AppendJournalEvent(context.Background(), f.base, snapshot.Intent.ID, request)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	objectAtDestination := func(id string) plans.JournalIdentity {
		t.Helper()
		observed := observedJournalCLIResult(t, f, id).Observation.DestinationLocation.ObjectIdentity
		if observed == nil {
			t.Fatal("missing fixture destination observation")
		}
		return plans.JournalIdentity{Device: observed.Device, Inode: observed.Inode, ChangedNS: observed.ChangedNS, Generation: q.SourceObject.Generation}
	}
	original = appendEvent(original, "b", "attempt_recorded", nil)
	object := objectAtDestination(original.Intent.ID)
	original = appendEvent(original, "c", "result_recorded", &object)
	restoreRequest := q
	restoreRequest.RequestKey, restoreRequest.Kind, restoreRequest.OriginalIntentID = strings.Repeat("d", 64), "restore_preparation", original.Intent.ID
	restoreRequest.SourcePathBytes, restoreRequest.DestinationPathBytes = q.DestinationPathBytes, q.SourcePathBytes
	restoreRequest.SourceParent, restoreRequest.DestinationParent, restoreRequest.SourceObject = q.DestinationParent, q.SourceParent, object
	restore, err := plans.PrepareJournal(context.Background(), f.base, restoreRequest)
	if err != nil {
		t.Fatal(err)
	}
	observed := observedJournalCLIResult(t, f, restore.Intent.ID)
	if observed.ReferenceID != restore.Intent.ID || observed.Observation.SourceLocation.Status != "observed_present" || observed.Observation.SourceLocation.IdentityRelation != "metadata_matches_reference" || observed.Observation.DestinationLocation.Status != "observed_absent" {
		t.Fatal("restore locations were confused with original intent", observed)
	}
	restore = appendEvent(restore, "e", "attempt_recorded", nil)
	if err := os.Rename(string(restoreRequest.SourcePathBytes), string(restoreRequest.DestinationPathBytes)); err != nil {
		t.Fatal(err)
	}
	object = objectAtDestination(restore.Intent.ID)
	restore = appendEvent(restore, "f", "result_recorded", &object)
	hashes := observationCLIHashes(t, f.base)
	observed = observedJournalCLIResult(t, f, restore.Intent.ID)
	if observed.SavedState != "recorded_at_destination" || observed.ReferenceKind != "recorded_event" || observed.ReferenceID != restore.Events[1].ID || observed.Observation.SourceLocation.Status != "observed_absent" || observed.Observation.DestinationLocation.IdentityRelation != "metadata_matches_reference" || observed.Observation.Status != "outcome_unknown" {
		t.Fatal("restore used stale original reference", observed)
	}
	if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("restore observation changed journal history")
	}
}
