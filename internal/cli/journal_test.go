package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

func journalCLIFixture(t *testing.T) (reviewCLI, plans.JournalSnapshot) {
	t.Helper()
	f := treeCLIFixture(t)
	observation := capturedCLIResult(t, f).Observation
	target := f.saved.Record.Selection.Targets[0]
	object := plans.JournalIdentity{Device: target.Target.Device, Inode: target.Target.Inode, ChangedNS: target.Target.ChangedNS, Generation: target.Target.Generation}
	parentInode := "1"
	if object.Inode == parentInode {
		parentInode = "2"
	}
	parent := plans.JournalIdentity{Device: object.Device, Inode: parentInode, ChangedNS: object.ChangedNS, Generation: object.Generation}
	request := plans.PreparationRequest{RequestKey: strings.Repeat("a", 64), Kind: "quarantine_preparation", PlanID: f.saved.ID, ObservationID: observation.ID, FindingID: target.FindingID,
		SourcePathBytes: f.saved.Record.Selection.Evidence.Findings[0].PathBytes, DestinationPathBytes: []byte(filepath.Join(filepath.Dir(f.root), "quarantine", "slot\t\n")),
		SourceObject: object, SourceParent: parent, DestinationParent: parent, DestinationMustBeAbsent: true}
	snapshot, err := plans.PrepareJournal(context.Background(), f.base, request)
	if err != nil {
		t.Fatal(err)
	}
	return f, snapshot
}

func TestJournalShowReadOnlyAndOffline(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	before, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	hashes := observationCLIHashes(t, f.base)
	code, raw := f.run("journal", "--show", snapshot.Intent.ID, "--json")
	var result struct{ Journal plans.JournalSnapshot }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || !reflect.DeepEqual(result.Journal, snapshot) || result.Journal.Executable || result.Journal.CurrentStateVerified {
		t.Fatal(code, raw, err)
	}
	code, human := f.run("journal", "--show", snapshot.Intent.ID)
	plain := strings.Join(strings.Fields(human), " ")
	if code != 0 || !strings.Contains(human, "PREPARATION RECORD - NO OPERATION ENABLED") || !strings.Contains(human, strconv.Quote(string(snapshot.Intent.Record.Request.DestinationPathBytes))) || !strings.Contains(plain, "caller-supplied observation records") || !strings.Contains(plain, "Missing results do not prove failure or permit retry") || strings.Contains(human, "slot\t\n") {
		t.Fatal(code, human)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	inventory := manualState(paths, f.root)
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(inventory, inventory+".offline"); err != nil {
		t.Fatal(err)
	}
	hashes = observationCLIHashes(t, f.base)
	if code, raw := f.run("journal", "--show", snapshot.Intent.ID, "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	after, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("read changed saved evidence", err)
	}
	if _, err := os.Stat(inventory); !os.IsNotExist(err) {
		t.Fatal("show initialized offline inventory", err)
	}
	got, err := os.ReadFile(filepath.Join(f.root+".offline", "node_modules", "fixture-dep", "bin.js"))
	if err != nil || !bytes.Equal(got, treeDependencyContents) {
		t.Fatal("source changed", err)
	}
}

func TestJournalShowUncertainAndRecordedOutcome(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	request := plans.EventRequest{RequestKey: strings.Repeat("b", 64), ExpectedSequence: 1, PredecessorID: snapshot.Intent.ID, Kind: "attempt_recorded", Outcome: "not_observed", SourceState: "unknown", DestinationState: "unknown"}
	snapshot, err := plans.AppendJournalEvent(context.Background(), f.base, snapshot.Intent.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	if code, human := f.run("journal", "--show", snapshot.Intent.ID); code != 0 || !strings.Contains(human, "RECORDED OUTCOME UNKNOWN - REVIEW REQUIRED") {
		t.Fatal(code, human)
	}
	identity := snapshot.Intent.Record.Request.SourceObject
	identity.ChangedNS++
	request = plans.EventRequest{RequestKey: strings.Repeat("c", 64), ExpectedSequence: 2, PredecessorID: snapshot.Events[0].ID, Kind: "result_recorded", Outcome: "recorded_at_destination", SourceState: "absent", DestinationState: "present", ObjectIdentity: &identity}
	snapshot, err = plans.AppendJournalEvent(context.Background(), f.base, snapshot.Intent.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	hashes := observationCLIHashes(t, f.base)
	code, raw := f.run("journal", "--show", snapshot.Intent.ID, "--json")
	var result struct{ Journal plans.JournalSnapshot }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || !reflect.DeepEqual(result.Journal, snapshot) || result.Journal.Events[1].Record.EvidenceSource != "caller_supplied_record" || result.Journal.Executable || result.Journal.CurrentStateVerified {
		t.Fatal(code, raw, err)
	}
	if code, human := f.run("journal", "--show", snapshot.Intent.ID); code != 0 || !strings.Contains(human, "RECORDED OBSERVATION: DESTINATION LOCATION") || !strings.Contains(strings.Join(strings.Fields(human), " "), "does not inspect either location") {
		t.Fatal(code, human)
	}
	if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("show wrote saved records")
	}
}

func TestJournalShowArgumentsMissingStoreAndCancellation(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	for _, args := range [][]string{{}, {"--show"}, {"--show", "invalid"}, {"--show", snapshot.Intent.ID, "--show", snapshot.Intent.ID}, {"--show", snapshot.Intent.ID, "-d", f.root}, {"--show", snapshot.Intent.ID, "extra"}, {"--create"}, {"--execute", snapshot.Intent.ID}} {
		args = append([]string{"--json", "journal"}, args...)
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	missingBase := filepath.Join(t.TempDir(), "absent")
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", missingBase, "journal", "--show", snapshot.Intent.ID, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "not_found") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
	if _, err := os.Stat(missingBase); !os.IsNotExist(err) {
		t.Fatal("show initialized missing storage", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out.Reset()
	code = Run(ctx, []string{"--data-dir", f.base, "journal", "--show", snapshot.Intent.ID, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "canceled") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
	code, raw := f.run("capabilities", "--json")
	var capabilities struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || !capabilities.Features["journal_records"] || capabilities.Features["cleanup"] || !strings.Contains(raw, "--show INTENT_ID") || !strings.Contains(raw, "journal_outcome_unknown") {
		t.Fatal(code, raw, err)
	}
}

func TestJournalShowCorruptionReturnsNoPartialRecord(t *testing.T) {
	f, snapshot := journalCLIFixture(t)
	db, err := sql.Open("sqlite", filepath.Join(f.base, "plans", "plans.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DROP TRIGGER journal_heads_no_delete; DELETE FROM journal_heads")
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	code, raw := f.run("journal", "--show", snapshot.Intent.ID, "--json")
	if code != 1 || !strings.Contains(raw, "journal_outcome_unknown") || strings.Contains(raw, "source_path_bytes") || strings.Contains(raw, "destination_path_bytes") || strings.Contains(raw, snapshot.Intent.ID) {
		t.Fatal(code, raw)
	}
}
