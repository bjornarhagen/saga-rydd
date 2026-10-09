package plans

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func journalFixture(t *testing.T) (string, Saved, CapturedObservation, PreparationRequest) {
	t.Helper()
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	selection := selection()
	selection.Targets[0].Target = state.EntryBinding{Device: "1", Inode: "2", ChangedNS: 100, Generation: 7}
	selection.Evidence.Findings[0].Device, selection.Evidence.Findings[0].Inode = "1", "2"
	saved, err := Save(ctx, base, selection)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the original schema3 -> journal migration, independently of the
	// schema6 installed by new saved-plan admission.
	admissionLegacyFixture(t, base, 3)
	saved, err = Load(ctx, base, saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := Capture(ctx, base, saved.ID, observationReport(saved))
	if err != nil {
		t.Fatal(err)
	}
	request := journalRequest(saved, observation)
	return base, saved, observation, request
}

func journalRequest(saved Saved, observation CapturedObservation) PreparationRequest {
	return PreparationRequest{RequestKey: strings.Repeat("1", 64), Kind: "quarantine_preparation", PlanID: saved.ID, ObservationID: observation.ID, FindingID: saved.Record.Selection.Targets[0].FindingID,
		SourcePathBytes: append([]byte(nil), saved.Record.Selection.Evidence.Findings[0].PathBytes...), DestinationPathBytes: []byte("/fixture/.rydd-quarantine/object-1"),
		SourceObject: journalIdentity(saved.Record.Selection.Targets[0].Target), SourceParent: JournalIdentity{Device: "1", Inode: "3", ChangedNS: 101, Generation: 7}, DestinationParent: JournalIdentity{Device: "1", Inode: "4", ChangedNS: 102, Generation: 7}, DestinationMustBeAbsent: true}
}

func journalEvent(snapshot JournalSnapshot, key, kind, outcome string) EventRequest {
	predecessor := snapshot.Intent.ID
	if len(snapshot.Events) > 0 {
		predecessor = snapshot.Events[len(snapshot.Events)-1].ID
	}
	q := EventRequest{RequestKey: strings.Repeat(key, 64), ExpectedSequence: len(snapshot.Events) + 1, PredecessorID: predecessor, Kind: kind, Outcome: outcome, SourceState: "unknown", DestinationState: "unknown"}
	if outcome == "recorded_at_source" || outcome == "recorded_at_destination" {
		identity := snapshot.Intent.Record.Request.SourceObject
		identity.ChangedNS++
		q.ObjectIdentity = &identity
		q.SourceState, q.DestinationState = "present", "absent"
		if outcome == "recorded_at_destination" {
			q.SourceState, q.DestinationState = "absent", "present"
		}
	} else if outcome == "recorded_conflict" {
		q.SourceState, q.DestinationState = "present", "present"
	}
	return q
}

func TestJournalPreparationAndEventRetries(t *testing.T) {
	ctx := context.Background()
	base, saved, observation, request := journalFixture(t)
	approved, err := approve(ctx, base, saved.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := PrepareJournal(ctx, base, request)
	if err != nil || !ValidJournalID(snapshot.Intent.ID) || snapshot.State != "prepared" || snapshot.Executable || snapshot.CurrentStateVerified || len(snapshot.Events) != 0 || snapshot.Intent.Record.Contract != JournalContract {
		t.Fatal(snapshot, err)
	}
	retry, err := PrepareJournal(ctx, base, request)
	if err != nil || !reflect.DeepEqual(retry, snapshot) {
		t.Fatal("intent retry changed history", retry, err)
	}
	changed := request
	changed.DestinationPathBytes = []byte("/fixture/.rydd-quarantine/different")
	if _, err = PrepareJournal(ctx, base, changed); !errors.Is(err, ErrJournalConflict) {
		t.Fatal(err)
	}
	changed = request
	changed.RequestKey = strings.Repeat("2", 64)
	if _, err = PrepareJournal(ctx, base, changed); !errors.Is(err, ErrJournalConflict) {
		t.Fatal("duplicate target", err)
	}
	attempt := journalEvent(snapshot, "a", "attempt_recorded", "not_observed")
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, attempt)
	if err != nil || snapshot.State != "outcome_unknown" || len(snapshot.Events) != 1 {
		t.Fatal(snapshot, err)
	}
	retry, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, attempt)
	if err != nil || !reflect.DeepEqual(retry, snapshot) {
		t.Fatal("lost attempt response retry", retry, err)
	}
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "b", "attempt_recorded", "not_observed")); !errors.Is(err, ErrJournalTransition) {
		t.Fatal("second attempt permitted", err)
	}
	result := journalEvent(snapshot, "c", "result_recorded", "outcome_unknown")
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, result)
	if err != nil || snapshot.State != "outcome_unknown" {
		t.Fatal(snapshot, err)
	}
	result = journalEvent(snapshot, "d", "reconciliation_recorded", "recorded_at_destination")
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, result)
	if err != nil || snapshot.State != "recorded_at_destination" || len(snapshot.Events) != 3 {
		t.Fatal(snapshot, err)
	}
	retry, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, result)
	if err != nil || !reflect.DeepEqual(retry, snapshot) {
		t.Fatal("result retry", retry, err)
	}
	result.Outcome = "recorded_at_source"
	result.SourceState, result.DestinationState = "present", "absent"
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, result); !errors.Is(err, ErrJournalConflict) {
		t.Fatal("event key conflict", err)
	}
	loaded, err := LoadJournal(ctx, base, snapshot.Intent.ID)
	if err != nil || !reflect.DeepEqual(loaded, snapshot) {
		t.Fatal(loaded, err)
	}
	shown, err := Show(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(shown.Record, saved.Record) || !reflect.DeepEqual(shown.Observation, &observation) || !reflect.DeepEqual(shown.Review, approved.Review) {
		t.Fatal("changed original records", shown, err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	for _, table := range []string{"journal_intents", "journal_events", "journal_heads"} {
		for _, query := range []string{"DELETE FROM " + table, "UPDATE " + table + " SET " + map[string]string{"journal_intents": "id=id", "journal_events": "id=id", "journal_heads": "event_id=event_id"}[table]} {
			if _, err = db.Exec(query); err == nil {
				t.Fatal("mutable journal", query)
			}
		}
	}
	var version, sync int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 4 {
		t.Fatal(version, err)
	}
	if err = db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatal(sync, err)
	}
}

func TestJournalRestoreLinkedHistory(t *testing.T) {
	ctx := context.Background()
	base, saved, _, request := journalFixture(t)
	if _, err := approve(ctx, base, saved.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := PrepareJournal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "b", "result_recorded", "recorded_at_destination"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Revoke(ctx, base, saved.ID); err != nil {
		t.Fatal(err)
	}
	restore := request
	restore.Kind = "restore_preparation"
	restore.RequestKey = strings.Repeat("f", 64)
	restore.OriginalIntentID = snapshot.Intent.ID
	restore.SourcePathBytes, restore.DestinationPathBytes = request.DestinationPathBytes, request.SourcePathBytes
	restore.SourceParent, restore.DestinationParent = request.DestinationParent, request.SourceParent
	restore.SourceObject = *snapshot.Events[len(snapshot.Events)-1].Record.Request.ObjectIdentity
	restored, err := PrepareJournal(ctx, base, restore)
	if err != nil || restored.State != "prepared" || restored.Intent.Record.Request.OriginalIntentID != snapshot.Intent.ID || !bytesEqual(restored.Intent.Record.Request.DestinationPathBytes, request.SourcePathBytes) {
		t.Fatal(restored, err)
	}
	if _, err = LoadJournal(ctx, base, restored.Intent.ID); err != nil {
		t.Fatal("revocation erased restore access", err)
	}
	changed := restore
	changed.RequestKey = strings.Repeat("e", 64)
	changed.DestinationPathBytes = []byte("/fixture/other/node_modules")
	if _, err = PrepareJournal(ctx, base, changed); !errors.Is(err, ErrJournalPreparation) {
		t.Fatal("restore wrong original", err)
	}
	changed = restore
	changed.SourceObject.ChangedNS++
	if _, err = PrepareJournal(ctx, base, changed); !errors.Is(err, ErrJournalPreparation) {
		t.Fatal("restore unknown object stamp", err)
	}
	loaded, err := LoadJournal(ctx, base, snapshot.Intent.ID)
	if err != nil || !reflect.DeepEqual(loaded, snapshot) {
		t.Fatal("original history changed", loaded, err)
	}
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func TestJournalInvalidPreparationDoesNotMigrate(t *testing.T) {
	cases := map[string]func(*PreparationRequest){
		"key": func(q *PreparationRequest) { q.RequestKey = "one" }, "kind": func(q *PreparationRequest) { q.Kind = "execute" }, "plan": func(q *PreparationRequest) { q.PlanID = "plan-v1-" + strings.Repeat("f", 64) },
		"finding": func(q *PreparationRequest) { q.FindingID = "node-modules-v1:1:99" }, "source": func(q *PreparationRequest) { q.SourcePathBytes = []byte("/other/node_modules") }, "identity": func(q *PreparationRequest) { q.SourceObject.ChangedNS++ },
		"unknown": func(q *PreparationRequest) { q.SourceObject.Inode = "0" }, "decimal": func(q *PreparationRequest) { q.SourceObject.Device = "01" }, "timestamp": func(q *PreparationRequest) { q.SourceParent.ChangedNS = 0 },
		"overwrite": func(q *PreparationRequest) { q.DestinationMustBeAbsent = false }, "relative": func(q *PreparationRequest) { q.DestinationPathBytes = []byte("target") }, "nul": func(q *PreparationRequest) { q.DestinationPathBytes = []byte("/target\x00bad") },
		"normalize": func(q *PreparationRequest) { q.DestinationPathBytes = []byte("/fixture/../target") }, "path_limit": func(q *PreparationRequest) {
			q.DestinationPathBytes = []byte("/" + strings.Repeat("x", JournalPathBytesLimit))
		},
		"depth": func(q *PreparationRequest) {
			q.DestinationPathBytes = []byte("/" + strings.Repeat("a/", state.LivePathDepthLimit) + "target")
		}, "same": func(q *PreparationRequest) { q.DestinationPathBytes = q.SourcePathBytes }, "inside_source": func(q *PreparationRequest) {
			q.DestinationPathBytes = append(append([]byte(nil), q.SourcePathBytes...), []byte("/saved")...)
		},
		"cross_device": func(q *PreparationRequest) { q.DestinationParent.Device = "2" }, "original": func(q *PreparationRequest) { q.OriginalIntentID = journalPrefix + strings.Repeat("f", 64) }, "object_parent": func(q *PreparationRequest) { q.SourceParent = q.SourceObject },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, _, _, request := journalFixture(t)
			change(&request)
			if _, err := PrepareJournal(ctx, base, request); !errors.Is(err, ErrJournalPreparation) {
				t.Fatal(err)
			}
			db, closeDB, err := open(ctx, base, false)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			var version, tables int
			if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name GLOB 'journal_*'").Scan(&tables); err != nil || version != 3 || tables != 0 {
				t.Fatal(version, tables, err)
			}
		})
	}
}

func TestJournalEventTransitionsAndLimit(t *testing.T) {
	ctx := context.Background()
	base, _, _, request := journalFixture(t)
	snapshot, err := PrepareJournal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "result_recorded", "recorded_at_destination")); !errors.Is(err, ErrJournalTransition) {
		t.Fatal("result without attempt", err)
	}
	cases := map[string]func(*EventRequest){"sequence": func(q *EventRequest) { q.ExpectedSequence = 2 }, "predecessor": func(q *EventRequest) { q.PredecessorID = journalPrefix + strings.Repeat("b", 64) }, "kind": func(q *EventRequest) { q.Kind = "move" }, "outcome": func(q *EventRequest) { q.Outcome = "verified" }, "attempt_evidence": func(q *EventRequest) { q.ObjectIdentity = &request.SourceObject }}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			q := journalEvent(snapshot, "a", "attempt_recorded", "not_observed")
			change(&q)
			if _, err := AppendJournalEvent(ctx, base, snapshot.Intent.ID, q); !errors.Is(err, ErrJournalTransition) {
				t.Fatal(err)
			}
		})
	}
	snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed"))
	if err != nil {
		t.Fatal(err)
	}
	q := journalEvent(snapshot, "b", "result_recorded", "recorded_at_destination")
	q.ObjectIdentity.Inode = "99"
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, q); !errors.Is(err, ErrJournalTransition) {
		t.Fatal("wrong result object", err)
	}
	for len(snapshot.Events) < JournalEventLimit {
		kind := "reconciliation_recorded"
		if len(snapshot.Events) == 1 {
			kind = "result_recorded"
		}
		q := journalEvent(snapshot, "b", kind, "outcome_unknown")
		q.RequestKey = fmt.Sprintf("%064x", len(snapshot.Events)+100)
		snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, q)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "c", "reconciliation_recorded", "outcome_unknown")); !errors.Is(err, ErrJournalTransition) {
		t.Fatal("counter cap", err)
	}
}

func TestJournalMigrationRollbackAndOldReaders(t *testing.T) {
	ctx := context.Background()
	base, saved, observation, request := journalFixture(t)
	sentinel := errors.New("injected publication failure")
	if _, err := prepareJournal(ctx, base, request, func(tx *sql.Tx) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		t.Fatal(err)
	}
	var version, tables int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name GLOB 'journal_*'").Scan(&tables); err != nil || version != 3 || tables != 0 {
		t.Fatal(version, tables, err)
	}
	closeDB()
	shown, err := Show(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(shown.Observation, &observation) {
		t.Fatal(shown, err)
	}
	for _, old := range []int{1, 2} {
		t.Run(fmt.Sprint(old), func(t *testing.T) {
			base, saved := reviewFixture(t)
			downgradeObservationStore(t, base, old)
			if _, err := LoadJournal(ctx, base, journalPrefix+strings.Repeat("a", 64)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if _, err := Show(ctx, base, saved.ID); err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := open(ctx, base, false)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != old {
				t.Fatal(version, err)
			}
		})
	}
}

func TestJournalWriterFencingAndCancellation(t *testing.T) {
	ctx := context.Background()
	base, _, _, request := journalFixture(t)
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareJournal(ctx, base, request); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal(err)
	}
	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	closeDB()
	snapshot, err := PrepareJournal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	_, closeDB, err = open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed")); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal(err)
	}
	if _, err = LoadJournal(ctx, base, snapshot.Intent.ID); err != nil {
		t.Fatal("read blocked", err)
	}
	closeDB()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = PrepareJournal(cancelled, base, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = AppendJournalEvent(cancelled, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = LoadJournal(cancelled, base, snapshot.Intent.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestJournalCorruptChainRefuses(t *testing.T) {
	cases := map[string]string{
		"event_tail":     "DROP TRIGGER journal_events_no_delete; DELETE FROM journal_events WHERE sequence=2",
		"event_gap":      "DROP TRIGGER journal_events_no_delete; DELETE FROM journal_events WHERE sequence=1",
		"head_tail":      "DROP TRIGGER journal_heads_no_delete; DELETE FROM journal_heads WHERE sequence=2",
		"head_gap":       "DROP TRIGGER journal_heads_no_delete; DELETE FROM journal_heads WHERE sequence=1",
		"head_binding":   "DROP TRIGGER journal_heads_no_update; UPDATE journal_heads SET event_id='bad' WHERE sequence=2",
		"event_payload":  "DROP TRIGGER journal_events_no_update; UPDATE journal_events SET payload=X'00' WHERE sequence=2",
		"intent_binding": "DROP TRIGGER journal_intents_no_update; UPDATE journal_intents SET finding_id='other'",
		"intent_payload": "DROP TRIGGER journal_intents_no_update; UPDATE journal_intents SET payload=X'00'",
		"store_binding":  "DROP TRIGGER plan_store_identity_no_update; UPDATE plan_store_identity SET token=lower(hex(randomblob(32)))",
		"oversize":       "DROP TRIGGER journal_events_no_update; PRAGMA ignore_check_constraints=1; UPDATE journal_events SET payload=zeroblob(32769) WHERE sequence=2",
		"counter":        "PRAGMA ignore_check_constraints=1; INSERT INTO journal_heads SELECT intent_id,17,'bad' FROM journal_heads WHERE sequence=0",
	}
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, _, _, request := journalFixture(t)
			snapshot, err := PrepareJournal(ctx, base, request)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed"))
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "b", "result_recorded", "outcome_unknown"))
			if err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(query); err != nil {
				t.Fatal(err)
			}
			closeDB()
			if _, err = LoadJournal(ctx, base, snapshot.Intent.ID); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatal(err)
			}
			if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "c", "reconciliation_recorded", "recorded_at_source")); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatal("wrote after corruption", err)
			}
		})
	}
}

func TestJournalCanonicalPayloadAndTimestampRefusal(t *testing.T) {
	_, _, _, request := journalFixture(t)
	record := JournalIntentRecord{Version: 1, Contract: JournalContract, CreatedAt: time.Now().UTC(), StoreID: strings.Repeat("a", 64), InventoryID: strings.Repeat("b", 64), Request: request, Status: "prepared"}
	payload, _ := json.Marshal(record)
	for name, altered := range map[string][]byte{"duplicate": []byte(strings.Replace(string(payload), `"version":1,`, `"version":1,"version":1,`, 1)), "unknown": []byte(strings.Replace(string(payload), `"version":1,`, `"unexpected":true,"version":1,`, 1)), "trailing": append(append([]byte(nil), payload...), []byte(" {}")...), "omitted": []byte(strings.Replace(string(payload), `,"executable":false`, "", 1))} {
		t.Run(name, func(t *testing.T) {
			if err := strictJournalDecode(altered, digest(journalPrefix, altered), journalPrefix, new(JournalIntentRecord)); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatal(err)
			}
		})
	}
	record.CreatedAt = time.Now().Add(time.Hour)
	if validateIntent(record, time.Now()) == nil {
		t.Fatal("future accepted")
	}
	record.CreatedAt = time.Time{}
	if validateIntent(record, time.Now()) == nil {
		t.Fatal("zero accepted")
	}
	record.CreatedAt = time.Now()
	record.Executable = true
	if validateIntent(record, time.Now()) == nil {
		t.Fatal("executable accepted")
	}
}

func TestJournalEventKeyCannotBindAnotherIntent(t *testing.T) {
	ctx := context.Background()
	base, saved, _, request := journalFixture(t)
	first, err := PrepareJournal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	first, err = AppendJournalEvent(ctx, base, first.Intent.ID, journalEvent(first, "a", "attempt_recorded", "not_observed"))
	if err != nil {
		t.Fatal(err)
	}
	secondPlan, err := Save(ctx, base, saved.Record.Selection)
	if err != nil {
		t.Fatal(err)
	}
	secondObservation, err := Capture(ctx, base, secondPlan.ID, observationReport(secondPlan))
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := journalRequest(secondPlan, secondObservation)
	secondRequest.RequestKey = strings.Repeat("2", 64)
	second, err := PrepareJournal(ctx, base, secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AppendJournalEvent(ctx, base, second.Intent.ID, journalEvent(second, "a", "attempt_recorded", "not_observed")); !errors.Is(err, ErrJournalConflict) {
		t.Fatal("event key bound another intent", err)
	}
	second, err = LoadJournal(ctx, base, second.Intent.ID)
	if err != nil || len(second.Events) != 0 {
		t.Fatal(second, err)
	}
}

func TestJournalRehashedInvalidEventStillRefused(t *testing.T) {
	cases := map[string]func(*JournalEventRecord){
		"future":        func(r *JournalEventRecord) { r.CreatedAt = time.Now().Add(time.Hour) },
		"before_intent": func(r *JournalEventRecord) { r.CreatedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"executable":    func(r *JournalEventRecord) { r.Executable = true },
		"verified":      func(r *JournalEventRecord) { r.CurrentStateVerified = true },
		"source":        func(r *JournalEventRecord) { r.EvidenceSource = "filesystem_verified" },
		"predecessor":   func(r *JournalEventRecord) { r.Request.PredecessorID = journalPrefix + strings.Repeat("e", 64) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, _, _, request := journalFixture(t)
			snapshot, err := PrepareJournal(ctx, base, request)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed"))
			if err != nil {
				t.Fatal(err)
			}
			record := snapshot.Events[0].Record
			change(&record)
			payload, _ := json.Marshal(record)
			id := digest(journalEventPrefix, payload)
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("DROP TRIGGER journal_events_no_update; DROP TRIGGER journal_heads_no_update"); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("UPDATE journal_events SET id=?,payload=? WHERE sequence=1", id, payload); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("UPDATE journal_heads SET event_id=? WHERE sequence=1", id); err != nil {
				t.Fatal(err)
			}
			closeDB()
			if _, err = LoadJournal(ctx, base, snapshot.Intent.ID); !errors.Is(err, ErrJournalCorrupt) {
				t.Fatal("rehash bypassed validation", err)
			}
		})
	}
}

func TestJournalCrashHelper(t *testing.T) {
	base, planID, mode := os.Getenv("RYDD_JOURNAL_CRASH_BASE"), os.Getenv("RYDD_JOURNAL_CRASH_PLAN"), os.Getenv("RYDD_JOURNAL_CRASH_MODE")
	if base == "" {
		return
	}
	ctx := context.Background()
	saved, err := Show(ctx, base, planID)
	if err != nil || saved.Observation == nil {
		t.Fatal(saved, err)
	}
	request := journalRequest(saved, *saved.Observation)
	ready := func() { fmt.Println("READY"); time.Sleep(time.Minute) }
	if mode == "uncommitted" {
		_, err = prepareJournal(ctx, base, request, func(tx *sql.Tx) error {
			for i := 0; i < 80; i++ {
				if _, err := tx.Exec("INSERT INTO journal_intents VALUES(?,?,?,?,?,?,?,?)", fmt.Sprint(i), fmt.Sprint(i), "fixture", fmt.Sprint(i), fmt.Sprint(i), fmt.Sprint(i), "", []byte(strings.Repeat("x", MaxJournalBytes))); err != nil {
					return err
				}
			}
			ready()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		snapshot, err := PrepareJournal(ctx, base, request)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "attempt" {
			if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "a", "attempt_recorded", "not_observed")); err != nil {
				t.Fatal(err)
			}
		}
		ready()
	}
}

func TestJournalCrashRecovery(t *testing.T) {
	for _, mode := range []string{"published", "attempt", "uncommitted"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base, saved, observation, request := journalFixture(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCrashHelper$")
			cmd.Env = append(os.Environ(), "RYDD_JOURNAL_CRASH_BASE="+base, "RYDD_JOURNAL_CRASH_PLAN="+saved.ID, "RYDD_JOURNAL_CRASH_MODE="+mode)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			ready := make(chan bool, 1)
			go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "READY" }()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("helper not ready")
				}
			case <-time.After(20 * time.Second):
				t.Fatal("helper timed out")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("helper was not killed")
			}
			shown, err := Show(ctx, base, saved.ID)
			if err != nil || !reflect.DeepEqual(shown.Record, saved.Record) || !reflect.DeepEqual(shown.Observation, &observation) {
				t.Fatal("original lost", shown, err)
			}
			if mode == "uncommitted" {
				db, closeDB, err := open(ctx, base, false)
				if err != nil {
					t.Fatal(err)
				}
				var version, tables int
				if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
					t.Fatal(err)
				}
				if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name GLOB 'journal_*'").Scan(&tables); err != nil || version != 3 || tables != 0 {
					t.Fatal(version, tables, err)
				}
				closeDB()
			}
			snapshot, err := PrepareJournal(ctx, base, request)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "attempt" {
				if snapshot.State != "outcome_unknown" || len(snapshot.Events) != 1 {
					t.Fatal("attempt was retried or lost", snapshot)
				}
				if _, err = AppendJournalEvent(ctx, base, snapshot.Intent.ID, journalEvent(snapshot, "b", "attempt_recorded", "not_observed")); !errors.Is(err, ErrJournalTransition) {
					t.Fatal(err)
				}
			} else if snapshot.State != "prepared" || len(snapshot.Events) != 0 {
				t.Fatal(snapshot)
			}
		})
	}
}
