package plans

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Reconstruct real historical schemas; do not merely change their version.
// Existing migration fixtures retain their original migration assertions.
func admissionLegacyFixture(t *testing.T, base string, version int) {
	t.Helper()
	db, closeDB, err := openWithMigration(context.Background(), base, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	if version < 1 || version > 5 {
		t.Fatal("invalid legacy fixture version")
	}
	queries := []string{"DROP TRIGGER IF EXISTS plans_capacity"}
	if version < 5 {
		for _, table := range []string{"finding_dismissal_undos", "finding_dismissals", "dismissal_store_identity"} {
			queries = append(queries, "DROP TABLE IF EXISTS "+table)
		}
	}
	if version < 4 {
		for _, table := range []string{"journal_heads", "journal_events", "journal_intents"} {
			queries = append(queries, "DROP TABLE IF EXISTS "+table)
		}
	}
	if version < 3 {
		queries = append(queries, "DROP TABLE IF EXISTS observations")
	}
	if version < 2 {
		for _, table := range []string{"review_revocations", "review_approvals", "plan_store_identity"} {
			queries = append(queries, "DROP TABLE IF EXISTS "+table)
		}
	}
	queries = append(queries, fmt.Sprintf("PRAGMA user_version=%d", version))
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range queries {
		if _, err = tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func admissionRecord(ordinal int) Record {
	return Record{Version: 1, CreatedAt: time.Unix(1700000000, int64(ordinal)).UTC(), Status: "unapproved", Action: "same_filesystem_quarantine", Activity: "unconfirmed", Selection: selection()}
}

func admissionPopulate(t *testing.T, base string, count int) []Saved {
	t.Helper()
	db, closeDB, err := openWithMigration(context.Background(), base, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	saved := make([]Saved, 0, count)
	for i := 0; i < count; i++ {
		r := admissionRecord(i)
		payload, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("plan-v1-%x", sha256.Sum256(payload))
		if _, err = tx.Exec("INSERT INTO plans VALUES(?,?)", id, payload); err != nil {
			t.Fatal(err)
		}
		saved = append(saved, Saved{ID: id, Record: r})
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return saved
}

func admissionCountVersion(t *testing.T, base string) (int, int) {
	t.Helper()
	db, closeDB, err := open(context.Background(), base, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var count, version int
	if err = db.QueryRow("SELECT count(*) FROM plans").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return count, version
}

func TestPlanAdmissionLimitRetryAndFreshSelection(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	old := admissionPopulate(t, base, PlanLimit-1)
	last, err := SaveRecord(ctx, base, admissionRecord(PlanLimit))
	if err != nil {
		t.Fatal(err)
	}
	if count, version := admissionCountVersion(t, base); count != PlanLimit || version != 6 {
		t.Fatal(count, version)
	}
	for _, saved := range []Saved{old[0], last} {
		retry, e := SaveRecord(ctx, base, saved.Record)
		loaded, le := Load(ctx, base, saved.ID)
		if e != nil || le != nil || !reflect.DeepEqual(retry, loaded) || retry.ID != saved.ID {
			t.Fatal("exact retry changed frozen evidence", e, le)
		}
	}
	if refused, e := Save(ctx, base, selection()); !errors.Is(e, ErrPlanCapacity) || refused.ID != "" {
		t.Fatal("fresh timestamp save bypassed capacity", refused.ID, e)
	}
	if count, _ := admissionCountVersion(t, base); count != PlanLimit {
		t.Fatal(count)
	}
	// Database enforcement applies even to an insertion without the Go census.
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	payload, _ := json.Marshal(admissionRecord(999))
	if _, err = db.Exec("INSERT INTO plans VALUES(?,?)", digest("plan-v1-", payload), payload); err == nil {
		t.Fatal("database admission trigger bypassed")
	}
}

func TestPlanAdmissionLegacyReadRefusalAndMigration(t *testing.T) {
	ctx := context.Background()
	for version := 1; version <= 5; version++ {
		for _, count := range []int{1, PlanLimit, PlanLimit + 3} {
			t.Run(fmt.Sprintf("v%d_rows%d", version, count), func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "state")
				old := admissionPopulate(t, base, count)
				// Construct all derived tables once, then remove precisely the later ones.
				db, closeDB, e := open(ctx, base, true)
				if e != nil {
					t.Fatal(e)
				}
				tx, e := db.Begin()
				if e != nil {
					t.Fatal(e)
				}
				if e = migrateJournalTx(ctx, tx, 3); e != nil {
					t.Fatal(e)
				}
				if _, e = tx.Exec(dismissalSchema); e != nil {
					t.Fatal(e)
				}
				if e = tx.Commit(); e != nil {
					t.Fatal(e)
				}
				closeDB()
				admissionLegacyFixture(t, base, version)
				before, e := Load(ctx, base, old[0].ID)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = Show(ctx, base, old[0].ID); e != nil {
					t.Fatal("legacy saved view unavailable", e)
				}
				retry, e := SaveRecord(ctx, base, before.Record)
				if e != nil || !reflect.DeepEqual(retry, before) {
					t.Fatal("legacy exact retry failed", e)
				}
				if rows, v := admissionCountVersion(t, base); rows != count || v != version {
					t.Fatal("reader/retry migrated legacy history", rows, v)
				}
				_, e = SaveRecord(ctx, base, admissionRecord(1000))
				if count >= PlanLimit {
					if !errors.Is(e, ErrPlanCapacity) {
						t.Fatal(e)
					}
					if rows, v := admissionCountVersion(t, base); rows != count || v != version {
						t.Fatal("refusal mutated legacy history", rows, v)
					}
				} else if e != nil {
					t.Fatal(e)
				} else if rows, v := admissionCountVersion(t, base); rows != count+1 || v != 6 {
					t.Fatal("atomic complete migration absent", rows, v)
				}
				if after, e := Load(ctx, base, before.ID); e != nil || !reflect.DeepEqual(after, before) {
					t.Fatal("admission changed old plan", e)
				}
			})
		}
	}
}

func TestPlanAdmissionMalformedRowsConsumeCapacity(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{
		"INSERT INTO plans VALUES(NULL,X'00')",
		"INSERT INTO plans VALUES('invalid',X'00')",
		"INSERT INTO plans VALUES(1,X'00')",
		"INSERT INTO plans VALUES(X'00',X'00')",
		"INSERT INTO plans VALUES('plan-v1-" + strings.Repeat("a", 64) + "',X'')",
	} {
		for _, count := range []int{1, PlanLimit} {
			t.Run(fmt.Sprintf("rows%d_%x", count, sha256.Sum256([]byte(query))), func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "state")
				admissionPopulate(t, base, count-1)
				db, closeDB, err := openWithMigration(ctx, base, true, false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(query); err != nil {
					t.Fatal(err)
				}
				closeDB()
				_, err = SaveRecord(ctx, base, admissionRecord(999))
				want := ErrCorrupt
				if count == PlanLimit {
					want = ErrPlanCapacity
				}
				if !errors.Is(err, want) {
					t.Fatal("malformed row created credit", err)
				}
				if rows, v := admissionCountVersion(t, base); rows != count || v != 1 {
					t.Fatal("refusal mutated malformed legacy history", rows, v)
				}
			})
		}
	}
}

func TestPlanAdmissionTransactionPublicationAndFrozenResult(t *testing.T) {
	for _, mode := range []string{"cancel_before", "write_fail", "commit_fail", "lost_reply", "cancel_after"} {
		t.Run(mode, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			admissionPopulate(t, base, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("injected publication failure")
			hooks := admissionHooks{}
			switch mode {
			case "cancel_before":
				hooks.beforeCommit = cancel
			case "write_fail":
				db, closeDB, err := openWithMigration(ctx, base, true, false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec("CREATE TRIGGER test_refuse BEFORE INSERT ON plans BEGIN SELECT RAISE(ABORT,'fixture refused'); END"); err != nil {
					t.Fatal(err)
				}
				closeDB()
			case "commit_fail":
				hooks.commit = func(*sql.Tx) error { return sentinel }
			case "lost_reply":
				hooks.commit = func(tx *sql.Tx) error {
					if err := tx.Commit(); err != nil {
						return err
					}
					return sentinel
				}
			case "cancel_after":
				hooks.afterCommit = cancel
			}
			r := admissionRecord(999)
			candidate, err := saveRecord(ctx, base, r, hooks)
			if err == nil {
				t.Fatal("injected failure succeeded")
			}
			committed := mode == "lost_reply" || mode == "cancel_after"
			rows, version := admissionCountVersion(t, base)
			if committed {
				if rows != 2 || version != 6 || candidate.ID == "" {
					t.Fatal(rows, version, candidate.ID, err)
				}
				retry, e := SaveRecord(context.Background(), base, candidate.Record)
				if e != nil || !reflect.DeepEqual(retry, candidate) {
					t.Fatal("exact uncertain-result recovery failed", e)
				}
			} else if rows != 1 || version != 1 {
				t.Fatal("rollback leaked row or migration", rows, version)
			}
			if mode == "commit_fail" || mode == "lost_reply" {
				if !errors.Is(err, ErrPlanPublication) || !errors.Is(err, sentinel) || candidate.ID == "" {
					t.Fatal("uncertain commit lost frozen candidate", err)
				}
			} else if !committed && candidate.ID != "" {
				t.Fatal("definite failure returned saved claim")
			}
		})
	}
	base := filepath.Join(t.TempDir(), "state")
	r := admissionRecord(1)
	saved, err := SaveRecord(context.Background(), base, r)
	if err != nil {
		t.Fatal(err)
	}
	r.Selection.Roots[0].PathBytes[1] = 'X'
	r.Selection.Targets[0].FindingID = "changed"
	r.Selection.Evidence.Findings[0].PathBytes[1] = 'Y'
	loaded, err := Load(context.Background(), base, saved.ID)
	if err != nil || !reflect.DeepEqual(loaded, saved) {
		t.Fatal("returned saved record aliases caller evidence", err)
	}
}

func TestPlanAdmissionDerivedHistoryAtCapacity(t *testing.T) {
	ctx := context.Background()
	base, saved, _, request := journalFixture(t)
	// The legacy journal fixture is schema3. Root publication upgrades to6.
	admissionPopulate(t, base, PlanLimit-2)
	lastRecord := saved.Record
	lastRecord.CreatedAt = admissionRecord(999).CreatedAt
	last, err := SaveRecord(ctx, base, lastRecord)
	if err != nil {
		t.Fatal(err)
	}
	// First observation publication is still available after all root slots fill.
	observed, err := Capture(ctx, base, last.ID, observationReport(last))
	if err != nil {
		t.Fatal("capacity blocked a first derived observation", err)
	}
	if retry, e := Capture(ctx, base, last.ID, observationReport(last)); e != nil || !reflect.DeepEqual(retry, observed) {
		t.Fatal("capacity blocked an exact observation retry", e)
	}
	if _, err := approve(ctx, base, saved.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	q, err := PrepareJournal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	q, err = AppendJournalEvent(ctx, base, q.Intent.ID, journalEvent(q, "a", "attempt_recorded", "not_observed"))
	if err != nil {
		t.Fatal(err)
	}
	q, err = AppendJournalEvent(ctx, base, q.Intent.ID, journalEvent(q, "b", "result_recorded", "outcome_unknown"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Revoke(ctx, base, saved.ID); err != nil {
		t.Fatal(err)
	}
	q, err = AppendJournalEvent(ctx, base, q.Intent.ID, journalEvent(q, "c", "reconciliation_recorded", "recorded_at_destination"))
	if err != nil {
		t.Fatal("capacity blocked unknown-outcome reconciliation", err)
	}
	restore := request
	restore.Kind, restore.RequestKey, restore.OriginalIntentID = "restore_preparation", strings.Repeat("f", 64), q.Intent.ID
	restore.SourcePathBytes, restore.DestinationPathBytes = request.DestinationPathBytes, request.SourcePathBytes
	restore.SourceParent, restore.DestinationParent = request.DestinationParent, request.SourceParent
	restore.SourceObject = *q.Events[len(q.Events)-1].Record.Request.ObjectIdentity
	if _, err = PrepareJournal(ctx, base, restore); err != nil {
		t.Fatal("capacity or revocation blocked linked restore", err)
	}
	shown, err := Show(ctx, base, saved.ID)
	if err != nil || shown.Review == nil || shown.Observation == nil {
		t.Fatal("capacity blocked old saved view", err)
	}
	// Independent dismissal and offline undo remain supported in schema6.
	dismissal, err := SaveDismissal(ctx, base, dismissalRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = UndoDismissal(ctx, base, dismissal.ID); err != nil {
		t.Fatal("schema6 blocked offline undo", err)
	}
	if count, version := admissionCountVersion(t, base); count != PlanLimit || version != 6 {
		t.Fatal(count, version)
	}
}

func TestPlanAdmissionCorruptGuardAndExactRow(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	saved, err := SaveRecord(ctx, base, admissionRecord(1))
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP TRIGGER plans_capacity"); err != nil {
		t.Fatal(err)
	}
	closeDB()
	if _, err = SaveRecord(ctx, base, admissionRecord(2)); !errors.Is(err, ErrCorrupt) {
		t.Fatal("missing schema6 database guard was ignored", err)
	}
	if retry, e := SaveRecord(ctx, base, saved.Record); e != nil || !reflect.DeepEqual(retry, saved) {
		t.Fatal("admission corruption blocked existing exact evidence", e)
	}
	db, closeDB, err = open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP TRIGGER plans_no_update; UPDATE plans SET payload=X'00'"); err != nil {
		t.Fatal(err)
	}
	closeDB()
	if _, err = SaveRecord(ctx, base, saved.Record); !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupted exact row treated as a successful retry", err)
	}
	if count, version := admissionCountVersion(t, base); count != 1 || version != 6 {
		t.Fatal("failed corruption checks rewrote history", count, version)
	}
}

// Alter only this generated fixture's schema text. SQLite may parse it again
// when reopening; the production projection separately bounds returned bytes.
func admissionReplaceTriggerText(t *testing.T, db *sql.DB, value any) {
	t.Helper()
	if _, err := db.Exec("PRAGMA writable_schema=ON"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec("UPDATE sqlite_master SET sql=? WHERE type='trigger' AND name='plans_capacity'", value)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatal("fixture capacity trigger missing", changed, err)
	}
	if _, err := db.Exec("PRAGMA writable_schema=OFF"); err != nil {
		t.Fatal(err)
	}
}

func TestPlanAdmissionTriggerTextBounds(t *testing.T) {
	ctx := context.Background()
	for _, fixture := range []struct {
		name  string
		sql   string
		valid bool
	}{
		{"canonical", admissionTrigger, true},
		{"maximum_surrounding_whitespace", admissionTrigger + strings.Repeat(" ", 128), true},
		{"too_much_surrounding_whitespace", admissionTrigger + strings.Repeat(" ", 129), false},
		{"huge_surrounding_whitespace", admissionTrigger + strings.Repeat(" ", 2<<20), false},
		{"huge_comment", strings.Replace(admissionTrigger, " BEFORE INSERT", " /*"+strings.Repeat("x", 2<<20)+"*/ BEFORE INSERT", 1), false},
		{"different_capacity", strings.Replace(admissionTrigger, "LIMIT 128", "LIMIT 127", 1), false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			first, err := SaveRecord(ctx, base, admissionRecord(1))
			if err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := openWithMigration(ctx, base, true, false)
			if err != nil {
				t.Fatal(err)
			}
			admissionReplaceTriggerText(t, db, fixture.sql)
			closeDB()
			saved, err := SaveRecord(ctx, base, admissionRecord(2))
			wantCount := 1
			if fixture.valid {
				wantCount = 2
				if err != nil || !ValidID(saved.ID) {
					t.Fatal("bounded canonical guard refused publication", err)
				}
			} else if !errors.Is(err, ErrCorrupt) || saved.ID != "" {
				t.Fatal("invalid guard admitted publication", saved.ID, err)
			}
			if count, version := admissionCountVersion(t, base); count != wantCount || version != 6 {
				t.Fatal("guard result changed unrelated history or schema", count, version)
			}
			if loaded, err := Load(ctx, base, first.ID); err != nil || !reflect.DeepEqual(loaded, first) {
				t.Fatal("guard check changed saved evidence", err)
			}
		})
	}
}

func TestPlanAdmissionTriggerScalarTypesAndMissing(t *testing.T) {
	ctx := context.Background()
	for _, fixture := range []struct {
		name  string
		value any
	}{
		{"blob", []byte(admissionTrigger)},
		{"null", nil},
		{"noncanonical_integer_text", int64(42)},
		{"missing", nil},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			if _, err := SaveRecord(ctx, base, admissionRecord(1)); err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := openWithMigration(ctx, base, true, false)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			if fixture.name == "missing" {
				if _, err = db.Exec("DROP TRIGGER plans_capacity"); err != nil {
					t.Fatal(err)
				}
			} else {
				admissionReplaceTriggerText(t, db, fixture.value)
			}
			// Inspect altered scalar kinds in the held connection before SQLite
			// can reject malformed schema text while opening another connection.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err = checkAdmissionTrigger(ctx, tx); !errors.Is(err, ErrCorrupt) {
				t.Fatal("invalid capacity guard scalar was accepted", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err = checkAdmissionTrigger(canceled, tx); !errors.Is(err, context.Canceled) {
				t.Fatal("corruption check concealed cancellation", err)
			}
		})
	}
}

func TestPlanAdmissionExactRetryAtCapacityPrecedesTriggerBound(t *testing.T) {
	ctx := context.Background()
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing_%t", missing), func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			old := admissionPopulate(t, base, PlanLimit-1)
			last, err := SaveRecord(ctx, base, admissionRecord(PlanLimit))
			if err != nil {
				t.Fatal(err)
			}
			first, err := Load(ctx, base, old[0].ID)
			if err != nil || !reflect.DeepEqual(first.Record.Selection.Evidence.Findings[0].PathBytes, old[0].Record.Selection.Evidence.Findings[0].PathBytes) {
				t.Fatal("fixture lost authoritative raw path bytes", err)
			}
			db, closeDB, err := openWithMigration(ctx, base, true, false)
			if err != nil {
				t.Fatal(err)
			}
			if missing {
				if _, err = db.Exec("DROP TRIGGER plans_capacity"); err != nil {
					t.Fatal(err)
				}
			} else {
				admissionReplaceTriggerText(t, db, admissionTrigger+strings.Repeat(" ", 2<<20))
			}
			closeDB()
			for _, saved := range []Saved{first, last} {
				retry, err := SaveRecord(ctx, base, saved.Record)
				if err != nil || !reflect.DeepEqual(retry, saved) {
					t.Fatal("guard bound or capacity blocked exact retry", err)
				}
			}
			if candidate, err := SaveRecord(ctx, base, admissionRecord(999)); !errors.Is(err, ErrPlanCapacity) || candidate.ID != "" {
				t.Fatal("new root bypassed capacity or was reported saved", candidate.ID, err)
			}
			if count, version := admissionCountVersion(t, base); count != PlanLimit || version != 6 {
				t.Fatal("exact retry or refusal rewrote history", count, version)
			}
		})
	}
}

func TestPlanAdmissionPayloadShapeAndUnrelatedSemanticCorruption(t *testing.T) {
	ctx := context.Background()
	for _, malformed := range []string{"text", "oversized", "semantic"} {
		t.Run(malformed, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			admissionPopulate(t, base, 0)
			db, closeDB, err := openWithMigration(ctx, base, true, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			var payload any = []byte{0}
			if malformed == "text" {
				payload = "invalid JSON"
			} else if malformed == "oversized" {
				payload = make([]byte, MaxRecordBytes+1)
			}
			id := "plan-v1-" + strings.Repeat("a", 64)
			if _, err = db.Exec("INSERT INTO plans VALUES(?,?)", id, payload); err != nil {
				t.Fatal(err)
			}
			closeDB()
			_, err = SaveRecord(ctx, base, admissionRecord(2))
			if malformed == "semantic" {
				if err != nil {
					t.Fatal("bounded census unnecessarily decoded unrelated history", err)
				}
				if _, err = Load(ctx, base, id); !errors.Is(err, ErrCorrupt) {
					t.Fatal("exact reader accepted corrupt history", err)
				}
				if count, _ := admissionCountVersion(t, base); count != 2 {
					t.Fatal("corrupt history was discounted or removed", count)
				}
			} else if !errors.Is(err, ErrCorrupt) {
				t.Fatal("malformed payload shape admitted another root", err)
			} else if count, version := admissionCountVersion(t, base); count != 1 || version != 1 {
				t.Fatal("malformed shape refusal changed history", count, version)
			}
		})
	}
}

func TestPlanAdmissionStructuralBounds(t *testing.T) {
	intents := 2 * state.PreviewTargetLimit
	rows := 4 + intents*(1+JournalEventLimit+JournalEventLimit+1)
	payloadBytes := MaxRecordBytes + 2*maxReviewBytes + MaxObservationBytes + intents*MaxJournalBytes*(1+JournalEventLimit)
	if MaxPlanHistoryRows != rows || MaxPlanHistoryPayloadBytes != payloadBytes || PlanLimit != 128 {
		t.Fatal("contract does not match supported derived-history bounds", rows, payloadBytes)
	}
	// Ordinary plans of the same selection retain their distinct capture times.
	base := filepath.Join(t.TempDir(), "state")
	one, err := Save(context.Background(), base, selection())
	if err != nil {
		t.Fatal(err)
	}
	two, err := Save(context.Background(), base, selection())
	if err != nil || one.ID == two.ID || one.Record.CreatedAt.Equal(two.Record.CreatedAt) {
		t.Fatal("new selection save silently deduplicated", err)
	}
	if _, err = Load(context.Background(), base, one.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(base, "plans", filename)); err != nil {
		t.Fatal(err)
	}
}
