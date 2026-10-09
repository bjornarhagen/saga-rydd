package plans

import (
	"bytes"
	"context"
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

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// These are synthetic saved observations; no source paths exist or are opened.
func dismissalSelection() state.SelectionSnapshot {
	path := []byte("/fixture/quote\"雪\xff/node_modules")
	manifest := []byte("/fixture/quote\"雪\xff/package.json")
	observed := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	logical := int64(123)
	return state.SelectionSnapshot{
		InventoryID: strings.Repeat("a", 64),
		Roots:       []state.RootBinding{{ID: 1, PathBytes: []byte("/fixture"), Fingerprint: "fixture-root", Revision: 1}},
		Targets:     []state.TargetBinding{{FindingID: "node-modules-v1:1:2", Target: state.EntryBinding{Device: "1", Inode: "2", ChangedNS: 100, Generation: 7}, Manifest: state.EntryBinding{Device: "1", Inode: "3", ChangedNS: 101, Generation: 7}}},
		Evidence: state.FindingReport{GeneratedAt: observed, Source: "saved_inventory", PageCoverage: "selected_entries_only", MinimumAgeDays: 90, EntriesExamined: 1, EntryLimit: 1, Diagnostics: []state.SelectionDiagnostic{{Code: "not_node_modules"}, {Code: "nested_dependency"}, {Code: "not_directory"}, {Code: "skipped"}, {Code: "manifest_missing_or_unsupported"}, {Code: "parent_incomplete_or_error"}, {Code: "parent_unconfirmed"}, {Code: "timestamp_unknown"}, {Code: "age_not_met"}, {Code: "selected", Count: 1, Explanation: "wording"}}, Findings: []state.Finding{{
			ID: "node-modules-v1:1:2", Rule: "node_modules_old_metadata", RuleVersion: 1, RootID: 1, EntryID: 2, Device: "1", Inode: "2", Path: string(path), PathBytes: path, ManifestPath: string(manifest), ManifestPathBytes: manifest,
			DirectoryModifiedAt: observed, ManifestModifiedAt: observed, DirectoryObservedAt: observed, ManifestObservedAt: observed, Classification: "review_required", Recognition: "manifest_filename_only", Actions: []string{},
			Measurement: state.DirectoryReport{Source: "saved_inventory", CoverageSource: "bounded_entry_check", AllocatedSizeSource: "unknown", Path: string(path), PathBytes: bytes.Clone(path), RootID: 1, GeneratedAt: observed, Status: "partial", LogicalBytes: &logical, AllocatedBytes: nil, EntriesExamined: 2, EntryLimit: state.DirectoryEntryLimit, FilePaths: 1, IncompleteDirectories: 1, Notes: []string{"wording"}},
		}}},
	}
}

func dismissalRequest(t *testing.T) DismissalRequest {
	t.Helper()
	r, err := NewDismissalRequest([]byte("/fixture"), dismissalSelection())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assertDismissalQualified(t *testing.T, saved SavedDismissal) {
	t.Helper()
	if saved.CurrentEvidenceMatchEvaluated || saved.CurrentStateVerified || saved.ApprovalAvailable || saved.Executable || saved.EstimatedReclaimableBytes != nil || saved.Record.Request.CurrentStateVerified || saved.Record.Request.ApprovalAvailable || saved.Record.Request.Executable || saved.Record.Request.EstimatedReclaimableBytes != nil {
		t.Fatal("dismissal gained authority", saved)
	}
}

func TestDismissalRoundTripExactRetryUndoAndRawPaths(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	request := dismissalRequest(t)
	saved, err := SaveDismissal(ctx, base, request)
	if err != nil || !ValidDismissalID(saved.ID) || saved.Status != "dismissed" || saved.Undo != nil || saved.Record.Request.ID != request.ID || saved.Record.StoreID == "" {
		t.Fatal(saved, err)
	}
	assertDismissalQualified(t, saved)
	if saved.Record.Request.Selection.Evidence.Findings[0].Measurement.Status != "partial" || saved.Record.Request.Selection.Evidence.Findings[0].Measurement.AllocatedBytes != nil {
		t.Fatal("partial evidence changed", saved)
	}
	for _, load := range []func() (SavedDismissal, error){func() (SavedDismissal, error) { return ShowDismissal(ctx, base, saved.ID) }, func() (SavedDismissal, error) { return FindDismissal(ctx, base, request.ID) }, func() (SavedDismissal, error) { return SaveDismissal(ctx, base, request) }} {
		again, e := load()
		if e != nil || !reflect.DeepEqual(again, saved) {
			t.Fatal("exact retry/reopen changed evidence", again, e)
		}
	}
	matched, err := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()})
	if err != nil || !reflect.DeepEqual(matched, []bool{true}) {
		t.Fatal(matched, err)
	}
	request.Selection.Evidence.Findings[0].PathBytes[0] = 'x'
	saved.Record.Request.Selection.Roots[0].PathBytes[0] = 'x'
	loaded, err := ShowDismissal(ctx, base, saved.ID)
	if err != nil || !bytes.Equal(loaded.Record.Request.Selection.Evidence.Findings[0].PathBytes, dismissalSelection().Evidence.Findings[0].PathBytes) || string(loaded.Record.Request.ManualRootBytes) != "/fixture" {
		t.Fatal("caller mutation changed stored evidence", loaded, err)
	}
	undone, err := UndoDismissal(ctx, base, saved.ID)
	if err != nil || undone.Status != "undone" || undone.Undo == nil || undone.Undo.Record.RequestID != loaded.Record.Request.ID {
		t.Fatal(undone, err)
	}
	assertDismissalQualified(t, undone)
	again, err := UndoDismissal(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(again, undone) {
		t.Fatal("undo retry changed record", again, err)
	}
	again, err = SaveDismissal(ctx, base, dismissalRequest(t))
	if err != nil || !reflect.DeepEqual(again, undone) {
		t.Fatal("save retry resurrected undone dismissal", again, err)
	}
	matched, err = DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()})
	if err != nil || !reflect.DeepEqual(matched, []bool{false}) {
		t.Fatal(matched, err)
	}
}

func TestDismissalCanonicalBindingAndVisibleUnusableEvidence(t *testing.T) {
	request := dismissalRequest(t)
	canonical := dismissalSelection()
	canonical.Evidence.GeneratedAt = time.Now().UTC()
	canonical.Evidence.Notes = []string{"new wording"}
	canonical.Evidence.Diagnostics[0].Explanation = "new wording"
	f := &canonical.Evidence.Findings[0]
	f.Path, f.ManifestPath, f.Measurement.Path = "lossy display", "lossy display", "lossy display"
	f.Measurement.GeneratedAt = time.Now().UTC()
	f.Measurement.Notes = []string{"new wording"}
	again, err := NewDismissalRequest([]byte("/fixture"), canonical)
	if err != nil || !reflect.DeepEqual(again, request) {
		t.Fatal("wording changed evidence key", again, err)
	}
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	if _, err = SaveDismissal(ctx, base, request); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*state.SelectionSnapshot)
	}{
		{"inventory", func(s *state.SelectionSnapshot) { s.InventoryID = strings.Repeat("b", 64) }},
		{"root revision", func(s *state.SelectionSnapshot) { s.Roots[0].Revision++ }},
		{"target ctime", func(s *state.SelectionSnapshot) { s.Targets[0].Target.ChangedNS++ }},
		{"manifest generation", func(s *state.SelectionSnapshot) { s.Targets[0].Manifest.Generation++ }},
		{"observation time", func(s *state.SelectionSnapshot) {
			s.Evidence.Findings[0].DirectoryObservedAt = s.Evidence.Findings[0].DirectoryObservedAt.Add(time.Nanosecond)
		}},
		{"measurement", func(s *state.SelectionSnapshot) { *s.Evidence.Findings[0].Measurement.LogicalBytes++ }},
		{"unknown identity", func(s *state.SelectionSnapshot) { s.Targets[0].Target.Inode = ""; s.Evidence.Findings[0].Inode = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := dismissalSelection()
			tc.mutate(&changed)
			matched, e := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection(), changed})
			if e != nil || !reflect.DeepEqual(matched, []bool{true, false}) {
				t.Fatal(matched, e)
			}
		})
	}
	if _, err = DismissedFindings(ctx, base, []byte("/fixture"), make([]state.SelectionSnapshot, 21)); !errors.Is(err, ErrDismissalRequest) {
		t.Fatal(err)
	}
}

func dismissalVersion(t *testing.T, base string) int {
	t.Helper()
	db, closeDB, err := open(context.Background(), base, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func TestDismissalMissingLegacyMigrationAndOtherRecordsUnchanged(t *testing.T) {
	ctx := context.Background()
	request := dismissalRequest(t)
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := ShowDismissal(ctx, missing, dismissalPrefix+strings.Repeat("0", 64)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := FindDismissal(ctx, missing, request.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := UndoDismissal(ctx, missing, dismissalPrefix+strings.Repeat("0", 64)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	matched, err := DismissedFindings(ctx, missing, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()})
	if err != nil || !reflect.DeepEqual(matched, []bool{false}) {
		t.Fatal(matched, err)
	}
	if _, err = os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reader/undo initialized missing store", err)
	}
	for version := 1; version <= 4; version++ {
		t.Run(fmt.Sprint("schema", version), func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			db, closeDB, e := openWithMigration(ctx, base, true, false)
			if e != nil {
				t.Fatal(e)
			}
			if version >= 2 {
				if e = migrateReviews(ctx, db); e != nil {
					t.Fatal(e)
				}
			}
			if version >= 3 {
				if e = migrateObservationStore(ctx, db, 2); e != nil {
					t.Fatal(e)
				}
			}
			if version == 4 {
				tx, e := db.Begin()
				if e != nil {
					t.Fatal(e)
				}
				if e = migrateJournalTx(ctx, tx, 3); e != nil {
					t.Fatal(e)
				}
				if e = tx.Commit(); e != nil {
					t.Fatal(e)
				}
			}
			closeDB()
			if _, e = FindDismissal(ctx, base, request.ID); !errors.Is(e, os.ErrNotExist) {
				t.Fatal(e)
			}
			m, e := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()})
			if e != nil || !reflect.DeepEqual(m, []bool{false}) || dismissalVersion(t, base) != version {
				t.Fatal("legacy reader migrated", m, e)
			}
			if _, e = SaveDismissal(ctx, base, request); e != nil || dismissalVersion(t, base) != 5 {
				t.Fatal(e)
			}
		})
	}
	base, plan, _, preparation := journalFixture(t)
	journal, err := PrepareJournal(ctx, base, preparation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = approve(ctx, base, plan.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, err := Show(ctx, base, plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	dismissed, err := SaveDismissal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = UndoDismissal(ctx, base, dismissed.ID); err != nil {
		t.Fatal(err)
	}
	after, err := Show(ctx, base, plan.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("dismissal changed plan/review/observation", after, err)
	}
	again, err := LoadJournal(ctx, base, journal.Intent.ID)
	if err != nil || !reflect.DeepEqual(again, journal) {
		t.Fatal("dismissal changed journal", again, err)
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var planIdentity string
	if err = db.QueryRow("SELECT token FROM plan_store_identity").Scan(&planIdentity); err != nil || planIdentity == dismissed.Record.StoreID {
		t.Fatal("dismissal reused approval identity", planIdentity, err)
	}
}

func TestDismissalCapacityKeepsExactUndoneRetry(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	first := dismissalRequest(t)
	saved, err := SaveDismissal(ctx, base, first)
	if err != nil {
		t.Fatal(err)
	}
	undone, err := UndoDismissal(ctx, base, saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < DismissalLimit; i++ {
		s := dismissalSelection()
		s.Targets[0].Target.ChangedNS += int64(i)
		r, e := NewDismissalRequest([]byte("/fixture"), s)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = SaveDismissal(ctx, base, r); e != nil {
			t.Fatal(i, e)
		}
	}
	again, err := SaveDismissal(ctx, base, first)
	if err != nil || !reflect.DeepEqual(again, undone) {
		t.Fatal("capacity broke exact retry", again, err)
	}
	s := dismissalSelection()
	s.Targets[0].Target.ChangedNS += 999
	r, err := NewDismissalRequest([]byte("/fixture"), s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = SaveDismissal(ctx, base, r); !errors.Is(err, ErrDismissalCapacity) {
		t.Fatal(err)
	}
	again, err = ShowDismissal(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(again, undone) {
		t.Fatal("capacity discarded history", again, err)
	}
}

func TestDismissalCancellationMigrationRollbackAndUncertainReplies(t *testing.T) {
	request := dismissalRequest(t)
	ctx := context.Background()
	missing := filepath.Join(t.TempDir(), "missing")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := SaveDismissal(canceled, missing, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	bad := request
	bad.Executable = true
	if _, err := SaveDismissal(ctx, missing, bad); !errors.Is(err, ErrDismissalRequest) {
		t.Fatal(err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid request initialized storage", err)
	}
	base := filepath.Join(t.TempDir(), "state")
	plan, err := Save(ctx, base, selection())
	if err != nil {
		t.Fatal(err)
	}
	version := dismissalVersion(t, base)
	call, cancel := context.WithCancel(ctx)
	saved, err := saveDismissal(call, base, request, dismissalHooks{beforeCommit: cancel})
	if !errors.Is(err, context.Canceled) || saved.ID != "" || dismissalVersion(t, base) != version {
		t.Fatal("cancellation published/migrated", saved, err)
	}
	if _, err = FindDismissal(ctx, base, request.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err = Load(ctx, base, plan.ID); err != nil {
		t.Fatal(err)
	}
	call, cancel = context.WithCancel(ctx)
	saved, err = saveDismissal(call, base, request, dismissalHooks{afterCommit: cancel})
	if !errors.Is(err, context.Canceled) || !ValidDismissalID(saved.ID) {
		t.Fatal("postcommit cancellation lost candidate ID", saved, err)
	}
	again, err := FindDismissal(ctx, base, request.ID)
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal("lost reply not recoverable offline", again, err)
	}
	other := dismissalSelection()
	other.Targets[0].Target.ChangedNS++
	r, err := NewDismissalRequest([]byte("/fixture"), other)
	if err != nil {
		t.Fatal(err)
	}
	saved, err = saveDismissal(ctx, base, r, dismissalHooks{commit: func(tx *sql.Tx) error {
		if e := tx.Commit(); e != nil {
			return e
		}
		return errors.New("lost commit reply")
	}})
	if !errors.Is(err, ErrDismissalPublication) || !ValidDismissalID(saved.ID) {
		t.Fatal(saved, err)
	}
	again, err = FindDismissal(ctx, base, r.ID)
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal(again, err)
	}
	call, cancel = context.WithCancel(ctx)
	if failed, e := undoDismissal(call, base, saved.ID, dismissalHooks{beforeCommit: cancel}); !errors.Is(e, context.Canceled) || failed.ID != "" {
		t.Fatal("canceled undo published", failed, e)
	}
	if still, e := ShowDismissal(ctx, base, saved.ID); e != nil || still.Undo != nil {
		t.Fatal("canceled undo changed state", still, e)
	}
	call, cancel = context.WithCancel(ctx)
	undone, e := undoDismissal(call, base, saved.ID, dismissalHooks{afterCommit: cancel})
	if !errors.Is(e, context.Canceled) || undone.ID != saved.ID || undone.Undo == nil {
		t.Fatal("committed undo lost candidate", undone, e)
	}
	if recovered, e := ShowDismissal(ctx, base, saved.ID); e != nil || !reflect.DeepEqual(recovered, undone) {
		t.Fatal("undo lost reply unrecoverable", recovered, e)
	}
}

func TestDismissalImmutableCorruptRecordsAndCanceledMatcher(t *testing.T) {
	for _, kind := range []string{"digest", "canonical", "wrong_store", "authority", "undo", "missing_identity", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			base := filepath.Join(t.TempDir(), "state")
			request := dismissalRequest(t)
			saved, err := SaveDismissal(ctx, base, request)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "undo" {
				saved, err = UndoDismissal(ctx, base, saved.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{"UPDATE finding_dismissals SET payload=X'00'", "DELETE FROM finding_dismissals", "UPDATE dismissal_store_identity SET token='bad'", "DELETE FROM dismissal_store_identity"} {
				if _, err = db.Exec(q); err == nil {
					t.Fatal("immutable dismissal changed", q)
				}
			}
			if _, err = db.Exec("DROP TRIGGER finding_dismissals_no_update"); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing_identity":
				_, err = db.Exec("DROP TRIGGER dismissal_store_identity_no_delete; DELETE FROM dismissal_store_identity")
			case "undo":
				_, err = db.Exec("DROP TRIGGER finding_dismissal_undos_no_update; UPDATE finding_dismissal_undos SET payload=X'00'")
			case "oversized":
				_, err = db.Exec("PRAGMA ignore_check_constraints=ON; UPDATE finding_dismissals SET payload=?", []byte(strings.Repeat("x", DismissalMaxRecordBytes+1)))
			default:
				record := saved.Record
				if kind == "wrong_store" {
					record.StoreID = strings.Repeat("b", 64)
				}
				if kind == "authority" {
					record.Request.Executable = true
				}
				payload, _ := json.Marshal(record)
				if kind == "canonical" {
					payload = append(payload, '\n')
				}
				id := digest(dismissalPrefix, payload)
				if kind == "digest" {
					id = saved.ID
					payload = []byte("{}")
				}
				_, err = db.Exec("UPDATE finding_dismissals SET id=?,payload=?", id, payload)
				saved.ID = id
			}
			closeDB()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ShowDismissal(ctx, base, saved.ID); !errors.Is(err, ErrDismissalCorrupt) {
				t.Fatal(kind, err)
			}
			m, err := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()})
			if !errors.Is(err, ErrDismissalCorrupt) || m != nil {
				t.Fatal("corruption produced partial matcher", m, err)
			}
			unknown := dismissalSelection()
			unknown.Targets[0].Target.Inode = ""
			unknown.Evidence.Findings[0].Inode = ""
			if kind == "missing_identity" {
				if _, err = DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{unknown}); !errors.Is(err, ErrDismissalCorrupt) {
					t.Fatal("unusable row bypassed store validation", err)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if m, err := DismissedFindings(ctx, filepath.Join(t.TempDir(), "missing"), []byte("/fixture"), []state.SelectionSnapshot{dismissalSelection()}); !errors.Is(err, context.Canceled) || m != nil {
		t.Fatal(m, err)
	}
	base := filepath.Join(t.TempDir(), "state")
	if _, err := SaveDismissal(context.Background(), base, dismissalRequest(t)); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := open(context.Background(), base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP TABLE finding_dismissal_undos"); err != nil {
		t.Fatal(err)
	}
	closeDB()
	if m, err := DismissedFindings(context.Background(), base, []byte("/fixture"), nil); !errors.Is(err, ErrDismissalCorrupt) || m != nil {
		t.Fatal("empty page bypassed corrupt schema5", m, err)
	}
}

func TestDismissalUndoNeverInitializesMissingLockAndReadersAllowWriter(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	request := dismissalRequest(t)
	saved, err := SaveDismissal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ShowDismissal(ctx, base, saved.ID); err != nil {
		t.Fatal("reader blocked by writer", err)
	}
	if _, err = UndoDismissal(ctx, base, saved.ID); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal(err)
	}
	var sync int
	if err = db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatal(sync, err)
	}
	closeDB()
	lock := filepath.Join(base, "plans", "writer.lock")
	if err = os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if _, err = UndoDismissal(ctx, base, saved.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("undo recreated missing lock", err)
	}
	if _, err = os.Lstat(lock); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lock recreated", err)
	}
	again, err := ShowDismissal(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal("refused undo changed record", again, err)
	}
}
