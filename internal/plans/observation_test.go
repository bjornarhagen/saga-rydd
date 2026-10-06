package plans

import (
	"bufio"
	"context"
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

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func observationReport(saved Saved) inventory.InspectionReport {
	r := inventory.InspectionReport{Status: "inputs_and_tree_observed", Source: "live_project_inputs_and_tree_metadata", CheckedAt: time.Now().UTC(), LocalDependencyEdits: "unknown"}
	for _, target := range saved.Record.Selection.Targets {
		r.Targets = append(r.Targets, inventory.InspectionResult{FindingID: target.FindingID, Status: r.Status, Message: "not stored",
			Inputs: &inventory.InputEvidence{LockfileVersion: 3, LockedPackages: 1, Files: []inventory.InputFileEvidence{
				{Name: "package.json", Bytes: 30, SHA256: strings.Repeat("b", 64)},
				{Name: "package-lock.json", Bytes: 400, SHA256: strings.Repeat("c", 64)},
			}},
			Tree: &inventory.TreeEvidence{Status: "metadata_observed", Entries: 4, Directories: 2, RegularFiles: 1, InternalBinLinks: 1, MetadataSHA256: strings.Repeat("d", 64)},
		})
	}
	return r
}

func TestObservationCaptureLoadAndRetry(t *testing.T) {
	ctx := context.Background()
	base, saved := reviewFixture(t)
	approved, err := approve(ctx, base, saved.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	r := observationReport(saved)
	captured, err := Capture(ctx, base, saved.ID, r)
	if err != nil || !ValidObservationID(captured.ID) || !captured.Record.ObservedAt.Equal(r.CheckedAt) || captured.Record.PlanID != saved.ID || captured.Record.InventoryID != saved.Record.Selection.InventoryID || captured.Record.Contract != ObservationContract {
		t.Fatal(captured, err)
	}
	loaded, plan, err := LoadObservation(ctx, base, captured.ID)
	if err != nil || !reflect.DeepEqual(captured, loaded) || !reflect.DeepEqual(plan.Record, saved.Record) || plan.Review != nil {
		t.Fatal(loaded, plan, err)
	}
	shown, err := Show(ctx, base, saved.ID)
	if err != nil || shown.Observation == nil || !reflect.DeepEqual(*shown.Observation, captured) || !reflect.DeepEqual(shown.Record, saved.Record) || !reflect.DeepEqual(shown.Review, approved.Review) {
		t.Fatal(shown, err)
	}
	// A retry's observation time and messages cannot advance the original baseline.
	r.CheckedAt = time.Now().UTC()
	r.Targets[0].Message = "a different explanation"
	again, err := Capture(ctx, base, saved.ID, r)
	if err != nil || !reflect.DeepEqual(again, captured) {
		t.Fatal("retry changed baseline", again, err)
	}
	r.Targets[0].Inputs.Files[1].SHA256 = strings.Repeat("e", 64)
	if _, err = Capture(ctx, base, saved.ID, r); !errors.Is(err, ErrObservationConflict) {
		t.Fatal("changed baseline replaced", err)
	}
	again, _, err = LoadObservation(ctx, base, captured.ID)
	if err != nil || !reflect.DeepEqual(again, captured) {
		t.Fatal(again, err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	for _, query := range []string{"UPDATE observations SET payload=payload", "DELETE FROM observations"} {
		if _, err = db.Exec(query); err == nil {
			t.Fatal("mutable observation", query)
		}
	}
	payload, _ := json.Marshal(captured.Record)
	if strings.Contains(string(payload), "not stored") || len(payload) >= MaxObservationBytes || !validDigest(captured.Record.StoreID) {
		t.Fatal("unbounded or unbound payload")
	}
	if _, err = db.Exec("INSERT INTO observations VALUES(?,?,?)", captured.ID+"other", saved.ID, payload); err == nil {
		t.Fatal("multiple observations per plan")
	}
	var sync, count int
	if err = db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatal(sync, err)
	}
	if err = db.QueryRow("SELECT count(*) FROM observations").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func downgradeObservationStore(t *testing.T, base string, version int) {
	t.Helper()
	db, closeDB, err := open(context.Background(), base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	query := "DROP TABLE observations; PRAGMA user_version=2"
	if version == 1 {
		query = "DROP TABLE observations; DROP TABLE review_revocations; DROP TABLE review_approvals; DROP TABLE plan_store_identity; PRAGMA user_version=1"
	}
	if _, err = db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

func TestObservationInvalidEvidenceDoesNotMigrate(t *testing.T) {
	cases := map[string]func(*inventory.InspectionReport){
		"aggregate":      func(r *inventory.InspectionReport) { r.Status = "blocked" },
		"source":         func(r *inventory.InspectionReport) { r.Source = "saved_inventory" },
		"empty":          func(r *inventory.InspectionReport) { r.Targets = nil },
		"partial":        func(r *inventory.InspectionReport) { r.Targets[0].Status = "blocked" },
		"finding":        func(r *inventory.InspectionReport) { r.Targets[0].FindingID = "node-modules-v1:1:999" },
		"code":           func(r *inventory.InspectionReport) { r.Targets[0].Code = "blocked" },
		"inputs_missing": func(r *inventory.InspectionReport) { r.Targets[0].Inputs = nil },
		"tree_missing":   func(r *inventory.InspectionReport) { r.Targets[0].Tree = nil },
		"future":         func(r *inventory.InspectionReport) { r.CheckedAt = time.Now().Add(time.Hour) },
		"zero_time":      func(r *inventory.InspectionReport) { r.CheckedAt = time.Time{} },
		"before_plan":    func(r *inventory.InspectionReport) { r.CheckedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"verified":       func(r *inventory.InspectionReport) { r.CurrentStateVerified = true },
		"executable":     func(r *inventory.InspectionReport) { r.Executable = true },
		"regeneration":   func(r *inventory.InspectionReport) { r.RegenerationVerified = true },
		"contents":       func(r *inventory.InspectionReport) { r.DependencyContentsChecked = true },
		"edits":          func(r *inventory.InspectionReport) { r.LocalDependencyEdits = "none" },
		"counter":        func(r *inventory.InspectionReport) { r.Targets[0].Tree.Entries++ },
		"digest":         func(r *inventory.InspectionReport) { r.Targets[0].Inputs.Files[0].SHA256 = strings.Repeat("B", 64) },
		"unknown_file":   func(r *inventory.InspectionReport) { r.Targets[0].Inputs.Files[0].Name = "secrets" },
		"input_only": func(r *inventory.InspectionReport) {
			r.Status = "inputs_observed"
			r.Source = "live_project_inputs"
			r.Targets[0].Tree = nil
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, saved := reviewFixture(t)
			downgradeObservationStore(t, base, 1)
			r := observationReport(saved)
			mutate(&r)
			if _, err := Capture(ctx, base, saved.ID, r); !errors.Is(err, ErrObservationEvidence) {
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
			if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name IN ('observations','plan_store_identity')").Scan(&tables); err != nil || version != 1 || tables != 0 {
				t.Fatal(version, tables, err)
			}
		})
	}
}

func TestObservationSelectionOrderAndBounds(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	s := selection()
	for i := 3; i < 22; i++ {
		finding := s.Evidence.Findings[0]
		finding.EntryID, finding.ID = int64(i), fmt.Sprintf("node-modules-v1:1:%d", i)
		s.Evidence.Findings = append(s.Evidence.Findings, finding)
		target := s.Targets[0]
		target.FindingID = finding.ID
		s.Targets = append(s.Targets, target)
	}
	saved, err := Save(ctx, base, s)
	if err != nil {
		t.Fatal(err)
	}
	r := observationReport(saved)
	r.Targets[0], r.Targets[1] = r.Targets[1], r.Targets[0]
	if _, err = Capture(ctx, base, saved.ID, r); !errors.Is(err, ErrObservationEvidence) {
		t.Fatal("reordered evidence accepted", err)
	}
	r = observationReport(saved)
	r.Targets[0].FindingID = r.Targets[1].FindingID
	if _, err = Capture(ctx, base, saved.ID, r); !errors.Is(err, ErrObservationEvidence) {
		t.Fatal("duplicate evidence accepted", err)
	}
	r = observationReport(saved)
	captured, err := Capture(ctx, base, saved.ID, r)
	if err != nil || len(captured.Record.Targets) != 20 {
		t.Fatal(captured, err)
	}
	payload, _ := json.Marshal(captured.Record)
	if len(payload) > MaxObservationBytes {
		t.Fatal(len(payload))
	}
	// Capture takes copies rather than retaining caller-owned evidence slices.
	r.Targets[0].Inputs.Files[0].SHA256 = strings.Repeat("e", 64)
	if captured.Record.Targets[0].Inputs.Files[0].SHA256 != strings.Repeat("b", 64) {
		t.Fatal("capture aliased input evidence")
	}
}

func TestObservationMigrationAndRollback(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, conflict := range []bool{false, true} {
			t.Run(fmt.Sprintf("schema%d/conflict%t", version, conflict), func(t *testing.T) {
				ctx := context.Background()
				base, saved := reviewFixture(t)
				downgradeObservationStore(t, base, version)
				before, err := Show(ctx, base, saved.ID)
				if err != nil || before.Observation != nil {
					t.Fatal(before, err)
				}
				if _, _, err = LoadObservation(ctx, base, observationPrefix+strings.Repeat("a", 64)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				if conflict {
					db, closeDB, e := openWithMigration(ctx, base, true, false)
					if e != nil {
						t.Fatal(e)
					}
					_, e = db.Exec("CREATE TABLE observations(conflict TEXT)")
					closeDB()
					if e != nil {
						t.Fatal(e)
					}
				}
				captured, err := Capture(ctx, base, saved.ID, observationReport(saved))
				if conflict == (err == nil) {
					t.Fatal(captured, err)
				}
				db, closeDB, err := open(ctx, base, false)
				if err != nil {
					t.Fatal(err)
				}
				defer closeDB()
				var actual, identities int
				if err = db.QueryRow("PRAGMA user_version").Scan(&actual); err != nil {
					t.Fatal(err)
				}
				want := 3
				if conflict {
					want = version
				}
				if actual != want {
					t.Fatal("migration version", actual, want)
				}
				if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='plan_store_identity'").Scan(&identities); err != nil || (version == 1 && conflict && identities != 0) {
					t.Fatal("partial migration", identities, err)
				}
				loaded, err := Load(ctx, base, saved.ID)
				if err != nil || !reflect.DeepEqual(loaded.Record, before.Record) {
					t.Fatal("plan migration changed bytes", loaded, err)
				}
			})
		}
	}
}

func TestObservationCorruptionAndBindings(t *testing.T) {
	cases := map[string]func(*ObservationRecord){
		"version":         func(r *ObservationRecord) { r.Version = 2 },
		"plan":            func(r *ObservationRecord) { r.PlanID = "plan-v1-" + strings.Repeat("f", 64) },
		"store":           func(r *ObservationRecord) { r.StoreID = strings.Repeat("f", 64) },
		"inventory":       func(r *ObservationRecord) { r.InventoryID = strings.Repeat("f", 64) },
		"contract":        func(r *ObservationRecord) { r.Contract = "future_executor" },
		"future":          func(r *ObservationRecord) { r.ObservedAt = time.Now().Add(time.Hour) },
		"old_time":        func(r *ObservationRecord) { r.ObservedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"status":          func(r *ObservationRecord) { r.Status = "approved" },
		"source":          func(r *ObservationRecord) { r.Source = "saved_inventory" },
		"target":          func(r *ObservationRecord) { r.Targets[0].FindingID = "node-modules-v1:1:999" },
		"target_format":   func(r *ObservationRecord) { r.Targets[0].FindingID = "node-modules-v1:01:2" },
		"target_status":   func(r *ObservationRecord) { r.Targets[0].Status = "blocked" },
		"no_targets":      func(r *ObservationRecord) { r.Targets = nil },
		"verified":        func(r *ObservationRecord) { r.CurrentStateVerified = true },
		"executable":      func(r *ObservationRecord) { r.Executable = true },
		"regeneration":    func(r *ObservationRecord) { r.RegenerationVerified = true },
		"contents":        func(r *ObservationRecord) { r.DependencyContentsChecked = true },
		"edits":           func(r *ObservationRecord) { r.LocalDependencyEdits = "none" },
		"lock_version":    func(r *ObservationRecord) { r.Targets[0].Inputs.LockfileVersion = 1 },
		"locked_negative": func(r *ObservationRecord) { r.Targets[0].Inputs.LockedPackages = -1 },
		"locked_limit":    func(r *ObservationRecord) { r.Targets[0].Inputs.LockedPackages = 50001 },
		"file_name":       func(r *ObservationRecord) { r.Targets[0].Inputs.Files[0].Name = ".npmrc" },
		"file_order":      func(r *ObservationRecord) { f := r.Targets[0].Inputs.Files; f[0], f[1] = f[1], f[0] },
		"file_limit":      func(r *ObservationRecord) { r.Targets[0].Inputs.Files[0].Bytes = inventory.ManifestInputLimit + 1 },
		"lock_limit":      func(r *ObservationRecord) { r.Targets[0].Inputs.Files[1].Bytes = inventory.LockInputLimit + 1 },
		"file_empty":      func(r *ObservationRecord) { r.Targets[0].Inputs.Files[0].Bytes = 0 },
		"file_digest":     func(r *ObservationRecord) { r.Targets[0].Inputs.Files[0].SHA256 = strings.Repeat("B", 64) },
		"tree_status":     func(r *ObservationRecord) { r.Targets[0].Tree.Status = "complete" },
		"tree_limit":      func(r *ObservationRecord) { r.Targets[0].Tree.Entries = inventory.TreeEntryLimit + 1 },
		"tree_count":      func(r *ObservationRecord) { r.Targets[0].Tree.Entries++ },
		"tree_negative":   func(r *ObservationRecord) { r.Targets[0].Tree.Directories = -1 },
		"tree_digest":     func(r *ObservationRecord) { r.Targets[0].Tree.MetadataSHA256 = "wrong" },
		"link_counts":     func(r *ObservationRecord) { r.Targets[0].Tree.Directories = 1; r.Targets[0].Tree.Entries = 3 },
	}
	for _, name := range []string{"payload", "digest", "unknown_field", "duplicate_field", "omitted_field", "oversize", "column_plan", "missing_plan", "corrupt_plan", "store_token"} {
		cases[name] = func(*ObservationRecord) {}
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, saved := reviewFixture(t)
			captured, err := Capture(ctx, base, saved.ID, observationReport(saved))
			if err != nil {
				t.Fatal(err)
			}
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			r := captured.Record
			mutate(&r)
			payload, _ := json.Marshal(r)
			switch name {
			case "payload":
				payload = []byte{0}
			case "unknown_field":
				payload = append(payload[:len(payload)-1], []byte(`,"extra":false}`)...)
			case "duplicate_field":
				payload = append(payload[:len(payload)-1], []byte(`,"version":1}`)...)
			case "omitted_field":
				payload = bytesReplace(payload, `,"executable":false`, "")
			case "oversize":
				payload = []byte(strings.Repeat("x", MaxObservationBytes+1))
			}
			id := digest(observationPrefix, payload)
			if name == "digest" {
				id = captured.ID
				payload = bytesReplace(payload, `"locked_packages":1`, `"locked_packages":2`)
			}
			if _, err = db.Exec("DROP TRIGGER observations_no_update; PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			columnPlan := saved.ID
			if name == "column_plan" {
				columnPlan = "wrong"
			}
			if _, err = db.Exec("UPDATE observations SET id=?,plan_id=?,payload=?", id, columnPlan, payload); err != nil {
				t.Fatal(err)
			}
			if name == "missing_plan" {
				_, err = db.Exec("DROP TRIGGER plans_no_delete; DELETE FROM plans")
			} else if name == "corrupt_plan" {
				_, err = db.Exec("DROP TRIGGER plans_no_update; UPDATE plans SET payload=X'00'")
			} else if name == "store_token" {
				_, err = db.Exec("DROP TRIGGER plan_store_identity_no_update; UPDATE plan_store_identity SET token=?", strings.Repeat("f", 64))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = LoadObservation(ctx, base, id); !errors.Is(err, ErrObservationCorrupt) {
				t.Fatal(name, err)
			}
			if name != "column_plan" && name != "missing_plan" && name != "corrupt_plan" {
				if _, err = Show(ctx, base, saved.ID); !errors.Is(err, ErrObservationCorrupt) && !errors.Is(err, ErrReviewCorrupt) {
					t.Fatal("show omitted corrupt baseline", err)
				}
			}
		})
	}
}

func bytesReplace(payload []byte, old, replacement string) []byte {
	return []byte(strings.Replace(string(payload), old, replacement, 1))
}

func TestObservationCompare(t *testing.T) {
	ctx := context.Background()
	base, saved := reviewFixture(t)
	captured, err := Capture(ctx, base, saved.ID, observationReport(saved))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, status string
		mutate       func(*inventory.InspectionReport)
		changes      []string
	}{
		{name: "match", status: "matches_observation", mutate: func(r *inventory.InspectionReport) {
			r.CheckedAt = time.Now().UTC()
			r.Targets[0].Message = "different message"
		}},
		{name: "manifest", status: "changed", mutate: func(r *inventory.InspectionReport) { r.Targets[0].Inputs.Files[0].SHA256 = strings.Repeat("e", 64) }, changes: []string{"manifest_bytes_changed"}},
		{name: "lock_size", status: "changed", mutate: func(r *inventory.InspectionReport) { r.Targets[0].Inputs.Files[1].Bytes++ }, changes: []string{"lock_bytes_changed"}},
		{name: "summary", status: "changed", mutate: func(r *inventory.InspectionReport) { r.Targets[0].Inputs.LockedPackages++ }, changes: []string{"input_summary_changed"}},
		{name: "tree", status: "changed", mutate: func(r *inventory.InspectionReport) { r.Targets[0].Tree.MetadataSHA256 = strings.Repeat("e", 64) }, changes: []string{"tree_metadata_changed"}},
		{name: "counts", status: "changed", mutate: func(r *inventory.InspectionReport) { r.Targets[0].Tree.Entries++; r.Targets[0].Tree.RegularFiles++ }, changes: []string{"tree_counts_changed"}},
		{name: "blocked", status: "blocked", mutate: func(r *inventory.InspectionReport) {
			r.Status = "blocked"
			r.Targets[0].Status = "blocked"
			r.Targets[0].Code = "tree_unsupported"
			r.Targets[0].Inputs = nil
			r.Targets[0].Tree = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := observationReport(saved)
			tc.mutate(&r)
			comparison, err := CompareObservation(captured, r)
			if err != nil || comparison.Status != tc.status || comparison.Executable || comparison.CurrentStateVerified || comparison.RegenerationVerified || comparison.DependencyContentsChecked || comparison.LocalDependencyEdits != "unknown" || !reflect.DeepEqual(comparison.Targets[0].Changes, tc.changes) {
				t.Fatal(comparison, err)
			}
		})
	}
	for _, mutate := range []func(*inventory.InspectionReport){
		func(r *inventory.InspectionReport) { r.Targets[0].FindingID = "other" },
		func(r *inventory.InspectionReport) { r.Executable = true },
		func(r *inventory.InspectionReport) { r.Status = "blocked" },
		func(r *inventory.InspectionReport) { r.Targets[0].Inputs = nil },
		func(r *inventory.InspectionReport) { r.Targets[0].Tree.RegularFiles = -1 },
		func(r *inventory.InspectionReport) { r.CheckedAt = captured.Record.ObservedAt.Add(-time.Nanosecond) },
		func(r *inventory.InspectionReport) { r.CheckedAt = time.Now().Add(time.Hour) },
	} {
		r := observationReport(saved)
		mutate(&r)
		if _, err := CompareObservation(captured, r); !errors.Is(err, ErrObservationEvidence) {
			t.Fatal(err)
		}
	}
	bad := captured
	bad.ID = observationPrefix + strings.Repeat("f", 64)
	if _, err = CompareObservation(bad, observationReport(saved)); !errors.Is(err, ErrObservationCorrupt) {
		t.Fatal(err)
	}
}

func TestObservationCancellationAndReadOnlyErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	base := filepath.Join(t.TempDir(), "missing")
	if _, err := Capture(ctx, base, "plan-v1-"+strings.Repeat("a", 64), inventory.InspectionReport{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := LoadObservation(context.Background(), base, "../escape"); !errors.Is(err, ErrObservationID) {
		t.Fatal(err)
	}
	if _, _, err := LoadObservation(context.Background(), base, observationPrefix+strings.Repeat("a", 64)); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatal("failed request created storage", err)
	}
	base, saved := reviewFixture(t)
	if _, err := Capture(ctx, base, saved.ID, observationReport(saved)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	captured, err := Capture(context.Background(), base, saved.ID, observationReport(saved))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadObservation(ctx, base, captured.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestObservationCrashHelper(t *testing.T) {
	base, planID, mode := os.Getenv("RYDD_OBSERVATION_CRASH_BASE"), os.Getenv("RYDD_OBSERVATION_CRASH_PLAN"), os.Getenv("RYDD_OBSERVATION_CRASH_MODE")
	if base == "" {
		return
	}
	ctx := context.Background()
	saved, err := Load(ctx, base, planID)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "published" {
		if _, err = Capture(ctx, base, planID, observationReport(saved)); err != nil {
			t.Fatal(err)
		}
	} else {
		db, closeDB, e := openWithMigration(ctx, base, true, false)
		if e != nil {
			t.Fatal(e)
		}
		defer closeDB()
		tx, e := db.Begin()
		if e != nil {
			t.Fatal(e)
		}
		if e = migrateObservationTx(ctx, tx, 1); e != nil {
			t.Fatal(e)
		}
		// Spill uncommitted pages past the cache while migration and rows stay private.
		for i := 0; i < 32; i++ {
			if _, e = tx.Exec("INSERT INTO observations VALUES(?,?,?)", fmt.Sprint(i), fmt.Sprint(i), []byte(strings.Repeat("x", MaxObservationBytes))); e != nil {
				t.Fatal(e)
			}
		}
	}
	fmt.Println("READY")
	time.Sleep(time.Minute)
}

func TestObservationCrashRecovery(t *testing.T) {
	for _, mode := range []string{"published", "uncommitted"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base, saved := reviewFixture(t)
			downgradeObservationStore(t, base, 1)
			cmd := exec.Command(os.Args[0], "-test.run=^TestObservationCrashHelper$")
			cmd.Env = append(os.Environ(), "RYDD_OBSERVATION_CRASH_BASE="+base, "RYDD_OBSERVATION_CRASH_PLAN="+saved.ID, "RYDD_OBSERVATION_CRASH_MODE="+mode)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			ready := make(chan bool, 1)
			go func() { scan := bufio.NewScanner(stdout); ready <- scan.Scan() && scan.Text() == "READY" }()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("crash helper not ready")
				}
			case <-time.After(20 * time.Second):
				t.Fatal("crash helper timed out")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("expected killed helper")
			}
			shown, err := Show(ctx, base, saved.ID)
			if err != nil || !reflect.DeepEqual(shown.Record, saved.Record) {
				t.Fatal(shown, err)
			}
			if mode == "published" {
				if shown.Observation == nil {
					t.Fatal("lost-response publication missing")
				}
				again, err := Capture(ctx, base, saved.ID, observationReport(saved))
				if err != nil || !reflect.DeepEqual(again, *shown.Observation) {
					t.Fatal(again, err)
				}
			} else {
				if shown.Observation != nil {
					t.Fatal("uncommitted capture leaked")
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
				if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name IN ('observations','plan_store_identity')").Scan(&tables); err != nil || version != 1 || tables != 0 {
					t.Fatal(version, tables, err)
				}
			}
		})
	}
}
