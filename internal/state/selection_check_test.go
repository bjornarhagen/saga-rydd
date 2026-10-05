package state

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func checkFixture(t *testing.T) (*Store, SelectionSnapshot) {
	t.Helper()
	s, id := savedSelectionFixture(t)
	if _, err := s.db.Exec(`UPDATE roots SET volume_id='root-device-inode';
INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1);
UPDATE directories SET complete=1,checked_at_ns=20`); err != nil {
		t.Fatal(err)
	}
	saved, err := s.SnapshotSelection(context.Background(), []string{id}, 90)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the JSON round trip used by the plan store, not pointer equality.
	payload, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &saved); err != nil {
		t.Fatal(err)
	}
	return s, saved
}

func TestSelectionCheckChangesAndUnknownEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, query, status, issue string
	}{
		{"unchanged", "", "matches_saved_inventory", ""},
		{"root fingerprint", "UPDATE roots SET volume_id='replacement'", "changed", "root_evidence_changed"},
		{"root revision", "UPDATE allocation_revisions SET revision=revision+1", "changed", "root_evidence_changed"},
		{"root path", "UPDATE roots SET path=CAST('/elsewhere' AS BLOB)", "changed", "root_evidence_changed"},
		{"disabled root", "UPDATE roots SET enabled=0", "changed", "selection_unavailable"},
		{"missing target", "DELETE FROM entries WHERE path=CAST('a/node_modules' AS BLOB)", "changed", "selection_unavailable"},
		{"newer target", "UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=CAST('a/node_modules' AS BLOB)", "changed", "selection_unavailable"},
		{"target replacement", "UPDATE entries SET inode='replacement' WHERE path=CAST('a/node_modules' AS BLOB)", "changed", "target_identity_changed"},
		{"target ctime", "UPDATE entries SET ctime_ns=3 WHERE path=CAST('a/node_modules' AS BLOB)", "changed", "target_identity_changed"},
		{"manifest replacement", "UPDATE entries SET inode='replacement' WHERE path=CAST('a/package.json' AS BLOB)", "changed", "target_identity_changed"},
		{"manifest ctime", "UPDATE entries SET ctime_ns=3 WHERE path=CAST('a/package.json' AS BLOB)", "changed", "target_identity_changed"},
		{"child changes", "UPDATE entries SET size=8 WHERE path=CAST('a/node_modules/file' AS BLOB)", "changed", "finding_evidence_changed"},
		{"incomplete listing", "UPDATE directories SET complete=0 WHERE path=CAST('a/node_modules' AS BLOB)", "changed", "target_evidence_unknown"},
		{"root error", "UPDATE roots SET last_error='offline'", "changed", "target_evidence_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, saved := checkFixture(t)
			if tc.query != "" {
				if _, err := s.db.Exec(tc.query); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.CheckSelection(context.Background(), saved)
			if err != nil || r.Status != tc.status || r.ApprovalAvailable || r.Executable || r.CurrentStateVerified || r.SelectedTargets != 1 {
				t.Fatal(r, err)
			}
			if tc.issue != "" && !hasIssue(r, tc.issue) {
				t.Fatal(r)
			}
		})
	}
	for _, tc := range []struct{ name, query, code string }{
		{"missing root identity", "UPDATE roots SET volume_id=''", "root_evidence_unknown"},
		{"missing revision", "DELETE FROM allocation_revisions", "root_evidence_unknown"},
		{"unknown target", "UPDATE entries SET inode='' WHERE path=CAST('a/node_modules' AS BLOB)", "target_evidence_unknown"},
		{"unknown manifest", "UPDATE entries SET device='' WHERE path=CAST('a/package.json' AS BLOB)", "target_evidence_unknown"},
		{"missing ctime", "UPDATE entries SET ctime_ns=0 WHERE path=CAST('a/node_modules' AS BLOB)", "target_evidence_unknown"},
		{"partial coverage", "UPDATE directories SET complete=0 WHERE path=CAST('a/node_modules' AS BLOB)", "target_evidence_unknown"},
		{"unknown file inode", "UPDATE entries SET inode='' WHERE kind='file'", "target_evidence_unknown"},
	} {
		t.Run("unchanged "+tc.name, func(t *testing.T) {
			s, saved := checkFixture(t)
			if _, err := s.db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			saved, err := s.SnapshotSelection(context.Background(), []string{saved.Targets[0].FindingID}, 90)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.CheckSelection(context.Background(), saved)
			if err != nil || r.Status != "unverifiable" || !hasIssue(r, tc.code) {
				t.Fatal(r, err)
			}
		})
	}
}

func hasIssue(r SelectionCheck, code string) bool {
	for _, issue := range r.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func TestSelectionCheckInventoryIdentityAndBounds(t *testing.T) {
	s, saved := checkFixture(t)
	other, _ := checkFixture(t)
	r, err := other.CheckSelection(context.Background(), saved)
	if err != nil || r.Status != "changed" || len(r.Issues) != 1 || r.Issues[0].Code != "different_inventory" {
		t.Fatal(r, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.CheckSelection(canceled, saved); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = s.CheckSelection(context.Background(), SelectionSnapshot{}); !errors.Is(err, ErrFindingSelection) {
		t.Fatal(err)
	}
	oversized := saved
	oversized.Evidence.Findings = make([]Finding, PreviewTargetLimit+1)
	if _, err = s.CheckSelection(context.Background(), oversized); !errors.Is(err, ErrFindingSelection) {
		t.Fatal(err)
	}
	// Unknown evidence in the frozen record does not become trusted when later
	// observations fill the gap. It requires a new selection.
	saved.Roots[0].Fingerprint = ""
	r, err = s.CheckSelection(context.Background(), saved)
	if err != nil || r.Status != "changed" || !hasIssue(r, "root_evidence_unknown") {
		t.Fatal(r, err)
	}
	// Old schema readers remain usable but cannot check incarnation identity.
	s.schema = 8
	if _, err = s.CheckSelection(context.Background(), saved); !errors.Is(err, ErrPlanSchema) {
		t.Fatal(err)
	}
}

func TestSelectionCheckSingleSnapshot(t *testing.T) {
	w, saved := checkFixture(t)
	ctx := context.Background()
	r, err := OpenReader(ctx, filepath.Dir(w.path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var token string
	if err = tx.QueryRow("SELECT token FROM inventory_identity").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if _, err = w.db.Exec("UPDATE entries SET ctime_ns=999; UPDATE allocation_revisions SET revision=2"); err != nil {
		t.Fatal(err)
	}
	check, err := r.checkSelection(ctx, saved, tx)
	if err != nil || check.Status != "matches_saved_inventory" {
		t.Fatal(check, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check, err = r.CheckSelection(ctx, saved)
	if err != nil || check.Status != "changed" || !hasIssue(check, "target_identity_changed") {
		t.Fatal(check, err)
	}
}

func TestSelectionCheckPathBytes(t *testing.T) {
	s, saved := checkFixture(t)
	f := saved.Evidence.Findings[0]
	f.Path, f.ManifestPath, f.Measurement.Path = "display only", "display only", "display only"
	if !sameFindingEvidence(f, saved.Evidence.Findings[0]) {
		t.Fatal("display paths used as identity")
	}
	f.PathBytes = []byte("/fixture/elsewhere/node_modules")
	if sameFindingEvidence(f, saved.Evidence.Findings[0]) || knownFinding(f, saved.Roots) {
		t.Fatal("authoritative path mismatch accepted")
	}
	for _, path := range []string{"relative", "/fixture/../elsewhere", "/fixture/zero\x00", "/" + strings.Repeat("x", 4096)} {
		if canonicalPath([]byte(path)) {
			t.Fatal(path)
		}
	}
	if !canonicalPath([]byte("/fixture/\xff/node_modules")) {
		t.Fatal("valid Unix byte path rejected")
	}
	// Reader operations leave SQLite's logical contents unchanged.
	var before, after int
	if err := s.db.QueryRow("PRAGMA data_version").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CheckSelection(context.Background(), saved); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("PRAGMA data_version").Scan(&after); err != nil || before != after {
		t.Fatal(before, after, err)
	}
}
