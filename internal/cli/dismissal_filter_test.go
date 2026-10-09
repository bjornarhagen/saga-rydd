package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func saveFixtureDismissal(t *testing.T, f guidedReviewFixture, id string) plans.SavedDismissal {
	t.Helper()
	ctx := context.Background()
	s, err := state.OpenReader(ctx, f.state)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := s.SnapshotSelection(ctx, []string{id}, 30)
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	request, err := plans.NewDismissalRequest([]byte(f.root), selection)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := plans.SaveDismissal(ctx, f.base, request)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func dismissalCandidateReport(t *testing.T, f guidedReviewFixture, options ...string) state.FindingReport {
	t.Helper()
	args := []string{"--data-dir", f.base, "report", "--candidates", "-d", f.root, "--min-age-days", "30", "--json"}
	args = append(args, options...)
	var out, stderr bytes.Buffer
	code := Run(context.Background(), args, &out, &stderr)
	var envelope struct {
		OK     bool             `json:"ok"`
		Report state.FileReport `json:"report"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 0 || stderr.Len() != 0 || !envelope.OK || envelope.Report.Candidates == nil {
		t.Fatal(code, out.String(), stderr.String(), err)
	}
	return *envelope.Report.Candidates
}

func diagnosticCount(r state.FindingReport, code string) int {
	for _, diagnostic := range r.Diagnostics {
		if diagnostic.Code == code {
			return diagnostic.Count
		}
	}
	return 0
}

func assertDismissalPageCounts(t *testing.T, before, after state.FindingReport, hidden int) {
	t.Helper()
	count := 0
	for _, diagnostic := range after.Diagnostics {
		count += diagnostic.Count
	}
	if before.EntriesExamined != after.EntriesExamined || before.EntryLimit != after.EntryLimit || before.NextCursor != after.NextCursor || before.PageCoverage != after.PageCoverage || count != after.EntriesExamined || diagnosticCount(after, "dismissed") != hidden || diagnosticCount(after, "selected") != len(after.Findings) {
		t.Fatal("filter changed raw coverage, cursor or diagnostic accounting", before, after)
	}
}

func TestDismissalCandidateFilterMixedUndoAndSavedPlanIndependence(t *testing.T) {
	f := newGuidedReviewFixture(t, 3, true)
	ctx := context.Background()
	before := dismissalCandidateReport(t, f)
	inventoryBefore, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	s, err := state.OpenReader(ctx, f.state)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := s.SnapshotSelection(ctx, []string{before.Findings[1].ID}, 30)
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := plans.Save(ctx, f.base, selection)
	if err != nil {
		t.Fatal(err)
	}
	dismissal := saveFixtureDismissal(t, f, before.Findings[1].ID)
	after := dismissalCandidateReport(t, f)
	assertDismissalPageCounts(t, before, after, 1)
	if len(after.Findings) != 2 || after.Findings[0].ID != before.Findings[0].ID || after.Findings[1].ID != before.Findings[2].ID || after.Findings[0].Measurement.Status != "partial" {
		t.Fatal("filter hid unrelated evidence or lost partial measurements", after)
	}
	visible := dismissalCandidateReport(t, f, "--include-dismissed")
	assertDismissalPageCounts(t, before, visible, 0)
	if len(visible.Findings) != 3 || !strings.Contains(strings.Join(visible.Notes, " "), before.Findings[1].ID) {
		t.Fatal("inspection omitted dismissed finding or its reference", visible)
	}
	shown, err := plans.Show(ctx, f.base, plan.ID)
	if err != nil || !reflect.DeepEqual(shown.Record, plan.Record) {
		t.Fatal("dismissal changed an existing saved plan", shown, err)
	}
	s, err = state.OpenReader(ctx, f.state)
	if err != nil {
		t.Fatal(err)
	}
	check, err := s.CheckSelection(ctx, selection)
	s.Close()
	if err != nil || check.Status == "changed" {
		t.Fatal("dismissal changed inventory evidence for the saved plan", check, err)
	}
	if _, err := plans.UndoDismissal(ctx, f.base, dismissal.ID); err != nil {
		t.Fatal(err)
	}
	restored := dismissalCandidateReport(t, f)
	assertDismissalPageCounts(t, before, restored, 0)
	if len(restored.Findings) != 3 {
		t.Fatal("undo did not restore visibility", restored)
	}
	inventoryAfter, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil || !bytes.Equal(inventoryBefore, inventoryAfter) {
		t.Fatal("dismissal/report/undo changed inventory", err)
	}
	for i := 0; i < 3; i++ {
		data, err := os.ReadFile(filepath.Join(f.root, fmt.Sprintf("project%02d", i), "node_modules", "keep.txt"))
		if err != nil || string(data) != "original dependency" {
			t.Fatal("dismissal changed source content", err)
		}
	}
}

func TestDismissalCandidateFilterAllHiddenKeepsBoundedContinuationAndReview(t *testing.T) {
	f := newGuidedReviewFixture(t, 21, false)
	before := dismissalCandidateReport(t, f)
	if len(before.Findings) != 20 || before.NextCursor == "" {
		t.Fatal("fixture needs a full continued candidate page", before)
	}
	for _, finding := range before.Findings {
		saveFixtureDismissal(t, f, finding.ID)
	}
	after := dismissalCandidateReport(t, f)
	assertDismissalPageCounts(t, before, after, 20)
	if len(after.Findings) != 0 {
		t.Fatal("filter refilled the all-hidden page", after)
	}
	page, err := loadDismissalReviewPage(context.Background(), f.base, f.state, f.root, "", 30)
	if err != nil || len(page.Evidence.Findings) != 0 || page.NextCursor != before.NextCursor || diagnosticCount(page.Evidence, "dismissed") != 20 {
		t.Fatal("guided review did not use the same bounded filter", page, err)
	}
	next := dismissalCandidateReport(t, f, "--cursor", after.NextCursor)
	if len(next.Findings) != 1 || next.NextCursor != "" || diagnosticCount(next, "dismissed") != 0 {
		t.Fatal("dismissal leaked into unselected next-page findings", next)
	}
	code, human, stderr := f.run(strings.NewReader("next\nquit\n"))
	flat := strings.Join(strings.Fields(human), " ")
	if code != 0 || stderr != "" || !strings.Contains(flat, "Dismissed saved findings 20") || !strings.Contains(human, next.Findings[0].ID) || strings.Contains(human, before.Findings[0].Path) {
		t.Fatal("review omitted the dismissal cause or lost next navigation", code, human, stderr)
	}
	var out, errorsOut bytes.Buffer
	code = Run(context.Background(), []string{"--data-dir", f.base, "report", "--candidates", "-d", f.root, "--min-age-days", "30", "--include-dismissed"}, &out, &errorsOut)
	if code != 0 || errorsOut.Len() != 0 || !strings.Contains(out.String(), "--include-dismissed --cursor ") {
		t.Fatal("included report continuation lost its mode", code, out.String(), errorsOut.String())
	}
}

func TestDismissalCandidateFilterChangedScanResurfacesAndReadersDoNotInitialize(t *testing.T) {
	f := newGuidedReviewFixture(t, 1, false)
	before := dismissalCandidateReport(t, f)
	f.assertNoPlan()
	saved := saveFixtureDismissal(t, f, before.Findings[0].ID)
	if report := dismissalCandidateReport(t, f); len(report.Findings) != 0 {
		t.Fatal("exact dismissal was not applied", report)
	}
	f.seed(1, false)
	after := dismissalCandidateReport(t, f)
	if len(after.Findings) != 1 || after.Findings[0].ID != before.Findings[0].ID || diagnosticCount(after, "dismissed") != 0 {
		t.Fatal("changed observations stayed hidden under a reused finding ID", before, after)
	}
	shown, err := plans.ShowDismissal(context.Background(), f.base, saved.ID)
	if err != nil || shown.Status != "dismissed" || shown.CurrentEvidenceMatchEvaluated {
		t.Fatal("saved-only show evaluated current applicability", shown, err)
	}
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", f.base, "report", "--include-dismissed", "--json"}, &out, &stderr)
	if code != 2 || stderr.Len() != 0 || !strings.Contains(out.String(), "invalid_arguments") {
		t.Fatal("inspection flag was accepted outside candidates", code, out.String(), stderr.String())
	}
}

func TestDismissalCandidateFilterLegacyInventoryStaysVisibleWithoutMigration(t *testing.T) {
	f := newGuidedReviewFixture(t, 1, false)
	before := dismissalCandidateReport(t, f)
	saveFixtureDismissal(t, f, before.Findings[0].ID)
	database := filepath.Join(f.state, state.Filename)
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA user_version=8"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	bytesBefore, err := os.ReadFile(database)
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range [][]string{nil, {"--include-dismissed"}} {
		after := dismissalCandidateReport(t, f, options...)
		if len(after.Findings) != 1 || after.Findings[0].ID != before.Findings[0].ID || diagnosticCount(after, "dismissed") != 0 {
			t.Fatal("legacy report hid a finding without schema-9 identity evidence", after)
		}
	}
	bytesAfter, err := os.ReadFile(database)
	if err != nil || !bytes.Equal(bytesBefore, bytesAfter) {
		t.Fatal("legacy report migrated or changed inventory", err)
	}
}
