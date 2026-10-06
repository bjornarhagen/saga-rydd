package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestReviewSelectionPreservesViewedEvidenceAfterInventoryChange(t *testing.T) {
	for _, kind := range []string{"rescan", "new_inventory"} {
		t.Run(kind, func(t *testing.T) {
			f := reviewCLIFixture(t, false)
			paths, err := config.ResolvePaths(f.base)
			if err != nil {
				t.Fatal(err)
			}
			dir := manualState(paths, f.root)
			page, err := loadReviewPage(context.Background(), dir, "", 90)
			if err != nil || len(page.Evidence.Findings) != 1 || !reflect.DeepEqual(page.Evidence, page.Selection.Evidence) {
				t.Fatal(page, err)
			}
			if kind == "new_inventory" {
				if err = os.Rename(dir, dir+".old"); err != nil {
					t.Fatal(err)
				}
			} else if err = os.WriteFile(filepath.Join(f.root, "node_modules", "keep.txt"), []byte("changed fixture data"), 0600); err != nil {
				t.Fatal(err)
			}
			if code, raw := f.run("scan", "-d", f.root, "--compact", "--now", "--json"); code != 0 {
				t.Fatal(code, raw)
			}
			selected, err := selectReviewPage(page, []int{1})
			if err != nil || selected.InventoryID != page.Selection.InventoryID || !reflect.DeepEqual(selected.Targets, page.Selection.Targets) || !reflect.DeepEqual(selected.Roots, page.Selection.Roots) || !reflect.DeepEqual(selected.Evidence.Findings, page.Evidence.Findings) {
				t.Fatal("human selection substituted fresh evidence", selected, err)
			}
			// A record saved after the change contains exactly the viewed evidence.
			// The independent check reports staleness, rather than capturing again.
			saved, err := plans.Save(context.Background(), f.base, selected)
			if err != nil || !reflect.DeepEqual(saved.Record.Selection, selected) {
				t.Fatal(saved, err)
			}
			s, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			check, err := s.CheckSelection(context.Background(), selected)
			s.Close()
			if err != nil || check.Status != "changed" || check.CurrentStateVerified || check.Executable {
				t.Fatal(check, err)
			}
			if kind == "new_inventory" && (len(check.Issues) != 1 || check.Issues[0].Code != "different_inventory") {
				t.Fatal(check)
			}
		})
	}
}

func TestReviewPageSavedReadsOfflineAndWithoutWrites(t *testing.T) {
	f := reviewCLIFixture(t, false)
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	dir := manualState(paths, f.root)
	before, err := os.ReadFile(filepath.Join(dir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	page, err := loadReviewPage(context.Background(), dir, "", 90)
	if err != nil || len(page.Evidence.Findings) != 1 || page.Evidence.CurrentStateVerified || page.Evidence.PageCoverage != "selected_entries_only" {
		t.Fatal(page, err)
	}
	selected, err := selectReviewPage(page, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	s, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	check, err := s.CheckSelection(context.Background(), selected)
	s.Close()
	if err != nil || check.Status != "matches_saved_inventory" {
		t.Fatal("unchanged frozen evidence no longer matches", check, err)
	}
	after, err := os.ReadFile(filepath.Join(dir, state.Filename))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("page capture changed inventory bytes", err)
	}
	contents, err := os.ReadFile(filepath.Join(f.root+".offline", "node_modules", "keep.txt"))
	if err != nil || string(contents) != "fixture data" {
		t.Fatal("saved page read changed source data", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = loadReviewPage(ctx, dir, "", 90); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = loadReviewPage(context.Background(), dir, "bad", 90); !errors.Is(err, state.ErrReportCursor) {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err = loadReviewPage(context.Background(), missing, "", 90); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err = os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing inventory initialized", err)
	}
}

func reviewSubsetFixture() reviewPage {
	full := state.SelectionSnapshot{InventoryID: strings.Repeat("a", 64), Roots: []state.RootBinding{
		{ID: 1, PathBytes: []byte("/fixture/a"), Fingerprint: "root-a", Revision: 7},
		{ID: 2, PathBytes: []byte("/fixture/b"), Fingerprint: "root-b", Revision: 8},
	}, Evidence: state.FindingReport{MinimumAgeDays: 30, PageCoverage: "selected_entries_only", Source: "saved_inventory", EntriesExamined: 3, EntryLimit: 3,
		Diagnostics: []state.SelectionDiagnostic{{Code: "not_node_modules", Count: 9}, {Code: "selected", Count: 3}},
		Notes:       []string{"Selection diagnostics count only this page, with one first-match outcome per examined entry in displayed order."},
	}}
	for i, root := range []int64{1, 2, 1} {
		path := fmt.Sprintf("/fixture/%c/project-%d\xff\n/node_modules", 'a'+root-1, i+1)
		logical := int64(10 + i)
		finding := state.Finding{ID: fmt.Sprintf("node-modules-v1:%d:%d", root, i+1), RootID: root, EntryID: int64(i + 1), Path: "lossy display", PathBytes: []byte(path), ManifestPath: "lossy display", ManifestPathBytes: []byte(filepath.Join(filepath.Dir(path), "package.json")), Actions: []string{}, Measurement: state.DirectoryReport{PathBytes: []byte(path), LogicalBytes: &logical, Notes: []string{"saved measurement"}}}
		full.Evidence.Findings = append(full.Evidence.Findings, finding)
		full.Targets = append(full.Targets, state.TargetBinding{FindingID: finding.ID, Target: state.EntryBinding{Device: "42", Inode: fmt.Sprint(i + 1), ChangedNS: 5, Generation: 6}})
	}
	return reviewPage{Selection: full, Evidence: full.Evidence, NextCursor: "nm2:30:1000"}
}

func TestReviewSubsetRootOrderAndBytePaths(t *testing.T) {
	page := reviewSubsetFixture()
	selected, err := selectReviewPage(page, []int{3, 2})
	if err != nil || len(selected.Targets) != 2 || selected.Targets[0] != page.Selection.Targets[1] || selected.Targets[1] != page.Selection.Targets[2] || len(selected.Roots) != 2 || selected.Roots[0].ID != 2 || selected.Roots[1].ID != 1 {
		t.Fatal(selected, err)
	}
	for i, originalIndex := range []int{1, 2} {
		if !reflect.DeepEqual(selected.Evidence.Findings[i], page.Evidence.Findings[originalIndex]) {
			t.Fatal("raw frozen finding evidence changed", selected.Evidence.Findings[i])
		}
	}
	e := selected.Evidence
	if e.PageCoverage != "selected_entries_only" || e.NextCursor != "" || e.EntryLimit != 2 || e.EntriesExamined != 2 || e.Diagnostics[0].Count != 0 || e.Diagnostics[1].Count != 2 || !strings.Contains(e.Notes[0], "only the explicitly selected saved findings") {
		t.Fatal("subset describes discovery page counts", e)
	}
	// Returned rows have independent storage; later presentation edits cannot
	// alter the originally displayed page or its evidence.
	selected.Roots[0].PathBytes[0] = 'x'
	selected.Evidence.Findings[0].PathBytes[0] = 'x'
	selected.Evidence.Findings[0].ManifestPathBytes[0] = 'x'
	selected.Evidence.Findings[0].Measurement.PathBytes[0] = 'x'
	selected.Evidence.Findings[0].Measurement.Notes[0] = "changed"
	*selected.Evidence.Findings[0].Measurement.LogicalBytes = 999
	if page.Selection.Roots[1].PathBytes[0] != '/' || page.Evidence.Findings[1].PathBytes[0] != '/' || page.Evidence.Findings[1].ManifestPathBytes[0] != '/' || page.Evidence.Findings[1].Measurement.PathBytes[0] != '/' || page.Evidence.Findings[1].Measurement.Notes[0] != "saved measurement" || *page.Evidence.Findings[1].Measurement.LogicalBytes != 11 || page.Evidence.Diagnostics[1].Count != 3 {
		t.Fatal("subset shares mutable frozen evidence", page)
	}
}

func TestReviewSubsetRejectsInvalidRowsAndBindings(t *testing.T) {
	page := reviewSubsetFixture()
	for _, numbers := range [][]int{nil, {}, {0}, {-1}, {4}, {1, 1}, make([]int, state.PreviewTargetLimit+1)} {
		if _, err := selectReviewPage(page, numbers); !errors.Is(err, errReviewSelection) {
			t.Fatal(numbers, err)
		}
	}
	for _, mutate := range []func(*reviewPage){
		func(p *reviewPage) { p.Selection.Targets = p.Selection.Targets[:1] },
		func(p *reviewPage) { p.Selection.Targets[0].FindingID = "different" },
		func(p *reviewPage) { p.Selection.Roots = p.Selection.Roots[1:] },
		func(p *reviewPage) { p.Selection.Roots = append(p.Selection.Roots, p.Selection.Roots[0]) },
		func(p *reviewPage) { p.Evidence.Findings = nil },
	} {
		broken := reviewSubsetFixture()
		mutate(&broken)
		if _, err := selectReviewPage(broken, []int{1}); !errors.Is(err, errReviewSelection) {
			t.Fatal("inconsistent display/bindings accepted", err)
		}
	}
}

func TestReviewEmptyPagePreservesContinuationAndDiagnostics(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	var out, stderr bytes.Buffer
	if code := Run(ctx, []string{"--data-dir", base, "init", "--root", "/offline-review-fixture"}, &out, &stderr); code != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
	s, err := state.OpenWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	const fileCount = state.FindingEntryLimit + 1
	for offset := 0; offset < fileCount; {
		j, err := s.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil || j == nil {
			t.Fatal(j, err)
		}
		batch := state.ScanBatch{Identity: "fixture", Generation: 1, Directory: state.Entry{Path: []byte("."), Kind: "directory"}}
		for len(batch.Entries) < state.MaxBatchEntries && offset < fileCount {
			batch.Entries = append(batch.Entries, state.Entry{Path: []byte(fmt.Sprintf("file-%04d", offset)), Kind: "file"})
			offset++
		}
		batch.Complete = offset == fileCount
		if err = s.CommitScan(ctx, *j, batch); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	page, err := loadReviewPage(ctx, base, "", 30)
	if err != nil || len(page.Evidence.Findings) != 0 || len(page.Selection.Targets) != 0 || page.NextCursor == "" || page.Evidence.NextCursor != page.NextCursor || page.Evidence.PageCoverage != "more_saved_entries" || page.Evidence.EntriesExamined != state.FindingEntryLimit || page.Evidence.Diagnostics[0].Count != state.FindingEntryLimit {
		t.Fatal(page, err)
	}
	next, err := loadReviewPage(ctx, base, page.NextCursor, 30)
	if err != nil || next.NextCursor != "" || next.Evidence.PageCoverage != "saved_entries_exhausted" || next.Evidence.EntriesExamined != 2 {
		t.Fatal(next, err)
	}
	if _, err = selectReviewPage(page, []int{1}); !errors.Is(err, errReviewSelection) {
		t.Fatal("empty page row accepted", err)
	}
}
