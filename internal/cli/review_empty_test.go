package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func newEmptyGuidedReviewFixture(t *testing.T) guidedReviewFixture {
	t.Helper()
	f := guidedReviewFixture{t: t, base: filepath.Join(t.TempDir(), "state's cache"), root: filepath.Join(t.TempDir(), "missing project's root")}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.state = manualState(paths, f.root)
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, f.state)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err = w.SyncRoots(ctx, []string{f.root}); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	entry := func(path, kind, inode string) state.Entry {
		return state.Entry{Path: []byte(path), Kind: kind, Device: "1", Inode: inode, MtimeNS: 1, CtimeNS: 2}
	}
	entries := []state.Entry{entry("node_modules", "directory", "2"), entry("package.json", "file", "3")}
	// This dependency folder is rejected because its sibling manifest is recent.
	// The remaining files force an empty first page with a real continuation.
	entries[1].MtimeNS = time.Now().Add(-time.Hour).UnixNano()
	for i := 0; i < state.FindingEntryLimit+5; i++ {
		entries = append(entries, entry(fmt.Sprintf("ordinary-%04d", i), "file", fmt.Sprint(i+4)))
	}
	offset := 0
	for {
		job, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			break
		}
		batch := state.ScanBatch{Identity: "fixture-root", Generation: 1, Complete: true, Directory: entry(string(job.Path), "directory", "1")}
		if string(job.Path) == "." {
			end := min(offset+state.MaxBatchEntries, len(entries))
			batch.Entries = entries[offset:end]
			offset = end
			batch.Complete = offset == len(entries)
		}
		if err = w.CommitScan(ctx, *job, batch); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func TestGuidedReviewEmptyPageExplainsRejectionsAndPreservesNavigationScope(t *testing.T) {
	f := newEmptyGuidedReviewFixture(t)
	ctx := context.Background()
	first, err := loadReviewPage(ctx, f.state, "", 30)
	if err != nil || len(first.Evidence.Findings) != 0 || first.NextCursor == "" || first.Evidence.EntriesExamined != state.FindingEntryLimit || first.Evidence.Diagnostics[0].Count != state.FindingEntryLimit-1 || first.Evidence.Diagnostics[8].Count != 1 {
		t.Fatal("fixture does not provide an empty continued rejection page", first, err)
	}
	second, err := loadReviewPage(ctx, f.state, first.NextCursor, 30)
	if err != nil || len(second.Evidence.Findings) != 0 || second.NextCursor != "" || second.Evidence.EntriesExamined != 8 {
		t.Fatal(second, err)
	}
	before, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	code, output, stderr := f.run(strings.NewReader("next\nrefresh\nquit\n"))
	if code != 0 || stderr != "" {
		t.Fatal(code, output, stderr)
	}
	pages := strings.Split(output, "REVIEW SAVED CANDIDATES - CLEANUP UNAVAILABLE")
	if len(pages) != 4 {
		t.Fatal("next/refresh did not each display a replacement page", output)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	command := "rydd --data-dir " + shellQuote(paths.StateDir) + " report --candidates --min-age-days 30 -d " + shellQuote(f.root)
	wantCommands := []string{command, command + " --cursor " + shellQuote(first.NextCursor), command}
	for i, page := range pages[1:] {
		plain := strings.Join(strings.Fields(page), " ")
		if !strings.Contains(page, "PAGE SUMMARY") || !strings.Contains(plain, "A later scan can change these results.") {
			t.Fatal("empty page omitted its diagnostics or freshness limit", page)
		}
		if i == 1 {
			if !strings.Contains(plain, "Saved entries checked 8") || !strings.Contains(plain, "Other entries 8") || strings.Contains(page, "Too recent / future-dated") {
				t.Fatal("next reused the first page's rejection counts", page)
			}
		} else if !strings.Contains(plain, "Saved entries checked 1,000") || !strings.Contains(plain, "Other entries 999") || !strings.Contains(plain, "Too recent / future-dated 1") {
			t.Fatal("first/refresh page omitted examined or rejected counts", page)
		}
		parts := strings.Split(page, "View this saved-entry page again:\n")
		if len(parts) != 2 {
			t.Fatal("empty page has no unambiguous details command", page)
		}
		got := strings.TrimSpace(strings.SplitN(parts[1], "\n", 2)[0])
		if got != wantCommands[i] {
			t.Fatalf("page %d details command changed scope, age, quoting or incoming cursor\ngot:  %s\nwant: %s", i+1, got, wantCommands[i])
		}
		if i != 1 && strings.Contains(got, "--cursor") {
			t.Fatal("first/refresh command used an outgoing cursor", got)
		}
	}
	if strings.Count(output, "View this saved-entry page again:") != 3 || !strings.Contains(output, "Review ended. Nothing saved. All folders stay unchanged.") || strings.Contains(output, "Type save") {
		t.Fatal("empty navigation reached a save flow or omitted cancellation", output)
	}
	f.assertNoPlan()
	after, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("empty-page review changed saved inventory", err)
	}
	if _, err = os.Lstat(f.root); !os.IsNotExist(err) {
		t.Fatal("saved-only review created the offline source directory", err)
	}
}

func TestGuidedReviewEmptySummaryLabelsAndNonemptyFrozenOmission(t *testing.T) {
	command := "rydd --data-dir '/fixture/state' report --candidates --min-age-days 30 -d '/fixture/project'"
	empty := reviewPage{Evidence: state.FindingReport{MinimumAgeDays: 30, EntriesExamined: 12, Diagnostics: []state.SelectionDiagnostic{
		{Code: "not_node_modules", Count: 1},
		{Code: "manifest_missing_or_unsupported", Count: 6},
		{Code: "age_not_met", Count: 5},
	}}}
	var out bytes.Buffer
	printReviewPage(&out, empty, 30, command)
	plain := strings.Join(strings.Fields(out.String()), " ")
	for _, want := range []string{"Saved entries checked 12", "Other entries 1", "No usable package.json 6", "Too recent / future-dated 5"} {
		if !strings.Contains(plain, want) {
			t.Fatal("shared candidate summary label or count missing", want, out.String())
		}
	}
	if !strings.Contains(out.String(), "\n  "+command+"\n") {
		t.Fatal("empty page details command was wrapped or changed", out.String())
	}
	f := newGuidedReviewFixture(t, 1, false)
	page, err := loadReviewPage(context.Background(), f.state, "", 30)
	if err != nil || len(page.Evidence.Findings) != 1 || page.Evidence.PageCoverage != "selected_entries_only" {
		t.Fatal(page, err)
	}
	out.Reset()
	printReviewPage(&out, page, 30, command)
	if strings.Contains(out.String(), "PAGE SUMMARY") || strings.Contains(out.String(), "Saved entries checked") || strings.Contains(out.String(), "View this saved-entry page again:") || !strings.Contains(out.String(), page.Evidence.Findings[0].ID) {
		t.Fatal("frozen numbered page reused discovery-only details", out.String())
	}
	f.assertNoPlan()
}
