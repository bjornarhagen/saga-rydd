package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type guidedReviewFixture struct {
	t                 *testing.T
	base, root, state string
	findings          []state.Finding
}

func newGuidedReviewFixture(t *testing.T, count int, partial bool) guidedReviewFixture {
	t.Helper()
	f := guidedReviewFixture{t: t, base: filepath.Join(t.TempDir(), "state"), root: filepath.Join(t.TempDir(), "projects")}
	for i := 0; i < count; i++ {
		project := filepath.Join(f.root, fmt.Sprintf("project%02d", i))
		if err := os.MkdirAll(filepath.Join(project, "node_modules"), 0700); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string]string{"package.json": "original manifest", "node_modules/keep.txt": "original dependency"} {
			if err := os.WriteFile(filepath.Join(project, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.state = manualState(paths, f.root)
	f.seed(count, partial)
	s, err := state.OpenReader(context.Background(), f.state)
	if err != nil {
		t.Fatal(err)
	}
	report, err := s.NodeModulesFindings(context.Background(), "", 30)
	s.Close()
	if err != nil || len(report.Findings) != min(count, state.PreviewTargetLimit) {
		t.Fatal(report, err)
	}
	f.findings = report.Findings
	return f
}

func (f guidedReviewFixture) seed(count int, partial bool) {
	f.t.Helper()
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, f.state)
	if err != nil {
		f.t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, []string{f.root}); err != nil {
		f.t.Fatal(err)
	}
	if err := w.SeedInventory(ctx); err != nil {
		f.t.Fatal(err)
	}
	entry := func(path, kind string) state.Entry {
		return state.Entry{Path: []byte(path), Kind: kind, Device: "fixture-device", Inode: path, Size: 12, Allocated: 4096, MtimeNS: 1, CtimeNS: 2}
	}
	committedProjects := 0
	for {
		job, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			f.t.Fatal(err)
		}
		if job == nil {
			break
		}
		path := string(job.Path)
		var entries []state.Entry
		switch {
		case path == ".":
			for i := 0; i < count; i++ {
				entries = append(entries, entry(fmt.Sprintf("project%02d", i), "directory"))
			}
		case filepath.Base(path) == "node_modules":
			entries = []state.Entry{entry(filepath.Join(path, "keep.txt"), "file")}
		default:
			entries = []state.Entry{entry(filepath.Join(path, "node_modules"), "directory"), entry(filepath.Join(path, "package.json"), "file")}
			committedProjects++
		}
		if err := w.CommitScan(ctx, *job, state.ScanBatch{Identity: "fixture-root", Generation: 1, Complete: true, Directory: entry(path, "directory"), Entries: entries}); err != nil {
			f.t.Fatal(err)
		}
		// Leave target listings unfinished while retaining confirmed parent
		// membership. The guided review may preserve this historical evidence.
		if partial && committedProjects == count {
			break
		}
	}
}

func (f guidedReviewFixture) run(input io.Reader, options ...string) (int, string, string) {
	f.t.Helper()
	args := []string{"--data-dir", f.base, "review", "-d", f.root, "--min-age-days", "30"}
	args = append(args, options...)
	var out, errOut bytes.Buffer
	code := runWithInput(context.Background(), args, input, &out, &errOut)
	return code, out.String(), errOut.String()
}

func (f guidedReviewFixture) assertNoPlan() {
	f.t.Helper()
	if _, err := os.Stat(filepath.Join(f.base, "plans")); !os.IsNotExist(err) {
		f.t.Fatal("review initialized plan storage without explicit save", err)
	}
}

func (f guidedReviewFixture) saved(output string) plans.Saved {
	f.t.Helper()
	ids := regexp.MustCompile(`plan-v1-[0-9a-f]{64}`).FindAllString(output, -1)
	if len(ids) == 0 {
		f.t.Fatal("no saved plan ID returned", output)
	}
	for _, id := range ids {
		if id != ids[0] {
			f.t.Fatal("one review saved more than one plan", output)
		}
	}
	saved, err := plans.Show(context.Background(), f.base, ids[0])
	if err != nil {
		f.t.Fatal(err)
	}
	return saved
}

func TestGuidedReviewExactSubsetUnapprovedAndReadOnlySource(t *testing.T) {
	for _, numbers := range []string{"1 3", "1,3"} {
		t.Run(numbers, func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			inventory, err := os.ReadFile(filepath.Join(f.state, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			code, output, stderr := f.run(strings.NewReader(numbers + "\nsave\n"))
			if code != 0 || stderr != "" || !strings.Contains(output, "CLEANUP UNAVAILABLE") || !strings.Contains(output, "Type save") {
				t.Fatal(code, output, stderr)
			}
			saved := f.saved(output)
			if saved.Record.Status != "unapproved" || saved.Record.Executable || saved.Record.ApprovalAvailable || saved.Review != nil || saved.Record.Selection.Evidence.MinimumAgeDays != 30 || len(saved.Record.Selection.Evidence.Findings) != 2 {
				t.Fatal(saved)
			}
			selected := map[string]bool{}
			for _, finding := range saved.Record.Selection.Evidence.Findings {
				selected[finding.ID] = true
			}
			if !selected[f.findings[0].ID] || selected[f.findings[1].ID] || !selected[f.findings[2].ID] {
				t.Fatal("saved selection differs from displayed numbers", selected, f.findings)
			}
			after, err := os.ReadFile(filepath.Join(f.state, state.Filename))
			if err != nil || sha256.Sum256(inventory) != sha256.Sum256(after) {
				t.Fatal("guided review changed inventory", err)
			}
			for i := 0; i < 3; i++ {
				for name, want := range map[string]string{"package.json": "original manifest", "node_modules/keep.txt": "original dependency"} {
					data, err := os.ReadFile(filepath.Join(f.root, fmt.Sprintf("project%02d", i), name))
					if err != nil || string(data) != want {
						t.Fatal("source data changed", name, err, string(data))
					}
				}
			}
		})
	}
}

func TestGuidedReviewRefusesAmbiguousOrInvalidNumbers(t *testing.T) {
	for _, numbers := range []string{"1 1", "1,1", "0", "4", "-1", "1-2", "1.0", "all", "99999999999999999999999999"} {
		t.Run(numbers, func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			code, output, stderr := f.run(strings.NewReader(numbers + "\nquit\n"))
			if code != 0 || stderr != "" || !strings.Contains(output, "CLEANUP UNAVAILABLE") || strings.Contains(output, "Type save") {
				t.Fatal(code, output, stderr)
			}
			f.assertNoPlan()
		})
	}
}

func TestGuidedReviewEOFBackQuitAndLiteralSave(t *testing.T) {
	for _, input := range []string{"", "quit\n", "1\n", "1\nsave", "1\nquit\n", "1\nback\nquit\n", "1\nback\n", "1\nyes\nquit\n", "1\nSave\nquit\n"} {
		t.Run(fmt.Sprintf("%q", input), func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			code, output, stderr := f.run(strings.NewReader(input))
			if code != 0 || stderr != "" {
				t.Fatal(code, output, stderr)
			}
			f.assertNoPlan()
		})
	}
	t.Run("back_replaces_selection", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 3, false)
		code, output, stderr := f.run(strings.NewReader("1\nback\n3\nsave\n"))
		if code != 0 || stderr != "" {
			t.Fatal(code, output, stderr)
		}
		saved := f.saved(output)
		if len(saved.Record.Selection.Evidence.Findings) != 1 || saved.Record.Selection.Evidence.Findings[0].ID != f.findings[2].ID {
			t.Fatal("back retained or remapped an earlier choice", saved)
		}
	})
}

type guidedReviewLineReader struct {
	lines  []string
	index  int
	before func(int)
}

func (r *guidedReviewLineReader) Read(p []byte) (int, error) {
	if r.index >= len(r.lines) {
		return 0, io.EOF
	}
	if r.before != nil {
		r.before(r.index)
	}
	line := r.lines[r.index]
	if len(p) < len(line) {
		return 0, io.ErrShortBuffer
	}
	r.index++
	return copy(p, line), nil
}

func TestGuidedReviewChangedInventoryCannotRemapSelection(t *testing.T) {
	for _, kind := range []string{"revision_changed", "inventory_rebuilt"} {
		t.Run(kind, func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			input := &guidedReviewLineReader{lines: []string{"1\n", "save\n"}, before: func(index int) {
				if index != 1 {
					return
				}
				if kind == "inventory_rebuilt" {
					if err := os.Rename(f.state, f.state+".old"); err != nil {
						t.Fatal(err)
					}
					f.seed(3, false)
					return
				}
				// A fresh pass replaces observed root revisions while preserving
				// the IDs and paths that were already displayed.
				f.seed(3, false)
			}}
			code, output, stderr := f.run(input)
			if code != 1 {
				t.Fatal("changed displayed evidence was saved or remapped", code, output, stderr)
			}
			f.assertNoPlan()
		})
	}
}

func TestGuidedReviewPartialHistoricalEvidenceAndPageReplacement(t *testing.T) {
	t.Run("partial_historical_evidence", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 3, true)
		code, output, stderr := f.run(strings.NewReader("1\nsave\n"))
		if code != 0 || stderr != "" {
			t.Fatal(code, output, stderr)
		}
		saved := f.saved(output)
		if saved.Record.Selection.Evidence.Findings[0].Measurement.Status != "partial" || saved.Record.Executable || saved.Review != nil {
			t.Fatal(saved)
		}
	})
	t.Run("next_replaces_numbered_page", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 21, false)
		code, output, stderr := f.run(strings.NewReader("next\n1\nsave\n"))
		if code != 0 || stderr != "" {
			t.Fatal(code, output, stderr)
		}
		saved := f.saved(output)
		if len(saved.Record.Selection.Evidence.Findings) != 1 {
			t.Fatal(saved)
		}
		for _, firstPage := range f.findings {
			if saved.Record.Selection.Evidence.Findings[0].ID == firstPage.ID {
				t.Fatal("next reused a number from the old page", saved)
			}
		}
	})
	t.Run("refresh_replaces_page", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 3, false)
		input := &guidedReviewLineReader{lines: []string{"refresh\n", "1\n", "save\n"}, before: func(index int) {
			if index == 0 {
				if err := os.Rename(f.state, f.state+".old"); err != nil {
					t.Fatal(err)
				}
				f.seed(3, false)
			}
		}}
		code, output, stderr := f.run(input)
		if code != 0 || stderr != "" {
			t.Fatal(code, output, stderr)
		}
		saved := f.saved(output)
		reader, err := state.OpenReader(context.Background(), f.state)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		current, err := reader.SnapshotSelection(context.Background(), []string{saved.Record.Selection.Evidence.Findings[0].ID}, 30)
		if err != nil || saved.Record.Selection.InventoryID != current.InventoryID {
			t.Fatal("refresh saved the old inventory incarnation", saved, current, err)
		}
	})
}

func TestGuidedReviewJSONRejectedBeforeInputOrState(t *testing.T) {
	base := filepath.Join(t.TempDir(), "not-created")
	input := &guidedReviewLineReader{lines: []string{"1\n", "save\n"}, before: func(int) { t.Fatal("JSON review read input") }}
	var out, errOut bytes.Buffer
	code := runWithInput(context.Background(), []string{"--data-dir", base, "review", "-d", "/nonexistent-fixture", "--json"}, input, &out, &errOut)
	var result struct {
		OK    bool `json:"ok"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || code != 2 || result.OK || result.Error.Code != "unsupported_output" || errOut.Len() != 0 {
		t.Fatal(code, out.String(), errOut.String(), err)
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatal("JSON review initialized state", err)
	}
}

type guidedReviewPromptWriter struct {
	bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (w *guidedReviewPromptWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if strings.Contains(string(p), "Choose numbers") {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}

func TestGuidedReviewWaitingPipeInputCancels(t *testing.T) {
	for _, partial := range []string{"", "1"} {
		t.Run(fmt.Sprintf("partial_line_%q", partial), func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			readEnd, writeEnd, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer readEnd.Close()
			defer writeEnd.Close()
			if _, err := writeEnd.Write([]byte(partial)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &guidedReviewPromptWriter{ready: make(chan struct{})}
			var errOut bytes.Buffer
			done := make(chan int, 1)
			go func() {
				done <- runWithInput(ctx, []string{"--data-dir", f.base, "review", "-d", f.root, "--min-age-days", "30"}, readEnd, out, &errOut)
			}()
			select {
			case <-out.ready:
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("review did not reach its input prompt")
			}
			// Keep the pipe writer open while review waits for a complete line.
			time.Sleep(150 * time.Millisecond)
			cancel()
			select {
			case code := <-done:
				if code != 1 {
					t.Fatal(code, out.String(), errOut.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not release blocked review input")
			}
			if _, err := readEnd.Stat(); err != nil {
				t.Fatal("review closed the caller's input descriptor", err)
			}
			f.assertNoPlan()
		})
	}
}

func TestGuidedReviewInputLimitSavesNothing(t *testing.T) {
	for _, prefix := range []string{"", "1\n"} {
		t.Run(fmt.Sprintf("prefix_%q", prefix), func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			code, output, stderr := f.run(strings.NewReader(prefix + strings.Repeat("1", 4097) + "\nsave\n"))
			if code != 1 {
				t.Fatal("oversized input was accepted", code, output, stderr)
			}
			f.assertNoPlan()
		})
	}
}

type guidedReviewFailWriter struct {
	match  string
	failed bool
}

func (w *guidedReviewFailWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.match) {
		w.failed = true
		return 0, errors.New("fixture output failed")
	}
	return len(p), nil
}

func TestGuidedReviewFailedOutputCannotConfirmSelection(t *testing.T) {
	for _, tc := range []struct {
		name, match string
		reads       int
	}{
		{"candidate_page", "REVIEW SAVED CANDIDATES", 0},
		{"selection_summary", "YOUR SELECTION", 1},
		{"save_prompt", "Type save", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGuidedReviewFixture(t, 3, false)
			input := &guidedReviewLineReader{lines: []string{"1\n", "save\n"}, before: func(index int) {
				if index >= tc.reads {
					t.Fatal("review read input after output failed")
				}
			}}
			out := &guidedReviewFailWriter{match: tc.match}
			var errOut bytes.Buffer
			code := runWithInput(context.Background(), []string{"--data-dir", f.base, "review", "-d", f.root, "--min-age-days", "30"}, input, out, &errOut)
			if code != 1 || !out.failed || input.index != tc.reads {
				t.Fatal("output failure did not stop review", code, out.failed, input.index, errOut.String())
			}
			f.assertNoPlan()
		})
	}
}

func TestGuidedReviewCancellationOutputFailure(t *testing.T) {
	for _, input := range []string{"", "quit\n", "1\nquit\n"} {
		f := newGuidedReviewFixture(t, 1, false)
		out := &guidedReviewFailWriter{match: "Review ended"}
		var errOut bytes.Buffer
		code := runWithInput(context.Background(), []string{"--data-dir", f.base, "review", "-d", f.root, "--min-age-days", "30"}, strings.NewReader(input), out, &errOut)
		if code != 1 || !out.failed {
			t.Fatal("cancellation output failure returned success", input, code, errOut.String())
		}
		f.assertNoPlan()
	}
}
