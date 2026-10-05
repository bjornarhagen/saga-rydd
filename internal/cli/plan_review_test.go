package cli

import (
	"bytes"
	"context"
	"encoding/json"
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

type reviewCLI struct {
	t          *testing.T
	base, root string
	saved      plans.Saved
}

func (f reviewCLI) run(args ...string) (int, string) {
	f.t.Helper()
	var out, stderr bytes.Buffer
	code := Run(context.Background(), append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	if stderr.Len() != 0 {
		f.t.Fatal(stderr.String())
	}
	return code, out.String()
}

func reviewCLIFixture(t *testing.T, partial bool) reviewCLI {
	t.Helper()
	f := reviewCLI{t: t, base: filepath.Join(t.TempDir(), "state"), root: filepath.Join(t.TempDir(), "project")}
	modules := filepath.Join(f.root, "node_modules")
	if err := os.MkdirAll(modules, 0700); err != nil {
		t.Fatal(err)
	}
	if partial {
		if err := os.Mkdir(filepath.Join(modules, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(f.root, "package.json"), filepath.Join(modules, "keep.txt")} {
		if err := os.WriteFile(path, []byte("fixture data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, path := range []string{modules, filepath.Join(f.root, "package.json")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if code, raw := f.run("scan", "-d", f.root, "--compact", "--now", "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	s, err := state.OpenReader(context.Background(), manualState(paths, f.root))
	if err != nil {
		t.Fatal(err)
	}
	report, err := s.NodeModulesFindings(context.Background(), "", 90)
	s.Close()
	if err != nil || len(report.Findings) != 1 {
		t.Fatal(report, err)
	}
	code, raw := f.run("plan", "--save", "-d", f.root, report.Findings[0].ID, "--json")
	var result struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	f.saved = result.Plan
	return f
}

func (f reviewCLI) approveArgs() []string {
	return []string{"plan", "--approve", f.saved.ID, "-d", f.root, "--confirm-project-review", "--confirm-quarantine", "--json"}
}

func TestPlanReviewCLIConfirmationsAndOfflineRevocation(t *testing.T) {
	f := reviewCLIFixture(t, false)
	id := f.saved.ID
	for _, args := range [][]string{
		{"plan", "--approve", id, "-d", f.root, "--json"},
		{"plan", "--approve", id, "--confirm-project-review", "--json"},
		{"plan", "--approve", id, "--confirm-project-review=false", "--confirm-quarantine", "--json"},
		{"plan", "--approve", id, "--confirm-project-review", "--confirm-quarantine", "--min-age-days", "90", "--json"},
		{"plan", "--approve", id, "--confirm-project-review", "--confirm-quarantine", "extra", "--json"},
		{"plan", "--approve", id, "--revoke", id, "--json"},
		{"plan", "--revoke", id, "-d", f.root, "--json"},
		{"plan", "--show", id, "--confirm-project-review=false", "--json"},
		{"plan", "--approve", "--json", "--json"},
		{"plan", "--revoke", "../escape", "--json"},
	} {
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	ctx := context.Background()
	shown, err := plans.Show(ctx, f.base, id)
	if err != nil || shown.Review != nil {
		t.Fatal(shown, err)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	dir := manualState(paths, f.root)
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	code, raw := f.run(f.approveArgs()...)
	w.Close()
	var approved struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(raw), &approved); err != nil || code != 0 || approved.Plan.Review == nil {
		t.Fatal(code, raw, err)
	}
	r := approved.Plan.Review
	if r.Status != "review_approved" || r.Executable || r.CurrentStateVerified || r.Approval.Contract != plans.ReviewContract || !reflect.DeepEqual(approved.Plan.Record, f.saved.Record) {
		t.Fatal(raw)
	}
	code, raw = f.run(f.approveArgs()...)
	var retry struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(raw), &retry); err != nil || code != 0 || !reflect.DeepEqual(retry.Plan.Review, r) {
		t.Fatal(code, raw, err)
	}
	code, human := f.run("plan", "--show", id)
	for _, want := range []string{"REVIEW CONSENT RECORDED - CLEANUP UNAVAILABLE", "Expires", "Review consent is not execution authorization.", "plan --revoke " + id} {
		if code != 0 || !strings.Contains(strings.Join(strings.Fields(human), " "), want) {
			t.Fatal(code, human)
		}
	}
	if err = os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(dir, dir+".offline"); err != nil {
		t.Fatal(err)
	}
	code, raw = f.run("plan", "--revoke", id, "--json")
	var revoked struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(raw), &revoked); err != nil || code != 0 || revoked.Plan.Review.Status != "revoked" || revoked.Plan.Review.ID != r.ID {
		t.Fatal(code, raw, err)
	}
	code, raw = f.run("plan", "--revoke", id, "--json")
	if err = json.Unmarshal([]byte(raw), &retry); err != nil || code != 0 || !reflect.DeepEqual(retry.Plan.Review, revoked.Plan.Review) {
		t.Fatal(code, raw, err)
	}
	if err = os.Rename(dir+".offline", dir); err != nil {
		t.Fatal(err)
	}
	if code, raw = f.run(f.approveArgs()...); code != 1 || !strings.Contains(raw, "review_unavailable") {
		t.Fatal(code, raw)
	}
	if content, err := os.ReadFile(filepath.Join(f.root+".offline", "node_modules", "keep.txt")); err != nil || string(content) != "fixture data" {
		t.Fatal(err)
	}
}

func TestPlanReviewCLIRejectsChangedUnknownAndDifferentInventory(t *testing.T) {
	for _, kind := range []string{"partial", "changed", "different", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f := reviewCLIFixture(t, kind == "partial")
			if kind == "changed" {
				if code, raw := f.run("scan", "-d", f.root, "--now", "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			if kind == "different" {
				paths, err := config.ResolvePaths(f.base)
				if err != nil {
					t.Fatal(err)
				}
				dir := manualState(paths, f.root)
				if err = os.Rename(dir, dir+".old"); err != nil {
					t.Fatal(err)
				}
				if code, raw := f.run("scan", "-d", f.root, "--now", "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			if kind == "canceled" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				var out, stderr bytes.Buffer
				code := Run(ctx, append([]string{"--data-dir", f.base}, f.approveArgs()...), &out, &stderr)
				if code != 1 || !strings.Contains(out.String(), "canceled") {
					t.Fatal(code, out.String(), stderr.String())
				}
			} else if code, raw := f.run(f.approveArgs()...); code != 1 || !strings.Contains(raw, "review_evidence_changed") {
				t.Fatal(code, raw)
			}
			shown, err := plans.Show(context.Background(), f.base, f.saved.ID)
			if err != nil || shown.Review != nil {
				t.Fatal(shown, err)
			}
		})
	}
}
