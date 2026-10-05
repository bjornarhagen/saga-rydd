package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestSavedPlanCheckCLI(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	root := filepath.Join(t.TempDir(), "project")
	modules := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(modules, 0700); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(root, "package.json"), filepath.Join(modules, "keep.txt")} {
		if err := os.WriteFile(p, []byte("original"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, p := range []string{modules, filepath.Join(root, "package.json")} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) (int, string) {
		t.Helper()
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &stderr)
		if stderr.Len() != 0 {
			t.Fatal(stderr.String())
		}
		return code, out.String()
	}
	if code, raw := run("scan", "-d", root, "--now", "--compact", "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	paths, err := config.ResolvePaths(base)
	if err != nil {
		t.Fatal(err)
	}
	dir := manualState(paths, root)
	s, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := s.NodeModulesFindings(ctx, "", 90)
	s.Close()
	if err != nil || len(findings.Findings) != 1 {
		t.Fatal(findings, err)
	}
	code, raw := run("plan", "--save", "-d", root, findings.Findings[0].ID, "--json")
	var saved struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(raw), &saved); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	id := saved.Plan.ID
	check := func(want string) {
		t.Helper()
		code, raw := run("plan", "--check", id, "-d", root, "--json")
		var result struct {
			OK   bool
			Plan CheckedPlan
		}
		if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || !result.OK || result.Plan.ID != id || result.Plan.Check.Status != want || result.Plan.Check.Executable || result.Plan.Check.ApprovalAvailable || result.Plan.Check.CurrentStateVerified {
			t.Fatal(code, raw, err)
		}
	}
	check("matches_saved_inventory")
	// Readers can compare while the inventory writer is held.
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	check("matches_saved_inventory")
	w.Close()
	for _, args := range [][]string{
		{"plan", "--check", id, "--save", "--json"},
		{"plan", "--check", id, "--show", id, "--json"},
		{"plan", "--check", id, "--min-age-days", "90", "--json"},
		{"plan", "--check", id, findings.Findings[0].ID, "--json"},
		{"plan", "--check", id, "-d", root, "--directory", root, "--json"},
		{"plan", "--check", "", "--json"},
		{"plan", "--check", "../escape", "--json"},
		{"plan", "--check", "--json", "--json"},
	} {
		if code, raw := run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	// Offline roots still match: this command explicitly does not verify live files.
	if err = os.Rename(root, root+".offline"); err != nil {
		t.Fatal(err)
	}
	hash := func(path string) [32]byte {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return sha256.Sum256(b)
	}
	db := filepath.Join(dir, state.Filename)
	planDB := filepath.Join(paths.StateDir, "plans", "plans.sqlite3")
	before, plansBefore := hash(db), hash(planDB)
	check("matches_saved_inventory")
	code, human := run("plan", "--check", id, "-d", root)
	for _, want := range []string{"SAVED OBSERVATIONS MATCH - NO EXECUTION AUTHORIZATION", "A match does not establish safe cleanup.", "plan --show " + id} {
		if code != 0 || !strings.Contains(strings.Join(strings.Fields(human), " "), want) {
			t.Fatal(code, human)
		}
	}
	if before != hash(db) || plansBefore != hash(planDB) {
		t.Fatal("check changed database bytes")
	}
	// Missing state is not created. Reopening the plan still works independently.
	if code, raw = run("plan", "--check", id, "-d", root+".missing", "--json"); code != 1 || !strings.Contains(raw, "not_found") {
		t.Fatal(code, raw)
	}
	if _, err = os.Stat(manualState(paths, root+".missing")); !os.IsNotExist(err) {
		t.Fatal("check created missing inventory", err)
	}
	if err = os.Rename(dir, dir+".old"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(root+".offline", root); err != nil {
		t.Fatal(err)
	}
	if code, raw = run("scan", "-d", root, "--now", "--compact", "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	check("changed")
	code, human = run("plan", "--check", id, "-d", root)
	if code != 0 || !strings.Contains(human, "SAVED EVIDENCE CHANGED - REVIEW AGAIN") {
		t.Fatal(code, human)
	}
	if contents, err := os.ReadFile(filepath.Join(root, "node_modules", "keep.txt")); err != nil || string(contents) != "original" {
		t.Fatal("source changed", err)
	}
	code, reopened := run("plan", "--show", id, "--json")
	var loaded struct{ Plan plans.Saved }
	if err = json.Unmarshal([]byte(reopened), &loaded); err != nil || code != 0 || loaded.Plan.ID != id || loaded.Plan.Record.Status != "unapproved" || plansBefore != hash(planDB) {
		t.Fatal(code, reopened, err)
	}
}
