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

func TestSavedPlanCLISelectionAndOfflineReopen(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	root := filepath.Join(t.TempDir(), "project")
	modules := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(filepath.Join(modules, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(modules, "keep.txt")
	if err := os.WriteFile(file, []byte("original data"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, path := range []string{modules, filepath.Join(root, "package.json")} {
		if err := os.Chtimes(path, old, old); err != nil {
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
	// An active inventory writer must not block saving an existing snapshot.
	w, err := state.OpenWriter(ctx, manualState(paths, root))
	if err != nil {
		t.Fatal(err)
	}
	findings, err := w.NodeModulesFindings(ctx, "", 90)
	if err != nil || len(findings.Findings) != 1 {
		t.Fatal(findings, err)
	}
	id := findings.Findings[0].ID
	code, raw := run("plan", "--save", "-d", root, id, "--json")
	var saved struct {
		OK   bool        `json:"ok"`
		Plan plans.Saved `json:"plan"`
	}
	if err = json.Unmarshal([]byte(raw), &saved); err != nil || code != 0 || !saved.OK {
		t.Fatal(code, raw, err)
	}
	if !plans.ValidID(saved.Plan.ID) || saved.Plan.Record.Status != "unapproved" || saved.Plan.Record.Executable || saved.Plan.Record.ApprovalAvailable || len(saved.Plan.Record.Selection.Evidence.Findings) != 1 || saved.Plan.Record.Selection.Evidence.Findings[0].Measurement.Status != "partial" {
		t.Fatal(raw)
	}
	w.Close()
	if code, raw := run("plan", "--check", saved.Plan.ID, "-d", root, "--json"); code != 0 || !strings.Contains(raw, `"status":"unverifiable"`) || !strings.Contains(raw, `"code":"target_evidence_unknown"`) {
		t.Fatal(code, raw)
	}
	if code, human := run("plan", "--check", saved.Plan.ID, "-d", root); code != 0 || !strings.Contains(human, "SAVED EVIDENCE INCOMPLETE - REVIEW REQUIRED") {
		t.Fatal(code, human)
	}
	for _, args := range [][]string{
		{"plan", "--save", "--preview", "-d", root, id, "--json"},
		{"plan", "--save=false", "-d", root, id, "--json"},
		{"plan", "--save", "-d", root, id, id, "--json"},
		{"plan", "--save", "-d", root, id, "node-modules-v1:1:99999", "--json"},
		{"plan", "--show", saved.Plan.ID, "-d", root, "--json"},
		{"plan", "--show", saved.Plan.ID, "--min-age-days", "90", "--json"},
		{"plan", "--show", "../escape", "--json"},
		{"plan", "--show=", "--json"},
		{"plan", "--show", saved.Plan.ID, id, "--json"},
	} {
		if code, raw := run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	// Recreate inventory with reusable row IDs. The old plan must stay frozen.
	dir := manualState(paths, root)
	if err = os.Rename(dir, dir+".old"); err != nil {
		t.Fatal(err)
	}
	if code, raw = run("scan", "-d", root, "--now", "--compact", "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	r, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := r.SnapshotSelection(ctx, []string{id}, 90)
	r.Close()
	if err != nil || fresh.InventoryID == saved.Plan.Record.Selection.InventoryID {
		t.Fatal(fresh, err)
	}
	if err = os.Rename(root, root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(dir, dir+".offline"); err != nil {
		t.Fatal(err)
	}
	code, raw = run("plan", "--show", saved.Plan.ID, "--json")
	var reopened struct {
		Plan plans.Saved `json:"plan"`
	}
	if err = json.Unmarshal([]byte(raw), &reopened); err != nil || code != 0 || !reflect.DeepEqual(reopened.Plan, saved.Plan) {
		t.Fatal(code, raw, err)
	}
	code, human := run("plan", "--show", saved.Plan.ID)
	for _, want := range []string{"SAVED FOR REVIEW - NOT APPROVED", saved.Plan.ID, "Incomplete measurement", "Unconfirmed", "Review consent can be recorded", "plan --show " + saved.Plan.ID} {
		if code != 0 || !strings.Contains(strings.Join(strings.Fields(human), " "), want) {
			t.Fatal(code, human)
		}
	}
	contents, err := os.ReadFile(filepath.Join(root+".offline", "node_modules", "keep.txt"))
	if err != nil || string(contents) != "original data" {
		t.Fatal("source data changed", err)
	}
	if code, raw = run("plan", "--show", "plan-v1-"+strings.Repeat("0", 64), "--json"); code != 1 || !strings.Contains(raw, "not_found") {
		t.Fatal(code, raw)
	}
}
