package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

var inspectManifest = []byte(`{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}}`)
var inspectLock = []byte(`{"name":"fixture","version":"1.0.0","lockfileVersion":3,"requires":true,"packages":{"":{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}},"node_modules/fixture-dep":{"version":"1.0.0","resolved":"https://registry.npmjs.org/fixture-dep/-/fixture-dep-1.0.0.tgz","integrity":"sha512-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="}}}`)

func inspectCLIFixture(t *testing.T, manifest, lock []byte, setup ...func(string)) reviewCLI {
	t.Helper()
	f := reviewCLI{t: t, base: filepath.Join(t.TempDir(), "state"), root: filepath.Join(t.TempDir(), "project")}
	modules := filepath.Join(f.root, "node_modules")
	if err := os.MkdirAll(modules, 0700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.root = canonical
	for name, contents := range map[string][]byte{
		"package.json": manifest, "package-lock.json": lock, "node_modules/keep.txt": []byte("fixture dependency contents"),
	} {
		if err := os.WriteFile(filepath.Join(f.root, name), contents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, prepare := range setup {
		prepare(f.root)
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"node_modules", "package.json"} {
		if err := os.Chtimes(filepath.Join(f.root, name), old, old); err != nil {
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
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	f.saved = result.Plan
	return f
}

func inspectCLIResult(t *testing.T, f reviewCLI) InspectedPlan {
	t.Helper()
	code, raw := f.run("plan", "--inspect", f.saved.ID, "-d", f.root, "--json")
	var result struct{ Plan InspectedPlan }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	return result.Plan
}

func TestPlanInspectCLIExactInputsAndNoWrites(t *testing.T) {
	f := inspectCLIFixture(t, inspectManifest, inspectLock)
	hash := func(path string) [32]byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return sha256.Sum256(data)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	planDB := filepath.Join(f.base, "plans", "plans.sqlite3")
	inventoryDB := filepath.Join(manualState(paths, f.root), "state.sqlite3")
	planBefore, inventoryBefore := hash(planDB), hash(inventoryDB)
	result := inspectCLIResult(t, f)
	r := result.Inspection
	if result.ID != f.saved.ID || result.InventoryCheck.Status != "matches_saved_inventory" || r == nil || r.Status != "inputs_observed" || r.Source != "live_project_inputs" || r.CheckedAt.IsZero() || r.CurrentStateVerified || r.Executable || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) != 1 {
		t.Fatalf("unexpected project input check: %+v", result)
	}
	target := r.Targets[0]
	if target.FindingID != f.saved.Record.Selection.Evidence.Findings[0].ID || target.Status != "inputs_observed" || target.Inputs == nil || target.Inputs.LockfileVersion != 3 || target.Inputs.LockedPackages != 1 || len(target.Inputs.Files) != 2 {
		t.Fatalf("unexpected input evidence: %+v", target)
	}
	observed := map[string]bool{}
	for _, input := range target.Inputs.Files {
		data := map[string][]byte{"package.json": inspectManifest, "package-lock.json": inspectLock}[input.Name]
		digest := sha256.Sum256(data)
		if data == nil || observed[input.Name] || input.Bytes != len(data) || input.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("input does not describe exact fixture bytes: %+v", input)
		}
		observed[input.Name] = true
	}
	code, human := f.run("plan", "--inspect", f.saved.ID, "-d", f.root)
	if code != 0 || !strings.Contains(human, "INPUTS OBSERVED - REVIEW REQUIRED") || !strings.Contains(human, "package-lock.json") || !strings.Contains(strings.Join(strings.Fields(human), " "), "Local dependency edits and successful reinstall remain unknown") {
		t.Fatal(code, human)
	}
	shown, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil || shown.Review != nil || !reflect.DeepEqual(shown.Record, f.saved.Record) {
		t.Fatal(shown, err)
	}
	if hash(planDB) != planBefore || hash(inventoryDB) != inventoryBefore {
		t.Fatal("input inspection changed database records")
	}
	for name, want := range map[string][]byte{"package.json": inspectManifest, "package-lock.json": inspectLock, "node_modules/keep.txt": []byte("fixture dependency contents")} {
		got, err := os.ReadFile(filepath.Join(f.root, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal(name, err, string(got))
		}
	}
}

func TestPlanInspectCLIArgumentsAndCapabilities(t *testing.T) {
	f := inspectCLIFixture(t, inspectManifest, inspectLock)
	for _, args := range [][]string{
		{"plan", "--inspect", f.saved.ID, "--verify", f.saved.ID, "--json"},
		{"plan", "--inspect", f.saved.ID, "--check", f.saved.ID, "--json"},
		{"plan", "--inspect", f.saved.ID, "--min-age-days", "90", "--json"},
		{"plan", "--inspect", f.saved.ID, "extra", "--json"},
		{"plan", "--inspect", f.saved.ID, "--confirm-project-review", "--json"},
		{"plan", "--inspect", f.saved.ID, "--confirm-quarantine", "--json"},
		{"plan", "--inspect", f.saved.ID, "-d", f.root, "--directory", f.root, "--json"},
		{"plan", "--inspect", "invalid", "--json"},
		{"plan", "--inspect=", "--json"},
	} {
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(code, raw)
		}
	}
	code, raw := f.run("capabilities", "--json")
	var capability struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capability); err != nil || code != 0 || !capability.Features["plan_input_inspection"] || capability.Features["cleanup"] {
		t.Fatal(code, raw, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	code = Run(ctx, []string{"--data-dir", f.base, "plan", "--inspect", f.saved.ID, "-d", f.root, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "canceled") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
}

func TestPlanInspectCLISavedEvidenceAndScope(t *testing.T) {
	t.Run("partial_saved_evidence", func(t *testing.T) {
		f := reviewCLIFixture(t, true)
		result := inspectCLIResult(t, f)
		if result.Inspection != nil || result.InventoryCheck.Status != "unverifiable" {
			t.Fatalf("live inspection ran with incomplete saved evidence: %+v", result)
		}
		code, human := f.run("plan", "--inspect", f.saved.ID, "-d", f.root)
		if code != 0 || !strings.Contains(human, "SAVED EVIDENCE BLOCKS PROJECT INPUT CHECK") {
			t.Fatal(code, human)
		}
	})
	for _, kind := range []string{"exclude_target", "exclude_child", "exclude_lock", "offline_source", "unscanned_manifest_change"} {
		t.Run(kind, func(t *testing.T) {
			f := inspectCLIFixture(t, inspectManifest, inspectLock)
			switch kind {
			case "exclude_target", "exclude_child", "exclude_lock":
				excluded := filepath.Join(f.root, "node_modules")
				if kind == "exclude_child" {
					excluded = filepath.Join(excluded, "keep.txt")
				}
				if kind == "exclude_lock" {
					excluded = filepath.Join(f.root, "package-lock.json")
				}
				if code, raw := f.run("init", "--root", f.root, "--exclude", excluded, "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			case "offline_source":
				if err := os.Rename(f.root, f.root+".offline"); err != nil {
					t.Fatal(err)
				}
			case "unscanned_manifest_change":
				if err := os.WriteFile(filepath.Join(f.root, "package.json"), []byte(`{"name":"changed"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			result := inspectCLIResult(t, f)
			if result.InventoryCheck.Status != "matches_saved_inventory" || result.Inspection == nil || result.Inspection.Status != "blocked" {
				t.Fatalf("live failure was not blocked: %+v", result)
			}
			code, human := f.run("plan", "--inspect", f.saved.ID, "-d", f.root)
			if code != 0 || !strings.Contains(human, "PROJECT INPUT CHECK BLOCKED - REVIEW REQUIRED") {
				t.Fatal(code, human)
			}
		})
	}
}

func TestPlanInspectCLINarrowInputsAndReviewIndependence(t *testing.T) {
	for _, kind := range []string{"malformed_manifest", "unsupported_lock", "local_dependency_edit", "revoked_review"} {
		t.Run(kind, func(t *testing.T) {
			manifest, lock := inspectManifest, inspectLock
			want := "inputs_observed"
			if kind == "malformed_manifest" {
				manifest, want = []byte("{broken"), "blocked"
			}
			if kind == "unsupported_lock" {
				lock, want = []byte(`{"lockfileVersion":1,"dependencies":{}}`), "blocked"
			}
			f := inspectCLIFixture(t, manifest, lock)
			if kind == "local_dependency_edit" {
				if err := os.WriteFile(filepath.Join(f.root, "node_modules", "keep.txt"), []byte("locally edited dependency"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "revoked_review" {
				if code, raw := f.run(f.approveArgs()...); code != 0 {
					t.Fatal(code, raw)
				}
				if code, raw := f.run("plan", "--revoke", f.saved.ID, "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			result := inspectCLIResult(t, f)
			if result.Inspection == nil || result.Inspection.Status != want || result.Inspection.RegenerationVerified || result.Inspection.DependencyContentsChecked || result.Inspection.LocalDependencyEdits != "unknown" {
				t.Fatalf("unexpected input scope: %+v", result)
			}
			// Existing --verify deliberately remains content-free, even when input
			// bytes cannot be parsed or existing dependency files were edited.
			code, raw := f.run("plan", "--verify", f.saved.ID, "-d", f.root, "--json")
			var verified struct{ Plan VerifiedPlan }
			if err := json.Unmarshal([]byte(raw), &verified); err != nil || code != 0 || verified.Plan.Live == nil || verified.Plan.Live.Status != "metadata_matches" {
				t.Fatal(code, raw, err)
			}
		})
	}
}
