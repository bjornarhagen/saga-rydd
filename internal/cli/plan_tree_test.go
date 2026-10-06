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

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"golang.org/x/sys/unix"
)

var treeDependencyContents = []byte("#!/bin/sh\nexit 0\n")

func treeCLIFixture(t *testing.T, extra ...func(string)) reviewCLI {
	t.Helper()
	setup := []func(string){func(root string) {
		if err := os.Remove(filepath.Join(root, "node_modules", "keep.txt")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"fixture-dep", ".bin"} {
			if err := os.Mkdir(filepath.Join(root, "node_modules", name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "node_modules", "fixture-dep", "bin.js"), treeDependencyContents, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("../fixture-dep/bin.js", filepath.Join(root, "node_modules", ".bin", "fixture-dep")); err != nil {
			t.Fatal(err)
		}
	}}
	return inspectCLIFixture(t, inspectManifest, inspectLock, append(setup, extra...)...)
}

func treeCLIResult(t *testing.T, f reviewCLI) InspectedPlan {
	t.Helper()
	code, raw := f.run("plan", "--inspect", f.saved.ID, "--tree", "-d", f.root, "--json")
	var result struct{ Plan InspectedPlan }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	return result.Plan
}

func TestPlanInspectTreeCLIOptInAndNoWrites(t *testing.T) {
	f := treeCLIFixture(t)
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
	result := treeCLIResult(t, f)
	r := result.Inspection
	if result.ID != f.saved.ID || result.InventoryCheck.Status != "matches_saved_inventory" || r == nil || r.Status != "inputs_and_tree_observed" || r.Source != "live_project_inputs_and_tree_metadata" || r.CheckedAt.IsZero() || r.CurrentStateVerified || r.Executable || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) != 1 {
		t.Fatalf("unexpected installed tree inspection: %+v", result)
	}
	target := r.Targets[0]
	if target.FindingID != f.saved.Record.Selection.Evidence.Findings[0].ID || target.Status != "inputs_and_tree_observed" || target.Inputs == nil || target.Tree == nil {
		t.Fatalf("missing input/tree evidence: %+v", target)
	}
	tree := target.Tree
	if tree.Status != "metadata_observed" || tree.Entries != 4 || tree.Directories != 2 || tree.RegularFiles != 1 || tree.InternalBinLinks != 1 {
		t.Fatalf("unexpected fixture counts: %+v", tree)
	}
	if digest, err := hex.DecodeString(tree.MetadataSHA256); err != nil || len(digest) != sha256.Size {
		t.Fatalf("invalid metadata digest: %+v", tree)
	}
	code, human := f.run("plan", "--inspect", f.saved.ID, "--tree", "-d", f.root)
	plain := strings.Join(strings.Fields(human), " ")
	if code != 0 || !strings.Contains(human, "INPUTS AND TREE LAYOUT OBSERVED - REVIEW REQUIRED") || !strings.Contains(human, "Tree entries") || !strings.Contains(human, "Internal executable links") || strings.Contains(human, tree.MetadataSHA256) || !strings.Contains(plain, "Ordinary dependency file contents are not read") || !strings.Contains(plain, "metadata digest has no saved baseline") || !strings.Contains(plain, "Local dependency edits and successful reinstall remain unknown") {
		t.Fatal(code, human)
	}
	for _, option := range []string{"", "--tree=false"} {
		args := []string{"plan", "--inspect", f.saved.ID, "-d", f.root, "--json"}
		if option != "" {
			args = append(args, option)
		}
		code, raw := f.run(args...)
		var inputOnly struct{ Plan InspectedPlan }
		if err := json.Unmarshal([]byte(raw), &inputOnly); err != nil || code != 0 || inputOnly.Plan.Inspection == nil || inputOnly.Plan.Inspection.Status != "inputs_observed" || inputOnly.Plan.Inspection.Source != "live_project_inputs" || inputOnly.Plan.Inspection.Targets[0].Tree != nil || strings.Contains(raw, `"tree"`) {
			t.Fatal(code, raw, err)
		}
	}
	shown, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil || shown.Review != nil || !reflect.DeepEqual(shown.Record, f.saved.Record) || hash(planDB) != planBefore || hash(inventoryDB) != inventoryBefore {
		t.Fatal("tree inspection changed saved records", shown, err)
	}
	for name, want := range map[string][]byte{"package.json": inspectManifest, "package-lock.json": inspectLock, "node_modules/fixture-dep/bin.js": treeDependencyContents} {
		got, err := os.ReadFile(filepath.Join(f.root, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal(name, err, string(got))
		}
	}
	if link, err := os.Readlink(filepath.Join(f.root, "node_modules", ".bin", "fixture-dep")); err != nil || link != "../fixture-dep/bin.js" {
		t.Fatal(link, err)
	}
}

func TestPlanInspectTreeCLIArgumentsAndCapabilities(t *testing.T) {
	f := treeCLIFixture(t)
	for _, mode := range []string{"--show", "--check", "--verify", "--approve", "--revoke"} {
		for _, tree := range []string{"--tree", "--tree=false"} {
			if code, raw := f.run("plan", mode, f.saved.ID, tree, "--json"); code != 2 || !strings.Contains(raw, "invalid_arguments") || !strings.Contains(raw, "accepted only with --inspect") {
				t.Fatal(mode, tree, code, raw)
			}
		}
	}
	for _, args := range [][]string{
		{"plan", "--tree", "--json"},
		{"plan", "--preview", "--tree=false", "finding", "--json"},
		{"plan", "--save", "--tree", "finding", "--json"},
		{"plan", "--inspect", f.saved.ID, "--tree=invalid", "--json"},
		{"plan", "--inspect", f.saved.ID, "--tree", "extra", "--json"},
		{"plan", "--inspect", f.saved.ID, "--tree", "--min-age-days", "90", "--json"},
	} {
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	code, raw := f.run("capabilities", "--json")
	var capability struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capability); err != nil || code != 0 || !capability.Features["plan_tree_inspection"] || capability.Features["cleanup"] || !strings.Contains(raw, "optional tree metadata listings") {
		t.Fatal(code, raw, err)
	}
	code, help := f.run("--help")
	if code != 0 || !strings.Contains(help, "--inspect PLAN_ID [--tree]") || !strings.Contains(help, "no ordinary file contents") {
		t.Fatal(code, help)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	code = Run(ctx, []string{"--data-dir", f.base, "plan", "--inspect", f.saved.ID, "--tree", "-d", f.root, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "canceled") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
}

func TestPlanInspectTreeCLISavedEvidenceAndScope(t *testing.T) {
	t.Run("partial_saved_evidence", func(t *testing.T) {
		f := reviewCLIFixture(t, true)
		result := treeCLIResult(t, f)
		if result.Inspection != nil || result.InventoryCheck.Status != "unverifiable" {
			t.Fatalf("tree inspection ran with incomplete saved evidence: %+v", result)
		}
	})
	for _, kind := range []string{"exclude_target", "exclude_child", "exclude_lock", "offline_source"} {
		t.Run(kind, func(t *testing.T) {
			f := treeCLIFixture(t)
			if kind == "offline_source" {
				if err := os.Rename(f.root, f.root+".offline"); err != nil {
					t.Fatal(err)
				}
			} else {
				excluded := filepath.Join(f.root, "node_modules")
				if kind == "exclude_child" {
					excluded = filepath.Join(excluded, "fixture-dep", "bin.js")
				}
				if kind == "exclude_lock" {
					excluded = filepath.Join(f.root, "package-lock.json")
				}
				if code, raw := f.run("init", "--root", f.root, "--exclude", excluded, "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			result := treeCLIResult(t, f)
			if result.InventoryCheck.Status != "matches_saved_inventory" || result.Inspection == nil || result.Inspection.Status != "blocked" || result.Inspection.Targets[0].Tree != nil || result.Inspection.Targets[0].Inputs != nil {
				t.Fatalf("unsupported scope was not blocked: %+v", result)
			}
		})
	}
}

func TestPlanInspectTreeCLIUnsupportedObjectsAndLocalEdits(t *testing.T) {
	t.Run("top_level_file_requires_opt_in", func(t *testing.T) {
		f := inspectCLIFixture(t, inspectManifest, inspectLock)
		if inputOnly := inspectCLIResult(t, f); inputOnly.Inspection == nil || inputOnly.Inspection.Status != "inputs_observed" {
			t.Fatal(inputOnly)
		}
		result := treeCLIResult(t, f)
		if result.Inspection == nil || result.Inspection.Status != "blocked" || result.Inspection.Targets[0].Tree != nil || result.Inspection.Targets[0].Inputs != nil {
			t.Fatalf("custom top-level file was accepted: %+v", result)
		}
	})
	for _, kind := range []string{"symlink", "fifo", "local_edit"} {
		t.Run(kind, func(t *testing.T) {
			f := treeCLIFixture(t)
			file := filepath.Join(f.root, "node_modules", "fixture-dep", "bin.js")
			want := "blocked"
			if kind == "local_edit" {
				if err := os.WriteFile(file, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
					t.Fatal(err)
				}
				want = "inputs_and_tree_observed"
			} else {
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(filepath.Join(f.root, "package.json"), file); err != nil {
						t.Fatal(err)
					}
				} else if err := unix.Mkfifo(file, 0600); err != nil {
					t.Fatal(err)
				}
			}
			result := treeCLIResult(t, f)
			if result.Inspection == nil || result.Inspection.Status != want || result.Inspection.RegenerationVerified || result.Inspection.DependencyContentsChecked || result.Inspection.LocalDependencyEdits != "unknown" {
				t.Fatalf("unexpected installed object result: %+v", result)
			}
			if want == "blocked" {
				code, human := f.run("plan", "--inspect", f.saved.ID, "--tree", "-d", f.root)
				if code != 0 || !strings.Contains(human, "PROJECT INPUT CHECK BLOCKED - REVIEW REQUIRED") {
					t.Fatal(code, human)
				}
			}
		})
	}
}
