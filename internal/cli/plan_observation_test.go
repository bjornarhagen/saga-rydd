package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func capturedCLIResult(t *testing.T, f reviewCLI) CapturedPlan {
	t.Helper()
	code, raw := f.run("plan", "--capture", f.saved.ID, "-d", f.root, "--json")
	var result struct{ Plan CapturedPlan }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	return result.Plan
}

func comparedCLIResult(t *testing.T, f reviewCLI, id string) ComparedPlan {
	t.Helper()
	code, raw := f.run("plan", "--compare", id, "-d", f.root, "--json")
	var result struct{ Plan ComparedPlan }
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	return result.Plan
}

func observationCLIHashes(t *testing.T, base string) map[string][32]byte {
	t.Helper()
	hashes := map[string][32]byte{}
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".sqlite3") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		hashes[path] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hashes
}

func TestPlanObservationCaptureCompareAndReviewIndependence(t *testing.T) {
	f := treeCLIFixture(t)
	if code, raw := f.run(f.approveArgs()...); code != 0 {
		t.Fatal(code, raw)
	}
	before, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil || before.Review == nil || before.Observation != nil {
		t.Fatal(before, err)
	}
	baseline := capturedCLIResult(t, f)
	if baseline.ID != f.saved.ID || baseline.InventoryCheck.Status != "matches_saved_inventory" || baseline.Inspection == nil || baseline.Inspection.Status != "inputs_and_tree_observed" || baseline.Observation == nil || !plans.ValidObservationID(baseline.Observation.ID) {
		t.Fatalf("missing full observation: %+v", baseline)
	}
	record := baseline.Observation.Record
	if record.PlanID != f.saved.ID || record.ObservedAt.IsZero() || record.Status != "inputs_and_tree_observed" || record.CurrentStateVerified || record.Executable || record.RegenerationVerified || record.DependencyContentsChecked || record.LocalDependencyEdits != "unknown" || len(record.Targets) != 1 || record.Targets[0].Tree.Entries != 4 {
		t.Fatalf("unexpected saved evidence: %+v", record)
	}
	after, err := plans.Show(context.Background(), f.base, f.saved.ID)
	if err != nil || !reflect.DeepEqual(before.Record, after.Record) || !reflect.DeepEqual(before.Review, after.Review) || !reflect.DeepEqual(after.Observation, baseline.Observation) {
		t.Fatal("capture changed selection or review", after, err)
	}
	hashes := observationCLIHashes(t, f.base)
	for i := 0; i < 2; i++ {
		result := comparedCLIResult(t, f, baseline.Observation.ID)
		comparison := result.Comparison
		if result.ID != f.saved.ID || result.ObservationID != baseline.Observation.ID || result.InventoryCheck.Status != "matches_saved_inventory" || comparison == nil || comparison.Status != "matches_observation" || comparison.CurrentStateVerified || comparison.Executable || comparison.RegenerationVerified || comparison.DependencyContentsChecked || comparison.LocalDependencyEdits != "unknown" || len(comparison.Targets) != 1 || len(comparison.Targets[0].Changes) != 0 {
			t.Fatalf("unexpected comparison: %+v", result)
		}
	}
	if got := capturedCLIResult(t, f); !reflect.DeepEqual(got.Observation, baseline.Observation) {
		t.Fatalf("retry replaced the baseline: %+v", got)
	}
	if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("comparison or matching retry changed databases")
	}
	code, human := f.run("plan", "--capture", f.saved.ID, "-d", f.root)
	plain := strings.Join(strings.Fields(human), " ")
	if code != 0 || !strings.Contains(human, "OBSERVATION SAVED - REVIEW REQUIRED") || !strings.Contains(human, "plan --compare "+baseline.Observation.ID+" -d "+shellQuote(f.root)) || !strings.Contains(human, "Tree entries") || strings.Contains(human, record.Targets[0].Tree.MetadataSHA256) || !strings.Contains(plain, "Ordinary dependency file contents are not read") || !strings.Contains(plain, "Local dependency edits and successful reinstall remain unknown") {
		t.Fatal(code, human)
	}
	code, human = f.run("plan", "--compare", baseline.Observation.ID, "-d", f.root)
	if code != 0 || !strings.Contains(human, "OBSERVED INPUTS AND TREE METADATA MATCH THE BASELINE") || strings.Contains(human, record.Targets[0].Tree.MetadataSHA256) || !strings.Contains(strings.Join(strings.Fields(human), " "), "This read-only result cannot authorize cleanup or renew review consent") {
		t.Fatal(code, human)
	}
	for name, want := range map[string][]byte{"package.json": inspectManifest, "package-lock.json": inspectLock, "node_modules/fixture-dep/bin.js": treeDependencyContents} {
		got, err := os.ReadFile(filepath.Join(f.root, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("source changed", name, err)
		}
	}
}

func TestPlanObservationComparisonChangesAndBlocks(t *testing.T) {
	for _, kind := range []string{"lock_bytes", "dependency_metadata", "manifest_metadata", "unsafe_link", "inventory_revision", "different_inventory"} {
		t.Run(kind, func(t *testing.T) {
			f := treeCLIFixture(t)
			captured := capturedCLIResult(t, f)
			id := captured.Observation.ID
			wantStatus, wantChange := "changed", ""
			switch kind {
			case "lock_bytes":
				if err := os.WriteFile(filepath.Join(f.root, "package-lock.json"), append(append([]byte{}, inspectLock...), '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				wantChange = "lock_bytes_changed"
			case "dependency_metadata":
				if err := os.WriteFile(filepath.Join(f.root, "node_modules", "fixture-dep", "bin.js"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
					t.Fatal(err)
				}
				wantChange = "tree_metadata_changed"
			case "manifest_metadata":
				if err := os.WriteFile(filepath.Join(f.root, "package.json"), append(append([]byte{}, inspectManifest...), '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				wantStatus = "blocked"
			case "unsafe_link":
				link := filepath.Join(f.root, "node_modules", ".bin", "fixture-dep")
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("../../package.json", link); err != nil {
					t.Fatal(err)
				}
				wantStatus = "blocked"
			case "inventory_revision", "different_inventory":
				if kind == "different_inventory" {
					paths, err := config.ResolvePaths(f.base)
					if err != nil {
						t.Fatal(err)
					}
					dir := manualState(paths, f.root)
					if err := os.Rename(dir, dir+".old"); err != nil {
						t.Fatal(err)
					}
				}
				if code, raw := f.run("scan", "-d", f.root, "--compact", "--now", "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			hashes := observationCLIHashes(t, f.base)
			result := comparedCLIResult(t, f, id)
			if kind == "inventory_revision" || kind == "different_inventory" {
				if result.Comparison != nil || result.InventoryCheck.Status != "changed" {
					t.Fatalf("comparison ignored changed inventory: %+v", result)
				}
			} else {
				if result.Comparison == nil || result.Comparison.Status != wantStatus || result.Comparison.LocalDependencyEdits != "unknown" || result.Comparison.DependencyContentsChecked {
					t.Fatalf("unexpected comparison: %+v", result)
				}
				if wantChange != "" && !strings.Contains(strings.Join(result.Comparison.Targets[0].Changes, ","), wantChange) {
					t.Fatal(result.Comparison.Targets[0])
				}
				if wantStatus == "blocked" && (result.Comparison.Targets[0].Inputs != nil || result.Comparison.Targets[0].Tree != nil) {
					t.Fatal("blocked target has success evidence", result.Comparison.Targets[0])
				}
			}
			if !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
				t.Fatal("comparison wrote database state")
			}
			if wantChange != "" {
				if code, raw := f.run("plan", "--capture", f.saved.ID, "-d", f.root, "--json"); code != 1 || !strings.Contains(raw, "observation_conflict") {
					t.Fatal("capture advanced immutable baseline", code, raw)
				}
				code, human := f.run("plan", "--compare", id, "-d", f.root)
				if code != 0 || !strings.Contains(human, "OBSERVATION CHANGED - REVIEW AGAIN") || !strings.Contains(human, observationChangeMessage(wantChange)) {
					t.Fatal(code, human)
				}
			}
			shown, err := plans.Show(context.Background(), f.base, f.saved.ID)
			if err != nil || !reflect.DeepEqual(shown.Record, f.saved.Record) || shown.Review != nil || !reflect.DeepEqual(shown.Observation, captured.Observation) {
				t.Fatal("saved review changed", shown, err)
			}
		})
	}
}

func TestPlanObservationCaptureBlockedSavesNothing(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "tree_refusal", true: "inventory_refusal"}[partial], func(t *testing.T) {
			var f reviewCLI
			if partial {
				f = reviewCLIFixture(t, true)
			} else {
				f = inspectCLIFixture(t, inspectManifest, inspectLock)
			}
			hashes := observationCLIHashes(t, f.base)
			result := capturedCLIResult(t, f)
			if result.Observation != nil || (partial && result.Inspection != nil) || (!partial && (result.Inspection == nil || result.Inspection.Status != "blocked")) || !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
				t.Fatalf("refusal saved or migrated a record: %+v", result)
			}
			if !partial && result.Inspection.Targets[0].Code != "tree_layout_unknown" {
				t.Fatal("fixture did not exercise a tree refusal", result.Inspection)
			}
			shown, err := plans.Show(context.Background(), f.base, f.saved.ID)
			if err != nil || shown.Observation != nil || !reflect.DeepEqual(shown.Record, f.saved.Record) || shown.Review != nil {
				t.Fatal(shown, err)
			}
		})
	}
}

func TestPlanObservationCaptureAllTargetsOrNothing(t *testing.T) {
	f := treeCLIFixture(t)
	project := filepath.Join(f.root, "second")
	modules := filepath.Join(project, "node_modules")
	if err := os.MkdirAll(filepath.Join(modules, "fixture-dep"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"package.json": inspectManifest, "package-lock.json": inspectLock, "node_modules/fixture-dep/bin.js": treeDependencyContents, "node_modules/custom.txt": []byte("custom boundary object")} {
		if err := os.WriteFile(filepath.Join(project, name), body, 0700); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{modules, filepath.Join(project, "package.json")} {
		if err := os.Chtimes(name, old, old); err != nil {
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
	findings, err := s.NodeModulesFindings(context.Background(), "", 90)
	s.Close()
	if err != nil || len(findings.Findings) != 2 {
		t.Fatal(findings, err)
	}
	code, raw := f.run("plan", "--save", "-d", f.root, findings.Findings[0].ID, findings.Findings[1].ID, "--json")
	var saved struct{ Plan plans.Saved }
	if err := json.Unmarshal([]byte(raw), &saved); err != nil || code != 0 {
		t.Fatal(code, raw, err)
	}
	f.saved = saved.Plan
	hashes := observationCLIHashes(t, f.base)
	result := capturedCLIResult(t, f)
	if result.Observation != nil || result.Inspection == nil || result.Inspection.Status != "blocked" || len(result.Inspection.Targets) != 2 || !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatalf("partial observation published: %+v", result)
	}
	statuses := map[string]int{}
	for _, target := range result.Inspection.Targets {
		statuses[target.Status]++
	}
	if statuses["inputs_and_tree_observed"] != 1 || statuses["blocked"] != 1 {
		t.Fatal(result.Inspection)
	}
}

func TestPlanObservationOfflineRecoveryAndNoInitialization(t *testing.T) {
	f := treeCLIFixture(t)
	baseline := capturedCLIResult(t, f).Observation
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	dir := manualState(paths, f.root)
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir, dir+".offline"); err != nil {
		t.Fatal(err)
	}
	hashes := observationCLIHashes(t, f.base)
	code, raw := f.run("plan", "--show", f.saved.ID, "--json")
	var shown struct{ Plan plans.Saved }
	if err := json.Unmarshal([]byte(raw), &shown); err != nil || code != 0 || !reflect.DeepEqual(shown.Plan.Observation, baseline) {
		t.Fatal(code, raw, err)
	}
	code, human := f.run("plan", "--show", f.saved.ID)
	if code != 0 || !strings.Contains(human, baseline.ID) || !strings.Contains(human, "plan --compare "+baseline.ID) {
		t.Fatal(code, human)
	}
	if code, raw := f.run("plan", "--compare", baseline.ID, "-d", f.root, "--json"); code != 1 || !strings.Contains(raw, "not_found") {
		t.Fatal(code, raw)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) || !reflect.DeepEqual(hashes, observationCLIHashes(t, f.base)) {
		t.Fatal("comparison initialized missing inventory", err)
	}
	// A missing baseline must be resolved before an invalid directory is used.
	missingID := "observation-v1-" + strings.Repeat("0", 64)
	if code, raw := f.run("plan", "--compare", missingID, "-d", "", "--json"); code != 1 || !strings.Contains(raw, "not_found") {
		t.Fatal(code, raw)
	}
	missingBase := filepath.Join(t.TempDir(), "absent")
	var out, stderr bytes.Buffer
	code = Run(context.Background(), []string{"--data-dir", missingBase, "plan", "--compare", baseline.ID, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "not_found") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
	if _, err := os.Stat(missingBase); !os.IsNotExist(err) {
		t.Fatal("comparison initialized data directory", err)
	}
}

func TestPlanObservationArgumentsAndCapabilities(t *testing.T) {
	f := treeCLIFixture(t)
	id := capturedCLIResult(t, f).Observation.ID
	for _, mode := range []string{"--capture", "--compare"} {
		selected := f.saved.ID
		if mode == "--compare" {
			selected = id
		}
		for _, option := range [][]string{{"--tree"}, {"--tree=false"}, {"--min-age-days", "90"}, {"--confirm-project-review"}, {"--confirm-quarantine=false"}, {"extra"}, {"--show", f.saved.ID}, {"-d", f.root, "--directory", f.root}} {
			args := append([]string{"plan", mode, selected}, option...)
			args = append(args, "--json")
			if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
				t.Fatal(args, code, raw)
			}
		}
		for _, value := range []string{"", "invalid", "--json", map[string]string{"--capture": id, "--compare": f.saved.ID}[mode]} {
			if code, raw := f.run("plan", mode, value, "--json"); code != 2 || !strings.Contains(raw, "invalid_arguments") {
				t.Fatal(mode, value, code, raw)
			}
		}
	}
	code, raw := f.run("capabilities", "--json")
	var capabilities struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || !capabilities.Features["plan_observation_capture"] || !capabilities.Features["plan_observation_comparison"] || capabilities.Features["cleanup"] || !strings.Contains(raw, "--capture PLAN_ID") || !strings.Contains(raw, "--compare OBSERVATION_ID") {
		t.Fatal(code, raw, err)
	}
	code, help := f.run("--help")
	if code != 0 || !strings.Contains(help, "--capture PLAN_ID") || !strings.Contains(help, "--compare OBSERVATION_ID") {
		t.Fatal(code, help)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	code = Run(ctx, []string{"--data-dir", f.base, "plan", "--capture", f.saved.ID, "-d", f.root, "--json"}, &out, &stderr)
	if code != 1 || !strings.Contains(out.String(), "canceled") || stderr.Len() != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
}
