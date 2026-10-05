package cli

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlanVerifyCLI(t *testing.T) {
	f := reviewCLIFixture(t, false)
	verify := func(want string) {
		t.Helper()
		code, raw := f.run("plan", "--verify", f.saved.ID, "-d", f.root, "--json")
		var result struct{ Plan VerifiedPlan }
		if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || result.Plan.Live == nil || result.Plan.Live.Status != want || result.Plan.Live.Executable || result.Plan.Live.CurrentStateVerified {
			t.Fatal(code, raw, err)
		}
	}
	hash := func(path string) [32]byte {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return sha256.Sum256(b)
	}
	planDB := filepath.Join(f.base, "plans", "plans.sqlite3")
	before := hash(planDB)
	verify("metadata_matches")
	code, human := f.run("plan", "--verify", f.saved.ID, "-d", f.root)
	if code != 0 || !strings.Contains(human, "SELECTED METADATA MATCHES - CLEANUP UNAVAILABLE") {
		t.Fatal(code, human)
	}
	for _, args := range [][]string{
		{"plan", "--verify", f.saved.ID, "--check", f.saved.ID, "--json"},
		{"plan", "--verify", f.saved.ID, "--min-age-days", "90", "--json"},
		{"plan", "--verify", f.saved.ID, "extra", "--json"},
		{"plan", "--verify", f.saved.ID, "--confirm-quarantine", "--json"},
		{"plan", "--verify", "--json", "--json"},
	} {
		if code, raw := f.run(args...); code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(code, raw)
		}
	}
	// Contents below the selected node are outside this metadata-only contract.
	if err := os.WriteFile(filepath.Join(f.root, "node_modules", "keep.txt"), []byte("edited contents"), 0600); err != nil {
		t.Fatal(err)
	}
	verify("metadata_matches")
	if err := os.Rename(filepath.Join(f.root, "package.json"), filepath.Join(f.root, "package.old")); err != nil {
		t.Fatal(err)
	}
	verify("blocked")
	// Saved-inventory checks still match until a scan observes the live change.
	if code, raw := f.run("plan", "--check", f.saved.ID, "-d", f.root, "--json"); code != 0 || !strings.Contains(raw, "matches_saved_inventory") {
		t.Fatal(code, raw)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	verify("blocked")
	if before != hash(planDB) {
		t.Fatal("verification changed saved plan storage")
	}
}

func TestPlanVerifyCurrentExclusionsAndPartialEvidence(t *testing.T) {
	for _, kind := range []string{"partial", "exclude", "exclude_child"} {
		t.Run(kind, func(t *testing.T) {
			f := reviewCLIFixture(t, kind == "partial")
			if kind != "partial" {
				excluded := filepath.Join(f.root, "node_modules")
				if kind == "exclude_child" {
					excluded = filepath.Join(excluded, "keep.txt")
				}
				if code, raw := f.run("init", "--root", f.root, "--exclude", excluded, "--json"); code != 0 {
					t.Fatal(code, raw)
				}
			}
			code, raw := f.run("plan", "--verify", f.saved.ID, "-d", f.root, "--json")
			var result struct{ Plan VerifiedPlan }
			if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 {
				t.Fatal(code, raw, err)
			}
			if kind == "partial" {
				if result.Plan.Live != nil || result.Plan.InventoryCheck.Status != "unverifiable" {
					t.Fatal(raw)
				}
			} else if result.Plan.Live == nil || result.Plan.Live.Status != "blocked" || result.Plan.Live.Targets[0].Code != "scope_excluded" {
				t.Fatal(raw)
			}
		})
	}
}
