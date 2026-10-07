package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

// All scans, hashes, choices and file mutations use generated temporary roots.
// No original or private trial source is part of these CLI acceptance fixtures.
func hashMetadataCLIChoice(t *testing.T, f *hashProposalCLIFixture, selection, keeper string, copies ...string) inventory.SavedHashKeeperChoice {
	t.Helper()
	args := append([]string{"hash", "--save-choice", selection, "--keeper", keeper}, copies...)
	code, raw, stderr := f.run(context.Background(), append(args, "--json")...)
	return hashChoiceCLIReport(t, code, raw, stderr, "hash")
}

func hashMetadataCLIReport(t *testing.T, code int, raw, stderr string) inventory.HashKeeperChoiceMetadataReport {
	t.Helper()
	var envelope struct {
		Version int                                      `json:"api_version"`
		Command string                                   `json:"command"`
		OK      bool                                     `json:"ok"`
		Hash    inventory.HashKeeperChoiceMetadataReport `json:"hash"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	err := decoder.Decode(&envelope)
	if err != nil || decoder.Decode(new(any)) != io.EOF || code != 0 || stderr != "" || envelope.Version != APIVersion || envelope.Command != "hash" || !envelope.OK {
		t.Fatal("metadata command lost its one standard result envelope", code, raw, stderr, err)
	}
	r := envelope.Hash
	if r.Source != "live_metadata_with_saved_inventory" || r.Contract != "historical_choice_metadata_check_v1" || r.CheckedAt.IsZero() || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("metadata command claimed body reads or verification/action authority", raw)
	}
	for _, literal := range []string{`"selected_file_body_requested_bytes":0`, `"selected_file_body_read_bytes":0`, `"approval_available":false`, `"provenance_verified":false`, `"content_verified":false`, `"current_state_verified":false`, `"duplicates_verified":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !strings.Contains(raw, literal) {
			t.Fatal("metadata JSON omitted a required explicit qualification", literal, raw)
		}
	}
	return r
}

func hashMetadataCLIExactRoles(t *testing.T, r inventory.HashKeeperChoiceMetadataReport, saved inventory.SavedHashKeeperChoice) {
	t.Helper()
	p := saved.Record.Evidence
	if r.ChoiceID != saved.ID || r.StoreID != p.StoreID || r.SelectionID != p.SelectionID || r.InventoryID != p.InventoryID || len(r.Targets) != 1+len(p.Copies) {
		t.Fatal("metadata report expanded or remapped exact saved scope", r)
	}
	want := append([]inventory.SavedHashPreviewMember{p.Keeper}, p.Copies...)
	for i, target := range r.Targets {
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		if target.Role != role || !reflect.DeepEqual(target.Observation, want[i]) {
			t.Fatal("metadata report changed role order or historical observation", target, want[i])
		}
		if r.Status == "metadata_matches" && (target.Status != "metadata_matches" || target.MetadataCheckedAt.IsZero()) {
			t.Fatal("positive metadata report omitted an individual check", r)
		}
	}
}

func TestHashChoiceMetadataCLIArgumentsJSONValuesAndMissingBeforeState(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	choice := "hash-choice-v1-" + strings.Repeat("a", 64)
	selection := strings.Repeat("a", 64)
	for _, args := range [][]string{
		{"hash", "--check-choice"}, {"hash", "--check-choice", "bad"},
		{"hash", "--check-choice", strings.ToUpper(choice)},
		{"hash", "--check-choice", "hash-choice-v1-" + strings.Repeat("a", 63)},
		{"hash", "--check-choice", "hash-choice-v1-" + strings.Repeat("a", 65)},
		{"hash", "--check-choice", choice + "\n"},
		{"hash", "--check-choice", choice, "--check-choice", choice},
		{"hash", "--check-choice", choice, "-check-choice=" + choice},
		{"hash", "--check-choice", choice, "1"},
		{"hash", "--check-choice", choice, "--show", selection},
		{"hash", "--check-choice", choice, "--select"},
		{"hash", "--check-choice", choice, "--approve", selection},
		{"hash", "--check-choice", choice, "--run", selection},
		{"hash", "--check-choice", choice, "--revoke", selection},
		{"hash", "--check-choice", choice, "--save-choice", selection, "--keeper", "1", "2"},
		{"hash", "--check-choice", choice, "--keeper", "1"},
		{"hash", "--check-choice", choice, "-d", "/generated/missing"},
		{"hash", "--check-choice", choice, "--directory", "/generated/missing"},
		{"hash", "--check-choice", choice, "--from", "/generated/missing"},
		{"hash", "--check-choice", choice, "--confirm-content-read=false"},
		{"hash", "--check-choice", choice, "--max-day-bytes", "1"},
		{"hash", "--check-choice", choice, "--max-total-bytes", "1"},
		{"hash", "--check-choice", choice, "--", "--json"},
		{"hash", "--", "--check-choice", choice},
		{"hashes", "--check-choice", choice},
	} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "invalid_arguments", 2)
	}
	for _, dataFlag := range []string{"--data-dir", "-data-dir"} {
		for _, option := range []string{"--check-choice", "-check-choice"} {
			for _, equals := range []bool{false, true} {
				var out, stderr bytes.Buffer
				args := []string{dataFlag, base, "hash", option, "--json"}
				if equals {
					args = []string{dataFlag, base, "hash", option + "=--json"}
				}
				if code := Run(context.Background(), args, &out, &stderr); code != 2 || out.Len() != 0 || stderr.Len() == 0 {
					t.Fatal("literal option value enabled JSON", args, code, out.String(), stderr.String())
				}
				out.Reset()
				stderr.Reset()
				code := Run(context.Background(), append([]string{dataFlag, base, "--json"}, args[2:]...), &out, &stderr)
				hashChoiceCLIError(t, code, out.String(), stderr.String(), "hash", "invalid_arguments", 2)
			}
		}
	}
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hash", "--check-choice", choice, "--json"}, &out, &stderr)
	hashChoiceCLIError(t, code, out.String(), stderr.String(), "hash", "not_found", 1)
	out.Reset()
	stderr.Reset()
	code = Run(context.Background(), []string{"--data-dir", base, "capabilities", "--json"}, &out, &stderr)
	var advertised struct {
		OK         bool            `json:"ok"`
		Features   map[string]bool `json:"features"`
		ErrorCodes []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &advertised); err != nil || code != 0 || stderr.Len() != 0 || !advertised.OK || !advertised.Features["saved_hash_choice_metadata_checks"] || advertised.Features["duplicates"] || advertised.Features["cleanup"] {
		t.Fatal("capabilities claimed current duplicates/cleanup or omitted the metadata screen", code, out.String(), stderr.String(), err)
	}
	if !slices.Contains(advertised.ErrorCodes, "hash_choice_metadata_unavailable") {
		t.Fatal("capabilities omitted metadata setup error", out.String())
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid/missing metadata request initialized private state", err)
	}
}

func TestHashChoiceMetadataCLIExactRolesReadOnlyAndHumanQualifications(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	// A revoked read consent must not be consulted or renewed by this screen.
	code, raw, stderr := f.run(context.Background(), "hash", "--revoke", f.consent.ID, "--json")
	_ = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "3", "1")
	code, beforeDefault, stderr := f.run(context.Background(), "hashes", "--json")
	hashCLIReport(t, code, beforeDefault, stderr)
	code, beforeGroups, stderr := f.run(context.Background(), "hashes", "--groups", "--json")
	hashGroupsCLIReport(t, code, beforeGroups, stderr)
	beforeBytes := hashCLIBytes(t, f.base, f.root)
	for _, args := range [][]string{
		{"hash", "--check-choice", saved.ID, "--json"},
		{"--json", "hash", "--check-choice", saved.ID},
		{"hash", "--json", "-check-choice=" + saved.ID},
	} {
		code, raw, stderr := f.run(context.Background(), args...)
		r := hashMetadataCLIReport(t, code, raw, stderr)
		if r.Status != "metadata_matches" || r.InventoryStatus != "matches_saved_inventory" {
			t.Fatal("unchanged generated files did not pass the qualified screen", raw)
		}
		hashMetadataCLIExactRoles(t, r, saved)
	}
	code, human, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{saved.ID, "Selected keeper for review", "Selected copy for review", "Metadata matched", "Selected file-body bytes requested 0 bytes", "Selected file-body bytes read 0 bytes", "Approval Unavailable", "Reclaimable space Unknown", "No screen result was saved"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("human metadata result lost scope or limits", want, code, human, stderr)
		}
	}
	if !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "nested\nfolder") || strings.Contains(human, fmt.Sprintf("%q", string(f.proposal.Targets[3].File.PathBytes))) {
		t.Fatal("human report exposed raw controls or an unselected file", human)
	}
	for _, member := range append([]inventory.SavedHashPreviewMember{saved.Record.Evidence.Keeper}, saved.Record.Evidence.Copies...) {
		if !strings.Contains(human, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("human report lost quoted exact path", human)
		}
	}
	code, afterDefault, stderr := f.run(context.Background(), "hashes", "--json")
	hashCLIReport(t, code, afterDefault, stderr)
	code, afterGroups, stderr := f.run(context.Background(), "hashes", "--groups", "--json")
	hashGroupsCLIReport(t, code, afterGroups, stderr)
	code, raw, stderr = f.run(context.Background(), "hashes", "--choice", saved.ID, "--json")
	if reopened := hashChoiceCLIReport(t, code, raw, stderr, "hashes"); !reflect.DeepEqual(reopened, saved) {
		t.Fatal("metadata check refreshed the immutable saved choice", raw)
	}
	if beforeDefault != afterDefault || beforeGroups != afterGroups || !reflect.DeepEqual(beforeBytes, hashCLIBytes(t, f.base, f.root)) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("metadata checks changed source, inventory, choice, work, budget or consent")
	}
}

func TestHashChoiceMetadataCLICurrentExclusionsAndIgnoredConfiguredRoots(t *testing.T) {
	for _, kind := range []string{"unrelated_configured_root", "direct_file", "physical_parent_alias"} {
		t.Run(kind, func(t *testing.T) {
			f := newHashGuidedReviewFixture(t, 3)
			saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
			cfg := config.Default()
			cfg.Roots = []string{filepath.Join(t.TempDir(), "unrelated-root")}
			if kind == "direct_file" {
				cfg.Excludes = []string{string(saved.Record.Evidence.Keeper.PathBytes)}
			}
			if kind == "physical_parent_alias" {
				alias := filepath.Join(t.TempDir(), "excluded-parent-alias")
				if err := os.Symlink(filepath.Dir(string(saved.Record.Evidence.Keeper.PathBytes)), alias); err != nil {
					t.Fatal(err)
				}
				cfg.Excludes = []string{alias}
			}
			home, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			if err = config.Create(filepath.Join(f.base, "config.toml"), home, cfg); err != nil {
				t.Fatal(err)
			}
			before := hashCLIBytes(t, f.base, f.root)
			code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
			r := hashMetadataCLIReport(t, code, raw, stderr)
			hashMetadataCLIExactRoles(t, r, saved)
			if kind == "unrelated_configured_root" {
				if r.Status != "metadata_matches" {
					t.Fatal("configured roots replaced exact frozen manual scope", raw)
				}
			} else if r.Status != "blocked" || r.Targets[0].Code != "scope_excluded" {
				t.Fatal("current direct/physical exclusion was not applied", raw)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("current-policy check modified source or private records")
			}
		})
	}
}

func TestHashChoiceMetadataCLIConfigurationAliasesRefuseBeforeParsing(t *testing.T) {
	for _, ordinal := range []int{1, 3} {
		t.Run(fmt.Sprintf("proposal_member_%d", ordinal+1), func(t *testing.T) {
			f := newHashGuidedReviewFixture(t, 4)
			saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
			path := string(f.proposal.Targets[ordinal].File.PathBytes)
			if err := os.Rename(path, filepath.Join(f.base, "config.toml")); err != nil {
				t.Fatal(err)
			}
			var st unix.Stat_t
			if err := unix.Lstat(filepath.Join(f.base, "config.toml"), &st); err != nil || st.Nlink != 1 {
				t.Fatal("config alias fixture did not retain one link", st, err)
			}
			before := hashCLIBytes(t, f.base, f.root)
			code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
			hashChoiceCLIError(t, code, raw, stderr, "hash", "command_failed", 1)
			if !strings.Contains(raw, "cannot alias selected evidence") || strings.Contains(raw, "decode config") {
				t.Fatal("selected or unselected proposal bytes reached TOML parsing", raw)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("config alias refusal changed files or saved records")
			}
		})
	}
}

func TestHashChoiceMetadataCLIDerivedInventoryMissingWithValidGlobalDecoy(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	body, err := os.ReadFile(filepath.Join(f.source, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(f.base, state.Filename), body, 0600); err != nil {
		t.Fatal(err)
	}
	decoy, err := state.OpenReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	page, err := decoy.SameSizeCandidates(context.Background(), 20, "", 1)
	closeErr := decoy.Close()
	if err != nil || closeErr != nil || page.InventoryID != f.proposal.InventoryID || len(page.Bands) != 1 || len(page.Bands[0].Files) != 4 {
		t.Fatal("global decoy is not a complete matching generated inventory", page, err, closeErr)
	}
	if err = os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
	r := hashMetadataCLIReport(t, code, raw, stderr)
	hashMetadataCLIExactRoles(t, r, saved)
	if r.Status != "blocked" || r.Code != "inventory_unavailable" || r.InventoryStatus != "unavailable" {
		t.Fatal("missing derived inventory silently fell back to a matching global store", raw)
	}
	if _, err = os.Lstat(f.source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("metadata screen initialized a missing manual inventory", err)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("missing-inventory screen changed decoy/source/saved records")
	}
}

func TestHashChoiceMetadataCLIChangedEvidenceReturnsBlockedResult(t *testing.T) {
	for _, kind := range []string{"edit_restore_mtime", "symlink", "missing_path", "inventory_ancestor"} {
		t.Run(kind, func(t *testing.T) {
			f := newHashGuidedReviewFixture(t, 3)
			saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
			member := saved.Record.Evidence.Keeper
			path := string(member.PathBytes)
			if kind == "inventory_ancestor" {
				db, err := sql.Open("sqlite", filepath.Join(f.source, state.Filename))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec("UPDATE entries SET mtime_ns=mtime_ns+1 WHERE kind='directory'"); err != nil {
					t.Fatal(err)
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			} else if kind == "symlink" {
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".old", path); err != nil {
					t.Fatal(err)
				}
			} else if kind == "missing_path" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(path, bytes.Repeat([]byte{'y'}, len(f.contents)), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, member.SavedModifiedAt, member.SavedModifiedAt); err != nil {
					t.Fatal(err)
				}
			}
			code, beforeDefault, stderr := f.run(context.Background(), "hashes", "--json")
			hashCLIReport(t, code, beforeDefault, stderr)
			before := hashCLIBytes(t, f.base, f.root)
			code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
			r := hashMetadataCLIReport(t, code, raw, stderr)
			hashMetadataCLIExactRoles(t, r, saved)
			if r.Status != "blocked" {
				t.Fatal("changed metadata became current verification", raw)
			}
			if kind == "inventory_ancestor" {
				if r.Code != "inventory_changed" || r.InventoryStatus != "changed" {
					t.Fatal("new ancestor observations became a replacement baseline", raw)
				}
			} else if r.Targets[0].Status != "blocked" {
				t.Fatal("edited or symlinked keeper passed live metadata", raw)
			}
			code, afterDefault, stderr := f.run(context.Background(), "hashes", "--json")
			hashCLIReport(t, code, afterDefault, stderr)
			if beforeDefault != afterDefault || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("blocked metadata check modified saved work or changed files")
			}
		})
	}
}

func TestHashChoiceMetadataCLISetupErrorsAndLegacyBeforeConfiguration(t *testing.T) {
	t.Run("malformed_config", func(t *testing.T) {
		f := newHashGuidedReviewFixture(t, 3)
		saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
		if err := os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("malformed [ TOML"), 0600); err != nil {
			t.Fatal(err)
		}
		before := hashCLIBytes(t, f.base, f.root)
		code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
		hashChoiceCLIError(t, code, raw, stderr, "hash", "command_failed", 1)
		if !strings.Contains(raw, "decode config") {
			t.Fatal("malformed config was ignored", raw)
		}
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
			t.Fatal("malformed-config setup modified source or saved records")
		}
	})
	t.Run("legacy_before_config", func(t *testing.T) {
		f := newHashCLIFixture(t)
		for range 2 {
			if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
				t.Fatal(err)
			}
		}
		snapshot, err := f.store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		preview, err := f.store.PreviewKeeper(context.Background(), snapshot.SelectionID, "2", []string{"1"})
		if err != nil {
			t.Fatal(err)
		}
		saved, err := f.store.SaveKeeperChoice(context.Background(), preview)
		if err != nil {
			t.Fatal(err)
		}
		before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
		code, raw, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
		hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_choice_metadata_unavailable", 1)
		if strings.Contains(raw, "decode config") || !strings.Contains(raw, "manual inventory locator") {
			t.Fatal("legacy request opened config or substituted source scope", raw)
		}
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
			t.Fatal("legacy refusal changed source or saved state")
		}
	})
}

func TestHashChoiceMetadataCLIWriterHeldAndReservedWorkDoNotRecover(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 2)
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TRIGGER fail_metadata_cli_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(context.Background(), "hash", "--run", f.consent.ID, "--json")
	hashProposalFailure(t, code, raw, stderr, "hash_recovery_required", 1)
	if _, err = db.Exec("DROP TRIGGER fail_metadata_cli_fixture"); err != nil {
		t.Fatal(err)
	}
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	writer, err := inventory.OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	code, beforeDefault, stderr := f.run(context.Background(), "hashes", "--json")
	snapshot := hashCLIReport(t, code, beforeDefault, stderr)
	third := snapshot.Work[2]
	if third.Status != "running" || third.LatestAttempt == nil || third.LatestAttempt.Status != "reserved" || third.LatestAttempt.ReadBytes != nil || third.LatestAttempt.RequestedBytes != nil || third.LatestAttempt.ElapsedNS != nil || snapshot.Budget.TotalReservedBytes != 3*int64(len(f.contents)) || snapshot.Budget.TotalReadBytes != 2*int64(len(f.contents)) || snapshot.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture does not contain a valid unsettled reservation", beforeDefault)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
	r := hashMetadataCLIReport(t, code, raw, stderr)
	hashMetadataCLIExactRoles(t, r, saved)
	if r.Status != "metadata_matches" {
		t.Fatal("metadata-only check needed a writer or recovered unrelated work", raw)
	}
	code, afterDefault, stderr := f.run(context.Background(), "hashes", "--json")
	hashCLIReport(t, code, afterDefault, stderr)
	if beforeDefault != afterDefault || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("metadata check recovered reserved work, charged new bytes or changed saved consent")
	}
}

func TestHashChoiceMetadataCLIRawPaths(t *testing.T) {
	for _, name := range []string{"raw\x1b\t雪\"", "raw\xff\x1b\t雪"} {
		t.Run(fmt.Sprintf("%x", []byte(name)), func(t *testing.T) {
			f := newHashProposalCLIFixtureWithFilename(t, name)
			if err := os.Remove(filepath.Join(f.base, "config.toml")); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
			proposal := hashCLIProposal(t, code, raw, stderr)
			consent := hashKeeperApprove(t, f, proposal)
			for _, id := range []string{"1", "2"} {
				hashKeeperRun(t, f, consent, id)
			}
			saved := hashMetadataCLIChoice(t, f, proposal.SelectionID, "2", "1")
			before := hashCLIBytes(t, f.base, f.root)
			code, raw, stderr = f.run(context.Background(), "hash", "--check-choice", saved.ID, "--json")
			r := hashMetadataCLIReport(t, code, raw, stderr)
			hashMetadataCLIExactRoles(t, r, saved)
			if r.Status != "metadata_matches" {
				t.Fatal("raw-byte path was changed or treated as display text", raw)
			}
			code, human, stderr := f.run(context.Background(), "hash", "--check-choice", saved.ID)
			if code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "nested\nfolder") {
				t.Fatal("human metadata output emitted raw controls/invalid UTF-8", code, human, stderr)
			}
			for _, target := range r.Targets {
				if !strings.Contains(human, fmt.Sprintf("%q", string(target.Observation.PathBytes))) {
					t.Fatal("human metadata output lost authoritative quoted path", human)
				}
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("raw-path metadata output modified selected files/private records")
			}
		})
	}
}

type hashMetadataCountErrorWriter struct{ bytes.Buffer }

func (w *hashMetadataCountErrorWriter) Write(p []byte) (int, error) {
	n, _ := w.Buffer.Write(p)
	return n, errors.New("fixture full-count output failed")
}

func hashMetadataNoPublicationClaim(t *testing.T, text string) {
	t.Helper()
	qualified := strings.ReplaceAll(strings.ToLower(text), "no screen result was saved", "")
	if strings.Contains(qualified, "was saved") || strings.Contains(qualified, "publication may") {
		t.Fatal("metadata-only output fabricated a publication/recovery claim", text)
	}
}

func TestHashChoiceMetadataCLIOutputAndCancellationNeverPublish(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	before := hashCLIBytes(t, f.base, f.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr := f.run(ctx, "hash", "--check-choice", saved.ID, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "hash", "--check-choice", saved.ID}
		if machine {
			args = append(args, "--json")
		}
		for _, short := range []bool{false, true} {
			var diagnostic bytes.Buffer
			code := Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic)
			if code != 1 || diagnostic.Len() == 0 {
				t.Fatal("metadata output failure reported success", code, diagnostic.String())
			}
			hashMetadataNoPublicationClaim(t, diagnostic.String())
		}
		var diagnostic bytes.Buffer
		fullError := &hashMetadataCountErrorWriter{}
		code := Run(context.Background(), args, fullError, &diagnostic)
		if code != 1 || diagnostic.Len() == 0 {
			t.Fatal("full-count output error was lost", code, diagnostic.String())
		}
		hashMetadataNoPublicationClaim(t, diagnostic.String())
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		diagnostic.Reset()
		code = Run(ctx, args, out, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), "canceled") {
			t.Fatal("full-count metadata output cancellation reported success", code, out.String(), diagnostic.String())
		}
		hashMetadataNoPublicationClaim(t, diagnostic.String())
		if machine {
			// A fully emitted result can precede cancellation, but must not be
			// followed by a second JSON envelope or a success process status.
			r := hashMetadataCLIReport(t, 0, out.String(), "")
			hashMetadataCLIExactRoles(t, r, saved)
			decoder := json.NewDecoder(strings.NewReader(fullError.String()))
			var envelope map[string]any
			if err := decoder.Decode(&envelope); err != nil || decoder.Decode(new(any)) != io.EOF {
				t.Fatal("output failure appended/retried a JSON reply", fullError.String(), err)
			}
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("canceled/failed metadata output saved or recovered state or changed files")
	}
}
