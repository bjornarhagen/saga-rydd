package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashChoiceCLIReport(t *testing.T, code int, raw, stderr, command string) inventory.SavedHashKeeperChoice {
	t.Helper()
	var envelope struct {
		Version int                             `json:"api_version"`
		OK      bool                            `json:"ok"`
		Command string                          `json:"command"`
		Hash    inventory.SavedHashKeeperChoice `json:"hash"`
		Hashes  inventory.SavedHashKeeperChoice `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != command {
		t.Fatal(code, raw, stderr, err)
	}
	saved := envelope.Hash
	if command == "hashes" {
		saved = envelope.Hashes
	}
	r := saved.Record.Evidence
	if !inventory.ValidHashKeeperChoiceID(saved.ID) || saved.Record.Version != 1 || saved.Record.Contract != "historical_hash_choice_v1" || saved.Record.CreatedAt.IsZero() || saved.Record.Status != "historical_unapproved" || r.Contract != inventory.HashKeeperPreviewContract || r.Source != "saved_hash_observations" || r.Scope != "explicit_saved_subset" || r.BudgetScope != "whole_saved_selection" || len(r.Copies) < 1 || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil || r.ReadConsent != nil && r.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("saved roles changed historical evidence or granted authority", raw)
	}
	return saved
}

func hashChoiceCLIError(t *testing.T, code int, raw, stderr, command, want string, exit int) {
	t.Helper()
	var envelope struct {
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.OK || envelope.Command != command || envelope.Error.Code != want {
		t.Fatal(code, raw, stderr, err)
	}
	for _, forbidden := range []string{`"hash":`, `"hashes":`, `"keeper":`, `"copies":`} {
		if strings.Contains(raw, forbidden) {
			t.Fatal("failed choice operation exposed partial roles", raw)
		}
	}
}

func hashChoiceIDs(t *testing.T, base string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='hash_keeper_choice'").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		return nil
	}
	rows, err := db.Query("SELECT id FROM hash_keeper_choice ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func hashChoiceOffline(t *testing.T, f *hashProposalCLIFixture) {
	t.Helper()
	if err := os.WriteFile(f.base+"/config.toml", []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{f.root, f.source} {
		if err := os.Rename(path, path+".offline"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHashKeeperChoiceCLIExactOfflineRetryOrderAndContext(t *testing.T) {
	ctx := context.Background()
	f := newHashGuidedReviewFixture(t, 4)
	code, raw, stderr := f.run(ctx, "hash", "--revoke", f.consent.ID, "--json")
	f.consent = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	code, beforeDefault, stderr := f.run(ctx, "hashes", "--json")
	ordinary := hashCLIReport(t, code, beforeDefault, stderr)
	code, beforeGroups, stderr := f.run(ctx, "hashes", "--groups", "--json")
	hashGroupsCLIReport(t, code, beforeGroups, stderr)
	code, raw, stderr = f.run(ctx, "hashes", "--preview", f.proposal.SelectionID, "--keeper", "2", "3", "1", "--json")
	expected := hashKeeperCLIReport(t, code, raw, stderr)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	sourceBefore := hashCLIBytes(t, f.root+".offline", f.source+".offline")
	code, raw, stderr = f.run(ctx, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "3", "1", "--json")
	saved := hashChoiceCLIReport(t, code, raw, stderr, "hash")
	if !reflect.DeepEqual(saved.Record.Evidence, expected) || saved.Record.Evidence.SHA256 != fmt.Sprintf("%x", sha256.Sum256(f.contents)) {
		t.Fatal("saving changed selected paths, roles, independent hash or historical context", raw)
	}
	for _, args := range [][]string{
		{"--json", "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "3", "1"},
		{"hash", "-save-choice=" + f.proposal.SelectionID, "-keeper=2", "3", "1", "--json"},
		{"hash", "--save-choice", f.proposal.SelectionID, "--json", "--keeper", "2", "3", "1"},
	} {
		code, raw, stderr = f.run(ctx, args...)
		if retry := hashChoiceCLIReport(t, code, raw, stderr, "hash"); !reflect.DeepEqual(retry, saved) {
			t.Fatal("exact retry changed ID, timestamp or payload", raw)
		}
	}
	for _, args := range [][]string{{"hashes", "--choice", saved.ID, "--json"}, {"--json", "hashes", "-choice=" + saved.ID}, {"hashes", "--json", "--choice", saved.ID}} {
		code, raw, stderr = f.run(ctx, args...)
		if reopened := hashChoiceCLIReport(t, code, raw, stderr, "hashes"); !reflect.DeepEqual(reopened, saved) {
			t.Fatal("offline reopening changed the immutable choice", raw)
		}
	}
	code, raw, stderr = f.run(ctx, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1", "3", "--json")
	reordered := hashChoiceCLIReport(t, code, raw, stderr, "hash")
	if reordered.ID == saved.ID || reordered.Record.Evidence.Copies[0].WorkID != "1" || reordered.Record.Evidence.Copies[1].WorkID != "3" || len(hashChoiceIDs(t, f.base)) != 2 {
		t.Fatal("different explicit role order was overwritten or normalized", raw)
	}
	code, afterDefault, stderr := f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, afterDefault, stderr); !reflect.DeepEqual(after, ordinary) || afterDefault != beforeDefault {
		t.Fatal("saving or reopening roles changed hashing work, budget or consent", afterDefault)
	}
	code, afterGroups, stderr := f.run(ctx, "hashes", "--groups", "--json")
	hashGroupsCLIReport(t, code, afterGroups, stderr)
	if afterGroups != beforeGroups || !reflect.DeepEqual(sourceBefore, hashCLIBytes(t, f.root+".offline", f.source+".offline")) {
		t.Fatal("saving changed groups, source files or saved inventory")
	}
	code, human, stderr := f.run(ctx, "hashes", "--choice", saved.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"HISTORICAL CHOICE SAVED - CLEANUP UNAVAILABLE", saved.ID, "Selected keeper for review", "Selected copy for review", "Current files have not been checked", "historical context", "Approval Unavailable", "Reclaimable space Unknown", "No cleanup consent was recorded"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("saved human output lost roles or uncertainty", want, code, human, stderr)
		}
	}
	if strings.Contains(human, "No decision was saved") || strings.Contains(human, "No source files or saved records were changed") {
		t.Fatal("saved output incorrectly claimed no publication", human)
	}
	for _, path := range []string{f.root, f.source} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("choice operation recreated offline source or inventory", path, err)
		}
	}
}

func TestHashKeeperChoiceCLIArgumentsJSONAndMissingBeforeStorage(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	selection, choice := strings.Repeat("a", 64), "hash-choice-v1-"+strings.Repeat("a", 64)
	for _, args := range [][]string{
		{"hash", "--save-choice", selection}, {"hash", "--keeper", "1"},
		{"hash", "--save-choice", selection, "--keeper", "1"},
		{"hash", "--save-choice", selection, "--keeper", "01", "2"},
		{"hash", "--save-choice", selection, "--keeper", "1", "1"},
		{"hash", "--save-choice", selection, "--keeper", "1", "2", "2"},
		{"hash", "--save-choice", selection, "--keeper", "1", "21"},
		{"hash", "--save-choice", strings.ToUpper(selection), "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "-save-choice", selection, "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--keeper", "1", "-keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--show", selection, "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--approve", selection, "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--select", "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "-d", "/generated/missing", "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--from", "/generated/missing", "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--confirm-content-read", "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--max-total-bytes", "8", "--keeper", "1", "2"},
		{"hash", "--save-choice", selection, "--keeper", "1", "2", "--max-total-bytes", "8"},
		{"hashes", "--choice", "bad"}, {"hashes", "--choice", choice, "--choice", choice},
		{"hashes", "--choice", choice, "--groups"}, {"hashes", "--choice", choice, "--groups=false"},
		{"hashes", "--choice", choice, "--preview", selection, "--keeper", "1", "2"},
		{"hashes", "--choice", choice, "--work", "1"}, {"hashes", "--choice", choice, "1"},
		{"hashes", "--choice", choice, "-d", "/generated/missing"},
	} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "invalid_arguments", 2)
	}
	for _, dataFlag := range []string{"--data-dir", "-data-dir"} {
		for _, args := range [][]string{
			{"hash", "--save-choice", "--json", "--keeper", "1", "2"},
			{"hash", "-save-choice", "--json", "-keeper", "1", "2"},
			{"hash", "--save-choice", selection, "--keeper", "--json", "2"},
			{"hashes", "--choice", "--json"}, {"hashes", "-choice", "--json"},
		} {
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{dataFlag, base}, args...), &out, &stderr)
			if code != 2 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("literal option value incorrectly enabled JSON", dataFlag, args, code, out.String(), stderr.String())
			}
			out.Reset()
			stderr.Reset()
			code = Run(context.Background(), append([]string{dataFlag, base, "--json"}, args...), &out, &stderr)
			hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "invalid_arguments", 2)
		}
	}
	for _, args := range [][]string{{"hash", "--save-choice", selection, "--keeper", "1", "2"}, {"hashes", "--choice", choice}} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "not_found", 1)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid or missing choice request initialized storage", err)
	}
}

func TestHashKeeperChoiceCLIUnmatchedMissingCanceledAndOutputRecovery(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	ctx := context.Background()
	before := hashCLIBytes(t, f.base, f.root)
	for _, args := range [][]string{
		{"hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "4"},
		{"hash", "--save-choice", strings.Repeat("f", 64), "--keeper", "2", "1"},
	} {
		code, raw, stderr := f.run(ctx, append(args, "--json")...)
		hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_preview_unavailable", 1)
	}
	code, raw, stderr := f.run(ctx, "hashes", "--choice", "hash-choice-v1-"+strings.Repeat("f", 64), "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hashes", "not_found", 1)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	code, raw, stderr = f.run(canceled, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1", "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) || len(hashChoiceIDs(t, f.base)) != 0 {
		t.Fatal("refused or canceled choice modified storage/source")
	}
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			args := []string{"--data-dir", f.base, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1"}
			if machine {
				args = append(args, "--json")
			}
			var diagnostic bytes.Buffer
			code = Run(ctx, args, hashFailWriter{short: short}, &diagnostic)
			ids := hashChoiceIDs(t, f.base)
			if code != 1 || len(ids) != 1 || !strings.Contains(diagnostic.String(), ids[0]) || !strings.Contains(diagnostic.String(), "saved") || !strings.Contains(diagnostic.String(), "hashes --choice") || strings.Contains(diagnostic.String(), "nothing saved") {
				t.Fatal("post-publication output failure lost exact recovery ID", code, diagnostic.String(), ids)
			}
			code, raw, stderr = f.run(ctx, "hashes", "--choice", ids[0], "--json")
			hashChoiceCLIReport(t, code, raw, stderr, "hashes")
		}
	}
}

func TestHashKeeperChoiceCLIOutputCancellationRetainsPublishedID(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	for _, machine := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		args := []string{"--data-dir", f.base, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1"}
		if machine {
			args = append(args, "--json")
		}
		code := Run(ctx, args, out, &stderr)
		cancel()
		ids := hashChoiceIDs(t, f.base)
		if code != 1 || len(ids) != 1 || !strings.Contains(stderr.String(), ids[0]) || !strings.Contains(stderr.String(), "saved") || !strings.Contains(stderr.String(), context.Canceled.Error()) || !strings.Contains(stderr.String(), "hashes --choice") || strings.Contains(stderr.String(), "nothing saved") {
			t.Fatal("output cancellation lost a published choice", machine, code, out.String(), stderr.String(), ids)
		}
		if machine {
			// A complete success envelope already reached the writer. Cancellation
			// changes the exit status and stderr, never emits a second JSON object.
			d := json.NewDecoder(strings.NewReader(out.String()))
			var envelope map[string]any
			if err := d.Decode(&envelope); err != nil || envelope["ok"] != true || envelope["command"] != "hash" {
				t.Fatal("published JSON reply was not a complete success envelope", out.String(), err)
			}
			if err := d.Decode(&envelope); !errors.Is(err, io.EOF) {
				t.Fatal("output cancellation emitted multiple JSON envelopes", out.String(), err)
			}
		} else if !strings.Contains(out.String(), "HISTORICAL CHOICE SAVED - CLEANUP UNAVAILABLE") {
			t.Fatal("successful text output lost saved choice status", out.String())
		}
		code, raw, diagnostic := f.run(context.Background(), "hashes", "--choice", ids[0], "--json")
		hashChoiceCLIReport(t, code, raw, diagnostic, "hashes")
	}
}

type hashChoiceCancelWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
	first  bool
}

func (w *hashChoiceCancelWriter) Write(p []byte) (int, error) {
	if !w.first {
		w.first = true
		w.cancel()
	}
	return w.Buffer.Write(p)
}

func TestHashKeeperChoiceCLIUnselectedReservationDoesNotRecover(t *testing.T) {
	ctx := context.Background()
	f, proposal, contents := hashKeeperFourCLIFixture(t)
	consent := hashKeeperApprove(t, f, proposal)
	for _, id := range []string{"1", "2"} {
		hashKeeperRun(t, f, consent, id)
	}
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TRIGGER fail_choice_cli_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(ctx, "hash", "--run", consent.ID, "--json")
	hashProposalFailure(t, code, raw, stderr, "hash_recovery_required", 1)
	if _, err = db.Exec("DROP TRIGGER fail_choice_cli_fixture"); err != nil {
		t.Fatal(err)
	}
	code, beforeDefault, stderr := f.run(ctx, "hashes", "--json")
	snapshot := hashCLIReport(t, code, beforeDefault, stderr)
	if snapshot.Work[2].Status != "running" || snapshot.Work[2].LatestAttempt == nil || snapshot.Work[2].LatestAttempt.Status != "reserved" || snapshot.Work[2].LatestAttempt.ReadBytes != nil {
		t.Fatal("fixture lost its unsettled reservation", beforeDefault)
	}
	sourceBefore := hashCLIBytes(t, f.root, f.source)
	code, raw, stderr = f.run(ctx, "hash", "--save-choice", proposal.SelectionID, "--keeper", "2", "1", "--json")
	saved := hashChoiceCLIReport(t, code, raw, stderr, "hash")
	r := saved.Record.Evidence
	if r.SelectedWork != 4 || r.CompletedObservations != 2 || r.UnfinishedWork != 2 || r.UnmatchedCompletedObservations != 0 || !reflect.DeepEqual(r.Budget, snapshot.Budget) || !reflect.DeepEqual(r.ReadConsent, snapshot.ReadConsent) || r.Budget.TotalReservedBytes != 3*int64(len(contents)) || r.Budget.TotalReadBytes != 2*int64(len(contents)) || r.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("saving recovered reserved work or invented known usage", raw)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--choice", saved.ID, "--json")
	if reopened := hashChoiceCLIReport(t, code, raw, stderr, "hashes"); !reflect.DeepEqual(reopened, saved) {
		t.Fatal("reopening a choice changed its historical reservation context", raw)
	}
	code, afterDefault, stderr := f.run(ctx, "hashes", "--json")
	hashCLIReport(t, code, afterDefault, stderr)
	if afterDefault != beforeDefault || !reflect.DeepEqual(sourceBefore, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("choice writer/reader recovered an attempt or changed source/inventory")
	}
}

func TestHashKeeperChoiceCLIRawPaths(t *testing.T) {
	for _, filename := range []string{"raw\x1b\t雪\"", "raw\xff\x1b\t雪"} {
		t.Run(fmt.Sprintf("%x", []byte(filename)), func(t *testing.T) {
			f := newHashProposalCLIFixtureWithFilename(t, filename)
			if err := os.Remove(f.base + "/config.toml"); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
			proposal := hashCLIProposal(t, code, raw, stderr)
			consent := hashKeeperApprove(t, f, proposal)
			for _, work := range []string{"1", "2"} {
				hashKeeperRun(t, f, consent, work)
			}
			hashChoiceOffline(t, f)
			before := hashCLIBytes(t, f.root+".offline", f.source+".offline")
			code, raw, stderr = f.run(context.Background(), "hash", "--save-choice", proposal.SelectionID, "--keeper", "2", "1", "--json")
			saved := hashChoiceCLIReport(t, code, raw, stderr, "hash")
			for _, member := range []inventory.SavedHashPreviewMember{saved.Record.Evidence.Keeper, saved.Record.Evidence.Copies[0]} {
				found := false
				for _, target := range proposal.Targets {
					found = found || target.File.ID == member.FileID && bytes.Equal(target.File.PathBytes, member.PathBytes)
				}
				if !found {
					t.Fatal("saved choice changed authoritative path bytes", raw)
				}
			}
			code, human, stderr := f.run(context.Background(), "hashes", "--choice", saved.ID)
			if code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "nested\nfolder") {
				t.Fatal("saved choice printed unquoted raw controls", code, human, stderr)
			}
			for _, target := range proposal.Targets {
				if !strings.Contains(human, fmt.Sprintf("%q", string(target.File.PathBytes))) {
					t.Fatal("saved human choice lost exact quoted path", human)
				}
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root+".offline", f.source+".offline")) {
				t.Fatal("choice save/show changed offline source or inventory")
			}
		})
	}
}

func TestGuidedHashChoiceExplicitSaveBackAndReadOnlyDefault(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.root+".offline", f.source+".offline")
	code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader("1\n2\n1\nsave\n"))
	if code != 0 || stderr != "" || strings.Contains(output, "Type save") || len(hashChoiceIDs(t, f.base)) != 0 {
		t.Fatal("default guided hash review became writable", code, output, stderr)
	}
	// A final back clears roles and returns to the same frozen member rows.
	input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n", "back\n", "3\n", "2,1\n", "yes\n", "save\n"}}
	code, output, stderr = runHashGuidedReview(t, context.Background(), f.base, input, "--save-choice")
	ids := hashChoiceIDs(t, f.base)
	if code != 0 || stderr != "" || input.index != 8 || len(ids) != 1 || strings.Count(output, "Type save") != 3 || !strings.Contains(output, "Nothing saved. Enter save") || !strings.Contains(output, "HISTORICAL CHOICE SAVED - CLEANUP UNAVAILABLE") || !strings.Contains(output, "hashes --choice "+ids[0]) {
		t.Fatal("guided saving accepted an implicit response or failed explicit back/save", code, output, stderr, input.index, ids)
	}
	code, raw, stderr := f.run(context.Background(), "hashes", "--choice", ids[0], "--json")
	saved := hashChoiceCLIReport(t, code, raw, stderr, "hashes")
	if saved.Record.Evidence.Keeper.WorkID != "3" || len(saved.Record.Evidence.Copies) != 2 || saved.Record.Evidence.Copies[0].WorkID != "2" || saved.Record.Evidence.Copies[1].WorkID != "1" || saved.Record.Evidence.SelectedWork != 4 || saved.Record.Evidence.CompletedObservations != 4 || saved.Record.Evidence.UnmatchedCompletedObservations != 1 || !reflect.DeepEqual(before, hashCLIBytes(t, f.root+".offline", f.source+".offline")) {
		t.Fatal("guided save remapped roles, added paths or lost whole context", raw)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, commandPrefix(paths)+" hashes --choice "+ids[0]) {
		t.Fatal("guided save lost global private data directory", output)
	}
}

func TestGuidedHashChoiceConfirmationInputAndOutputBoundaries(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	before := hashCLIBytes(t, f.base, f.root)
	for _, response := range []string{"", "save", "quit\n", "\n", "yes\n", "save "} {
		code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader("1\n2\n1\n"+response), "--save-choice")
		if code != 0 || stderr != "" || strings.Contains(output, "HISTORICAL CHOICE SAVED") || !strings.Contains(output, "No decisions were saved") || len(hashChoiceIDs(t, f.base)) != 0 {
			t.Fatal("EOF, partial save or non-save confirmed a choice", response, code, output, stderr)
		}
	}
	for _, response := range []string{strings.Repeat("s", reviewInputLimit+1) + "\n", "save\n"} {
		ctx, cancel := context.WithCancel(context.Background())
		input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n", response}, before: func(index int) {
			if response == "save\n" && index == 3 {
				cancel()
			}
		}}
		code, output, stderr := runHashGuidedReview(t, ctx, f.base, input, "--save-choice")
		cancel()
		if code != 1 || stderr == "" || strings.Contains(output, "HISTORICAL CHOICE SAVED") || len(hashChoiceIDs(t, f.base)) != 0 {
			t.Fatal("oversized or canceled save was published", code, output, stderr)
		}
	}
	for _, short := range []bool{false, true} {
		out := &hashReviewFailAtWriter{match: "Type save", short: short}
		input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n", "save\n"}, before: func(index int) {
			if index == 3 {
				t.Fatal("failed save prompt consumed confirmation")
			}
		}}
		var stderr bytes.Buffer
		code := runWithInput(context.Background(), []string{"--data-dir", f.base, "review", "--hashes", "--save-choice"}, input, out, &stderr)
		if code != 1 || stderr.Len() == 0 || input.index != 3 || !out.failed || out.after != 0 || len(hashChoiceIDs(t, f.base)) != 0 {
			t.Fatal("output failure before confirmation published a choice", code, stderr.String(), input.index)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("unconfirmed save changed original fixture/storage bytes")
	}
	for _, short := range []bool{false, true} {
		out := &hashReviewFailAtWriter{match: "HISTORICAL CHOICE SAVED", short: short}
		var stderr bytes.Buffer
		code := runWithInput(context.Background(), []string{"--data-dir", f.base, "review", "--hashes", "--save-choice"}, strings.NewReader("1\n2\n1\nsave\n"), out, &stderr)
		ids := hashChoiceIDs(t, f.base)
		if code != 1 || len(ids) != 1 || !strings.Contains(stderr.String(), ids[0]) || !strings.Contains(stderr.String(), "saved") || strings.Contains(stderr.String(), "nothing saved") || out.after != 0 {
			t.Fatal("postcommit guided output lost saved ID or asserted no save", code, stderr.String(), ids)
		}
	}
}

func TestGuidedHashChoiceReplacedStorageAndSelectedEvidenceAtSave(t *testing.T) {
	for _, change := range []string{"removed", "replaced", "observation_changed"} {
		t.Run(change, func(t *testing.T) {
			f := newHashGuidedReviewFixture(t, 3)
			var after map[string][32]byte
			input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n", "save\n"}, before: func(index int) {
				if index != 3 {
					return
				}
				if change == "observation_changed" {
					db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
					if err != nil {
						t.Fatal(err)
					}
					// Change a selected completed work sequence after display. The
					// exact proposal and reusable work ordinals remain unchanged.
					_, err = db.Exec("UPDATE hash_work SET sequence=sequence+1 WHERE id=2")
					if closeErr := db.Close(); err != nil || closeErr != nil {
						t.Fatal(err, closeErr)
					}
				} else {
					if err := os.Rename(filepath.Join(f.base, "hashes"), filepath.Join(f.base, "old-hashes")); err != nil {
						t.Fatal(err)
					}
					if change == "replaced" {
						writer, err := inventory.OpenHashWriter(context.Background(), f.base)
						if err != nil {
							t.Fatal(err)
						}
						if err = writer.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				after = hashCLIBytes(t, f.base, f.root)
			}}
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, input, "--save-choice")
			if code != 1 || stderr == "" || strings.Contains(output, "HISTORICAL CHOICE SAVED") || !reflect.DeepEqual(after, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("saving remapped a displayed role or changed refused storage", change, code, output, stderr)
			}
			if change == "removed" {
				if _, err := os.Lstat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("choice writer recreated removed storage", err)
				}
			} else if len(hashChoiceIDs(t, f.base)) != 0 {
				t.Fatal("changed evidence published a choice")
			}
		})
	}
}

func TestGuidedHashChoiceFlagsAndJSONPreserveBooleanParsing(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	input := &guidedReviewLineReader{lines: []string{"save\n"}, before: func(int) { t.Fatal("invalid flags read confirmation") }}
	for _, flags := range [][]string{
		{"--save-choice"}, {"--save-choice=false"}, {"--hashes", "--save-choice=false"},
		{"--hashes", "--save-choice", "--save-choice"}, {"--hashes", "--save-choice", "-save-choice"},
		{"--hashes", "--save-choice=bad"}, {"--hashes", "--save-choice", "-d", "/generated/missing"},
		{"--hashes", "--save-choice", "--min-age-days", "30"}, {"--hashes", "--save-choice", "1"},
	} {
		var out, stderr bytes.Buffer
		code := runWithInput(context.Background(), append([]string{"--data-dir", base, "review"}, flags...), input, &out, &stderr)
		if code != 2 || out.Len() != 0 || stderr.Len() == 0 {
			t.Fatal("invalid guided choice options reached storage", flags, code, out.String(), stderr.String())
		}
	}
	for _, dataFlag := range []string{"--data-dir", "-data-dir"} {
		for _, saveFlag := range []string{"--save-choice", "-save-choice", "--save-choice=true", "-save-choice=true"} {
			var out, stderr bytes.Buffer
			args := []string{dataFlag, base, "review", "--hashes", saveFlag, "--json"}
			code := runWithInput(context.Background(), args, input, &out, &stderr)
			hashChoiceCLIError(t, code, out.String(), stderr.String(), "review", "unsupported_output", 2)
		}
	}
	if input.index != 0 {
		t.Fatal("invalid guided save consumed input", input.index)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("guided flag or JSON refusal initialized storage", err)
	}
}

func TestGuidedHashChoiceNativePartialSaveCancelsWithoutClosingInput(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	if _, err = writeEnd.WriteString("1\n2\n1\nsave"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &hashChoicePipePromptWriter{ready: make(chan struct{})}
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runWithInput(ctx, []string{"--data-dir", f.base, "review", "--hashes", "--save-choice"}, readEnd, out, &stderr)
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("native pipe did not reach save confirmation")
	}
	select {
	case code := <-done:
		t.Fatal("unterminated save was accepted", code)
	case <-time.After(120 * time.Millisecond):
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || !strings.Contains(stderr.String(), context.Canceled.Error()) || len(hashChoiceIDs(t, f.base)) != 0 || strings.Contains(out.String(), "HISTORICAL CHOICE SAVED") {
			t.Fatal("partial native save could not cancel without publication", code, out.String(), stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("native save prompt did not release on cancellation")
	}
	if _, err = readEnd.Stat(); err != nil {
		t.Fatal("guided save closed caller input", err)
	}
}

type hashChoicePipePromptWriter struct {
	bytes.Buffer
	ready chan struct{}
	sent  bool
}

func (w *hashChoicePipePromptWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if !w.sent && strings.Contains(string(p), "Type save") {
		w.sent = true
		close(w.ready)
	}
	return n, err
}
