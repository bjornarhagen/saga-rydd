package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashGroupsCLIReport(t *testing.T, code int, raw, stderr string) inventory.HashGroupsReport {
	t.Helper()
	var envelope struct {
		Version int                        `json:"api_version"`
		OK      bool                       `json:"ok"`
		Command string                     `json:"command"`
		Hashes  inventory.HashGroupsReport `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hashes" {
		t.Fatal(code, raw, stderr, err)
	}
	r := envelope.Hashes
	if r.Source != "saved_hash_observations" || r.Contract != inventory.HashGroupsContract || r.HashContract != inventory.FileHashContract || r.Scope != "whole_saved_selection" || r.BudgetScope != "whole_saved_selection" || r.Groups == nil || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("group report widened scope or verification/cleanup claims", raw)
	}
	if r.ReadConsent != nil && r.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("saved groups evaluated current read permission", raw)
	}
	return r
}

func hashGroupsCLIError(t *testing.T, code int, raw, stderr, want string, exit int) {
	t.Helper()
	var envelope struct {
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.OK || envelope.Command != "hashes" || envelope.Error.Code != want {
		t.Fatal(code, raw, stderr, want, err)
	}
	for _, forbidden := range []string{`"hashes":`, `"groups":`, `"path_bytes":`, `"sha256":`} {
		if strings.Contains(raw, forbidden) {
			t.Fatal("failed groups report exposed partial evidence", raw)
		}
	}
}

func TestHashesGroupsCLICompletedMatchesConsentAndOfflineReadOnly(t *testing.T) {
	ctx := context.Background()
	contents := bytes.Repeat([]byte("equal generated contents"), 9)
	f, proposal := newHashReadCLIFixture(t, contents)
	code, raw, stderr := f.run(ctx, hashReadApproveArgs(proposal.SelectionID, 4096, 8192)...)
	consent := hashCLIConsentResult(t, code, raw, stderr, "approve")
	for _, workID := range []string{"1", "2"} {
		code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
		step := hashCLIStepResult(t, code, raw, stderr)
		if step.Result.WorkID != workID || step.Result.Status != "hash_observed" {
			t.Fatal("fixture did not record complete full hashes", raw)
		}
	}
	code, raw, stderr = f.run(ctx, "hash", "--revoke", consent.ID, "--json")
	consent = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	code, defaultRaw, stderr := f.run(ctx, "hashes", "--json")
	defaultReport := hashCLIReport(t, code, defaultRaw, stderr)
	code, raw, stderr = f.run(ctx, "hashes", "--groups", "--json")
	report := hashGroupsCLIReport(t, code, raw, stderr)
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(contents))
	if report.StoreID != proposal.StoreID || report.SelectionID != proposal.SelectionID || report.InventoryID != proposal.InventoryID || report.SelectedWork != 2 || report.CompletedObservations != 2 || report.UnfinishedWork != 0 || report.UnmatchedCompletedObservations != 0 || len(report.Groups) != 1 || !reflect.DeepEqual(report.Budget, defaultReport.Budget) || !reflect.DeepEqual(report.ReadConsent, defaultReport.ReadConsent) || report.ReadConsent == nil || !reflect.DeepEqual(*report.ReadConsent, consent) || report.Budget.TotalReservedBytes != 2*int64(len(contents)) {
		t.Fatal("matching report lost counts, whole-selection budget or saved consent", raw)
	}
	group := report.Groups[0]
	if group.SHA256 != wantSHA || group.LogicalBytes != int64(len(contents)) || len(group.Members) != 2 || group.SavedIdentities != 2 || group.RepeatedSavedPaths != 0 || group.ConflictingSavedIdentities != 0 {
		t.Fatal("group does not match independently calculated full hashes", raw)
	}
	for i, member := range group.Members {
		file := proposal.Targets[i].File
		work := defaultReport.Work[i]
		if member.WorkID != work.ID || member.FileID != file.ID || member.RootID != file.RootID || !bytes.Equal(member.PathBytes, file.PathBytes) || !member.CheckedAt.Equal(work.CheckedAt) || member.SavedDevice != file.Device || member.SavedInode != file.Inode || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			t.Fatal("group member lost exact saved evidence", member, file, work)
		}
	}
	// The optional mode must not change the default hashes payload schema.
	var envelope struct {
		Hashes map[string]json.RawMessage `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(defaultRaw), &envelope); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{"groups", "hash_contract", "scope", "selected_work", "completed_observations", "unfinished_work", "unmatched_completed_observations"} {
		if _, exists := envelope.Hashes[extra]; exists {
			t.Fatal("groups changed the default hashes schema", extra, defaultRaw)
		}
	}
	if _, exists := envelope.Hashes["work"]; !exists {
		t.Fatal("default hashes lost saved work", defaultRaw)
	}
	if err := os.WriteFile(f.base+"/config.toml", []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root+".offline")
	for _, mode := range []string{"--groups", "-groups", "--groups=true", "-groups=true"} {
		code, raw, stderr = f.run(ctx, "hashes", mode, "--json")
		if shown := hashGroupsCLIReport(t, code, raw, stderr); !reflect.DeepEqual(shown, report) {
			t.Fatal("offline groups depended on sources, inventory or configuration", raw)
		}
	}
	code, human, stderr := f.run(ctx, "hashes", "--groups")
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"HISTORICAL HASH MATCHES - CURRENT FILES NOT CHECKED", wantSHA, "observations need not be simultaneous", "do not prove current duplicate files or safe cleanup", "whole selection", "Current read permission Not evaluated", "No source files or saved records were changed"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal(want, code, human, stderr)
		}
	}
	for _, member := range group.Members {
		if !strings.Contains(human, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("human group lost quoted control/Unicode path", human)
		}
	}
	if strings.Contains(human, "nested\nfolder") || strings.ContainsRune(human, '\x1b') || !utf8.ValidString(human) {
		t.Fatal("human group printed unquoted controls or invalid bytes", human)
	}
	code, afterDefault, stderr := f.run(ctx, "hashes", "--json")
	if shown := hashCLIReport(t, code, afterDefault, stderr); !reflect.DeepEqual(shown, defaultReport) || afterDefault != defaultRaw {
		t.Fatal("groups changed the ordinary saved report", afterDefault)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline")) {
		t.Fatal("saved group reporting changed database, source or configuration bytes")
	}
	for _, unavailable := range []string{f.root, f.source} {
		if _, err := os.Lstat(unavailable); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("saved groups initialized unavailable source/inventory", unavailable, err)
		}
	}
}

func TestHashesGroupsCLIPartialAndUnmatchedCounts(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	for i, allowance := range []int64{64, inventory.FileHashStepByteLimit, inventory.FileHashStepByteLimit} {
		if _, err := f.store.RunNext(ctx, f.source, f.scanner, allowance, 4096); err != nil {
			t.Fatal(err)
		}
		before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
		code, raw, stderr := f.run(ctx, "hashes", "--groups", "--json")
		r := hashGroupsCLIReport(t, code, raw, stderr)
		wantGroups, wantUnmatched := 0, i
		if i == 2 {
			wantGroups, wantUnmatched = 1, 0
		}
		if r.SelectedWork != 2 || r.CompletedObservations != i || r.UnfinishedWork != 2-i || r.UnmatchedCompletedObservations != wantUnmatched || len(r.Groups) != wantGroups || r.ReadConsent != nil {
			t.Fatal("partial prefixes were grouped or coverage counts were overstated", i, raw)
		}
		code, defaultRaw, stderr := f.run(ctx, "hashes", "--json")
		ordinary := hashCLIReport(t, code, defaultRaw, stderr)
		if !reflect.DeepEqual(r.Budget, ordinary.Budget) {
			t.Fatal("groups filtered whole-selection charges", raw, defaultRaw)
		}
		code, human, stderr := f.run(ctx, "hashes", "--groups")
		if code != 0 || stderr != "" || (i < 2 && !strings.Contains(strings.Join(strings.Fields(human), " "), "This does not prove that there are no duplicate files")) {
			t.Fatal("empty groups made a stronger negative claim", code, human, stderr)
		}
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
			t.Fatal("groups reporting progressed work or changed bytes")
		}
	}
}

func TestHashesGroupsCLIExistingEmptyStore(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base = filepath.Join(base, "hash-state")
	w, err := inventory.OpenHashWriter(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	before := hashCLIBytes(t, base)
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hashes", "--groups", "--json"}, &out, &stderr)
	r := hashGroupsCLIReport(t, code, out.String(), stderr.String())
	if r.SelectionID != "" || r.InventoryID != "" || r.SelectedWork != 0 || r.CompletedObservations != 0 || r.UnfinishedWork != 0 || r.UnmatchedCompletedObservations != 0 || len(r.Groups) != 0 || r.Budget != nil || r.ReadConsent != nil || !strings.Contains(out.String(), `"groups":[]`) {
		t.Fatal("empty store invented selection, observations or budget", out.String())
	}
	out.Reset()
	code = Run(context.Background(), []string{"--data-dir", base, "hashes", "--groups"}, &out, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(strings.Join(strings.Fields(out.String()), " "), "No selection is saved") || !reflect.DeepEqual(before, hashCLIBytes(t, base)) {
		t.Fatal("empty saved report changed store or omitted its scope", code, out.String(), stderr.String())
	}
}

func TestHashesGroupsCLIReservedAttemptIsUnfinishedAndNotRecovered(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TRIGGER fail_groups_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.RunNext(ctx, f.source, f.scanner, 64, 4096); !errors.Is(err, inventory.ErrHashRecoveryRequired) {
		t.Fatal("fixture did not leave uncertain reserved work", err)
	}
	if _, err = db.Exec("DROP TRIGGER fail_groups_fixture"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(ctx, "hashes", "--json")
	ordinary := hashCLIReport(t, code, raw, stderr)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	code, raw, stderr = f.run(ctx, "hashes", "--groups", "--json")
	r := hashGroupsCLIReport(t, code, raw, stderr)
	if r.SelectedWork != 2 || r.CompletedObservations != 0 || r.UnfinishedWork != 2 || r.UnmatchedCompletedObservations != 0 || len(r.Groups) != 0 || r.Budget.TotalReservedBytes != 64 || r.Budget.TotalReadBytes != 0 || !reflect.DeepEqual(r.Budget, ordinary.Budget) {
		t.Fatal("reserved work was grouped, refunded or given invented usage", raw)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, raw, stderr); !reflect.DeepEqual(ordinary, after) || after.Work[0].Status != "running" || after.Work[0].LatestAttempt.Status != "reserved" || after.Work[0].LatestAttempt.ReadBytes != nil || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("groups recovered an attempt or changed saved/source bytes", raw)
	}
}

func TestHashesGroupsCLIArgumentsMissingAndCancellationBeforeStorage(t *testing.T) {
	base := filepath.Join(t.TempDir(), "absent")
	for _, args := range [][]string{
		{"--groups", "--work", "1"}, {"--work=1", "--groups"}, {"-groups", "-work=1"},
		{"--groups", "--groups"}, {"--groups=true", "-groups"}, {"-groups=true", "--groups=true"},
		{"--groups=false"}, {"-groups=false"}, {"--groups", "false"}, {"--groups=bad"},
		{"--groups", "--directory", "/fixture"}, {"--groups", "-d", "/fixture"}, {"--groups", "--run"},
		{"--groups", "--limit", "1"}, {"--groups", "--cursor", "fixture"}, {"--groups", "extra"},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base, "--json", "hashes"}, args...), &out, &stderr)
		hashGroupsCLIError(t, code, out.String(), stderr.String(), "invalid_arguments", 2)
		if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid groups arguments touched storage", args, err)
		}
	}
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		}
		var out, stderr bytes.Buffer
		code := Run(ctx, []string{"--data-dir", base, "hashes", "--groups", "--json"}, &out, &stderr)
		cancel()
		want := "not_found"
		if canceled {
			want = "canceled"
		}
		hashGroupsCLIError(t, code, out.String(), stderr.String(), want, 1)
		if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("missing/canceled groups report initialized storage", err)
		}
	}
	f := newHashCLIFixture(t)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	code, raw, stderr := f.run(ctx, "hashes", "--groups", "--json")
	hashGroupsCLIError(t, code, raw, stderr, "canceled", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("canceled groups report changed existing records or sources")
	}
}

func TestHashesGroupsCLIRawBytePaths(t *testing.T) {
	ctx := context.Background()
	f := newHashProposalCLIFixtureWithFilename(t, "raw\xff\x1b\t雪")
	code, raw, stderr := f.run(ctx, f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	if err := os.Remove(f.base + "/config.toml"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr = f.run(ctx, hashReadApproveArgs(p.SelectionID, 4096, 8192)...)
	c := hashCLIConsentResult(t, code, raw, stderr, "approve")
	for i := 0; i < 2; i++ {
		code, raw, stderr = f.run(ctx, "hash", "--run", c.ID, "--json")
		if step := hashCLIStepResult(t, code, raw, stderr); step.Result.Status != "hash_observed" {
			t.Fatal("raw-path fixture did not complete a full read", raw)
		}
	}
	code, raw, stderr = f.run(ctx, "hashes", "--groups", "--json")
	r := hashGroupsCLIReport(t, code, raw, stderr)
	if len(r.Groups) != 1 || len(r.Groups[0].Members) != 2 {
		t.Fatal("raw paths were omitted from equal historical hashes", raw)
	}
	found := false
	for i, member := range r.Groups[0].Members {
		if !bytes.Equal(member.PathBytes, p.Targets[i].File.PathBytes) {
			t.Fatal("raw path bytes changed in machine output", raw)
		}
		found = found || bytes.Contains(member.PathBytes, []byte{0xff})
	}
	if !found {
		t.Fatal("invalid UTF-8 fixture did not reach grouping output", raw)
	}
	code, human, stderr := f.run(ctx, "hashes", "--groups")
	if code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "nested\nfolder") {
		t.Fatal("raw group paths were not safely quoted", code, human, stderr)
	}
	for _, member := range r.Groups[0].Members {
		if !strings.Contains(human, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("human output did not preserve the exact quoted byte path", human)
		}
	}
}

func TestHashesGroupsCLIOutputFailures(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	for i := 0; i < 2; i++ {
		if _, err := f.store.RunNext(ctx, f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
			t.Fatal(err)
		}
	}
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			var diagnostic bytes.Buffer
			args := []string{"--data-dir", f.base, "hashes", "--groups"}
			if machine {
				args = append(args, "--json")
			}
			code := Run(ctx, args, hashFailWriter{short: short}, &diagnostic)
			if code != 1 || diagnostic.Len() == 0 || (short && !strings.Contains(diagnostic.String(), "short write")) {
				t.Fatal("failed group output returned success", machine, short, code, diagnostic.String())
			}
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("group output failures changed records or source bytes")
	}
}
