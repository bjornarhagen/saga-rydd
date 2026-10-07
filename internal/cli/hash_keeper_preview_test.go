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
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashKeeperCLIReport(t *testing.T, code int, raw, stderr string) inventory.HashKeeperPreview {
	t.Helper()
	if inventory.HashKeeperPreviewContract != "historical_keeper_preview_v1" {
		t.Fatal("keeper preview contract changed", inventory.HashKeeperPreviewContract)
	}
	var envelope struct {
		Version int                         `json:"api_version"`
		OK      bool                        `json:"ok"`
		Command string                      `json:"command"`
		Hashes  inventory.HashKeeperPreview `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hashes" {
		t.Fatal(code, raw, stderr, err)
	}
	r := envelope.Hashes
	if r.Source != "saved_hash_observations" || r.Contract != inventory.HashKeeperPreviewContract || r.HashContract != inventory.FileHashContract || r.Scope != "explicit_saved_subset" || r.BudgetScope != "whole_saved_selection" || len(r.Copies) == 0 || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("keeper preview widened scope, verification or cleanup authority", raw)
	}
	if r.ReadConsent != nil && r.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("historical preview evaluated current read permission", raw)
	}
	for _, claim := range []string{`"approval_available":false`, `"provenance_verified":false`, `"content_verified":false`, `"current_state_verified":false`, `"duplicates_verified":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !strings.Contains(raw, claim) {
			t.Fatal("preview omitted a required uncertainty/authority field", claim, raw)
		}
	}
	return r
}

func hashKeeperCLIError(t *testing.T, code int, raw, stderr, want string, exit int) {
	t.Helper()
	hashGroupsCLIError(t, code, raw, stderr, want, exit)
	for _, forbidden := range []string{`"keeper":`, `"copies":`, `"observation_sequence":`, `"approval_available":`} {
		if strings.Contains(raw, forbidden) {
			t.Fatal("failed keeper preview exposed partial historical roles", raw)
		}
	}
}

// Extend a disposable production scan before its first immutable selection.
// Each name fixes a work ordinal through an explicit caller-ordered selection.
func hashKeeperRescanAndSelect(t *testing.T, f *hashProposalCLIFixture, names []string) inventory.HashProposal {
	t.Helper()
	ctx := context.Background()
	code, raw, stderr := f.run(ctx, "scan", "-d", f.root, "--now", "--detailed", "--json")
	if code != 0 || stderr != "" {
		t.Fatal(code, raw, stderr)
	}
	code, raw, stderr = f.run(ctx, "report", "--same-size", "-d", f.root, "--min-size-bytes", "1", "--json")
	var envelope hashReportEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || envelope.Report.SameSize == nil || len(envelope.Report.SameSize.Bands) != 1 || len(envelope.Report.SameSize.Bands[0].Files) != len(names) {
		t.Fatal("extended fixture did not produce its exact saved band", code, raw, stderr, err)
	}
	f.page, f.input = *envelope.Report.SameSize, []byte(raw)
	if err := os.WriteFile(f.report, f.input, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--json", "hash", "--select", "-d", f.root, "--from", f.report}
	for _, name := range names {
		found := false
		for _, file := range f.page.Bands[0].Files {
			if filepath.Base(string(file.PathBytes)) == name {
				args = append(args, strconv.FormatInt(file.ID, 10))
				found = true
				break
			}
		}
		if !found {
			t.Fatal("fixture selection name is missing", name)
		}
	}
	code, raw, stderr = f.run(ctx, args...)
	return hashCLIProposal(t, code, raw, stderr)
}

func hashKeeperFourCLIFixture(t *testing.T) (*hashProposalCLIFixture, inventory.HashProposal, []byte) {
	t.Helper()
	f := newHashProposalCLIFixture(t)
	if err := os.Remove(f.base + "/config.toml"); err != nil {
		t.Fatal(err)
	}
	contents := bytes.Repeat([]byte("generated"), 17)
	parent := filepath.Dir(string(f.page.Bands[0].Files[0].PathBytes))
	for name, data := range map[string][]byte{"another equal": contents, "different": bytes.Repeat([]byte{'x'}, len(contents))} {
		if err := os.WriteFile(filepath.Join(parent, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	p := hashKeeperRescanAndSelect(t, f, []string{"peer", "quote\"雪", "another equal", "different"})
	return f, p, contents
}

func hashKeeperApprove(t *testing.T, f *hashProposalCLIFixture, p inventory.HashProposal) inventory.HashReadConsent {
	t.Helper()
	code, raw, stderr := f.run(context.Background(), hashReadApproveArgs(p.SelectionID, 8192, 16384)...)
	return hashCLIConsentResult(t, code, raw, stderr, "approve")
}

func hashKeeperRun(t *testing.T, f *hashProposalCLIFixture, c inventory.HashReadConsent, work string) {
	t.Helper()
	code, raw, stderr := f.run(context.Background(), "hash", "--run", c.ID, "--json")
	r := hashCLIStepResult(t, code, raw, stderr)
	if r.Result.WorkID != work || r.Result.Status != "hash_observed" {
		t.Fatal("keeper fixture did not record the expected full hash", raw)
	}
}

func TestHashesKeeperPreviewCLIExplicitSubsetOfflineAndUnchangedReports(t *testing.T) {
	ctx := context.Background()
	f, p, contents := hashKeeperFourCLIFixture(t)
	c := hashKeeperApprove(t, f, p)
	for _, work := range []string{"1", "2", "3", "4"} {
		hashKeeperRun(t, f, c, work)
	}
	code, raw, stderr := f.run(ctx, "hash", "--revoke", c.ID, "--json")
	c = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	code, defaultRaw, stderr := f.run(ctx, "hashes", "--json")
	ordinary := hashCLIReport(t, code, defaultRaw, stderr)
	code, groupsRaw, stderr := f.run(ctx, "hashes", "--groups", "--json")
	groups := hashGroupsCLIReport(t, code, groupsRaw, stderr)
	code, raw, stderr = f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "1", "--json")
	oneCopy := hashKeeperCLIReport(t, code, raw, stderr)
	if oneCopy.Keeper.WorkID != "2" || len(oneCopy.Copies) != 1 || oneCopy.Copies[0].WorkID != "1" || oneCopy.SelectedWork != 4 || oneCopy.CompletedObservations != 4 || oneCopy.UnfinishedWork != 0 || oneCopy.UnmatchedCompletedObservations != 1 || !reflect.DeepEqual(oneCopy.Budget, ordinary.Budget) || !reflect.DeepEqual(oneCopy.ReadConsent, ordinary.ReadConsent) {
		t.Fatal("preview added the third matching path or lost whole coverage", raw)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "3", "1", "--json")
	r := hashKeeperCLIReport(t, code, raw, stderr)
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(contents))
	if r.StoreID != p.StoreID || r.SelectionID != p.SelectionID || r.InventoryID != p.InventoryID || r.LogicalBytes != int64(len(contents)) || r.SHA256 != wantSHA || r.Keeper.WorkID != "2" || len(r.Copies) != 2 || r.Copies[0].WorkID != "3" || r.Copies[1].WorkID != "1" || r.SelectedWork != 4 || r.CompletedObservations != 4 || r.UnfinishedWork != 0 || r.UnmatchedCompletedObservations != 1 || !reflect.DeepEqual(r.Budget, ordinary.Budget) || !reflect.DeepEqual(r.ReadConsent, ordinary.ReadConsent) || r.ReadConsent == nil || !reflect.DeepEqual(*r.ReadConsent, c) || r.Budget.TotalReservedBytes != 4*int64(len(contents)) {
		t.Fatal("preview remapped explicit roles or filtered whole-selection evidence", raw)
	}
	for _, member := range append([]inventory.SavedHashPreviewMember{r.Keeper}, r.Copies...) {
		ordinal, err := strconv.Atoi(member.WorkID)
		if err != nil {
			t.Fatal(err)
		}
		file, observation := p.Targets[ordinal-1].File, ordinary.Work[ordinal-1]
		if member.FileID != file.ID || member.RootID != file.RootID || !bytes.Equal(member.PathBytes, file.PathBytes) || member.Sequence != observation.Sequence || !member.CheckedAt.Equal(observation.CheckedAt) || member.SavedDevice != file.Device || member.SavedInode != file.Inode || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			t.Fatal("preview lost exact historical member evidence", member, file, observation)
		}
	}
	// A distinct, unmatched completed file is outside this requested subset.
	if bytes.Equal(r.Keeper.PathBytes, p.Targets[3].File.PathBytes) || len(groups.Groups) != 1 || len(groups.Groups[0].Members) != 3 {
		t.Fatal("fixture does not prove subset roles within whole coverage", raw, groupsRaw)
	}
	before := hashCLIBytes(t, f.base, f.root)
	for _, args := range [][]string{
		{"--preview", strings.Repeat("f", 64), "--keeper", "2", "1"},
		{"--preview", p.SelectionID, "--keeper", "20", "1"},
		{"--preview", p.SelectionID, "--keeper", "2", "20"},
		{"--preview", p.SelectionID, "--keeper", "2", "4"},
	} {
		code, raw, stderr = f.run(ctx, append([]string{"--json", "hashes"}, args...)...)
		hashKeeperCLIError(t, code, raw, stderr, "hash_preview_unavailable", 1)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("refused roles changed observations or source bytes")
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
	before = hashCLIBytes(t, f.base, f.root+".offline")
	for _, args := range [][]string{
		{"--preview", p.SelectionID, "--keeper", "2", "3", "1"},
		{"-preview=" + p.SelectionID, "-keeper=2", "3", "1"},
	} {
		code, raw, stderr = f.run(ctx, append([]string{"--json", "hashes"}, args...)...)
		if shown := hashKeeperCLIReport(t, code, raw, stderr); !reflect.DeepEqual(shown, r) {
			t.Fatal("offline preview depended on sources, inventory or invalid config", raw)
		}
	}
	code, human, stderr := f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "3", "1")
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"HISTORICAL KEEPER PREVIEW - CURRENT FILES NOT CHECKED", "Possible keeper", "Possible copy for review", "No decision was saved", "Approval Unavailable", "Reclaimable space Unknown", wantSHA} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal(want, code, human, stderr)
		}
	}
	for _, member := range append([]inventory.SavedHashPreviewMember{r.Keeper}, r.Copies...) {
		if !strings.Contains(human, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("historical role path was not quoted exactly", human)
		}
	}
	if strings.Contains(human, "nested\nfolder") || strings.ContainsRune(human, '\x1b') || !utf8.ValidString(human) {
		t.Fatal("preview printed unquoted path controls", human)
	}
	code, afterDefault, stderr := f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, afterDefault, stderr); !reflect.DeepEqual(after, ordinary) || afterDefault != defaultRaw {
		t.Fatal("preview changed default hashes schema or records", afterDefault)
	}
	code, afterGroups, stderr := f.run(ctx, "hashes", "--groups", "--json")
	if after := hashGroupsCLIReport(t, code, afterGroups, stderr); !reflect.DeepEqual(after, groups) || afterGroups != groupsRaw {
		t.Fatal("preview changed groups schema or records", afterGroups)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline")) {
		t.Fatal("offline preview changed database, config or source bytes")
	}
	for _, unavailable := range []string{f.root, f.source} {
		if _, err := os.Lstat(unavailable); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("preview initialized unavailable source/inventory", unavailable, err)
		}
	}
}

func TestHashesKeeperPreviewCLILegacyPartialAndCompleted(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	code, raw, stderr := f.run(ctx, "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	args := []string{"hashes", "--preview", saved.SelectionID, "--keeper", "1", "2", "--json"}
	for i := 0; i < 2; i++ {
		if i == 1 {
			if _, err := f.store.RunNext(ctx, f.source, f.scanner, 64, 4096); err != nil {
				t.Fatal(err)
			}
		}
		before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
		code, raw, stderr = f.run(ctx, args...)
		hashKeeperCLIError(t, code, raw, stderr, "hash_preview_unavailable", 1)
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
			t.Fatal("pending/partial preview advanced work or changed bytes")
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := f.store.RunNext(ctx, f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
			t.Fatal(err)
		}
	}
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	code, raw, stderr = f.run(ctx, args...)
	r := hashKeeperCLIReport(t, code, raw, stderr)
	if r.ReadConsent != nil || r.SelectedWork != 2 || r.CompletedObservations != 2 || r.UnfinishedWork != 0 || r.SHA256 != fmt.Sprintf("%x", sha256.Sum256(f.contents)) || r.Keeper.WorkID != "1" || r.Copies[0].WorkID != "2" || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("completed legacy evidence could not form a read-only preview", raw)
	}
}

func TestHashesKeeperPreviewCLIUnselectedReservedWorkDoesNotRecover(t *testing.T) {
	ctx := context.Background()
	f, p, contents := hashKeeperFourCLIFixture(t)
	c := hashKeeperApprove(t, f, p)
	for _, work := range []string{"1", "2"} {
		hashKeeperRun(t, f, c, work)
	}
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TRIGGER fail_keeper_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(ctx, "hash", "--run", c.ID, "--json")
	hashProposalFailure(t, code, raw, stderr, "hash_recovery_required", 1)
	if _, err = db.Exec("DROP TRIGGER fail_keeper_fixture"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	if saved.Work[2].Status != "running" || saved.Work[2].LatestAttempt == nil || saved.Work[2].LatestAttempt.Status != "reserved" || saved.Work[2].LatestAttempt.ReadBytes != nil {
		t.Fatal("fixture did not retain an unsettled reservation", raw)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "1", "--json")
	r := hashKeeperCLIReport(t, code, raw, stderr)
	if r.SelectedWork != 4 || r.CompletedObservations != 2 || r.UnfinishedWork != 2 || r.UnmatchedCompletedObservations != 0 || !reflect.DeepEqual(r.Budget, saved.Budget) || !reflect.DeepEqual(r.ReadConsent, saved.ReadConsent) || r.Budget.TotalReservedBytes != 3*int64(len(contents)) || r.Budget.TotalReadBytes != 2*int64(len(contents)) || r.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("preview narrowed whole coverage, invented usage or recovered work", raw)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, raw, stderr); !reflect.DeepEqual(after, saved) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("historical preview reconciled work or changed saved/source bytes", raw)
	}
}

func TestHashesKeeperPreviewCLIAliasOutsideSubsetIsAmbiguous(t *testing.T) {
	ctx := context.Background()
	f := newHashProposalCLIFixture(t)
	if err := os.Remove(f.base + "/config.toml"); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(string(f.page.Bands[0].Files[0].PathBytes))
	if err := os.Link(filepath.Join(parent, "peer"), filepath.Join(parent, "unselected alias")); err != nil {
		t.Fatal(err)
	}
	p := hashKeeperRescanAndSelect(t, f, []string{"peer", "quote\"雪", "unselected alias"})
	c := hashKeeperApprove(t, f, p)
	for _, work := range []string{"1", "2"} {
		hashKeeperRun(t, f, c, work)
	}
	code, raw, stderr := f.run(ctx, "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	if saved.Work[0].Status != "complete" || saved.Work[1].Status != "complete" || saved.Work[2].Status != "pending" {
		t.Fatal("alias fixture does not exercise an unrequested unfinished row", raw)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "1", "2", "--json")
	hashKeeperCLIError(t, code, raw, stderr, "hash_preview_identity_ambiguous", 1)
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, raw, stderr); !reflect.DeepEqual(after, saved) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("alias refusal changed current/saved evidence", raw)
	}
}

func TestHashesKeeperPreviewCLIArgumentsMissingAndCanceled(t *testing.T) {
	id := strings.Repeat("a", 64)
	base := filepath.Join(t.TempDir(), "absent")
	invalid := [][]string{
		{"--preview"}, {"--keeper", "1", "2"}, {"--preview", id, "2"}, {"--preview", id, "--keeper", "1"},
		{"--preview", "short", "--keeper", "1", "2"}, {"--preview", strings.ToUpper(id), "--keeper", "1", "2"},
		{"--preview", strings.Repeat("g", 64), "--keeper", "1", "2"}, {"--preview", id + "\n", "--keeper", "1", "2"},
		{"--preview", "--json", "--keeper", "1", "2"}, {"--preview", id, "--keeper", "--json", "2"},
		{"--preview", id, "--preview", id, "--keeper", "1", "2"}, {"--preview=" + id, "-preview=" + id, "--keeper", "1", "2"},
		{"--preview", id, "--keeper", "1", "--keeper", "1", "2"}, {"--preview", id, "--keeper=1", "-keeper=1", "2"},
		{"--preview", id, "--keeper", "1", "--groups", "2"}, {"--groups=false", "--preview", id, "--keeper", "1", "2"},
		{"--work", "1", "--preview", id, "--keeper", "1", "2"}, {"--preview", id, "--keeper", "1", "--work=1", "2"},
		{"--preview", id, "--keeper", "1", "1"}, {"--preview", id, "--keeper", "1", "2", "2"},
		{"--preview", id, "--keeper", "01", "2"}, {"--preview", id, "--keeper", "0", "2"}, {"--preview", id, "--keeper", "21", "2"},
		{"--preview", id, "--keeper", "1", "01"}, {"--preview", id, "--keeper", "1", "+2"}, {"--preview", id, "--keeper", "1", "0"},
		{"--preview", id, "--keeper", "1", "21"}, {"--preview", id, "--keeper", "1", "not-a-work-id"},
		{"--preview", id, "--keeper", "1", "2", "--keeper", "3"}, {"--preview", id, "--keeper", "1", "2", "--preview", id},
		{"--preview", id, "--keeper", "1", "2", "--groups"}, {"--preview", id, "--keeper", "1", "--directory", "/fixture", "2"},
	}
	tooMany := []string{"--preview", id, "--keeper", "1"}
	for work := 2; work <= 20; work++ {
		tooMany = append(tooMany, strconv.Itoa(work))
	}
	invalid = append(invalid, append(append([]string{}, tooMany...), "20"))
	for _, args := range invalid {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base, "--json", "hashes"}, args...), &out, &stderr)
		hashKeeperCLIError(t, code, out.String(), stderr.String(), "invalid_arguments", 2)
		if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid preview touched storage", args, err)
		}
	}
	for _, args := range [][]string{
		{"--preview", "--json", "--keeper", "1", "2"},
		{"-preview", "--json", "-keeper", "1", "2"},
		{"--preview", id, "--keeper", "--json", "2"},
		{"-preview", id, "-keeper", "--json", "2"},
	} {
		for _, dataFlag := range []string{"--data-dir", "-data-dir"} {
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{dataFlag, base, "hashes"}, args...), &out, &stderr)
			if code != 2 || out.Len() != 0 || stderr.Len() == 0 || strings.Contains(stderr.String(), `"api_version"`) {
				t.Fatal("literal option value incorrectly enabled JSON output", dataFlag, args, code, out.String(), stderr.String())
			}
			out.Reset()
			stderr.Reset()
			code = Run(context.Background(), append([]string{dataFlag, base, "--json", "hashes"}, args...), &out, &stderr)
			hashKeeperCLIError(t, code, out.String(), stderr.String(), "invalid_arguments", 2)
			if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("literal JSON option value touched storage", dataFlag, args, err)
			}
		}
	}
	// Exactly twenty roles are valid syntax; unavailable storage must refuse
	// without creating a replacement store, rather than rejecting that bound.
	for _, args := range [][]string{{"--preview", id, "--keeper", "1", "2"}, tooMany} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json", "hashes"}, args...), &out, &stderr)
		hashKeeperCLIError(t, code, out.String(), stderr.String(), "not_found", 1)
		if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("missing preview initialized storage", err)
		}
	}
	f := newHashCLIFixture(t)
	code, raw, stderr := f.run(context.Background(), "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr = f.run(ctx, "hashes", "--preview", saved.SelectionID, "--keeper", "1", "2", "--json")
	hashKeeperCLIError(t, code, raw, stderr, "canceled", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("canceled preview changed saved/source bytes")
	}
}

func TestHashesKeeperPreviewCLIRawPaths(t *testing.T) {
	for _, filename := range []string{"raw\x1b\t雪\"", "raw\xff\x1b\t雪"} {
		t.Run(fmt.Sprintf("%x", []byte(filename)), func(t *testing.T) {
			ctx := context.Background()
			f := newHashProposalCLIFixtureWithFilename(t, filename)
			if err := os.Remove(f.base + "/config.toml"); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(ctx, f.selectArgs()...)
			p := hashCLIProposal(t, code, raw, stderr)
			c := hashKeeperApprove(t, f, p)
			for _, work := range []string{"1", "2"} {
				hashKeeperRun(t, f, c, work)
			}
			before := hashCLIBytes(t, f.base, f.root)
			code, raw, stderr = f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "1", "--json")
			r := hashKeeperCLIReport(t, code, raw, stderr)
			found := false
			for _, member := range []inventory.SavedHashPreviewMember{r.Keeper, r.Copies[0]} {
				ordinal, err := strconv.Atoi(member.WorkID)
				if err != nil || !bytes.Equal(member.PathBytes, p.Targets[ordinal-1].File.PathBytes) {
					t.Fatal("preview changed authoritative path bytes", raw, err)
				}
				found = found || filepath.Base(string(member.PathBytes)) == filename
			}
			code, human, stderr := f.run(ctx, "hashes", "--preview", p.SelectionID, "--keeper", "2", "1")
			if !found || code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "nested\nfolder") {
				t.Fatal("raw role paths were dropped or printed unquoted", code, human, stderr)
			}
			for _, member := range []inventory.SavedHashPreviewMember{r.Keeper, r.Copies[0]} {
				if !strings.Contains(human, fmt.Sprintf("%q", string(member.PathBytes))) {
					t.Fatal("human preview changed exact byte-path quoting", human)
				}
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("raw-path preview changed saved/source bytes")
			}
		})
	}
}

func TestHashesKeeperPreviewCLIOutputFailures(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	for i := 0; i < 2; i++ {
		if _, err := f.store.RunNext(ctx, f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
			t.Fatal(err)
		}
	}
	code, raw, stderr := f.run(ctx, "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			args := []string{"--data-dir", f.base, "hashes", "--preview", saved.SelectionID, "--keeper", "1", "2"}
			if machine {
				args = append(args, "--json")
			}
			var diagnostic bytes.Buffer
			code := Run(ctx, args, hashFailWriter{short: short}, &diagnostic)
			if code != 1 || diagnostic.Len() == 0 || (short && !strings.Contains(diagnostic.String(), "short write")) {
				t.Fatal("failed preview output returned success", machine, short, code, diagnostic.String())
			}
		}
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	if after := hashCLIReport(t, code, raw, stderr); !reflect.DeepEqual(after, saved) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("output failures saved roles or changed historical/source evidence", raw)
	}
}

func TestHashesKeeperPreviewCLIPlanBooleanJSONCompatibility(t *testing.T) {
	ctx := context.Background()
	f := reviewCLIFixture(t, true)
	if len(f.saved.Record.Selection.Evidence.Findings) != 1 {
		t.Fatal("plan compatibility fixture did not capture one finding", f.saved)
	}
	id := f.saved.Record.Selection.Evidence.Findings[0].ID
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root+".offline")
	run := func(args []string) PlanPreview {
		t.Helper()
		var out, stderr bytes.Buffer
		code := Run(ctx, args, &out, &stderr)
		var envelope struct {
			Version int         `json:"api_version"`
			OK      bool        `json:"ok"`
			Command string      `json:"command"`
			Plan    PlanPreview `json:"plan"`
		}
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 0 || stderr.Len() != 0 || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "plan" {
			t.Fatal("boolean plan preview consumed the JSON flag", args, code, out.String(), stderr.String(), err)
		}
		r := envelope.Plan
		if r.Mode != "preview" || r.Executable || r.ApprovalAvailable || r.QuarantineReclaimsSpace || r.EstimatedReclaimableBytes != nil || r.Evidence.CurrentStateVerified || len(r.Evidence.Findings) != 1 || r.Evidence.Findings[0].ID != id || r.Evidence.GeneratedAt.IsZero() || r.Evidence.Findings[0].Measurement.GeneratedAt.IsZero() {
			t.Fatal("plan JSON compatibility changed its evidence or authority", out.String())
		}
		// Each saved-only report has fresh report generation times. All other
		// JSON evidence, notes and authority must match across flag placement.
		r.Evidence.GeneratedAt = time.Time{}
		for i := range r.Evidence.Findings {
			r.Evidence.Findings[i].Measurement.GeneratedAt = time.Time{}
		}
		return r
	}
	want := run([]string{"--data-dir", f.base, "plan", "--preview", "-d", f.root, id, "--json"})
	for _, dataFlag := range []string{"--data-dir", "-data-dir"} {
		for _, previewFlag := range []string{"--preview", "-preview"} {
			args := []string{dataFlag, f.base, "plan", previewFlag, "--json", "-d", f.root, id}
			if got := run(args); !reflect.DeepEqual(got, want) {
				t.Fatal("boolean preview or global flag aliases changed plan JSON", args, got, want)
			}
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline")) {
		t.Fatal("plan JSON parsing compatibility changed saved/source bytes")
	}
	if _, err := os.Lstat(f.root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("plan preview initialized its offline source", err)
	}
}
