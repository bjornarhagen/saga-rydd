package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshReadCLIReport(t *testing.T, code int, raw, diagnostic, mode string) inventory.HashFreshReadConsent {
	t.Helper()
	var envelope struct {
		OK      bool                       `json:"ok"`
		Version int                        `json:"api_version"`
		Command string                     `json:"command"`
		Hash    HashFreshReadConsentResult `json:"hash"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(&envelope); err != nil || d.Decode(new(any)) != io.EOF || code != 0 || diagnostic != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hash" || envelope.Hash.Mode != mode {
		t.Fatal("fresh consent lost its one result envelope", code, raw, diagnostic, err)
	}
	c := envelope.Hash.ReadConsent
	a := c.Approval
	if !inventory.ValidHashFreshReadApprovalID(c.ID) || a.Contract != inventory.HashFreshReadApprovalContract || !inventory.ValidHashFreshJobID(a.JobID) || !inventory.ValidHashFreshJobKey(a.JobKey) || !inventory.ValidHashKeeperChoiceFreshRequestID(a.RequestID) || a.StepByteLimit != inventory.FileHashStepByteLimit || !a.ExpiresAt.Equal(a.CreatedAt.Add(24*time.Hour)) || a.InitialTotalReservedBytes != 0 || !a.ConfirmFullFileRead || c.CurrentReadPermissionEvaluated || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.Executable || c.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh consent imported authority or lost fixed bounds", raw)
	}
	return c
}

func hashFreshReadCLIJob(t *testing.T) (hashGuidedReviewFixture, inventory.SavedFreshJob) {
	t.Helper()
	f := newHashGuidedReviewFixture(t, 3)
	c := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	key := "hash-job-key-v1-" + strings.Repeat("a", 64)
	code, raw, diagnostic := f.run(context.Background(), "hash", "--save-choice-job", c.ID, "--job-key", key, "--json")
	return f, hashFreshJobCLIReport(t, code, raw, diagnostic, "save").Job
}

func freshReadCLIApproveArgs(job string) []string {
	return []string{"hash", "--approve-job", job, "--confirm-content-read", "--max-day-bytes", "1024", "--max-total-bytes", "2048", "--json"}
}

func TestHashFreshReadCLIArgumentsBeforeStorageAndJSONValues(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	job := "hash-choice-job-v1-" + strings.Repeat("a", 64)
	approval := "hash-job-read-v1-" + strings.Repeat("b", 64)
	old := strings.Repeat("c", 64)
	bad := [][]string{{"hash", "--approve-job", job}, {"hash", "--approve-job", job, "--confirm-content-read=false", "--max-day-bytes", "1", "--max-total-bytes", "1"}, {"hash", "--show-job-read", old}, {"hash", "--revoke-job", old}, {"hash", "--approve-job", old, "--confirm-content-read", "--max-day-bytes", "1", "--max-total-bytes", "1"}, {"hash", "--show-job-read", strings.ToUpper(approval)}, {"hash", "--show-job-read", approval, "--show-job-read", approval}, {"hash", "--revoke-job", approval, "--confirm-content-read=false"}}
	for _, mode := range [][]string{freshReadCLIApproveArgs(job), {"hash", "--show-job-read", approval}, {"hash", "--revoke-job", approval}} {
		for _, other := range [][]string{{"--show", old}, {"--approve", old}, {"--run", old}, {"--revoke", old}, {"--show-job", job}, {"--new-job-key"}, {"--job-key", "hash-job-key-v1-" + old}, {"--keeper", "1"}, {"-d", "/generated/missing"}, {"--from", "/generated/missing"}, {"1"}} {
			bad = append(bad, append(slices.Clone(mode), other...))
		}
	}
	for _, cap := range []string{"0", "01", "-1", "1125899906842625", "1.0"} {
		args := freshReadCLIApproveArgs(job)
		args[5] = cap
		bad = append(bad, args)
	}
	for _, args := range bad {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &diagnostic)
		hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hash", "invalid_arguments", 2)
	}
	for _, option := range []string{"--approve-job", "-approve-job", "--show-job-read", "-show-job-read", "--revoke-job", "-revoke-job"} {
		for _, args := range [][]string{{"hash", option, "--json"}, {"hash", option + "=--json"}} {
			var out, diagnostic bytes.Buffer
			code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &diagnostic)
			if code != 2 || out.Len() != 0 || diagnostic.Len() == 0 {
				t.Fatal("literal fresh consent value enabled JSON", args, code, out.String(), diagnostic.String())
			}
		}
	}
	for _, args := range [][]string{freshReadCLIApproveArgs(job), {"hash", "--show-job-read", approval, "--json"}, {"hash", "--revoke-job", approval, "--json"}} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &diagnostic)
		hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hash", "not_found", 1)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid/missing fresh consent initialized storage", err)
	}
}

func TestHashFreshReadCLIExactOfflineLifecycleAndOriginalContext(t *testing.T) {
	f, job := hashFreshReadCLIJob(t)
	code, oldReport, diagnostic := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, oldReport, diagnostic)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	sources := hashCLIBytes(t, f.root+".offline", f.source+".offline")
	code, raw, diagnostic := f.run(context.Background(), freshReadCLIApproveArgs(job.ID)...)
	c := hashFreshReadCLIReport(t, code, raw, diagnostic, "approve")
	if c.ID == f.consent.ID || c.Approval.JobID != job.ID || c.Approval.JobKey != job.Record.JobKey || c.Approval.RequestID != job.Record.Request.RequestID || c.Approval.DailyReservedByteLimit != 1024 || c.Approval.LifetimeReservedByteLimit != 2048 {
		t.Fatal("fresh CLI changed immutable job/caps", c)
	}
	for _, args := range [][]string{freshReadCLIApproveArgs(job.ID), {"hash", "--show-job-read", c.ID, "--json"}} {
		mode := "approve"
		if args[1] == "--show-job-read" {
			mode = "show"
		}
		code, raw, diagnostic = f.run(context.Background(), args...)
		shown := hashFreshReadCLIReport(t, code, raw, diagnostic, mode)
		if shown.ID != c.ID || !reflect.DeepEqual(shown.Approval, c.Approval) {
			t.Fatal("exact retry renewed consent", shown, c)
		}
	}
	code, human, diagnostic := f.run(context.Background(), "hash", "--show-job", job.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{c.ID, "SAVED FRESH JOB - PERMISSION NOT EVALUATED", "Fresh read consent:", "Fresh reserved bytes 0 bytes", "Current read permission Not evaluated"} {
		if code != 0 || diagnostic != "" || !strings.Contains(flat, want) {
			t.Fatal("human saved job hid fresh consent", want, code, human, diagnostic)
		}
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	shown := hashFreshJobCLIReport(t, code, raw, diagnostic, "show").Job
	if !reflect.DeepEqual(shown.Record, job.Record) || !reflect.DeepEqual(shown.Work, job.Work) || shown.ReadConsent == nil || shown.ReadConsent.ID != c.ID {
		t.Fatal("fresh consent changed seed/context", raw)
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--revoke-job", c.ID, "--json")
	revoked := hashFreshReadCLIReport(t, code, raw, diagnostic, "revoke")
	if revoked.Status != "revoked" || revoked.Revocation == nil || revoked.Revocation.ApprovalID != c.ID {
		t.Fatal("fresh revocation omitted identity", raw)
	}
	before := hashCLIBytes(t, f.base)
	code, raw, diagnostic = f.run(context.Background(), "hash", "--revoke-job", c.ID, "--json")
	if retry := hashFreshReadCLIReport(t, code, raw, diagnostic, "revoke"); !reflect.DeepEqual(retry, revoked) {
		t.Fatal("revocation retry replaced record", retry, revoked)
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job-read", c.ID, "--json")
	if got := hashFreshReadCLIReport(t, code, raw, diagnostic, "show"); !reflect.DeepEqual(got, revoked) {
		t.Fatal("saved view changed clock/lifecycle", got, revoked)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("saved view or exact revocation retry changed records")
	}
	code, raw, diagnostic = f.run(context.Background(), freshReadCLIApproveArgs(job.ID)...)
	hashChoiceCLIError(t, code, raw, diagnostic, "hash", "read_consent_revoked", 1)
	if !reflect.DeepEqual(sources, hashCLIBytes(t, f.root+".offline", f.source+".offline")) {
		t.Fatal("fresh consent lifecycle changed sources")
	}
	code, after, diagnostic := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, after, diagnostic)
	if after != oldReport {
		t.Fatal("fresh consent altered original work/approval/accounting")
	}
}

func TestHashFreshReadCLIConflictAndHeldWriterSavedView(t *testing.T) {
	f, job := hashFreshReadCLIJob(t)
	code, raw, diagnostic := f.run(context.Background(), freshReadCLIApproveArgs(job.ID)...)
	c := hashFreshReadCLIReport(t, code, raw, diagnostic, "approve")
	args := freshReadCLIApproveArgs(job.ID)
	args[5] = "512"
	before := hashCLIBytes(t, f.base, f.root, f.source)
	code, raw, diagnostic = f.run(context.Background(), args...)
	hashChoiceCLIError(t, code, raw, diagnostic, "hash", "fresh_read_consent_conflict", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root, f.source)) {
		t.Fatal("changed-cap conflict mutated saved evidence")
	}
	w, err := inventory.OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job-read", c.ID, "--json")
	if got := hashFreshReadCLIReport(t, code, raw, diagnostic, "show"); !reflect.DeepEqual(got, c) {
		t.Fatal("saved consent required writer", got, c)
	}
}

func TestHashFreshReadCLICorruptionRefusesPartialConsent(t *testing.T) {
	f, job := hashFreshReadCLIJob(t)
	code, raw, diagnostic := f.run(context.Background(), freshReadCLIApproveArgs(job.ID)...)
	c := hashFreshReadCLIReport(t, code, raw, diagnostic, "approve")
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	// Remove the immutable trigger only in generated corrupt-storage fixtures.
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='hash_fresh_read_approval'")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err = db.Exec("DROP TRIGGER \"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec("UPDATE hash_fresh_read_approval SET payload=? WHERE job_id=?", []byte("{}"), job.ID); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"hash", "--show-job-read", c.ID, "--json"}, {"hash", "--revoke-job", c.ID, "--json"}, freshReadCLIApproveArgs(job.ID)} {
		code, raw, diagnostic = f.run(context.Background(), args...)
		hashChoiceCLIError(t, code, raw, diagnostic, "hash", "fresh_read_consent_invalid", 1)
	}
}

func TestHashFreshReadCLIOutputCancellationPreservesSavedIdentity(t *testing.T) {
	for _, machine := range []bool{false, true} {
		f, job := hashFreshReadCLIJob(t)
		args := freshReadCLIApproveArgs(job.ID)
		if !machine {
			args = args[:len(args)-1]
		}
		args = append([]string{"--data-dir", f.base}, args...)
		var diagnostic bytes.Buffer
		full := &hashMetadataCountErrorWriter{}
		code := Run(context.Background(), args, full, &diagnostic)
		reader, err := inventory.OpenHashReader(context.Background(), f.base)
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.FreshJob(context.Background(), job.ID)
		reader.Close()
		if err != nil || got.ReadConsent == nil {
			t.Fatal("failed reply lost durable consent", got, err)
		}
		id := got.ReadConsent.ID
		if code != 1 || !strings.Contains(diagnostic.String(), id) {
			t.Fatal("failed reply hid exact saved consent", code, diagnostic.String())
		}
		if machine {
			_ = hashFreshReadCLIReport(t, 0, full.String(), "", "approve")
		}
		for _, short := range []bool{false, true} {
			diagnostic.Reset()
			code = Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic)
			if code != 1 || !strings.Contains(diagnostic.String(), id) {
				t.Fatal("short reply hid saved consent", code, diagnostic.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		diagnostic.Reset()
		code = Run(ctx, args, out, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), id) || !strings.Contains(diagnostic.String(), "canceled") {
			t.Fatal("late canceled reply hid consent", code, diagnostic.String())
		}
		if machine {
			_ = hashFreshReadCLIReport(t, 0, out.String(), "", "approve")
		}
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		code, raw, stderr := f.run(ctx, "hash", "--revoke-job", id, "--json")
		hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
		code, raw, stderr = f.run(context.Background(), "hash", "--show-job-read", id, "--json")
		if c := hashFreshReadCLIReport(t, code, raw, stderr, "show"); c.Status != "recorded" || c.Revocation != nil {
			t.Fatal("early cancellation revoked consent", raw)
		}
	}
}

func TestHashFreshReadCLICapabilities(t *testing.T) {
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &diagnostic)
	var value struct {
		Features map[string]bool `json:"features"`
		Codes    []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &value); err != nil || code != 0 || diagnostic.Len() != 0 || !value.Features["fresh_hash_read_consent"] || value.Features["duplicates"] || value.Features["cleanup"] {
		t.Fatal("fresh capabilities overclaimed authority", code, out.String(), diagnostic.String(), err)
	}
	for _, want := range []string{"fresh_read_consent_required", "fresh_read_consent_conflict", "fresh_read_consent_invalid"} {
		if !slices.Contains(value.Codes, want) {
			t.Fatal("missing consent refusal", want)
		}
	}
}
