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

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshJobCLIReport(t *testing.T, code int, raw, stderr, mode string) HashFreshChoiceJobResult {
	t.Helper()
	var envelope struct {
		Version int                      `json:"api_version"`
		Command string                   `json:"command"`
		OK      bool                     `json:"ok"`
		Hash    HashFreshChoiceJobResult `json:"hash"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	err := d.Decode(&envelope)
	if err != nil || d.Decode(new(any)) != io.EOF || code != 0 || stderr != "" || envelope.Version != APIVersion || envelope.Command != "hash" || !envelope.OK || envelope.Hash.Mode != mode {
		t.Fatal("fresh job lost its one standard result envelope", code, raw, stderr, err)
	}
	j := envelope.Hash.Job
	if !inventory.ValidHashFreshJobID(j.ID) || j.Record.Version != 1 || j.Record.Contract != inventory.HashFreshJobContract || j.Record.Status != "unapproved" || !inventory.ValidHashFreshJobKey(j.Record.JobKey) || j.Record.CreatedAt.IsZero() || j.FreshReservedBytes != 0 || j.FreshRequestedBytes != 0 || j.FreshReadBytes != 0 || j.ApprovalAvailable || j.ProvenanceVerified || j.ContentVerified || j.CurrentStateVerified || j.DuplicatesVerified || j.Executable || j.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh job inherited progress or claimed authority", raw)
	}
	if len(j.Work) != len(j.Record.Request.Targets) || len(j.Work) < 2 {
		t.Fatal("fresh job work expanded or lost exact targets", raw)
	}
	for i, work := range j.Work {
		if work.Ordinal != i+1 || work.Role != j.Record.Request.Targets[i].Role || work.HistoricalWorkID != j.Record.Request.Targets[i].Observation.WorkID || work.Status != "pending" || work.Sequence != 0 || work.CheckedOffset != 0 {
			t.Fatal("fresh job remapped roles or imported old progress", raw)
		}
	}
	return envelope.Hash
}

func hashFreshJobCLIIDs(t *testing.T, base string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var exists int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='hash_fresh_job' AND type='table'").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		return nil
	}
	rows, err := db.Query("SELECT id FROM hash_fresh_job ORDER BY id")
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

func TestHashFreshJobCLIArgumentsAndJSONValuesBeforeStorage(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	choice := "hash-choice-v1-" + strings.Repeat("a", 64)
	key := "hash-job-key-v1-" + strings.Repeat("b", 64)
	job := "hash-choice-job-v1-" + strings.Repeat("c", 64)
	bad := [][]string{
		{"hash", "--new-job-key=false"}, {"hash", "--new-job-key", "--new-job-key"},
		{"hash", "--new-job-key", "--job-key", key}, {"hash", "--new-job-key", "1"},
		{"hash", "--save-choice-job", choice}, {"hash", "--save-choice-job", choice, "--job-key", "bad"},
		{"hash", "--save-choice-job", "bad", "--job-key", key},
		{"hash", "--save-choice-job", choice, "--job-key", strings.ToUpper(key)},
		{"hash", "--save-choice-job", choice, "--job-key", key, "--job-key", key},
		{"hash", "--save-choice-job", choice, "--job-key", key, "--save-choice-job", choice},
		{"hash", "--show-job"}, {"hash", "--show-job", "bad"},
		{"hash", "--show-job", strings.ToUpper(job)}, {"hash", "--show-job", job, "--job-key", key},
		{"hash", "--show-job", job, "--show-job", job}, {"hash", "--job-key", key, "--show", strings.Repeat("a", 64)},
		{"hash", "--request-choice", choice, "--job-key", key},
		{"hashes", "--show-job", job}, {"hash", "--show-job", job, "--", "--json"},
	}
	for _, mode := range [][]string{{"--new-job-key"}, {"--save-choice-job", choice, "--job-key", key}, {"--show-job", job}} {
		for _, other := range [][]string{{"--show", strings.Repeat("a", 64)}, {"--select"}, {"--request-choice", choice}, {"--check-choice", choice}, {"--approve", strings.Repeat("a", 64)}, {"--run", strings.Repeat("a", 64)}, {"--revoke", strings.Repeat("a", 64)}, {"-d", "/generated/missing"}, {"--from", "/generated/missing"}, {"--keeper", "1"}, {"--confirm-content-read=false"}, {"--max-day-bytes", "1"}, {"--max-total-bytes", "1"}, {"1"}} {
			bad = append(bad, append(append([]string{"hash"}, mode...), other...))
		}
	}
	for _, args := range bad {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "invalid_arguments", 2)
	}
	for _, option := range []string{"--save-choice-job", "-save-choice-job", "--show-job", "-show-job", "--job-key", "-job-key"} {
		for _, args := range [][]string{{"hash", option, "--json"}, {"hash", option + "=--json"}} {
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &stderr)
			if code != 2 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("literal fresh-job flag value enabled JSON", args, code, out.String(), stderr.String())
			}
		}
	}
	for _, args := range [][]string{{"hash", "--save-choice-job", choice, "--job-key", key}, {"hash", "--show-job", job}} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), "hash", "not_found", 1)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid or missing job initialized storage", err)
	}
}

func TestHashFreshJobCLINewKeyHasNoStorageOrConsent(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hash", "--new-job-key", "--json"}, &out, &stderr)
	var envelope struct {
		OK   bool                  `json:"ok"`
		Hash HashFreshJobKeyResult `json:"hash"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 0 || stderr.Len() != 0 || !envelope.OK || !inventory.ValidHashFreshJobKey(envelope.Hash.JobKey) || envelope.Hash.Contract != "fresh_hash_job_key_v1" || envelope.Hash.Saved || envelope.Hash.ApprovalAvailable || envelope.Hash.Executable {
		t.Fatal("key generation created authority or lost its contract", code, out.String(), stderr.String(), err)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pure key generation opened/initialized storage", err)
	}
	out.Reset()
	stderr.Reset()
	code = Run(context.Background(), []string{"--data-dir", base, "hash", "--new-job-key"}, &out, &stderr)
	flat := strings.Join(strings.Fields(out.String()), " ")
	if code != 0 || stderr.Len() != 0 || !strings.Contains(flat, "FRESH JOB KEY GENERATED - NOTHING SAVED") || !strings.Contains(flat, "Reuse it with the same choice after a failed reply") || !strings.Contains(flat, "No storage or source files were opened") {
		t.Fatal("human key generation omitted retry/no-storage guidance", code, out.String(), stderr.String())
	}
}

func TestHashFreshJobCLIOfflinePublicationRetryAndSeparateContexts(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "3", "1")
	code, raw, stderr := f.run(context.Background(), "hash", "--revoke", f.consent.ID, "--json")
	revoked := hashCLIConsentResult(t, code, raw, stderr, "revoke")
	code, beforeDefault, stderr := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, beforeDefault, stderr)
	code, beforeGroups, stderr := f.run(context.Background(), "hashes", "--groups", "--json")
	_ = hashGroupsCLIReport(t, code, beforeGroups, stderr)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	sourceBefore := hashCLIBytes(t, f.root+".offline", f.source+".offline")
	key := "hash-job-key-v1-" + strings.Repeat("a", 64)
	code, raw, stderr = f.run(context.Background(), "hash", "--save-choice-job", saved.ID, "--job-key", key, "--json")
	first := hashFreshJobCLIReport(t, code, raw, stderr, "save").Job
	if first.Record.JobKey != key || !reflect.DeepEqual(first.Record.Request.HistoricalChoice, saved) || !reflect.DeepEqual(first.Record.OriginalContext.ReadConsent, &revoked) || first.Record.OriginalContext.SelectedWork != 4 || first.Record.OriginalContext.CompletedObservations != 4 || first.Record.OriginalContext.UnfinishedWork != 0 || first.Record.OriginalContext.BudgetScope != "whole_original_selection" {
		t.Fatal("fresh job merged archived context and first-publication context", raw)
	}
	before := hashCLIBytes(t, f.base)
	for _, args := range [][]string{{"hash", "--save-choice-job", saved.ID, "--job-key", key, "--json"}, {"--json", "hash", "-save-choice-job=" + saved.ID, "-job-key=" + key}, {"hash", "--show-job", first.ID, "--json"}, {"hash", "--json", "-show-job=" + first.ID}} {
		mode := "save"
		if slices.Contains(args, "--show-job") || slices.Contains(args, "-show-job="+first.ID) {
			mode = "show"
		}
		code, raw, stderr := f.run(context.Background(), args...)
		if got := hashFreshJobCLIReport(t, code, raw, stderr, mode).Job; !reflect.DeepEqual(got, first) {
			t.Fatal("exact retry or offline reopen refreshed/reset the job", raw)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) || !reflect.DeepEqual(sourceBefore, hashCLIBytes(t, f.root+".offline", f.source+".offline")) || len(hashFreshJobCLIIDs(t, f.base)) != 1 {
		t.Fatal("retry/show changed records or source evidence")
	}
	code, afterDefault, stderr := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, afterDefault, stderr)
	code, afterGroups, stderr := f.run(context.Background(), "hashes", "--groups", "--json")
	_ = hashGroupsCLIReport(t, code, afterGroups, stderr)
	if beforeDefault != afterDefault || beforeGroups != afterGroups {
		t.Fatal("fresh publication appeared in old reports or changed original accounting")
	}
	code, human, stderr := f.run(context.Background(), "hash", "--show-job", first.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{first.ID, key, "SAVED FRESH KEEPER/COPY COMPARISON - HISTORICAL", "Completed fresh observations 0 of 3", "Comparison Incomplete", "Selected keeper for review", "Selected copy for review", "Fresh observation NOT RECORDED", "Fresh reserved bytes 0 bytes", "FRESH-JOB READ CONSENT", "Fresh read consent NOT RECORDED", "ORIGINAL HASHING CONTEXT AT FIRST JOB PUBLICATION", "Reclaimable space Unknown", "No source files, configuration or inventory were opened"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("human job report omitted zero progress/exact roles/context", want, code, human, stderr)
		}
	}
	if strings.Index(human, "Completed fresh observations") > strings.Index(human, "Job:") || strings.Index(human, "FRESH-JOB READ CONSENT") > strings.Index(human, "ORIGINAL HASHING CONTEXT") || strings.Contains(flat, "UNAVAILABLE") || strings.Contains(flat, "Saved fresh prefix") || strings.Contains(flat, "Initial work status") {
		t.Fatal("human seed report buried the result or invented a fresh observation", human)
	}
	for _, target := range first.Record.Request.Targets {
		if strings.Count(human, fmt.Sprintf("%q", string(target.Target.File.PathBytes))) != 1 {
			t.Fatal("human seed report repeated or omitted an exact selected path", human)
		}
	}
}

func TestHashFreshJobCLIConflictingKeyAndIndependentGeneration(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	firstChoice := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "3", "1")
	otherChoice := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1", "3")
	key := "hash-job-key-v1-" + strings.Repeat("a", 64)
	code, raw, stderr := f.run(context.Background(), "hash", "--save-choice-job", firstChoice.ID, "--job-key", key, "--json")
	first := hashFreshJobCLIReport(t, code, raw, stderr, "save").Job
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(context.Background(), "hash", "--save-choice-job", otherChoice.ID, "--job-key", key, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_fresh_job_conflict", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) || len(hashFreshJobCLIIDs(t, f.base)) != 1 {
		t.Fatal("conflicting key published another job or changed history")
	}
	otherKey := "hash-job-key-v1-" + strings.Repeat("b", 64)
	code, raw, stderr = f.run(context.Background(), "hash", "--save-choice-job", firstChoice.ID, "--job-key", otherKey, "--json")
	second := hashFreshJobCLIReport(t, code, raw, stderr, "save").Job
	if second.ID == first.ID || second.Record.Request.RequestID != first.Record.Request.RequestID || second.Record.JobKey != otherKey || len(hashFreshJobCLIIDs(t, f.base)) != 2 {
		t.Fatal("new explicit generation reused old job identity/progress", raw)
	}
}

func TestHashFreshJobCLIShowBeforeMigrationAndHeldWriter(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	missing := "hash-choice-job-v1-" + strings.Repeat("a", 64)
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "hash", "--show-job", missing, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "not_found", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) || len(hashFreshJobCLIIDs(t, f.base)) != 0 {
		t.Fatal("show migrated schema3 or created job state")
	}
	key := "hash-job-key-v1-" + strings.Repeat("c", 64)
	code, raw, stderr = f.run(context.Background(), "hash", "--save-choice-job", saved.ID, "--job-key", key, "--json")
	job := hashFreshJobCLIReport(t, code, raw, stderr, "save").Job
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	w, err := inventory.OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	code, raw, stderr = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	if got := hashFreshJobCLIReport(t, code, raw, stderr, "show").Job; !reflect.DeepEqual(got, job) {
		t.Fatal("saved job needed a writer or source access", raw)
	}
}

func TestHashFreshJobCLICorruptSavedJobRefusesPartialResult(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	key := "hash-job-key-v1-" + strings.Repeat("d", 64)
	code, raw, stderr := f.run(context.Background(), "hash", "--save-choice-job", saved.ID, "--job-key", key, "--json")
	job := hashFreshJobCLIReport(t, code, raw, stderr, "save").Job
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt one bound pending row without changing its logical role scope.
	_, err = db.Exec("DROP TRIGGER hash_fresh_work_no_update; PRAGMA ignore_check_constraints=ON")
	if err == nil {
		_, err = db.Exec("UPDATE hash_fresh_work SET checked_offset=1 WHERE job_id=? AND ordinal=1", job.ID)
	}
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, stderr = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_fresh_job_invalid", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
		t.Fatal("corrupt show repaired or migrated records")
	}
}

func TestHashFreshJobCLIOutputCancellationPreservesExactRetry(t *testing.T) {
	for _, machine := range []bool{false, true} {
		f := newHashGuidedReviewFixture(t, 3)
		saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
		key := "hash-job-key-v1-" + strings.Repeat("e", 64)
		args := []string{"--data-dir", f.base, "hash", "--save-choice-job", saved.ID, "--job-key", key}
		if machine {
			args = append(args, "--json")
		}
		var diagnostic bytes.Buffer
		fullError := &hashMetadataCountErrorWriter{}
		code := Run(context.Background(), args, fullError, &diagnostic)
		ids := hashFreshJobCLIIDs(t, f.base)
		if code != 1 || len(ids) != 1 || !strings.Contains(diagnostic.String(), ids[0]) || !strings.Contains(diagnostic.String(), key) {
			t.Fatal("failed publication reply lost job/key or created extra jobs", code, fullError.String(), diagnostic.String(), ids)
		}
		if machine {
			_ = hashFreshJobCLIReport(t, 0, fullError.String(), "", "save")
		}
		for _, short := range []bool{false, true} {
			diagnostic.Reset()
			code = Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic)
			if code != 1 || !strings.Contains(diagnostic.String(), ids[0]) || !strings.Contains(diagnostic.String(), key) || len(hashFreshJobCLIIDs(t, f.base)) != 1 {
				t.Fatal("short/error retry reply lost exact saved job", code, diagnostic.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		diagnostic.Reset()
		code = Run(ctx, args, out, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), "canceled") || !strings.Contains(diagnostic.String(), ids[0]) || !strings.Contains(diagnostic.String(), key) || len(hashFreshJobCLIIDs(t, f.base)) != 1 {
			t.Fatal("canceled publication reply lost exact retry guidance", code, out.String(), diagnostic.String())
		}
		if machine {
			_ = hashFreshJobCLIReport(t, 0, out.String(), "", "save")
		}
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		code, raw, stderr := f.run(ctx, "hash", "--save-choice-job", saved.ID, "--job-key", "hash-job-key-v1-"+strings.Repeat("f", 64), "--json")
		hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
		if len(hashFreshJobCLIIDs(t, f.base)) != 1 {
			t.Fatal("early canceled save published another generation")
		}
	}
}

func TestHashFreshJobCLICapabilities(t *testing.T) {
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &stderr)
	var advertised struct {
		OK         bool            `json:"ok"`
		Features   map[string]bool `json:"features"`
		ErrorCodes []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &advertised); err != nil || code != 0 || stderr.Len() != 0 || !advertised.OK || !advertised.Features["saved_fresh_hash_jobs"] || advertised.Features["duplicates"] || advertised.Features["cleanup"] {
		t.Fatal("capabilities omitted unapproved job storage or claimed cleanup", code, out.String(), stderr.String(), err)
	}
	for _, errorCode := range []string{"hash_fresh_job_evidence_changed", "hash_fresh_job_conflict", "hash_fresh_job_capacity", "hash_fresh_job_invalid"} {
		if !slices.Contains(advertised.ErrorCodes, errorCode) {
			t.Fatal("capabilities omitted fresh job refusal", errorCode)
		}
	}
}
