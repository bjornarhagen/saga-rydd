package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshRunCLIConsent(t *testing.T) (hashGuidedReviewFixture, inventory.SavedFreshJob, inventory.HashFreshReadConsent) {
	t.Helper()
	f, job := hashFreshReadCLIJob(t)
	code, raw, diagnostic := f.run(context.Background(), freshReadCLIApproveArgs(job.ID)...)
	return f, job, hashFreshReadCLIReport(t, code, raw, diagnostic, "approve")
}

func hashFreshRunCLIReport(t *testing.T, code int, raw, diagnostic string) HashFreshStepReport {
	t.Helper()
	var envelope struct {
		OK      bool                `json:"ok"`
		Version int                 `json:"api_version"`
		Command string              `json:"command"`
		Hash    HashFreshStepReport `json:"hash"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(&envelope); err != nil || d.Decode(new(any)) != io.EOF || code != 0 || diagnostic != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hash" {
		t.Fatal("fresh step lost one standard envelope", code, raw, diagnostic, err)
	}
	report := envelope.Hash
	r := report.Result
	p := r.Progress
	if report.Mode != "run" || report.StepByteLimit != inventory.FileHashStepByteLimit || report.BudgetScope != "whole_fresh_job" || !inventory.ValidHashFreshJobID(r.JobID) || !inventory.ValidHashFreshJobKey(r.JobKey) || !inventory.ValidHashKeeperChoiceFreshRequestID(r.RequestID) || !inventory.ValidHashKeeperChoiceID(r.ChoiceID) || r.ApprovalID != report.ReadConsent.ID || !inventory.ValidHashFreshReadApprovalID(r.ApprovalID) || r.ReservedBytes > inventory.FileHashStepByteLimit || r.Usage.RequestedBytes > r.ReservedBytes || r.Usage.ReadBytes > r.Usage.RequestedBytes || p.ProvenanceVerified || p.ContentVerified || p.CurrentStateVerified || p.DuplicatesVerified || p.Executable || p.EstimatedReclaimableBytes != nil || report.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("fresh step expanded scope/caps or claimed authority", raw)
	}
	return report
}

func TestHashFreshRunCLIArgumentsAndMissingStateBeforeAccess(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	id := "hash-job-read-v1-" + strings.Repeat("a", 64)
	for _, args := range [][]string{{"hash", "--run-job"}, {"hash", "--run-job", "bad"}, {"hash", "--run-job", strings.Repeat("a", 64)}, {"hash", "--run-job", strings.ToUpper(id)}, {"hash", "--run-job", id, "--run-job", id}, {"hash", "--run-job", id, "--show-job-read", id}, {"hash", "--run-job", id, "--run", strings.Repeat("a", 64)}, {"hash", "--run-job", id, "--new-job-key"}, {"hash", "--run-job", id, "--confirm-content-read=false"}, {"hash", "--run-job", id, "--max-day-bytes", "1"}, {"hash", "--run-job", id, "--max-total-bytes", "1"}, {"hash", "--run-job", id, "--keeper", "1"}, {"hash", "--run-job", id, "--job-key", "hash-job-key-v1-" + strings.Repeat("b", 64)}, {"hash", "--run-job", id, "-d", "/generated/missing"}, {"hash", "--run-job", id, "--from", "/generated/missing"}, {"hash", "--run-job", id, "1"}, {"hash", "--run-job", id, "--", "--json"}} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &diagnostic)
		hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hash", "invalid_arguments", 2)
	}
	for _, args := range [][]string{{"hash", "--run-job", "--json"}, {"hash", "--run-job=--json"}, {"hash", "-run-job", "--json"}} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &diagnostic)
		if code != 2 || out.Len() != 0 || diagnostic.Len() == 0 {
			t.Fatal("literal run ID enabled JSON", args, code, out.String(), diagnostic.String())
		}
	}
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hash", "--run-job", id, "--json"}, &out, &diagnostic)
	hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hash", "not_found", 1)
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused run initialized missing storage", err)
	}
}

func TestHashFreshRunCLIRehashesExactRolesWithIndependentAccounting(t *testing.T) {
	f, job, c := hashFreshRunCLIConsent(t)
	code, original, diagnostic := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, original, diagnostic)
	sources := hashCLIBytes(t, f.root, f.source)
	wantSHA := sha256.Sum256(f.contents)
	var charged int64
	for i, target := range job.Record.Request.Targets {
		code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", c.ID, "--json")
		r := hashFreshRunCLIReport(t, code, raw, diagnostic).Result
		if r.JobID != job.ID || r.JobKey != job.Record.JobKey || r.RequestID != job.Record.Request.RequestID || r.Ordinal != i+1 || r.HistoricalWorkID != target.Observation.WorkID || r.Role != target.Role || r.Status != "hash_observed" || r.DurableOffset != target.Target.File.Size || r.Usage.ReadBytes != target.Target.File.Size || r.ReservedBytes != target.Target.File.Size || r.Progress.SHA256 != hex.EncodeToString(wantSHA[:]) {
			t.Fatal("fresh run reused old bytes or remapped exact role", raw)
		}
		charged += target.Target.File.Size
		if r.FreshBudget == nil || r.FreshBudget.TotalReservedBytes != charged || r.FreshBudget.TotalReadBytes != charged || r.FreshBudget.TotalUnknownReservedBytes != 0 {
			t.Fatal("fresh step inherited original accounting", raw)
		}
	}
	code, raw, diagnostic := f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	shown := hashFreshJobCLIReportWithProgress(t, code, raw, diagnostic, job)
	if len(shown.Progress) != len(job.Work) || shown.FreshReadBytes != charged || shown.FreshReservedBytes != charged {
		t.Fatal("saved job hid genuine fresh progress", raw)
	}
	for i, p := range shown.Progress {
		if p.Ordinal != i+1 || p.Status != "complete" || p.SHA256 != hex.EncodeToString(wantSHA[:]) || p.DurableOffset != job.Record.Request.Targets[i].Target.File.Size {
			t.Fatal("saved progress remapped or lost full observation", raw)
		}
	}
	code, idle, diagnostic := f.run(context.Background(), "hash", "--run-job", c.ID, "--json")
	r := hashFreshRunCLIReport(t, code, idle, diagnostic).Result
	if r.Status != "idle" || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || r.Ordinal != 0 || r.FreshBudget.TotalReservedBytes != charged {
		t.Fatal("idle fresh job reread or charged contents", idle)
	}
	code, after, diagnostic := f.run(context.Background(), "hashes", "--json")
	_ = hashCLIReport(t, code, after, diagnostic)
	if original != after || !reflect.DeepEqual(sources, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("fresh run changed original records/inventory/source bodies")
	}
	code, human, diagnostic := f.run(context.Background(), "hash", "--show-job", job.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{job.ID, c.ID, "SAVED FRESH PROGRESS", "Historical fresh SHA-256", "Reclaimable space Unknown"} {
		if code != 0 || diagnostic != "" || !strings.Contains(flat, want) {
			t.Fatal("human job report hid fresh historical progress", want, code, human, diagnostic)
		}
	}
}

func hashFreshJobCLIReportWithProgress(t *testing.T, code int, raw, diagnostic string, seed inventory.SavedFreshJob) inventory.SavedFreshJob {
	t.Helper()
	var value struct {
		OK   bool                     `json:"ok"`
		Hash HashFreshChoiceJobResult `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &value); err != nil || code != 0 || diagnostic != "" || !value.OK || value.Hash.Mode != "show" {
		t.Fatal("fresh progress view failed", code, raw, diagnostic, err)
	}
	j := value.Hash.Job
	if j.ID != seed.ID || !reflect.DeepEqual(j.Record, seed.Record) || !reflect.DeepEqual(j.Work, seed.Work) || j.ApprovalAvailable || j.ProvenanceVerified || j.ContentVerified || j.CurrentStateVerified || j.DuplicatesVerified || j.Executable || j.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh progress replaced immutable job/seeds or claimed authority", raw)
	}
	return j
}

func TestHashFreshRunCLICurrentConfigAndChangedFrozenInventoryRefuse(t *testing.T) {
	for _, kind := range []string{"malformed_config", "selected_config_alias", "excluded", "changed_non_job_inventory"} {
		t.Run(kind, func(t *testing.T) {
			f, job, c := hashFreshRunCLIConsent(t)
			code, original, diagnostic := f.run(context.Background(), "hashes", "--json")
			_ = hashCLIReport(t, code, original, diagnostic)
			configPath := filepath.Join(f.base, "config.toml")
			switch kind {
			case "malformed_config":
				if err := os.WriteFile(configPath, []byte("invalid [ TOML"), 0600); err != nil {
					t.Fatal(err)
				}
			case "selected_config_alias":
				if err := os.Link(string(f.proposal.Targets[3].File.PathBytes), configPath); err != nil {
					t.Fatal(err)
				}
			case "excluded":
				body, _ := json.Marshal(string(job.Record.Request.Targets[0].Target.File.PathBytes))
				if err := os.WriteFile(configPath, []byte("excludes = ["+string(body)+"]\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "changed_non_job_inventory":
				if err := os.WriteFile(string(f.proposal.Targets[3].File.PathBytes), []byte("changed generated file"), 0600); err != nil {
					t.Fatal(err)
				}
				code, raw, diagnostic := f.run(context.Background(), "scan", "-d", f.root, "--now", "--json")
				if code != 0 || diagnostic != "" {
					t.Fatal("fixture rescan failed", code, raw, diagnostic)
				}
			}
			code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", c.ID, "--json")
			if code != 1 {
				t.Fatal("unsafe fresh run was accepted", kind, code, raw, diagnostic)
			}
			var value struct {
				OK    bool `json:"ok"`
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(raw), &value); err != nil || value.OK || diagnostic != "" {
				t.Fatal("blocked run returned positive/partial envelope", raw, diagnostic, err)
			}
			if kind == "changed_non_job_inventory" && value.Error.Code != "hash_inventory_changed" {
				t.Fatal("full frozen evidence mismatch was hidden", raw)
			}
			code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
			shown := hashFreshJobCLIReportWithProgress(t, code, raw, diagnostic, job)
			if shown.FreshReadBytes != 0 || shown.FreshRequestedBytes != 0 {
				t.Fatal("refused scope/config read selected bodies", raw)
			}
			code, after, diagnostic := f.run(context.Background(), "hashes", "--json")
			_ = hashCLIReport(t, code, after, diagnostic)
			if original != after {
				t.Fatal("refused fresh run changed original hash records")
			}
		})
	}
}

func TestHashFreshRunCLISavedProgressOfflineDoesNotRecover(t *testing.T) {
	f, job, c := hashFreshRunCLIConsent(t)
	code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", c.ID, "--json")
	_ = hashFreshRunCLIReport(t, code, raw, diagnostic)
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	prior := hashFreshJobCLIReportWithProgress(t, code, raw, diagnostic, job)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	shown := hashFreshJobCLIReportWithProgress(t, code, raw, diagnostic, job)
	if !reflect.DeepEqual(shown, prior) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
		t.Fatal("saved fresh progress needed source access or changed clock/work", raw)
	}
}

func TestHashFreshRunCLIOutputFailureAndCancellationDoNotRetry(t *testing.T) {
	for _, machine := range []bool{false, true} {
		f, job, c := hashFreshRunCLIConsent(t)
		args := []string{"--data-dir", f.base, "hash", "--run-job", c.ID}
		if machine {
			args = append(args, "--json")
		}
		var diagnostic bytes.Buffer
		out := &hashMetadataCountErrorWriter{}
		code := Run(context.Background(), args, out, &diagnostic)
		if code != 1 || !strings.Contains(diagnostic.String(), job.ID) {
			t.Fatal("failed step reply lost exact job guidance", code, diagnostic.String())
		}
		if machine {
			_ = hashFreshRunCLIReport(t, 0, out.String(), "")
		}
		code, raw, stderr := f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
		shown := hashFreshJobCLIReportWithProgress(t, code, raw, stderr, job)
		if len(shown.Progress) != 2 || shown.Progress[0].Status != "complete" || shown.Progress[1].DurableOffset != 0 {
			t.Fatal("failed output retried or lost exactly one step", raw)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		code, raw, stderr = f.run(ctx, "hash", "--run-job", c.ID, "--json")
		hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
		code, raw, stderr = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
		if got := hashFreshJobCLIReportWithProgress(t, code, raw, stderr, job); !reflect.DeepEqual(got, shown) {
			t.Fatal("early cancellation ran a step", raw)
		}
		ctx, cancel = context.WithCancel(context.Background())
		late := &hashChoiceCancelWriter{cancel: cancel}
		diagnostic.Reset()
		code = Run(ctx, args, late, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), job.ID) || !strings.Contains(diagnostic.String(), "canceled") {
			t.Fatal("late reply cancellation lost saved job", code, diagnostic.String())
		}
		if machine {
			_ = hashFreshRunCLIReport(t, 0, late.String(), "")
		}
		code, raw, stderr = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
		got := hashFreshJobCLIReportWithProgress(t, code, raw, stderr, job)
		if got.Progress[1].Status != "complete" || got.FreshReadBytes != 2*job.Record.Request.Targets[0].Target.File.Size {
			t.Fatal("late output cancellation changed finite step count", raw)
		}
	}
}

func TestHashFreshRunCLICapabilities(t *testing.T) {
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &diagnostic)
	var value struct {
		Features map[string]bool `json:"features"`
		Codes    []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &value); err != nil || code != 0 || diagnostic.Len() != 0 || !value.Features["guarded_fresh_hash_steps"] || value.Features["duplicates"] || value.Features["cleanup"] {
		t.Fatal("fresh-run capabilities claimed cleanup", code, out.String(), diagnostic.String(), err)
	}
	if !slices.Contains(value.Codes, "fresh_hash_progress_invalid") || !slices.Contains(value.Codes, "fresh_read_window_too_short") {
		t.Fatal("missing fresh progress or read-window refusal")
	}
	out.Reset()
	diagnostic.Reset()
	if code := operationFailure(&out, &diagnostic, "hash", inventory.ErrHashFreshReadWindow); code != 1 || !strings.Contains(out.String(), `"code":"fresh_read_window_too_short"`) || diagnostic.Len() != 0 {
		t.Fatal("short window mislabeled", code, out.String(), diagnostic.String())
	}
}
