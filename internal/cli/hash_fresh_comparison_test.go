package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshComparisonCLIReport(t *testing.T, code int, raw, diagnostic string, job inventory.SavedFreshJob) inventory.SavedFreshJob {
	t.Helper()
	var result inventory.SavedFreshJob
	var head struct {
		Hash HashFreshChoiceJobResult `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &head); err != nil {
		t.Fatal(err)
	}
	if len(head.Hash.Job.Progress) == 0 {
		result = hashFreshJobCLIReport(t, code, raw, diagnostic, "show").Job
	} else {
		result = hashFreshJobCLIReportWithProgress(t, code, raw, diagnostic, job)
	}
	c := result.Comparison
	r := result.Record.Request
	if c == nil || c.Contract != inventory.HashFreshJobComparisonContract || c.HashContract != inventory.FileHashContract || c.Source != "saved_fresh_job_observations" || c.Scope != "exact_saved_fresh_job" || c.JobID != result.ID || c.JobKey != result.Record.JobKey || c.RequestID != r.RequestID || c.ChoiceID != r.ChoiceID || c.StoreID != r.StoreID || c.SelectionID != r.SelectionID || c.InventoryID != r.InventoryID || c.ProgressInitialized != (len(result.Progress) != 0) || c.CurrentReadPermissionEvaluated || c.ApprovalAvailable || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.Executable || c.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh comparison lost exact saved scope or claimed current authority", raw)
	}
	if c.Keeper.Ordinal != 1 || c.Keeper.Role != "keeper" || c.Keeper.HistoricalWorkID != r.Targets[0].Observation.WorkID || !bytes.Equal(c.Keeper.PathBytes, r.Targets[0].Target.File.PathBytes) || len(c.Copies) != len(r.Targets)-1 || c.MatchingCopies+c.DifferingCopies+c.IncompleteCopies+c.BlockedCopies != len(c.Copies) {
		t.Fatal("fresh comparison changed exact ordered roles", raw)
	}
	for i, pair := range c.Copies {
		target := r.Targets[i+1]
		if pair.Copy.Ordinal != i+2 || pair.Copy.Role != "copy" || pair.Copy.HistoricalWorkID != target.Observation.WorkID || pair.Copy.FileID != target.Target.File.ID || pair.Copy.RootID != target.Target.Root.ID || pair.Copy.TargetDigest != result.Work[i+1].TargetDigest || !bytes.Equal(pair.Copy.PathBytes, target.Target.File.PathBytes) {
			t.Fatal("fresh comparison remapped a saved copy", raw)
		}
		if len(result.Progress) == 0 {
			if c.Keeper.Observation != nil || pair.Copy.Observation != nil {
				t.Fatal("untouched seeds became checked observations", raw)
			}
		} else if c.Keeper.Observation == nil || pair.Copy.Observation == nil || !reflect.DeepEqual(*c.Keeper.Observation, result.Progress[0]) || !reflect.DeepEqual(*pair.Copy.Observation, result.Progress[i+1]) {
			t.Fatal("comparison replaced genuine fresh status, sequence or time", raw)
		}
	}
	return result
}

func TestHashFreshComparisonCLIInitialPartialAndGenuineEqualHeads(t *testing.T) {
	f, job, consent := hashFreshRunCLIConsent(t)
	ctx := context.Background()
	code, original, diagnostic := f.run(ctx, "hashes", "--json")
	_ = hashCLIReport(t, code, original, diagnostic)
	code, raw, diagnostic := f.run(ctx, "hash", "--show-job", job.ID, "--json")
	initial := hashFreshComparisonCLIReport(t, code, raw, diagnostic, job)
	if initial.Comparison.Status != "incomplete" || initial.Comparison.ProgressInitialized || initial.Comparison.IncompleteCopies != 1 || initial.Comparison.Copies[0].Relation != "incomplete" {
		t.Fatal("initial job implied a fresh relation", raw)
	}
	code, raw, diagnostic = f.run(ctx, "hash", "--run-job", consent.ID, "--json")
	_ = hashFreshRunCLIReport(t, code, raw, diagnostic)
	code, raw, diagnostic = f.run(ctx, "hash", "--show-job", job.ID, "--json")
	partial := hashFreshComparisonCLIReport(t, code, raw, diagnostic, job)
	if partial.Comparison.Status != "incomplete" || !partial.Comparison.ProgressInitialized || partial.Comparison.Keeper.Observation.Status != "complete" || partial.Comparison.Copies[0].Copy.Observation.Status != "pending" || partial.Comparison.MatchingCopies != 0 {
		t.Fatal("single completed head implied equal copies", raw)
	}
	code, human, diagnostic := f.run(ctx, "hash", "--show-job", job.ID)
	flat := strings.Join(strings.Fields(human), " ")
	if code != 0 || diagnostic != "" || !strings.Contains(flat, "Completed fresh observations 1 of 2") || !strings.Contains(flat, "Comparison Incomplete") || strings.Contains(flat, "Initial work status") || strings.Count(flat, "Saved fresh state complete") != 1 || strings.Count(flat, "Saved fresh state pending") != 1 {
		t.Fatal("human partial report replaced actual saved progress with initial seed states", code, human, diagnostic)
	}
	code, raw, diagnostic = f.run(ctx, "hash", "--run-job", consent.ID, "--json")
	_ = hashFreshRunCLIReport(t, code, raw, diagnostic)
	code, raw, diagnostic = f.run(ctx, "hash", "--show-job", job.ID, "--json")
	complete := hashFreshComparisonCLIReport(t, code, raw, diagnostic, partial)
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(f.contents))
	if complete.Comparison.Status != "historical_hashes_match" || complete.Comparison.MatchingCopies != 1 || complete.Comparison.Copies[0].Relation != "historical_hashes_match" || complete.Comparison.Keeper.Observation.SHA256 != wantSHA || complete.Comparison.Copies[0].Copy.Observation.SHA256 != wantSHA {
		t.Fatal("genuine fresh observations did not compare independent content", raw)
	}
	code, human, diagnostic = f.run(ctx, "hash", "--show-job", job.ID)
	for _, text := range []string{"SAVED FRESH KEEPER/COPY COMPARISON - HISTORICAL", "Completed fresh observations 2 of 2", "Historical hashes match", "Comparison with keeper", "Saved fresh state complete", "Observed at", "Reclaimable space remains unknown."} {
		if code != 0 || diagnostic != "" || !strings.Contains(strings.Join(strings.Fields(human), " "), text) {
			t.Fatal("human comparison lost historical labels", code, human, diagnostic, text)
		}
	}
	if strings.Index(human, "Completed fresh observations") > strings.Index(human, "Job:") || strings.Index(human, "Comparison ") > strings.Index(human, "Job:") || strings.Contains(human, "Initial work status") || strings.Contains(human, "Initial checked offset") {
		t.Fatal("human complete report hid the result behind stale initial work", human)
	}
	for _, work := range complete.Progress {
		if strings.Count(human, fmt.Sprintf("%q", string(work.PathBytes))) != 1 || !strings.Contains(human, work.SHA256) || !strings.Contains(human, work.CheckedAt.UTC().Format(time.RFC3339Nano)) {
			t.Fatal("human completed report repeated a path or omitted its historical evidence", human)
		}
	}
	// Formatter-only variants are authored report shapes, not live mismatch evidence.
	comparison := *complete.Comparison
	comparison.Copies = append([]inventory.HashFreshJobCopyComparison(nil), comparison.Copies...)
	for _, status := range []string{"historical_hashes_differ", "incomplete", "blocked"} {
		comparison.Status, comparison.Copies[0].Relation = status, status
		var out bytes.Buffer
		printHashFreshComparison(&out, comparison)
		if !strings.Contains(out.String(), hashFreshComparisonLabel(status)) || !strings.Contains(out.String(), "saved observations made at separate times") {
			t.Fatal("human relation label lost qualified meaning", out.String())
		}
	}
	for _, state := range []struct {
		work, attempt, label string
	}{
		{"running", "reserved", "Reserved; usage not settled"},
		{"pending", "interrupted_unknown", "Interrupted; usage unknown"},
	} {
		member := complete.Comparison.Keeper
		observation := *member.Observation
		observation.Status, observation.SHA256 = state.work, ""
		observation.LatestAttempt = &inventory.HashAttempt{Status: state.attempt, ReservationDay: "2026-01-01", ReservedBytes: 64}
		member.Observation = &observation
		var out bytes.Buffer
		printHashFreshComparisonMember(&out, member)
		flat := strings.Join(strings.Fields(out.String()), " ")
		for _, want := range []string{state.label, "Observed requested Unknown", "Observed read Unknown", "Observed elapsed Unknown"} {
			if !strings.Contains(flat, want) || strings.Contains(flat, "Historical fresh SHA-256") {
				t.Fatal("human unsettled attempt reported known usage or a full digest", want, out.String())
			}
		}
	}
	code, after, diagnostic := f.run(ctx, "hashes", "--json")
	_ = hashCLIReport(t, code, after, diagnostic)
	if after != original {
		t.Fatal("fresh comparison changed original records")
	}
}

func TestHashFreshComparisonCLILiveRefusalStaysBlocked(t *testing.T) {
	f, job, consent := hashFreshRunCLIConsent(t)
	changed := bytes.Clone(f.contents)
	changed[0] ^= 1
	if err := os.WriteFile(string(job.Record.Request.Targets[0].Target.File.PathBytes), changed, 0600); err != nil {
		t.Fatal(err)
	}
	code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
	if code != 1 || diagnostic != "" {
		t.Fatal("fixture live replacement did not refuse fresh read", code, raw, diagnostic)
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	shown := hashFreshComparisonCLIReport(t, code, raw, diagnostic, job)
	if shown.Comparison.Status != "blocked" || shown.Comparison.BlockedCopies != 1 || shown.Comparison.Copies[0].Relation != "blocked" || shown.Comparison.Keeper.Observation.Status != "invalidated" || shown.FreshReadBytes != 0 || shown.FreshRequestedBytes != 0 {
		t.Fatal("invalidated source became a historical comparison", raw)
	}
}

func TestHashFreshComparisonCLIOfflineHeldWriterAndFailedRepliesChangeNothing(t *testing.T) {
	f, job, consent := hashFreshRunCLIConsent(t)
	for range 2 {
		code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
		_ = hashFreshRunCLIReport(t, code, raw, diagnostic)
	}
	code, raw, diagnostic := f.run(context.Background(), "hash", "--revoke-job", consent.ID, "--json")
	_ = hashFreshReadCLIReport(t, code, raw, diagnostic, "revoke")
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	shown := hashFreshComparisonCLIReport(t, code, raw, diagnostic, job)
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	w, err := inventory.OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
	if got := hashFreshComparisonCLIReport(t, code, raw, diagnostic, shown); !reflect.DeepEqual(got, shown) {
		t.Fatal("offline held-writer view changed evidence", raw)
	}
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "hash", "--show-job", job.ID}
		if machine {
			args = append(args, "--json")
		}
		var errors bytes.Buffer
		failed := &hashMetadataCountErrorWriter{}
		if code := Run(context.Background(), args, failed, &errors); code != 1 || !strings.Contains(errors.String(), job.ID) {
			t.Fatal("failed comparison reply lost saved identity", code, errors.String())
		}
		if machine {
			_ = hashFreshComparisonCLIReport(t, 0, failed.String(), "", shown)
		}
		ctx, cancel := context.WithCancel(context.Background())
		late := &hashChoiceCancelWriter{cancel: cancel}
		errors.Reset()
		code = Run(ctx, args, late, &errors)
		cancel()
		if code != 1 || !strings.Contains(errors.String(), job.ID) || !strings.Contains(errors.String(), "canceled") {
			t.Fatal("late comparison reply lost one saved result", code, errors.String())
		}
		if machine {
			_ = hashFreshComparisonCLIReport(t, 0, late.String(), "", shown)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
		t.Fatal("comparison/output opened offline source or changed records/clock/charges")
	}
}

func TestHashFreshComparisonCLICapability(t *testing.T) {
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &diagnostic)
	var report struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || code != 0 || diagnostic.Len() != 0 || !report.Features["fresh_hash_choice_comparisons"] || report.Features["duplicates"] || report.Features["cleanup"] {
		t.Fatal("comparison capability claimed current duplicates/cleanup", code, out.String(), diagnostic.String(), err)
	}
}
