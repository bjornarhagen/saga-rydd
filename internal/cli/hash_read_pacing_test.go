package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashReadPacingBodies(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	result := map[string][32]byte{}
	for path, digest := range hashCLIBytes(t, root) {
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		result[relative] = digest
	}
	return result
}

func setCLIHashReadRate(t *testing.T, f *hashProposalCLIFixture, rate int64) {
	t.Helper()
	path := filepath.Join(f.base, "config.toml")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = []string{f.root}
	cfg.Scan.ReadBytesPerSecond = rate
	if err := config.Create(path, f.root, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestHashReadPacingCLIKnownCapacityRefusesBeforeSourceAndWriter(t *testing.T) {
	t.Run("original", func(t *testing.T) {
		f, proposal := newHashReadCLIFixture(t, bytes.Repeat([]byte{'x'}, 1024))
		consent := hashBoundaryApprove(t, f, proposal, 8192, 16384)
		setCLIHashReadRate(t, f, 1)
		before := hashBoundarySaved(t, f)
		body := hashReadPacingBodies(t, f.root)
		// Both frozen scopes become unavailable. A known-impossible rate must
		// refuse before opening them or a recovery-capable run writer.
		if err := os.Rename(f.root, f.root+".offline"); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(f.source, f.source+".offline"); err != nil {
			t.Fatal(err)
		}
		writer, err := inventory.OpenExistingHashWriter(context.Background(), f.base)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		code, raw, diagnostic := f.run(context.Background(), "hash", "--run", consent.ID, "--json")
		hashProposalFailure(t, code, raw, diagnostic, "hash_read_pacing_capacity", 1)
		after := hashBoundarySaved(t, f)
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(body, hashReadPacingBodies(t, f.root+".offline")) {
			t.Fatal("capacity refusal changed saved work, consent, charges or bodies")
		}
		if _, err := os.Lstat(f.source); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("capacity refusal initialized replacement inventory", err)
		}
	})
	t.Run("fresh", func(t *testing.T) {
		f, job, consent := hashFreshRunCLIConsent(t)
		setCLIHashReadRate(t, f.hashProposalCLIFixture, 1)
		code, raw, diagnostic := f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
		before := hashFreshJobCLIReport(t, code, raw, diagnostic, "show").Job
		original := hashBoundarySaved(t, f.hashProposalCLIFixture)
		body := hashReadPacingBodies(t, f.root)
		if err := os.Rename(f.root, f.root+".offline"); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(f.source, f.source+".offline"); err != nil {
			t.Fatal(err)
		}
		code, raw, diagnostic = f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
		hashChoiceCLIError(t, code, raw, diagnostic, "hash", "hash_read_pacing_capacity", 1)
		code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job", job.ID, "--json")
		after := hashFreshJobCLIReport(t, code, raw, diagnostic, "show").Job
		if !reflect.DeepEqual(before, after) || after.Progress != nil || after.FreshBudget != nil || !reflect.DeepEqual(original, hashBoundarySaved(t, f.hashProposalCLIFixture)) || !reflect.DeepEqual(body, hashReadPacingBodies(t, f.root+".offline")) {
			t.Fatal("fresh preflight initialized progress, changed original context or read bodies")
		}
	})
}

func TestHashReadPacingCLIOriginalPartialKeepsChargeAndExplicitContinuation(t *testing.T) {
	contents := bytes.Repeat([]byte("generated"), 2048)
	f, proposal := newHashReadCLIFixture(t, contents)
	consent := hashBoundaryApprove(t, f, proposal, 1<<20, 2<<20)
	setCLIHashReadRate(t, f, 1024)
	body := hashCLIBytes(t, f.root)
	code, raw, diagnostic := f.run(context.Background(), "hash", "--run", consent.ID, "--json")
	first := hashCLIStepResult(t, code, raw, diagnostic)
	r := first.Result
	if r.ReadPacing == nil || r.ReadPacing.Contract != inventory.HashReadPacingContract || r.ReadPacing.RequestedBytesPerSecond != 1024 || r.ReadPacing.ObservedWait == nil || *r.ReadPacing.ObservedWait <= 0 || !r.ReadPacing.Yielded || r.Status != "pending" || r.DurableOffset < 64 || r.DurableOffset >= int64(len(contents)) || r.DurableOffset%64 != 0 || r.ReservedBytes != int64(len(contents)) || r.Usage.RequestedBytes != r.Usage.ReadBytes || r.Usage.ReadBytes <= 0 || r.Usage.ReadBytes >= r.ReservedBytes {
		t.Fatal("paced prefix lost partial progress, full charge or request accounting", raw)
	}
	saved := hashBoundarySaved(t, f)
	if saved.Budget.TotalReservedBytes != r.ReservedBytes || saved.Budget.TotalReadBytes != r.Usage.ReadBytes || saved.Budget.TotalUnknownReservedBytes != 0 || saved.ReadConsent.ID != consent.ID || !reflect.DeepEqual(saved.ReadConsent.Approval, consent.Approval) {
		t.Fatal("pacing refunded the reservation or changed immutable consent")
	}
	encoded, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("read_pacing")) || bytes.Contains(encoded, []byte("requested_bytes_per_second")) {
		t.Fatal("operation pacing leaked into saved records")
	}
	// A new explicit operation can use the current validated ceiling. It
	// cannot renew consent, replay the partial head, or inherit burst credits.
	setCLIHashReadRate(t, f, 1<<20)
	for i := 0; i < 3; i++ {
		code, raw, diagnostic = f.run(context.Background(), "hash", "--run", consent.ID, "--json")
		hashCLIStepResult(t, code, raw, diagnostic)
	}
	final := hashBoundarySaved(t, f)
	digest := sha256.Sum256(contents)
	for _, member := range final.Work {
		if member.Status != "complete" || member.DurableOffset != int64(len(contents)) || member.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("explicit continuation lost a full historical digest")
		}
	}
	if final.Budget.TotalReservedBytes <= final.Budget.TotalReadBytes || !reflect.DeepEqual(final.ReadConsent.Approval, consent.Approval) || !reflect.DeepEqual(body, hashCLIBytes(t, f.root)) {
		t.Fatal("continuation refunded unused bytes, changed consent or changed source contents")
	}
}

func TestHashReadPacingCLIFreshStepKeepsExactIndependentScope(t *testing.T) {
	f, job, consent := hashFreshRunCLIConsent(t)
	setCLIHashReadRate(t, f.hashProposalCLIFixture, 1024)
	original := hashBoundarySaved(t, f.hashProposalCLIFixture)
	body := hashCLIBytes(t, f.root)
	for i := range job.Work {
		code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
		r := hashFreshRunCLIReport(t, code, raw, diagnostic).Result
		if r.Ordinal != i+1 || r.JobID != job.ID || r.JobKey != job.Record.JobKey || r.RequestID != job.Record.Request.RequestID || r.ApprovalID != consent.ID || r.ReadPacing == nil || r.ReadPacing.RequestedBytesPerSecond != 1024 || r.ReadPacing.ObservedWait == nil || *r.ReadPacing.ObservedWait <= 0 || r.ReadPacing.Yielded || r.Status != "hash_observed" || r.ReservedBytes != int64(len(f.contents)) || r.Usage.ReadBytes != r.ReservedBytes {
			t.Fatal("paced fresh step lost exact scope or measured accounting", raw)
		}
	}
	reader, err := inventory.OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	final, err := reader.FreshJob(context.Background(), job.ID)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || (final.Comparison == nil || final.Comparison.Status != "historical_hashes_match") || final.FreshReadBytes != int64(len(job.Work)*len(f.contents)) || !reflect.DeepEqual(final.ReadConsent.Approval, consent.Approval) || !reflect.DeepEqual(original, hashBoundarySaved(t, f.hashProposalCLIFixture)) || !reflect.DeepEqual(body, hashCLIBytes(t, f.root)) {
		t.Fatal("fresh pacing changed original records, immutable consent or generated bodies", err, closeErr)
	}
}

func TestHashReadPacingCLIMinimumTailIdleAndNullableOutput(t *testing.T) {
	work := []inventory.SavedHashWork{{Status: "complete", LogicalBytes: 8192, DurableOffset: 8192}, {Status: "running", LogicalBytes: 65, DurableOffset: 64}, {Status: "pending", LogicalBytes: 8192}}
	if got := originalHashReadMinimum(work); got != 1 {
		t.Fatal("final tail was treated as a full durable quantum", got)
	}
	if got := originalHashReadMinimum(work[:1]); got != 0 {
		t.Fatal("idle queue required another body read", got)
	}
	job := inventory.SavedFreshJob{Progress: []inventory.SavedFreshHashWork{{Status: "pending", LogicalBytes: 65, DurableOffset: 64}}}
	if got := freshHashReadMinimum(job); got != 1 {
		t.Fatal("fresh tail preflight ignored independent progress", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := checkCLIHashReadPacing(ctx, 1, 1); err != nil {
		t.Fatal("exact final tail capacity refused", err)
	}
	var deferred bytes.Buffer
	printHashPacingZeroProgress(&deferred, "pacing_window_exhausted")
	if !strings.Contains(strings.Join(strings.Fields(deferred.String()), " "), "full byte reservation stays charged") {
		t.Fatal("charged zero-progress step hid its permanent reservation", deferred.String())
	}
	zero := time.Duration(0)
	for _, wait := range []*time.Duration{nil, &zero} {
		var out bytes.Buffer
		printHashReadPacing(&out, &inventory.HashReadPacingObservation{Contract: inventory.HashReadPacingContract, RequestedBytesPerSecond: 1024, ObservedWait: wait})
		want := "NOT RECORDED"
		if wait != nil {
			want = "0s"
		}
		if !strings.Contains(strings.Join(strings.Fields(out.String()), " "), "Read pacing wait "+want) || !strings.Contains(strings.Join(strings.Fields(out.String()), " "), "not a global or physical I/O limit") {
			t.Fatal("unknown wait became a measured zero or rate widened", out.String())
		}
	}
}

func TestHashReadPacingCLICapabilitiesErrorsAndCancellation(t *testing.T) {
	c := capabilities()
	if c["features"].(map[string]bool)["paced_explicit_hash_reads"] != true || c["hash_read_pacing"].(map[string]any)["global_rate_limit"] != false {
		t.Fatal("pacing capability missing or global rate claimed")
	}
	for _, tc := range []struct {
		err  error
		code string
	}{{inventory.ErrHashReadPacingInput, "hash_read_pacing_invalid"}, {inventory.ErrHashReadPacingCapacity, "hash_read_pacing_capacity"}, {inventory.ErrHashReadReservationDay, "hash_read_reservation_day"}, {errors.Join(inventory.ErrHashReadReservationDay, context.Canceled), "canceled"}} {
		var out, diagnostic bytes.Buffer
		exit := operationFailure(&out, &diagnostic, "hash", tc.err)
		hashChoiceCLIError(t, exit, out.String(), diagnostic.String(), "hash", tc.code, 1)
		if !slices.Contains(c["error_codes"].([]string), tc.code) {
			t.Fatal("pacing error not advertised", tc.code)
		}
	}
}
