package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func setCLIHashDailyLimit(t *testing.T, f *hashProposalCLIFixture, limit int64) {
	t.Helper()
	p := filepath.Join(f.base, "config.toml")
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = []string{f.root}
	cfg.Scan.ReadBytesPerDay = limit
	if err := config.Create(p, f.root, cfg); err != nil {
		t.Fatal(err)
	}
}

func hashCLIStoreBudget(t *testing.T, f *hashProposalCLIFixture) HashStoreBudgetReport {
	t.Helper()
	code, raw, diagnostic := f.run(context.Background(), "hashes", "--store-budget", "--json")
	var envelope struct {
		OK     bool                  `json:"ok"`
		Report HashStoreBudgetReport `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || diagnostic != "" || !envelope.OK || envelope.Report.Contract != inventory.HashStoreReadBudgetContract || envelope.Report.Activated != (envelope.Report.SavedBudget != nil) {
		t.Fatal(code, raw, diagnostic, err)
	}
	if b := envelope.Report.SavedBudget; b != nil && (b.CurrentReadPermissionEvaluated || b.CurrentConfiguredLimitEvaluated || b.PhysicalIOVerified) {
		t.Fatal("saved report asserted current authority", raw)
	}
	return envelope.Report
}

func TestHashStoreBudgetCLISharedOriginalFreshExhaustionAndChangedCap(t *testing.T) {
	f, job, consent := hashFreshRunCLIConsent(t)
	original := hashBoundarySaved(t, f.hashProposalCLIFixture)
	body := hashCLIBytes(t, f.root, f.source)
	seed := original.Budget.TotalReservedBytes
	limit := seed + 64
	setCLIHashDailyLimit(t, f.hashProposalCLIFixture, limit)
	code, raw, diagnostic := f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
	r := hashFreshRunCLIReport(t, code, raw, diagnostic).Result
	if r.Status != "pending" || r.ReservedBytes != 64 || r.DurableOffset != 64 || r.StoreReadBudget == nil || r.StoreReadBudget.TotalReservedBytes != limit || r.StoreReadBudget.TotalReadBytes != original.Budget.TotalReadBytes+64 || r.ConfiguredDailyReservedByteLimit == nil || *r.ConfiguredDailyReservedByteLimit != limit {
		t.Fatal("shared ceiling did not bound the exact fresh grant", raw)
	}
	before := hashCLIStoreBudget(t, f.hashProposalCLIFixture)
	for _, args := range [][]string{{"hash", "--run-job", consent.ID, "--json"}, {"hash", "--run", f.consent.ID, "--json"}} {
		code, raw, diagnostic = f.run(context.Background(), args...)
		hashChoiceCLIError(t, code, raw, diagnostic, "hash", "configured_daily_byte_limit", 1)
		if after := hashCLIStoreBudget(t, f.hashProposalCLIFixture); !reflect.DeepEqual(before.SavedBudget, after.SavedBudget) {
			// The clock may advance during explicit admission; charges and saved
			// progress must remain exact, while that newer clock is retained.
			if after.SavedBudget.ReservedBytes != before.SavedBudget.ReservedBytes || after.SavedBudget.TotalReservedBytes != before.SavedBudget.TotalReservedBytes || after.SavedBudget.TotalReadBytes != before.SavedBudget.TotalReadBytes {
				t.Fatal("exhausted step changed charged/read bytes", after)
			}
		}
	}
	expectedOriginal := hashBoundarySaved(t, f.hashProposalCLIFixture)
	if expectedOriginal.Budget.TotalReservedBytes != original.Budget.TotalReservedBytes || expectedOriginal.Budget.TotalReadBytes != original.Budget.TotalReadBytes || !reflect.DeepEqual(expectedOriginal.ReadConsent.Approval, original.ReadConsent.Approval) {
		t.Fatal("exhausted original attempt changed immutable authority or charges")
	}
	setCLIHashDailyLimit(t, f.hashProposalCLIFixture, seed)
	code, raw, diagnostic = f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
	hashChoiceCLIError(t, code, raw, diagnostic, "hash", "configured_daily_byte_limit", 1)
	setCLIHashDailyLimit(t, f.hashProposalCLIFixture, seed+2*int64(len(f.contents)))
	completed := map[int]bool{}
	for i := 0; i < 2; i++ {
		code, raw, diagnostic = f.run(context.Background(), "hash", "--run-job", consent.ID, "--json")
		r = hashFreshRunCLIReport(t, code, raw, diagnostic).Result
		if r.Status != "hash_observed" || r.Ordinal < 1 || r.Ordinal > len(job.Record.Request.Targets) || completed[r.Ordinal] || r.DurableOffset != int64(len(f.contents)) {
			t.Fatal("explicit continuation lost exact fresh work", raw)
		}
		target := job.Record.Request.Targets[r.Ordinal-1]
		if r.HistoricalWorkID != target.Observation.WorkID || r.Role != target.Role {
			t.Fatal("continuation remapped the frozen role", raw)
		}
		completed[r.Ordinal] = true
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show-job-read", consent.ID, "--json")
	afterConsent := hashFreshReadCLIReport(t, code, raw, diagnostic, "show")
	if !reflect.DeepEqual(consent.Approval, afterConsent.Approval) || !reflect.DeepEqual(expectedOriginal, hashBoundarySaved(t, f.hashProposalCLIFixture)) || !reflect.DeepEqual(body, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("configuration change renewed consent, original scope or source")
	}
	final := hashCLIStoreBudget(t, f.hashProposalCLIFixture).SavedBudget
	if final.TotalReservedBytes != seed+2*int64(len(f.contents)) || final.TotalReadBytes != original.Budget.TotalReadBytes+2*int64(len(f.contents)) || final.TotalUnknownReservedBytes != 0 || final.TotalOutstandingReservedBytes != 0 || job.ID != r.JobID {
		t.Fatal("shared accounting lost permanently charged work", final)
	}
}

func TestHashStoreBudgetCLIKnownTinyCapRefusesBeforeSourceAndWriter(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh"}[fresh], func(t *testing.T) {
			var f *hashProposalCLIFixture
			var args []string
			if fresh {
				fixture, _, c := hashFreshRunCLIConsent(t)
				f, args = fixture.hashProposalCLIFixture, []string{"hash", "--run-job", c.ID, "--json"}
			} else {
				fixture, p := newHashReadCLIFixture(t, bytes.Repeat([]byte{'x'}, 256))
				c := hashBoundaryApprove(t, fixture, p, 8192, 16384)
				f, args = fixture, []string{"hash", "--run", c.ID, "--json"}
			}
			setCLIHashDailyLimit(t, f, 1)
			before := hashBoundarySaved(t, f)
			budget := hashCLIStoreBudget(t, f)
			if err := os.Rename(f.root, f.root+".offline"); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(f.source, f.source+".offline"); err != nil {
				t.Fatal(err)
			}
			writer, err := inventory.OpenExistingHashSelectionWriter(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			code, raw, diagnostic := f.run(context.Background(), args...)
			hashChoiceCLIError(t, code, raw, diagnostic, "hash", "configured_daily_byte_limit", 1)
			if !reflect.DeepEqual(before, hashBoundarySaved(t, f)) || !reflect.DeepEqual(budget, hashCLIStoreBudget(t, f)) {
				t.Fatal("known tiny-cap refusal changed saved work or recovered a writer")
			}
		})
	}
}

func TestHashStoreBudgetCLISavedViewOfflineAndIndependentStores(t *testing.T) {
	f, _, _ := hashFreshRunCLIConsent(t)
	expected := hashCLIStoreBudget(t, f.hashProposalCLIFixture)
	other, _ := newHashReadCLIFixture(t, nil)
	if hashCLIStoreBudget(t, other).Activated {
		t.Fatal("another store inherited shared activation")
	}
	original := hashBoundarySaved(t, f.hashProposalCLIFixture)
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("invalid configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	writer, err := inventory.OpenExistingHashSelectionWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if after := hashCLIStoreBudget(t, f.hashProposalCLIFixture); !reflect.DeepEqual(expected, after) || !reflect.DeepEqual(original, hashBoundarySaved(t, f.hashProposalCLIFixture)) {
		t.Fatal("offline saved view sampled time, recovered work or changed records")
	}
	code, raw, diagnostic := f.run(context.Background(), "hashes", "--store-budget")
	flat := strings.Join(strings.Fields(raw), " ")
	for _, want := range []string{"SHARED HASH-STORE RESERVATIONS", "every fresh job", "samples no current clock", "no current configured limit", "not measure physical I/O"} {
		if code != 0 || diagnostic != "" || !strings.Contains(flat, want) {
			t.Fatal(want, code, raw, diagnostic)
		}
	}
}

func TestHashStoreBudgetCLICapabilitiesErrorsAndExclusiveMode(t *testing.T) {
	c := capabilities()
	if c["features"].(map[string]bool)["configured_hash_store_daily_reservations"] != true || c["hash_store_read_budget_contract"].(map[string]any)["default_daily_reserved_bytes"] != config.Default().Scan.ReadBytesPerDay {
		t.Fatal(c)
	}
	for _, tc := range []struct {
		err  error
		code string
	}{{inventory.ErrHashReadExecutionLimits, "hash_execution_limits_invalid"}, {inventory.ErrHashStoreReadBudgetRequired, "hash_store_budget_required"}, {inventory.ErrHashStoreReadBudgetCorrupt, "hash_store_budget_invalid"}, {errors.Join(inventory.ErrHashStoreReadBudgetCorrupt, context.Canceled), "canceled"}} {
		var out, diagnostic bytes.Buffer
		code := operationFailure(&out, &diagnostic, "hash", tc.err)
		hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hash", tc.code, 1)
	}
	base := filepath.Join(t.TempDir(), "must-not-exist")
	for _, opts := range [][]string{{"--store-budget=false"}, {"--store-budget", "--store-budget"}, {"--store-budget", "--groups"}, {"--store-budget", "--work", "1"}, {"--store-budget", "--choice", "bad"}, {"--store-budget", "1"}, {"--store-budget", "-d", "missing"}} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "hashes", "--json"}, opts...), &out, &diagnostic)
		hashChoiceCLIError(t, code, out.String(), diagnostic.String(), "hashes", "invalid_arguments", 2)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid mode initialized storage", err)
	}
}

type hashStoreBudgetCancelWriter struct {
	output bytes.Buffer
	cancel context.CancelFunc
}

func (w *hashStoreBudgetCancelWriter) Write(p []byte) (int, error) {
	n, e := w.output.Write(p)
	w.cancel()
	return n, e
}
func TestHashStoreBudgetCLICanceledSavedReply(t *testing.T) {
	f, _, _ := hashFreshRunCLIConsent(t)
	before := hashCLIStoreBudget(t, f.hashProposalCLIFixture)
	for _, machine := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashStoreBudgetCancelWriter{cancel: cancel}
		var diagnostic bytes.Buffer
		args := []string{"--data-dir", f.base, "hashes", "--store-budget"}
		if machine {
			args = append(args, "--json")
		}
		code := Run(ctx, args, out, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), "reply was canceled") || !strings.Contains(diagnostic.String(), "no saved records or source files were changed") {
			t.Fatal(code, out.output.String(), diagnostic.String())
		}
		if machine {
			var envelope struct {
				OK bool `json:"ok"`
			}
			if err := json.Unmarshal(out.output.Bytes(), &envelope); err != nil || !envelope.OK {
				t.Fatal("cancellation appended another envelope", err, out.output.String())
			}
		}
		if !reflect.DeepEqual(before, hashCLIStoreBudget(t, f.hashProposalCLIFixture)) {
			t.Fatal("canceled saved reply changed records")
		}
	}
}
