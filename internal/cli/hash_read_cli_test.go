package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func newHashReadCLIFixture(t *testing.T, contents []byte) (*hashProposalCLIFixture, inventory.HashProposal) {
	t.Helper()
	f := newHashProposalCLIFixture(t)
	// Run permits an absent config. Remove the deliberate malformed config
	// used to prove proposal/approval/report commands do not load it.
	if err := os.Remove(f.base + "/config.toml"); err != nil {
		t.Fatal(err)
	}
	if contents != nil {
		for _, file := range f.page.Bands[0].Files {
			if err := os.WriteFile(string(file.PathBytes), contents, 0600); err != nil {
				t.Fatal(err)
			}
		}
		code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--now", "--detailed", "--json")
		if code != 0 || stderr != "" {
			t.Fatal(code, raw, stderr)
		}
		code, raw, stderr = f.run(context.Background(), "report", "--same-size", "-d", f.root, "--min-size-bytes", "1", "--json")
		var envelope hashReportEnvelope
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || envelope.Report.SameSize == nil {
			t.Fatal(code, raw, stderr, err)
		}
		f.page, f.input = *envelope.Report.SameSize, []byte(raw)
		if err := os.WriteFile(f.report, f.input, 0600); err != nil {
			t.Fatal(err)
		}
	}
	code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
	return f, hashCLIProposal(t, code, raw, stderr)
}

func hashReadApproveArgs(selectionID string, day, total int64) []string {
	return []string{"hash", "--approve", selectionID, "--confirm-content-read", "--max-day-bytes", strconv.FormatInt(day, 10), "--max-total-bytes", strconv.FormatInt(total, 10), "--json"}
}

func hashCLIConsentResult(t *testing.T, code int, raw, stderr, mode string) inventory.HashReadConsent {
	t.Helper()
	var envelope struct {
		OK      bool              `json:"ok"`
		Version int               `json:"api_version"`
		Command string            `json:"command"`
		Hash    HashConsentResult `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hash" || envelope.Hash.Mode != mode {
		t.Fatal(code, raw, stderr, err)
	}
	c := envelope.Hash.ReadConsent
	if !validHashSelectionID(c.ID) || c.Approval.Contract != inventory.HashReadApprovalContract || c.Approval.StepByteLimit != inventory.FileHashStepByteLimit || !c.Approval.ExpiresAt.Equal(c.Approval.CreatedAt.Add(inventory.HashReadApprovalLifetime)) || c.CurrentReadPermissionEvaluated || c.Executable || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.EstimatedReclaimableBytes != nil {
		t.Fatal("stronger read consent claim or wrong limits", raw)
	}
	return c
}

func hashCLIStepResult(t *testing.T, code int, raw, stderr string) HashStepReport {
	t.Helper()
	var envelope struct {
		OK      bool           `json:"ok"`
		Version int            `json:"api_version"`
		Command string         `json:"command"`
		Hash    HashStepReport `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hash" || envelope.Hash.Mode != "run" {
		t.Fatal(code, raw, stderr, err)
	}
	r := envelope.Hash
	if !validHashSelectionID(r.ApprovalID) || r.ApprovalID != r.ReadConsent.ID || r.StepByteLimit != inventory.FileHashStepByteLimit || r.BudgetScope != "whole_saved_selection" || r.Result.ReservedBytes > r.StepByteLimit || r.Result.Progress.CurrentStateVerified || r.Result.Progress.ContentVerified || r.Result.Progress.ProvenanceVerified || r.Result.Progress.DuplicatesVerified || r.Result.Progress.Executable || r.Result.Progress.EstimatedReclaimableBytes != nil || r.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("step widened allowance or verification claim", raw)
	}
	return r
}

func TestHashCLIApproveRevokeOfflineExactImmutableAndSavedDisplay(t *testing.T) {
	ctx := context.Background()
	f, proposal := newHashReadCLIFixture(t, nil)
	if err := os.WriteFile(f.base+"/config.toml", []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.root+".offline", f.source+".offline")
	code, raw, stderr := f.run(ctx, hashReadApproveArgs(proposal.SelectionID, 1024, 4096)...)
	consent := hashCLIConsentResult(t, code, raw, stderr, "approve")
	if consent.Status != "recorded" || consent.Approval.StoreID != proposal.StoreID || consent.Approval.InventoryID != proposal.InventoryID || consent.Approval.SelectionID != proposal.SelectionID || !reflect.DeepEqual(consent.Approval.SourceLocator, *proposal.SourceLocator) || consent.Approval.DailyReservedByteLimit != 1024 || consent.Approval.LifetimeReservedByteLimit != 4096 {
		t.Fatal(raw)
	}
	code, raw, stderr = f.run(ctx, hashReadApproveArgs(proposal.SelectionID, 1024, 4096)...)
	retry := hashCLIConsentResult(t, code, raw, stderr, "approve")
	if retry.ID != consent.ID || !reflect.DeepEqual(retry.Approval, consent.Approval) {
		t.Fatal("approval retry renewed or changed limits", raw)
	}
	code, raw, stderr = f.run(ctx, hashReadApproveArgs(proposal.SelectionID, 2048, 4096)...)
	hashProposalFailure(t, code, raw, stderr, "already_exists", 1)
	code, raw, stderr = f.run(ctx, "hash", "--show", proposal.SelectionID, "--json")
	shown := hashCLIProposal(t, code, raw, stderr)
	if shown.ReadConsent == nil || shown.ReadConsent.ID != consent.ID {
		t.Fatal("saved show cannot recover approval ID", raw)
	}
	code, raw, stderr = f.run(ctx, "hash", "--revoke", consent.ID, "--json")
	revoked := hashCLIConsentResult(t, code, raw, stderr, "revoke")
	if revoked.ID != consent.ID || revoked.Status != "revoked" || revoked.Revocation == nil {
		t.Fatal(raw)
	}
	code, raw, stderr = f.run(ctx, "hash", "--revoke", consent.ID, "--json")
	repeat := hashCLIConsentResult(t, code, raw, stderr, "revoke")
	if !reflect.DeepEqual(repeat.Revocation, revoked.Revocation) || !reflect.DeepEqual(repeat.Approval, consent.Approval) {
		t.Fatal("revocation retry changed immutable records", raw)
	}
	code, raw, stderr = f.run(ctx, "hash", "--revoke", consent.ID)
	flat := strings.Join(strings.Fields(raw), " ")
	if code != 0 || stderr != "" || !strings.Contains(flat, "READ CONSENT REVOKED") || !strings.Contains(raw, consent.ID) || !strings.Contains(flat, "No source or inventory was read") {
		t.Fatal(code, raw, stderr)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.root+".offline", f.source+".offline")) {
		t.Fatal("approval/revocation touched offline source or inventory")
	}
}

func TestHashCLIRunOneMiBStepFairContinuationAndIndependentDigest(t *testing.T) {
	ctx := context.Background()
	contents := bytes.Repeat([]byte{'g'}, int(inventory.FileHashStepByteLimit)+65)
	f, proposal := newHashReadCLIFixture(t, contents)
	before := hashCLIBytes(t, f.root, f.source)
	code, raw, stderr := f.run(ctx, hashReadApproveArgs(proposal.SelectionID, 4<<20, 4<<20)...)
	consent := hashCLIConsentResult(t, code, raw, stderr, "approve")
	var last HashStepReport
	for i, wantWork := range []string{"1", "2", "1", "2"} {
		code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
		last = hashCLIStepResult(t, code, raw, stderr)
		if last.Result.WorkID != wantWork {
			t.Fatal("finite queue did not continue fairly", raw)
		}
		if i < 2 {
			if last.Result.ReservedBytes != inventory.FileHashStepByteLimit || last.Result.Usage.RequestedBytes != inventory.FileHashStepByteLimit || last.Result.Usage.ReadBytes != inventory.FileHashStepByteLimit || last.Result.DurableOffset != inventory.FileHashStepByteLimit || last.Result.Progress.SHA256 != "" {
				t.Fatal("one invocation looped or changed fixed step", raw)
			}
			var human bytes.Buffer
			if err := printHashStepReport(&human, last); err != nil || !strings.Contains(human.String(), "rydd hash --run "+consent.ID) || !strings.Contains(strings.Join(strings.Fields(human.String()), " "), "with the same global options") {
				t.Fatal("partial output lost exact bounded continuation", human.String(), err)
			}
		} else {
			want := fmt.Sprintf("%x", sha256.Sum256(contents))
			if last.Result.Status != "hash_observed" || last.Result.ReservedBytes != 65 || last.Result.Usage.ReadBytes != 65 || last.Result.DurableOffset != int64(len(contents)) || last.Result.Progress.SHA256 != want {
				t.Fatal("final tail/digest incorrect", raw)
			}
		}
		code, raw, stderr = f.run(ctx, "hashes", "--json")
		snapshot := hashCLIReport(t, code, raw, stderr)
		if i == 0 && (snapshot.Work[0].Sequence != 1 || snapshot.Work[1].Sequence != 0 || snapshot.Work[1].LatestAttempt != nil || snapshot.Budget.TotalReservedBytes != inventory.FileHashStepByteLimit) {
			t.Fatal("one invocation dispatched more than one work row", raw)
		}
	}
	var human bytes.Buffer
	if err := printHashStepReport(&human, last); err != nil || !strings.Contains(human.String(), "Historical SHA-256: "+last.Result.Progress.SHA256) || !strings.Contains(human.String(), fmt.Sprintf("%q", string(last.Result.Progress.PathBytes))) {
		t.Fatal(human.String(), err)
	}
	stateIndex := strings.Index(human.String(), "Historical full-file hash observed")
	if stateIndex < 0 || stateIndex > strings.Index(human.String(), "Read consent ID:") || !strings.Contains(human.String(), "rydd hashes") {
		t.Fatal("completed step hid its result or omitted saved progress command", human.String())
	}
	code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
	idle := hashCLIStepResult(t, code, raw, stderr)
	if idle.Result.Status != "idle" || idle.Result.WorkID != "" || idle.Result.ReservedBytes != 0 || idle.Result.Budget.TotalReservedBytes != 2*int64(len(contents)) {
		t.Fatal("idle run invented a reservation", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("hash reads changed source or inventory bytes")
	}
}

func TestHashCLIConsentAndStepLostOutputRetainDiscoverableSavedState(t *testing.T) {
	ctx := context.Background()
	f, proposal := newHashReadCLIFixture(t, nil)
	var stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, hashReadApproveArgs(proposal.SelectionID, 4096, 8192)...), hashFailWriter{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "write JSON") {
		t.Fatal(code, stderr.String())
	}
	code, raw, diagnostic := f.run(ctx, "hash", "--show", proposal.SelectionID, "--json")
	shown := hashCLIProposal(t, code, raw, diagnostic)
	if shown.ReadConsent == nil {
		t.Fatal("lost approval output erased saved ID", raw)
	}
	stderr.Reset()
	code = Run(ctx, []string{"--data-dir", f.base, "hash", "--run", shown.ReadConsent.ID, "--json"}, hashFailWriter{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "write JSON") {
		t.Fatal(code, stderr.String())
	}
	code, raw, diagnostic = f.run(ctx, "hashes", "--json")
	snapshot := hashCLIReport(t, code, raw, diagnostic)
	if snapshot.Work[0].Status != "complete" || snapshot.Work[1].LatestAttempt != nil || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.ReadBytes == nil || *snapshot.Work[0].LatestAttempt.ReadBytes != proposal.Targets[0].File.Size || snapshot.Budget.TotalReservedBytes != proposal.Targets[0].File.Size {
		t.Fatal("lost run output rolled back or retried", raw)
	}
	stderr.Reset()
	code = Run(ctx, []string{"--data-dir", f.base, "hash", "--revoke", shown.ReadConsent.ID}, hashFailWriter{short: true}, &stderr)
	if code != 1 || stderr.Len() == 0 {
		t.Fatal(code, stderr.String())
	}
	code, raw, diagnostic = f.run(ctx, "hash", "--show", proposal.SelectionID, "--json")
	if after := hashCLIProposal(t, code, raw, diagnostic); after.ReadConsent == nil || after.ReadConsent.Status != "revoked" {
		t.Fatal("lost revocation reply erased revocation", raw)
	}
}

func TestHashCLIApprovalIncludesPriorChargeInBothOutputModes(t *testing.T) {
	ctx := context.Background()
	f, proposal := newHashReadCLIFixture(t, nil)
	source, err := state.OpenReader(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	scanner, err := inventory.New([]string{f.root}, nil, []string{f.base})
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	writer, err := inventory.OpenHashWriter(ctx, f.base)
	if err != nil {
		t.Fatal(err)
	}
	// Generated library work predating consent contributes a lifetime charge.
	result, err := writer.RunNext(ctx, source, scanner, 64, 4096)
	closeErr := writer.Close()
	if err != nil || closeErr != nil || result.ReservedBytes <= 1 {
		t.Fatal(result, err, closeErr)
	}
	args := hashReadApproveArgs(proposal.SelectionID, 4096, 1)
	code, raw, stderr := f.run(ctx, args...)
	hashProposalFailure(t, code, raw, stderr, "invalid_arguments", 2)
	code, raw, stderr = f.run(ctx, args[:len(args)-1]...)
	if code != 2 || raw != "" || !strings.Contains(stderr, "include prior lifetime charges") {
		t.Fatal("human and JSON lifetime-limit failures disagree", code, raw, stderr)
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	snapshot := hashCLIReport(t, code, raw, stderr)
	if snapshot.ReadConsent != nil || snapshot.Budget.TotalReservedBytes != result.ReservedBytes || snapshot.Work[1].LatestAttempt != nil {
		t.Fatal("refused approval changed consent or dispatched reads", raw)
	}
}

func TestHashCLIReadModeInvalidFlagsBeforeStorage(t *testing.T) {
	id := strings.Repeat("a", 64)
	valid := hashReadApproveArgs(id, 64, 128)
	valid = valid[:len(valid)-1]
	cases := [][]string{
		{"hash", "--approve", id, "--confirm-content-read", "--max-day-bytes", "64"},
		{"hash", "--approve", id, "--max-day-bytes", "64", "--max-total-bytes", "128"},
		{"hash", "--approve", id, "--confirm-content-read=false", "--max-day-bytes", "64", "--max-total-bytes", "128"},
		{"hash", "--run", id, "--confirm-content-read=false"}, {"hash", "--revoke", id, "--max-day-bytes", "64"},
		{"hash", "--run", id, "--revoke", id}, {"hash", "--run", id, "--run", id}, {"hash", "--revoke", id, "-revoke", id},
		{"hash", "--run", id, "-d", "/missing"}, {"hash", "--revoke", id, "--from", "/missing"}, {"hash", "--run", id, "1"},
		{"hash", "--run", strings.ToUpper(id)}, {"hash", "--approve", id, "--select=false"},
	}
	for _, value := range []string{"", "0", "-1", "+64", "064", "1.5", "1125899906842625", "9223372036854775808", "--json"} {
		for _, at := range []int{5, 7} {
			args := append([]string(nil), valid...)
			args[at] = value
			cases = append(cases, args)
		}
	}
	cases = append(cases, append(append([]string(nil), valid...), "--max-day-bytes", "64"))
	cases = append(cases, append(append([]string(nil), valid...), "--confirm-content-read"))
	for i, args := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "absent")
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{"--json", "--data-dir", base}, args...), &out, &stderr)
			hashProposalFailure(t, code, out.String(), stderr.String(), "invalid_arguments", 2)
			out.Reset()
			stderr.Reset()
			code = Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &stderr)
			if code != 2 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("human invalid flags differ", code, out.String(), stderr.String())
			}
			if _, err := os.Stat(base); !os.IsNotExist(err) {
				t.Fatal("invalid flags touched storage", err)
			}
		})
	}
}

func TestHashCLIReadModesMissingStorageOrIDsNeverInitialize(t *testing.T) {
	ctx := context.Background()
	id := strings.Repeat("a", 64)
	for _, args := range [][]string{hashReadApproveArgs(id, 64, 128), {"hash", "--run", id, "--json"}, {"hash", "--revoke", id, "--json"}} {
		t.Run(args[1], func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "absent")
			var out, stderr bytes.Buffer
			code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &stderr)
			hashProposalFailure(t, code, out.String(), stderr.String(), "not_found", 1)
			if _, err := os.Stat(base); !os.IsNotExist(err) {
				t.Fatal("missing consent storage initialized", err)
			}
		})
	}
	f, _ := newHashReadCLIFixture(t, nil)
	before := hashCLIBytes(t, f.base, f.root)
	for _, args := range [][]string{hashReadApproveArgs(id, 64, 128), {"hash", "--run", id, "--json"}, {"hash", "--revoke", id, "--json"}} {
		code, raw, stderr := f.run(ctx, args...)
		hashProposalFailure(t, code, raw, stderr, "not_found", 1)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("missing saved IDs changed stored bytes")
	}
}

func TestHashCLIReadDenialsPreserveCodesWithoutDigestOrInventedUsage(t *testing.T) {
	for _, mode := range []string{"daily_byte_limit", "durable_quantum", "read_consent_revoked"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f, p := newHashReadCLIFixture(t, bytes.Repeat([]byte{'q'}, 128))
			day := int64(64)
			if mode == "durable_quantum" {
				day = 63
			}
			code, raw, stderr := f.run(ctx, hashReadApproveArgs(p.SelectionID, day, 256)...)
			consent := hashCLIConsentResult(t, code, raw, stderr, "approve")
			var charged int64
			if mode == "daily_byte_limit" {
				code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
				step := hashCLIStepResult(t, code, raw, stderr)
				charged = 64
				if step.Result.DurableOffset != 64 || step.Result.ReservedBytes != charged || step.Result.Usage.ReadBytes != 64 {
					t.Fatal(raw)
				}
			} else if mode == "read_consent_revoked" {
				code, raw, stderr = f.run(ctx, "hash", "--revoke", consent.ID, "--json")
				hashCLIConsentResult(t, code, raw, stderr, "revoke")
			}
			code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
			hashProposalFailure(t, code, raw, stderr, mode, 1)
			for _, forbidden := range []string{`"usage"`, `"sha256"`, `"read_bytes"`, `"reserved_bytes"`} {
				if strings.Contains(raw, forbidden) {
					t.Fatal("denial exposed result or invented accounting", raw)
				}
			}
			code, raw, stderr = f.run(ctx, "hashes", "--json")
			snapshot := hashCLIReport(t, code, raw, stderr)
			if snapshot.Budget != nil && (snapshot.Budget.TotalReservedBytes != charged || snapshot.Budget.TotalReadBytes != charged || snapshot.Budget.TotalUnknownReservedBytes != 0) {
				t.Fatal("denied run changed accounting", raw)
			}
			if snapshot.Work[1].Sequence != 0 || snapshot.Work[1].LatestAttempt != nil || snapshot.Work[0].SHA256 != "" || (charged == 0 && snapshot.Work[0].LatestAttempt != nil) {
				t.Fatal("denied run read or dispatched another target", raw)
			}
		})
	}
}
