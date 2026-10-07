package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func hashFreshRequestCLIReport(t *testing.T, code int, raw, stderr string) inventory.HashKeeperChoiceFreshRequestReport {
	t.Helper()
	var envelope struct {
		Version int                                          `json:"api_version"`
		Command string                                       `json:"command"`
		OK      bool                                         `json:"ok"`
		Hash    inventory.HashKeeperChoiceFreshRequestReport `json:"hash"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	err := d.Decode(&envelope)
	if err != nil || d.Decode(new(any)) != io.EOF || code != 0 || stderr != "" || envelope.Version != APIVersion || envelope.Command != "hash" || !envelope.OK {
		t.Fatal("fresh request lost its one standard result envelope", code, raw, stderr, err)
	}
	r := envelope.Hash
	if r.Version != 1 || r.Contract != inventory.HashKeeperChoiceFreshRequestContract || r.HashContract != inventory.FileHashContract || r.Source != "saved_hash_choice" || r.Status != "unapproved" || !inventory.ValidHashKeeperChoiceFreshRequestID(r.RequestID) || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh request changed contract or claimed verification/authority", raw)
	}
	for _, text := range []string{`"approval_available":false`, `"provenance_verified":false`, `"content_verified":false`, `"current_state_verified":false`, `"duplicates_verified":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !strings.Contains(raw, text) {
			t.Fatal("fresh request omitted an explicit qualification", text, raw)
		}
	}
	return r
}

func TestHashChoiceFreshRequestCLIArgumentsValuesAndNoInitialization(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	choice := "hash-choice-v1-" + strings.Repeat("a", 64)
	selection := strings.Repeat("a", 64)
	bad := [][]string{
		{"hash", "--request-choice"}, {"hash", "--request-choice", "bad"},
		{"hash", "--request-choice", strings.ToUpper(choice)},
		{"hash", "--request-choice", choice + "\n"},
		{"hash", "--request-choice", choice[:len(choice)-1]},
		{"hash", "--request-choice", choice, "--request-choice", choice},
		{"hash", "--request-choice", choice, "-request-choice=" + choice},
		{"hash", "--request-choice", choice, "1"},
		{"hash", "--request-choice", choice, "--", "--json"},
		{"hash", "--", "--request-choice", choice},
		{"hashes", "--request-choice", choice},
	}
	for _, other := range [][]string{{"--select"}, {"--show", selection}, {"--save-choice", selection, "--keeper", "1", "2"}, {"--check-choice", choice}, {"--approve", selection}, {"--run", selection}, {"--revoke", selection}, {"--keeper", "1"}, {"-d", "/generated/missing"}, {"--directory", "/generated/missing"}, {"--from", "/generated/missing"}, {"--confirm-content-read=false"}, {"--max-day-bytes", "1"}, {"--max-total-bytes", "1"}} {
		bad = append(bad, append([]string{"hash", "--request-choice", choice}, other...))
	}
	for _, args := range bad {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
		hashChoiceCLIError(t, code, out.String(), stderr.String(), args[0], "invalid_arguments", 2)
	}
	for _, option := range []string{"--request-choice", "-request-choice"} {
		for _, args := range [][]string{{"hash", option, "--json"}, {"hash", option + "=--json"}} {
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &stderr)
			if code != 2 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatal("literal request option value enabled JSON", args, code, out.String(), stderr.String())
			}
			out.Reset()
			stderr.Reset()
			code = Run(context.Background(), append([]string{"--data-dir", base, "--json"}, args...), &out, &stderr)
			hashChoiceCLIError(t, code, out.String(), stderr.String(), "hash", "invalid_arguments", 2)
		}
	}
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hash", "--request-choice", choice, "--json"}, &out, &stderr)
	hashChoiceCLIError(t, code, out.String(), stderr.String(), "hash", "not_found", 1)
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("request parsing/missing storage initialized private state", err)
	}
}

func TestHashChoiceFreshRequestCLIOfflineExactScopeAndArchivedContext(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "3", "1")
	// Later consent changes do not renew consent or refresh archived context.
	code, raw, stderr := f.run(context.Background(), "hash", "--revoke", f.consent.ID, "--json")
	_ = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	var first inventory.HashKeeperChoiceFreshRequestReport
	for _, args := range [][]string{{"hash", "--request-choice", saved.ID, "--json"}, {"--json", "hash", "--request-choice", saved.ID}, {"hash", "--json", "-request-choice=" + saved.ID}} {
		code, raw, stderr := f.run(context.Background(), args...)
		r := hashFreshRequestCLIReport(t, code, raw, stderr)
		if r.ChoiceID != saved.ID || r.StoreID != f.proposal.StoreID || r.SelectionID != f.proposal.SelectionID || r.InventoryID != f.proposal.InventoryID || !reflect.DeepEqual(r.SourceLocator, *f.proposal.SourceLocator) || !reflect.DeepEqual(r.HistoricalChoice, saved) || len(r.Targets) != 3 {
			t.Fatal("request changed exact saved scope or archived choice", raw)
		}
		members := append([]inventory.SavedHashPreviewMember{saved.Record.Evidence.Keeper}, saved.Record.Evidence.Copies...)
		for i, target := range r.Targets {
			role := "copy"
			if i == 0 {
				role = "keeper"
			}
			ordinal, err := strconv.Atoi(members[i].WorkID)
			if err != nil || target.Role != role || !reflect.DeepEqual(target.Observation, members[i]) || !reflect.DeepEqual(target.Target, f.proposal.Targets[ordinal-1]) {
				t.Fatal("request expanded/reordered roles or omitted frozen target evidence", raw)
			}
		}
		if first.RequestID == "" {
			first = r
		} else if !reflect.DeepEqual(first, r) {
			t.Fatal("equivalent finite reviews changed the ephemeral request", raw)
		}
	}
	code, human, stderr := f.run(context.Background(), "hash", "--request-choice", saved.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{first.RequestID, "UNAPPROVED FRESH-READ REQUEST - NOTHING SAVED", "Selected keeper for review", "Selected copy for review", "Frozen ancestor records", "HISTORICAL CONTEXT FROM THE SAVED CHOICE", "New read approval", "Unavailable", "Unknown", "No source files, inventory or configuration were opened", "no previous approval was renewed", "not a job, read approval or cleanup token"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("human request omitted scope or saved-only qualification", want, code, human, stderr)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("offline request review wrote/recovered saved records or read source/configuration")
	}
}

func TestHashChoiceFreshRequestCLIMissingChoiceAndHeldWriter(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	w, err := inventory.OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, stderr := f.run(context.Background(), "hash", "--request-choice", saved.ID, "--json")
	_ = hashFreshRequestCLIReport(t, code, raw, stderr)
	missing := "hash-choice-v1-" + strings.Repeat("b", 64)
	code, raw, stderr = f.run(context.Background(), "hash", "--request-choice", missing, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "not_found", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
		t.Fatal("request needed a writer or changed saved bytes")
	}
}

func TestHashChoiceFreshRequestCLICapabilities(t *testing.T) {
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &stderr)
	var advertised struct {
		OK         bool            `json:"ok"`
		Features   map[string]bool `json:"features"`
		ErrorCodes []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &advertised); err != nil || code != 0 || stderr.Len() != 0 || !advertised.OK || !advertised.Features["fresh_hash_choice_requests"] || advertised.Features["duplicates"] || advertised.Features["cleanup"] || !slices.Contains(advertised.ErrorCodes, "hash_choice_request_unavailable") {
		t.Fatal("capabilities omitted request contract or advertised duplicate/cleanup authority", code, out.String(), stderr.String(), err)
	}
}

func TestHashChoiceFreshRequestCLIOutputAndCancellationNeverPublish(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	before := hashCLIBytes(t, f.base, f.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr := f.run(ctx, "hash", "--request-choice", saved.ID, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "canceled", 1)
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "hash", "--request-choice", saved.ID}
		if machine {
			args = append(args, "--json")
		}
		for _, short := range []bool{false, true} {
			var diagnostic bytes.Buffer
			if code := Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic); code != 1 || diagnostic.Len() == 0 || !strings.Contains(diagnostic.String(), "no request was saved") {
				t.Fatal("failed request output implied publication or success", code, diagnostic.String())
			}
		}
		var diagnostic bytes.Buffer
		fullError := &hashMetadataCountErrorWriter{}
		if code := Run(context.Background(), args, fullError, &diagnostic); code != 1 || !strings.Contains(diagnostic.String(), "no request was saved") {
			t.Fatal("full-count request output error was lost", code, diagnostic.String())
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		diagnostic.Reset()
		code = Run(ctx, args, out, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), "canceled") || !strings.Contains(diagnostic.String(), "no request was saved") {
			t.Fatal("canceled request reply implied publication or success", code, out.String(), diagnostic.String())
		}
		if machine {
			_ = hashFreshRequestCLIReport(t, 0, out.String(), "")
			_ = hashFreshRequestCLIReport(t, 0, fullError.String(), "")
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("failed/canceled output saved a request or altered history")
	}
}
