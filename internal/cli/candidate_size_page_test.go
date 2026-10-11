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
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func candidateSizeCLIArgs(f guidedReviewFixture, options ...string) []string {
	args := []string{"--data-dir", f.base, "report", "--candidates", "-d", f.root, "--min-age-days", "30"}
	return append(args, options...)
}

func candidateSizeCLIReport(t *testing.T, f guidedReviewFixture, options ...string) (reportResult, string) {
	t.Helper()
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), candidateSizeCLIArgs(f, append(options, "--json")...), &out, &diagnostic)
	var envelope struct {
		Version int          `json:"api_version"`
		OK      bool         `json:"ok"`
		Command string       `json:"command"`
		Report  reportResult `json:"report"`
	}
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	if err := decoder.Decode(&envelope); err != nil || code != 0 || diagnostic.Len() != 0 || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "report" || envelope.Report.Candidates == nil {
		t.Fatal("candidate report failed", code, out.String(), diagnostic.String(), err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatal("candidate reply had trailing output", out.String(), err)
	}
	return envelope.Report, out.String()
}

func candidateSizeNoAuthority(t *testing.T, summary *state.CandidatePageSizeSummary) {
	t.Helper()
	if summary == nil || summary.Contract != "candidate_page_size_summary_v1" || summary.Source != "saved_inventory" || summary.Scope != "visible_candidate_page" || summary.EstimatedReclaimableBytes != nil || summary.CurrentStateVerified || summary.ContentVerified || summary.PhysicalAllocationVerified || summary.CrossFolderObjectDeduplicationVerified || summary.ApprovalAvailable || summary.Executable {
		t.Fatal("historical summary acquired authority or the wrong scope", summary)
	}
	body, err := json.Marshal(summary)
	if err != nil || len(body) > 4096 {
		t.Fatal("summary exceeded its finite JSON budget", len(body), err)
	}
}

func candidateSizeStableFindings(findings []state.Finding) []state.Finding {
	result := append([]state.Finding(nil), findings...)
	for i := range result {
		// Only the existing reply's wall timestamp differs between captures.
		result[i].Measurement.GeneratedAt = time.Time{}
	}
	return result
}

func TestCandidatePageSizeCLIParserRefusesBeforeStorage(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"numeric_true", []string{"--candidates", "--page-size-summary=1"}},
		{"short_true", []string{"--candidates", "--page-size-summary=t"}},
		{"uppercase_true", []string{"--candidates", "--page-size-summary=TRUE"}},
		{"numeric_false", []string{"--candidates", "--page-size-summary=0"}},
		{"empty_literal", []string{"--candidates", "--page-size-summary="}},
		{"invalid_literal", []string{"--candidates", "--page-size-summary=invalid"}},
		{"repeated_bare", []string{"--candidates", "--page-size-summary", "--page-size-summary"}},
		{"repeated_false", []string{"--candidates", "--page-size-summary=false", "--page-size-summary=false"}},
		{"mixed_values", []string{"--candidates", "--page-size-summary=true", "--page-size-summary=false"}},
		{"separate_boolean", []string{"--candidates", "--page-size-summary", "true"}},
		{"positional_tail", []string{"--candidates", "--page-size-summary", "tail"}},
		{"after_separator", []string{"--candidates", "--", "--page-size-summary"}},
		{"no_candidates", []string{"--page-size-summary"}},
		{"false_without_candidates", []string{"--page-size-summary=false"}},
		{"candidates_false", []string{"--candidates=false", "--page-size-summary"}},
		{"other_report", []string{"--same-size", "--page-size-summary"}},
		{"mixed_candidates", []string{"--candidates", "--same-size", "--page-size-summary"}},
		{"cargo_mode", []string{"--build-output", "--page-size-summary", "-d", "/generated-unused-root"}},
		{"go_mode", []string{"--go-cache", "--page-size-summary", "-d", "/generated-unused-root"}},
		{"limit", []string{"--candidates", "--page-size-summary", "--limit", "20"}},
		{"age_zero", []string{"--candidates", "--page-size-summary", "--min-age-days", "0"}},
		{"directory_aliases", []string{"--candidates", "--page-size-summary", "-d", "/generated-unused-root", "--directory", "/generated-unused-root"}},
	}
	for _, tc := range cases {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/machine_%t", tc.name, machine), func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "absent-instance")
				args := append([]string{"--data-dir", base, "report"}, tc.args...)
				if machine {
					args = append([]string{"--json"}, args...)
				}
				var out, diagnostic bytes.Buffer
				code := Run(context.Background(), args, &out, &diagnostic)
				if machine {
					buildOutputCLIError(t, code, out.String(), diagnostic.String(), "report", "invalid_arguments", 2)
				} else if code != 2 || diagnostic.Len() == 0 || out.Len() != 0 {
					t.Fatal("invalid selector produced evidence", code, out.String(), diagnostic.String())
				}
				if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("argument refusal initialized storage", err)
				}
			})
		}
	}
}

func TestCandidatePageSizeCLIDefaultFalseAndFindingContract(t *testing.T) {
	f := newGuidedReviewFixture(t, 3, false)
	before := hashCLIBytes(t, f.base, f.root)
	plain, plainJSON := candidateSizeCLIReport(t, f)
	disabled, disabledJSON := candidateSizeCLIReport(t, f, "--page-size-summary=false")
	if plain.CandidateSizeSummary != nil || disabled.CandidateSizeSummary != nil || strings.Contains(plainJSON, "candidate_size_summary") || strings.Contains(disabledJSON, "candidate_size_summary") || !reflect.DeepEqual(candidateSizeStableFindings(plain.Candidates.Findings), candidateSizeStableFindings(disabled.Candidates.Findings)) {
		t.Fatal("default or false changed the existing finding contract", plainJSON, disabledJSON)
	}
	// The absent optional sibling must serialize as the original report exactly.
	original, err := json.Marshal(plain.FileReport)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := json.Marshal(reportResult{FileReport: plain.FileReport})
	if err != nil || !bytes.Equal(original, unchanged) {
		t.Fatal("nil summary altered default serialized report bytes", string(original), string(unchanged), err)
	}
	for _, option := range []string{"--page-size-summary", "--page-size-summary=true"} {
		t.Run(option, func(t *testing.T) {
			r, raw := candidateSizeCLIReport(t, f, option)
			candidateSizeNoAuthority(t, r.CandidateSizeSummary)
			if r.CandidateSizeSummary.DisplayedFolders != 3 || r.CandidateSizeSummary.LogicalPathBytes == nil || *r.CandidateSizeSummary.LogicalPathBytes != 36 || r.CandidateSizeSummary.MeasuredFilePaths == nil || *r.CandidateSizeSummary.MeasuredFilePaths != 3 || r.CandidateSizeSummary.WithinFolderAllocatedBytes != nil || r.CandidateSizeSummary.AllocatedUnavailableReason != "cross_folder_identity_evidence_unavailable" || !reflect.DeepEqual(candidateSizeStableFindings(plain.Candidates.Findings), candidateSizeStableFindings(r.Candidates.Findings)) {
				t.Fatal("visible path denominator or immutable finding changed", raw)
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
				t.Fatal(err)
			}
			var reportFields map[string]json.RawMessage
			if err := json.Unmarshal(envelope["report"], &reportFields); err != nil || len(reportFields["candidate_size_summary"]) == 0 || len(reportFields["candidate_size_summary"]) > 4096 {
				t.Fatal("summary is not a bounded optional report sibling", raw, err)
			}
			if len(reportFields["candidates"]) == 0 || strings.Contains(string(reportFields["candidates"]), "candidate_size_summary") {
				t.Fatal("summary leaked into the saved finding payload", raw)
			}
		})
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("read-only summary changed inventory, plans or source")
	}
}

func TestCandidatePageSizeCLIScopeDismissalsAndNoRefill(t *testing.T) {
	f := newGuidedReviewFixture(t, 21, false)
	before := dismissalCandidateReport(t, f)
	saveFixtureDismissal(t, f, before.Findings[1].ID)
	bytesBefore := hashCLIBytes(t, f.base, f.root)
	r, raw := candidateSizeCLIReport(t, f, "--page-size-summary")
	candidateSizeNoAuthority(t, r.CandidateSizeSummary)
	assertDismissalPageCounts(t, before, *r.Candidates, 1)
	if r.CandidateSizeSummary.DisplayedFolders != 19 || len(r.Candidates.Findings) != 19 || r.CandidateSizeSummary.LogicalPathBytes == nil || *r.CandidateSizeSummary.LogicalPathBytes != 19*12 || r.Candidates.NextCursor != before.NextCursor {
		t.Fatal("summary refilled or counted hidden candidates", raw)
	}
	for _, finding := range r.Candidates.Findings {
		if finding.ID == before.Findings[1].ID || string(finding.PathBytes) != finding.Path || !strings.HasPrefix(finding.Path, f.root+string(filepath.Separator)) {
			t.Fatal("summary lost exact visible scope", finding)
		}
	}
	all, allJSON := candidateSizeCLIReport(t, f, "--page-size-summary", "--include-dismissed")
	if all.CandidateSizeSummary.DisplayedFolders != 20 || len(all.Candidates.Findings) != 20 || all.CandidateSizeSummary.LogicalPathBytes == nil || *all.CandidateSizeSummary.LogicalPathBytes != 240 || all.Candidates.NextCursor != before.NextCursor {
		t.Fatal("inspection flag did not use the same original page", allJSON)
	}
	if !reflect.DeepEqual(bytesBefore, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("visibility summary changed dismissal or saved source evidence")
	}
	t.Run("empty_visible_subset", func(t *testing.T) {
		empty := newGuidedReviewFixture(t, 1, false)
		saveFixtureDismissal(t, empty, empty.findings[0].ID)
		result, raw := candidateSizeCLIReport(t, empty, "--page-size-summary")
		candidateSizeNoAuthority(t, result.CandidateSizeSummary)
		s := result.CandidateSizeSummary
		if len(result.Candidates.Findings) != 0 || s.Status != "empty_selection" || s.DisplayedFolders != 0 || s.LogicalPathBytes == nil || *s.LogicalPathBytes != 0 || s.MeasuredFilePaths == nil || *s.MeasuredFilePaths != 0 || s.WithinFolderAllocatedBytes == nil || *s.WithinFolderAllocatedBytes != 0 {
			t.Fatal("all-dismissed page became unknown or counted hidden rows", raw)
		}
	})
}

func TestCandidatePageSizeCLIOfflineOnePageAndCursor(t *testing.T) {
	f := newGuidedReviewFixture(t, 21, false)
	if err := os.Rename(f.root, f.root+"-offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root+"-offline")
	first, raw := candidateSizeCLIReport(t, f, "--page-size-summary")
	candidateSizeNoAuthority(t, first.CandidateSizeSummary)
	if first.CandidateSizeSummary.DisplayedFolders != 20 || first.CandidateSizeSummary.LogicalPathBytes == nil || *first.CandidateSizeSummary.LogicalPathBytes != 240 || first.Candidates.NextCursor == "" {
		t.Fatal("first historical page did not remain bounded", raw)
	}
	second, raw := candidateSizeCLIReport(t, f, "--page-size-summary", "--cursor", first.Candidates.NextCursor)
	if second.CandidateSizeSummary.DisplayedFolders != 1 || second.CandidateSizeSummary.LogicalPathBytes == nil || *second.CandidateSizeSummary.LogicalPathBytes != 12 || second.Candidates.NextCursor != "" || second.Candidates.Findings[0].ID == first.Candidates.Findings[0].ID {
		t.Fatal("next observation accumulated the previous page", raw)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	result, err := report(context.Background(), []string{"--candidates", "--page-size-summary", "--include-dismissed", "-d", f.root, "--min-age-days", "30"}, paths)
	if err != nil {
		t.Fatal(err)
	}
	want := candidateReportCommand(paths, f.root, 30) + " --page-size-summary --include-dismissed"
	var human bytes.Buffer
	printReport(&human, result)
	if result.candidateCommand != want || !strings.Contains(human.String(), want+" --cursor "+shellQuote(result.Candidates.NextCursor)) {
		t.Fatal("copyable cursor command lost original selection options", result.candidateCommand, human.String())
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+"-offline")) {
		t.Fatal("offline saved-page report touched source or history")
	}
}

func TestCandidatePageSizeCLIPartialAndLegacyRemainQualified(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 2, true)
		before := hashCLIBytes(t, f.base, f.root)
		r, raw := candidateSizeCLIReport(t, f, "--page-size-summary")
		candidateSizeNoAuthority(t, r.CandidateSizeSummary)
		if r.CandidateSizeSummary.Status != "historical_measured_portion" || r.CandidateSizeSummary.PartialFolders != 2 || r.CandidateSizeSummary.WithinFolderAllocatedBytes != nil {
			t.Fatal("unfinished saved inventory became whole/current evidence", raw)
		}
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
			t.Fatal("partial report settled or changed unfinished work")
		}
	})
	t.Run("legacy_schema8", func(t *testing.T) {
		f := newGuidedReviewFixture(t, 1, false)
		database := filepath.Join(f.state, state.Filename)
		db, err := sql.Open("sqlite", database)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec("PRAGMA user_version=8")
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		before := hashCLIBytes(t, f.base, f.root)
		plain, _ := candidateSizeCLIReport(t, f)
		if len(plain.Candidates.Findings) != 1 || plain.CandidateSizeSummary != nil {
			t.Fatal("legacy default report changed", plain)
		}
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), candidateSizeCLIArgs(f, "--page-size-summary", "--json"), &out, &diagnostic)
		buildOutputCLIError(t, code, out.String(), diagnostic.String(), "report", "command_failed", 1)
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
			t.Fatal("legacy refusal migrated or modified storage")
		}
	})
}

type candidateSizeReaderFixture struct {
	store            *state.Store
	readError        error
	closeError       error
	cancelAfterClose context.CancelFunc
	omitReceipt      bool
	reads, closes    int
}

func (f *candidateSizeReaderFixture) NodeModulesSizePage(ctx context.Context, cursor string, age int) (state.SelectionSnapshot, *state.CandidateSizePage, error) {
	f.reads++
	if f.readError != nil {
		return state.SelectionSnapshot{}, nil, f.readError
	}
	page, receipt, err := f.store.NodeModulesSizePage(ctx, cursor, age)
	if f.omitReceipt {
		receipt = nil
	}
	return page, receipt, err
}

func (f *candidateSizeReaderFixture) Close() error {
	f.closes++
	var err error
	if f.store != nil {
		err = f.store.Close()
	}
	if f.cancelAfterClose != nil {
		f.cancelAfterClose()
	}
	return errors.Join(err, f.closeError)
}

func TestCandidatePageSizeCLIRetainedReceiptReadCloseAndCancellation(t *testing.T) {
	f := newGuidedReviewFixture(t, 1, false)
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root)
	readCause, closeCause := errors.New("fixture bounded read refused"), errors.New("fixture checked close refused")
	for _, label := range []string{"success", "read_and_close_error", "close_error", "canceled_after_close", "missing_receipt", "invalid_cursor"} {
		t.Run(label, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &candidateSizeReaderFixture{}
			cursor := ""
			if label != "read_and_close_error" {
				reader.store, err = state.OpenReader(ctx, f.state)
				if err != nil {
					t.Fatal(err)
				}
			}
			switch label {
			case "read_and_close_error":
				reader.readError, reader.closeError = readCause, closeCause
			case "close_error":
				reader.closeError = closeCause
			case "canceled_after_close":
				reader.cancelAfterClose = cancel
			case "missing_receipt":
				reader.omitReceipt = true
			case "invalid_cursor":
				cursor = "not-a-page-cursor"
			}
			result, got := readCandidateSizePage(ctx, reader, paths, f.root, cursor, 30, false)
			if reader.reads != 1 || reader.closes != 1 {
				t.Fatal("summary reread or failed to close the captured inventory", reader.reads, reader.closes)
			}
			if label == "success" {
				candidateSizeNoAuthority(t, result.CandidateSizeSummary)
				if got != nil || result.CandidateSizeSummary.DisplayedFolders != 1 || result.CandidateSizeSummary.WithinFolderAllocatedBytes == nil || *result.CandidateSizeSummary.WithinFolderAllocatedBytes != 4096 {
					t.Fatal("single-folder known allocation disappeared", result, got)
				}
				return
			}
			if got == nil || result.Candidates != nil || result.CandidateSizeSummary != nil {
				t.Fatal("failed read/close/cancellation published a successful summary", result, got)
			}
			switch label {
			case "read_and_close_error":
				if !errors.Is(got, readCause) || !errors.Is(got, closeCause) {
					t.Fatal("read failure lost checked-close cause", got)
				}
			case "close_error":
				if !errors.Is(got, closeCause) {
					t.Fatal("checked-close failure became success", got)
				}
			case "canceled_after_close":
				if !errors.Is(got, context.Canceled) {
					t.Fatal("late cancellation lost its cause", got)
				}
			case "invalid_cursor":
				var usage usageError
				if !errors.As(got, &usage) || !errors.Is(got, state.ErrReportCursor) {
					t.Fatal("bad cursor was not a usage refusal", got)
				}
			}
		})
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("failure/close fixtures changed saved inventory or source")
	}
}

func TestCandidatePageSizeCLIOutputFailureAndLateCancellation(t *testing.T) {
	f := newGuidedReviewFixture(t, 1, false)
	before := hashCLIBytes(t, f.base, f.root)
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("writer_machine_%t_short_%t", machine, short), func(t *testing.T) {
				args := candidateSizeCLIArgs(f, "--page-size-summary")
				if machine {
					args = append(args, "--json")
				}
				var diagnostic bytes.Buffer
				if code := Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic); code != 1 || diagnostic.Len() == 0 {
					t.Fatal("broken output acknowledged a complete summary", code, diagnostic.String())
				}
			})
		}
		t.Run(fmt.Sprintf("late_cancel_machine_%t", machine), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &hashChoiceCancelWriter{cancel: cancel}
			args := candidateSizeCLIArgs(f, "--page-size-summary")
			if machine {
				args = append(args, "--json")
			}
			var diagnostic bytes.Buffer
			code := Run(ctx, args, out, &diagnostic)
			if code != 1 || !strings.Contains(diagnostic.String(), context.Canceled.Error()) || !strings.Contains(diagnostic.String(), "Historical candidate page size") {
				t.Fatal("completed-output cancellation was acknowledged as success", code, out.String(), diagnostic.String())
			}
			if machine {
				var reply map[string]any
				decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
				if err := decoder.Decode(&reply); err != nil || reply["ok"] != true || reply["command"] != "report" {
					t.Fatal("completed machine reply was lost", out.String(), err)
				}
				if err := decoder.Decode(&reply); !errors.Is(err, io.EOF) {
					t.Fatal("late cancellation appended another machine reply", out.String(), err)
				}
			}
		})
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("output failures mutated saved records or source")
	}
}

func TestCandidatePageSizeCLIHumanLabelsAndCapabilities(t *testing.T) {
	f := newGuidedReviewFixture(t, 1, false)
	var out, diagnostic bytes.Buffer
	if code := Run(context.Background(), candidateSizeCLIArgs(f, "--page-size-summary"), &out, &diagnostic); code != 0 || diagnostic.Len() != 0 {
		t.Fatal(code, out.String(), diagnostic.String())
	}
	for _, text := range []string{"Historical sizes on this page", "Historical logical bytes by file path", "Historical measured file paths", "Historical within-folder allocated bytes", "12 bytes", "4096 bytes", "Space you could free is unknown", "Dismissals use a separate", f.findings[0].ID, fmt.Sprintf("%q", f.findings[0].Path)} {
		if !strings.Contains(strings.Join(strings.Fields(out.String()), " "), text) {
			t.Fatal("historical human view lost scope, exact identity or denominator", text, out.String())
		}
	}
	var narrow bytes.Buffer
	writeField(&narrow, "Historical logical bytes by file path", candidateSizeAmount(nil, "measurement_unavailable"), 32)
	if !strings.Contains(narrow.String(), "Historical logical bytes by file path:\n") || !strings.Contains(narrow.String(), "unknown") || !strings.Contains(strings.Join(strings.Fields(narrow.String()), " "), "a saved size measurement is not available") || strings.Contains(narrow.String(), "measurement_unavailable") {
		t.Fatal("narrow output lost the explicit unknown qualification", narrow.String())
	}
	for _, tc := range []struct {
		reason string
		want   string
	}{
		{"overlap_detected", "unknown (folder paths overlap)"},
		{"measurement_unavailable", "unknown (a saved size measurement is not available)"},
		{"aggregate_overflow", "unknown (the total exceeds the supported range)"},
		{"single_folder_incomplete_or_stale", "unknown (saved folder evidence is incomplete or stale)"},
		{"single_folder_identity_unavailable", "unknown (saved folder or file identities are not available)"},
		{"cross_folder_identity_evidence_unavailable", "unknown (file identities across folders are not available)"},
		{"", "unknown"},
		{"future_unrecognized_reason", "unknown"},
	} {
		t.Run("human_unknown_"+tc.reason, func(t *testing.T) {
			for _, got := range []string{candidateSizeAmount(nil, tc.reason), candidateSizeCount(nil, tc.reason)} {
				if got != tc.want || (tc.reason != "" && strings.Contains(got, tc.reason)) {
					t.Fatal("human unknown value exposed a machine reason or lost its qualification", tc.reason, got)
				}
			}
			var rendered bytes.Buffer
			printCandidateSizeSummary(&rendered, state.CandidatePageSizeSummary{LogicalUnavailableReason: tc.reason, FilePathsUnavailableReason: tc.reason, AllocatedUnavailableReason: tc.reason})
			if !strings.Contains(strings.Join(strings.Fields(rendered.String()), " "), tc.want) || (tc.reason != "" && strings.Contains(rendered.String(), tc.reason)) {
				t.Fatal("human summary lost plain unknown wording", tc.reason, rendered.String())
			}
			value := int64(4096)
			if candidateSizeAmount(&value, tc.reason) != fmt.Sprintf("%s (4096 bytes)", humanBytes(value)) || candidateSizeCount(&value, tc.reason) != humanCount(value) {
				t.Fatal("unknown wording changed a known amount or count", tc.reason)
			}
		})
	}
	caps := capabilities()
	contract, ok := caps["candidate_page_size_summary_contract"].(map[string]any)
	features, featureOK := caps["features"].(map[string]bool)
	if !ok || !featureOK || !features["candidate_page_size_summaries"] || contract["maximum_folders"] != 20 || contract["maximum_summary_JSON_bytes"] != 4096 || contract["same_inventory_snapshot"] != true || contract["dismissal_snapshot_separate"] != true || contract["source_operations"] != false || contract["executable"] != false || contract["approval_available"] != false || contract["estimated_reclaimable_bytes"] != nil {
		t.Fatal("capability disclosure changed historical/authority bounds", contract, features)
	}
	encoded, err := json.Marshal(caps)
	if err != nil || !strings.Contains(string(encoded), "--page-size-summary[=true|false]") || !strings.Contains(string(encoded), "once-only with --candidates") {
		t.Fatal("capabilities omitted the strict option contract", string(encoded), err)
	}
}
