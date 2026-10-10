package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func choiceSizesCLIReport(t *testing.T, code int, raw, stderr string) inventory.HashKeeperChoiceSizesReport {
	t.Helper()
	var envelope struct {
		Version int                                   `json:"api_version"`
		OK      bool                                  `json:"ok"`
		Command string                                `json:"command"`
		Hashes  inventory.HashKeeperChoiceSizesReport `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "hashes" {
		t.Fatal("invalid derived choice response", code, raw, stderr, err)
	}
	if !inventory.ValidHashKeeperChoiceID(envelope.Hashes.ID) || envelope.Hashes.HistoricalCopySizes.Contract != inventory.HistoricalCopySizesContract {
		t.Fatal("derived response lost its existing ID or new contract", raw)
	}
	return envelope.Hashes
}

func TestHashKeeperChoiceSizesCLIOfflineAdditiveJSONAndUnchangedSave(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 4)
	ctx := context.Background()
	code, saveRaw, stderr := f.run(ctx, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "3", "1", "--json")
	saved := hashChoiceCLIReport(t, code, saveRaw, stderr, "hash")
	var saveEnvelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(saveRaw), &saveEnvelope); err != nil {
		t.Fatal(err)
	}
	var saveResult map[string]json.RawMessage
	if err := json.Unmarshal(saveEnvelope["hash"], &saveResult); err != nil || len(saveResult) != 2 || saveResult["id"] == nil || saveResult["record"] == nil || strings.Contains(saveRaw, "historical_copy_sizes") {
		t.Fatal("saving gained derived fields or changed immutable output", saveRaw, err)
	}
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, stderr := f.run(ctx, "hashes", "--choice", saved.ID, "--json")
	report := choiceSizesCLIReport(t, code, raw, stderr)
	if !reflect.DeepEqual(report.SavedHashKeeperChoice, saved) || report.Record.Evidence.Keeper.WorkID != "2" || report.Record.Evidence.Copies[0].WorkID != "3" || report.Record.Evidence.Copies[1].WorkID != "1" {
		t.Fatal("derived view changed chosen roles, context or ID", raw)
	}
	var showEnvelope, showResult map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &showEnvelope); err != nil || json.Unmarshal(showEnvelope["hashes"], &showResult) != nil || len(showResult) != 3 || !bytes.Equal(showResult["id"], saveResult["id"]) || !bytes.Equal(showResult["record"], saveResult["record"]) || len(showResult["historical_copy_sizes"]) > 1024 {
		t.Fatal("show changed original id/record bytes or exceeded summary bound", raw, err)
	}
	s := report.HistoricalCopySizes
	logical := int64(2) * saved.Record.Evidence.LogicalBytes
	allocated := saved.Record.Evidence.Copies[0].SavedAllocatedBytes + saved.Record.Evidence.Copies[1].SavedAllocatedBytes
	if s.SelectedCopyPaths != 2 || s.KnownSavedObjects != 2 || s.KnownAllocationPaths != 2 || s.LogicalBytes == nil || *s.LogicalBytes != logical || s.LogicalBytesStatus != "known" || s.SavedAllocatedBytes == nil || *s.SavedAllocatedBytes != allocated || s.SavedAllocatedBytesStatus != "known" || s.ReclaimableBytes != nil || s.CurrentStateVerified || s.IndependentAllocationVerified || s.Executable {
		t.Fatal("summary included keeper/unselected work or claimed savings", s)
	}
	code, retryRaw, stderr := f.run(ctx, "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "3", "1", "--json")
	if code != 0 || stderr != "" || retryRaw != saveRaw {
		t.Fatal("derived view changed exact save/retry output", code, retryRaw, stderr)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
		t.Fatal("saved sizes changed source, inventory or hash records")
	}
	code, human, stderr := f.run(ctx, "hashes", "--choice", saved.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{saved.ID, "HISTORICAL SIZES OF SELECTED POSSIBLE COPIES", "Selected possible copy paths 2", "Known saved objects among copies 2", "Historical logical total across selected copy paths", "Historical allocated total across selected copy paths", "totals exclude the keeper", "allocation may overlap shared extents", "reclaimable space remain unknown"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("human sizes lost denominator or historical qualification", want, code, human, stderr)
		}
	}
}

func TestHashKeeperChoiceSizesCLIReadCloseCancellationRetainRequestedID(t *testing.T) {
	id := "hash-choice-v1-" + strings.Repeat("a", 64)
	readCause, closeCause := errors.New("generated read refusal"), errors.New("generated close refusal")
	for _, kind := range []string{"read", "close", "canceled", "read_close_canceled"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var readErr, closeErr, want error
			report := inventory.HashKeeperChoiceSizesReport{}
			switch kind {
			case "read":
				readErr, closeErr, want = readCause, closeCause, readCause
			case "close":
				closeErr, want = closeCause, closeCause
			case "canceled":
				cancel()
				want = context.Canceled
			case "read_close_canceled":
				cancel()
				readErr, closeErr, want = readCause, closeCause, readCause
				report.ID = id
				report.HistoricalCopySizes.SelectedCopyPaths = 1
			}
			got, err := completeHashChoiceSizesView(ctx, id, report, readErr, closeErr)
			if got != nil || !errors.Is(err, want) || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "requested historical choice") || strings.Contains(err.Error(), "was saved") {
				t.Fatal("failed view lost requested ID or claimed publication", got, err)
			}
			if kind == "read_close_canceled" && (!errors.Is(err, readCause) || !errors.Is(err, closeCause) || !errors.Is(err, context.Canceled) || strings.Count(err.Error(), "requested historical choice") != 1) {
				t.Fatal("simultaneous read, close and cancellation causes were not retained in one requested-ID wrapper", got, err)
			}
		})
	}
	f := newHashGuidedReviewFixture(t, 3)
	code, raw, stderr := f.run(context.Background(), "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1", "--json")
	saved := hashChoiceCLIReport(t, code, raw, stderr, "hash")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr = f.run(canceled, "hashes", "--choice", saved.ID, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hashes", "canceled", 1)
	if !strings.Contains(raw, saved.ID) || strings.Contains(raw, "historical_copy_sizes") {
		t.Fatal("canceled read lost full ID or emitted partial summary", raw)
	}
}

func TestHashKeeperChoiceSizesCLIOutputFailureAndCancellationKeepID(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	code, raw, stderr := f.run(context.Background(), "hash", "--save-choice", f.proposal.SelectionID, "--keeper", "2", "1", "--json")
	saved := hashChoiceCLIReport(t, code, raw, stderr, "hash")
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("failed_machine_%t_short_%t", machine, short), func(t *testing.T) {
				args := []string{"--data-dir", f.base, "hashes", "--choice", saved.ID}
				if machine {
					args = append(args, "--json")
				}
				var diagnostic bytes.Buffer
				code := Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic)
				if code != 1 || !strings.Contains(diagnostic.String(), saved.ID) || !strings.Contains(diagnostic.String(), "hashes --choice") {
					t.Fatal("size output failure lost validated ID", code, diagnostic.String())
				}
			})
		}
		t.Run(fmt.Sprintf("canceled_machine_%t", machine), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &hashChoiceCancelWriter{cancel: cancel}
			var diagnostic bytes.Buffer
			args := []string{"--data-dir", f.base, "hashes", "--choice", saved.ID}
			if machine {
				args = append(args, "--json")
			}
			code := Run(ctx, args, out, &diagnostic)
			if code != 1 || !strings.Contains(diagnostic.String(), saved.ID) || !strings.Contains(diagnostic.String(), context.Canceled.Error()) || !strings.Contains(diagnostic.String(), "hashes --choice") {
				t.Fatal("successful-output cancellation lost validated ID", code, diagnostic.String())
			}
			if machine {
				d := json.NewDecoder(strings.NewReader(out.String()))
				var response map[string]any
				if err := d.Decode(&response); err != nil || response["ok"] != true || response["command"] != "hashes" {
					t.Fatal("completed response was not preserved", out.String(), err)
				}
				if err := d.Decode(&response); !errors.Is(err, io.EOF) {
					t.Fatal("cancellation appended a second machine result", out.String(), err)
				}
			}
		})
	}
}

func TestHashKeeperChoiceSizesCLIOverflowLabelsStayUnknown(t *testing.T) {
	logical, allocated := int64(0), int64(123)
	for _, tc := range []struct {
		value  *int64
		status string
		want   string
	}{{nil, "overflow", "Unknown (overflow)"}, {&logical, "known", humanBytes(0)}, {&allocated, "known", humanBytes(123)}, {nil, "known", "Unknown"}} {
		if got := historicalCopySizeText(tc.value, tc.status); got != tc.want {
			t.Fatal("size label confused missing/overflow/known zero", got, tc.want)
		}
	}
}
