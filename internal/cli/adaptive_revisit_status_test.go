package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestAdaptiveRevisitStatusKeepsHistoricalEvidenceAndOutputFailure(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 1, time.UTC)
	p := worker.AdaptiveRevisitSnapshot{PolicyEnabled: true, CachedAt: at, AdaptiveRevisitStatus: state.AdaptiveRevisitStatus{Contract: state.AdaptiveRevisitContract, TrackedRoots: 2, ActiveEpochs: 1, UnknownEpochs: 1, DailyRoots: 1, WeeklyRoots: 1, StableRoots: 1, HistoricalMetadataOnly: true}}
	var out bytes.Buffer
	printWorkerAdaptiveRevisits(&out, &p)
	text := strings.Join(strings.Fields(out.String()), " ")
	for _, want := range []string{"Adaptive revisits: ENABLED", "saved passes in progress: 1", "daily=1; weekly=1", "uncertain evidence: 1", "cached historical metadata", "Status does not inspect source folders", "After saved work drains", "do not prove current contents", "resource gates remain in force"} {
		if !strings.Contains(text, want) {
			t.Fatal(want, text)
		}
	}
	encoded, err := json.Marshal(worker.Snapshot{AdaptiveRevisits: &p})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Policy worker.AdaptiveRevisitSnapshot `json:"adaptive_revisits"`
	}
	if err = json.Unmarshal(encoded, &decoded); err != nil || decoded.Policy.CurrentContentVerified || decoded.Policy.CleanupApproved || !decoded.Policy.HistoricalMetadataOnly || decoded.Policy.UnknownEpochs != 1 || !decoded.Policy.CachedAt.Equal(at) {
		t.Fatal(string(encoded), err)
	}
	guard := &reviewOutput{writer: serviceFailWriter{}}
	printWorkerAdaptiveRevisits(guard, &p)
	if guard.err == nil {
		t.Fatal("failed status output lost its error")
	}
	out.Reset()
	printWorkerAdaptiveRevisits(&out, nil)
	if out.Len() != 0 {
		t.Fatal("ordinary/disabled worker invented adaptive summary", out.String())
	}
}

func TestAdaptiveRevisitCapabilitiesAndErrorsStayNarrow(t *testing.T) {
	caps := capabilities()
	features := caps["features"].(map[string]bool)
	if !features["adaptive_inventory_revisits"] || features["portable_directory_continuation"] || features["cpu_limit"] || features["cleanup"] {
		t.Fatal(features)
	}
	contract := caps["adaptive_revisit_contract"].(map[string]any)
	if contract["name"] != state.AdaptiveRevisitContract || contract["initial_or_changed_or_unknown_interval_ns"] != int64(24*time.Hour) || contract["stable_interval_ns"] != int64(7*24*time.Hour) || contract["required_unchanged_epochs"] != 2 || contract["maximum_admitted_roots"] != state.MaxFairInventoryRoots {
		t.Fatal(contract)
	}
	for _, key := range []string{"enabled_by_default", "status_inspects_sources", "manual_scans_included", "current_content_verified", "inactivity_verified", "cleanup_approved"} {
		if contract[key] != false {
			t.Fatal(key, contract)
		}
	}
	for _, item := range []struct {
		err  error
		code string
	}{{state.ErrAdaptiveRevisitInput, "adaptive_revisit_invalid"}, {state.ErrAdaptiveRevisitCorrupt, "adaptive_revisit_invalid"}, {state.ErrAdaptiveRevisitClock, "adaptive_revisit_clock"}, {errors.Join(state.ErrAdaptiveRevisitClock, context.Canceled), "canceled"}} {
		var out, errOut bytes.Buffer
		if exit := operationFailure(&out, &errOut, "status", item.err); exit != 1 {
			t.Fatal(exit, out.String(), errOut.String())
		}
		var reply struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.Error.Code != item.code {
			t.Fatal(out.String(), err)
		}
		advertised := false
		for _, code := range caps["error_codes"].([]string) {
			advertised = advertised || code == item.code
		}
		if !advertised {
			t.Fatal("projected code missing from capabilities", item.code)
		}
	}
}
