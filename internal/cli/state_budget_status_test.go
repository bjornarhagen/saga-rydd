package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestInventoryStateStatusUnknownAndKnownZeroRemainDistinct(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"not_evaluated", "unavailable", "limit_reached"} {
		p := worker.InventoryStatePolicySnapshot{Contract: worker.InventoryStatePolicyContract, PolicyEnabled: true, SourceOnly: true, Limit: config.DefaultMaxStateBytes, RetryIntervalNS: int64(5 * time.Minute), LastDecisionStatus: "not_evaluated"}
		if mode != "not_evaluated" {
			backoff := true
			retry := at.Add(5 * time.Minute)
			p.LastDecisionStatus, p.LastAttemptPhase, p.LastSourceBackoff, p.LastAttemptStartedAt, p.LastAttemptFinishedAt, p.RetryStartedAt, p.RetryAfterAt = "source_deferred", "after_receipt", &backoff, &at, &at, &at, &retry
			p.LastDecisionReason = "inventory_state_unavailable"
		}
		if mode == "limit_reached" {
			database, wal, total := p.Limit, int64(0), p.Limit
			p.LastDecisionReason = "inventory_state_limit"
			p.Observation = &state.InventoryStateBudget{Contract: state.InventoryStateBudgetContract, Scope: "configured_inventory_database_and_wal", Available: true, Status: mode, Reason: "inventory_state_limit", Limit: p.Limit, DatabaseBytes: &database, WALBytes: &wal, TotalBytes: &total, SampleStartedAt: at, SampleFinishedAt: at, SequentialObservations: true}
		}
		var out bytes.Buffer
		printWorkerInventoryState(&out, &p)
		text := strings.Join(strings.Fields(out.String()), " ")
		if mode == "limit_reached" {
			if !strings.Contains(text, "WAL logical length: 0 bytes") || strings.Contains(text, "State-length sample: NOT RECORDED") {
				t.Fatal(text)
			}
		} else if !strings.Contains(text, "State-length sample: NOT RECORDED") || strings.Contains(text, "WAL logical length: 0 bytes") {
			t.Fatal("unknown became measured zero", text)
		}
		for _, want := range []string{"cached source admission evidence", "other stores are not measured", "process-local"} {
			if !strings.Contains(text, want) {
				t.Fatal(want, text)
			}
		}
		encoded, err := json.Marshal(worker.Snapshot{InventoryState: &p})
		if err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Policy worker.InventoryStatePolicySnapshot `json:"inventory_state"`
		}
		if err = json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Policy.Persistent || decoded.Policy.Observation != nil && (decoded.Policy.Observation.HardLimitEnforced || decoded.Policy.Observation.PhysicalAllocationVerified || decoded.Policy.Observation.OtherStoresIncluded) {
			t.Fatal(string(encoded))
		}
		if mode != "limit_reached" && !bytes.Contains(encoded, []byte(`"observation":null`)) {
			t.Fatal(string(encoded))
		}
	}
	var empty bytes.Buffer
	printWorkerInventoryState(&empty, nil)
	if empty.Len() != 0 {
		t.Fatal(empty.String())
	}
}

func TestInventoryStateCapabilitiesQualifyAdmissionThreshold(t *testing.T) {
	features := capabilities()["features"].(map[string]bool)
	if !features["inventory_state_length_observations"] || !features["source_state_threshold"] || features["hard_state_byte_limit"] {
		t.Fatal(features)
	}
	contract := capabilities()["inventory_state_admission_contract"].(map[string]any)
	if contract["name"] != worker.InventoryStatePolicyContract || contract["observation_contract"] != state.InventoryStateBudgetContract || contract["default_limit_bytes"] != config.DefaultMaxStateBytes || contract["retry_interval_ns"] != int64(5*time.Minute) {
		t.Fatal(contract)
	}
	for _, key := range []string{"persistent_retry", "status_samples_files", "hard_limit_enforced", "physical_allocation_verified", "other_stores_included", "scanner_allowances_include_state_samples", "entered_metadata_calls_interruptible"} {
		if contract[key] != false {
			t.Fatal(key, contract)
		}
	}
}
