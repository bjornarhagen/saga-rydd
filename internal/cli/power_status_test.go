package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestPowerStatusHistoricalDecisionAndNullableObservation(t *testing.T) {
	for _, witnessed := range []bool{false, true} {
		var witness *bool
		if witnessed {
			v := true
			witness = &v
		}
		at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
		id := uint64(7)
		backoff, returned := true, true
		power := worker.PowerPolicySnapshot{Contract: worker.PowerPolicyContract, PolicyEnabled: true, LastDecisionStatus: "observing", LastDecisionReason: "power_sample_pending", LastDecisionAt: &at, LastSourceBackoff: &backoff, TicketID: &id, SampleLaunchAt: &at, SampleCompletedAt: &at, CallbackReturned: &returned, SampleStatus: "completed", Observation: &powerinfo.Observation{Profile: "unsupported", SystemBatteryDischargingObserved: witness}, SourceOnly: true}
		var out bytes.Buffer
		printWorkerPower(&out, &power)
		text := out.String()
		for _, want := range []string{"Last source power decision: observing", "Power check finished: true", "last source admission decision", "not a power quota"} {
			if !strings.Contains(text, want) {
				t.Fatal(want, text)
			}
		}
		if witnessed != strings.Contains(text, "OBSERVED IN THIS SAMPLE") || (!witnessed && !strings.Contains(text, "System-battery discharge: UNKNOWN")) {
			t.Fatal(text)
		}
		encoded, err := json.Marshal(worker.Snapshot{Power: &power})
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]json.RawMessage
		if err = json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		var report map[string]json.RawMessage
		if err = json.Unmarshal(decoded["power"], &report); err != nil {
			t.Fatal(err)
		}
		if string(report["physical_power_verified"]) != "false" || string(report["last_wait_ns"]) != "null" {
			t.Fatal(string(encoded))
		}
	}
	var empty bytes.Buffer
	printWorkerPower(&empty, nil)
	if empty.Len() != 0 {
		t.Fatal(empty.String())
	}
}

func TestPowerCapabilitiesQualified(t *testing.T) {
	features := capabilities()["features"].(map[string]bool)
	if !features["bounded_power_observations"] || !features["source_discharge_backoff"] || features["power_controls"] {
		t.Fatal(features)
	}
	contract := capabilities()["power_policy_contract"].(map[string]any)
	if contract["status_starts_probe"] != false || contract["physical_power_verified"] != false || contract["scanner_allowances_include_power_calls"] != false {
		t.Fatal(contract)
	}
}
