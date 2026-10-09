package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestAPIPacingStatusKeepsProcessLocalEvidenceAndOutputFailure(t *testing.T) {
	for _, ready := range []bool{true, false} {
		p := worker.APIPacingSnapshot{RatePerSecond: 5000, CapacityReady: ready, WaitNS: uint64(time.Second), Waiting: true}
		var out bytes.Buffer
		printWorkerAPIPacing(&out, &p)
		text := strings.Join(strings.Fields(out.String()), " ")
		for _, want := range []string{"5000 attempts/s", "last status sample: YES", "Accumulated pacing wait: 1s", "process-local spacing", "all six source-scanner API kinds", "separate from entry pacing", "Status does not inspect source folders", "eligible saved-data work and controls", "not a global, byte or physical I/O limit", "Manual scans, hash reads and database work", "can outlast cancellation"} {
			if !strings.Contains(text, want) {
				t.Fatal(want, text)
			}
		}
		if !ready && !strings.Contains(text, "SOURCE DEFERRED") {
			t.Fatal(text)
		}
		raw, err := json.Marshal(worker.Snapshot{APIPacing: &p})
		if err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Pacing worker.APIPacingSnapshot `json:"api_pacing"`
		}
		if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Pacing != p {
			t.Fatal(string(raw), err)
		}
	}
	var out bytes.Buffer
	printWorkerAPIPacing(&out, nil)
	raw, err := json.Marshal(worker.Snapshot{})
	if err != nil || out.Len() != 0 || bytes.Contains(raw, []byte("api_pacing")) {
		t.Fatal(out.String(), string(raw), err)
	}
	guard := &reviewOutput{writer: serviceFailWriter{}}
	printWorkerAPIPacing(guard, &worker.APIPacingSnapshot{RatePerSecond: 100000, WaitNS: math.MaxUint64})
	if guard.err == nil {
		t.Fatal("lost output failure")
	}
}

func TestAPIPacingCapabilitiesAndErrorsStayNarrow(t *testing.T) {
	caps := capabilities()
	features := caps["features"].(map[string]bool)
	if !features["scanner_api_pacing"] || features["metadata_rate_limit"] || features["cpu_limit"] || features["cleanup"] {
		t.Fatal(features)
	}
	c := caps["scanner_api_pacing_contract"].(map[string]any)
	if c["default_rate_per_second"] != 0 || c["maximum_rate_per_second"] != 100000 || c["minimum_next_attempts"] != int64(32794) || c["final_validation_attempts"] != int64(16393) || c["child_attempts"] != int64(6) {
		t.Fatal(c)
	}
	for _, key := range []string{"manual_scans_included", "hash_reads_included", "database_work_included", "global_rate_limit", "physical_io_measured", "unused_reservations_refunded", "status_inspects_sources"} {
		if c[key] != false {
			t.Fatal(key, c)
		}
	}
	for _, item := range []struct {
		err  error
		code string
	}{{inventory.ErrAPIPacingInput, "api_pacing_invalid"}, {inventory.ErrAPIPacingProfile, "api_pacing_unsupported"}, {errors.Join(inventory.ErrAPIPacingProfile, context.Canceled), "canceled"}} {
		var out, stderr bytes.Buffer
		if exit := operationFailure(&out, &stderr, "status", item.err); exit != 1 {
			t.Fatal(exit, out.String(), stderr.String())
		}
		var reply struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.Error.Code != item.code {
			t.Fatal(out.String(), err)
		}
		found := false
		for _, code := range caps["error_codes"].([]string) {
			found = found || code == item.code
		}
		if !found {
			t.Fatal(item.code)
		}
	}
}

func TestAPIPacingMachineStatusPreservesExactNestedNumbers(t *testing.T) {
	input := []byte(`{"worker":{"live":{"api_pacing":{"rate_per_second":100000,"wait_ns":18446744073709551615,"waiting":false,"capacity_ready":true},"metadata":{"large_counter":9007199254740993},"signed":{"minimum":-9223372036854775808}}},"state":{"entries":0},"unrecorded":null}`)
	result := map[string]any{"api_version": APIVersion, "ok": true, "command": "status"}
	if err := decodeMachineStatus(input, &result); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := emit(&out, &stderr, result, 0); code != 0 || stderr.Len() != 0 {
		t.Fatal(code, stderr.String())
	}
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	decoder.UseNumber()
	var got map[string]any
	if err := decoder.Decode(&got); err != nil {
		t.Fatal(err)
	}
	live := got["worker"].(map[string]any)["live"].(map[string]any)
	for _, item := range []struct{ section, key, want string }{{"api_pacing", "wait_ns", "18446744073709551615"}, {"metadata", "large_counter", "9007199254740993"}, {"signed", "minimum", "-9223372036854775808"}} {
		value, ok := live[item.section].(map[string]any)[item.key].(json.Number)
		if !ok || value.String() != item.want {
			t.Fatal("status rounded typed integer", item, out.String())
		}
	}
	if got["unrecorded"] != nil || got["state"].(map[string]any)["entries"].(json.Number).String() != "0" || got["command"] != "status" {
		t.Fatal(out.String())
	}
	for _, bad := range []string{`{"state":{"entries":01}}`, `{"state":`, string(input) + ` {"second":true}`, string(input) + ` broken`} {
		result := map[string]any{}
		if err := decodeMachineStatus([]byte(bad), &result); err == nil {
			t.Fatal("status accepted malformed or trailing result", bad)
		}
	}
}
