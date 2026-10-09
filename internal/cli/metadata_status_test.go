package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestScannerMetadataStatusUnknownAndChargedUsage(t *testing.T) {
	started := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		budget     *state.MetadataBudget
		want, omit []string
	}{
		{name: "older worker", omit: []string{"scanner"}},
		{name: "old schema", budget: &state.MetadataBudget{Status: "unavailable", Limit: 100, PreTrackingUsage: "unknown"}, want: []string{"unavailable", "Earlier usage is unknown", "Daily scanner charges: Unknown"}, omit: []string{"Reserved for saved day"}},
		{name: "untracked", budget: &state.MetadataBudget{Available: true, Status: "untracked", Limit: 100, PreTrackingUsage: "unknown"}, want: []string{"untracked", "Daily scanner charges: Unknown"}, omit: []string{"Reserved since tracking began"}},
		{name: "tracked zero", budget: &state.MetadataBudget{Available: true, Status: "tracked", Day: "2026-10-09", Limit: 100, TrackingStartedAt: &started, PreTrackingUsage: "unknown", DayCharges: &state.MetadataCharges{}, TotalCharges: &state.MetadataCharges{}}, want: []string{"Reserved for saved day (2026-10-09 UTC): 0; observed: 0", "usage before tracking: unknown", "Unused allowances are not refunded"}, omit: []string{"Daily scanner charges: Unknown"}},
		{name: "outstanding old day", budget: &state.MetadataBudget{Available: true, Status: "tracked", Day: "2026-10-08", Limit: 100, TrackingStartedAt: &started, PreTrackingUsage: "unknown", DayCharges: &state.MetadataCharges{Reserved: 100, OutstandingReserved: 100}}, want: []string{"Reserved for saved day (2026-10-08 UTC): 100", "outstanding: 100"}, omit: []string{"Reserved today"}},
		{name: "interrupted", budget: &state.MetadataBudget{Available: true, Status: "tracked", Day: "2026-10-09", Limit: 100, TrackingStartedAt: &started, PreTrackingUsage: "unknown", DayCharges: &state.MetadataCharges{Reserved: 90, Observed: 5, UnknownReserved: 40, OutstandingReserved: 30, KnownUnusedReserved: 15}, TotalCharges: &state.MetadataCharges{Reserved: 90}, Reason: "daily_metadata_limit", NextAllowedAt: &started}, want: []string{"observed: 5; unknown charge: 40; outstanding: 30; known unused charge: 15", "daily_metadata_limit", "exclude manual scans", "do not measure syscalls or physical I/O", "Saved views do not recover work"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			printScannerMetadata(&out, test.budget)
			for _, want := range test.want {
				if !strings.Contains(out.String(), want) {
					t.Fatal("missing allowance qualification", want, out.String())
				}
			}
			for _, omit := range test.omit {
				if strings.Contains(out.String(), omit) {
					t.Fatal("unknown allowance was invented", omit, out.String())
				}
			}
		})
	}
	data, err := json.Marshal(capabilities())
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Features map[string]bool `json:"features"`
		Contract struct {
			Name, Scope string
			Manual      bool `json:"manual_scans_included"`
			Refund      bool `json:"unused_reservations_refunded"`
			Physical    bool `json:"physical_io_measured"`
		} `json:"scanner_metadata_contract"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Features["durable_scanner_api_allowances"] || report.Features["metadata_rate_limit"] || report.Features["cpu_limit"] || report.Features["power_controls"] || report.Contract.Name != state.MetadataBudgetContract || report.Contract.Scope != "experimental_background_scanner_source_apis" || report.Contract.Manual || report.Contract.Refund || report.Contract.Physical {
		t.Fatal("scanner-only allowance became a global resource claim", string(data))
	}
}

func TestScannerMetadataSavedStatusDoesNotRecover(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "state")
	run := func(args ...string) (int, string, string) {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), append([]string{"--data-dir", dir}, args...), &out, &diagnostic)
		return code, out.String(), diagnostic.String()
	}
	if code, _, diagnostic := run("init", "--root", filepath.Join(base, "offline-root")); code != 0 {
		t.Fatal(code, diagnostic)
	}
	read := func() state.MetadataBudget {
		code, raw, diagnostic := run("status", "--json")
		var result struct {
			OK     bool                 `json:"ok"`
			Budget state.MetadataBudget `json:"scanner_metadata_budget"`
		}
		if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || !result.OK || diagnostic != "" {
			t.Fatal(code, raw, diagnostic, err)
		}
		return result.Budget
	}
	before := read()
	if !before.Available || before.DayCharges != nil || before.TotalCharges != nil || before.TrackingStartedAt != nil || before.PreTrackingUsage != "unknown" {
		t.Fatal("untracked saved view fabricated charges", before)
	}
	writer, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	now := time.Now()
	if _, err := writer.ReserveMetadata(context.Background(), now, state.MetadataStartup, nil, 100, before.Limit); err != nil {
		t.Fatal(err)
	}
	want, err := writer.MetadataBudget(context.Background(), time.Now(), before.Limit)
	if err != nil {
		t.Fatal(err)
	}
	got := read() // A saved reader works while the writer owns an unsettled attempt.
	if !reflect.DeepEqual(got, want) || got.DayCharges == nil || got.DayCharges.OutstandingReserved != 100 || got.DayCharges.UnknownReserved != 0 {
		t.Fatal("status recovered or reinterpreted an outstanding attempt", got, want)
	}
	after, err := writer.MetadataBudget(context.Background(), time.Now(), before.Limit)
	if err != nil || !reflect.DeepEqual(after, want) {
		t.Fatal("saved status changed accounting", after, want, err)
	}
}
