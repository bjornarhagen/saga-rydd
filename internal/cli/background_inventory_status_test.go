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

func TestBackgroundInventoryStatusSelectedModeAndOutputFailure(t *testing.T) {
	for _, compact := range []bool{false, true} {
		p := worker.InventoryModeSnapshot{Compact: compact}
		var out bytes.Buffer
		printWorkerInventoryMode(&out, &p)
		text := strings.Join(strings.Fields(out.String()), " ")
		mode := "DETAILED"
		if compact {
			mode = "COMPACT"
		}
		if !strings.Contains(text, "Background inventory storage: "+mode) {
			t.Fatal(text)
		}
		if compact {
			for _, want := range []string{"inside node_modules", "Other files keep detailed records", "fixed daily interval", "does not prove that a scan or saved-data work has finished", "Status does not inspect source folders"} {
				if !strings.Contains(text, want) {
					t.Fatal(want, text)
				}
			}
		}
		raw, err := json.Marshal(worker.Snapshot{InventoryMode: &p})
		if err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Mode worker.InventoryModeSnapshot `json:"inventory_mode"`
		}
		if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Mode != p {
			t.Fatal(string(raw), err)
		}
	}
	var out bytes.Buffer
	printWorkerInventoryMode(&out, nil)
	raw, err := json.Marshal(worker.Snapshot{})
	if err != nil || out.Len() != 0 || bytes.Contains(raw, []byte("inventory_mode")) {
		t.Fatal(out.String(), string(raw), err)
	}
	guard := &reviewOutput{writer: serviceFailWriter{}}
	printWorkerInventoryMode(guard, &worker.InventoryModeSnapshot{Compact: true})
	if guard.err == nil {
		t.Fatal("lost output failure")
	}
}

func TestBackgroundInventoryCapabilitiesAndErrorsStayNarrow(t *testing.T) {
	caps := capabilities()
	features := caps["features"].(map[string]bool)
	if !features["compact_background_inventory"] || !features["compact_manual_scan"] || features["cleanup"] || features["cpu_limit"] {
		t.Fatal(features)
	}
	c := caps["compact_background_contract"].(map[string]any)
	if c["fixed_revisit_interval_ns"] != int64(24*time.Hour) || c["scope"] != "configured_experimental_background_inventory" || c["enabled_by_default"] != false || c["adaptive_revisits_supported"] != false || c["status_inspects_sources"] != false || c["source_body_reads"] != false || c["cleanup_approved"] != false || c["mode_changes_require_safe_saved_boundary"] != true {
		t.Fatal(c)
	}
	for _, item := range []struct {
		err  error
		code string
	}{{state.ErrBackgroundInventoryInput, "inventory_mode_invalid"}, {state.ErrBackgroundInventoryCorrupt, "inventory_mode_invalid"}, {state.ErrBackgroundInventoryModePending, "inventory_mode_pending"}, {errors.Join(state.ErrBackgroundInventoryModePending, context.Canceled), "canceled"}} {
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
