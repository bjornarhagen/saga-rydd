package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func cpuChargesCLIFixture(t *testing.T, active bool) (*state.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	if active {
		at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
		if _, err = w.ActivateCPUCharges(context.Background(), at); err != nil {
			t.Fatal(err)
		}
		cpu := int64(1234567)
		_, _, err = w.BeginCPUSession(context.Background(), state.CPUSessionStart{Nonce: strings.Repeat("b", 64), Instance: strings.Repeat("c", 32), ObservedAt: at, SelfCPUNS: &cpu})
		if err != nil {
			t.Fatal(err)
		}
	}
	return w, dir
}

func cpuChargesCLISaved(t *testing.T, w *state.Store) state.CPUChargeState {
	t.Helper()
	s, e := w.CPUCharges(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestCPUChargesCLIOfflineExactSavedView(t *testing.T) {
	w, dir := cpuChargesCLIFixture(t, true)
	before := cpuChargesCLISaved(t, w)
	// A configuration error must not affect an existing-only saved query.
	if e := os.WriteFile(filepath.Join(dir, "config.toml"), []byte("invalid = ["), 0600); e != nil {
		t.Fatal(e)
	}
	for _, machine := range []bool{false, true} {
		var out, diag bytes.Buffer
		args := []string{"--data-dir", dir, "status", "--cpu-charges"}
		if machine {
			args = append(args, "--json")
		}
		code := Run(context.Background(), args, &out, &diag)
		if code != 0 || diag.Len() != 0 {
			t.Fatal(code, out.String(), diag.String())
		}
		if machine {
			var envelope struct {
				OK     bool             `json:"ok"`
				Report CPUChargesReport `json:"cpu_charges"`
			}
			if e := json.Unmarshal(out.Bytes(), &envelope); e != nil || !envelope.OK || !reflect.DeepEqual(envelope.Report.SavedState, before) {
				t.Fatal(e, out.String())
			}
			if envelope.Report.CurrentPermissionEvaluated || envelope.Report.HourlyLimitEnforced || envelope.Report.DailyLimitEnforced || envelope.Report.PhysicalPowerMeasured {
				t.Fatal(out.String())
			}
			if strings.Contains(out.String(), `"worker"`) {
				t.Fatal("saved view added a worker reply", out.String())
			}
		} else {
			for _, word := range []string{"CONSERVATIVE CPU CHARGES", "Prefixes can overlap", "contacts no worker", "complete upper bound", "Open tail unobserved"} {
				if !strings.Contains(strings.Join(strings.Fields(out.String()), " "), word) {
					t.Fatal("missing qualification", word, out.String())
				}
			}
		}
		if !reflect.DeepEqual(before, cpuChargesCLISaved(t, w)) {
			t.Fatal("saved view recovered or changed the active session")
		}
	}
}

func TestCPUChargesCLILegacyAndMissingState(t *testing.T) {
	w, dir := cpuChargesCLIFixture(t, false)
	before, err := w.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range []bool{false, true} {
		var out, diag bytes.Buffer
		args := []string{"--data-dir", dir, "status", "--cpu-charges"}
		if machine {
			args = append(args, "--json")
		}
		if code := Run(context.Background(), args, &out, &diag); code != 0 || diag.Len() != 0 {
			t.Fatal(code, out.String(), diag.String())
		}
		if machine {
			var env struct {
				Report CPUChargesReport `json:"cpu_charges"`
			}
			if e := json.Unmarshal(out.Bytes(), &env); e != nil || env.Report.SavedState.Available || env.Report.SavedState.Status != "unavailable" {
				t.Fatal(e, out.String())
			}
		} else if !strings.Contains(out.String(), "NOT ACTIVATED") {
			t.Fatal(out.String())
		}
	}
	after, err := w.Summary(context.Background())
	if err != nil || before.Schema != 14 || !reflect.DeepEqual(before, after) {
		t.Fatal("legacy view migrated state", before, after, err)
	}
	missing := filepath.Join(t.TempDir(), "absent")
	var out, diag bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", missing, "status", "--cpu-charges", "--json"}, &out, &diag); code != 1 {
		t.Fatal(code, out.String(), diag.String())
	}
	if _, err = os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("view initialized missing state", err)
	}
}

func TestCPUChargesCLIStrictModeBeforeStorage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	for _, args := range [][]string{{"--cpu-charges=false"}, {"--cpu-charges=true"}, {"--cpu-charges", "--cpu-charges"}, {"--cpu-charges", "--work", "1"}, {"--cpu-charges", "extra"}, {"-cpu-charges"}, {"--cpu-charges", "--"}} {
		for _, machine := range []bool{false, true} {
			var out, diag bytes.Buffer
			all := []string{"--data-dir", dir, "status"}
			if machine {
				all = append(all, "--json")
			}
			all = append(all, args...)
			code := Run(context.Background(), all, &out, &diag)
			if machine {
				hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "invalid_arguments", 2)
			} else if code != 2 {
				t.Fatal(code, out.String(), diag.String())
			}
		}
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid mode initialized state", err)
	}
}

func TestCPUChargesCLICorruptSavedLedger(t *testing.T) {
	w, dir := cpuChargesCLIFixture(t, true)
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", filepath.Join(dir, state.Filename))
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec("UPDATE worker_self_cpu_charges SET state_json=x'7b'")
	closeErr := db.Close()
	if e != nil || closeErr != nil {
		t.Fatal(e, closeErr)
	}
	var out, diag bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", dir, "status", "--cpu-charges", "--json"}, &out, &diag)
	hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "cpu_charges_invalid", 1)
	if strings.Contains(out.String(), "state_json") || strings.Contains(out.String(), `"cpu_charges"`) {
		t.Fatal("corruption reply leaked payload or partial saved result", out.String())
	}
}

type cpuChargesShortWriter struct{}

func (cpuChargesShortWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		return len(p) - 1, nil
	}
	return 0, nil
}

func TestCPUChargesCLICanceledAndFailedReplies(t *testing.T) {
	w, dir := cpuChargesCLIFixture(t, true)
	before := cpuChargesCLISaved(t, w)
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", dir, "status", "--cpu-charges"}
		if machine {
			args = append(args, "--json")
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashStoreBudgetCancelWriter{cancel: cancel}
		var diag bytes.Buffer
		code := Run(ctx, args, out, &diag)
		cancel()
		if code != 1 || !strings.Contains(diag.String(), "reply was canceled") || !strings.Contains(diag.String(), "no saved records or source files were changed") {
			t.Fatal(code, out.output.String(), diag.String())
		}
		if machine {
			var env struct {
				OK bool `json:"ok"`
			}
			if e := json.Unmarshal(out.output.Bytes(), &env); e != nil || !env.OK {
				t.Fatal("appended second cancellation envelope", e, out.output.String())
			}
		}
		diag.Reset()
		if code = Run(context.Background(), args, cpuChargesShortWriter{}, &diag); code != 1 || !strings.Contains(diag.String(), io.ErrShortWrite.Error()) {
			t.Fatal("short write accepted", code, diag.String())
		}
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		var reply bytes.Buffer
		diag.Reset()
		code = Run(ctx, args, &reply, &diag)
		if code != 1 {
			t.Fatal("canceled view accepted", code, reply.String(), diag.String())
		}
		if machine {
			hashChoiceCLIError(t, code, reply.String(), diag.String(), "status", "canceled", 1)
		}
		if !reflect.DeepEqual(before, cpuChargesCLISaved(t, w)) {
			t.Fatal("failed reply changed saved state")
		}
	}
}

func TestCPUChargesCLIStableErrorAndCapabilities(t *testing.T) {
	var out, diag bytes.Buffer
	code := operationFailure(&out, &diag, "status", errors.Join(state.ErrCPUChargesCorrupt, errors.New("raw SQL secret")))
	hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "cpu_charges_invalid", 1)
	if strings.Contains(out.String(), "raw SQL secret") {
		t.Fatal(out.String())
	}
	out.Reset()
	diag.Reset()
	code = operationFailure(&out, &diag, "status", errors.Join(state.ErrCPUChargesCorrupt, context.Canceled))
	hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "canceled", 1)
	out.Reset()
	diag.Reset()
	if code = Run(context.Background(), []string{"capabilities", "--json"}, &out, &diag); code != 0 {
		t.Fatal(code, out.String(), diag.String())
	}
	var env struct {
		Contract   map[string]any  `json:"cpu_charges_contract"`
		Configured map[string]any  `json:"configured_cpu_session_charges"`
		Features   map[string]bool `json:"features"`
		ErrorCodes []string        `json:"error_codes"`
	}
	if e := json.Unmarshal(out.Bytes(), &env); e != nil || env.Contract["name"] != state.CPUChargesContract || !env.Features["saved_cpu_session_charges"] || env.Contract["saved_status_activates_or_recovers"] != false || env.Contract["work_permission_granted"] != false {
		t.Fatal(e, out.String())
	}
	expectedConfigured := map[string]any{
		"configuration": "scan.cpu_session_charges", "enabled_by_default": false,
		"experimental_scan_required": true, "optional": true, "activated_schema_version": float64(15),
		"opt_out_retains_saved_history": true, "opt_out_charge_debt_gate_active": false,
		"unknown_or_uncertain_refuses_new_work": true, "unique_process_cpu_verified": false,
		"full_process_lifetime_verified": false, "hourly_limit_enforced": false, "daily_limit_enforced": false,
	}
	if !env.Features["configured_cpu_session_charges"] || !reflect.DeepEqual(env.Configured, expectedConfigured) || env.Features["cpu_limit"] || env.Features["power_controls"] {
		t.Fatal("configured session charges overclaim scope, activation, or quotas", out.String())
	}
	for _, code := range []string{"cpu_charges_invalid", "cpu_charges_unavailable"} {
		found := false
		for _, advertised := range env.ErrorCodes {
			if advertised == code {
				found = true
			}
		}
		if !found {
			t.Fatal("CPU charge error not advertised", code)
		}
	}
}

func TestCPUChargesCLICanceledErrorDiscardsStorageCause(t *testing.T) {
	for _, terminal := range []error{context.Canceled, context.DeadlineExceeded} {
		err := cpuChargesStatusError(errors.Join(state.ErrCPUChargesCorrupt, errors.New("raw SQL secret"), terminal))
		if err != terminal {
			t.Fatal("cancellation retained storage detail", err)
		}
		var out, diag bytes.Buffer
		code := operationFailure(&out, &diag, "status", err)
		hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "canceled", 1)
		if strings.Contains(out.String(), "raw SQL secret") {
			t.Fatal(out.String())
		}
	}
}
