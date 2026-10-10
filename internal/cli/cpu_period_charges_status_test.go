package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func cpuPeriodCLIFixture(t *testing.T) (*state.Store, string) {
	t.Helper()
	w, dir := cpuChargesCLIFixture(t, false)
	ctx := context.Background()
	at := time.Date(2026, 10, 10, 12, 40, 0, 0, time.UTC)
	if _, err := w.ActivateCPUCharges(ctx, at); err != nil {
		t.Fatal(err)
	}
	prefix := int64(2 * time.Millisecond)
	legacy, _, err := w.BeginCPUSession(ctx, state.CPUSessionStart{Nonce: strings.Repeat("a", 64), Instance: strings.Repeat("b", 32), ObservedAt: at, SelfCPUNS: &prefix})
	if err != nil {
		t.Fatal(err)
	}
	prefix = int64(3 * time.Millisecond)
	elapsed := int64(time.Second)
	if _, err = w.FinishCPUSession(ctx, legacy, state.CPUSessionSample{Ordinal: 1, ObservedAt: at.Add(time.Second), SelfCPUNS: &prefix, ElapsedNS: &elapsed}); err != nil {
		t.Fatal(err)
	}
	limits := state.CPUChargeLimits{HourNS: int64(2 * time.Second), DayNS: int64(7 * time.Second)}
	if _, err = w.ActivateCPUChargeAdmission(ctx, at.Add(2*time.Second), limits); err != nil {
		t.Fatal(err)
	}
	prefix = int64(9 * time.Millisecond)
	m, _, _, err := w.BeginCPUSessionLimited(ctx, state.CPULimitedSessionStart{Start: state.CPUSessionStart{ExpectedGeneration: 1, Nonce: strings.Repeat("c", 64), Instance: strings.Repeat("d", 32), ObservedAt: at.Add(3 * time.Second), SelfCPUNS: &prefix}, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	prefix = int64(10 * time.Millisecond)
	nextDay := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, _, err = w.SampleCPUSessionLimited(ctx, m, state.CPUSessionSample{Ordinal: 1, ObservedAt: nextDay, SelfCPUNS: &prefix, ElapsedNS: &elapsed}); err != nil {
		t.Fatal(err)
	}
	return w, dir
}

func cpuPeriodCLISaved(t *testing.T, w *state.Store) state.CPUChargeAdmissionState {
	t.Helper()
	a, err := w.CPUChargeAdmission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func cpuPeriodCLIArgs(dir string, machine bool) []string {
	a := []string{"--data-dir", dir, "status", "--cpu-period-charges"}
	if machine {
		a = append(a, "--json")
	}
	return a
}

func TestCPUPeriodChargesCLIOfflineSavedViewAndCompatibility(t *testing.T) {
	w, dir := cpuPeriodCLIFixture(t)
	before := cpuPeriodCLISaved(t, w)
	cpuBefore := cpuChargesCLISaved(t, w)
	// Absolute private paths must work without home lookup or valid config. The
	// live writer is held throughout; the saved query does not ask its control IPC.
	t.Setenv("HOME", "")
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte("invalid = ["), 0600); err != nil {
		t.Fatal(err)
	}
	for _, machine := range []bool{false, true} {
		var out, diag bytes.Buffer
		if code := Run(context.Background(), cpuPeriodCLIArgs(dir, machine), &out, &diag); code != 0 || diag.Len() != 0 {
			t.Fatal(code, out.String(), diag.String())
		}
		if machine {
			var e struct {
				Version int                    `json:"api_version"`
				OK      bool                   `json:"ok"`
				Command string                 `json:"command"`
				Report  CPUPeriodChargesReport `json:"cpu_period_charges"`
			}
			if err := json.Unmarshal(out.Bytes(), &e); err != nil || !e.OK || e.Version != APIVersion || e.Command != "status" || !reflect.DeepEqual(e.Report, CPUPeriodChargesReport{SavedState: before}) {
				t.Fatal(err, out.String())
			}
			if strings.Contains(out.String(), `"worker"`) || strings.Contains(out.String(), `"cpu_charges"`) {
				t.Fatal("period view added live or legacy output", out.String())
			}
		} else {
			text := strings.Join(strings.Fields(out.String()), " ")
			for _, want := range []string{"Saved UTC hour", "Saved UTC day", "Activation baseline charge 3ms", "Assigned CPU charge 1ms", "Retired CPU charge 9ms", "Frozen charge limit 2s", "Frozen charge limit 7s", "Saved slot start 2026-10-11T00:00:00Z", "Saved next UTC boundary 2026-10-11T01:00:00Z", "Saved next UTC boundary 2026-10-12T00:00:00Z", "Saved policy revision", "Saved partial tracking", "Saved blocking unknown", "Saved tracking gap open", "prefixes can overlap", "contacts no worker", "no remaining quota", "not verified"} {
				if !strings.Contains(text, want) {
					t.Fatal("missing saved value or qualification", want, out.String())
				}
			}
		}
		if !reflect.DeepEqual(before, cpuPeriodCLISaved(t, w)) || !reflect.DeepEqual(cpuBefore, cpuChargesCLISaved(t, w)) {
			t.Fatal("view changed or recovered joint accounting")
		}
	}
	// The earlier saved command keeps its exact envelope and typed projection on
	// a coherent schema-16 store. It remains separate from period inspection.
	var out, diag bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", dir, "status", "--cpu-charges", "--json"}, &out, &diag); code != 0 || diag.Len() != 0 {
		t.Fatal(code, out.String(), diag.String())
	}
	want, err := json.Marshal(map[string]any{"api_version": APIVersion, "ok": true, "command": "status", "cpu_charges": CPUChargesReport{SavedState: cpuBefore}})
	if err != nil || !bytes.Equal(out.Bytes(), append(want, '\n')) {
		t.Fatal("legacy saved CPU envelope changed", err, out.String())
	}
	if !reflect.DeepEqual(before, cpuPeriodCLISaved(t, w)) {
		t.Fatal("legacy CPU view changed period state")
	}
}

func TestCPUPeriodChargesCLILegacyAndMissingState(t *testing.T) {
	for _, activated := range []bool{false, true} {
		t.Run(map[bool]string{false: "schema14", true: "schema15"}[activated], func(t *testing.T) {
			w, dir := cpuChargesCLIFixture(t, activated)
			before, err := w.Summary(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			cpuBefore := cpuChargesCLISaved(t, w)
			for _, machine := range []bool{false, true} {
				var out, diag bytes.Buffer
				if code := Run(context.Background(), cpuPeriodCLIArgs(dir, machine), &out, &diag); code != 0 || diag.Len() != 0 {
					t.Fatal(code, out.String(), diag.String())
				}
				if machine {
					var e struct {
						Report CPUPeriodChargesReport `json:"cpu_period_charges"`
					}
					if err := json.Unmarshal(out.Bytes(), &e); err != nil || e.Report.SavedState.Available || e.Report.SavedState.Contract != state.CPUChargeAdmissionContract || e.Report.CurrentPermissionEvaluated || e.Report.WorkPermissionGranted {
						t.Fatal(err, out.String())
					}
				} else if !strings.Contains(out.String(), "CPU PERIOD ACCOUNTING NOT ACTIVATED") {
					t.Fatal(out.String())
				}
			}
			after, err := w.Summary(context.Background())
			if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(cpuBefore, cpuChargesCLISaved(t, w)) {
				t.Fatal("legacy saved query migrated/recovered state", before, after, err)
			}
		})
	}
	missing := filepath.Join(t.TempDir(), "absent")
	var out, diag bytes.Buffer
	code := Run(context.Background(), cpuPeriodCLIArgs(missing, true), &out, &diag)
	hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "not_found", 1)
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing store was created", err)
	}
}

func TestCPUPeriodChargesCLIStrictModeBeforePaths(t *testing.T) {
	t.Setenv("HOME", "")
	dir := filepath.Join(t.TempDir(), "absent")
	for _, args := range [][]string{{"--cpu-period-charges=false"}, {"--cpu-period-charges=true"}, {"--cpu-period-charges", "--cpu-period-charges"}, {"--cpu-period-charges", "--cpu-charges"}, {"--cpu-charges", "--cpu-period-charges"}, {"--cpu-period-charges", "extra"}, {"--cpu-period-charges", "--"}, {"-cpu-period-charges"}, {"--cpu-period-charges-extra"}, {"--cpu-period-charges", "--work", "1"}} {
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
			} else if code != 2 || out.Len() != 0 {
				t.Fatal(code, out.String(), diag.String())
			}
		}
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid mode inspected or created storage", err)
	}
}

func TestCPUPeriodChargesCLICorruptionIsSanitized(t *testing.T) {
	for _, mutation := range []string{"UPDATE worker_cpu_charge_admission SET state_json=x'7b'", "UPDATE worker_self_cpu_charges SET state_json=x'7b'", "DROP TABLE worker_cpu_charge_admission", "UPDATE worker_cpu_charge_admission SET policy_revision=999"} {
		t.Run(mutation, func(t *testing.T) {
			w, dir := cpuPeriodCLIFixture(t)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Exec(mutation)
			closeErr := db.Close()
			if err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			for _, machine := range []bool{false, true} {
				var out, diag bytes.Buffer
				code := Run(context.Background(), cpuPeriodCLIArgs(dir, machine), &out, &diag)
				if machine {
					hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "cpu_period_charges_invalid", 1)
				} else if code != 1 || out.Len() != 0 || strings.TrimSpace(diag.String()) != state.ErrCPUChargeAdmissionCorrupt.Error() {
					t.Fatal(code, out.String(), diag.String())
				}
				if strings.Contains(out.String(), "state_json") || strings.Contains(out.String(), "worker_cpu_charge_admission") || strings.Contains(out.String(), `"cpu_period_charges"`) {
					t.Fatal("corruption exposed SQL or partial saved state", out.String())
				}
			}
		})
	}
}

func TestCPUPeriodChargesCLITrackingGapAndUnknownStayHistorical(t *testing.T) {
	w, dir := cpuPeriodCLIFixture(t)
	a := cpuPeriodCLISaved(t, w)
	if _, _, err := w.OpenCPUTrackingGap(context.Background(), state.CPUTrackingGapRequest{ExpectedPolicyRevision: a.PolicyRevision, ExpectedGeneration: a.CPUGeneration, Nonce: strings.Repeat("e", 64), ObservedAt: a.ClockHighWater.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	before := cpuPeriodCLISaved(t, w)
	if !before.TrackingGapOpen || !before.Hour.PartialTracking || !before.Day.PartialTracking || !before.Hour.BlockingUnknown || !before.Day.BlockingUnknown {
		t.Fatal("fixture did not retain unknown gap", before)
	}
	var out, diag bytes.Buffer
	if code := Run(context.Background(), cpuPeriodCLIArgs(dir, true), &out, &diag); code != 0 || diag.Len() != 0 {
		t.Fatal(code, out.String(), diag.String())
	}
	var e struct {
		Report CPUPeriodChargesReport `json:"cpu_period_charges"`
	}
	if err := json.Unmarshal(out.Bytes(), &e); err != nil || !reflect.DeepEqual(e.Report, CPUPeriodChargesReport{SavedState: before}) || !reflect.DeepEqual(before, cpuPeriodCLISaved(t, w)) {
		t.Fatal("view changed gap/unknown evidence", err, out.String())
	}
}

func TestCPUPeriodChargesCLICanceledAndFailedReplies(t *testing.T) {
	w, dir := cpuPeriodCLIFixture(t)
	before := cpuPeriodCLISaved(t, w)
	cpuBefore := cpuChargesCLISaved(t, w)
	for _, machine := range []bool{false, true} {
		args := cpuPeriodCLIArgs(dir, machine)
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashStoreBudgetCancelWriter{cancel: cancel}
		var diag bytes.Buffer
		code := Run(ctx, args, out, &diag)
		cancel()
		if code != 1 || !strings.Contains(diag.String(), "reply was canceled") || !strings.Contains(diag.String(), "no saved records or source files were changed") {
			t.Fatal(code, out.output.String(), diag.String())
		}
		if machine {
			var e struct {
				Report CPUPeriodChargesReport `json:"cpu_period_charges"`
			}
			if err := json.Unmarshal(out.output.Bytes(), &e); err != nil || !reflect.DeepEqual(e.Report, CPUPeriodChargesReport{SavedState: before}) {
				t.Fatal("cancellation appended a second envelope or granted permission", err, out.output.String())
			}
		}
		diag.Reset()
		if code = Run(context.Background(), args, cpuChargesShortWriter{}, &diag); code != 1 || !strings.Contains(diag.String(), io.ErrShortWrite.Error()) {
			t.Fatal("short output accepted", code, diag.String())
		}
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		var reply bytes.Buffer
		diag.Reset()
		code = Run(ctx, args, &reply, &diag)
		if machine {
			hashChoiceCLIError(t, code, reply.String(), diag.String(), "status", "canceled", 1)
		} else if code != 1 || reply.Len() != 0 {
			t.Fatal(code, reply.String(), diag.String())
		}
		if !reflect.DeepEqual(before, cpuPeriodCLISaved(t, w)) || !reflect.DeepEqual(cpuBefore, cpuChargesCLISaved(t, w)) {
			t.Fatal("failed reply changed joint accounting")
		}
	}
}

func TestCPUPeriodChargesCLIExistingPrivateReaderAliasRules(t *testing.T) {
	for _, alias := range []string{"directory_symlink", "database_symlink", "database_hardlink", "shared_database", "nonregular_database"} {
		t.Run(alias, func(t *testing.T) {
			w, dir := cpuPeriodCLIFixture(t)
			before := cpuPeriodCLISaved(t, w)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, state.Filename)
			bytesBefore, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			original := path
			switch alias {
			case "directory_symlink":
				link := filepath.Join(t.TempDir(), "linked")
				err = os.Symlink(dir, link)
				dir = link
			case "database_symlink":
				original = filepath.Join(dir, "original.sqlite")
				if err = os.Rename(path, original); err == nil {
					err = os.Symlink(original, path)
				}
			case "database_hardlink":
				err = os.Link(path, filepath.Join(dir, "alias.sqlite"))
			case "shared_database":
				err = os.Chmod(path, 0644)
			case "nonregular_database":
				original = filepath.Join(dir, "original.sqlite")
				if err = os.Rename(path, original); err == nil {
					err = os.Mkdir(path, 0700)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var out, diag bytes.Buffer
			code := Run(context.Background(), cpuPeriodCLIArgs(dir, true), &out, &diag)
			if alias == "database_hardlink" {
				// Existing read-only state policy permits private regular hardlinks.
				// Preserve that behavior without claiming exclusive object identity.
				var e struct {
					Report CPUPeriodChargesReport `json:"cpu_period_charges"`
				}
				if err := json.Unmarshal(out.Bytes(), &e); err != nil || code != 0 || diag.Len() != 0 || !reflect.DeepEqual(e.Report, CPUPeriodChargesReport{SavedState: before}) {
					t.Fatal("historical hardlink view changed existing reader policy", code, err, out.String(), diag.String())
				}
			} else if code != 1 || strings.Contains(out.String(), `"cpu_period_charges"`) {
				t.Fatal("unsupported private alias accepted", code, out.String(), diag.String())
			}
			bytesAfter, err := os.ReadFile(original)
			if err != nil || sha256.Sum256(bytesBefore) != sha256.Sum256(bytesAfter) {
				t.Fatal("refused alias changed saved database", err)
			}
		})
	}
}

func TestCPUPeriodChargesCLIStableErrorsAndCapabilities(t *testing.T) {
	for _, terminal := range []error{context.Canceled, context.DeadlineExceeded} {
		err := cpuPeriodChargesStatusError(errors.Join(state.ErrCPUChargeAdmissionCorrupt, errors.New("raw SQL secret"), terminal))
		if err != terminal {
			t.Fatal("cancellation retained raw cause", err)
		}
		var out, diag bytes.Buffer
		code := operationFailure(&out, &diag, "status", err)
		hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "canceled", 1)
	}
	for _, cause := range []error{state.ErrCPUChargeAdmissionCorrupt, state.ErrCPUChargesCorrupt} {
		var out, diag bytes.Buffer
		code := operationFailure(&out, &diag, "status", cpuPeriodChargesStatusError(errors.Join(cause, errors.New("raw SQL secret"))))
		hashChoiceCLIError(t, code, out.String(), diag.String(), "status", "cpu_period_charges_invalid", 1)
		if strings.Contains(out.String(), "raw SQL secret") {
			t.Fatal(out.String())
		}
	}
	var out, diag bytes.Buffer
	if code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &diag); code != 0 || diag.Len() != 0 {
		t.Fatal(code, out.String(), diag.String())
	}
	var e struct {
		Contract   map[string]any  `json:"cpu_period_charges_contract"`
		Features   map[string]bool `json:"features"`
		Configured map[string]any  `json:"configured_cpu_period_charges"`
		ErrorCodes []string        `json:"error_codes"`
	}
	if err := json.Unmarshal(out.Bytes(), &e); err != nil || e.Contract["name"] != state.CPUChargeAdmissionContract || e.Contract["scope"] != state.CPUChargeAdmissionScope || !e.Features["saved_cpu_period_charges"] || !e.Features["configured_cpu_period_charges"] || e.Features["cpu_limit"] {
		t.Fatal("saved period contract missing or overclaims configured integration", err, out.String())
	}
	for _, flag := range []string{"saved_status_samples_current_cpu", "saved_status_samples_admission_clock", "saved_status_loads_config", "saved_status_contacts_worker", "saved_status_activates_or_recovers", "remaining_quota_calculated", "current_work_permission_evaluated", "work_permission_granted", "physical_period_cpu_verified", "global_cpu_quota_verified", "full_process_cpu_verified"} {
		if e.Contract[flag] != false {
			t.Fatal("saved contract overclaims", flag, out.String())
		}
	}
	if e.Configured["hour_configuration"] != "scan.cpu_charge_seconds_per_hour" || e.Configured["day_configuration"] != "scan.cpu_charge_seconds_per_day" || e.Configured["default_hour_seconds"] != float64(0) || e.Configured["default_day_seconds"] != float64(0) || e.Configured["activated_schema_version"] != float64(16) {
		t.Fatal("configured period scope/defaults unavailable", out.String())
	}
	for _, flag := range []string{"positive_limits_require_session_tracking", "actual_activated_state_selects_protocol", "activated_tracking_opt_out_records_durable_gap", "activated_tracked_zero_profile_uses_limited_protocol", "known_overshoot_retained", "live_elapsed_fences_preserved"} {
		if e.Configured[flag] != true {
			t.Fatal("configured period guarantee missing", flag, out.String())
		}
	}
	for _, flag := range []string{"opt_out_charge_gate_active", "hard_limit_enforced", "physical_period_cpu_verified", "global_cpu_quota_verified", "full_process_cpu_verified", "representative_defaults_verified"} {
		if e.Configured[flag] != false {
			t.Fatal("configured period capability overclaims", flag, out.String())
		}
	}
	for _, code := range []string{"cpu_period_charges_invalid", "cpu_period_charges_unavailable"} {
		found := false
		for _, advertised := range e.ErrorCodes {
			found = found || advertised == code
		}
		if !found {
			t.Fatal("stable period error missing", code)
		}
	}
}
