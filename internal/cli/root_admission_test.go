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

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func rootAdmissionCLIFixture(t *testing.T, count ...int) (config.Paths, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state-é-'quoted'")
	paths, err := config.ResolvePaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	retained := state.RootAdmissionLimit
	if len(count) > 0 {
		retained = count[0]
	}
	roots := make([]string, retained)
	for i := range roots {
		roots[i] = fmt.Sprintf("/generated/root-%03d", i)
	}
	if err = w.SyncRoots(context.Background(), roots); err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), roots[:1]); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return paths, roots[0]
}

func rootAdmissionCLIEnabled(t *testing.T, dir string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT id,hex(path),enabled FROM roots ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id, enabled int64
		var path string
		if err = rows.Scan(&id, &path, &enabled); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%d:%s:%d", id, path, enabled))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRootAdmissionCLIStateInitRefusalAndInitPartialConfiguration(t *testing.T) {
	for _, command := range []string{"state init", "init"} {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", command, machine), func(t *testing.T) {
				paths, _ := rootAdmissionCLIFixture(t)
				before := rootAdmissionCLIEnabled(t, paths.StateDir)
				cfg := config.Default()
				cfg.Roots = []string{"/generated/new-root"}
				args := []string{"--data-dir", paths.StateDir}
				if command == "state init" {
					if err := config.Create(paths.ConfigFile, "/generated-home", cfg); err != nil {
						t.Fatal(err)
					}
					args = append(args, "state", "init")
				} else {
					args = append(args, "init", "--root", cfg.Roots[0])
				}
				if machine {
					args = append(args, "--json")
				}
				var out, diagnostic bytes.Buffer
				if exit := Run(context.Background(), args, &out, &diagnostic); exit != 1 {
					t.Fatal(exit, out.String(), diagnostic.String())
				}
				message := diagnostic.String()
				if machine {
					var envelope struct {
						Version int               `json:"api_version"`
						OK      bool              `json:"ok"`
						Command string            `json:"command"`
						Error   map[string]string `json:"error"`
					}
					if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.Version != 1 || envelope.OK || envelope.Command != command || envelope.Error["code"] != "root_capacity_reached" || diagnostic.Len() != 0 {
						t.Fatal(err, out.String(), diagnostic.String())
					}
					message = envelope.Error["message"]
					if envelope.Error["request_id"] != "" || envelope.Error["publication_outcome"] != "" {
						t.Fatal("definite refusal claimed uncertain publication", out.String())
					}
				} else if out.Len() != 0 {
					t.Fatal("refused setup reported ready", out.String())
				}
				flat := strings.Join(strings.Fields(message), " ")
				for _, required := range []string{"128-record", "Disabled roots", "requested root selection was not applied", "history remain available", "Earlier state initialization or migration"} {
					if !strings.Contains(flat, required) {
						t.Fatal("missing refusal qualification", required, message)
					}
				}
				if command == "init" && !strings.Contains(flat, "Configuration was saved") {
					t.Fatal("published configuration omitted", message)
				}
				if strings.Contains(message, "Use rydd init") || strings.Contains(message, "delete state") || strings.Contains(message, "fresh store") {
					t.Fatal("unsafe recovery recommendation", message)
				}
				if !reflect.DeepEqual(before, rootAdmissionCLIEnabled(t, paths.StateDir)) {
					t.Fatal("refusal changed root identity or enabled selection")
				}
				got, err := config.Load(paths.ConfigFile, "/generated-home")
				if err != nil || !reflect.DeepEqual(got.Roots, cfg.Roots) {
					t.Fatal("configuration publication was misrepresented", got, err)
				}
			})
		}
	}
}

func TestRootAdmissionCLISafeCodesCancellationAndUnknownRequest(t *testing.T) {
	paths := config.Paths{ConfigFile: "/generated/state/config.toml", StateDir: "/generated/state"}
	id := "root-sync-v1-" + strings.Repeat("a", 64)
	private := errors.New("PRIVATE-SQL-CAUSE")
	for _, tc := range []struct {
		name, code, request, publication string
		err                              error
		exit                             int
	}{
		{"input", "invalid_arguments", "", "", errors.Join(state.ErrRootAdmissionInput, private), 2},
		{"capacity", "root_capacity_reached", "", "", errors.Join(state.ErrRootAdmissionCapacity, private), 1},
		{"corrupt", "root_admission_invalid", "", "", errors.Join(state.ErrRootAdmissionCorrupt, private), 1},
		{"unavailable", "root_admission_unavailable", "", "", errors.Join(state.ErrRootAdmissionUnavailable, private), 1},
		{"unknown", "root_outcome_unknown", id, "unknown", &state.RootAdmissionPublicationError{RequestID: id, Cause: private}, 1},
		{"unknown_canceled", "canceled", id, "unknown", &state.RootAdmissionPublicationError{RequestID: id, Cause: errors.Join(context.Canceled, private)}, 1},
		{"unknown_deadline", "canceled", id, "unknown", &state.RootAdmissionPublicationError{RequestID: id, Cause: errors.Join(context.DeadlineExceeded, private)}, 1},
		{"input_canceled", "canceled", "", "", errors.Join(state.ErrRootAdmissionInput, context.Canceled, private), 1},
		{"bad_request", "root_outcome_unknown", "", "unknown", &state.RootAdmissionPublicationError{RequestID: "PRIVATE-INVALID-ID\x1b[31m", Cause: private}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diagnostic, ok := rootAdmissionDiagnostic("state init", paths, nil, tc.err)
			if !ok || !errors.Is(diagnostic, private) {
				t.Fatal("safe projection lost identity", diagnostic, ok)
			}
			var out, stderr bytes.Buffer
			if exit := operationFailure(&out, &stderr, "state init", diagnostic); exit != tc.exit || stderr.Len() != 0 {
				t.Fatal(exit, out.String(), stderr.String())
			}
			var envelope struct {
				OK    bool              `json:"ok"`
				Error map[string]string `json:"error"`
			}
			if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.OK || envelope.Error["code"] != tc.code || envelope.Error["request_id"] != tc.request || envelope.Error["publication_outcome"] != tc.publication {
				t.Fatal(err, out.String())
			}
			if strings.Contains(out.String(), "PRIVATE-") || strings.Contains(out.String(), "\\u001b") {
				t.Fatal("private cause or invalid request escaped", out.String())
			}
			if tc.publication != "" && (strings.Contains(diagnostic.Error(), "selection was not applied") || !strings.Contains(diagnostic.Error(), "outcome is unknown")) {
				t.Fatal("uncertainty was rewritten as definite refusal", diagnostic)
			}
			var human bytes.Buffer
			if err := printRootAdmissionDiagnostic(&human, diagnostic); err != nil {
				t.Fatal(err)
			}
			if tc.request != "" && !strings.Contains(human.String(), "Request: "+tc.request+"\n") {
				t.Fatal("exact request was wrapped or lost", human.String())
			}
		})
	}
	manual := []byte("/generated/raw-'quote'-\n-")
	manual = append(manual, 0xff)
	diagnostic, ok := rootAdmissionDiagnostic("scan", paths, manual, state.ErrRootAdmissionCapacity)
	if !ok || !strings.Contains(diagnostic.Error(), "scan-mode setting may already have completed") || !strings.Contains(diagnostic.reportCommand, " -d "+shellQuote(string(manual))+" --json") || strings.ContainsAny(diagnostic.Error(), "\x1b\xff") {
		t.Fatal("manual scope/earlier effects/raw path qualification lost", diagnostic)
	}
}

func TestRootAdmissionCLIAdditionsCannotFitBelowLimit(t *testing.T) {
	paths, _ := rootAdmissionCLIFixture(t, state.RootAdmissionLimit-1)
	before := rootAdmissionCLIEnabled(t, paths.StateDir)
	cfg := config.Default()
	cfg.Roots = []string{"/generated/missing-one", "/generated/missing-two"}
	if err := config.Create(paths.ConfigFile, "/generated-home", cfg); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", paths.StateDir, "state", "init", "--json"}, &out, &stderr); code != 1 {
		t.Fatal(code, out.String(), stderr.String())
	}
	var envelope struct {
		Error map[string]string `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || stderr.Len() != 0 || envelope.Error["code"] != "root_capacity_reached" {
		t.Fatal(err, out.String(), stderr.String())
	}
	if !strings.Contains(envelope.Error["message"], "requested additions cannot fit") || strings.Contains(envelope.Error["message"], "has reached") {
		t.Fatal("refusal invented a current count", out.String())
	}
	if len(before) != 127 || !reflect.DeepEqual(before, rootAdmissionCLIEnabled(t, paths.StateDir)) {
		t.Fatal("last-slot refusal changed saved roots")
	}
}

func TestRootAdmissionCLIManualModeCanPrecedeRefusedRoots(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "private"))
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			base := manualState(paths, root)
			w, err := state.OpenWriter(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			roots := make([]string, state.RootAdmissionLimit)
			for i := range roots {
				roots[i] = fmt.Sprintf("/generated/manual-old-%03d", i)
			}
			if err = w.SyncRoots(context.Background(), roots); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			before := rootAdmissionCLIEnabled(t, base)
			args := []string{"--data-dir", paths.StateDir, "scan", "-d", root, "--compact"}
			if machine {
				args = append(args, "--json")
			}
			var out, stderr bytes.Buffer
			if code := Run(context.Background(), args, &out, &stderr); code != 1 {
				t.Fatal(code, out.String(), stderr.String())
			}
			message := stderr.String()
			if machine {
				var envelope struct {
					OK    bool              `json:"ok"`
					Error map[string]string `json:"error"`
				}
				if err = json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.OK || envelope.Error["code"] != "root_capacity_reached" || stderr.Len() != 0 {
					t.Fatal(err, out.String(), stderr.String())
				}
				message = envelope.Error["message"]
			} else if out.Len() != 0 {
				t.Fatal("refused manual scan reported progress", out.String())
			}
			flat := strings.Join(strings.Fields(message), " ")
			if !strings.Contains(flat, "Metadata scanning did not start") || !strings.Contains(flat, "scan-mode setting may already have completed") || !strings.Contains(flat, " -d "+shellQuote(root)+" --json") {
				t.Fatal("earlier mode effect or exact inspection scope lost", message)
			}
			if !reflect.DeepEqual(before, rootAdmissionCLIEnabled(t, base)) {
				t.Fatal("manual refusal changed roots")
			}
			w, err = state.OpenWriter(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			compact, e := w.ConfigureCompact(context.Background(), nil)
			summary, e2 := w.Summary(context.Background())
			w.Close()
			if e != nil || e2 != nil || !compact || summary.Entries != 0 || summary.PendingJobs != 0 || summary.RunningJobs != 0 {
				t.Fatal("earlier compact publication was lost or source work began", compact, summary, e, e2)
			}
		})
	}
}

type rootAdmissionFailWriter struct{ calls int }

func (w *rootAdmissionFailWriter) Write([]byte) (int, error) { w.calls++; return 0, io.ErrClosedPipe }

func TestRootAdmissionCLIFailedReplyAndCapabilities(t *testing.T) {
	paths := config.Paths{StateDir: filepath.Join(t.TempDir(), "not-created")}
	id := "root-sync-v1-" + strings.Repeat("b", 64)
	diagnostic, _ := rootAdmissionDiagnostic("init", paths, nil, &state.RootAdmissionPublicationError{RequestID: id, Cause: context.Canceled})
	var out rootAdmissionFailWriter
	var stderr bytes.Buffer
	if exit := operationFailure(&out, &stderr, "init", diagnostic); exit != 1 || out.calls != 1 {
		t.Fatal("failed reply retried output", exit, out.calls, stderr.String())
	}
	if !strings.Contains(stderr.String(), id) || !strings.Contains(strings.Join(strings.Fields(stderr.String()), " "), "outcome is unknown") || !strings.Contains(stderr.String(), diagnostic.reportCommand) {
		t.Fatal("failed reply lost exact unknown-outcome inspection guidance", stderr.String())
	}
	var human rootAdmissionFailWriter
	if err := printRootAdmissionDiagnostic(&human, diagnostic); !errors.Is(err, io.ErrClosedPipe) || human.calls != 1 {
		t.Fatal("human output failure was not sticky", err, human.calls)
	}
	caps := capabilities()
	contract := caps["inventory_root_admission_contract"].(map[string]any)
	if contract["name"] != state.RootAdmissionContract || contract["retained_record_limit"] != 128 || contract["input_path_limit"] != 128 || contract["maximum_path_bytes"] != 4096 || contract["census_row_limit"] != 129 || contract["legacy_existing_only_reactivation"] != true || contract["disabled_roots_count"] != true {
		t.Fatal(contract)
	}
	for _, field := range []string{"history_deleted", "hard_cardinality_enforced", "schema_changed", "older_writers_enforce", "external_sql_enforced", "physical_size_limit_enforced", "hard_latency_bound", "source_operations"} {
		if contract[field] != false {
			t.Fatal("capability overclaim", field, contract)
		}
	}
	for _, code := range []string{"root_capacity_reached", "root_admission_invalid", "root_admission_unavailable", "root_outcome_unknown", "canceled", "invalid_arguments"} {
		found := false
		for _, advertised := range caps["error_codes"].([]string) {
			found = found || advertised == code
		}
		if !found {
			t.Fatal("unadvertised error code", code)
		}
	}
	if caps["features"].(map[string]bool)["retained_root_admission"] != true {
		t.Fatal("missing bounded capability")
	}
	if _, err := os.Stat(paths.StateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pure diagnostics accessed/created scope", err)
	}
}
