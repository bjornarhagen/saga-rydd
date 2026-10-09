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
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type ignoreCLIFixture struct {
	base, root, inventory, requestFile, findingID string
}

func newIgnoreCLIFixture(t *testing.T, partial bool, rootName string) ignoreCLIFixture {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := ignoreCLIFixture{base: filepath.Join(temp, "private state"), root: filepath.Join(temp, rootName), requestFile: filepath.Join(temp, "preview.json")}
	modules := filepath.Join(f.root, "node_modules")
	if err := os.MkdirAll(modules, 0700); err != nil {
		t.Fatal(err)
	}
	if partial {
		if err := os.Mkdir(filepath.Join(modules, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(f.root, "package.json"), filepath.Join(modules, "keep.txt")} {
		if err := os.WriteFile(path, []byte("generated fixture data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, path := range []string{modules, filepath.Join(f.root, "package.json")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--compact", "--now", "--json")
	if code != 0 || stderr != "" {
		t.Fatal(code, raw, stderr)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.inventory = manualState(paths, f.root)
	reader, err := state.OpenReader(context.Background(), f.inventory)
	if err != nil {
		t.Fatal(err)
	}
	report, readErr := reader.NodeModulesFindings(context.Background(), "", state.FindingAgeDays)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(report.Findings) != 1 {
		t.Fatal(report, readErr, closeErr)
	}
	f.findingID = report.Findings[0].ID
	// None of the dismissal commands may load this malformed configuration.
	if err := os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f ignoreCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func ignoreCLIPreview(t *testing.T, f ignoreCLIFixture) (plans.DismissalRequest, []byte) {
	t.Helper()
	code, raw, stderr := f.run(context.Background(), "ignore", "--preview", "-d", f.root, f.findingID, "--json")
	var result ignoreRequestEnvelope
	if err := json.Unmarshal([]byte(raw), &result); err != nil || code != 0 || stderr != "" || result.Version != APIVersion || !result.OK || result.Command != "ignore" || plans.ValidateDismissalRequest(result.Request) != nil {
		t.Fatal("preview did not emit one valid exact request", code, raw, stderr, err)
	}
	for _, want := range []string{`"current_state_verified":false`, `"approval_available":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !strings.Contains(raw, want) {
			t.Fatal("preview omitted its authority qualification", want, raw)
		}
	}
	if err := os.WriteFile(f.requestFile, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return result.Request, []byte(raw)
}

func ignoreCLISaved(t *testing.T, code int, raw, stderr string) plans.SavedDismissal {
	t.Helper()
	var envelope struct {
		Version   int                  `json:"api_version"`
		OK        bool                 `json:"ok"`
		Command   string               `json:"command"`
		Dismissal plans.SavedDismissal `json:"dismissal"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "ignore" || !plans.ValidDismissalID(envelope.Dismissal.ID) {
		t.Fatal("dismissal command did not emit one saved historical record", code, raw, stderr, err)
	}
	saved := envelope.Dismissal
	if saved.CurrentEvidenceMatchEvaluated || saved.CurrentStateVerified || saved.ApprovalAvailable || saved.Executable || saved.EstimatedReclaimableBytes != nil {
		t.Fatal("saved dismissal claimed current applicability or authority", raw)
	}
	for _, want := range []string{`"current_evidence_match_evaluated":false`, `"current_state_verified":false`, `"approval_available":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !strings.Contains(raw, want) {
			t.Fatal("saved dismissal omitted its authority qualification", want, raw)
		}
	}
	return saved
}

func ignoreCLIError(t *testing.T, code int, raw, stderr string, want string, exit int) {
	t.Helper()
	var envelope struct {
		Version int    `json:"api_version"`
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.OK || envelope.Version != APIVersion || envelope.Command != "ignore" || envelope.Error.Code != want || strings.Contains(raw, `"dismissal_request":`) || strings.Contains(raw, `"dismissal":`) {
		t.Fatal("refusal emitted partial evidence or the wrong error", code, raw, stderr, want, err)
	}
}

func TestIgnoreCLIExactLifecycleOfflineRetryAndUndo(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial_%t", partial), func(t *testing.T) {
			f := newIgnoreCLIFixture(t, partial, "project")
			before := hashCLIBytes(t, f.root, f.inventory)
			request, raw := ignoreCLIPreview(t, f)
			second, _ := ignoreCLIPreview(t, f)
			if !reflect.DeepEqual(request, second) {
				t.Fatal("preview generation time changed the exact request", raw)
			}
			code, body, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
			saved := ignoreCLISaved(t, code, body, stderr)
			if saved.Status != "dismissed" || !reflect.DeepEqual(saved.Record.Request, request) || saved.Record.Status != "historical_dismissed" || saved.Undo != nil {
				t.Fatal("save changed or widened the exact request", body)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.inventory)) {
				t.Fatal("dismissal preview/save changed source or inventory bytes")
			}
			if err := os.Rename(f.root, f.root+".offline"); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(f.inventory, f.inventory+".offline"); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"ignore", "--show", saved.ID, "--json"}, {"ignore", "--save", "--from", f.requestFile, "--json"}} {
				code, body, stderr = f.run(context.Background(), args...)
				if got := ignoreCLISaved(t, code, body, stderr); !reflect.DeepEqual(got, saved) {
					t.Fatal("offline retry/show changed the first record", body)
				}
			}
			code, body, stderr = f.run(context.Background(), "ignore", "--undo", saved.ID, "--json")
			undone := ignoreCLISaved(t, code, body, stderr)
			if undone.Status != "undone" || undone.Undo == nil || undone.ID != saved.ID || !reflect.DeepEqual(undone.Record, saved.Record) || undone.Undo.Record.DismissalID != saved.ID || undone.Undo.Record.RequestID != request.ID {
				t.Fatal("undo replaced historical evidence or lost exact bindings", body)
			}
			for _, args := range [][]string{{"ignore", "--undo", saved.ID, "--json"}, {"ignore", "--save", "--from", f.requestFile, "--json"}, {"ignore", "--show", saved.ID, "--json"}} {
				code, body, stderr = f.run(context.Background(), args...)
				if got := ignoreCLISaved(t, code, body, stderr); !reflect.DeepEqual(got, undone) {
					t.Fatal("retry renewed an undone dismissal", body)
				}
			}
			for _, missing := range []string{f.root, f.inventory} {
				if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("offline operations initialized source or inventory", missing, err)
				}
			}
		})
	}
}

func TestIgnoreCLIArgumentsJSONValuesAndMissingStores(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	f := ignoreCLIFixture{base: base}
	id := "dismissal-v1-" + strings.Repeat("a", 64)
	for _, args := range [][]string{
		{"ignore"}, {"ignore", "--preview=false"}, {"ignore", "--save=false"},
		{"ignore", "--preview", "-d", "/generated", "bad"}, {"ignore", "--preview", "-d", "/generated", "node-modules-v1:01:2"},
		{"ignore", "--preview", "node-modules-v1:1:2"}, {"ignore", "--preview", "-d", "/generated", "node-modules-v1:1:2", "node-modules-v1:1:3"},
		{"ignore", "--preview", "--preview", "-d", "/generated", "node-modules-v1:1:2"}, {"ignore", "-preview", "--preview", "-d", "/generated", "node-modules-v1:1:2"},
		{"ignore", "--preview", "-d", "/generated", "--directory", "/generated", "node-modules-v1:1:2"},
		{"ignore", "--preview", "-d", "/generated", "--min-age-days", "90", "--min-age-days", "90", "node-modules-v1:1:2"},
		{"ignore", "--preview", "-d", "/generated", "--min-age-days", "0", "node-modules-v1:1:2"},
		{"ignore", "--save", "--from", "-"}, {"ignore", "--save", "--from", "preview.json", "-d", "/override"},
		{"ignore", "--save", "--from", "preview.json", "--min-age-days", "90"}, {"ignore", "--save", "--from", "preview.json", "node-modules-v1:1:2"},
		{"ignore", "--save", "--from", "preview.json", "-from", "preview.json"}, {"ignore", "--save", "--preview", "--from", "preview.json"},
		{"ignore", "--show", id, "--undo", id}, {"ignore", "--show", id, "-show", id}, {"ignore", "--undo", id, "--undo", id},
		{"ignore", "--show", id, "-d", "/override"}, {"ignore", "--undo", id, "--from", "preview.json"},
		{"ignore", "--show", "bad"}, {"ignore", "--unknown"},
	} {
		code, raw, stderr := f.run(context.Background(), append(args, "--json")...)
		ignoreCLIError(t, code, raw, stderr, "invalid_arguments", 2)
	}
	for _, args := range [][]string{{"ignore", "--show", id}, {"ignore", "--undo", id}, {"ignore", "--preview", "-d", "/generated", "node-modules-v1:1:2"}} {
		code, raw, stderr := f.run(context.Background(), append(args, "--json")...)
		ignoreCLIError(t, code, raw, stderr, "not_found", 1)
	}
	// A flag's literal value must not select JSON or turn a bool into a value flag.
	code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", "--json")
	if code != 1 || raw != "" || !strings.Contains(stderr, "named preview request file") {
		t.Fatal("JSON-looking request filename was consumed as an output option", code, raw, stderr)
	}
	for _, args := range [][]string{{"--json", "ignore", "--preview", "-d", "/generated", "node-modules-v1:1:2"}, {"ignore", "--preview", "--json", "-d", "/generated", "node-modules-v1:1:2"}} {
		code, raw, stderr = f.run(context.Background(), args...)
		ignoreCLIError(t, code, raw, stderr, "not_found", 1)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid or missing dismissal initialized storage", err)
	}
}

func TestIgnoreCLIChangedEvidenceAndUnknownIdentityRefusePublication(t *testing.T) {
	for _, changed := range []string{"rescan", "replacement", "unknown_identity"} {
		t.Run(changed, func(t *testing.T) {
			f := newIgnoreCLIFixture(t, false, "project")
			request, _ := ignoreCLIPreview(t, f)
			if changed == "unknown_identity" {
				db, err := sql.Open("sqlite", filepath.Join(f.inventory, state.Filename))
				if err != nil {
					t.Fatal(err)
				}
				_, editErr := db.Exec("UPDATE entries SET device='',inode='' WHERE id=?", request.Selection.Evidence.Findings[0].EntryID)
				closeErr := db.Close()
				if editErr != nil || closeErr != nil {
					t.Fatal(editErr, closeErr)
				}
				code, raw, stderr := f.run(context.Background(), "ignore", "--preview", "-d", f.root, f.findingID, "--json")
				ignoreCLIError(t, code, raw, stderr, "dismissal_unavailable", 1)
			} else {
				if err := os.Remove(filepath.Join(f.base, "config.toml")); err != nil {
					t.Fatal(err)
				}
				if changed == "replacement" {
					if err := os.Rename(f.inventory, f.inventory+".previous"); err != nil {
						t.Fatal(err)
					}
				}
				if code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--compact", "--now", "--json"); code != 0 || stderr != "" {
					t.Fatal(code, raw, stderr)
				}
			}
			code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
			ignoreCLIError(t, code, raw, stderr, "dismissal_evidence_changed", 1)
			if _, err := os.Lstat(filepath.Join(f.base, "plans")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refused changed evidence initialized dismissal storage", err)
			}
		})
	}
}

func TestIgnoreCLIKnownManifestStorageAliasesRefuseBeforeSQLite(t *testing.T) {
	for _, scope := range []string{"plans_database", "inventory_database"} {
		t.Run(scope, func(t *testing.T) {
			f := newIgnoreCLIFixture(t, false, "project")
			ignoreCLIPreview(t, f)
			destination := filepath.Join(f.inventory, state.Filename)
			if scope == "plans_database" {
				private := filepath.Join(f.base, "plans")
				if err := os.Mkdir(private, 0700); err != nil {
					t.Fatal(err)
				}
				destination = filepath.Join(private, "plans.sqlite3")
			} else if err := os.Rename(destination, destination+".previous"); err != nil {
				t.Fatal(err)
			}
			// The exact saved manifest object, containing generated non-SQLite
			// bytes, now occupies a private database name. SQLite must never
			// inspect its bytes to discover that it is not a database.
			if err := os.Rename(filepath.Join(f.root, "package.json"), destination); err != nil {
				t.Fatal(err)
			}
			before := hashCLIBytes(t, f.root, f.base)
			code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
			ignoreCLIError(t, code, raw, stderr, "dismissal_unavailable", 1)
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.base)) {
				t.Fatal("known-object alias refusal changed storage or generated source")
			}
			if scope == "inventory_database" {
				if _, err := os.Lstat(filepath.Join(f.base, "plans")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("inventory alias refusal initialized dismissal storage", err)
				}
			}
		})
	}
}

func TestIgnoreCLIOutputFailureAndLateCancellationRetainExactRecords(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "project")
	request, _ := ignoreCLIPreview(t, f)
	for _, jsonMode := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "ignore", "--save", "--from", f.requestFile}
		if jsonMode {
			args = append(args, "--json")
		}
		for _, short := range []bool{false, true} {
			var stderr bytes.Buffer
			code := Run(context.Background(), args, hashFailWriter{short: short}, &stderr)
			saved, err := plans.FindDismissal(context.Background(), f.base, request.ID)
			if err != nil || code != 1 || !strings.Contains(stderr.String(), saved.ID) || !strings.Contains(stderr.String(), "ignore --show "+saved.ID) {
				t.Fatal("lost save output hid the committed exact record", code, stderr.String(), saved, err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		code := Run(ctx, args, out, &stderr)
		cancel()
		saved, err := plans.FindDismissal(context.Background(), f.base, request.ID)
		if err != nil || code != 1 || !strings.Contains(stderr.String(), saved.ID) || !strings.Contains(stderr.String(), "canceled") {
			t.Fatal("late canceled output hid committed dismissal", code, out.String(), stderr.String(), err)
		}
		if jsonMode {
			decoder := json.NewDecoder(strings.NewReader(out.String()))
			var first, extra map[string]any
			if err := decoder.Decode(&first); err != nil || first["ok"] != true {
				t.Fatal("late cancellation corrupted the one completed JSON envelope", out.String(), err)
			}
			if err := decoder.Decode(&extra); err != io.EOF {
				t.Fatal("late cancellation emitted a second JSON envelope", out.String(), err)
			}
		}
	}
	saved, err := plans.FindDismissal(context.Background(), f.base, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", f.base, "ignore", "--undo", saved.ID, "--json"}, hashFailWriter{short: true}, &stderr)
	undone, err := plans.ShowDismissal(context.Background(), f.base, saved.ID)
	if err != nil || code != 1 || undone.Status != "undone" || undone.Undo == nil || !strings.Contains(stderr.String(), undone.Undo.ID) {
		t.Fatal("lost undo reply hid the exact immutable undo record", code, stderr.String(), undone, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, diagnostic := f.run(ctx, "ignore", "--save", "--from", f.requestFile, "--json")
	ignoreCLIError(t, code, raw, diagnostic, "canceled", 1)
	if after, err := plans.ShowDismissal(context.Background(), f.base, saved.ID); err != nil || !reflect.DeepEqual(after, undone) {
		t.Fatal("early cancellation changed the undone record", after, err)
	}
}

func TestIgnoreCLIRawPathsHumanOutputAndCapabilities(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "quote\"雪\x1b\tproject")
	request, _ := ignoreCLIPreview(t, f)
	code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
	saved := ignoreCLISaved(t, code, raw, stderr)
	code, human, stderr := f.run(context.Background(), "ignore", "--show", saved.ID)
	prose := strings.Join(strings.Fields(human), " ")
	if code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || !bytes.Equal(request.ManualRootBytes, []byte(f.root)) || !bytes.Equal(saved.Record.Request.Selection.Evidence.Findings[0].PathBytes, []byte(filepath.Join(f.root, "node_modules"))) || !strings.Contains(human, fmt.Sprintf("Target: %q", string(request.Selection.Evidence.Findings[0].PathBytes))) || !strings.Contains(human, commandForIgnoreTest(t, f.base)+" ignore --undo "+saved.ID) || !strings.Contains(prose, "No source files or configuration were read or changed.") {
		t.Fatal("human dismissal lost exact safe paths or scope qualifications", code, human, stderr)
	}
	code, raw, stderr = f.run(context.Background(), "capabilities", "--json")
	var capabilities struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || stderr != "" || !capabilities.Features["finding_dismissals"] || !capabilities.Features["finding_dismissal_undo"] || capabilities.Features["cleanup"] || !strings.Contains(raw, "--save --from REQUEST_JSON") || !strings.Contains(raw, "dismissal_evidence_changed") {
		t.Fatal("capabilities omitted the finite dismissal contract or claimed cleanup", code, raw, stderr, err)
	}
	code, human, stderr = f.run(context.Background(), "--help")
	if code != 0 || stderr != "" || !strings.Contains(human, "ignore --preview -d ROOT") || !strings.Contains(human, "ignore --show ID / --undo ID") {
		t.Fatal("human help omitted finite ignore commands", code, human, stderr)
	}
}

func TestIgnoreCLIInvalidByteFilesystemPathLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("invalid-byte filesystem fixture is Linux-only; native macOS refused its filename; Unicode, control-character and capability checks run separately")
	}
	f := newIgnoreCLIFixture(t, false, "invalid-\xff-project")
	request, _ := ignoreCLIPreview(t, f)
	code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
	saved := ignoreCLISaved(t, code, raw, stderr)
	if !bytes.Equal(request.ManualRootBytes, []byte(f.root)) || !bytes.Equal(saved.Record.Request.Selection.Evidence.Findings[0].PathBytes, []byte(filepath.Join(f.root, "node_modules"))) {
		t.Fatal("machine request or saved dismissal lost invalid path bytes", raw)
	}
	code, human, stderr := f.run(context.Background(), "ignore", "--show", saved.ID)
	if code != 0 || stderr != "" || !utf8.ValidString(human) || !strings.Contains(human, fmt.Sprintf("Target: %q", string(request.Selection.Evidence.Findings[0].PathBytes))) {
		t.Fatal("human dismissal lost exact escaped invalid path bytes", code, human, stderr)
	}
}

func commandForIgnoreTest(t *testing.T, base string) string {
	t.Helper()
	paths, err := config.ResolvePaths(base)
	if err != nil {
		t.Fatal(err)
	}
	return commandPrefix(paths)
}
