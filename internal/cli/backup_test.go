package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/storeexport"
)

func TestBackupArgumentsRefuseBeforeOperation(t *testing.T) {
	calls := 0
	ops := backupOperations{
		export: func(context.Context, string, string) (storeexport.ExportResult, error) {
			calls++
			return storeexport.ExportResult{}, nil
		},
		verify: func(context.Context, string, string) (storeexport.VerificationResult, error) {
			calls++
			return storeexport.VerificationResult{}, nil
		},
	}
	for _, tc := range []struct {
		name, base string
		count      int
		args       []string
	}{
		{"no_mode", "/base", 1, nil},
		{"missing_destination", "/base", 1, []string{"--plans"}},
		{"default_store", "", 0, []string{"--plans", "--to", "/exports/one"}},
		{"empty_explicit_store", "", 1, []string{"--plans", "--to", "/exports/one"}},
		{"repeated_global_store", "/base", 2, []string{"--plans", "--to", "/exports/one"}},
		{"relative_store", "base", 1, []string{"--plans", "--to", "/exports/one"}},
		{"unclean_store", "/base/../other", 1, []string{"--plans", "--to", "/exports/one"}},
		{"root_store", "/", 1, []string{"--plans", "--to", "/exports/one"}},
		{"relative_destination", "/base", 1, []string{"--plans", "--to", "exports"}},
		{"unclean_destination", "/base", 1, []string{"--plans", "--to", "/exports/../one"}},
		{"root_destination", "/base", 1, []string{"--plans", "--to", "/"}},
		{"nested_destination", "/base", 1, []string{"--plans", "--to", "/base/export"}},
		{"destination_equals_store", "/base", 1, []string{"--plans", "--to", "/base"}},
		{"oversized_path", "/base", 1, []string{"--plans", "--to", "/" + strings.Repeat("a", 4096)}},
		{"oversized_name", "/base", 1, []string{"--plans", "--to", "/exports/" + strings.Repeat("a", 256)}},
		{"nul", "/base", 1, []string{"--plans", "--to", "/exports/\x00"}},
		{"invalid_utf8", "/base", 1, []string{"--plans", "--to", "/exports/\xff"}},
		{"false_mode", "/base", 1, []string{"--plans=false", "--to", "/exports/one"}},
		{"repeated_mode", "/base", 1, []string{"--plans", "-plans", "--to", "/exports/one"}},
		{"repeated_destination_alias", "/base", 1, []string{"--plans", "--to", "/exports/one", "-to", "/exports/two"}},
		{"mixed_modes", "/base", 1, []string{"--plans", "--to", "/exports/one", "--verify", "/artifact", "--scratch-dir", "/scratch"}},
		{"export_scratch", "/base", 1, []string{"--plans", "--to", "/exports/one", "--scratch-dir", "/scratch"}},
		{"positional", "/base", 1, []string{"--plans", "--to", "/exports/one", "extra"}},
		{"verify_default_scratch", "", 0, []string{"--verify", "/artifact"}},
		{"verify_store_option", "/base", 1, []string{"--verify", "/artifact", "--scratch-dir", "/scratch"}},
		{"verify_destination_option", "", 0, []string{"--verify", "/artifact", "--scratch-dir", "/scratch", "--to", "/exports/one"}},
		{"verify_nested_scratch", "", 0, []string{"--verify", "/artifact", "--scratch-dir", "/artifact/scratch"}},
		{"verify_repeated_alias", "", 0, []string{"--verify", "/artifact", "-verify", "/artifact", "--scratch-dir", "/scratch"}},
		{"verify_empty", "", 0, []string{"--verify=", "--scratch-dir", "/scratch"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dispatchBackup(context.Background(), tc.args, tc.base, tc.count, ops)
			var usage usageError
			if !errors.As(err, &usage) || calls != 0 {
				t.Fatal("invalid request entered an operation", calls, err)
			}
		})
	}
}

func TestBackupExactScopeAndCancellation(t *testing.T) {
	exports, verifications := 0, 0
	ops := backupOperations{
		export: func(ctx context.Context, base, destination string) (storeexport.ExportResult, error) {
			exports++
			if base != "/base" || destination != "/exports/one" {
				t.Fatal(base, destination)
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 120*time.Second {
				t.Fatal("unbounded operation")
			}
			return storeexport.ExportResult{Contract: storeexport.Contract, Destination: destination, Published: true, Verified: true, SyncCompleted: true, EngineClosureKnown: true}, nil
		},
		verify: func(ctx context.Context, artifact, scratch string) (storeexport.VerificationResult, error) {
			verifications++
			if artifact != "/exports/one" || scratch != "/scratch" {
				t.Fatal(artifact, scratch)
			}
			return storeexport.VerificationResult{Contract: storeexport.Contract, Verified: true, EngineClosureKnown: true}, nil
		},
	}
	r, err := dispatchBackup(context.Background(), []string{"--plans", "--to", "/exports/one"}, "/base", 1, ops)
	if err != nil || r.Mode != "export_plans" || r.Export == nil || r.Verification != nil || exports != 1 || verifications != 0 {
		t.Fatal(r, err)
	}
	r, err = dispatchBackup(context.Background(), []string{"--verify", "/exports/one", "--scratch-dir", "/scratch"}, "", 0, ops)
	if err != nil || r.Mode != "verify_artifact" || r.Verification == nil || r.Export != nil || exports != 1 || verifications != 1 {
		t.Fatal(r, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = dispatchBackup(ctx, []string{"--plans", "--to", "/exports/one"}, "/base", 1, ops)
	if !errors.Is(err, context.Canceled) || exports != 1 || verifications != 1 {
		t.Fatal("cancellation entered operation", err)
	}
	if r.FullInstanceBackup || r.ImportAvailable || r.RestoreAvailable || r.CleanupAuthority || r.CurrentPermissionEvaluated || r.HardLatencyBound || !r.PrivateUnusedPageRemnantsPossible || len(r.OmittedStores) != 6 {
		t.Fatal("scope or authority widened", r)
	}
}

func TestBackupPartialResultsSurviveMachineErrors(t *testing.T) {
	for _, mode := range []string{"export", "verify"} {
		for _, observed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-observed%t", mode, observed), func(t *testing.T) {
				calls := 0
				ops := backupOperations{
					export: func(context.Context, string, string) (storeexport.ExportResult, error) {
						calls++
						return storeexport.ExportResult{Contract: storeexport.Contract, Destination: "/exports/one", Candidate: "/exports/.candidate", CandidateRetained: true, CandidateIdentityObserved: observed}, storeexport.ErrChanged
					},
					verify: func(context.Context, string, string) (storeexport.VerificationResult, error) {
						calls++
						return storeexport.VerificationResult{Contract: storeexport.Contract, Scratch: "/scratch/.candidate", ScratchRetained: true, ScratchIdentityObserved: observed}, storeexport.ErrProfile
					},
				}
				args, base, count := []string{"--plans", "--to", "/exports/one"}, "/base", 1
				if mode == "verify" {
					args, base, count = []string{"--verify", "/exports/one", "--scratch-dir", "/scratch"}, "", 0
				}
				var out, diagnostic bytes.Buffer
				code := runBackupWithOperations(context.Background(), args, base, count, &out, &diagnostic, true, ops)
				var envelope struct {
					Version    int               `json:"api_version"`
					OK         bool              `json:"ok"`
					Command    string            `json:"command"`
					Backup     BackupReport      `json:"backup"`
					Error      map[string]string `json:"error"`
					NextAction string            `json:"next_action"`
				}
				decoder := json.NewDecoder(&out)
				if err := decoder.Decode(&envelope); err != nil || code != 1 || calls != 1 || envelope.Version != APIVersion || envelope.OK || envelope.Command != "backup" || diagnostic.Len() != 0 {
					t.Fatal(code, calls, envelope, diagnostic.String(), err)
				}
				if err := decoder.Decode(new(any)); err != io.EOF {
					t.Fatal("more than one envelope", err)
				}
				if mode == "export" {
					if envelope.Backup.Export == nil || !envelope.Backup.Export.CandidateRetained || envelope.Backup.Export.CandidateIdentityObserved != observed || envelope.Backup.Export.Candidate != "/exports/.candidate" || envelope.Error["code"] != "backup_changed" || !strings.Contains(envelope.NextAction, "/exports/.candidate") {
						t.Fatal(envelope)
					}
				} else if envelope.Backup.Verification == nil || !envelope.Backup.Verification.ScratchRetained || envelope.Backup.Verification.ScratchIdentityObserved != observed || envelope.Backup.Verification.Scratch != "/scratch/.candidate" || envelope.Error["code"] != "backup_profile_unsupported" || !strings.Contains(envelope.NextAction, "/scratch/.candidate") {
					t.Fatal(envelope)
				}
				if observed && !strings.Contains(envelope.NextAction, "identity was observed earlier") || !observed && !strings.Contains(envelope.NextAction, "identity was not confirmed") {
					t.Fatal("identity qualification missing", envelope.NextAction)
				}
				calls = 0
				out.Reset()
				diagnostic.Reset()
				if code = runBackupWithOperations(context.Background(), args, base, count, &out, &diagnostic, false, ops); code != 1 || calls != 1 || !strings.Contains(out.String(), ".candidate") || !strings.Contains(diagnostic.String(), ".candidate") {
					t.Fatal(code, calls, out.String(), diagnostic.String())
				}
				human := strings.Join(strings.Fields(out.String()), " ")
				if observed && !strings.Contains(human, "identity was observed earlier") || !observed && !strings.Contains(human, "identity was not confirmed") {
					t.Fatal("human identity qualification missing", out.String())
				}
			})
		}
	}
}

type backupFailWriter struct{ short bool }

func (w backupFailWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("generated output failure")
}

type backupCancelWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *backupCancelWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.cancel()
	return n, err
}

func TestBackupPublishedResultOutputFailureDoesNotRepeat(t *testing.T) {
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("machine%t-short%t", machine, short), func(t *testing.T) {
				calls := 0
				ops := backupOperations{export: func(context.Context, string, string) (storeexport.ExportResult, error) {
					calls++
					return storeexport.ExportResult{Contract: storeexport.Contract, Destination: "/exports/one", Published: true, Verified: true, SyncCompleted: true, EngineClosureKnown: true}, nil
				}}
				var diagnostic bytes.Buffer
				code := runBackupWithOperations(context.Background(), []string{"--plans", "--to", "/exports/one"}, "/base", 1, backupFailWriter{short}, &diagnostic, machine, ops)
				if code != 1 || calls != 1 || !strings.Contains(diagnostic.String(), `"/exports/one" was published`) || !strings.Contains(diagnostic.String(), "before any new export") {
					t.Fatal(code, calls, diagnostic.String())
				}
			})
		}
	}
}

func TestBackupCancellationDuringReplyRetainsPublishedResult(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("machine%t", machine), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &backupCancelWriter{cancel: cancel}
			var diagnostic bytes.Buffer
			calls := 0
			ops := backupOperations{export: func(context.Context, string, string) (storeexport.ExportResult, error) {
				calls++
				return storeexport.ExportResult{Contract: storeexport.Contract, Destination: "/exports/one", Published: true, Verified: true, SyncCompleted: true, EngineClosureKnown: true}, nil
			}}
			if code := runBackupWithOperations(ctx, []string{"--plans", "--to", "/exports/one"}, "/base", 1, out, &diagnostic, machine, ops); code != 1 || calls != 1 || !strings.Contains(diagnostic.String(), "was published") {
				t.Fatal(code, calls, diagnostic.String())
			}
			if machine {
				decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
				var envelope map[string]any
				if err := decoder.Decode(&envelope); err != nil {
					t.Fatal(err)
				}
				if err := decoder.Decode(new(any)); err != io.EOF {
					t.Fatal("second reply after cancellation", err)
				}
			}
		})
	}
}

func TestBackupRouteNeverResolvesDefaultHome(t *testing.T) {
	t.Setenv("HOME", "")
	for _, args := range [][]string{
		{"backup", "--verify", "/nonexistent-export", "--scratch-dir", "/nonexistent-scratch", "--json"},
		{"--data-dir", "/nonexistent-base", "backup", "--plans", "--to", "/nonexistent-output/export", "--json"},
	} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), args, &out, &diagnostic)
		var envelope struct {
			Command string            `json:"command"`
			Error   map[string]string `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 1 || envelope.Command != "backup" || envelope.Error["code"] != "not_found" {
			t.Fatal(code, out.String(), diagnostic.String(), err)
		}
	}
	for _, valueFlag := range []string{"--to", "-to", "--verify", "-verify", "--scratch-dir", "-scratch-dir"} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), []string{"backup", valueFlag, "--json"}, &out, &diagnostic)
		if code != 2 || strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
			t.Fatal("flag value was consumed as JSON mode", valueFlag, code, out.String(), diagnostic.String())
		}
	}
	var out, diagnostic bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", "/one", "-data-dir=/two", "backup", "--plans", "--to", "/exports/one", "--json"}, &out, &diagnostic); code != 2 {
		t.Fatal("repeated global option accepted", code, out.String())
	}
}

// This is a generated structural schema fixture. It verifies the CLI wiring,
// not historical record semantics or an owner database.
func backupCLIGeneratedStore(t *testing.T) (string, string, string) {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(temp, 0700); err != nil {
		t.Fatal(err)
	}
	base, exports, scratch := filepath.Join(temp, "state"), filepath.Join(temp, "exports"), filepath.Join(temp, "scratch")
	for _, dir := range []string{base, filepath.Join(base, "plans"), exports, scratch} {
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"writer.lock", "plans.sqlite3"} {
		f, openErr := os.OpenFile(filepath.Join(base, "plans", name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(base, "plans", "plans.sqlite3")}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app, objects, ok := plans.ExportProfile(1)
	if !ok {
		t.Fatal("missing schema fixture")
	}
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, object := range objects {
			if object.Type == kind && object.SQL != "" {
				if _, err = db.ExecContext(ctx, object.SQL); err != nil {
					db.Close()
					t.Fatal(err)
				}
			}
		}
	}
	for _, query := range []string{fmt.Sprintf("PRAGMA application_id=%d", app), "PRAGMA user_version=1", "INSERT INTO plans(id,payload) VALUES('generated-history',x'616263')"} {
		if _, err = db.ExecContext(ctx, query); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return base, exports, scratch
}

func TestBackupGeneratedCLIExportVerifyAndNoOverwrite(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("machine%t", machine), func(t *testing.T) {
			base, exports, scratch := backupCLIGeneratedStore(t)
			destination := filepath.Join(exports, "history")
			t.Setenv("HOME", "")
			args := []string{"--data-dir", base, "backup", "--plans", "--to", destination}
			if machine {
				args = append(args, "--json")
			}
			var out, diagnostic bytes.Buffer
			if code := Run(context.Background(), args, &out, &diagnostic); code != 0 {
				t.Fatal(code, out.String(), diagnostic.String())
			}
			if machine {
				var envelope struct {
					OK     bool         `json:"ok"`
					Backup BackupReport `json:"backup"`
				}
				if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || !envelope.OK || envelope.Backup.Export == nil || !envelope.Backup.Export.Published || !envelope.Backup.Export.SyncCompleted {
					t.Fatal(out.String(), err)
				}
			} else if human := strings.Join(strings.Fields(out.String()), " "); !strings.Contains(human, "PLANS HISTORY EXPORT SAVED") || !strings.Contains(human, "Unused database pages may retain private remnants") || !strings.Contains(human, "Import, restore and cleanup are unavailable") {
				t.Fatal(out.String())
			}
			out.Reset()
			diagnostic.Reset()
			if code := Run(context.Background(), args, &out, &diagnostic); code != 1 {
				t.Fatal("existing destination was replaced", code, out.String(), diagnostic.String())
			}
			out.Reset()
			diagnostic.Reset()
			args = []string{"backup", "--verify", destination, "--scratch-dir", scratch}
			if machine {
				args = append(args, "--json")
			}
			if code := Run(context.Background(), args, &out, &diagnostic); code != 0 {
				t.Fatal(code, out.String(), diagnostic.String())
			}
			if machine {
				var envelope struct {
					OK     bool         `json:"ok"`
					Backup BackupReport `json:"backup"`
				}
				if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || !envelope.OK || envelope.Backup.Verification == nil || !envelope.Backup.Verification.Verified || envelope.Backup.Verification.ScratchRetained {
					t.Fatal(out.String(), err)
				}
			} else if human := strings.Join(strings.Fields(out.String()), " "); !strings.Contains(human, "ARTIFACT STRUCTURE VERIFIED") || !strings.Contains(human, "record meanings") {
				t.Fatal(out.String())
			}
			names, err := os.ReadDir(scratch)
			if err != nil || len(names) != 0 {
				t.Fatal("scratch did not finish", names, err)
			}
		})
	}
}

func TestBackupCapabilitiesDescribePartialScope(t *testing.T) {
	c := capabilities()
	contract := c["plans_export_contract"].(map[string]any)
	if contract["name"] != storeexport.Contract || contract["full_instance_backup"] != false || contract["restore_available"] != false || contract["hard_latency_bound"] != false || contract["record_semantics_checked"] != false || contract["private_unused_page_remnants_possible"] != true {
		t.Fatal(contract)
	}
	features := c["features"].(map[string]bool)
	if !features["plans_history_export"] || !features["plans_export_verification"] || features["full_instance_backup"] || features["backup_import"] || features["backup_restore"] {
		t.Fatal("missing or widened feature scope")
	}
}
