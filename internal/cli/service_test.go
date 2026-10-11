package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func TestServicePreviewHasNoStorageOrExecutableAccess(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "missing state $%&")
	executable := filepath.Join(base, "missing executable $%&.bin")
	t.Setenv("RYDD_RUNTIME_DIR", filepath.Join(base, "missing runtime"))
	run := func(tail ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		args := append([]string{"--data-dir", dir, "service", "preview", "--executable", executable}, tail...)
		code := Run(context.Background(), args, &out, &errOut)
		return code, out.String(), errOut.String()
	}
	code, text, errText := run()
	if code != 0 || !strings.Contains(text, "SERVICE DESCRIPTOR PREVIEW") || !strings.Contains(text, "Nothing was installed or started by this command") || !strings.Contains(text, "future") || !strings.Contains(text, "contents were not checked") || strings.Contains(text, "--experimental-scan") {
		t.Fatal("preview authority/scope differs", code, text, errText)
	}
	code, raw, errText := run("--json")
	var report struct {
		OK      bool               `json:"ok"`
		Command string             `json:"command"`
		Service service.Descriptor `json:"service"`
	}
	if code != 0 || json.Unmarshal([]byte(raw), &report) != nil || !report.OK || report.Command != "service" || report.Service.Contract != "service_descriptor_preview_v1" || report.Service.Executable != executable || report.Service.StateDir != dir || report.Service.RuntimeDir != os.Getenv("RYDD_RUNTIME_DIR") || report.Service.InstallationPerformed || report.Service.ActivationPerformed || report.Service.ScanningEnabled {
		t.Fatal("JSON preview changed scope or claimed an operation", code, raw, errText)
	}
	for _, path := range []string{dir, executable, os.Getenv("RYDD_RUNTIME_DIR")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("preview accessed or created missing scope", path, err)
		}
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"config.toml", "state.sqlite3", "writer.lock"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte("generated invalid storage canary\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if code, raw, errText := run("--json"); code != 0 {
		t.Fatal("preview interpreted existing storage", code, raw, errText)
	}
	for _, file := range []string{"config.toml", "state.sqlite3", "writer.lock"} {
		data, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil || string(data) != "generated invalid storage canary\n" {
			t.Fatal("preview changed existing storage", file, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
		t.Fatal("preview created storage artifacts", len(entries), err)
	}
}

func TestServicePreviewStrictArgumentsCancellationAndOutputFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	for _, args := range [][]string{
		{}, {"install"}, {"start"}, {"preview"},
		{"preview", "--executable", "/a", "--executable", "/b"},
		{"preview", "--executable", "/a", "extra"},
		{"preview", "--executable", "/a", "--experimental-scan=TRUE"},
		{"preview", "--executable", "--json"},
		{"preview", "--executable=relative"},
	} {
		var out, errOut bytes.Buffer
		full := append([]string{"--data-dir", dir, "service"}, args...)
		full = append(full, "--json")
		if code := Run(context.Background(), full, &out, &errOut); code != 2 || !strings.Contains(out.String(), `"invalid_arguments"`) {
			t.Fatal("unsupported service mode/arguments accepted", args, code, out.String(), errOut.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := []string{"--data-dir", dir, "service", "preview", "--executable", "/generated/missing/rydd", "--json"}
	var out, errOut bytes.Buffer
	if code := Run(ctx, args, &out, &errOut); code != 1 || !strings.Contains(out.String(), `"canceled"`) {
		t.Fatal("canceled preview succeeded", code, out.String(), errOut.String())
	}
	if code := Run(context.Background(), args, serviceFailWriter{}, &errOut); code != 1 {
		t.Fatal("failed JSON output reported success", code)
	}
	if code := Run(context.Background(), args[:len(args)-1], serviceFailWriter{}, &errOut); code != 1 {
		t.Fatal("failed human output reported success", code)
	}
	for _, outputArgs := range [][]string{args, args[:len(args)-1]} {
		if code := Run(context.Background(), outputArgs, serviceShortWriter{}, &errOut); code != 1 {
			t.Fatal("short output reported success", code)
		}
		lateCtx, lateCancel := context.WithCancel(context.Background())
		if code := Run(lateCtx, outputArgs, serviceCancelWriter{cancel: lateCancel}, &errOut); code != 1 {
			t.Fatal("reply cancellation reported success", code)
		}
		lateCancel()
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed preview created state", err)
	}
}

type serviceFailWriter struct{}

func (serviceFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type serviceShortWriter struct{}

func (serviceShortWriter) Write(data []byte) (int, error) { return len(data) / 2, nil }

type serviceCancelWriter struct{ cancel context.CancelFunc }

func (w serviceCancelWriter) Write(data []byte) (int, error) {
	w.cancel()
	return len(data), nil
}

func scannerPreviewFixture(t *testing.T) (string, string) {
	t.Helper()
	base := t.TempDir()
	t.Setenv("HOME", base)
	t.Setenv("RYDD_RUNTIME_DIR", filepath.Join(base, "absent runtime $%&"))
	return filepath.Join(base, "absent state $%&"), filepath.Join(base, "absent executable $%&")
}

func scannerPreviewArgs(dir, executable string, tail ...string) []string {
	return append([]string{"--data-dir", dir, "service", "preview", "--executable", executable}, tail...)
}

func scannerPreviewCapture(ctx context.Context, args []string) (int, string, string) {
	var out, diagnostic bytes.Buffer
	code := Run(ctx, args, &out, &diagnostic)
	return code, out.String(), diagnostic.String()
}

func scannerPreviewDecode(t *testing.T, raw string) (service.Descriptor, map[string]json.RawMessage) {
	t.Helper()
	var envelope map[string]json.RawMessage
	var descriptor service.Descriptor
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &envelope) != nil || string(envelope["ok"]) != "true" || string(envelope["command"]) != "\"service\"" || json.Unmarshal(envelope["service"], &descriptor) != nil || json.Unmarshal(envelope["service"], &fields) != nil {
		t.Fatal("invalid service preview envelope", raw)
	}
	return descriptor, fields
}

func TestServiceScannerPreviewIntentIsNotOperation(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	for _, machine := range []bool{false, true} {
		t.Run(map[bool]string{false: "human", true: "JSON"}[machine], func(t *testing.T) {
			args := scannerPreviewArgs(dir, executable, "--experimental-scan")
			if machine {
				args = append(args, "--json")
			}
			code, raw, diagnostic := scannerPreviewCapture(context.Background(), args)
			if code != 0 || diagnostic != "" {
				t.Fatal("scanner preview failed", code, raw, diagnostic)
			}
			if !machine {
				for _, phrase := range []string{"SERVICE SCANNER DESCRIPTOR PREVIEW", "Nothing was installed or started", "No scanning was enabled", "then-current configuration", "current source permission", "not checked", "resources were not verified", "no hashing or cleanup authority", "lifecycle commands are supported by the API"} {
					if !strings.Contains(raw, phrase) {
						t.Fatal("scanner human preview lost an authority qualification", phrase, raw)
					}
				}
				return
			}
			descriptor, fields := scannerPreviewDecode(t, raw)
			want := service.ScannerIntent{Contract: "service_scanner_profile_intent_v2", WorkerProfile: service.ProfileExperimentalInventory, ActivationFlag: "--experimental-scan", ConfigurationScope: "configured_roots_at_future_worker_start", DescriptorOnly: true, LifecycleSupported: true}
			if descriptor.Contract != service.ScannerContract || descriptor.ScannerIntent == nil || *descriptor.ScannerIntent != want || descriptor.InstallationPerformed || descriptor.ActivationPerformed || descriptor.ScanningEnabled || len(descriptor.Argv) == 0 || descriptor.Argv[len(descriptor.Argv)-1] != "--experimental-scan" {
				t.Fatal("scanner JSON implies an operation or changed its intent", raw)
			}
			for _, name := range []string{"installation_performed", "activation_performed", "scanning_enabled"} {
				if string(fields[name]) != "false" {
					t.Fatal("preview lost explicit false effect", name, raw)
				}
			}
			var intent map[string]json.RawMessage
			if json.Unmarshal(fields["scanner_intent"], &intent) != nil {
				t.Fatal("missing scanner intent", raw)
			}
			for _, name := range []string{"native_lifecycle_verified", "configured_roots_verified", "current_source_permission_evaluated", "representative_resources_verified", "cleanup_authority"} {
				if string(intent[name]) != "false" {
					t.Fatal("scanner intent lost explicit unmet gate", name, raw)
				}
			}
		})
	}
}

func TestServiceScannerPreviewValidModeSpellings(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	for _, machine := range []bool{false, true} {
		idleArgs := scannerPreviewArgs(dir, executable)
		if machine {
			idleArgs = append(idleArgs, "--json")
		}
		idleCode, idle, idleDiagnostic := scannerPreviewCapture(context.Background(), idleArgs)
		if idleCode != 0 || idleDiagnostic != "" {
			t.Fatal("idle baseline failed", idleCode, idle, idleDiagnostic)
		}
		for _, mode := range []string{"--experimental-scan", "-experimental-scan", "--experimental-scan=true", "-experimental-scan=true", "--experimental-scan=false", "-experimental-scan=false"} {
			t.Run(map[bool]string{false: "human/", true: "JSON/"}[machine]+mode, func(t *testing.T) {
				args := scannerPreviewArgs(dir, executable, mode)
				if machine {
					args = append(args, "--json")
				}
				code, raw, diagnostic := scannerPreviewCapture(context.Background(), args)
				if code != 0 || diagnostic != "" {
					t.Fatal("exact mode spelling failed", mode, code, raw, diagnostic)
				}
				if strings.HasSuffix(mode, "=false") {
					if raw != idle {
						t.Fatal("false changed idle output bytes", raw, idle)
					}
				} else if machine {
					descriptor, _ := scannerPreviewDecode(t, raw)
					if descriptor.ScannerIntent == nil || descriptor.Contract != service.ScannerContract {
						t.Fatal("true did not select scanner intent", raw)
					}
				} else if !strings.Contains(raw, "SERVICE SCANNER DESCRIPTOR PREVIEW") {
					t.Fatal("true did not select scanner human preview", raw)
				}
			})
		}
	}
}

func TestServiceScannerPreviewStrictInput(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	tails := [][]string{
		{"--experimental-scan", "--experimental-scan"},
		{"--experimental-scan", "-experimental-scan"},
		{"--experimental-scan=true", "-experimental-scan=false"},
		{"-experimental-scan=false", "--experimental-scan=true"},
		{"--experimental-scan", "false"},
		{"--experimental-scan", "extra"},
		{"--worker-profile=experimental_inventory_v1"},
		{"--argv=--experimental-scan"},
		{"-executable=" + executable},
		{"--executable", executable},
	}
	for _, prefix := range []string{"--experimental-scan=", "-experimental-scan="} {
		for _, value := range []string{"TRUE", "True", "T", "t", "1", "FALSE", "False", "F", "f", "0", "", "yes", " true", "false "} {
			tails = append(tails, []string{prefix + value})
		}
	}
	for index, tail := range tails {
		for _, machine := range []bool{false, true} {
			t.Run(strconv.Itoa(index)+map[bool]string{false: "/human", true: "/JSON"}[machine], func(t *testing.T) {
				args := scannerPreviewArgs(dir, executable, tail...)
				if machine {
					args = append(args, "--json")
				}
				code, raw, diagnostic := scannerPreviewCapture(context.Background(), args)
				if code != 2 || (machine && !strings.Contains(raw, "\"invalid_arguments\"")) {
					t.Fatal("broader flag syntax or duplicate profile accepted", tail, code, raw, diagnostic)
				}
			})
		}
	}
	for _, args := range [][]string{{"--data-dir", dir, "service", "preview", "--experimental-scan"}, scannerPreviewArgs(dir, "")} {
		if code, raw, diagnostic := scannerPreviewCapture(context.Background(), append(args, "--json")); code != 2 || !strings.Contains(raw, "\"invalid_arguments\"") {
			t.Fatal("missing exact executable accepted", code, raw, diagnostic)
		}
	}
	for _, selected := range []string{dir, executable, os.Getenv("RYDD_RUNTIME_DIR")} {
		if _, err := os.Lstat(selected); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refused preview accessed generated storage", selected, err)
		}
	}
}

func TestServiceScannerPreviewJSONArgumentPreservation(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	standard := scannerPreviewArgs(dir, executable, "--experimental-scan", "--json")
	before := append([]string{"--json"}, scannerPreviewArgs(dir, executable, "--experimental-scan")...)
	firstCode, first, firstErr := scannerPreviewCapture(context.Background(), standard)
	secondCode, second, secondErr := scannerPreviewCapture(context.Background(), before)
	if firstCode != 0 || secondCode != 0 || firstErr != "" || secondErr != "" || first != second {
		t.Fatal("JSON position changed the scanner profile", firstCode, secondCode, first, second)
	}
	for _, args := range [][]string{
		{"--json", "--data-dir", dir, "service", "preview", "--executable", "--json", "--experimental-scan"},
		{"--json", "--data-dir", dir, "service", "preview", "-executable", "--experimental-scan"},
		append([]string{"--json"}, scannerPreviewArgs(dir, executable, "--", "--json", "--experimental-scan")...),
		scannerPreviewArgs(dir, executable, "--", "--json"),
	} {
		if code, raw, diagnostic := scannerPreviewCapture(context.Background(), args); code != 2 {
			t.Fatal("flag values or tokens after -- were reinterpreted", args, code, raw, diagnostic)
		}
	}
}

func TestServiceScannerPreviewNoStorageAccess(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	for _, machine := range []bool{false, true} {
		tail := []string{"--experimental-scan"}
		if machine {
			tail = append(tail, "--json")
		}
		if code, raw, diagnostic := scannerPreviewCapture(context.Background(), scannerPreviewArgs(dir, executable, tail...)); code != 0 {
			t.Fatal("missing-path scanner preview examined storage", code, raw, diagnostic)
		}
	}
	for _, selected := range []string{dir, executable, os.Getenv("RYDD_RUNTIME_DIR")} {
		if _, err := os.Lstat(selected); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("scanner preview created a missing path", selected, err)
		}
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	canary := []byte("generated invalid scanner preview storage\n")
	for _, name := range []string{"config.toml", "state.sqlite3", "writer.lock"} {
		if err := os.WriteFile(filepath.Join(dir, name), canary, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, machine := range []bool{false, true} {
		tail := []string{"--experimental-scan"}
		if machine {
			tail = append(tail, "--json")
		}
		if code, raw, diagnostic := scannerPreviewCapture(context.Background(), scannerPreviewArgs(dir, executable, tail...)); code != 0 {
			t.Fatal("scanner preview interpreted canary storage", code, raw, diagnostic)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
		t.Fatal("scanner preview changed the canary directory", entries, err)
	}
	for _, name := range []string{"config.toml", "state.sqlite3", "writer.lock"} {
		p := filepath.Join(dir, name)
		data, err := os.ReadFile(p)
		info, statErr := os.Lstat(p)
		if err != nil || statErr != nil || !bytes.Equal(data, canary) || info.Mode().Perm() != 0600 {
			t.Fatal("scanner preview changed a private storage canary", name, err, statErr)
		}
	}
}

func TestServiceScannerPreviewOutputAndCancellation(t *testing.T) {
	dir, executable := scannerPreviewFixture(t)
	for _, machine := range []bool{false, true} {
		args := scannerPreviewArgs(dir, executable, "--experimental-scan")
		if machine {
			args = append(args, "--json")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if code, raw, diagnostic := scannerPreviewCapture(ctx, args); code != 1 || (machine && !strings.Contains(raw, "\"canceled\"")) {
			t.Fatal("canceled scanner preview succeeded", code, raw, diagnostic)
		}
		for _, writer := range []io.Writer{serviceFailWriter{}, serviceShortWriter{}} {
			var diagnostic bytes.Buffer
			if code := Run(context.Background(), args, writer, &diagnostic); code != 1 {
				t.Fatal("failed scanner output reported success", code, diagnostic.String())
			}
		}
		lateCtx, lateCancel := context.WithCancel(context.Background())
		var diagnostic bytes.Buffer
		if code := Run(lateCtx, args, serviceCancelWriter{cancel: lateCancel}, &diagnostic); code != 1 {
			t.Fatal("cancellation during scanner output reported success", code, diagnostic.String())
		}
		lateCancel()
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed scanner preview created state", err)
	}
}

func TestServiceScannerPreviewCapabilityAndRouteBoundary(t *testing.T) {
	code, raw, diagnostic := scannerPreviewCapture(context.Background(), []string{"capabilities", "--json"})
	var report struct{ Features map[string]bool }
	if code != 0 || diagnostic != "" || json.Unmarshal([]byte(raw), &report) != nil {
		t.Fatal("scanner capability reply failed", code, raw, diagnostic)
	}
	for _, field := range []string{"service_scanner_descriptor_previews", "service_scanner_lifecycle", "service_scanner_installation"} {
		if !report.Features[field] {
			t.Fatal("explicit API capability absent", field)
		}
	}
	for _, field := range []string{"service_scanner_runtime_verification", "service_activation", "service_runtime_controls", "service_installation", "cpu_limit", "power_controls"} {
		if value, found := report.Features[field]; !found || value {
			t.Fatal("API capability became native/resource verification", field, found, value)
		}
	}
}
