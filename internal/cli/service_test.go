package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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
		{"preview", "--executable", "/a", "--experimental-scan"},
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
