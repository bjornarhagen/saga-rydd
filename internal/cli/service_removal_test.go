package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func TestServiceRemovalUsageAndMissingArtifactCreateNothing(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(home, "uncreated data")
	for _, args := range [][]string{
		{"uninstall"}, {"uninstall", "--executable"},
		{"uninstall", "--executable", "relative"},
		{"uninstall", "--executable", "/generated/rydd", "--stop"},
		{"uninstall", "--executable", "/generated/rydd", "--purge"},
		{"uninstall", "--executable", "/generated/rydd", "--executable", "/other"},
	} {
		var out, diagnostic bytes.Buffer
		argv := append([]string{"--json", "--data-dir", data, "service"}, args...)
		if code := Run(context.Background(), argv, &out, &diagnostic); code != 2 || diagnostic.Len() != 0 || !strings.Contains(out.String(), `"code":"invalid_arguments"`) {
			t.Fatal("invalid removal arguments escaped preflight", args, code, out.String(), diagnostic.String())
		}
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(home, "systemd", "user")
	}
	spec := service.InstallSpec{Service: service.Spec{GOOS: runtime.GOOS, Executable: filepath.Join(home, "uncreated executable"), Paths: config.Paths{StateDir: data, ConfigFile: filepath.Join(data, "config.toml")}, RuntimeDir: filepath.Join(home, "uncreated runtime")}, HomeDir: home, Directory: dir, BusSocket: filepath.Join(home, "absent bus")}
	r, err := runServiceRemoval(context.Background(), spec)
	if !errors.Is(err, os.ErrNotExist) || r.RemovalAttempted || r.UnlinkCompleted || r.RemovalObserved != nil || r.ManagerRequestAttempted || r.Running != nil || r.Stopped != nil {
		t.Fatal("missing artifact mutated scope or inferred removal", r, err)
	}
	for _, path := range []string{dir, data, spec.Service.Executable, spec.Service.RuntimeDir} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("removal initialized missing scope", path, err)
		}
	}
}

func TestServiceRemovalReportsPartialEffectsWithoutRuntimeProof(t *testing.T) {
	observed := true
	r := service.RemovalResult{Contract: service.RemovalContract, Action: "uninstall", Platform: "darwin", DescriptorPath: "/generated/descriptor", RemovalStatus: "unknown", RemovalAttempted: true, UnlinkCompleted: true, RemovalObserved: &observed, DescriptorAbsent: &observed}
	var out, diagnostic bytes.Buffer
	if err := printServiceResult(&out, r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Unlink completed: true", "Directory sync completed: false", "Running state: Unknown", "Stopped state: Unknown", "no stop", "can continue running", "stop before", "current absence"} {
		if !strings.Contains(strings.ToLower(out.String()), strings.ToLower(want)) {
			t.Fatal("partial removal report lost effect qualification", want, out.String())
		}
	}
	if err := printServiceResult(serviceShortWriter{}, r); err == nil {
		t.Fatal("short removal output succeeded")
	}
	out.Reset()
	code := serviceMachineFailure(&out, &diagnostic, r, errors.Join(service.ErrArtifactRemoval, context.Canceled, errors.New("private helper text")))
	var envelope struct {
		OK      bool                  `json:"ok"`
		Service service.RemovalResult `json:"service"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if code != 1 || json.Unmarshal(out.Bytes(), &envelope) != nil || envelope.OK || envelope.Error.Code != "canceled" || !envelope.Service.UnlinkCompleted || envelope.Service.SyncCompleted || envelope.Service.RemovalObserved == nil || !*envelope.Service.RemovalObserved || envelope.Service.Running != nil || envelope.Service.Stopped != nil || diagnostic.Len() != 0 || strings.Contains(out.String(), "private helper") {
		t.Fatal("failed removal lost known stages or widened evidence", code, out.String(), diagnostic.String())
	}
	message := serviceReplyMessage(r)
	if !strings.Contains(message, "completed: true") || !strings.Contains(message, "sync completed: false") || !strings.Contains(message, "Runtime remains unknown") {
		t.Fatal("failed reply lost effect or inspection guidance", message)
	}
}
