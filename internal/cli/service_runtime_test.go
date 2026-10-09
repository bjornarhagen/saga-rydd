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

func TestServiceRuntimeUsageBeforeManagerOrStorage(t *testing.T) {
	base := filepath.Join(t.TempDir(), "unused state")
	for _, action := range []string{"start", "stop"} {
		for _, args := range [][]string{
			{action}, {action, "--executable"}, {action, "--executable", "relative"},
			{action, "--executable", "/generated/rydd", "extra"},
			{action, "--executable", "/generated/rydd", "--executable", "/other"},
			{action, "--executable", "/generated/rydd", "--directory", ""},
			{action, "--executable", "/generated/rydd", "--enable"},
			{action, "--executable", "/generated/rydd", "--experimental-scan"},
		} {
			var out, diagnostic bytes.Buffer
			argv := append([]string{"--data-dir", base, "service"}, args...)
			code := Run(context.Background(), append([]string{"--json"}, argv...), &out, &diagnostic)
			if code != 2 || diagnostic.Len() != 0 || !strings.Contains(out.String(), `"code":"invalid_arguments"`) {
				t.Fatal("invalid runtime options escaped preflight", args, code, out.String(), diagnostic.String())
			}
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("runtime usage created selected data", err)
	}
}

func TestServiceRuntimeMissingArtifactCreatesNothing(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(home, "systemd", "user")
	}
	data := filepath.Join(home, "uncreated data")
	spec := service.InstallSpec{Service: service.Spec{GOOS: runtime.GOOS, Executable: filepath.Join(home, "uncreated executable"), Paths: config.Paths{StateDir: data, ConfigFile: filepath.Join(data, "config.toml")}, RuntimeDir: filepath.Join(home, "uncreated runtime")}, HomeDir: home, Directory: dir, BusSocket: filepath.Join(home, "absent bus")}
	for _, action := range []string{"start", "stop"} {
		r, err := runServiceRuntime(context.Background(), action, spec)
		if !errors.Is(err, os.ErrNotExist) || r.RequestAttempted || r.RequestAccepted != nil || r.Running != nil || r.Stopped != nil || r.DescriptorChanged || r.ScanningRequested {
			t.Fatal("missing artifact requested manager work", action, r, err)
		}
	}
	for _, path := range []string{dir, data, spec.Service.Executable, spec.Service.RuntimeDir} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("runtime preflight initialized scope", path, err)
		}
	}
}

func TestServiceRuntimeReportsHistoricalAcceptanceAndUnknownRuntime(t *testing.T) {
	accepted := true
	job := "/org/freedesktop/systemd1/job/123"
	for _, platform := range []string{"linux", "darwin"} {
		r := service.RuntimeResult{Contract: service.RuntimeContract, Action: "start", Platform: platform, Label: service.ManagedLabel, DescriptorPath: "/generated/descriptor", RequestAttempted: true, RequestStatus: "accepted", RequestAccepted: &accepted, AcceptanceEvidence: "opaque_launchctl_exit_zero"}
		if platform == "linux" {
			r.JobPath = &job
			r.AcceptanceEvidence = "typed_queued_job"
			r.UnitLoadAttempted = true
		}
		var out bytes.Buffer
		if err := printServiceResult(&out, r); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"Client reported acceptance", "Running state: Unknown", "Stopped state: Unknown", "No scanning, enablement or reload", "future-login effects", "Inspect uncertain requests"} {
			if !strings.Contains(out.String(), want) {
				t.Fatal("request report inferred runtime proof", want, out.String())
			}
		}
		if platform == "linux" && (!strings.Contains(out.String(), "queued job") || !strings.Contains(out.String(), "dependencies") || !strings.Contains(out.String(), "bookkeeping")) {
			t.Fatal("queued request lost effect qualifications", out.String())
		}
		if platform == "darwin" && !strings.Contains(out.String(), "does not authenticate") {
			t.Fatal("opaque reply authenticated loaded origin", out.String())
		}
		if err := printServiceResult(serviceShortWriter{}, r); err == nil {
			t.Fatal("short request report succeeded")
		}
		out.Reset()
		var diagnostic bytes.Buffer
		code := serviceMachineFailure(&out, &diagnostic, r, errors.Join(service.ErrRuntimeOutcome, context.Canceled, errors.New("untrusted helper secret")))
		// A known acknowledgment remains historical even when a later check fails.
		var envelope struct {
			OK      bool                  `json:"ok"`
			Service service.RuntimeResult `json:"service"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		encoded := out.Bytes()
		if code != 1 || json.Unmarshal(encoded, &envelope) != nil || envelope.OK || envelope.Error.Code != "canceled" || envelope.Service.RequestAccepted == nil || !*envelope.Service.RequestAccepted || envelope.Service.Running != nil || envelope.Service.Stopped != nil || diagnostic.Len() != 0 || bytes.Contains(encoded, []byte("secret")) {
			t.Fatal("failed request lost known ack or widened proof", code, string(encoded), diagnostic.String())
		}
		if message := serviceReplyMessage(r); !strings.Contains(message, "unverified") || !strings.Contains(message, "do not automatically retry") {
			t.Fatal("failed reply lost inspection guidance", message)
		}
		if platform == "linux" {
			r.RequestAttempted = false
			r.RequestAccepted = nil
			r.RequestStatus = "refused"
			if message := serviceReplyMessage(r); !strings.Contains(message, "Request attempted: false") || !strings.Contains(message, "preflight unit loading was attempted") || !strings.Contains(message, "bookkeeping can change") {
				t.Fatal("preflight failure hid a unit-load effect", message)
			}
		}
	}
}
