package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func TestServiceRuntimeObservationCLIUsageBeforeStorageOrManager(t *testing.T) {
	base := filepath.Join(t.TempDir(), "uncreated data")
	t.Setenv("PATH", t.TempDir())
	for _, tail := range [][]string{
		{}, {"--executable"}, {"--executable", "relative"},
		{"--executable", "/generated/rydd", "extra"},
		{"--executable", "/generated/rydd", "status"},
		{"--executable", "/generated/rydd", "--executable=/other"},
		{"--executable", "/generated/rydd", "--directory="},
		{"--executable", "/generated/rydd", "--directory=/a", "--directory=/b"},
		{"--executable", "/generated/rydd", "--start"},
		{"--executable", "/generated/rydd", "--experimental-scan"},
		{"--executable", "--json"},
		{"--executable", "/generated/rydd", "--directory", "--json"},
	} {
		for _, machine := range []bool{false, true} {
			var out, diagnostic bytes.Buffer
			args := append([]string{"--data-dir", base, "service", "runtime-status"}, tail...)
			if machine {
				args = append([]string{"--json"}, args...)
			}
			if code := Run(context.Background(), args, &out, &diagnostic); code != 2 || machine && (!strings.Contains(out.String(), `"code":"invalid_arguments"`) || diagnostic.Len() != 0) {
				t.Fatal("runtime observation usage escaped preflight", args, code, out.String(), diagnostic.String())
			}
		}
	}
	for _, action := range []string{"status", "runtime-status"} {
		got, executable, directory, err := serviceLifecycleArguments([]string{action, "--executable=/generated/rydd $%", "--directory=/generated/systemd/user"})
		if err != nil || got != action || executable != "/generated/rydd $%" || directory != "/generated/systemd/user" {
			t.Fatal("exclusive action or literal arguments changed", got, executable, directory, err)
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid runtime observation initialized selected data", err)
	}
}

func TestServiceRuntimeObservationCLIWrapperPreservesScopeAndRefusals(t *testing.T) {
	spec := service.InstallSpec{Service: service.Spec{GOOS: "linux", Executable: "/generated/rydd", Paths: config.Paths{ConfigFile: "/generated/data/config.toml", StateDir: "/generated/data"}, RuntimeDir: "/generated/runtime"}, HomeDir: "/generated/home", Directory: "/generated/systemd/user", BusSocket: "/generated/bus"}
	want := service.RuntimeObservation{Contract: service.RuntimeObservationContract, Status: "unavailable", LookupAttempted: true, ManagerCalls: 3}
	calls := 0
	observer := func(ctx context.Context, got service.InstallSpec) (service.RuntimeObservation, error) {
		calls++
		if !reflect.DeepEqual(got, spec) || ctx.Err() != nil {
			t.Fatal("wrapper changed frozen scope", got)
		}
		return want, service.ErrManagerUnavailable
	}
	got, err := serviceRuntimeObservationWithObserver(context.Background(), spec, observer)
	if calls != 1 || !reflect.DeepEqual(got, want) || !errors.Is(err, service.ErrManagerUnavailable) {
		t.Fatal("wrapper lost partial attempt evidence", got, err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err = serviceRuntimeObservationWithObserver(ctx, spec, observer)
	if !errors.Is(err, context.Canceled) || calls != 1 || !reflect.DeepEqual(got, service.RuntimeObservation{}) {
		t.Fatal("canceled observation reached provider", got, err, calls)
	}
	_, err = serviceRuntimeObservationWithObserver(context.Background(), spec, func(context.Context, service.InstallSpec) (service.RuntimeObservation, error) {
		return want, service.ErrSpec
	})
	var usage usageError
	if !errors.As(err, &usage) || usage.Error() != service.ErrSpec.Error() {
		t.Fatal("invalid scope did not retain usage classification", err)
	}
}

func cliRuntimeObservationFixture() service.RuntimeObservation {
	active, sub, result, invocation := "active", "running", "success", "000102030405060708090a0b0c0d0e0f"
	matched, pid := true, uint32(1234)
	unitAt := time.Date(2026, 10, 9, 12, 0, 1, 0, time.UTC)
	serviceAt, bindingAt, finish := unitAt.Add(time.Second), unitAt.Add(2*time.Second), unitAt.Add(3*time.Second)
	return service.RuntimeObservation{Contract: service.RuntimeObservationContract, Platform: "linux", ManagerProfile: "systemd_user_v255", Label: service.ManagedLabel, Directory: "/generated/systemd/user", DescriptorPath: "/generated/systemd/user/managed.service", Supported: true, Status: "observed", ArtifactStatus: "exact", LookupAttempted: true, LookupRechecked: true, LoadedBindingMatched: &matched, UnitObservedAt: &unitAt, ServiceObservedAt: &serviceAt, BindingCheckedAt: &bindingAt, FinishedAt: &finish, ActiveState: &active, SubState: &sub, ServiceResult: &result, MainPID: &pid, InvocationID: &invocation, InvocationIDStatus: "observed", ManagerCalls: 7, ManagerReplyBytes: 4096, RuntimeStateObserved: true, SequentialObservation: true}
}

func TestServiceRuntimeObservationCLIHumanIsHistoricalAndNullable(t *testing.T) {
	r := cliRuntimeObservationFixture()
	for _, active := range []string{"active", "inactive", "future-state"} {
		r.ActiveState = &active
		var out bytes.Buffer
		if err := printServiceResult(&out, r); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"SERVICE RUNTIME OBSERVATION", "HISTORICAL MANAGER DECLARATIONS", "Active state: " + fmt.Sprintf("%q", active), "Substate: \"running\"", "Main PID: 1234", "Unit observed at: 2026-10-09T12:00:01Z", "Service observed at: 2026-10-09T12:00:02Z", "Running state: UNKNOWN", "Stopped state: UNKNOWN", "Application readiness: UNKNOWN", "loaded-file origin", "broker infrastructure", "No unit loading", "No report or Rydd state was saved"} {
			if !strings.Contains(out.String(), want) {
				t.Fatal("historical declaration acquired authority or lost scope", want, out.String())
			}
		}
	}
	r.ActiveState, r.SubState, r.ServiceResult, r.MainPID, r.InvocationID = nil, nil, nil, nil, nil
	r.UnitObservedAt, r.ServiceObservedAt, r.BindingCheckedAt, r.FinishedAt = nil, nil, nil, nil
	r.LoadedBindingMatched = nil
	r.Status, r.InvocationIDStatus, r.Platform = "unsupported", "unexamined", "darwin"
	var out bytes.Buffer
	if err := printServiceResult(&out, r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Active state: NOT RECORDED", "Main PID: NOT RECORDED", "Unit observed at: NOT RECORDED", "Loaded settings match: NOT RECORDED", "No artifact or launchctl probe was made"} {
		if !strings.Contains(out.String(), want) {
			t.Fatal("missing declaration became zero or known state", want, out.String())
		}
	}
	if strings.Contains(out.String(), "0001-") || strings.Contains(out.String(), "Main PID: 0") {
		t.Fatal("unobserved value rendered as measured zero", out.String())
	}
	for _, writer := range []interface{ Write([]byte) (int, error) }{serviceFailWriter{}, serviceShortWriter{}} {
		if err := printServiceResult(writer, r); err == nil {
			t.Fatal("failed observation reply succeeded")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := printServiceResult(serviceCancelWriter{cancel: cancel}, r); err != nil || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("full-count late cancellation was hidden", err, ctx.Err())
	}
}

func TestServiceRuntimeObservationCLIErrorRetainsOnlyPartialEvidence(t *testing.T) {
	matched := false
	r := service.RuntimeObservation{Contract: service.RuntimeObservationContract, Platform: "linux", Status: "refused", ArtifactStatus: "exact", DescriptorPath: "/generated/systemd/user/managed.service", Label: service.ManagedLabel, LookupAttempted: true, LoadedBindingMatched: &matched, ManagerCalls: 5, ManagerReplyBytes: 1024}
	for _, tc := range []struct {
		err  error
		code string
	}{
		{service.ErrRuntimeObservationClock, "service_observation_clock"},
		{service.ErrRuntimeBinding, "service_runtime_binding"},
		{service.ErrManagerProtocol, "service_manager_protocol"},
		{service.ErrManagerUnavailable, "service_manager_unavailable"},
		{errors.Join(service.ErrRuntimeObservationClock, context.DeadlineExceeded), "canceled"},
		{errors.Join(service.ErrManagerUnavailable, context.Canceled), "canceled"},
	} {
		var out, diagnostic bytes.Buffer
		code := serviceMachineFailure(&out, &diagnostic, r, errors.Join(tc.err, errors.New("untrusted manager secret\x1b")))
		var envelope struct {
			Version int                        `json:"api_version"`
			OK      bool                       `json:"ok"`
			Command string                     `json:"command"`
			Service service.RuntimeObservation `json:"service"`
			Error   struct{ Code string }      `json:"error"`
		}
		if code != 1 || json.Unmarshal(out.Bytes(), &envelope) != nil || envelope.Version != APIVersion || envelope.OK || envelope.Command != "service" || envelope.Error.Code != tc.code || envelope.Service.Contract != service.RuntimeObservationContract || envelope.Service.ManagerCalls != 5 || envelope.Service.LoadedBindingMatched == nil || *envelope.Service.LoadedBindingMatched || envelope.Service.ActiveState != nil || envelope.Service.MainPID != nil || envelope.Service.Running != nil || envelope.Service.Stopped != nil || envelope.Service.ApplicationReady != nil || diagnostic.Len() != 0 || strings.Contains(out.String(), "secret") || strings.Contains(out.String(), "untrusted") {
			t.Fatal("observation failure lost attempts or projected proof/helper text", code, out.String(), diagnostic.String())
		}
	}
	var diagnostic bytes.Buffer
	if code := serviceMachineFailure(serviceShortWriter{}, &diagnostic, r, service.ErrManagerUnavailable); code != 1 || !strings.Contains(diagnostic.String(), "write JSON") || !strings.Contains(diagnostic.String(), "No report or Rydd state was saved") || strings.Contains(diagnostic.String(), "publication") {
		t.Fatal("failed error reply invented publication or lost qualification", code, diagnostic.String())
	}
}

func TestServiceRuntimeObservationCLIPlatformAndLateReply(t *testing.T) {
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, executable, runtimeDir := filepath.Join(temp, "uncreated data"), filepath.Join(temp, "uncreated executable"), filepath.Join(temp, "uncreated runtime")
	t.Setenv("PATH", t.TempDir())
	t.Setenv("RYDD_RUNTIME_DIR", runtimeDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(temp, "uncreated manager"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(temp, "uncreated bus"))
	args := []string{"--data-dir", data, "service", "runtime-status", "--executable", executable}
	var out, diagnostic bytes.Buffer
	code := Run(context.Background(), append([]string{"--json"}, args...), &out, &diagnostic)
	var envelope struct {
		OK      bool                       `json:"ok"`
		Service service.RuntimeObservation `json:"service"`
		Error   struct{ Code string }      `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatal(code, out.String(), diagnostic.String(), err)
	}
	if runtime.GOOS == "darwin" {
		if code != 0 || !envelope.OK || envelope.Service.Status != "unsupported" || envelope.Service.Supported || envelope.Service.ManagerCalls != 0 || envelope.Service.LookupAttempted || envelope.Service.ActiveState != nil || envelope.Service.Running != nil || diagnostic.Len() != 0 {
			t.Fatal("unsupported Mac observation probed or claimed state", code, out.String(), diagnostic.String())
		}
		for _, outputArgs := range [][]string{args, append([]string{"--json"}, args...)} {
			for _, writer := range []interface{ Write([]byte) (int, error) }{serviceFailWriter{}, serviceShortWriter{}} {
				diagnostic.Reset()
				if code := Run(context.Background(), outputArgs, writer, &diagnostic); code != 1 || !strings.Contains(diagnostic.String(), "No report or Rydd state was saved") {
					t.Fatal("unsupported observation reply failure succeeded", code, diagnostic.String())
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			diagnostic.Reset()
			code := Run(ctx, outputArgs, serviceCancelWriter{cancel: cancel}, &diagnostic)
			cancel()
			if code != 1 || !strings.Contains(diagnostic.String(), "No report or Rydd state was saved") {
				t.Fatal("late cancellation reported successful observation reply", code, diagnostic.String())
			}
		}
	} else if code != 1 || envelope.OK || envelope.Error.Code != "not_found" || envelope.Service.ManagerCalls != 0 || envelope.Service.ActiveState != nil || envelope.Service.Running != nil {
		t.Fatal("missing generated Linux artifact inferred runtime or invoked manager", code, out.String(), diagnostic.String())
	}
	for _, path := range []string{data, executable, runtimeDir, os.Getenv("XDG_CONFIG_HOME"), os.Getenv("XDG_RUNTIME_DIR")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("runtime observation initialized selected storage", path, err)
		}
	}
}

func cliRuntimeObservationWire(t *testing.T, signature string, value any) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{"type": signature, "data": []any{value}})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestServiceRuntimeObservationCLIGeneratedLinuxManager(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("generated Linux manager protocol; native Mac unsupported is tested separately")
	}
	temp, err := os.MkdirTemp("", "rydd-observation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(temp) })
	temp, err = filepath.EvalSymlinks(temp)
	if err != nil {
		t.Fatal(err)
	}
	data, executable, runtimeDir := filepath.Join(temp, "data"), filepath.Join(temp, "rydd"), filepath.Join(temp, "runtime")
	directory, busDir := filepath.Join(temp, "systemd", "user"), filepath.Join(temp, "bus")
	for _, path := range []string{directory, busDir, data} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	canary := []byte("generated invalid config/state/executable body remains unopened\n")
	for _, path := range []string{filepath.Join(data, "config.toml"), filepath.Join(data, "state.sqlite3"), executable} {
		if err := os.WriteFile(path, canary, 0600); err != nil {
			t.Fatal(err)
		}
	}
	descriptor, err := service.Build(service.Spec{GOOS: "linux", Executable: executable, Paths: config.Paths{ConfigFile: filepath.Join(data, "config.toml"), StateDir: data}, RuntimeDir: runtimeDir})
	if err != nil {
		t.Fatal(err)
	}
	descriptorPath := filepath.Join(directory, descriptor.Filename)
	if err := os.WriteFile(descriptorPath, []byte(descriptor.Content), 0600); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(directory, "."+service.ManagedLabel+".lock")
	if err := os.WriteFile(lockPath, []byte("generated existing coordinator\n"), 0600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(busDir, "bus")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	property := func(signature string, value any) any { return map[string]any{"type": signature, "data": value} }
	unit := map[string]any{"Id": property("s", descriptor.Filename), "FragmentPath": property("s", descriptorPath), "LoadState": property("s", "loaded"), "SourcePath": property("s", ""), "Following": property("s", ""), "DropInPaths": property("as", []string{}), "NeedDaemonReload": property("b", false), "Transient": property("b", false), "ActiveState": property("s", "active"), "SubState": property("s", "running"), "InvocationID": property("ay", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})}
	settings := map[string]any{"Type": property("s", "exec"), "Restart": property("s", "on-failure"), "RootDirectory": property("s", ""), "RootImage": property("s", ""), "Environment": property("as", []string{"RYDD_RUNTIME_DIR=" + runtimeDir}), "EnvironmentFiles": property("a(sb)", []any{}), "PassEnvironment": property("as", []string{}), "UnsetEnvironment": property("as", []string{}), "RestartUSec": property("t", 30_000_000), "TimeoutStopUSec": property("t", 10_000_000), "MainPID": property("u", 1234), "Result": property("s", "success"), "PrivateCanary": property("s", "discard-private-manager-canary")}
	for _, name := range []string{"ExecConditionEx", "ExecStartPreEx", "ExecStartPostEx", "ExecStopEx", "ExecStopPostEx", "ExecReloadEx"} {
		settings[name] = property("a(sasasttttuii)", []any{})
	}
	settings["ExecStartEx"] = property("a(sasasttttuii)", []any{[]any{executable, descriptor.Argv, []string{"no-env-expand"}, 111, 222, 333, 444, 555, 1, -2}})
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
	logPath := filepath.Join(temp, "calls")
	script := "#!/bin/sh\nprintf '%s\\n' '--CALL--' \"$@\" >> " + quote(logPath) + "\ncase \"${11}\" in\nGetNameOwner) printf '%s\\n' " + quote(cliRuntimeObservationWire(t, "s", ":1.15")) + ";;\nGet) printf '%s\\n' " + quote(cliRuntimeObservationWire(t, "v", map[string]any{"type": "as", "data": []string{directory}})) + ";;\nGetUnit) printf '%s\\n' " + quote(cliRuntimeObservationWire(t, "o", "/org/freedesktop/systemd1/unit/managed")) + ";;\nGetAll) case \"${13}\" in\norg.freedesktop.systemd1.Unit) printf '%s\\n' " + quote(cliRuntimeObservationWire(t, "a{sv}", unit)) + ";;\norg.freedesktop.systemd1.Service) printf '%s\\n' " + quote(cliRuntimeObservationWire(t, "a{sv}", settings)) + ";;\n*) exit 64;; esac;;\n*) exit 64;; esac\n"
	program := filepath.Join(temp, "busctl")
	if err := os.WriteFile(program, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", temp)
	t.Setenv("RYDD_RUNTIME_DIR", runtimeDir)
	t.Setenv("XDG_RUNTIME_DIR", busDir)
	args := []string{"--data-dir", data, "service", "runtime-status", "--executable", executable, "--directory", directory}
	for _, machine := range []bool{false, true} {
		if err := os.WriteFile(logPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		var out, diagnostic bytes.Buffer
		argv := args
		if machine {
			argv = append([]string{"--json"}, args...)
		}
		if code := Run(context.Background(), argv, &out, &diagnostic); code != 0 || diagnostic.Len() != 0 {
			t.Fatal("generated manager observation failed", code, out.String(), diagnostic.String())
		}
		if strings.Contains(out.String(), "discard-private-manager-canary") || strings.Contains(out.String(), "PrivateCanary") {
			t.Fatal("unprojected private manager fields escaped output")
		}
		if machine {
			var envelope struct {
				OK      bool                       `json:"ok"`
				Command string                     `json:"command"`
				Service service.RuntimeObservation `json:"service"`
			}
			if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || !envelope.OK || envelope.Command != "service" || envelope.Service.Contract != service.RuntimeObservationContract || envelope.Service.Status != "observed" || envelope.Service.ManagerCalls != 7 || envelope.Service.MainPID == nil || *envelope.Service.MainPID != 1234 || envelope.Service.Running != nil || envelope.Service.Stopped != nil || envelope.Service.ApplicationReady != nil || envelope.Service.RuntimeStateVerified || envelope.Service.LoadedOriginVerified || envelope.Service.ProcessIdentityVerified || envelope.Service.ApplicationReadinessVerified || envelope.Service.UnitLoadRequested || envelope.Service.StartStopRequested || envelope.Service.ScanningRequested || envelope.Service.EnablementChanged || envelope.Service.ReloadRequested || envelope.Service.DescriptorChanged || envelope.Service.SourceContentsOpened {
				t.Fatal("machine observation acquired authority", out.String(), err)
			}
		} else if !strings.Contains(out.String(), "Main PID: 1234") || !strings.Contains(out.String(), "Running state: UNKNOWN") || !strings.Contains(out.String(), "sequential historical manager declarations") {
			t.Fatal("human observation lost historical qualification", out.String())
		}
		logged, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		calls := strings.Split(strings.TrimSpace(string(logged)), "--CALL--\n")
		if len(calls) != 8 || calls[0] != "" {
			t.Fatal("request count changed", string(logged))
		}
		methods := []string{"GetNameOwner", "Get", "GetUnit", "GetAll", "GetAll", "GetUnit", "GetNameOwner"}
		for i, call := range calls[1:] {
			values := strings.Split(strings.TrimSpace(call), "\n")
			prefix := []string{"--address=unix:path=" + socket, "--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call"}
			if len(values) < 13 || !reflect.DeepEqual(values[:7], prefix) || values[10] != methods[i] || methods[i] != "GetNameOwner" && values[7] != ":1.15" {
				t.Fatal("CLI observation gained another manager route/action", values)
			}
		}
	}
	for _, outputArgs := range [][]string{args, append([]string{"--json"}, args...)} {
		for _, writer := range []interface{ Write([]byte) (int, error) }{serviceFailWriter{}, serviceShortWriter{}} {
			var diagnostic bytes.Buffer
			if code := Run(context.Background(), outputArgs, writer, &diagnostic); code != 1 || !strings.Contains(diagnostic.String(), "No report or Rydd state was saved") {
				t.Fatal("generated successful observation survived failed reply", code, diagnostic.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		var diagnostic bytes.Buffer
		code := Run(ctx, outputArgs, serviceCancelWriter{cancel: cancel}, &diagnostic)
		cancel()
		if code != 1 || !strings.Contains(diagnostic.String(), "No report or Rydd state was saved") {
			t.Fatal("generated successful observation hid late reply cancellation", code, diagnostic.String())
		}
	}
	for _, path := range []string{filepath.Join(data, "config.toml"), filepath.Join(data, "state.sqlite3"), executable} {
		if body, err := os.ReadFile(path); err != nil || !bytes.Equal(body, canary) {
			t.Fatal("runtime observation changed source/config/state/executable", path, err)
		}
	}
	if body, err := os.ReadFile(descriptorPath); err != nil || string(body) != descriptor.Content {
		t.Fatal("runtime observation changed artifact", err)
	}
	if body, err := os.ReadFile(lockPath); err != nil || string(body) != "generated existing coordinator\n" {
		t.Fatal("runtime observation changed coordinator", err)
	}
	if _, err := os.Lstat(runtimeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("runtime observation initialized worker IPC", err)
	}
}
