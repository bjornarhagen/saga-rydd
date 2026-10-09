package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func TestServiceLifecyclePlacementIsSeparateFromChildData(t *testing.T) {
	home := filepath.Join(t.TempDir(), "generated home")
	stateDir := filepath.Join(home, "selected child data")
	paths := config.Paths{ConfigFile: filepath.Join(stateDir, "config.toml"), StateDir: stateDir}
	t.Setenv("RYDD_RUNTIME_DIR", filepath.Join(home, "worker runtime"))
	for _, test := range []struct {
		host    serviceHost
		wantDir string
		wantBus string
	}{
		{serviceHost{GOOS: "darwin", HomeDir: home, UID: 42}, filepath.Join(home, "Library", "LaunchAgents"), ""},
		{serviceHost{GOOS: "linux", HomeDir: home, UID: 42}, filepath.Join(home, ".config", "systemd", "user"), "/run/user/42/bus"},
		{serviceHost{GOOS: "linux", HomeDir: home, XDGConfigHome: "/generated config $%", XDGRuntimeDir: "/generated runtime", UID: 42}, "/generated config $%/systemd/user", "/generated runtime/bus"},
	} {
		spec := serviceInstallSpec(paths, "/generated/bin/rydd", "", test.host)
		if spec.Directory != test.wantDir || spec.BusSocket != test.wantBus || spec.Service.Paths != paths || spec.Service.RuntimeDir != os.Getenv("RYDD_RUNTIME_DIR") || spec.HomeDir != home {
			t.Fatal("service placement widened or reused child data scope", spec)
		}
		overridden := serviceInstallSpec(paths, "/generated/bin/rydd", "/explicit/systemd/user", test.host)
		if overridden.Directory != "/explicit/systemd/user" || overridden.Service.Paths != paths || overridden.BusSocket != test.wantBus {
			t.Fatal("explicit placement changed child state or bus scope", overridden)
		}
	}
	if _, err := os.Lstat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("path resolution initialized home/data/runtime", err)
	}
}

func TestServiceLifecycleStrictArgumentsBeforePlacement(t *testing.T) {
	base := filepath.Join(t.TempDir(), "uncreated child state")
	for _, action := range []string{"install", "status"} {
		for _, tail := range [][]string{
			{}, {"--executable", "/a", "--executable=/b"},
			{"--executable", "/a", "--directory", "/a", "--directory=/b"},
			{"--executable", "/a", "--directory="},
			{"--executable", "/a", "extra"}, {"--executable", "/a", "--experimental-scan"},
			{"--executable", "--json"}, {"--executable", "/a", "--directory", "--json"},
		} {
			var out, errOut bytes.Buffer
			args := append([]string{"--data-dir", base, "service", action}, tail...)
			args = append(args, "--json")
			if code := Run(context.Background(), args, &out, &errOut); code != 2 || !strings.Contains(out.String(), `"invalid_arguments"`) {
				t.Fatal("invalid lifecycle input passed preflight", args, code, out.String(), errOut.String())
			}
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid lifecycle command initialized data", err)
	}
	if action, executable, directory, err := serviceLifecycleArguments([]string{"install", "--executable=/a $%", "--directory=/b/systemd/user"}); err != nil || action != "install" || executable != "/a $%" || directory != "/b/systemd/user" {
		t.Fatal("literal lifecycle values changed", action, executable, directory, err)
	}
}

func TestServiceLifecycleGeneratedPublicationAndReports(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(home, "uncreated state $%")
	t.Setenv("RYDD_RUNTIME_DIR", filepath.Join(home, "uncreated runtime"))
	paths := config.Paths{ConfigFile: filepath.Join(dataDir, "config.toml"), StateDir: dataDir}
	spec := serviceInstallSpec(paths, filepath.Join(home, "uncreated executable $%"), "", serviceHost{GOOS: "darwin", HomeDir: home})
	before, err := runServiceLifecycle(context.Background(), "status", spec)
	if err != nil || before.ArtifactStatus != "absent" || before.InstallationPerformed || before.ActivationPerformed {
		t.Fatal("absent artifact status differs", before, err)
	}
	installed, err := runServiceLifecycle(context.Background(), "install", spec)
	if err != nil || installed.Publication != "saved" || !installed.InstallationPerformed || installed.ActivationPerformed || installed.ScanningEnabled {
		t.Fatal("generated install differs", installed, err)
	}
	wantBytes, err := os.ReadFile(installed.DescriptorPath)
	if err != nil || string(wantBytes) != installed.Content {
		t.Fatal("published artifact differs", err)
	}
	again, err := runServiceLifecycle(context.Background(), "install", spec)
	if err != nil || again.Publication != "not_needed" || again.InstallationPerformed {
		t.Fatal("exact install retry claimed another publication", again, err)
	}
	status, err := runServiceLifecycle(context.Background(), "status", spec)
	if err != nil || status.ArtifactStatus != "exact" || status.Publication != "not_requested" || status.InstallationPerformed || status.Manager.Status != "not_checked" {
		t.Fatal("artifact status inferred runtime state", status, err)
	}
	var out bytes.Buffer
	if err := printServiceResult(&out, installed); err != nil || !strings.Contains(out.String(), "future login") || !strings.Contains(out.String(), "Runtime state and") || !strings.Contains(out.String(), "contents were not checked") {
		t.Fatal("human install report lost its scope", err, out.String())
	}
	if err := printServiceResult(serviceFailWriter{}, installed); err == nil {
		t.Fatal("failed publication report claimed success")
	}
	if err := printServiceResult(serviceShortWriter{}, installed); err == nil {
		t.Fatal("short publication report claimed success")
	}
	if message := serviceReplyMessage(installed); !strings.Contains(message, "saved") || !strings.Contains(message, "Inspect service status") || strings.Contains(message, "changed no") {
		t.Fatal("failed reply hid published artifact", message)
	}
	for _, path := range []string{dataDir, spec.Service.Executable, spec.Service.RuntimeDir} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("artifact operation accessed/created child scope", path, err)
		}
	}
}

func TestServiceLifecycleErrorEnvelopeRetainsUncertainPublication(t *testing.T) {
	r := service.LifecycleResult{Directory: "/generated/Library/LaunchAgents", DescriptorPath: "/generated/Library/LaunchAgents/rydd.plist", Publication: "uncertain", Action: "install", DirectoriesCreated: []string{"/generated/Library", "/generated/Library/LaunchAgents"}, LockCreated: true}
	for _, test := range []struct {
		err  error
		code string
	}{
		{service.ErrArtifactPublication, "service_outcome_unknown"},
		{service.ErrArtifactConflict, "service_artifact_conflict"},
		{service.ErrArtifactChanged, "service_artifact_changed"},
		{service.ErrArtifactBusy, "service_artifact_busy"},
		{service.ErrManagerUnavailable, "service_manager_unavailable"},
		{service.ErrManagerProtocol, "service_manager_protocol"},
		{service.ErrManagerPath, "service_manager_path"},
		{service.ErrLifecycleBounds, "service_bounds"},
		{errors.Join(service.ErrArtifactPublication, context.Canceled), "canceled"},
	} {
		var out, errOut bytes.Buffer
		code := serviceMachineFailure(&out, &errOut, r, errors.Join(test.err, errors.New("untrusted private helper output\x1b")))
		var envelope struct {
			OK      bool                    `json:"ok"`
			Service service.LifecycleResult `json:"service"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if code != 1 || json.Unmarshal(out.Bytes(), &envelope) != nil || envelope.OK || envelope.Error.Code != test.code || envelope.Service.Publication != "uncertain" || envelope.Service.DescriptorPath != r.DescriptorPath || strings.Contains(out.String(), "untrusted") || strings.Contains(errOut.String(), "untrusted") || !strings.Contains(errOut.String(), "Created coordinator lock") || !strings.Contains(errOut.String(), `Created directory: "/generated/Library"`) {
			t.Fatal("error lost stages or leaked external output", code, out.String(), errOut.String())
		}
	}
}

func TestServiceLifecycleHumanManagerMembershipIsNotInferred(t *testing.T) {
	for _, membership := range []*bool{nil, new(bool), func() *bool { value := true; return &value }()} {
		r := service.LifecycleResult{Action: "status", Manager: service.ManagerObservation{Status: "observed", DirectoryInUnitPath: membership}}
		var out bytes.Buffer
		if err := printServiceResult(&out, r); err != nil {
			t.Fatal(err)
		}
		want := "Not checked"
		if membership != nil {
			if *membership {
				want = "true"
			} else {
				want = "false"
			}
		}
		if !strings.Contains(out.String(), "Directory in manager UnitPath: "+want) {
			t.Fatal("manager observation hid unknown or false directory visibility", out.String())
		}
	}
}
