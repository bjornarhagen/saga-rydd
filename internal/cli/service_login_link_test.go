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
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func TestServiceLoginLinkCLIStrictArgumentsBeforeAccess(t *testing.T) {
	base := filepath.Join(t.TempDir(), "uncreated state")
	for _, action := range []string{"enable-login", "disable-login"} {
		for _, tail := range [][]string{
			{}, {"--executable", "/a", "--executable=/b"},
			{"--executable", "/a", "--directory", "/a", "--directory=/b"},
			{"--executable", "/a", "--directory="},
			{"--executable", "/a", "extra"}, {"--executable", "/a", "--experimental-scan"},
			{"--executable", "/a", "--now=false"}, {"--executable", "/a", "--force"},
			{"--executable", "/a", "--enable-login=false"}, {"--executable", "/a", "--disable-login"},
			{"--executable", "--json"}, {"-executable", "-json"},
			{"--executable", "/a", "--directory", "--json"},
			{"-executable", "/a", "-directory", "-json"},
			{"--executable", "/a", "--", "--json"},
			{"--executable=relative"},
		} {
			var out, errOut bytes.Buffer
			args := append([]string{"--json", "--data-dir", base, "service", action}, tail...)
			code := Run(context.Background(), args, &out, &errOut)
			if code != 2 || !strings.Contains(out.String(), `"invalid_arguments"`) || errOut.Len() != 0 || strings.Count(out.String(), `"api_version"`) != 1 {
				t.Fatal("invalid selected-link input reached access or changed output", args, code, out.String(), errOut.String())
			}
		}
		for _, tail := range [][]string{{"--executable", "--json"}, {"-executable", "-json"}, {"--executable", "/a", "--directory", "--json"}, {"-executable", "/a", "-directory", "-json"}} {
			var out, errOut bytes.Buffer
			args := append([]string{"--data-dir", base, "service", action}, tail...)
			if code := Run(context.Background(), args, &out, &errOut); code != 2 || out.Len() != 0 || strings.Contains(errOut.String(), `"api_version"`) {
				t.Fatal("output-looking value was consumed as JSON mode", args, code, out.String(), errOut.String())
			}
		}
		if gotAction, executable, directory, err := serviceLifecycleArguments([]string{action, "--executable=/a $%", "--directory=/b \"quoted\"/systemd/user"}); err != nil || gotAction != action || executable != "/a $%" || directory != "/b \"quoted\"/systemd/user" {
			t.Fatal("literal selected-link scope changed", gotAction, executable, directory, err)
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused selected-link command initialized state", err)
	}
}

func TestServiceLoginLinkCLIMissingAndUnsupportedRemainExistingOnly(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(home, "uncreated state")
	spec := serviceInstallSpec(config.Paths{ConfigFile: filepath.Join(data, "config.toml"), StateDir: data}, filepath.Join(home, "uncreated executable"), "", serviceHost{GOOS: runtime.GOOS, HomeDir: home, UID: os.Geteuid()})
	spec.Service.RuntimeDir = filepath.Join(home, "uncreated runtime")
	spec.BusSocket = filepath.Join(home, "uncreated bus")
	t.Setenv("PATH", "") // Missing artifacts must refuse before resolving a client.
	for _, action := range []string{"enable-login", "disable-login"} {
		result, err := runServiceLoginLink(context.Background(), action, spec)
		if runtime.GOOS == "linux" {
			if !errors.Is(err, os.ErrNotExist) || result.ChangeAttempted || result.UnitLoadAttempted || result.DescriptorPath == "" {
				t.Fatal("missing selected-link scope reached a manager or initialized", result, err)
			}
		} else {
			var usage usageError
			if !errors.As(err, &usage) || !errors.Is(usage.error, service.ErrSpec) || result.Contract != "" {
				t.Fatal("unsupported native platform accessed selected-link scope", result, err)
			}
		}
		var out, errOut bytes.Buffer
		args := []string{"--data-dir", data, "service", action, "--executable", spec.Service.Executable, "--directory", spec.Directory, "--json"}
		code := Run(context.Background(), args, &out, &errOut)
		wantCode, wantError := 1, `"not_found"`
		if runtime.GOOS != "linux" {
			wantCode, wantError = 2, `"invalid_arguments"`
		}
		if code != wantCode || !strings.Contains(out.String(), wantError) || errOut.Len() != 0 || strings.Count(out.String(), `"api_version"`) != 1 {
			t.Fatal("missing/unsupported CLI result differs", action, code, out.String(), errOut.String())
		}
		if runtime.GOOS != "linux" {
			out.Reset()
			errOut.Reset()
			if code := Run(context.Background(), args[:len(args)-1], &out, &errOut); code != 2 || out.Len() != 0 || !strings.Contains(errOut.String(), "did not attempt a link change") || strings.Contains(errOut.String(), "Service preview") {
				t.Fatal("unsupported human mode was mislabeled or implied an effect", code, out.String(), errOut.String())
			}
		}
	}
	for _, path := range []string{data, spec.Directory, spec.Service.Executable, spec.Service.RuntimeDir, spec.BusSocket} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("selected-link refusal created private scope", path, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	if code := Run(ctx, []string{"--data-dir", data, "service", "enable-login", "--executable", spec.Service.Executable, "--json"}, &out, &errOut); code != 1 || !strings.Contains(out.String(), `"canceled"`) {
		t.Fatal("canceled selected-link command succeeded", code, out.String(), errOut.String())
	}
}

func generatedLoginLinkCLIResult() service.LoginLinkResult {
	present, removed, matched := false, true, true
	observed := time.Date(2026, 10, 9, 10, 11, 12, 0, time.UTC)
	directory := "/generated $%/\"quoted\"\\Ω/systemd/user"
	descriptor := filepath.Join(directory, service.ManagedLabel+".service")
	return service.LoginLinkResult{
		Contract: service.LoginLinkContract, Action: "disable-login", Scope: "selected_default_target_dependency", Platform: "linux", ManagerProfile: "systemd_user_v255", Label: service.ManagedLabel,
		Directory: directory, DescriptorPath: descriptor, DescriptorSHA256: strings.Repeat("a", 64),
		LinkPath: filepath.Join(directory, "default.target.wants", service.ManagedLabel+".service"), LinkTarget: descriptor,
		LinkStatus: "absent", LinkPresent: &present, LinkObservedAt: &observed, RemovalObserved: &removed,
		ChangeStatus: "changed", ChangeAttempted: true, ChangeCompleted: true,
		DirectoriesCreated: []string{filepath.Join(directory, "default.target.wants")}, DirectorySyncCompleted: true,
		UnitLoadAttempted: true, LoadedBindingMatched: &matched, BindingCheckedAt: &observed,
	}
}

func TestServiceLoginLinkCLIRenderingKeepsHistoricalAndUnknownEvidence(t *testing.T) {
	r := generatedLoginLinkCLIResult()
	var out bytes.Buffer
	if err := printServiceResult(&out, r); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Selected link change: changed", "Change syscall completed: true", "Link parent sync completed: false", "Created directory parent sync completed: true", "Last checked selected link: absent", "Link present at that check: false", "Removed held link observed: Observed", "2026-10-09T10:11:12Z", "Global enablement: Unknown", "Running state: Unknown", "Future-login behavior: Unknown", "creator is unknown", "identical manually created link", "unit loading", "bookkeeping can change", "activate the broker", "Disable this link before uninstalling", "No scanning or cleanup", "historical"} {
		if !strings.Contains(out.String(), text) {
			t.Fatal("selected-link report lost scoped evidence", text, out.String())
		}
	}
	if strings.Contains(out.String(), r.LinkPath) || !strings.Contains(out.String(), `\"quoted\"\\Ω`) {
		t.Fatal("human selected-link paths were not safely quoted", out.String())
	}
	for _, writer := range []interface{ Write([]byte) (int, error) }{serviceFailWriter{}, serviceShortWriter{}} {
		if err := printServiceResult(writer, r); err == nil {
			t.Fatal("failed selected-link reply reported success")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := printServiceResult(serviceCancelWriter{cancel: cancel}, r); err != nil || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("full-count writer cancellation did not retain a late-canceled reply", err, ctx.Err())
	}
	message := serviceReplyMessage(r)
	for _, text := range []string{"status is changed", "syscall completed: true", "link parent sync completed: false", "created directory parent sync completed: true", "Last checked link state: absent", "Created directory:", "same executable, data directory and service directory", "before deciding to retry", "link origin is unverified", "bookkeeping can change"} {
		if !strings.Contains(message, text) {
			t.Fatal("failed reply lost exact partial-effect guidance", text, message)
		}
	}
	r.LinkPresent, r.LinkObservedAt, r.BindingCheckedAt, r.LoadedBindingMatched, r.RemovalObserved = nil, nil, nil, nil, nil
	out.Reset()
	if err := printServiceResult(&out, r); err != nil || !strings.Contains(out.String(), "Link present at that check: Unknown") || !strings.Contains(out.String(), "Link checked at: Not recorded") || !strings.Contains(out.String(), "Loaded settings matched at preflight: Unknown") || strings.Contains(out.String(), "0001-") {
		t.Fatal("missing selected-link evidence became a false or zero observation", err, out.String())
	}
}

func TestServiceLoginLinkCLIErrorEnvelopeKeepsPartialEffects(t *testing.T) {
	r := generatedLoginLinkCLIResult()
	for _, test := range []struct {
		err  error
		code string
	}{
		{service.ErrLoginLinkOutcome, "service_outcome_unknown"}, {service.ErrLoginLinkConflict, "service_login_link_conflict"},
		{service.ErrLoginLinkChanged, "service_login_link_changed"}, {errors.Join(service.ErrLoginLinkOutcome, context.Canceled), "canceled"},
	} {
		var out, errOut bytes.Buffer
		code := serviceMachineFailure(&out, &errOut, r, errors.Join(test.err, errors.New("untrusted manager private canary\x1b")))
		var envelope struct {
			OK      bool                    `json:"ok"`
			Service service.LoginLinkResult `json:"service"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if code != 1 || json.Unmarshal(out.Bytes(), &envelope) != nil || envelope.OK || envelope.Error.Code != test.code || envelope.Service.LinkPath != r.LinkPath || !envelope.Service.ChangeCompleted || !envelope.Service.DirectorySyncCompleted || envelope.Service.SyncCompleted || len(envelope.Service.DirectoriesCreated) != 1 || !envelope.Service.UnitLoadAttempted || envelope.Service.Enabled != nil || envelope.Service.Running != nil || envelope.Service.Stopped != nil || envelope.Service.FutureLoginMayStart != nil || envelope.Service.EffectiveEnablementVerified || strings.Contains(out.String(), "private canary") || strings.Contains(out.String(), "activation_performed") || errOut.Len() != 0 {
			t.Fatal("partial selected-link JSON changed scope/authority or leaked a helper", code, out.String(), errOut.String())
		}
	}
	var diagnostic bytes.Buffer
	if code := serviceMachineFailure(serviceShortWriter{}, &diagnostic, r, service.ErrLoginLinkOutcome); code != 1 || !strings.Contains(diagnostic.String(), "write JSON") || !strings.Contains(diagnostic.String(), "Created directory:") || !strings.Contains(diagnostic.String(), "syscall completed: true") || !strings.Contains(diagnostic.String(), "link parent sync completed: false") {
		t.Fatal("failed partial-effect JSON reply hid inspection scope", code, diagnostic.String())
	}
}

func TestServiceLoginLinkCLIHelpAndCapabilitiesAreNarrow(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := Run(context.Background(), []string{"--help"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "service enable-login/disable-login") || !strings.Contains(out.String(), "one Linux login link; runtime unknown") {
		t.Fatal("help lost finite selected-link modes", code, out.String(), errOut.String())
	}
	out.Reset()
	if code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &errOut); code != 0 {
		t.Fatal(code, out.String(), errOut.String())
	}
	var envelope struct {
		Features   map[string]bool                 `json:"features"`
		ErrorCodes []string                        `json:"error_codes"`
		Commands   []struct{ Name, Effect string } `json:"commands"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || !envelope.Features["service_linux_login_link_controls"] || envelope.Features["service_enablement_verification"] || envelope.Features["service_runtime_state_verification"] || !envelope.Features["source_thread_priority_requests"] {
		t.Fatal("capabilities widened login/runtime authority or lost prior features", err, out.String())
	}
	found := false
	for _, command := range envelope.Commands {
		if command.Name == "service enable-login/disable-login" {
			found = command.Effect == "selected_linux_default_target_dependency"
		}
	}
	if !found || !strings.Contains(out.String(), `"service_login_link_conflict"`) || !strings.Contains(out.String(), `"service_login_link_changed"`) {
		t.Fatal("capabilities lost command scope or stable refusal codes", out.String())
	}
}
