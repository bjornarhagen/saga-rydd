//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func scannerLifecycleActions() []string {
	return []string{"install", "status", "runtime-status", "start", "stop", "uninstall", "enable-login", "disable-login"}
}

func TestServiceScannerLifecycleCLIProfileRoutes(t *testing.T) {
	paths := config.Paths{ConfigFile: "/generated/state/config.toml", StateDir: "/generated/state"}
	for _, action := range scannerLifecycleActions() {
		for _, spelling := range []string{"--experimental-scan", "-experimental-scan", "--experimental-scan=true", "-experimental-scan=true", "--experimental-scan=false", "-experimental-scan=false"} {
			t.Run(action+"/"+spelling, func(t *testing.T) {
				calls, hosts := 0, 0
				hooks := serviceDispatchHooks{host: func() (serviceHost, error) {
					hosts++
					return serviceHost{GOOS: "linux", HomeDir: "/generated/home", XDGConfigHome: "/generated/config", XDGRuntimeDir: "/generated/broker", RuntimeDir: "/generated/runtime", UID: 42}, nil
				}, run: func(_ context.Context, gotAction string, spec service.InstallSpec) (any, error) {
					calls++
					want := service.ProfileExperimentalInventory
					if strings.HasSuffix(spelling, "=false") {
						want = service.ProfileIdle
					}
					if gotAction != action || spec.Service.Profile != want || spec.Service.Executable != "/generated/executable" || spec.Service.Paths != paths || spec.Service.RuntimeDir != "/generated/runtime" || spec.HomeDir != "/generated/home" || spec.Directory != "/generated/config/systemd/user" || spec.BusSocket != "/generated/broker/bus" {
						t.Fatal("route lost frozen scope", gotAction, spec)
					}
					return "generated captured route", nil
				}}
				result, err := dispatchServiceWithHooks(context.Background(), []string{action, "--executable", "/generated/executable", spelling}, paths, hooks)
				if err != nil || calls != 1 || hosts != 1 || result != "generated captured route" {
					t.Fatal("closed route admission failed", result, err, calls, hosts)
				}
			})
		}
	}
	for _, action := range scannerLifecycleActions() {
		t.Run(action+"/omitted_idle_literal_values", func(t *testing.T) {
			calls := 0
			hooks := serviceDispatchHooks{host: func() (serviceHost, error) {
				return serviceHost{GOOS: "linux", HomeDir: "/generated/home", RuntimeDir: "/generated/runtime"}, nil
			}, run: func(_ context.Context, gotAction string, spec service.InstallSpec) (any, error) {
				calls++
				if gotAction != action || spec.Service.Profile != service.ProfileIdle || spec.Service.Executable != "--experimental-scan=true" || spec.Directory != "--json" {
					t.Fatal("string values reinterpreted as selectors", spec)
				}
				return nil, nil
			}}
			_, err := dispatchServiceWithHooks(context.Background(), []string{action, "--executable", "--experimental-scan=true", "--directory", "--json"}, paths, hooks)
			if err != nil || calls != 1 {
				t.Fatal("literal string argument rejected before owning validation", err, calls)
			}
		})
	}
}

func TestServiceScannerLifecycleCLIStrictInput(t *testing.T) {
	cases := []struct {
		name string
		tail []string
	}{
		{"same_alias_duplicate", []string{"--experimental-scan", "--experimental-scan"}},
		{"alternate_alias_duplicate", []string{"--experimental-scan", "-experimental-scan"}},
		{"true_then_false", []string{"--experimental-scan=true", "-experimental-scan=false"}},
		{"false_then_true", []string{"--experimental-scan=false", "--experimental-scan=true"}},
		{"positional_false", []string{"--experimental-scan", "false"}},
		{"unknown_option", []string{"--generated-unknown"}},
		{"tokens_after_separator", []string{"--", "--experimental-scan"}},
		{"missing_executable", nil},
		{"duplicate_executable", []string{"--executable", "/generated/second"}},
		{"duplicate_directory", []string{"--directory", "/generated/one", "-directory=/generated/two"}},
	}
	for _, action := range scannerLifecycleActions() {
		for _, test := range cases {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				args := []string{action, "--executable", "/generated/executable"}
				if test.name == "missing_executable" {
					args = []string{action}
				}
				args = append(args, test.tail...)
				calls := 0
				hooks := serviceDispatchHooks{host: func() (serviceHost, error) { calls++; return serviceHost{}, errors.New("forbidden generated host") }, run: func(context.Context, string, service.InstallSpec) (any, error) {
					calls++
					return nil, errors.New("forbidden generated route")
				}}
				_, err := dispatchServiceWithHooks(context.Background(), args, config.Paths{}, hooks)
				var usage usageError
				if !errors.As(err, &usage) || calls != 0 {
					t.Fatal("invalid selector entered host/service", err, calls)
				}
				// An invalid data-dir would fail resolution or HOME before this pure
				// selector refusal if either output mode had the wrong ordering.
				for _, machine := range []bool{false, true} {
					cli := append([]string{"--data-dir", "\x00", "service"}, args...)
					if machine {
						cli = append([]string{"--json"}, cli...)
					}
					var out, diagnostic bytes.Buffer
					code := Run(context.Background(), cli, &out, &diagnostic)
					if code != 2 || machine && (!strings.Contains(out.String(), "\"invalid_arguments\"") || diagnostic.Len() != 0) {
						t.Fatal("invalid route reached path admission", code, out.String(), diagnostic.String())
					}
				}
			})
		}
	}
	for _, prefix := range []string{"--experimental-scan=", "-experimental-scan="} {
		for _, value := range []string{"TRUE", "True", "T", "t", "1", "FALSE", "False", "F", "f", "0", "", "yes", " true", "false "} {
			t.Run(prefix+value, func(t *testing.T) {
				if _, _, _, _, err := serviceLifecycleSelection([]string{"start", "--executable", "/generated/executable", prefix + value}); err == nil {
					t.Fatal("broader boolean accepted", prefix, value)
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	_, err := dispatchServiceWithHooks(ctx, []string{"start", "--executable", "/generated/executable", "--experimental-scan"}, config.Paths{}, serviceDispatchHooks{host: func() (serviceHost, error) { calls++; return serviceHost{}, nil }})
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("canceled valid route reached host", err, calls)
	}
}

func scannerLifecycleOutputResults(scanner bool) []any {
	var intent *service.ScannerLifecycleIntent
	if scanner {
		intent = &service.ScannerLifecycleIntent{Contract: "service_scanner_lifecycle_intent_v1", WorkerProfile: service.ProfileExperimentalInventory, ActivationFlag: "--experimental-scan", ConfigurationScope: "configured_roots_at_future_worker_start", DescriptorStartMayScan: true, LifecycleSupported: true}
	}
	return []any{
		service.LifecycleResult{Descriptor: service.Descriptor{ScannerLifecycleIntent: intent, Platform: "darwin", Label: service.ManagedLabel}, Action: "install", DescriptorPath: "/generated/descriptor", ArtifactStatus: "exact", Publication: "published", FutureLoginMayStart: true},
		service.RuntimeResult{ScannerLifecycleIntent: intent, Platform: "darwin", Action: "start", Label: service.ManagedLabel, DescriptorPath: "/generated/descriptor", RequestAttempted: true, RequestStatus: "unknown", ScanningRequested: scanner},
		service.LoginLinkResult{ScannerLifecycleIntent: intent, Platform: "linux", Action: "enable-login", DescriptorPath: "/generated/descriptor", LinkPath: "/generated/link", LinkTarget: "/generated/descriptor", ChangeStatus: "unknown"},
		service.RemovalResult{ScannerLifecycleIntent: intent, Action: "uninstall", DescriptorPath: "/generated/descriptor", RemovalStatus: "unknown"},
		service.RuntimeObservation{ScannerLifecycleIntent: intent, Platform: "linux", Contract: service.RuntimeObservationContract, DescriptorPath: "/generated/descriptor", Status: "unavailable", InvocationIDStatus: "unexamined"},
	}
}

func TestServiceScannerLifecycleCLIOutputQualification(t *testing.T) {
	families := []string{"artifact", "request", "login_link", "removal", "observation"}
	for _, mode := range []string{"human", "JSON"} {
		for _, test := range []string{"idle_exact_bytes", "scanner_profile_qualified", "short_write", "failed_write", "canceled_output"} {
			for index, family := range families {
				t.Run(mode+"/"+test+"/"+family, func(t *testing.T) {
					result := scannerLifecycleOutputResults(test != "idle_exact_bytes")[index]
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var out, diagnostic bytes.Buffer
					var writer io.Writer = &out
					if test == "short_write" {
						writer = serviceShortWriter{}
					}
					if test == "failed_write" {
						writer = serviceFailWriter{}
					}
					if test == "canceled_output" {
						writer = serviceCancelWriter{cancel: cancel}
					}
					var err error
					if mode == "human" {
						err = printServiceResult(writer, result)
					} else if emit(writer, &diagnostic, map[string]any{"api_version": APIVersion, "ok": true, "command": "service", "service": result}, 0) != 0 {
						err = io.ErrShortWrite
					}
					if test == "short_write" || test == "failed_write" {
						if err == nil {
							t.Fatal("lost output accepted")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if test == "canceled_output" {
						if !errors.Is(ctx.Err(), context.Canceled) {
							t.Fatal("output cancellation absent")
						}
						return
					}
					if test == "idle_exact_bytes" {
						if strings.Contains(out.String(), "scanner_lifecycle_intent") || strings.Contains(out.String(), "experimental_inventory_v1") {
							t.Fatal("idle output gained scanner fields", out.String())
						}
						return
					}
					if mode == "human" {
						for _, phrase := range []string{"experimental_inventory_v1", "current root permission", "No hashing or cleanup authority"} {
							if !strings.Contains(out.String(), phrase) {
								t.Fatal("scanner human qualifier missing", phrase, out.String())
							}
						}
					} else {
						var envelope struct{ Service map[string]json.RawMessage }
						if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
							t.Fatal(err)
						}
						var intent service.ScannerLifecycleIntent
						if json.Unmarshal(envelope.Service["scanner_lifecycle_intent"], &intent) != nil || !intent.LifecycleSupported || intent.NativeLifecycleVerified || intent.CurrentSourcePermissionEvaluated || intent.CleanupAuthority {
							t.Fatal("machine scanner qualification changed", out.String())
						}
						if _, found := envelope.Service["scanner_intent"]; found {
							t.Fatal("preview-only intent leaked to operation", out.String())
						}
					}
					var failed, stderr bytes.Buffer
					if serviceMachineFailure(&failed, &stderr, result, context.Canceled) != 1 || stderr.Len() != 0 || !strings.Contains(failed.String(), "\"canceled\"") {
						t.Fatal("complete failure envelope lost partial intent", failed.String(), stderr.String())
					}
				})
			}
		}
	}
	// String-value JSON preservation uses the ordinary top-level dispatcher.
	args := []string{"--data-dir", "/generated/data", "service", "preview", "--executable", "--json", "--experimental-scan"}
	var out, diagnostic bytes.Buffer
	if code := Run(context.Background(), args, &out, &diagnostic); code != 2 || out.Len() != 0 {
		t.Fatal("literal executable --json became machine mode", code, out.String(), diagnostic.String())
	}
	if !reflect.DeepEqual(scannerLifecycleActions(), []string{"install", "status", "runtime-status", "start", "stop", "uninstall", "enable-login", "disable-login"}) {
		t.Fatal("route scope changed")
	}
}
