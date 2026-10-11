//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"golang.org/x/sys/unix"
)

func scannerArtifactFixture(t *testing.T, layout string) (InstallSpec, artifactHooks) {
	t.Helper()
	platform := "linux"
	if layout == "darwin_unified" {
		platform = "darwin"
	}
	spec, hooks := serviceInstallFixture(t, platform)
	spec.Service.Profile = ProfileExperimentalInventory
	if layout == "linux_split" {
		configBase := filepath.Join(spec.HomeDir, "split config", "saga-rydd")
		stateBase := filepath.Join(spec.HomeDir, "split state", "saga-rydd")
		for _, name := range []string{configBase, stateBase} {
			if err := os.MkdirAll(name, 0700); err != nil {
				t.Fatal(err)
			}
		}
		spec.Service.Paths = config.Paths{ConfigFile: filepath.Join(configBase, "config.toml"), StateDir: stateBase}
		if err := os.WriteFile(spec.Service.Paths.ConfigFile, []byte("generated split configuration sentinel"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return spec, hooks
}

func scannerInstalledFixture(t *testing.T, platform string) InstallSpec {
	t.Helper()
	spec, hooks := scannerArtifactFixture(t, platform+"_unified")
	if _, err := lifecycle(context.Background(), spec, true, hooks); err != nil {
		t.Fatal(err)
	}
	if platform == "linux" {
		spec.BusSocket = generatedServiceSocket(t)
	}
	return spec
}

func scannerBinding(t *testing.T, f *runtimeFakeManager, spec InstallSpec) {
	t.Helper()
	d, err := Build(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for key, value := range d.Env {
		env = append(env, key+"="+value)
	}
	sort.Strings(env)
	observationProperty(f.service, "Environment", "as", env)
	observationProperty(f.service, "ExecStartEx", "a(sasasttttuii)", []any{[]any{d.Executable, d.Argv, []string{"no-env-expand"}, uint64(111), uint64(222), uint64(333), uint64(444), uint32(555), int32(1), int32(-2)}})
}

func scannerExec(f *runtimeFakeManager) []any {
	return f.service["ExecStartEx"].(map[string]any)["data"].([]any)[0].([]any)
}

func scannerCanaryProof(t *testing.T, spec InstallSpec) func() {
	t.Helper()
	paths := []string{spec.Service.Executable, spec.Service.Paths.ConfigFile}
	bodies := make([][]byte, len(paths))
	infos := make([]os.FileInfo, len(paths))
	for i, name := range paths {
		var err error
		bodies[i], err = os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		infos[i], err = os.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		for i, name := range paths {
			body, err := os.ReadFile(name)
			info, statErr := os.Lstat(name)
			if err != nil || statErr != nil || !bytes.Equal(body, bodies[i]) || !os.SameFile(info, infos[i]) || info.Mode() != infos[i].Mode() || info.Size() != infos[i].Size() || !info.ModTime().Equal(infos[i].ModTime()) {
				t.Fatal("generated configuration/executable identity or contents changed", name, err, statErr)
			}
		}
	}
}

func assertScannerLifecycleIntent(t *testing.T, intent *ScannerLifecycleIntent) {
	t.Helper()
	want := &ScannerLifecycleIntent{Contract: "service_scanner_lifecycle_intent_v1", WorkerProfile: ProfileExperimentalInventory, ActivationFlag: "--experimental-scan", ConfigurationScope: "configured_roots_at_future_worker_start", DescriptorStartMayScan: true, LifecycleSupported: true}
	if !reflect.DeepEqual(intent, want) {
		t.Fatal("scanner intent became observed authority", intent)
	}
}

func TestServiceScannerBuildIdleBoundary(t *testing.T) {
	for _, profile := range []Profile{"", ProfileIdle, ProfileExperimentalInventory} {
		t.Run(map[Profile]string{"": "omitted_idle", ProfileIdle: "explicit_idle", ProfileExperimentalInventory: "scanner_still_refused"}[profile], func(t *testing.T) {
			spec := unifiedSpec("linux")
			spec.Profile = profile
			idle, idleErr := BuildIdle(spec)
			selected, err := buildLifecycle(spec)
			if err != nil {
				t.Fatal(err)
			}
			if profile == ProfileExperimentalInventory {
				if !errors.Is(idleErr, ErrSpec) || !reflect.DeepEqual(idle, Descriptor{}) || selected.ScannerIntent != nil {
					t.Fatal("idle guard admitted scanner", idle, idleErr, selected)
				}
				assertScannerLifecycleIntent(t, selected.ScannerLifecycleIntent)
			} else if idleErr != nil || !reflect.DeepEqual(idle, selected) {
				t.Fatal("idle builder bytes/fields changed", idle, selected, idleErr)
			}
		})
	}
}

func TestServiceScannerArtifactOppositeProfiles(t *testing.T) {
	for _, layout := range []string{"darwin_unified", "linux_unified", "linux_split"} {
		for _, direction := range []string{"selected_idle_existing_scanner", "selected_scanner_existing_idle"} {
			for _, action := range []string{"install", "status"} {
				t.Run(layout+"/"+direction+"/"+action, func(t *testing.T) {
					spec, hooks := scannerArtifactFixture(t, layout)
					if direction == "selected_scanner_existing_idle" {
						spec.Service.Profile = ProfileIdle
					}
					installed, err := lifecycle(context.Background(), spec, true, hooks)
					if err != nil {
						t.Fatal(err)
					}
					before, _ := os.ReadFile(installed.DescriptorPath)
					info, _ := os.Lstat(installed.DescriptorPath)
					preserve := scannerCanaryProof(t, spec)
					if spec.Service.Profile == ProfileIdle {
						spec.Service.Profile = ProfileExperimentalInventory
					} else {
						spec.Service.Profile = ProfileIdle
					}
					calls := 0
					getter := hooks.manager
					if getter != nil {
						hooks.manager = func(ctx context.Context, socket string) (ManagerObservation, error) {
							calls++
							return getter(ctx, socket)
						}
					}
					r, err := lifecycle(context.Background(), spec, action == "install", hooks)
					after, _ := os.ReadFile(installed.DescriptorPath)
					afterInfo, _ := os.Lstat(installed.DescriptorPath)
					if !errors.Is(err, ErrArtifactConflict) || r.Publication != "not_requested" || r.InstallationPerformed || r.ActivationPerformed || r.ScanningEnabled || !bytes.Equal(before, after) || !os.SameFile(info, afterInfo) {
						t.Fatal("opposite profile adopted or mutated", r, err)
					}
					if layout != "darwin_unified" && calls != 1 {
						t.Fatal("Linux getter evidence miscounted", calls)
					}
					preserve()
				})
			}
		}
	}
}

func TestServiceScannerArtifactFiniteFaults(t *testing.T) {
	for _, layout := range []string{"darwin_unified", "linux_unified", "linux_split"} {
		for _, fault := range []string{"absent", "exact_retry", "symlink", "hardlink", "wide_permissions", "destination_race", "locked_coordinator"} {
			t.Run(layout+"/"+fault, func(t *testing.T) {
				spec, hooks := scannerArtifactFixture(t, layout)
				preserve := scannerCanaryProof(t, spec)
				d, err := Build(spec.Service)
				if err != nil {
					t.Fatal(err)
				}
				name := filepath.Join(spec.Directory, d.Filename)
				if fault == "destination_race" {
					hooks.beforePublish = func() {
						if err := os.WriteFile(name, []byte("generated foreign destination"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					r, err := lifecycle(context.Background(), spec, true, hooks)
					body, readErr := os.ReadFile(name)
					if err == nil || r.InstallationPerformed || readErr != nil || string(body) != "generated foreign destination" {
						t.Fatal("destination conflict replaced", r, err, readErr)
					}
					preserve()
					return
				}
				installed, err := lifecycle(context.Background(), spec, true, hooks)
				if err != nil {
					t.Fatal(err)
				}
				assertScannerLifecycleIntent(t, installed.ScannerLifecycleIntent)
				if installed.ScannerIntent != nil || installed.Contract != ScannerLifecycleContract || installed.ActivationPerformed || installed.ScanningEnabled {
					t.Fatal("artifact has preview or activation claim", installed)
				}
				var lock *os.File
				switch fault {
				case "symlink", "hardlink":
					other := filepath.Join(spec.HomeDir, "foreign descriptor")
					if err := os.Rename(name, other); err != nil {
						t.Fatal(err)
					}
					if fault == "symlink" {
						err = os.Symlink(other, name)
					} else {
						err = os.Link(other, name)
					}
					if err != nil {
						t.Fatal(err)
					}
				case "wide_permissions":
					if err := os.Chmod(name, 0644); err != nil {
						t.Fatal(err)
					}
				case "locked_coordinator":
					lock, err = os.OpenFile(filepath.Join(spec.Directory, "."+ManagedLabel+".lock"), os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					defer lock.Close()
					if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
						t.Fatal(err)
					}
					defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
				}
				if fault == "locked_coordinator" {
					beforeBody, readErr := os.ReadFile(name)
					beforeInfo, statErr := os.Lstat(name)
					if readErr != nil || statErr != nil {
						t.Fatal("locked descriptor baseline unavailable", readErr, statErr)
					}
					status, inspectErr := lifecycle(context.Background(), spec, false, hooks)
					if inspectErr != nil || status.ArtifactStatus != "exact" || status.Publication != "not_requested" || len(status.DirectoriesCreated) != 0 || status.LockCreated || status.InstallationPerformed || status.ActivationPerformed || status.ScanningEnabled {
						t.Fatal("locked read-only status gained mutation authority", status, inspectErr)
					}
					refused, refusalErr := lifecycle(context.Background(), spec, true, hooks)
					if !errors.Is(refusalErr, ErrArtifactBusy) || refused.Publication != "not_requested" || len(refused.DirectoriesCreated) != 0 || refused.LockCreated || refused.InstallationPerformed || refused.ActivationPerformed || refused.ScanningEnabled {
						t.Fatal("busy scanner publication was not exactly refused", refused, refusalErr)
					}
					afterBody, afterReadErr := os.ReadFile(name)
					afterInfo, afterStatErr := os.Lstat(name)
					if afterReadErr != nil || afterStatErr != nil || !bytes.Equal(beforeBody, afterBody) || !os.SameFile(beforeInfo, afterInfo) || beforeInfo.Mode() != afterInfo.Mode() || beforeInfo.Size() != afterInfo.Size() || !beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
						t.Fatal("locked publication changed generated descriptor", afterReadErr, afterStatErr)
					}
					preserve()
					return
				}
				r, err := lifecycle(context.Background(), spec, fault == "exact_retry", hooks)
				if fault == "absent" || fault == "exact_retry" {
					if err != nil || r.ArtifactStatus != "exact" || r.InstallationPerformed {
						t.Fatal("exact scanner retry changed publication", r, err)
					}
				} else if err == nil {
					t.Fatal("foreign or busy scanner artifact admitted", fault, r)
				}
				preserve()
			})
		}
	}
}

func scannerMutateManager(t *testing.T, f *runtimeFakeManager, fault string) {
	t.Helper()
	exec := scannerExec(f)
	argv := append([]string{}, exec[1].([]string)...)
	switch fault {
	case "opposite_idle_argv", "missing_scanner_flag":
		argv = argv[:len(argv)-1]
	case "extra_scanner_flag":
		argv = append(argv, "--generated-extra")
	case "reordered_scanner_flag":
		argv[len(argv)-2], argv[len(argv)-1] = argv[len(argv)-1], argv[len(argv)-2]
	case "duplicate_scanner_flag":
		argv = append(argv, "--experimental-scan")
	case "extra_exec_command":
		observationProperty(f.service, "ExecStartEx", "a(sasasttttuii)", []any{exec, exec})
	case "extra_environment":
		observationProperty(f.service, "Environment", "as", []string{"RYDD_RUNTIME_DIR=" + f.spec.Service.RuntimeDir, "EXTRA=generated"})
	case "missing_no_env_expand":
		exec[2] = []string{}
	case "foreign_fragment_path":
		observationProperty(f.unit, "FragmentPath", "s", filepath.Join(f.spec.HomeDir, "foreign service"))
	case "drop_in":
		observationProperty(f.unit, "DropInPaths", "as", []string{filepath.Join(f.spec.HomeDir, "generated drop-in")})
	case "manager_owner_replaced":
		f.ownerChanged = true
	}
	exec[1] = argv
}

func TestServiceScannerLinuxExactManagerProfile(t *testing.T) {
	routes := []string{"start", "stop", "runtime-status"}
	if runtime.GOOS == "linux" {
		routes = append(routes, "enable-login", "disable-login")
	}
	for _, action := range routes {
		for _, fault := range []string{"selected_scanner_match", "opposite_idle_argv", "missing_scanner_flag", "extra_scanner_flag", "reordered_scanner_flag", "duplicate_scanner_flag", "extra_exec_command", "extra_environment", "missing_no_env_expand", "foreign_fragment_path", "drop_in", "manager_owner_replaced", "bus_socket_replaced", "artifact_replaced"} {
			t.Run(action+"/"+fault, func(t *testing.T) {
				spec := scannerInstalledFixture(t, "linux")
				preserve := scannerCanaryProof(t, spec)
				f := newRuntimeObservationFake(t, spec)
				scannerBinding(t, f.runtimeFakeManager, spec)
				scannerMutateManager(t, f.runtimeFakeManager, fault)
				mutate := func() {
					if fault == "artifact_replaced" {
						name := filepath.Join(spec.Directory, ManagedLabel+".service")
						body, _ := os.ReadFile(name)
						if err := os.Rename(name, name+".old"); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(name, body, 0600); err != nil {
							t.Fatal(err)
						}
					}
					if fault == "bus_socket_replaced" {
						if err := os.Rename(spec.BusSocket, spec.BusSocket+".old"); err != nil {
							t.Fatal(err)
						}
						if err := os.Symlink(spec.BusSocket+".old", spec.BusSocket); err != nil {
							t.Fatal(err)
						}
					}
				}
				var err error
				if action == "runtime-status" {
					if fault == "artifact_replaced" || fault == "bus_socket_replaced" {
						f.after = func(n int) {
							if n == 4 {
								mutate()
							}
						}
					}
					r, e := observeRuntime(context.Background(), spec, observationHooks(f))
					err = e
					assertScannerLifecycleIntent(t, r.ScannerLifecycleIntent)
					assertNoRuntimeAuthority(t, r)
					if err != nil {
						assertUnknownRuntimeObservation(t, r)
					}
				} else if action == "enable-login" || action == "disable-login" {
					hooks := loginLinkHooks{manager: runtimeTestHooks(f.runtimeFakeManager.run), beforeChange: mutate}
					if fault == "artifact_replaced" || fault == "bus_socket_replaced" {
						f.onOwnerCheck = mutate
					}
					r, e := controlLoginLink(context.Background(), spec, action, hooks)
					err = e
					assertScannerLifecycleIntent(t, r.ScannerLifecycleIntent)
					if !r.UnitLoadAttempted || err != nil && r.ChangeAttempted {
						t.Fatal("manager load evidence or link mutation wrong", r, err)
					}
				} else {
					hooks := runtimeTestHooks(f.runtimeFakeManager.run)
					hooks.beforeRequest = mutate
					r, e := requestRuntime(context.Background(), spec, action, hooks)
					err = e
					assertScannerLifecycleIntent(t, r.ScannerLifecycleIntent)
					if !r.UnitLoadAttempted || err != nil && r.RequestAttempted {
						t.Fatal("manager load evidence or action wrong", r, err)
					}
					if fault == "selected_scanner_match" && (r.ScanningRequested != (action == "start") || r.RequestAccepted == nil || !*r.RequestAccepted || r.Running != nil || r.Stopped != nil) {
						t.Fatal("request became runtime proof", r)
					}
				}
				if (fault == "selected_scanner_match") != (err == nil) {
					t.Fatal("exact scanner binding result", fault, err)
				}
				for _, method := range f.methods {
					if method == "EnableUnitFiles" || method == "DisableUnitFiles" || method == "Reload" || action == "runtime-status" && method == "LoadUnit" {
						t.Fatal("forbidden manager method", method)
					}
				}
				preserve()
			})
		}
	}
}

func TestServiceScannerObservationLookupBoundary(t *testing.T) {
	for _, fault := range []string{"getunit_absent_no_fallback", "lookup_identity_changed", "typed_dynamic_or_clock_refusal_suppresses_positives"} {
		t.Run(fault, func(t *testing.T) {
			spec := scannerInstalledFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			scannerBinding(t, f.runtimeFakeManager, spec)
			switch fault {
			case "getunit_absent_no_fallback":
				f.lookupFailed = true
			case "lookup_identity_changed":
				f.lookupChanged = true
			default:
				observationProperty(f.unit, "ActiveState", "u", uint32(7))
			}
			r, err := observeRuntime(context.Background(), spec, observationHooks(f))
			if err == nil {
				t.Fatal("invalid lookup/dynamic evidence admitted", r)
			}
			assertUnknownRuntimeObservation(t, r)
			for _, method := range f.methods {
				if method == "LoadUnit" || method == "StartUnit" || method == "StopUnit" {
					t.Fatal("lookup fallback/action", method)
				}
			}
		})
	}
}

func TestServiceScannerMacRequestQualification(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		for _, fault := range []string{"opaque_zero", "failure_before_child", "lost_reply_after_started", "canceled_before_request", "canceled_after_request", "descriptor_changed", "coordinator_changed"} {
			t.Run(action+"/"+fault, func(t *testing.T) {
				spec := scannerInstalledFixture(t, "darwin")
				preserve := scannerCanaryProof(t, spec)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				hooks := runtimeHooks{resolve: func(string) (string, error) { return "/generated-launchctl", nil }, run: func(child context.Context, program string, args []string) (runtimeReply, error) {
					calls++
					want := []string{"bootstrap", fmt.Sprintf("gui/%d", os.Geteuid()), filepath.Join(spec.Directory, ManagedLabel+".plist")}
					if action == "stop" {
						want = []string{"bootout", fmt.Sprintf("gui/%d/%s", os.Geteuid(), ManagedLabel)}
					}
					deadline, bounded := child.Deadline()
					if program != "/generated-launchctl" || !reflect.DeepEqual(args, want) || !bounded || time.Until(deadline) > 2*time.Second {
						t.Fatal("unselected or unbounded generated manager", program, args)
					}
					if fault == "failure_before_child" {
						return runtimeReply{}, ErrManagerUnavailable
					}
					if fault == "lost_reply_after_started" {
						return runtimeReply{started: true}, ErrManagerUnavailable
					}
					return runtimeReply{started: true, data: []byte("opaque generated reply")}, nil
				}}
				if fault == "canceled_before_request" {
					cancel()
				}
				if fault == "canceled_after_request" {
					hooks.afterRequest = cancel
				}
				if fault == "descriptor_changed" || fault == "coordinator_changed" {
					hooks.beforeRequest = func() {
						name := filepath.Join(spec.Directory, ManagedLabel+".plist")
						if fault == "coordinator_changed" {
							name = filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
						}
						if err := os.Chmod(name, 0644); err != nil {
							t.Fatal(err)
						}
					}
				}
				r, err := requestRuntime(ctx, spec, action, hooks)
				if r.RequestAttempted != (calls == 1 && fault != "failure_before_child") || r.ScanningRequested != (r.RequestAttempted && action == "start") || r.LoadedOriginVerified || r.RuntimeStateVerified || r.LoadedBindingMatched != nil || r.Running != nil || r.Stopped != nil {
					t.Fatal("opaque request intent or unknowns wrong", r, err, calls)
				}
				if (fault == "opaque_zero") != (err == nil) {
					t.Fatal("unexpected generated request outcome", fault, r, err)
				}
				if fault == "lost_reply_after_started" && r.RequestStatus != "unknown" {
					t.Fatal("lost request reply erased attempt", r)
				}
				preserve()
			})
		}
	}
}

func TestServiceScannerMacPublicationQualification(t *testing.T) {
	t.Run("future_login_may_request_scanner_no_runtime_proof", func(t *testing.T) {
		spec, hooks := scannerArtifactFixture(t, "darwin_unified")
		r, err := lifecycle(context.Background(), spec, true, hooks)
		if err != nil || !r.FutureLoginMayStart || !r.InstallationPerformed || r.ActivationPerformed || r.ScanningEnabled || r.ScannerIntent != nil {
			t.Fatal("publication became scanning/runtime evidence", r, err)
		}
		assertScannerLifecycleIntent(t, r.ScannerLifecycleIntent)
	})
}

func TestServiceScannerLoginLinkQualification(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Run("darwin_unsupported_without_probe", func(t *testing.T) {
			spec, _ := scannerArtifactFixture(t, "darwin_unified")
			calls := 0
			_, err := controlLoginLink(context.Background(), spec, "enable-login", loginLinkHooks{beforeChange: func() { calls++ }})
			if !errors.Is(err, ErrSpec) || calls != 0 {
				t.Fatal("unsupported platform entered login link", err, calls)
			}
		})
		return
	}
	for _, action := range []string{"enable-login", "disable-login"} {
		for _, fault := range []string{"absent", "exact_existing", "manual_identical_unknown_origin", "foreign_link", "destination_race", "binding_refused", "changed_after_mutation"} {
			t.Run(action+"/"+fault, func(t *testing.T) {
				spec := scannerInstalledFixture(t, "linux")
				preserve := scannerCanaryProof(t, spec)
				f := newRuntimeFakeManager(t, spec)
				scannerBinding(t, f, spec)
				dir := filepath.Join(spec.Directory, "default.target.wants")
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				name := filepath.Join(dir, ManagedLabel+".service")
				target := filepath.Join(spec.Directory, ManagedLabel+".service")
				other := filepath.Join(dir, "generated-other.service")
				if err := os.Symlink("generated-other", other); err != nil {
					t.Fatal(err)
				}
				if fault == "exact_existing" || fault == "manual_identical_unknown_origin" || action == "disable-login" && (fault == "destination_race" || fault == "changed_after_mutation") {
					if err := os.Symlink(target, name); err != nil {
						t.Fatal(err)
					}
				}
				if fault == "foreign_link" {
					if err := os.Symlink("generated-foreign", name); err != nil {
						t.Fatal(err)
					}
				}
				if fault == "binding_refused" {
					scannerMutateManager(t, f, "missing_scanner_flag")
				}
				hooks := loginLinkHooks{manager: runtimeTestHooks(f.run)}
				if fault == "destination_race" {
					hooks.beforeChange = func() {
						if action == "disable-login" {
							if err := os.Remove(name); err != nil {
								t.Fatal(err)
							}
						}
						if err := os.Symlink("generated-race", name); err != nil {
							t.Fatal(err)
						}
					}
				}
				if fault == "changed_after_mutation" {
					hooks.afterChange = func() {
						if action == "enable-login" {
							if err := os.Remove(name); err != nil {
								t.Fatal(err)
							}
						}
						if err := os.Symlink("generated-replacement", name); err != nil {
							t.Fatal(err)
						}
					}
				}
				r, err := controlLoginLink(context.Background(), spec, action, hooks)
				positive := fault == "absent" || fault == "exact_existing" || fault == "manual_identical_unknown_origin"
				if positive != (err == nil) || r.ScanningRequested || r.StartRequested || r.StopRequested || r.ReloadRequested || r.ManagerEnablementRequested || r.LinkOriginVerified || r.RuntimeStateVerified || r.LoadedOriginVerified || r.Enabled != nil {
					t.Fatal("link result lost scope/uncertainty", r, err)
				}
				assertScannerLifecycleIntent(t, r.ScannerLifecycleIntent)
				if fault == "binding_refused" && (!r.UnitLoadAttempted || r.ChangeAttempted) {
					t.Fatal("load versus link mutation evidence wrong", r)
				}
				if fault == "changed_after_mutation" && !r.ChangeAttempted {
					t.Fatal("partial mutation lost", r)
				}
				if target, err := os.Readlink(other); err != nil || target != "generated-other" {
					t.Fatal("other manual link changed", target, err)
				}
				preserve()
			})
		}
	}
}

func TestServiceScannerObservationRemovalAndTransition(t *testing.T) {
	for _, scenario := range []string{"scanner_existing_only_status", "scanner_getunit_missing_no_load", "scanner_getunit_malformed_suppression", "mac_runtime_status_no_probe", "opposite_profile_uninstall_canaries", "selected_missing_uninstall_unknown_history", "accepted_old_stop_not_shutdown_proof", "old_remove_not_manager_unregistration", "other_links_preserved", "canceled_before_binding", "partial_unlink_or_sync_uncertainty", "coordinator_busy"} {
		t.Run(scenario, func(t *testing.T) {
			platform := "linux"
			if scenario == "mac_runtime_status_no_probe" {
				platform = "darwin"
			}
			var spec InstallSpec
			var transitionHooks artifactHooks
			if scenario == "old_remove_not_manager_unregistration" || scenario == "accepted_old_stop_not_shutdown_proof" {
				spec, transitionHooks = serviceInstallFixture(t, "linux")
				if _, err := lifecycle(context.Background(), spec, true, transitionHooks); err != nil {
					t.Fatal(err)
				}
				// Only the stop request needs a runtime socket; later publication
				// retains the original getter's captured socket for the old-remove case.
				if scenario == "accepted_old_stop_not_shutdown_proof" {
					spec.BusSocket = generatedServiceSocket(t)
				}
			} else {
				spec = scannerInstalledFixture(t, platform)
			}
			preserve := scannerCanaryProof(t, spec)
			if scenario == "mac_runtime_status_no_probe" {
				calls := 0
				r, err := observeRuntime(context.Background(), spec, runtimeObservationHooks{resolve: func(string) (string, error) { calls++; return "", ErrManagerUnavailable }})
				if err != nil || r.Status != "unsupported" || calls != 0 {
					t.Fatal("mac observation probed", r, err, calls)
				}
				assertUnknownRuntimeObservation(t, r)
				preserve()
				return
			}
			f := newRuntimeObservationFake(t, spec)
			scannerBinding(t, f.runtimeFakeManager, spec)
			switch scenario {
			case "scanner_existing_only_status", "scanner_getunit_missing_no_load", "scanner_getunit_malformed_suppression":
				f.lookupFailed = scenario == "scanner_getunit_missing_no_load"
				if scenario == "scanner_getunit_malformed_suppression" {
					f.lookupReply = []byte("generated malformed lookup")
				}
				r, err := observeRuntime(context.Background(), spec, observationHooks(f))
				if (scenario == "scanner_existing_only_status") != (err == nil) {
					t.Fatal("lookup outcome wrong", r, err)
				}
				if err != nil {
					assertUnknownRuntimeObservation(t, r)
				}
				assertNoRuntimeAuthority(t, r)
				for _, method := range f.methods {
					if method == "LoadUnit" {
						t.Fatal("observation loaded missing unit")
					}
				}
			case "accepted_old_stop_not_shutdown_proof":
				r, err := requestRuntime(context.Background(), spec, "stop", runtimeTestHooks(f.runtimeFakeManager.run))
				if err != nil || r.RequestAccepted == nil || !*r.RequestAccepted || r.Stopped != nil || r.ScanningRequested {
					t.Fatal("accepted stop became shutdown", r, err)
				}
			case "canceled_before_binding":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := requestRuntime(ctx, spec, "start", runtimeTestHooks(f.runtimeFakeManager.run))
				if !errors.Is(err, context.Canceled) || len(f.methods) != 0 {
					t.Fatal("canceled request entered manager", err, f.methods)
				}
			default:
				name := filepath.Join(spec.Directory, ManagedLabel+".service")
				body, _ := os.ReadFile(name)
				hooks := removalHooks{}
				if scenario == "opposite_profile_uninstall_canaries" {
					spec.Service.Profile = ProfileIdle
				}
				if scenario == "selected_missing_uninstall_unknown_history" {
					if err := os.Remove(name); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "partial_unlink_or_sync_uncertainty" {
					hooks.syncParent = func(*os.File) error { return errors.New("generated sync refusal") }
				}
				other := filepath.Join(spec.Directory, "generated-other.service")
				if err := os.WriteFile(other, []byte("other generated artifact"), 0600); err != nil {
					t.Fatal(err)
				}
				var lock *os.File
				if scenario == "coordinator_busy" {
					var err error
					lock, err = os.OpenFile(filepath.Join(spec.Directory, "."+ManagedLabel+".lock"), os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					defer lock.Close()
					if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
						t.Fatal(err)
					}
					defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
				}
				r, err := removeDescriptor(context.Background(), spec, hooks)
				if r.StopRequested || r.ManagerRequestAttempted || r.ScanningRequested || r.Running != nil || r.Stopped != nil || r.DataRemoved || r.ExecutableRemoved || r.LockRemoved {
					t.Fatal("removal gained action authority", r)
				}
				if scenario == "opposite_profile_uninstall_canaries" {
					after, _ := os.ReadFile(name)
					if !errors.Is(err, ErrArtifactConflict) || !bytes.Equal(after, body) {
						t.Fatal("opposite removal changed artifact", r, err)
					}
				} else if scenario == "coordinator_busy" {
					if !errors.Is(err, ErrArtifactBusy) {
						t.Fatal("busy removal entered", r, err)
					}
				} else if scenario == "partial_unlink_or_sync_uncertainty" {
					if err == nil || !r.UnlinkCompleted || r.SyncCompleted {
						t.Fatal("partial unlink lost", r, err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if scenario == "selected_missing_uninstall_unknown_history" && (r.RemovalAttempted || r.DescriptorAbsent == nil || !*r.DescriptorAbsent || (r.RemovalObserved == nil || *r.RemovalObserved)) {
					t.Fatal("missing retry invented removal history", r)
				}
				if got, err := os.ReadFile(other); err != nil || string(got) != "other generated artifact" {
					t.Fatal("other artifact changed", err)
				}
				if scenario == "old_remove_not_manager_unregistration" {
					if r.ManagerRequestAttempted || r.StopRequested || r.LoadedOriginVerified {
						t.Fatal("removal invented unregistration", r)
					}
					spec.Service.Profile = ProfileExperimentalInventory
					fresh, err := lifecycle(context.Background(), spec, true, transitionHooks)
					if err != nil || fresh.Publication != "saved" || fresh.Contract != ScannerLifecycleContract || fresh.ActivationPerformed || fresh.ScanningEnabled {
						t.Fatal("explicit absent-destination transition failed", fresh, err)
					}
				}

			}
			preserve()
		})
	}
}
