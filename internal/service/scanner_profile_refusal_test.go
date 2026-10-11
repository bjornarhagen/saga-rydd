//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func scannerRefusalSpec(t *testing.T) (InstallSpec, string) {
	t.Helper()
	base := t.TempDir()
	canary := filepath.Join(base, "unchanged canary")
	if err := os.WriteFile(canary, []byte("generated service refusal canary\n"), 0600); err != nil {
		t.Fatal(err)
	}
	spec := unifiedSpec(runtime.GOOS)
	spec.Profile = "generated_unsupported_profile"
	spec.Executable = filepath.Join(base, "absent executable")
	spec.Paths.StateDir = filepath.Join(base, "absent state")
	spec.Paths.ConfigFile = filepath.Join(spec.Paths.StateDir, "config.toml")
	spec.RuntimeDir = filepath.Join(base, "absent runtime")
	directory := filepath.Join(base, "systemd", "user")
	if runtime.GOOS == "darwin" {
		directory = filepath.Join(base, "Library", "LaunchAgents")
	}
	return InstallSpec{Service: spec, HomeDir: base, Directory: directory, BusSocket: filepath.Join(base, "absent bus")}, canary
}

func assertScannerRefusalCanary(t *testing.T, spec InstallSpec, canary string) {
	t.Helper()
	body, err := os.ReadFile(canary)
	info, statErr := os.Lstat(canary)
	entries, dirErr := os.ReadDir(spec.HomeDir)
	if err != nil || statErr != nil || dirErr != nil || string(body) != "generated service refusal canary\n" || info.Mode().Perm() != 0600 || len(entries) != 1 || entries[0].Name() != filepath.Base(canary) {
		t.Fatal("refused scanner operation changed its generated scope", err, statErr, dirErr, entries)
	}
}

func TestServiceScannerPublicClosedProfileRefuses(t *testing.T) {
	spec, canary := scannerRefusalSpec(t)
	ctx := context.Background()
	for _, malformed := range []bool{false, true} {
		selected := spec
		if malformed {
			selected.HomeDir, selected.Directory, selected.BusSocket = "\x00", "relative", "\x00"
		}
		tests := []struct {
			name string
			call func() (any, error)
			zero any
		}{
			{"install", func() (any, error) { return Install(ctx, selected) }, LifecycleResult{}},
			{"status", func() (any, error) { return Inspect(ctx, selected) }, LifecycleResult{}},
			{"start", func() (any, error) { return Start(ctx, selected) }, RuntimeResult{}},
			{"stop", func() (any, error) { return Stop(ctx, selected) }, RuntimeResult{}},
			{"uninstall", func() (any, error) { return Uninstall(ctx, selected) }, RemovalResult{}},
			{"enable-login", func() (any, error) { return EnableLoginLink(ctx, selected) }, LoginLinkResult{}},
			{"disable-login", func() (any, error) { return DisableLoginLink(ctx, selected) }, LoginLinkResult{}},
			{"runtime-status", func() (any, error) { return ObserveRuntime(ctx, selected) }, RuntimeObservation{}},
		}
		for _, test := range tests {
			t.Run(test.name+map[bool]string{false: "/valid_strings", true: "/malformed_placement"}[malformed], func(t *testing.T) {
				result, err := test.call()
				if !errors.Is(err, ErrSpec) || !reflect.DeepEqual(result, test.zero) {
					t.Fatal("scanner profile gained an operational result", result, err)
				}
				assertScannerRefusalCanary(t, spec, canary)
			})
		}
	}
}

func TestServiceScannerInternalClosedProfileRefuses(t *testing.T) {
	spec, canary := scannerRefusalSpec(t)
	calls := 0
	unexpected := errors.New("generated forbidden scanner lifecycle callback")
	touch := func() { calls++ }
	resolve := func(string) (string, error) { calls++; return "", unexpected }
	run := func(context.Context, string, []string) (runtimeReply, error) {
		calls++
		return runtimeReply{}, unexpected
	}
	syncParent := func(*os.File) error { calls++; return unexpected }
	unlink := func(int, string) error { calls++; return unexpected }
	runtimeCallbacks := runtimeHooks{resolve: resolve, run: run, beforeRequest: touch, afterRequest: touch}
	ctx := context.Background()
	tests := []struct {
		name string
		call func() (any, error)
		zero any
	}{
		{"install", func() (any, error) {
			return lifecycle(ctx, spec, true, artifactHooks{manager: func(context.Context, string) (ManagerObservation, error) {
				calls++
				return ManagerObservation{}, unexpected
			}, beforePublish: touch, afterPublish: touch, publish: func(int, string, int, string) error { calls++; return unexpected }, syncParent: syncParent})
		}, LifecycleResult{}},
		{"status", func() (any, error) {
			return lifecycle(ctx, spec, false, artifactHooks{manager: func(context.Context, string) (ManagerObservation, error) {
				calls++
				return ManagerObservation{}, unexpected
			}, beforePublish: touch, afterPublish: touch, publish: func(int, string, int, string) error { calls++; return unexpected }, syncParent: syncParent})
		}, LifecycleResult{}},
		{"start", func() (any, error) { return requestRuntime(ctx, spec, "start", runtimeCallbacks) }, RuntimeResult{}},
		{"stop", func() (any, error) { return requestRuntime(ctx, spec, "stop", runtimeCallbacks) }, RuntimeResult{}},
		{"uninstall", func() (any, error) {
			return removeDescriptor(ctx, spec, removalHooks{beforeUnlink: touch, afterUnlink: touch, unlink: unlink, syncParent: syncParent})
		}, RemovalResult{}},
		{"enable-login", func() (any, error) {
			return controlLoginLink(ctx, spec, "enable-login", loginLinkHooks{manager: runtimeCallbacks, beforeChange: touch, afterChange: touch, symlink: func(string, int, string) error { calls++; return unexpected }, unlink: unlink, syncParent: syncParent})
		}, LoginLinkResult{}},
		{"disable-login", func() (any, error) {
			return controlLoginLink(ctx, spec, "disable-login", loginLinkHooks{manager: runtimeCallbacks, beforeChange: touch, afterChange: touch, symlink: func(string, int, string) error { calls++; return unexpected }, unlink: unlink, syncParent: syncParent})
		}, LoginLinkResult{}},
		{"runtime-status", func() (any, error) {
			return observeRuntime(ctx, spec, runtimeObservationHooks{resolve: resolve, run: run})
		}, RuntimeObservation{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.call()
			if !errors.Is(err, ErrSpec) || !reflect.DeepEqual(result, test.zero) || calls != 0 {
				t.Fatal("internal scanner profile reached filesystem/provider work", result, err, calls)
			}
			assertScannerRefusalCanary(t, spec, canary)
		})
	}
}
