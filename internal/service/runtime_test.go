//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func runtimeFixture(t *testing.T, platform string) InstallSpec {
	t.Helper()
	spec, hooks := serviceInstallFixture(t, platform)
	if _, err := lifecycle(context.Background(), spec, true, hooks); err != nil {
		t.Fatal(err)
	}
	if platform == "linux" {
		spec.BusSocket = generatedServiceSocket(t)
	}
	return spec
}

func runtimeWire(t *testing.T, signature string, value any) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"type": signature, "data": []any{value}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type runtimeFakeManager struct {
	t            *testing.T
	spec         InstallSpec
	owner        string
	unit         map[string]any
	service      map[string]any
	methods      []string
	ownerChanged bool
	onOwnerCheck func()
	action       func() (runtimeReply, error)
}

func newRuntimeFakeManager(t *testing.T, spec InstallSpec) *runtimeFakeManager {
	t.Helper()
	property := func(signature string, value any) any {
		return map[string]any{"type": signature, "data": value}
	}
	filename := ManagedLabel + ".service"
	f := &runtimeFakeManager{t: t, spec: spec, owner: ":1.15", unit: map[string]any{}, service: map[string]any{}}
	for name, value := range map[string]string{"Id": filename, "FragmentPath": filepath.Join(spec.Directory, filename), "LoadState": "loaded", "SourcePath": "", "Following": ""} {
		f.unit[name] = property("s", value)
	}
	f.unit["DropInPaths"] = property("as", []string{})
	f.unit["NeedDaemonReload"] = property("b", false)
	f.unit["Transient"] = property("b", false)
	for name, value := range map[string]string{"Type": "exec", "Restart": "on-failure", "RootDirectory": "", "RootImage": ""} {
		f.service[name] = property("s", value)
	}
	f.service["Environment"] = property("as", []string{"RYDD_RUNTIME_DIR=" + spec.Service.RuntimeDir})
	f.service["EnvironmentFiles"] = property("a(sb)", []any{})
	f.service["PassEnvironment"] = property("as", []string{})
	f.service["UnsetEnvironment"] = property("as", []string{})
	f.service["RestartUSec"] = property("t", uint64(30_000_000))
	f.service["TimeoutStopUSec"] = property("t", uint64(10_000_000))
	for _, name := range []string{"ExecConditionEx", "ExecStartPreEx", "ExecStartPostEx", "ExecStopEx", "ExecStopPostEx", "ExecReloadEx"} {
		f.service[name] = property("a(sasasttttuii)", []any{})
	}
	argv := []string{spec.Service.Executable, "--data-dir", spec.Service.Paths.StateDir, "daemon"}
	f.service["ExecStartEx"] = property("a(sasasttttuii)", []any{[]any{spec.Service.Executable, argv, []string{"no-env-expand"}, uint64(111), uint64(222), uint64(333), uint64(444), uint32(555), int32(1), int32(-2)}})
	// The bounded GetAll body can contain other private manager metadata. It
	// must neither authorize work nor appear in the projected public result.
	f.service["PrivateUnknownMetadata"] = property("s", "discard-private-manager-canary")
	return f
}

func (f *runtimeFakeManager) run(ctx context.Context, program string, args []string) (runtimeReply, error) {
	f.t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Second || program != "/generated-manager" {
		f.t.Fatal("unbounded/unselected manager client", program)
	}
	wantPrefix := []string{"--address=unix:path=" + dbusAddressPath(f.spec.BusSocket), "--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call"}
	if len(args) < 12 || !reflect.DeepEqual(args[:7], wantPrefix) {
		f.t.Fatal("manager request gained default routing or activation", args)
	}
	method := args[10]
	f.methods = append(f.methods, method)
	if method != "GetNameOwner" && args[7] != f.owner {
		f.t.Fatal("well-known manager name was reused after owner binding", args)
	}
	var data []byte
	switch method {
	case "GetNameOwner":
		if !reflect.DeepEqual(args[7:], []string{"org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus", "GetNameOwner", "s", "org.freedesktop.systemd1"}) {
			f.t.Fatal("owner lookup scope changed", args)
		}
		owner := f.owner
		if f.onOwnerCheck != nil && len(f.methods) > 1 {
			f.onOwnerCheck()
		}
		if f.ownerChanged && len(f.methods) > 1 {
			owner = ":1.16"
		}
		data = runtimeWire(f.t, "s", owner)
	case "Get":
		if !reflect.DeepEqual(args[8:], []string{"/org/freedesktop/systemd1", "org.freedesktop.DBus.Properties", "Get", "ss", "org.freedesktop.systemd1.Manager", "UnitPath"}) {
			f.t.Fatal("placement lookup scope changed", args)
		}
		data = runtimeWire(f.t, "v", map[string]any{"type": "as", "data": []string{f.spec.Directory, "/usr/lib/systemd/user"}})
	case "LoadUnit":
		if !reflect.DeepEqual(args[8:], []string{"/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "LoadUnit", "s", ManagedLabel + ".service"}) {
			f.t.Fatal("load scope changed", args)
		}
		data = runtimeWire(f.t, "o", "/org/freedesktop/systemd1/unit/managed")
	case "GetAll":
		if args[8] != "/org/freedesktop/systemd1/unit/managed" || args[9] != "org.freedesktop.DBus.Properties" || len(args) != 13 || args[11] != "s" {
			f.t.Fatal("property query escaped the bound unit", args)
		}
		properties := f.unit
		if args[12] == "org.freedesktop.systemd1.Service" {
			properties = f.service
		} else if args[12] != "org.freedesktop.systemd1.Unit" {
			f.t.Fatal("extra manager interface requested", args)
		}
		data = runtimeWire(f.t, "a{sv}", properties)
	case "StartUnit", "StopUnit":
		if !reflect.DeepEqual(args[8:], []string{"/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", method, "ss", ManagedLabel + ".service", "fail"}) {
			f.t.Fatal("runtime action replaced jobs or widened scope", args)
		}
		if f.action != nil {
			return f.action()
		}
		data = runtimeWire(f.t, "o", "/org/freedesktop/systemd1/job/123456789")
	default:
		f.t.Fatal("unexpected manager call", args)
	}
	return runtimeReply{started: true, data: data}, nil
}

func runtimeTestHooks(run runtimeRunner) runtimeHooks {
	return runtimeHooks{resolve: func(string) (string, error) { return "/generated-manager", nil }, run: run}
}

func TestServiceRuntimeExactLinuxRequestsPreserveUserData(t *testing.T) {
	spec := runtimeFixture(t, "linux")
	filename := filepath.Join(spec.Directory, ManagedLabel+".service")
	paths := []string{filename, filepath.Join(spec.Directory, "."+ManagedLabel+".lock"), spec.Service.Executable, spec.Service.Paths.ConfigFile}
	before := make([][]byte, len(paths))
	infos := make([]os.FileInfo, len(paths))
	for i, path := range paths {
		var err error
		before[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		infos[i], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, action := range []string{"start", "stop"} {
		f := newRuntimeFakeManager(t, spec)
		r, err := requestRuntime(context.Background(), spec, action, runtimeTestHooks(f.run))
		if err != nil {
			t.Fatal(err)
		}
		method := "StartUnit"
		if action == "stop" {
			method = "StopUnit"
		}
		if !reflect.DeepEqual(f.methods, []string{"GetNameOwner", "Get", "LoadUnit", "GetAll", "GetAll", "GetNameOwner", method}) {
			t.Fatal("request was repeated or binding omitted", f.methods)
		}
		if r.Contract != RuntimeContract || r.RequestStatus != "accepted" || !r.RequestAttempted || r.RequestAccepted == nil || !*r.RequestAccepted || r.JobPath == nil || *r.JobPath != "/org/freedesktop/systemd1/job/123456789" || r.AcceptanceEvidence != "typed_queued_job" || r.LoadedBindingMatched == nil || !*r.LoadedBindingMatched || r.BindingCheckedAt == nil || r.ReplyObservedAt == nil || !r.UnitLoadAttempted || r.Running != nil || r.Stopped != nil || r.RuntimeStateVerified || r.LoadedOriginVerified || r.ScanningRequested || r.EnablementChanged || r.ReloadRequested || r.DescriptorChanged {
			t.Fatal("queued request became runtime/authority claim", r)
		}
		encoded, _ := json.Marshal(r)
		if strings.Contains(string(encoded), "discard-private-manager-canary") || strings.Contains(string(encoded), "activation_performed") {
			t.Fatal("unrelated manager metadata or stale activation assertion exposed", string(encoded))
		}
	}
	for i, path := range paths {
		after, err := os.ReadFile(path)
		info, statErr := os.Lstat(path)
		if err != nil || statErr != nil || !reflect.DeepEqual(before[i], after) || !os.SameFile(infos[i], info) || !infos[i].ModTime().Equal(info.ModTime()) {
			t.Fatal("runtime request changed user data/artifact", path, err, statErr)
		}
	}
	if _, err := os.Stat(spec.Service.RuntimeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fake runtime request initialized worker runtime", err)
	}
}

func TestServiceRuntimeLinuxForeignLoadedSettingsRefuse(t *testing.T) {
	for _, kind := range []string{"foreign fragment", "foreign executable", "experimental scan", "dollar expansion", "extra command", "extra environment", "environment file", "root override", "drop-in", "stale", "transient", "missing property", "unknown value", "owner changed"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeFakeManager(t, spec)
			set := func(properties map[string]any, name, signature string, value any) {
				properties[name] = map[string]any{"type": signature, "data": value}
			}
			want := ErrRuntimeBinding
			switch kind {
			case "foreign fragment":
				set(f.unit, "FragmentPath", "s", "/foreign/managed.service")
			case "foreign executable", "experimental scan", "dollar expansion":
				path := spec.Service.Executable
				argv := []string{path, "--data-dir", spec.Service.Paths.StateDir, "daemon"}
				flags := []string{"no-env-expand"}
				if kind == "foreign executable" {
					path = "/foreign/executable"
				} else if kind == "experimental scan" {
					argv = append(argv, "--experimental-scan")
				} else {
					flags = []string{}
				}
				set(f.service, "ExecStartEx", "a(sasasttttuii)", []any{[]any{path, argv, flags, 0, 0, 0, 0, 0, 0, 0}})
			case "extra command":
				set(f.service, "ExecStopEx", "a(sasasttttuii)", []any{[]any{}})
			case "extra environment":
				set(f.service, "Environment", "as", []string{"RYDD_RUNTIME_DIR=/foreign/runtime"})
			case "environment file":
				set(f.service, "EnvironmentFiles", "a(sb)", []any{[]any{"/foreign/env", false}})
			case "root override":
				set(f.service, "RootDirectory", "s", "/foreign/root")
			case "drop-in":
				set(f.unit, "DropInPaths", "as", []string{"/foreign/override.conf"})
			case "stale":
				set(f.unit, "NeedDaemonReload", "b", true)
			case "transient":
				set(f.unit, "Transient", "b", true)
			case "missing property":
				delete(f.service, "ExecStartEx")
				want = ErrManagerProtocol
			case "unknown value":
				set(f.unit, "NeedDaemonReload", "b", nil)
				want = ErrManagerProtocol
			case "owner changed":
				f.ownerChanged = true
			}
			r, err := requestRuntime(context.Background(), spec, "start", runtimeTestHooks(f.run))
			if !errors.Is(err, want) || r.RequestAttempted || r.RequestAccepted != nil || r.RequestStatus != "refused" || r.JobPath != nil || r.Running != nil || r.Stopped != nil {
				t.Fatal("foreign/unknown binding reached action", r, err)
			}
			for _, method := range f.methods {
				if method == "StartUnit" || method == "StopUnit" {
					t.Fatal("foreign unit received runtime action")
				}
			}
		})
	}
}

func TestServiceRuntimeMacOpaqueRepliesAndExactFixedTarget(t *testing.T) {
	spec := runtimeFixture(t, "darwin")
	for _, action := range []string{"start", "stop"} {
		calls := 0
		hooks := runtimeTestHooks(func(ctx context.Context, program string, args []string) (runtimeReply, error) {
			calls++
			want := []string{"bootstrap", fmt.Sprintf("gui/%d", os.Geteuid()), filepath.Join(spec.Directory, ManagedLabel+".plist")}
			if action == "stop" {
				want = []string{"bootout", fmt.Sprintf("gui/%d/%s", os.Geteuid(), ManagedLabel)}
			}
			if program != "/generated-manager" || !reflect.DeepEqual(args, want) {
				t.Fatal("launchctl widened domain/label or added enable/restart", args)
			}
			return runtimeReply{started: true, data: []byte("opaque private launchctl output, not a protocol")}, nil
		})
		r, err := requestRuntime(context.Background(), spec, action, hooks)
		if err != nil || calls != 1 || r.RequestStatus != "accepted" || r.AcceptanceEvidence != "opaque_launchctl_exit_zero" || r.JobPath != nil || r.LoadedBindingMatched != nil || r.LoadedOriginVerified || r.RuntimeStateVerified || r.Running != nil || r.Stopped != nil || r.UnitLoadAttempted {
			t.Fatal("opaque client success became loaded/runtime evidence", r, err)
		}
	}
}

func TestServiceRuntimeMissingForeignBusyArtifactsNeverCallManager(t *testing.T) {
	for _, kind := range []string{"missing directory", "missing descriptor", "missing lock", "foreign descriptor", "symlink descriptor", "hardlink descriptor", "symlink lock", "busy lock"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "darwin")
			filename := filepath.Join(spec.Directory, ManagedLabel+".plist")
			lockname := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
			var held *os.File
			switch kind {
			case "missing directory":
				if err := os.Rename(spec.Directory, spec.Directory+"-offline"); err != nil {
					t.Fatal(err)
				}
			case "missing descriptor":
				if err := os.Remove(filename); err != nil {
					t.Fatal(err)
				}
			case "missing lock":
				if err := os.Remove(lockname); err != nil {
					t.Fatal(err)
				}
			case "foreign descriptor":
				if err := os.WriteFile(filename, []byte("foreign sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink descriptor", "hardlink descriptor":
				if err := os.Remove(filename); err != nil {
					t.Fatal(err)
				}
				var err error
				if kind == "symlink descriptor" {
					err = os.Symlink(spec.Service.Executable, filename)
				} else {
					err = os.Link(spec.Service.Executable, filename)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "symlink lock":
				if err := os.Remove(lockname); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(spec.Service.Executable, lockname); err != nil {
					t.Fatal(err)
				}
			case "busy lock":
				var err error
				held, err = os.OpenFile(lockname, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				if err = unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			hooks := runtimeTestHooks(func(context.Context, string, []string) (runtimeReply, error) {
				calls++
				return runtimeReply{}, nil
			})
			r, err := requestRuntime(context.Background(), spec, "start", hooks)
			if err == nil || calls != 0 || r.RequestAttempted || r.RequestAccepted != nil {
				t.Fatal("unsafe artifact reached manager or was initialized", r, calls, err)
			}
			if kind == "busy lock" && !errors.Is(err, ErrArtifactBusy) {
				t.Fatal("busy identity lost", err)
			}
			if kind == "missing lock" {
				if _, err = os.Lstat(lockname); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("runtime command created lock", err)
				}
			}
		})
	}
}

func TestServiceRuntimeCancellationReplacementsAndUnknownRequests(t *testing.T) {
	for _, kind := range []string{"cancel before", "replace before", "replace during owner check", "cancel during", "malformed reply", "cancel after", "replace after"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeFakeManager(t, spec)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := runtimeTestHooks(f.run)
			replace := func() {
				path := filepath.Join(spec.Directory, ManagedLabel+".service")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "cancel before":
				hooks.beforeRequest = cancel
			case "replace before":
				hooks.beforeRequest = replace
			case "replace during owner check":
				f.onOwnerCheck = replace
			case "cancel during":
				f.action = func() (runtimeReply, error) { cancel(); return runtimeReply{started: true}, context.Canceled }
			case "malformed reply":
				f.action = func() (runtimeReply, error) {
					return runtimeReply{started: true, data: []byte(`{"type":"o","data":["/foreign/job"]}`)}, nil
				}
			case "cancel after":
				hooks.afterRequest = cancel
			case "replace after":
				hooks.afterRequest = replace
			}
			r, err := requestRuntime(ctx, spec, "start", hooks)
			if err == nil || r.Running != nil || r.Stopped != nil || r.RuntimeStateVerified {
				t.Fatal("interruption became successful runtime state", r, err)
			}
			before := strings.HasSuffix(kind, "before") || kind == "replace during owner check"
			if before {
				if r.RequestAttempted || r.RequestAccepted != nil || errors.Is(err, ErrRuntimeOutcome) {
					t.Fatal("pre-dispatch refusal claimed a sent request", r, err)
				}
			} else {
				if !r.RequestAttempted || !errors.Is(err, ErrRuntimeOutcome) {
					t.Fatal("attempt/unknown outcome lost", r, err)
				}
				if strings.HasSuffix(kind, "after") {
					if r.RequestAccepted == nil || !*r.RequestAccepted || r.RequestStatus != "accepted" || r.JobPath == nil {
						t.Fatal("known acknowledgement discarded", r, err)
					}
				} else if r.RequestStatus != "unknown" || r.RequestAccepted != nil || r.JobPath != nil {
					t.Fatal("uncertain request got fabricated acceptance", r, err)
				}
			}
			if strings.HasPrefix(kind, "cancel") && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation identity lost", err)
			}
		})
	}
}
