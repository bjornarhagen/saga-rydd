//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type runtimeObservationFake struct {
	*runtimeFakeManager
	lookupFailed  bool
	lookupChanged bool
	lookupReply   []byte
	after         func(int)
	refuseAt      int
}

func newRuntimeObservationFake(t *testing.T, spec InstallSpec) *runtimeObservationFake {
	f := &runtimeObservationFake{runtimeFakeManager: newRuntimeFakeManager(t, spec)}
	observationProperty(f.unit, "ActiveState", "s", "active")
	observationProperty(f.unit, "SubState", "s", "running")
	observationProperty(f.unit, "InvocationID", "ay", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	observationProperty(f.service, "MainPID", "u", uint32(1234))
	observationProperty(f.service, "Result", "s", "success")
	return f
}

func observationProperty(properties map[string]any, name, signature string, data any) {
	properties[name] = map[string]any{"type": signature, "data": data}
}

func (f *runtimeObservationFake) run(ctx context.Context, program string, args []string) (runtimeReply, error) {
	f.t.Helper()
	var reply runtimeReply
	var err error
	if len(args) >= 12 && args[10] == "GetUnit" {
		deadline, ok := ctx.Deadline()
		want := []string{"--address=unix:path=" + dbusAddressPath(f.spec.BusSocket), "--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call", f.owner, "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "GetUnit", "s", ManagedLabel + ".service"}
		if !ok || time.Until(deadline) > 2*time.Second || program != "/generated-manager" || !reflect.DeepEqual(args, want) {
			f.t.Fatal("existing-unit lookup lost exact route/bounds", args)
		}
		f.methods = append(f.methods, "GetUnit")
		reply.started = true
		if f.lookupFailed {
			err = ErrManagerUnavailable
		} else if f.lookupReply != nil {
			reply.data = f.lookupReply
		} else {
			object := "/org/freedesktop/systemd1/unit/managed"
			if f.lookupChanged && len(f.methods) > 3 {
				object += "_replaced"
			}
			reply.data = runtimeWire(f.t, "o", object)
		}
	} else {
		if len(args) < 12 || (args[10] != "GetNameOwner" && args[10] != "Get" && args[10] != "GetAll") {
			f.t.Fatal("observation gained loading/lifecycle/list call", args)
		}
		reply, err = f.runtimeFakeManager.run(ctx, program, args)
	}
	if f.after != nil {
		f.after(len(f.methods))
	}
	if f.refuseAt == len(f.methods) {
		err = ErrManagerUnavailable
	}
	return reply, err
}

func observationHooks(f *runtimeObservationFake) runtimeObservationHooks {
	return runtimeObservationHooks{resolve: func(name string) (string, error) {
		if name != "busctl" {
			f.t.Fatal("unexpected executable lookup", name)
		}
		return "/generated-manager", nil
	}, run: f.run}
}

func assertUnknownRuntimeObservation(t *testing.T, r RuntimeObservation) {
	t.Helper()
	if r.ActiveState != nil || r.SubState != nil || r.ServiceResult != nil || r.MainPID != nil || r.InvocationID != nil || r.BindingCheckedAt != nil || r.UnitObservedAt != nil || r.ServiceObservedAt != nil || r.FinishedAt != nil || r.RuntimeStateObserved || r.SequentialObservation || r.LoadedBindingMatched != nil && *r.LoadedBindingMatched {
		t.Fatal("failed/unsupported observation published dynamic positives", r)
	}
	assertNoRuntimeAuthority(t, r)
}

func assertNoRuntimeAuthority(t *testing.T, r RuntimeObservation) {
	t.Helper()
	if r.Running != nil || r.Stopped != nil || r.ApplicationReady != nil || r.RuntimeStateVerified || r.LoadedOriginVerified || r.ProcessIdentityVerified || r.ApplicationReadinessVerified || r.UnitLoadRequested || r.StartStopRequested || r.ScanningRequested || r.EnablementChanged || r.ReloadRequested || r.DescriptorChanged || r.SourceContentsOpened {
		t.Fatal("manager labels became readiness, identity or action authority", r)
	}
}

func TestServiceRuntimeObservationExactSevenCallsPreservesUserData(t *testing.T) {
	spec := runtimeFixture(t, "linux")
	paths := []string{filepath.Join(spec.Directory, ManagedLabel+".service"), filepath.Join(spec.Directory, "."+ManagedLabel+".lock"), spec.Service.Executable, spec.Service.Paths.ConfigFile}
	before := make([][]byte, len(paths))
	stamps := make([]os.FileInfo, len(paths))
	for i, path := range paths {
		var err error
		before[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		stamps[i], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	f := newRuntimeObservationFake(t, spec)
	r, err := observeRuntime(context.Background(), spec, observationHooks(f))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"GetNameOwner", "Get", "GetUnit", "GetAll", "GetAll", "GetUnit", "GetNameOwner"}
	if !reflect.DeepEqual(f.methods, want) || r.ManagerCalls != 7 || r.ManagerReplyBytes <= 0 || r.ManagerReplyBytes > 448<<10 {
		t.Fatal("call receipt/whitelist changed", f.methods, r)
	}
	if r.Contract != RuntimeObservationContract || !r.Supported || r.Status != "observed" || r.ArtifactStatus != "exact" || !r.LookupAttempted || !r.LookupRechecked || r.LoadedBindingMatched == nil || !*r.LoadedBindingMatched || r.ActiveState == nil || *r.ActiveState != "active" || r.SubState == nil || *r.SubState != "running" || r.MainPID == nil || *r.MainPID != 1234 || r.ServiceResult == nil || *r.ServiceResult != "success" || r.InvocationID == nil || *r.InvocationID != "000102030405060708090a0b0c0d0e0f" || !r.RuntimeStateObserved || !r.SequentialObservation || r.UnitObservedAt == nil || r.ServiceObservedAt == nil || r.BindingCheckedAt == nil || r.FinishedAt == nil {
		t.Fatal("typed manager evidence lost or misqualified", r)
	}
	for _, stamp := range []*time.Time{r.UnitObservedAt, r.ServiceObservedAt, r.BindingCheckedAt, r.FinishedAt} {
		if stamp.Location() != time.UTC {
			t.Fatal("wall observation not UTC", stamp)
		}
	}
	assertNoRuntimeAuthority(t, r)
	encoded, _ := json.Marshal(r)
	if strings.Contains(string(encoded), "discard-private-manager-canary") || strings.Contains(string(encoded), "PrivateUnknownMetadata") || strings.Contains(string(encoded), "ExecMainPID") {
		t.Fatal("raw/historical manager fields escaped projection")
	}
	for i, path := range paths {
		after, err := os.ReadFile(path)
		info, statErr := os.Lstat(path)
		if err != nil || statErr != nil || !reflect.DeepEqual(before[i], after) || !os.SameFile(stamps[i], info) || !stamps[i].ModTime().Equal(info.ModTime()) {
			t.Fatal("observation changed user data/artifact", path, err, statErr)
		}
	}
	if _, err := os.Stat(spec.Service.RuntimeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("observation initialized worker runtime", err)
	}
}

func TestServiceRuntimeObservationInactiveAndFutureLabelsRemainDeclarations(t *testing.T) {
	for _, active := range []string{"inactive", "failed", "future-state"} {
		t.Run(active, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			observationProperty(f.unit, "ActiveState", "s", active)
			observationProperty(f.unit, "SubState", "s", "future-detail-2")
			observationProperty(f.unit, "InvocationID", "ay", make([]int, 16))
			observationProperty(f.service, "MainPID", "u", uint32(0))
			r, err := observeRuntime(context.Background(), spec, observationHooks(f))
			if err != nil || r.ActiveState == nil || *r.ActiveState != active || r.MainPID == nil || *r.MainPID != 0 || r.InvocationID != nil || r.InvocationIDStatus != "unset" {
				t.Fatal("bounded declaration/unset identity lost", r, err)
			}
			assertNoRuntimeAuthority(t, r)
		})
	}
}

func TestServiceRuntimeObservationNoLoadedLookupFallback(t *testing.T) {
	spec := runtimeFixture(t, "linux")
	f := newRuntimeObservationFake(t, spec)
	f.lookupFailed = true
	r, err := observeRuntime(context.Background(), spec, observationHooks(f))
	if !errors.Is(err, ErrManagerUnavailable) || r.Status != "unavailable" || !r.LookupAttempted || r.LookupRechecked || r.ManagerCalls != 3 || !reflect.DeepEqual(f.methods, []string{"GetNameOwner", "Get", "GetUnit"}) {
		t.Fatal("failed lookup was retried, loaded or treated as absence", r, err, f.methods)
	}
	assertUnknownRuntimeObservation(t, r)
}

func TestServiceRuntimeObservationUnsupportedMacNeverProbesOrInitializes(t *testing.T) {
	spec, _ := serviceInstallFixture(t, "darwin")
	for _, p := range []string{spec.Directory, spec.Service.RuntimeDir} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("fixture unexpectedly initialized", p, err)
		}
	}
	h := runtimeObservationHooks{resolve: func(string) (string, error) { t.Fatal("unsupported Mac resolved helper"); return "", nil }, run: func(context.Context, string, []string) (runtimeReply, error) {
		t.Fatal("unsupported Mac invoked helper")
		return runtimeReply{}, nil
	}}
	r, err := observeRuntime(context.Background(), spec, h)
	if err != nil || r.Supported || r.Status != "unsupported" || r.ArtifactStatus != "unexamined" || r.ManagerCalls != 0 || r.LookupAttempted {
		t.Fatal("unsupported Mac asserted runtime/artifact facts", r, err)
	}
	assertUnknownRuntimeObservation(t, r)
	for _, p := range []string{spec.Directory, spec.Service.RuntimeDir} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("unsupported Mac initialized path", p, err)
		}
	}
}

func TestServiceRuntimeObservationPreflightRefusesBeforeHelper(t *testing.T) {
	for _, kind := range []string{"missing parent", "missing descriptor", "missing lock", "foreign descriptor", "symlink descriptor", "hardlink descriptor", "symlink lock", "busy lock"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			path := filepath.Join(spec.Directory, ManagedLabel+".service")
			lock := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
			switch kind {
			case "missing parent":
				if err := os.Rename(spec.Directory, spec.Directory+"-offline"); err != nil {
					t.Fatal(err)
				}
			case "missing descriptor":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "missing lock":
				if err := os.Remove(lock); err != nil {
					t.Fatal(err)
				}
			case "foreign descriptor":
				if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink descriptor", "hardlink descriptor":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				var err error
				if kind == "symlink descriptor" {
					err = os.Symlink(spec.Service.Executable, path)
				} else {
					err = os.Link(spec.Service.Executable, path)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "symlink lock":
				if err := os.Remove(lock); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(spec.Service.Executable, lock); err != nil {
					t.Fatal(err)
				}
			case "busy lock":
				f, err := os.OpenFile(lock, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
			}
			h := runtimeObservationHooks{resolve: func(string) (string, error) { t.Fatal("bad artifact resolved manager"); return "", nil }, run: func(context.Context, string, []string) (runtimeReply, error) {
				t.Fatal("bad artifact reached manager")
				return runtimeReply{}, nil
			}}
			r, err := observeRuntime(context.Background(), spec, h)
			if err == nil || r.ManagerCalls != 0 || r.LookupAttempted {
				t.Fatal("unsafe/missing artifact reached observation", r, err)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
}

func TestServiceRuntimeObservationChangedBindingOrScopeSuppressesValues(t *testing.T) {
	for _, kind := range []string{"foreign argv", "foreign environment", "drop-in", "reload pending", "owner changed", "lookup changed", "descriptor changed", "socket changed"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			want := ErrRuntimeBinding
			switch kind {
			case "foreign argv":
				observationProperty(f.service, "ExecStartEx", "a(sasasttttuii)", []any{[]any{spec.Service.Executable, []string{spec.Service.Executable, "daemon", "--experimental-scan"}, []string{"no-env-expand"}, 0, 0, 0, 0, 0, 0, 0}})
			case "foreign environment":
				observationProperty(f.service, "Environment", "as", []string{"RYDD_RUNTIME_DIR=/foreign/runtime"})
			case "drop-in":
				observationProperty(f.unit, "DropInPaths", "as", []string{"/foreign/override"})
			case "reload pending":
				observationProperty(f.unit, "NeedDaemonReload", "b", true)
			case "owner changed":
				f.ownerChanged = true
			case "lookup changed":
				f.lookupChanged = true
			case "descriptor changed":
				want = ErrArtifactConflict
				f.after = func(n int) {
					if n == 7 {
						if err := os.WriteFile(filepath.Join(spec.Directory, ManagedLabel+".service"), []byte("changed"), 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
			case "socket changed":
				want = ErrManagerUnavailable
				f.after = func(n int) {
					if n == 5 {
						if err := os.Rename(spec.BusSocket, spec.BusSocket+"-offline"); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			r, err := observeRuntime(context.Background(), spec, observationHooks(f))
			if !errors.Is(err, want) {
				t.Fatal("changed scope not refused", r, err, want)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
}

func TestServiceRuntimeObservationTypedDynamicBounds(t *testing.T) {
	for _, kind := range []string{"missing state", "null state", "state type", "state empty", "state oversized", "state control", "pid null", "pid type", "pid negative", "pid fractional", "pid overflow", "invocation empty", "invocation null", "invocation string", "invocation oversized", "invocation byte overflow", "invocation byte null", "result missing"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			switch kind {
			case "missing state":
				delete(f.unit, "ActiveState")
			case "null state":
				observationProperty(f.unit, "ActiveState", "s", nil)
			case "state type":
				observationProperty(f.unit, "ActiveState", "b", true)
			case "state empty":
				observationProperty(f.unit, "ActiveState", "s", "")
			case "state oversized":
				observationProperty(f.unit, "ActiveState", "s", strings.Repeat("a", 65))
			case "state control":
				observationProperty(f.unit, "SubState", "s", "running\nprivate")
			case "pid null":
				observationProperty(f.service, "MainPID", "u", nil)
			case "pid type":
				observationProperty(f.service, "MainPID", "t", 1)
			case "pid negative":
				observationProperty(f.service, "MainPID", "u", -1)
			case "pid fractional":
				observationProperty(f.service, "MainPID", "u", 1.5)
			case "pid overflow":
				observationProperty(f.service, "MainPID", "u", uint64(1)<<32)
			case "invocation empty":
				observationProperty(f.unit, "InvocationID", "ay", []int{})
			case "invocation null":
				observationProperty(f.unit, "InvocationID", "ay", nil)
			case "invocation string":
				observationProperty(f.unit, "InvocationID", "ay", "AAECAwQFBgcICQoLDA0ODw==")
			case "invocation oversized":
				observationProperty(f.unit, "InvocationID", "ay", make([]int, 17))
			case "invocation byte overflow":
				v := make([]int, 16)
				v[15] = 256
				observationProperty(f.unit, "InvocationID", "ay", v)
			case "invocation byte null":
				v := make([]any, 16)
				for i := range v {
					v[i] = 0
				}
				v[15] = nil
				observationProperty(f.unit, "InvocationID", "ay", v)
			case "result missing":
				delete(f.service, "Result")
			}
			r, err := observeRuntime(context.Background(), spec, observationHooks(f))
			if !errors.Is(err, ErrManagerProtocol) || r.ManagerCalls != 5 {
				t.Fatal("malformed dynamic value accepted", r, err)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
}

func TestServiceRuntimeObservationLookupWireBounds(t *testing.T) {
	for _, body := range []string{`{"type":"o","data":["/foreign/unit"]}`, `{"type":"o","data":[null]}`, `{"type":"o","type":"o","data":["/org/freedesktop/systemd1/unit/managed"]}`, `{"type":"o","data":["/org/freedesktop/systemd1/unit/managed"]} {}`} {
		spec := runtimeFixture(t, "linux")
		f := newRuntimeObservationFake(t, spec)
		f.lookupReply = []byte(body)
		r, err := observeRuntime(context.Background(), spec, observationHooks(f))
		if !errors.Is(err, ErrManagerProtocol) || r.ManagerCalls != 3 {
			t.Fatal("invalid lookup wire accepted", r, err)
		}
		assertUnknownRuntimeObservation(t, r)
	}
	spec := runtimeFixture(t, "linux")
	f := newRuntimeObservationFake(t, spec)
	observationProperty(f.service, "PrivateUnknownMetadata", "s", strings.Repeat("x", 64<<10))
	r, err := observeRuntime(context.Background(), spec, observationHooks(f))
	if !errors.Is(err, ErrLifecycleBounds) || r.ManagerCalls != 5 {
		t.Fatal("oversized group accepted", r, err)
	}
	assertUnknownRuntimeObservation(t, r)
}

func TestServiceRuntimeObservationCancellationAndFailedReplies(t *testing.T) {
	for n := 1; n <= 7; n++ {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			f.refuseAt = n
			r, err := observeRuntime(context.Background(), spec, observationHooks(f))
			if !errors.Is(err, ErrManagerUnavailable) || r.ManagerCalls != n {
				t.Fatal("failed helper retried or published", r, err)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
	spec := runtimeFixture(t, "linux")
	f := newRuntimeObservationFake(t, spec)
	ctx, cancel := context.WithCancel(context.Background())
	f.after = func(n int) {
		if n == 5 {
			cancel()
		}
	}
	r, err := observeRuntime(ctx, spec, observationHooks(f))
	if !errors.Is(err, context.Canceled) || r.ManagerCalls != 5 {
		t.Fatal("canceled callback reached further manager calls", r, err)
	}
	assertUnknownRuntimeObservation(t, r)
	f = newRuntimeObservationFake(t, spec)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	r, err = observeRuntime(ctx, spec, observationHooks(f))
	if !errors.Is(err, context.Canceled) || len(f.methods) != 0 {
		t.Fatal("pre-canceled command started observation", r, err)
	}
}

func TestServiceRuntimeObservationRealClientFailuresKeepUnknown(t *testing.T) {
	for _, mode := range []string{"overflow", "stderr overflow", "invalid-mode"} {
		t.Run(mode, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			calls := 0
			h := runtimeObservationHooks{resolve: func(string) (string, error) { return os.Args[0], nil }, run: func(ctx context.Context, program string, args []string) (runtimeReply, error) {
				calls++
				return runRuntimeProcessWithEnv(ctx, program, []string{"-test.run=^TestServiceManagerProcessFixture$"}, append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE="+mode))
			}}
			r, err := observeRuntime(context.Background(), spec, h)
			want := ErrLifecycleBounds
			if mode == "invalid-mode" {
				want = ErrManagerUnavailable
			}
			if !errors.Is(err, want) || calls != 1 || r.ManagerCalls != 1 || r.LookupAttempted {
				t.Fatal("real failed/overflowed client escaped bound or reached lookup", r, err)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
}

func TestServiceRuntimeObservationWallElapsedAndTerminalStamp(t *testing.T) {
	base := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, kind := range []string{"wall deadline", "wall rollback", "elapsed deadline", "elapsed rollback", "terminal deadline"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "linux")
			f := newRuntimeObservationFake(t, spec)
			wall, elapsed := base, base
			terminalPhase, terminalCalls := false, 0
			h := observationHooks(f)
			h.wallNow = func() time.Time {
				if terminalPhase {
					terminalCalls++
					if kind == "terminal deadline" && terminalCalls == 2 {
						return base.Add(5 * time.Second)
					}
				}
				return wall
			}
			h.elapsedNow = func() time.Time { return elapsed }
			f.after = func(n int) {
				if n == 7 {
					terminalPhase = true
				}
				if n == 4 {
					switch kind {
					case "wall deadline":
						wall = base.Add(5 * time.Second)
					case "wall rollback":
						wall = base.Add(time.Second)
					case "elapsed deadline":
						elapsed = base.Add(5 * time.Second)
					case "elapsed rollback":
						elapsed = base.Add(time.Second)
					}
				}
				if n == 5 {
					if kind == "wall rollback" {
						wall = base.Add(500 * time.Millisecond)
					}
					if kind == "elapsed rollback" {
						elapsed = base.Add(500 * time.Millisecond)
					}
				}
			}
			r, err := observeRuntime(context.Background(), spec, h)
			if !errors.Is(err, ErrRuntimeObservationClock) {
				t.Fatal("clock boundary published observation", r, err, terminalCalls)
			}
			assertUnknownRuntimeObservation(t, r)
		})
	}
	spec := runtimeFixture(t, "linux")
	f := newRuntimeObservationFake(t, spec)
	wall := base
	terminalPhase, terminalCalls := false, 0
	h := observationHooks(f)
	h.wallNow = func() time.Time {
		if terminalPhase {
			terminalCalls++
			if terminalCalls > 2 {
				return base.Add(10 * time.Second)
			}
		}
		return wall
	}
	h.elapsedNow = func() time.Time { return base }
	f.after = func(n int) {
		if n == 4 {
			wall = base.Add(time.Second)
		}
		if n == 5 {
			wall = base.Add(2 * time.Second)
		}
		if n == 7 {
			terminalPhase = true
			wall = base.Add(3 * time.Second)
		}
	}
	r, err := observeRuntime(context.Background(), spec, h)
	if err != nil || terminalCalls != 2 || r.UnitObservedAt == nil || !r.UnitObservedAt.Equal(base.Add(time.Second)) || r.ServiceObservedAt == nil || !r.ServiceObservedAt.Equal(base.Add(2*time.Second)) || r.FinishedAt == nil || !r.FinishedAt.Equal(base.Add(3*time.Second)) {
		t.Fatal("terminal time sampled outside guarded window/sequential times lost", r, err, terminalCalls)
	}
}

func TestServiceRuntimeObservationClockRefusalIsSticky(t *testing.T) {
	base := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	wall, elapsed := base, base
	w, err := newRuntimeObservationWindow(context.Background(), runtimeObservationHooks{wallNow: func() time.Time { return wall }, elapsedNow: func() time.Time { return elapsed }})
	if err != nil {
		t.Fatal(err)
	}
	wall = base.Add(5 * time.Second)
	if _, err := w.stamp(context.Background()); !errors.Is(err, ErrRuntimeObservationClock) {
		t.Fatal(err)
	}
	wall = base
	if stamp, err := w.stamp(context.Background()); !errors.Is(err, ErrRuntimeObservationClock) || !stamp.IsZero() {
		t.Fatal("clock refusal cleared after rollback", stamp, err)
	}
}

func TestServiceRuntimeObservationPublicPlatformMismatch(t *testing.T) {
	spec, _ := serviceInstallFixture(t, "linux")
	if runtime.GOOS == "linux" {
		spec.Service.GOOS = "darwin"
	}
	if r, err := ObserveRuntime(context.Background(), spec); !errors.Is(err, ErrSpec) || r.Contract != "" {
		t.Fatal("public API accepted foreign native platform", r, err)
	}
}
