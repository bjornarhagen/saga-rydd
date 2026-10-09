//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const RuntimeObservationContract = "service_runtime_observation_v1"
const runtimeObservationLimit = 5 * time.Second

var ErrRuntimeObservationClock = errors.New("service runtime observation cannot establish a bounded clock window")

// RuntimeObservation projects historical, sequential manager declarations.
// Supported identifies the implemented Linux profile, not artifact presence,
// installed-manager compatibility, a current process or application readiness.
// No loaded unit or process is authenticated, and no dynamic field authorizes
// activation, source access or cleanup. Failed operations omit dynamic positives.
type RuntimeObservation struct {
	Contract                     string             `json:"contract"`
	Platform                     string             `json:"platform"`
	ManagerProfile               string             `json:"manager_profile"`
	Label                        string             `json:"label"`
	Directory                    string             `json:"directory"`
	DescriptorPath               string             `json:"descriptor_path"`
	DescriptorSHA256             string             `json:"descriptor_sha256"`
	Executable                   string             `json:"executable"`
	ConfigFile                   string             `json:"config_file"`
	StateDir                     string             `json:"state_dir"`
	RuntimeDir                   string             `json:"runtime_dir"`
	Argv                         []string           `json:"argv"`
	Env                          map[string]string  `json:"env"`
	Supported                    bool               `json:"supported"`
	Status                       string             `json:"status"`
	ArtifactStatus               string             `json:"artifact_status"`
	Manager                      ManagerObservation `json:"manager"`
	ManagerUniqueName            string             `json:"manager_unique_name"`
	UnitObjectPath               string             `json:"unit_object_path"`
	LookupAttempted              bool               `json:"lookup_attempted"`
	LookupRechecked              bool               `json:"lookup_rechecked"`
	LoadedBindingMatched         *bool              `json:"loaded_binding_matched"`
	BindingCheckedAt             *time.Time         `json:"binding_checked_at"`
	UnitObservedAt               *time.Time         `json:"unit_observed_at"`
	ServiceObservedAt            *time.Time         `json:"service_observed_at"`
	FinishedAt                   *time.Time         `json:"finished_at"`
	ActiveState                  *string            `json:"active_state"`
	SubState                     *string            `json:"sub_state"`
	ServiceResult                *string            `json:"service_result"`
	MainPID                      *uint32            `json:"main_pid"`
	InvocationID                 *string            `json:"invocation_id"`
	InvocationIDStatus           string             `json:"invocation_id_status"`
	ManagerCalls                 int                `json:"manager_calls"`
	ManagerReplyBytes            int                `json:"manager_reply_bytes"`
	RuntimeStateObserved         bool               `json:"runtime_state_observed"`
	SequentialObservation        bool               `json:"sequential_observation"`
	Running                      *bool              `json:"running"`
	Stopped                      *bool              `json:"stopped"`
	ApplicationReady             *bool              `json:"application_ready"`
	RuntimeStateVerified         bool               `json:"runtime_state_verified"`
	LoadedOriginVerified         bool               `json:"loaded_origin_verified"`
	ProcessIdentityVerified      bool               `json:"process_identity_verified"`
	ApplicationReadinessVerified bool               `json:"application_readiness_verified"`
	UnitLoadRequested            bool               `json:"unit_load_requested"`
	StartStopRequested           bool               `json:"start_stop_requested"`
	ScanningRequested            bool               `json:"scanning_requested"`
	EnablementChanged            bool               `json:"enablement_changed"`
	ReloadRequested              bool               `json:"reload_requested"`
	DescriptorChanged            bool               `json:"descriptor_changed"`
	SourceContentsOpened         bool               `json:"source_contents_opened"`
}

// ObserveRuntime reads only an exact existing descriptor/coordinator and an
// already loaded Linux unit in the cooperating current-user manager namespace.
// It never calls LoadUnit, a lifecycle action, a list operation or worker IPC.
// Connecting to the local broker socket can still affect broker infrastructure.
// macOS returns unsupported without examining artifacts or invoking launchctl.
func ObserveRuntime(ctx context.Context, spec InstallSpec) (RuntimeObservation, error) {
	if spec.Service.GOOS != runtime.GOOS {
		return RuntimeObservation{}, fmt.Errorf("%w: service observation platform must match this executable", ErrSpec)
	}
	return observeRuntime(ctx, spec, runtimeObservationHooks{})
}

type runtimeObservationHooks struct {
	resolve    func(string) (string, error)
	run        runtimeRunner
	wallNow    func() time.Time
	elapsedNow func() time.Time
}

func observeRuntime(ctx context.Context, spec InstallSpec, hooks runtimeObservationHooks) (r RuntimeObservation, resultErr error) {
	if ctx == nil {
		return r, ErrSpec
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeObservationLimit)
	defer cancel()
	window, err := newRuntimeObservationWindow(ctx, hooks)
	if err != nil {
		return r, err
	}
	d, err := Build(spec.Service)
	if err != nil {
		return r, err
	}
	for _, p := range []string{spec.HomeDir, spec.Directory} {
		if err := validatePath("service runtime observation", p); err != nil {
			return r, err
		}
		if len(strings.Split(strings.TrimPrefix(p, "/"), "/")) > 64 {
			return r, ErrLifecycleBounds
		}
	}
	digest := sha256.Sum256([]byte(d.Content))
	r = RuntimeObservation{Contract: RuntimeObservationContract, Platform: d.Platform, ManagerProfile: d.ManagerProfile, Label: d.Label, Directory: spec.Directory, DescriptorPath: filepath.Join(spec.Directory, d.Filename), DescriptorSHA256: hex.EncodeToString(digest[:]), Executable: d.Executable, ConfigFile: d.ConfigFile, StateDir: d.StateDir, RuntimeDir: d.RuntimeDir, Argv: append([]string{}, d.Argv...), Env: d.Env, Supported: d.Platform == "linux", Status: "unexamined", ArtifactStatus: "unexamined", InvocationIDStatus: "unexamined", Manager: ManagerObservation{Status: "not_checked", UnitPath: []string{}}}
	defer func() {
		if resultErr != nil {
			r.Status = "unavailable"
			if errors.Is(resultErr, ErrSpec) || errors.Is(resultErr, ErrRuntimeBinding) || errors.Is(resultErr, ErrArtifactConflict) || errors.Is(resultErr, ErrArtifactChanged) || errors.Is(resultErr, ErrArtifactBusy) || errors.Is(resultErr, ErrRuntimeObservationClock) {
				r.Status = "refused"
			}
			r.suppressRuntimeFields()
		}
	}()
	if d.Platform == "darwin" {
		if spec.Directory != filepath.Join(spec.HomeDir, "Library", "LaunchAgents") {
			return r, ErrSpec
		}
		if _, err := window.stamp(ctx); err != nil {
			return r, err
		}
		r.Status = "unsupported"
		return r, nil
	}
	if filepath.Base(spec.Directory) != "user" || filepath.Base(filepath.Dir(spec.Directory)) != "systemd" {
		return r, ErrSpec
	}
	if err := validatePath("bus socket", spec.BusSocket); err != nil {
		return r, err
	}
	if _, err := window.stamp(ctx); err != nil {
		return r, err
	}
	guard, err := openRuntimeGuard(ctx, filepath.Dir(filepath.Dir(spec.Directory)), spec.Directory, d)
	if err != nil {
		return r, err
	}
	defer guard.close()
	if _, err := window.stamp(ctx); err != nil {
		return r, err
	}
	r.ArtifactStatus = "exact"
	resolve := hooks.resolve
	if resolve == nil {
		resolve = exec.LookPath
	}
	program, resolveErr := resolve("busctl")
	if _, err := window.stamp(ctx); err != nil {
		return r, err
	}
	if resolveErr != nil {
		return r, ErrManagerUnavailable
	}
	run := hooks.run
	if run == nil {
		run = runRuntimeProcess
	}
	bus, err := openRuntimeBus(ctx, spec.BusSocket, program, run)
	if err != nil {
		return r, err
	}
	defer bus.close()
	defer func() { r.ManagerCalls, r.ManagerReplyBytes = bus.calls, bus.bytes }()
	call := func(dest, object, iface, method, signature string, values ...string) (runtimeReply, time.Time, error) {
		if _, err := window.stamp(ctx); err != nil {
			return runtimeReply{}, time.Time{}, err
		}
		reply, callErr := bus.call(ctx, dest, object, iface, method, signature, values...)
		stamp, clockErr := window.stamp(ctx)
		return reply, stamp, errors.Join(callErr, clockErr)
	}
	owner := func() (string, error) {
		reply, _, err := call("org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus", "GetNameOwner", "s", "org.freedesktop.systemd1")
		if err != nil {
			return "", err
		}
		name, err := decodeRuntimeString(reply.data)
		if err != nil || !runtimeUniqueName(name) {
			return "", ErrManagerProtocol
		}
		return name, nil
	}
	r.ManagerUniqueName, err = owner()
	if err != nil {
		return r, err
	}
	reply, placementAt, err := call(r.ManagerUniqueName, "/org/freedesktop/systemd1", "org.freedesktop.DBus.Properties", "Get", "ss", "org.freedesktop.systemd1.Manager", "UnitPath")
	if err != nil {
		return r, err
	}
	paths, err := decodeManagerUnitPath(reply.data)
	if err != nil {
		return r, err
	}
	inPath := false
	for _, path := range paths {
		if path == spec.Directory {
			inPath = true
		}
	}
	r.Manager = ManagerObservation{Status: "observed", ObservedAt: &placementAt, BusSocket: spec.BusSocket, UnitPath: paths, DirectoryInUnitPath: &inPath}
	if !inPath {
		return r, ErrManagerPath
	}
	lookup := func() (runtimeReply, string, error) {
		reply, _, err := call(r.ManagerUniqueName, "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "GetUnit", "s", d.Filename)
		if err != nil {
			// A client exit does not preserve a typed D-Bus missing-unit error.
			// Do not infer that the service is unloaded or stopped.
			return reply, "", err
		}
		object, err := decodeRuntimeObject(reply.data)
		if err != nil || !runtimeObjectPath(object) || !strings.HasPrefix(object, "/org/freedesktop/systemd1/unit/") {
			return reply, "", ErrManagerProtocol
		}
		return reply, object, nil
	}
	reply, r.UnitObjectPath, err = lookup()
	r.LookupAttempted = reply.started
	if err != nil {
		return r, err
	}
	reply, unitAt, err := call(r.ManagerUniqueName, r.UnitObjectPath, "org.freedesktop.DBus.Properties", "GetAll", "s", "org.freedesktop.systemd1.Unit")
	if err != nil {
		return r, err
	}
	unit, err := decodeRuntimeProperties(reply.data)
	if err != nil {
		return r, err
	}
	reply, serviceAt, err := call(r.ManagerUniqueName, r.UnitObjectPath, "org.freedesktop.DBus.Properties", "GetAll", "s", "org.freedesktop.systemd1.Service")
	if err != nil {
		return r, err
	}
	service, err := decodeRuntimeProperties(reply.data)
	if err != nil {
		return r, err
	}
	if err := matchRuntimeProperties(d, r.DescriptorPath, unit, service); err != nil {
		if errors.Is(err, ErrRuntimeBinding) {
			matched := false
			r.LoadedBindingMatched = &matched
		}
		return r, err
	}
	active, sub, result, pid, invocation, err := runtimeObservedFields(unit, service)
	if err != nil {
		return r, err
	}
	bindingAt, err := window.stamp(ctx)
	if err != nil {
		return r, err
	}
	_, object, err := lookup()
	if err != nil {
		return r, err
	}
	if object != r.UnitObjectPath {
		return r, ErrRuntimeBinding
	}
	r.LookupRechecked = true
	finalOwner, err := owner()
	if err != nil {
		return r, err
	}
	if finalOwner != r.ManagerUniqueName {
		return r, ErrRuntimeBinding
	}
	if err := guard.check(ctx); err != nil {
		return r, err
	}
	if err := bus.check(ctx); err != nil {
		return r, err
	}
	finishedAt, err := window.stamp(ctx)
	if err != nil {
		return r, err
	}
	matched := true
	r.Status, r.LoadedBindingMatched = "observed", &matched
	r.BindingCheckedAt, r.UnitObservedAt, r.ServiceObservedAt, r.FinishedAt = &bindingAt, &unitAt, &serviceAt, &finishedAt
	r.ActiveState, r.SubState, r.ServiceResult, r.MainPID, r.InvocationID = &active, &sub, &result, &pid, invocation
	r.InvocationIDStatus = "observed"
	if invocation == nil {
		r.InvocationIDStatus = "unset"
	}
	r.RuntimeStateObserved, r.SequentialObservation = true, true
	return r, ctx.Err()
}

func (r *RuntimeObservation) suppressRuntimeFields() {
	if r.LoadedBindingMatched != nil && *r.LoadedBindingMatched {
		r.LoadedBindingMatched = nil
	}
	r.BindingCheckedAt, r.UnitObservedAt, r.ServiceObservedAt, r.FinishedAt = nil, nil, nil, nil
	r.ActiveState, r.SubState, r.ServiceResult, r.MainPID, r.InvocationID = nil, nil, nil, nil, nil
	r.InvocationIDStatus = "unexamined"
	r.RuntimeStateObserved, r.SequentialObservation = false, false
}

func runtimeObservedFields(unit, service map[string]runtimeVariant) (active, sub, result string, pid uint32, invocation *string, err error) {
	label := func(properties map[string]runtimeVariant, key string) (string, error) {
		value, err := runtimeProperty[string](properties, key, "s")
		if err != nil || len(value) < 1 || len(value) > 64 || strings.ContainsFunc(value, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') }) {
			return "", ErrManagerProtocol
		}
		return value, nil
	}
	if active, err = label(unit, "ActiveState"); err != nil {
		return
	}
	if sub, err = label(unit, "SubState"); err != nil {
		return
	}
	if result, err = label(service, "Result"); err != nil {
		return
	}
	if pid, err = runtimeProperty[uint32](service, "MainPID", "u"); err != nil {
		return
	}
	var raw []json.RawMessage
	if raw, err = runtimeProperty[[]json.RawMessage](unit, "InvocationID", "ay"); err != nil || len(raw) != 16 {
		err = ErrManagerProtocol
		return
	}
	var id [16]byte
	for i, value := range raw {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || runtimeJSON(value, &id[i]) != nil {
			err = ErrManagerProtocol
			return
		}
	}
	if id != [16]byte{} {
		hexID := hex.EncodeToString(id[:])
		invocation = &hexID
	}
	return
}

// Separate elapsed and UTC wall observations are never combined into one time.
// Either deadline or a wall/elapsed rollback permanently refuses this operation.
// A successful stamp is the only terminal time published; no later fresh clock
// call can stamp evidence outside this checked window.
type runtimeObservationWindow struct {
	wallNow, elapsedNow            func() time.Time
	wallStart, highWater           time.Time
	elapsedStart, elapsedHighWater time.Time
	denied                         error
}

func newRuntimeObservationWindow(ctx context.Context, hooks runtimeObservationHooks) (*runtimeObservationWindow, error) {
	w := &runtimeObservationWindow{wallNow: hooks.wallNow, elapsedNow: hooks.elapsedNow}
	if w.wallNow == nil {
		w.wallNow = time.Now
	}
	if w.elapsedNow == nil {
		w.elapsedNow = time.Now
	}
	w.wallStart, w.elapsedStart = w.wallNow().UTC(), w.elapsedNow()
	w.highWater = w.wallStart
	w.elapsedHighWater = w.elapsedStart
	if w.wallStart.IsZero() || w.elapsedStart.IsZero() {
		return nil, ErrRuntimeObservationClock
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *runtimeObservationWindow) stamp(ctx context.Context) (time.Time, error) {
	if w.denied != nil {
		return time.Time{}, w.denied
	}
	if err := ctx.Err(); err != nil {
		w.denied = err
		return time.Time{}, err
	}
	wall, elapsed := w.wallNow().UTC(), w.elapsedNow()
	if wall.IsZero() || elapsed.IsZero() || wall.Before(w.highWater) || elapsed.Before(w.elapsedHighWater) || wall.Sub(w.wallStart) >= runtimeObservationLimit || elapsed.Sub(w.elapsedStart) >= runtimeObservationLimit {
		w.denied = ErrRuntimeObservationClock
		return time.Time{}, w.denied
	}
	if err := ctx.Err(); err != nil {
		w.denied = err
		return time.Time{}, err
	}
	w.highWater = wall
	w.elapsedHighWater = elapsed
	return wall, nil
}
