//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const RuntimeContract = "service_runtime_request_v1"

var (
	ErrRuntimeBinding = errors.New("loaded service settings differ from the exact requested scope")
	ErrRuntimeOutcome = errors.New("service manager request outcome requires inspection")
)

// RuntimeResult describes one manager request, not its eventual runtime result.
// Accepted is the selected manager client's success evidence: a typed queued job
// on Linux, or an opaque launchctl exit status on macOS. Neither proves readiness,
// shutdown completion, loaded-file identity or a hostile same-user namespace.
type RuntimeResult struct {
	Contract             string             `json:"contract"`
	Action               string             `json:"action"`
	Platform             string             `json:"platform"`
	ManagerProfile       string             `json:"manager_profile"`
	Label                string             `json:"label"`
	Directory            string             `json:"directory"`
	DescriptorPath       string             `json:"descriptor_path"`
	DescriptorSHA256     string             `json:"descriptor_sha256"`
	Executable           string             `json:"executable"`
	ConfigFile           string             `json:"config_file"`
	StateDir             string             `json:"state_dir"`
	RuntimeDir           string             `json:"runtime_dir"`
	Argv                 []string           `json:"argv"`
	Env                  map[string]string  `json:"env"`
	ArtifactStatus       string             `json:"artifact_status"`
	Manager              ManagerObservation `json:"manager"`
	ManagerUniqueName    string             `json:"manager_unique_name"`
	UnitObjectPath       string             `json:"unit_object_path"`
	UnitLoadAttempted    bool               `json:"unit_load_attempted"`
	LoadedBindingMatched *bool              `json:"loaded_binding_matched"`
	BindingCheckedAt     *time.Time         `json:"binding_checked_at"`
	RequestAttempted     bool               `json:"request_attempted"`
	RequestStatus        string             `json:"request_status"`
	RequestAccepted      *bool              `json:"request_accepted"`
	AcceptanceEvidence   string             `json:"acceptance_evidence"`
	JobPath              *string            `json:"job_path"`
	ReplyObservedAt      *time.Time         `json:"reply_observed_at"`
	Running              *bool              `json:"running"`
	Stopped              *bool              `json:"stopped"`
	RuntimeStateVerified bool               `json:"runtime_state_verified"`
	LoadedOriginVerified bool               `json:"loaded_origin_verified"`
	ScanningRequested    bool               `json:"scanning_requested"`
	EnablementChanged    bool               `json:"enablement_changed"`
	ReloadRequested      bool               `json:"reload_requested"`
	DescriptorChanged    bool               `json:"descriptor_changed"`
}

// Start requests the fixed idle-only service in the cooperating current user's
// manager namespace. It creates no artifacts and performs no enable or reload.
func Start(ctx context.Context, spec InstallSpec) (RuntimeResult, error) {
	if spec.Service.GOOS != runtime.GOOS {
		return RuntimeResult{}, fmt.Errorf("%w: service runtime platform must match this executable", ErrSpec)
	}
	return requestRuntime(ctx, spec, "start", runtimeHooks{})
}

// Stop requests the fixed service's stop. A queued job or opaque client success
// does not prove shutdown. The descriptor and its future-login effects remain.
func Stop(ctx context.Context, spec InstallSpec) (RuntimeResult, error) {
	if spec.Service.GOOS != runtime.GOOS {
		return RuntimeResult{}, fmt.Errorf("%w: service runtime platform must match this executable", ErrSpec)
	}
	return requestRuntime(ctx, spec, "stop", runtimeHooks{})
}

type runtimeReply struct {
	started bool
	data    []byte
}
type runtimeRunner func(context.Context, string, []string) (runtimeReply, error)
type runtimeHooks struct {
	resolve       func(string) (string, error)
	run           runtimeRunner
	beforeRequest func()
	afterRequest  func()
}

func requestRuntime(ctx context.Context, spec InstallSpec, action string, hooks runtimeHooks) (RuntimeResult, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if action != "start" && action != "stop" {
		return RuntimeResult{}, ErrSpec
	}
	d, err := Build(spec.Service)
	if err != nil {
		return RuntimeResult{}, err
	}
	for _, p := range []string{spec.HomeDir, spec.Directory} {
		if err = validatePath("service runtime", p); err != nil {
			return RuntimeResult{}, err
		}
		if len(strings.Split(strings.TrimPrefix(p, "/"), "/")) > 64 {
			return RuntimeResult{}, ErrLifecycleBounds
		}
	}
	base := spec.HomeDir
	program := "/bin/launchctl"
	if d.Platform == "darwin" {
		if spec.Directory != filepath.Join(spec.HomeDir, "Library", "LaunchAgents") {
			return RuntimeResult{}, ErrSpec
		}
	} else {
		if filepath.Base(spec.Directory) != "user" || filepath.Base(filepath.Dir(spec.Directory)) != "systemd" {
			return RuntimeResult{}, ErrSpec
		}
		base = filepath.Dir(filepath.Dir(spec.Directory))
		if err = validatePath("bus socket", spec.BusSocket); err != nil {
			return RuntimeResult{}, err
		}
		program = "busctl"
	}
	digest := sha256.Sum256([]byte(d.Content))
	r := RuntimeResult{Contract: RuntimeContract, Action: action, Platform: d.Platform, ManagerProfile: d.ManagerProfile, Label: d.Label, Directory: spec.Directory, DescriptorPath: filepath.Join(spec.Directory, d.Filename), DescriptorSHA256: hex.EncodeToString(digest[:]), Executable: d.Executable, ConfigFile: d.ConfigFile, StateDir: d.StateDir, RuntimeDir: d.RuntimeDir, Argv: append([]string{}, d.Argv...), Env: d.Env, ArtifactStatus: "unexamined", RequestStatus: "not_requested", Manager: ManagerObservation{Status: "not_checked", UnitPath: []string{}}}
	guard, err := openRuntimeGuard(ctx, base, spec.Directory, d)
	if err != nil {
		return r, err
	}
	defer guard.close()
	r.ArtifactStatus = "exact"
	resolve := exec.LookPath
	if hooks.resolve != nil {
		resolve = hooks.resolve
	}
	program, err = resolve(program)
	if err != nil {
		return r, errors.Join(ErrManagerUnavailable, ctx.Err())
	}
	run := runRuntimeProcess
	if hooks.run != nil {
		run = hooks.run
	}
	var bus *runtimeBus
	if d.Platform == "linux" {
		bus, err = openRuntimeBus(ctx, spec.BusSocket, program, run)
		if err != nil {
			return r, err
		}
		defer bus.close()
		if err = bindRuntimeUnit(ctx, bus, d, &r); err != nil {
			r.RequestStatus = "refused"
			return r, err
		}
	}
	if hooks.beforeRequest != nil {
		hooks.beforeRequest()
	}
	if err = guard.check(ctx); err != nil {
		r.RequestStatus = "refused"
		return r, err
	}
	var reply runtimeReply
	if bus != nil {
		if err = bus.checkOwner(ctx, r.ManagerUniqueName); err != nil {
			r.RequestStatus = "refused"
			return r, err
		}
		// The owner query is an external call. Repeat the exact artifact/lock
		// checks after it, immediately before the one runtime request.
		if err = guard.check(ctx); err != nil {
			r.RequestStatus = "refused"
			return r, err
		}
		method := "StartUnit"
		if action == "stop" {
			method = "StopUnit"
		}
		reply, err = bus.call(ctx, r.ManagerUniqueName, "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", method, "ss", d.Filename, "fail")
	} else {
		args := []string{"bootstrap", fmt.Sprintf("gui/%d", os.Geteuid()), r.DescriptorPath}
		if action == "stop" {
			args = []string{"bootout", fmt.Sprintf("gui/%d/%s", os.Geteuid(), ManagedLabel)}
		}
		childCtx, childCancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err = run(childCtx, program, args)
		childCancel()
		if len(reply.data) > 64<<10 {
			err = errors.Join(err, ErrLifecycleBounds)
		}
	}
	r.RequestAttempted = reply.started
	if err != nil {
		return runtimeFailure(r, errors.Join(err, ctx.Err()))
	}
	if !reply.started {
		return r, ErrManagerProtocol
	}
	if bus != nil {
		job, decodeErr := decodeRuntimeObject(reply.data)
		if decodeErr != nil || !runtimeJobPath(job) {
			return runtimeFailure(r, ErrManagerProtocol)
		}
		r.JobPath = &job
		r.AcceptanceEvidence = "typed_queued_job"
	} else {
		// launchctl output is opaque. Exit status is client-reported success,
		// never an origin, registration, running or completed-stop observation.
		r.AcceptanceEvidence = "opaque_launchctl_exit_zero"
	}
	accepted := true
	now := time.Now().UTC()
	r.RequestAccepted = &accepted
	r.RequestStatus = "accepted"
	r.ReplyObservedAt = &now
	if hooks.afterRequest != nil {
		hooks.afterRequest()
	}
	if err = guard.check(ctx); err != nil {
		return runtimeFailure(r, err)
	}
	if bus != nil {
		if err = bus.check(ctx); err != nil {
			return runtimeFailure(r, err)
		}
	}
	if err = ctx.Err(); err != nil {
		return runtimeFailure(r, err)
	}
	return r, nil
}

func runtimeFailure(r RuntimeResult, err error) (RuntimeResult, error) {
	if !r.RequestAttempted {
		return r, err
	}
	if r.RequestAccepted == nil {
		r.RequestStatus = "unknown"
	}
	return r, fmt.Errorf("%w: %s for managed label %q using descriptor %q: %w", ErrRuntimeOutcome, r.Action, r.Label, r.DescriptorPath, err)
}

type runtimeGuard struct {
	chain    *serviceChain
	lock     *os.File
	lockStat unix.Stat_t
	fileStat unix.Stat_t
	d        Descriptor
}

func openRuntimeGuard(ctx context.Context, base, directory string, d Descriptor) (*runtimeGuard, error) {
	chain, err := openServiceBase(ctx, base)
	if err != nil {
		return nil, err
	}
	g := &runtimeGuard{chain: chain, d: d}
	fail := func(err error) (*runtimeGuard, error) { g.close(); return nil, err }
	for _, name := range []string{filepath.Base(filepath.Dir(directory)), filepath.Base(directory)} {
		if _, err = chain.child(ctx, name, false); err != nil {
			return fail(err)
		}
	}
	parent := chain.last()
	if chain.stamps[len(chain.stamps)-1].Uid != uint32(os.Geteuid()) || chain.stamps[len(chain.stamps)-1].Mode&0022 != 0 {
		return fail(ErrArtifactConflict)
	}
	if err = unix.Fstatat(int(parent.Fd()), d.Filename, &g.fileStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fail(err)
	}
	status, err := readArtifact(ctx, parent, d.Filename, []byte(d.Content))
	if err != nil {
		return fail(err)
	}
	if status != "exact" {
		return fail(os.ErrNotExist)
	}
	fd, err := unix.Openat(int(parent.Fd()), "."+ManagedLabel+".lock", unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return fail(err)
	}
	g.lock = os.NewFile(uintptr(fd), filepath.Join(parent.Name(), "."+ManagedLabel+".lock"))
	if err = checkServiceLock(parent, g.lock); err != nil {
		return fail(err)
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			err = ErrArtifactBusy
		}
		return fail(err)
	}
	if err = unix.Fstat(fd, &g.lockStat); err != nil {
		return fail(err)
	}
	if err = g.check(ctx); err != nil {
		return fail(err)
	}
	return g, nil
}

func (g *runtimeGuard) close() {
	if g.lock != nil {
		_ = g.lock.Close()
	}
	if g.chain != nil {
		g.chain.close()
	}
}

func (g *runtimeGuard) check(ctx context.Context) error {
	if err := g.chain.check(ctx); err != nil {
		return err
	}
	p := g.chain.last()
	if err := checkServiceLock(p, g.lock, g.lockStat); err != nil {
		return err
	}
	status, err := readArtifact(ctx, p, g.d.Filename, []byte(g.d.Content))
	if err != nil {
		return err
	}
	var named unix.Stat_t
	if err = unix.Fstatat(int(p.Fd()), g.d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if status != "exact" || !sameServiceStamp(g.fileStat, named) {
		return ErrArtifactChanged
	}
	return ctx.Err()
}
