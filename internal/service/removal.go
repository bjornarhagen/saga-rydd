//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const RemovalContract = "service_descriptor_removal_v1"

var ErrArtifactRemoval = errors.New("service descriptor removal requires inspection with the exact scope")

// RemovalResult separates a pathname operation, checked metadata and sync calls.
// Observations describe their recorded instants, not lasting namespace absence.
// Removing this descriptor neither stops a service nor proves its registration,
// enablement or future-login state. The scope is cooperating managed metadata;
// pre/post checks do not turn unlinkat into an inode-conditioned source action.
type RemovalResult struct {
	Contract                string            `json:"contract"`
	Action                  string            `json:"action"`
	Platform                string            `json:"platform"`
	ManagerProfile          string            `json:"manager_profile"`
	Label                   string            `json:"label"`
	Directory               string            `json:"directory"`
	DescriptorPath          string            `json:"descriptor_path"`
	DescriptorSHA256        string            `json:"descriptor_sha256"`
	Executable              string            `json:"executable"`
	ConfigFile              string            `json:"config_file"`
	StateDir                string            `json:"state_dir"`
	RuntimeDir              string            `json:"runtime_dir"`
	Argv                    []string          `json:"argv"`
	Env                     map[string]string `json:"env"`
	ArtifactStatus          string            `json:"artifact_status"`
	RemovalStatus           string            `json:"removal_status"`
	RemovalAttempted        bool              `json:"removal_attempted"`
	UnlinkCompleted         bool              `json:"unlink_completed"`
	RemovalObserved         *bool             `json:"removal_observed"`
	DescriptorAbsent        *bool             `json:"descriptor_absent"`
	ObservationAt           *time.Time        `json:"observation_at"`
	SyncCompleted           bool              `json:"sync_completed"`
	ManagerRequestAttempted bool              `json:"manager_request_attempted"`
	StopRequested           bool              `json:"stop_requested"`
	EnablementChanged       bool              `json:"enablement_changed"`
	ReloadRequested         bool              `json:"reload_requested"`
	ScanningRequested       bool              `json:"scanning_requested"`
	Running                 *bool             `json:"running"`
	Stopped                 *bool             `json:"stopped"`
	FutureLoginMayStart     *bool             `json:"future_login_may_start"`
	RuntimeStateVerified    bool              `json:"runtime_state_verified"`
	LoadedOriginVerified    bool              `json:"loaded_origin_verified"`
	DataRemoved             bool              `json:"data_removed"`
	ExecutableRemoved       bool              `json:"executable_removed"`
	DirectoriesRemoved      bool              `json:"directories_removed"`
	LockRemoved             bool              `json:"lock_removed"`
}

// Uninstall removes only the exact managed descriptor. It invokes no manager,
// creates no files, and retains the executable, data, lock and directories.
// An existing service can continue; request Stop before removal when wanted.
func Uninstall(ctx context.Context, spec InstallSpec) (RemovalResult, error) {
	if spec.Service.GOOS != runtime.GOOS {
		return RemovalResult{}, fmt.Errorf("%w: service removal platform must match this executable", ErrSpec)
	}
	return removeDescriptor(ctx, spec, removalHooks{})
}

type removalHooks struct {
	beforeUnlink func()
	afterUnlink  func()
	unlink       func(int, string) error
	syncParent   func(*os.File) error
}

func removeDescriptor(ctx context.Context, spec InstallSpec, hooks removalHooks) (RemovalResult, error) {
	if err := ctx.Err(); err != nil {
		return RemovalResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	d, err := Build(spec.Service)
	if err != nil {
		return RemovalResult{}, err
	}
	for _, p := range []string{spec.HomeDir, spec.Directory} {
		if err = validatePath("service removal", p); err != nil {
			return RemovalResult{}, err
		}
		if len(strings.Split(strings.TrimPrefix(p, "/"), "/")) > 64 {
			return RemovalResult{}, ErrLifecycleBounds
		}
	}
	base := spec.HomeDir
	if d.Platform == "darwin" {
		if spec.Directory != filepath.Join(spec.HomeDir, "Library", "LaunchAgents") {
			return RemovalResult{}, ErrSpec
		}
	} else {
		if filepath.Base(spec.Directory) != "user" || filepath.Base(filepath.Dir(spec.Directory)) != "systemd" {
			return RemovalResult{}, ErrSpec
		}
		base = filepath.Dir(filepath.Dir(spec.Directory))
	}
	digest := sha256.Sum256([]byte(d.Content))
	r := RemovalResult{Contract: RemovalContract, Action: "uninstall", Platform: d.Platform, ManagerProfile: d.ManagerProfile, Label: d.Label, Directory: spec.Directory, DescriptorPath: filepath.Join(spec.Directory, d.Filename), DescriptorSHA256: hex.EncodeToString(digest[:]), Executable: d.Executable, ConfigFile: d.ConfigFile, StateDir: d.StateDir, RuntimeDir: d.RuntimeDir, Argv: append([]string{}, d.Argv...), Env: d.Env, ArtifactStatus: "unexamined", RemovalStatus: "not_requested"}
	g, err := openRemovalGuard(ctx, base, spec.Directory, d)
	if err != nil {
		return r, err
	}
	defer g.close()
	if g.file == nil {
		if err = g.checkScope(ctx); err != nil {
			return r, err
		}
		var named unix.Stat_t
		if err = unix.Fstatat(int(g.parent().Fd()), d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
			if err == nil {
				err = ErrArtifactChanged
			}
			return r, err
		}
		if err = g.checkScope(ctx); err != nil {
			return r, err
		}
		absent, removed := true, false
		now := time.Now().UTC()
		r.ArtifactStatus, r.RemovalStatus = "absent", "not_needed"
		r.DescriptorAbsent, r.RemovalObserved, r.ObservationAt = &absent, &removed, &now
		return r, nil
	}
	r.ArtifactStatus = "exact"
	if hooks.beforeUnlink != nil {
		hooks.beforeUnlink()
	}
	if err = g.checkBeforeUnlink(ctx); err != nil {
		return r, err
	}
	unlink := func(fd int, name string) error { return unix.Unlinkat(fd, name, 0) }
	if hooks.unlink != nil {
		unlink = hooks.unlink
	}
	r.RemovalAttempted, r.RemovalStatus, r.ArtifactStatus = true, "unknown", "unexamined"
	err = unlink(int(g.parent().Fd()), d.Filename)
	r.UnlinkCompleted = err == nil
	if hooks.afterUnlink != nil {
		hooks.afterUnlink()
	}
	checkedErr := g.observeRemoval(ctx, &r)
	if err != nil || checkedErr != nil {
		return removalFailure(r, errors.Join(err, checkedErr, ctx.Err()))
	}
	syncParent := func(f *os.File) error { return f.Sync() }
	if hooks.syncParent != nil {
		syncParent = hooks.syncParent
	}
	if err = ctx.Err(); err != nil {
		return removalFailure(r, err)
	}
	if err = syncParent(g.parent()); err != nil {
		return removalFailure(r, errors.Join(err, ctx.Err()))
	}
	r.SyncCompleted = true
	if err = g.observeRemoval(ctx, &r); err != nil {
		return removalFailure(r, err)
	}
	return r, nil
}

func removalFailure(r RemovalResult, err error) (RemovalResult, error) {
	return r, fmt.Errorf("%w: descriptor %q, expected SHA-256 %s: %w", ErrArtifactRemoval, r.DescriptorPath, r.DescriptorSHA256, err)
}

type removalGuard struct {
	chain    *serviceChain
	lock     *os.File
	file     *os.File
	lockStat unix.Stat_t
	fileStat unix.Stat_t
	d        Descriptor
}

func (g *removalGuard) parent() *os.File { return g.chain.last() }
func (g *removalGuard) close() {
	if g.file != nil {
		_ = g.file.Close()
	}
	if g.lock != nil {
		_ = g.lock.Close()
	}
	if g.chain != nil {
		g.chain.close()
	}
}

func openRemovalGuard(ctx context.Context, base, directory string, d Descriptor) (*removalGuard, error) {
	chain, err := openServiceBase(ctx, base)
	if err != nil {
		return nil, err
	}
	g := &removalGuard{chain: chain, d: d}
	fail := func(err error) (*removalGuard, error) { g.close(); return nil, err }
	for _, name := range []string{filepath.Base(filepath.Dir(directory)), filepath.Base(directory)} {
		if _, err = chain.child(ctx, name, false); err != nil {
			return fail(err)
		}
	}
	parent := g.parent()
	if st := chain.stamps[len(chain.stamps)-1]; st.Uid != uint32(os.Geteuid()) || st.Mode&0022 != 0 {
		return fail(ErrArtifactConflict)
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
	if err = g.checkScope(ctx); err != nil {
		return fail(err)
	}
	fd, err = unix.Openat(int(parent.Fd()), d.Filename, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return g, nil
	}
	if err != nil {
		return fail(errors.Join(ErrArtifactConflict, err))
	}
	g.file = os.NewFile(uintptr(fd), filepath.Join(parent.Name(), d.Filename))
	if err = unix.Fstat(fd, &g.fileStat); err != nil {
		return fail(err)
	}
	if !serviceFile(g.fileStat) || g.fileStat.Size > MaxDescriptorBytes {
		return fail(ErrArtifactConflict)
	}
	var data bytes.Buffer
	buffer := make([]byte, 32<<10)
	for {
		if err = ctx.Err(); err != nil {
			return fail(err)
		}
		n, readErr := g.file.Read(buffer)
		if data.Len()+n > MaxDescriptorBytes {
			return fail(ErrLifecycleBounds)
		}
		data.Write(buffer[:n])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fail(readErr)
		}
		if n == 0 {
			return fail(io.ErrNoProgress)
		}
	}
	if err = g.checkBeforeUnlink(ctx); err != nil {
		return fail(err)
	}
	if !bytes.Equal(data.Bytes(), []byte(d.Content)) {
		return fail(ErrArtifactConflict)
	}
	return g, nil
}

func (g *removalGuard) checkScope(ctx context.Context) error {
	if err := g.chain.check(ctx); err != nil {
		return err
	}
	if err := checkServiceLock(g.parent(), g.lock, g.lockStat); err != nil {
		return err
	}
	return ctx.Err()
}

func (g *removalGuard) checkBeforeUnlink(ctx context.Context) error {
	if err := g.checkScope(ctx); err != nil {
		return err
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(int(g.file.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(g.parent().Fd()), g.d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !serviceFile(held) || !serviceFile(named) || !sameServiceStamp(g.fileStat, held) || !sameServiceStamp(held, named) {
		return ErrArtifactChanged
	}
	return ctx.Err()
}

func (g *removalGuard) observeRemoval(ctx context.Context, r *RemovalResult) error {
	if err := g.checkScope(ctx); err != nil {
		return err
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(int(g.file.Fd()), &held); err != nil {
		return err
	}
	// Unlink changes ctime and link count. Keep the held-object evidence separate
	// from named absence, and never reuse the pre-operation ctime as a baseline.
	if !sameServiceIdentity(g.fileStat, held) || held.Mode != g.fileStat.Mode || held.Uid != g.fileStat.Uid || held.Size != g.fileStat.Size || held.Mtim != g.fileStat.Mtim {
		return ErrArtifactChanged
	}
	removed := held.Nlink == 0
	r.RemovalObserved = &removed
	err := unix.Fstatat(int(g.parent().Fd()), g.d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	absent := errors.Is(err, unix.ENOENT)
	r.DescriptorAbsent = &absent
	now := time.Now().UTC()
	r.ObservationAt = &now
	if absent {
		r.ArtifactStatus = "absent"
	} else {
		r.ArtifactStatus = "unexamined"
	}
	if removed && absent {
		r.RemovalStatus = "removed"
	} else {
		r.RemovalStatus = "unknown"
		return ErrArtifactChanged
	}
	return errors.Join(g.checkScope(ctx), ctx.Err())
}
