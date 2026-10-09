//go:build linux

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
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const loginWantsDirectory = "default.target.wants"

func controlLoginLink(ctx context.Context, spec InstallSpec, action string, hooks loginLinkHooks) (LoginLinkResult, error) {
	if err := ctx.Err(); err != nil {
		return LoginLinkResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if spec.Service.GOOS != "linux" || (action != "enable-login" && action != "disable-login") {
		return LoginLinkResult{}, ErrSpec
	}
	d, err := Build(spec.Service)
	if err != nil {
		return LoginLinkResult{}, err
	}
	linkPath := filepath.Join(spec.Directory, loginWantsDirectory, d.Filename)
	descriptorPath := filepath.Join(spec.Directory, d.Filename)
	for _, p := range []string{spec.HomeDir, spec.Directory, descriptorPath, linkPath, spec.BusSocket} {
		if err = validatePath("login link scope", p); err != nil {
			return LoginLinkResult{}, err
		}
		if len(strings.Split(strings.TrimPrefix(p, "/"), "/")) > 64 {
			return LoginLinkResult{}, ErrLifecycleBounds
		}
	}
	if filepath.Base(spec.Directory) != "user" || filepath.Base(filepath.Dir(spec.Directory)) != "systemd" {
		return LoginLinkResult{}, ErrSpec
	}
	digest := sha256.Sum256([]byte(d.Content))
	r := LoginLinkResult{Contract: LoginLinkContract, Action: action, Scope: "selected_default_target_dependency", Platform: d.Platform, ManagerProfile: d.ManagerProfile, Label: d.Label, Directory: spec.Directory, DescriptorPath: descriptorPath, DescriptorSHA256: hex.EncodeToString(digest[:]), Executable: d.Executable, ConfigFile: d.ConfigFile, StateDir: d.StateDir, RuntimeDir: d.RuntimeDir, Argv: append([]string{}, d.Argv...), Env: d.Env, Manager: ManagerObservation{Status: "not_checked", UnitPath: []string{}}, LinkPath: linkPath, LinkTarget: descriptorPath, LinkStatus: "unexamined", ChangeStatus: "not_requested", DirectoriesCreated: []string{}}
	g, err := openRuntimeGuard(ctx, filepath.Dir(filepath.Dir(spec.Directory)), spec.Directory, d)
	if err != nil {
		return r, err
	}
	defer g.close()
	resolve := exec.LookPath
	if hooks.manager.resolve != nil {
		resolve = hooks.manager.resolve
	}
	program, err := resolve("busctl")
	if err != nil {
		return r, errors.Join(ErrManagerUnavailable, ctx.Err())
	}
	run := runRuntimeProcess
	if hooks.manager.run != nil {
		run = hooks.manager.run
	}
	bus, err := openRuntimeBus(ctx, spec.BusSocket, program, run)
	if err != nil {
		return r, err
	}
	defer bus.close()
	bound := RuntimeResult{Directory: r.Directory, DescriptorPath: r.DescriptorPath, Manager: r.Manager}
	err = bindRuntimeUnit(ctx, bus, d, &bound)
	r.Manager, r.ManagerUniqueName, r.UnitObjectPath = bound.Manager, bound.ManagerUniqueName, bound.UnitObjectPath
	r.UnitLoadAttempted, r.LoadedBindingMatched, r.BindingCheckedAt = bound.UnitLoadAttempted, bound.LoadedBindingMatched, bound.BindingCheckedAt
	if err != nil {
		return r, err
	}
	if err = g.check(ctx); err != nil {
		return r, err
	}
	dir, err := openLoginDirectory(ctx, g, action == "enable-login", &r, hooks.syncParent)
	if err != nil {
		return loginLinkFailure(r, err)
	}
	if dir != nil {
		defer dir.close()
	}
	if err = bus.checkOwner(ctx, r.ManagerUniqueName); err != nil {
		return loginLinkFailure(r, err)
	}
	if err = g.check(ctx); err != nil {
		return loginLinkFailure(r, err)
	}
	if dir == nil {
		// Disable does not create a missing wants directory. Recheck its absence
		// relative to the still-held exact descriptor directory after the query.
		var st unix.Stat_t
		err = unix.Fstatat(int(g.chain.last().Fd()), loginWantsDirectory, &st, unix.AT_SYMLINK_NOFOLLOW)
		if !errors.Is(err, unix.ENOENT) {
			if err == nil {
				err = ErrLoginLinkChanged
			}
			return r, err
		}
		present := false
		now := time.Now().UTC()
		r.LinkStatus, r.ChangeStatus, r.LinkPresent, r.LinkObservedAt = "absent", "not_needed", &present, &now
		return r, g.check(ctx)
	}
	link, err := dir.openLink(ctx, r.LinkTarget)
	if err != nil {
		return loginLinkFailure(r, err)
	}
	if link != nil {
		defer link.Close()
	}
	if err = dir.observe(ctx, link, r.LinkTarget, &r); err != nil {
		return loginLinkFailure(r, err)
	}
	if (action == "enable-login" && link != nil) || (action == "disable-login" && link == nil) {
		r.ChangeStatus = "not_needed"
		return r, nil
	}
	if hooks.beforeChange != nil {
		hooks.beforeChange()
	}
	if err = dir.observe(ctx, link, r.LinkTarget, &r); err != nil {
		return loginLinkFailure(r, err)
	}
	// The immediately preceding manager query and exact metadata fences are
	// sequential observations, not namespace authentication or atomic binding.
	r.ChangeAttempted, r.ChangeStatus, r.LinkStatus = true, "unknown", "unexamined"
	if action == "enable-login" {
		symlink := unix.Symlinkat
		if hooks.symlink != nil {
			symlink = hooks.symlink
		}
		err = symlink(r.LinkTarget, int(dir.file.Fd()), d.Filename)
	} else {
		unlink := func(fd int, name string) error { return unix.Unlinkat(fd, name, 0) }
		if hooks.unlink != nil {
			unlink = hooks.unlink
		}
		err = unlink(int(dir.file.Fd()), d.Filename)
	}
	r.ChangeCompleted = err == nil
	if hooks.afterChange != nil {
		hooks.afterChange()
	}
	checkedErr := dir.observeChanged(ctx, link, r.LinkTarget, action, &r)
	if err != nil || checkedErr != nil {
		return loginLinkFailure(r, errors.Join(err, checkedErr, ctx.Err()))
	}
	syncParent := func(f *os.File) error { return f.Sync() }
	if hooks.syncParent != nil {
		syncParent = hooks.syncParent
	}
	if err = ctx.Err(); err != nil {
		return loginLinkFailure(r, err)
	}
	if err = syncParent(dir.file); err != nil {
		return loginLinkFailure(r, errors.Join(err, ctx.Err()))
	}
	r.SyncCompleted = true
	if err = bus.checkOwner(ctx, r.ManagerUniqueName); err != nil {
		return loginLinkFailure(r, err)
	}
	if err = dir.observeChanged(ctx, link, r.LinkTarget, action, &r); err != nil {
		return loginLinkFailure(r, err)
	}
	return r, nil
}

func loginLinkFailure(r LoginLinkResult, err error) (LoginLinkResult, error) {
	if !r.ChangeAttempted && len(r.DirectoriesCreated) == 0 {
		return r, err
	}
	return r, fmt.Errorf("%w: link %q targeting %q, descriptor SHA-256 %s: %w", ErrLoginLinkOutcome, r.LinkPath, r.LinkTarget, r.DescriptorSHA256, err)
}

type loginDirectory struct {
	file             *os.File
	stamp            unix.Stat_t
	guard            *runtimeGuard
	initialLinkStamp *unix.Stat_t
}

func (d *loginDirectory) close() { _ = d.file.Close() }

func openLoginDirectory(ctx context.Context, guard *runtimeGuard, create bool, r *LoginLinkResult, syncParent func(*os.File) error) (*loginDirectory, error) {
	if err := guard.check(ctx); err != nil {
		return nil, err
	}
	parent := guard.chain.last()
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
	fd, err := unix.Openat(int(parent.Fd()), loginWantsDirectory, flags, 0)
	if create && errors.Is(err, unix.ENOENT) {
		if err = unix.Mkdirat(int(parent.Fd()), loginWantsDirectory, 0700); err == nil {
			r.DirectoriesCreated = append(r.DirectoriesCreated, filepath.Dir(r.LinkPath))
			if syncParent == nil {
				syncParent = func(f *os.File) error { return f.Sync() }
			}
			if err = syncParent(parent); err != nil {
				return nil, err
			}
			r.DirectorySyncCompleted = true
		} else if !errors.Is(err, unix.EEXIST) {
			return nil, err
		}
		fd, err = unix.Openat(int(parent.Fd()), loginWantsDirectory, flags, 0)
	}
	if !create && errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Join(ErrLoginLinkConflict, err)
	}
	d := &loginDirectory{file: os.NewFile(uintptr(fd), filepath.Dir(r.LinkPath)), guard: guard}
	if err = unix.Fstat(fd, &d.stamp); err == nil {
		err = d.check(ctx)
	}
	if err != nil {
		d.close()
		return nil, err
	}
	return d, nil
}

func (d *loginDirectory) check(ctx context.Context) error {
	if err := d.guard.check(ctx); err != nil {
		return err
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(int(d.file.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(d.guard.chain.last().Fd()), loginWantsDirectory, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if held.Mode&unix.S_IFMT != unix.S_IFDIR || held.Uid != uint32(os.Geteuid()) || held.Mode&0022 != 0 || !sameServiceIdentity(d.stamp, held) || !sameServiceIdentity(held, named) || held.Mode != d.stamp.Mode || held.Uid != d.stamp.Uid || held.Mode != named.Mode || held.Uid != named.Uid {
		return ErrLoginLinkChanged
	}
	return ctx.Err()
}

func (d *loginDirectory) openLink(ctx context.Context, target string) (*os.File, error) {
	if err := d.check(ctx); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(d.file.Fd()), d.guard.d.Filename, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(d.file.Name(), d.guard.d.Filename))
	stamp, err := d.linkStamp(ctx, file, target)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if d.initialLinkStamp == nil {
		d.initialLinkStamp = &stamp
	}
	return file, nil
}

func (d *loginDirectory) linkStamp(ctx context.Context, file *os.File, target string) (unix.Stat_t, error) {
	if err := d.check(ctx); err != nil {
		return unix.Stat_t{}, err
	}
	var before, after, named unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &before); err != nil {
		return before, err
	}
	if d.initialLinkStamp != nil && !sameServiceStamp(*d.initialLinkStamp, before) {
		return before, ErrLoginLinkChanged
	}
	if before.Mode&unix.S_IFMT != unix.S_IFLNK || before.Uid != uint32(os.Geteuid()) || before.Nlink != 1 || before.Size != int64(len(target)) {
		return before, ErrLoginLinkConflict
	}
	buffer := make([]byte, MaxPathBytes+1)
	n, err := unix.Readlinkat(int(d.file.Fd()), d.guard.d.Filename, buffer)
	if err != nil {
		return before, err
	}
	if n > MaxPathBytes || string(buffer[:n]) != target {
		return before, ErrLoginLinkConflict
	}
	if err = unix.Fstat(int(file.Fd()), &after); err != nil {
		return before, err
	}
	if err = unix.Fstatat(int(d.file.Fd()), d.guard.d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return before, err
	}
	if !sameServiceStamp(before, after) || !sameServiceStamp(after, named) {
		return before, ErrLoginLinkChanged
	}
	return before, ctx.Err()
}

func (d *loginDirectory) observe(ctx context.Context, file *os.File, target string, r *LoginLinkResult) error {
	if err := d.check(ctx); err != nil {
		return err
	}
	present := file != nil
	if present {
		if _, err := d.linkStamp(ctx, file, target); err != nil {
			return err
		}
	} else {
		var named unix.Stat_t
		if err := unix.Fstatat(int(d.file.Fd()), d.guard.d.Filename, &named, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
			if err == nil {
				return ErrLoginLinkChanged
			}
			return err
		}
	}
	now := time.Now().UTC()
	r.LinkPresent, r.LinkObservedAt = &present, &now
	if present {
		r.LinkStatus = "exact"
	} else {
		r.LinkStatus = "absent"
	}
	return d.check(ctx)
}

func (d *loginDirectory) observeChanged(ctx context.Context, original *os.File, target, action string, r *LoginLinkResult) error {
	if action == "enable-login" {
		file, err := d.openLink(ctx, target)
		if err != nil {
			return err
		}
		if file == nil {
			return ErrLoginLinkChanged
		}
		defer file.Close()
		if err = d.observe(ctx, file, target, r); err != nil {
			return err
		}
	} else {
		if err := d.check(ctx); err != nil {
			return err
		}
		var held unix.Stat_t
		if err := unix.Fstat(int(original.Fd()), &held); err != nil {
			return err
		}
		before := d.initialLinkStamp
		if before == nil || !sameServiceIdentity(*before, held) || before.Mode != held.Mode || before.Uid != held.Uid || before.Size != held.Size || before.Mtim != held.Mtim {
			return ErrLoginLinkChanged
		}
		removed := held.Nlink == 0
		r.RemovalObserved = &removed
		if err := d.observe(ctx, nil, target, r); err != nil {
			return err
		}
		if !removed {
			return ErrLoginLinkChanged
		}
	}
	r.ChangeStatus = "changed"
	return ctx.Err()
}
