//go:build linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func loginLinkFixture(t *testing.T) (InstallSpec, *runtimeFakeManager, loginLinkHooks) {
	t.Helper()
	spec := runtimeFixture(t, "linux")
	manager := newRuntimeFakeManager(t, spec)
	return spec, manager, loginLinkHooks{manager: runtimeTestHooks(manager.run)}
}

func assertLoginUnknown(t *testing.T, r LoginLinkResult) {
	t.Helper()
	if r.Enabled != nil || r.Running != nil || r.Stopped != nil || r.FutureLoginMayStart != nil || r.EffectiveEnablementVerified || r.RuntimeStateVerified || r.LoadedOriginVerified || r.LinkOriginVerified || r.ManagerEnablementRequested || r.StartRequested || r.StopRequested || r.ReloadRequested || r.ScanningRequested || r.DescriptorChanged || r.OtherLinksChanged || r.DataRemoved || r.ExecutableRemoved {
		t.Fatal("fixed dependency link widened effects or invented authority", r)
	}
}

func TestServiceLoginLinkLifecyclePreservesOtherManualLinksAndData(t *testing.T) {
	spec, manager, hooks := loginLinkFixture(t)
	filename := ManagedLabel + ".service"
	name := filepath.Join(spec.Directory, loginWantsDirectory, filename)
	target := filepath.Join(spec.Directory, filename)
	kept := []string{target, filepath.Join(spec.Directory, "."+ManagedLabel+".lock"), spec.Service.Executable, spec.Service.Paths.ConfigFile}
	before := make(map[string]os.FileInfo)
	contents := make(map[string][]byte)
	for _, path := range kept {
		var err error
		before[path], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		contents[path], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err := controlLoginLink(context.Background(), spec, "enable-login", hooks)
	if err != nil || r.Contract != LoginLinkContract || r.Scope != "selected_default_target_dependency" || r.ChangeStatus != "changed" || !r.ChangeAttempted || !r.ChangeCompleted || !r.SyncCompleted || !r.DirectorySyncCompleted || len(r.DirectoriesCreated) != 1 || r.LinkStatus != "exact" || r.LinkPresent == nil || !*r.LinkPresent || r.LinkObservedAt == nil || !r.UnitLoadAttempted || r.LoadedBindingMatched == nil || !*r.LoadedBindingMatched {
		t.Fatal("exact link creation lacks scoped evidence", r, err)
	}
	assertLoginUnknown(t, r)
	if !reflect.DeepEqual(manager.methods, []string{"GetNameOwner", "Get", "LoadUnit", "GetAll", "GetAll", "GetNameOwner", "GetNameOwner"}) {
		t.Fatal("manager mutation/reload or unbounded queries introduced", manager.methods)
	}
	if actual, err := os.Readlink(name); err != nil || actual != target || r.LinkPath != name || r.LinkTarget != target {
		t.Fatal("literal selected link changed", actual, err, r)
	}
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	manager.methods = nil
	retry, err := controlLoginLink(context.Background(), spec, "enable-login", hooks)
	if err != nil || retry.ChangeStatus != "not_needed" || retry.ChangeAttempted || retry.ChangeCompleted || retry.SyncCompleted || retry.DirectorySyncCompleted || len(retry.DirectoriesCreated) != 0 || retry.LinkOriginVerified {
		t.Fatal("exact retry replaced or adopted the link", retry, err)
	}
	retried, err := os.Lstat(name)
	if err != nil || !os.SameFile(info, retried) || !info.ModTime().Equal(retried.ModTime()) {
		t.Fatal("retry replaced the original symlink", err)
	}
	otherTarget := filepath.Join(spec.Directory, "other.target.wants")
	if err = os.Mkdir(otherTarget, 0700); err != nil {
		t.Fatal(err)
	}
	otherLinks := []string{filepath.Join(otherTarget, filename), filepath.Join(filepath.Dir(name), "manual-other.service")}
	otherInfos := map[string]os.FileInfo{}
	for _, path := range otherLinks {
		if err = os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		otherInfos[path], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	manager.methods = nil
	r, err = controlLoginLink(context.Background(), spec, "disable-login", hooks)
	if err != nil || r.ChangeStatus != "changed" || !r.ChangeAttempted || !r.ChangeCompleted || !r.SyncCompleted || r.LinkStatus != "absent" || r.LinkPresent == nil || *r.LinkPresent || r.RemovalObserved == nil || !*r.RemovalObserved || len(r.DirectoriesCreated) != 0 {
		t.Fatal("exact disable lost unlink evidence", r, err)
	}
	assertLoginUnknown(t, r)
	for _, path := range otherLinks {
		after, err := os.Lstat(path)
		actual, linkErr := os.Readlink(path)
		if err != nil || linkErr != nil || !os.SameFile(otherInfos[path], after) || actual != target {
			t.Fatal("disable touched another manual link", path, err, linkErr)
		}
	}
	if _, err = os.Lstat(filepath.Dir(name)); err != nil {
		t.Fatal("disable removed the wants directory", err)
	}
	r, err = controlLoginLink(context.Background(), spec, "disable-login", hooks)
	if err != nil || r.ChangeStatus != "not_needed" || r.ChangeAttempted || r.ChangeCompleted || r.SyncCompleted || r.RemovalObserved != nil || r.LinkPresent == nil || *r.LinkPresent {
		t.Fatal("absent disable fabricated an action", r, err)
	}
	for _, path := range kept {
		after, err := os.Lstat(path)
		actual, readErr := os.ReadFile(path)
		if err != nil || readErr != nil || !os.SameFile(before[path], after) || !before[path].ModTime().Equal(after.ModTime()) || !reflect.DeepEqual(contents[path], actual) {
			t.Fatal("link controls changed descriptor/data/executable/lock", path, err, readErr)
		}
	}
	encoded, _ := json.Marshal(r)
	if strings.Contains(string(encoded), "discard-private-manager-canary") || strings.Contains(string(encoded), "activation_performed") {
		t.Fatal("unrelated/private manager data or stale actions exposed", string(encoded))
	}
	var fields map[string]any
	if err = json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"enabled", "running", "stopped", "future_login_may_start"} {
		value, ok := fields[key]
		if !ok || value != nil {
			t.Fatal("unknown state became omitted/false", key, string(encoded))
		}
	}
}

func TestServiceLoginLinkMissingAndForeignScopesRefuseWithoutInitialization(t *testing.T) {
	for _, kind := range []string{"missing wants", "missing descriptor", "missing lock", "foreign target", "relative target", "regular file", "directory", "FIFO", "hardlink", "symlink wants", "wide wants", "busy", "foreign loaded", "owner changed"} {
		t.Run(kind, func(t *testing.T) {
			spec, manager, hooks := loginLinkFixture(t)
			wants := filepath.Join(spec.Directory, loginWantsDirectory)
			name := filepath.Join(wants, ManagedLabel+".service")
			target := filepath.Join(spec.Directory, ManagedLabel+".service")
			lock := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
			if kind != "missing wants" && kind != "symlink wants" {
				if err := os.Mkdir(wants, 0700); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch kind {
			case "missing descriptor":
				err = os.Remove(target)
			case "missing lock":
				err = os.Remove(lock)
			case "foreign target":
				err = os.Symlink(spec.Service.Executable, name)
			case "relative target":
				err = os.Symlink("../"+ManagedLabel+".service", name)
			case "regular file":
				err = os.WriteFile(name, []byte("foreign"), 0600)
			case "directory":
				err = os.Mkdir(name, 0700)
			case "FIFO":
				err = unix.Mkfifo(name, 0600)
			case "hardlink":
				if err = os.Symlink(target, name); err == nil {
					err = os.Link(name, name+"-retained")
				}
			case "symlink wants":
				err = os.Symlink(spec.Service.Paths.StateDir, wants)
			case "wide wants":
				err = os.Chmod(wants, 0777)
			case "busy":
				file, openErr := os.OpenFile(lock, os.O_RDWR, 0)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer file.Close()
				err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			case "foreign loaded":
				manager.unit["FragmentPath"] = map[string]any{"type": "s", "data": "/foreign/managed.service"}
			case "owner changed":
				manager.ownerChanged = true
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(name)
			r, err := controlLoginLink(context.Background(), spec, "disable-login", hooks)
			if kind == "missing wants" {
				if err != nil || r.ChangeStatus != "not_needed" || r.ChangeAttempted || r.SyncCompleted || len(r.DirectoriesCreated) != 0 || r.LinkPresent == nil || *r.LinkPresent {
					t.Fatal("absent directory disable initialized scope", r, err)
				}
				if _, err = os.Lstat(wants); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("disable initialized wants directory", err)
				}
				return
			}
			if err == nil || r.ChangeAttempted || r.ChangeCompleted || r.SyncCompleted || len(r.DirectoriesCreated) != 0 {
				t.Fatal("unsafe scope reached mutation", r, err)
			}
			assertLoginUnknown(t, r)
			if before != nil {
				after, statErr := os.Lstat(name)
				if statErr != nil || !os.SameFile(before, after) {
					t.Fatal("refusal removed the foreign object", statErr)
				}
			}
			if kind == "missing descriptor" || kind == "missing lock" || kind == "busy" {
				if len(manager.methods) != 0 {
					t.Fatal("unsafe artifact scope reached manager", manager.methods)
				}
			}
		})
	}
}

func TestServiceLoginLinkPrechangeFencesAndPartialOutcomes(t *testing.T) {
	for _, mode := range []string{"enable-login", "disable-login"} {
		for _, kind := range []string{"cancel before", "link replacement", "descriptor replacement", "wants replacement", "owner prequery replacement", "syscall failure", "executed then error", "cancel after", "sync failure", "replacement after", "cancel after sync", "owner after"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				spec, manager, hooks := loginLinkFixture(t)
				wants := filepath.Join(spec.Directory, loginWantsDirectory)
				if err := os.Mkdir(wants, 0700); err != nil {
					t.Fatal(err)
				}
				name := filepath.Join(wants, ManagedLabel+".service")
				target := filepath.Join(spec.Directory, ManagedLabel+".service")
				if mode == "disable-login" {
					// Explicit disable is authorized for this matching fixed link
					// even though the generated owner, not EnableLoginLink, made it.
					if err := os.Symlink(target, name); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				replaceLink := func() {
					if mode == "disable-login" {
						if err := os.Rename(name, name+"-retained"); err != nil {
							t.Fatal(err)
						}
					}
					if err := os.Symlink("/foreign/target", name); err != nil {
						t.Fatal(err)
					}
				}
				before := strings.HasPrefix(kind, "cancel before") || kind == "link replacement" || kind == "descriptor replacement" || kind == "wants replacement" || kind == "owner prequery replacement"
				switch kind {
				case "cancel before":
					hooks.beforeChange = cancel
				case "link replacement":
					hooks.beforeChange = replaceLink
				case "descriptor replacement", "owner prequery replacement":
					replace := func() {
						if err := os.Rename(target, target+"-retained"); err != nil {
							t.Fatal(err)
						}
						d, _ := Build(spec.Service)
						if err := os.WriteFile(target, []byte(d.Content), 0600); err != nil {
							t.Fatal(err)
						}
					}
					if kind == "owner prequery replacement" {
						manager.onOwnerCheck = replace
					} else {
						hooks.beforeChange = replace
					}
				case "wants replacement":
					hooks.beforeChange = func() {
						if err := os.Rename(wants, wants+"-retained"); err != nil {
							t.Fatal(err)
						}
						if err := os.Mkdir(wants, 0700); err != nil {
							t.Fatal(err)
						}
					}
				case "syscall failure", "executed then error":
					hooks.symlink = func(target string, fd int, name string) error {
						if kind == "executed then error" {
							if err := unix.Symlinkat(target, fd, name); err != nil {
								return err
							}
						}
						return unix.EIO
					}
					hooks.unlink = func(fd int, name string) error {
						if kind == "executed then error" {
							if err := unix.Unlinkat(fd, name, 0); err != nil {
								return err
							}
						}
						return unix.EIO
					}
				case "cancel after":
					hooks.afterChange = cancel
				case "sync failure":
					hooks.syncParent = func(*os.File) error { return unix.EIO }
				case "replacement after":
					hooks.afterChange = func() {
						if mode == "enable-login" {
							if err := os.Remove(name); err != nil {
								t.Fatal(err)
							}
						}
						if err := os.Symlink("/foreign/replacement", name); err != nil {
							t.Fatal(err)
						}
					}
				case "cancel after sync":
					hooks.syncParent = func(f *os.File) error { err := f.Sync(); cancel(); return err }
				case "owner after":
					hooks.afterChange = func() { manager.ownerChanged = true }
				}
				r, err := controlLoginLink(ctx, spec, mode, hooks)
				if err == nil {
					t.Fatal("mutation/refusal seam succeeded", r)
				}
				assertLoginUnknown(t, r)
				if before {
					if r.ChangeAttempted || r.ChangeCompleted || errors.Is(err, ErrLoginLinkOutcome) {
						t.Fatal("preflight failure claimed mutation", r, err)
					}
				} else if !r.ChangeAttempted || !errors.Is(err, ErrLoginLinkOutcome) || r.LinkPath != name || r.DescriptorSHA256 == "" || !strings.Contains(err.Error(), r.DescriptorSHA256) {
					t.Fatal("partial outcome lost exact inspection scope", r, err)
				}
				if strings.HasPrefix(kind, "cancel") && !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation identity lost", err)
				}
				if kind == "executed then error" {
					if r.ChangeCompleted || r.ChangeStatus != "changed" || r.LinkPresent == nil || *r.LinkPresent != (mode == "enable-login") {
						t.Fatal("checked result was confused with syscall success", r)
					}
				}
				if kind == "sync failure" || kind == "cancel after sync" || kind == "owner after" {
					if !r.ChangeCompleted || r.ChangeStatus != "changed" || r.LinkPresent == nil || *r.LinkPresent != (mode == "enable-login") {
						t.Fatal("known link observation discarded after later failure", r)
					}
				}
				if kind == "cancel after sync" || kind == "owner after" {
					if !r.SyncCompleted {
						t.Fatal("completed sync observation lost", r)
					}
				}
				if kind == "replacement after" {
					if !r.ChangeCompleted {
						t.Fatal("replacement erased completed syscall", r)
					}
					actual, linkErr := os.Readlink(name)
					if linkErr != nil || actual != "/foreign/replacement" {
						t.Fatal("replacement was removed or followed", actual, linkErr)
					}
				}
			})
		}
	}
}

func TestServiceLoginLinkCreatedDirectoryAndOriginalDeadlineEvidence(t *testing.T) {
	spec, _, hooks := loginLinkFixture(t)
	hooks.syncParent = func(*os.File) error { return unix.EIO }
	r, err := controlLoginLink(context.Background(), spec, "enable-login", hooks)
	if !errors.Is(err, ErrLoginLinkOutcome) || !errors.Is(err, unix.EIO) || len(r.DirectoriesCreated) != 1 || r.DirectorySyncCompleted || r.ChangeAttempted || r.ChangeCompleted {
		t.Fatal("created directory effect was lost or labeled a link mutation", r, err)
	}
	if _, err = os.Lstat(filepath.Dir(r.LinkPath)); err != nil {
		t.Fatal("partial effect directory disappeared", err)
	}
	if _, err = os.Lstat(r.LinkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed directory sync created a link", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	hooks.syncParent = nil
	hooks.beforeChange = func() { <-ctx.Done() }
	r, err = controlLoginLink(ctx, spec, "enable-login", hooks)
	if !errors.Is(err, context.DeadlineExceeded) || r.ChangeAttempted || time.Now().After(deadline.Add(500*time.Millisecond)) {
		t.Fatal("link controls renewed the original deadline", r, err)
	}
}

func TestServiceLoginLinkManagerFailureKeepsUnobservedStateAndNoLinkEffects(t *testing.T) {
	for _, kind := range []string{"missing client", "canceled resolution", "owner reply failure"} {
		t.Run(kind, func(t *testing.T) {
			spec, _, hooks := loginLinkFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "owner reply failure" {
				hooks.manager.run = func(context.Context, string, []string) (runtimeReply, error) {
					return runtimeReply{started: true}, ErrManagerProtocol
				}
			} else {
				hooks.manager.resolve = func(string) (string, error) {
					if kind == "canceled resolution" {
						cancel()
					}
					return "", os.ErrNotExist
				}
			}
			r, err := controlLoginLink(ctx, spec, "enable-login", hooks)
			if err == nil || r.Manager.Status != "not_checked" || r.ManagerUniqueName != "" || r.LoadedBindingMatched != nil || r.UnitLoadAttempted || r.ChangeAttempted || len(r.DirectoriesCreated) != 0 || r.LinkPresent != nil || r.LinkObservedAt != nil {
				t.Fatal("unavailable manager fabricated observations or link effects", r, err)
			}
			if kind == "canceled resolution" && !errors.Is(err, context.Canceled) {
				t.Fatal("resolution cancellation identity lost", err)
			}
			if _, err = os.Lstat(filepath.Dir(r.LinkPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed manager preflight initialized wants directory", err)
			}
		})
	}
}
