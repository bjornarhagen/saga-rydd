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

func removalFixture(t *testing.T, platform string) InstallSpec {
	t.Helper()
	spec, hooks := serviceInstallFixture(t, platform)
	if _, err := lifecycle(context.Background(), spec, true, hooks); err != nil {
		t.Fatal(err)
	}
	// Removal must work without a selected manager endpoint or an executable.
	// The executable remains an exact name in the descriptor, not a body input.
	spec.BusSocket = "unsupported-and-unused-manager-address"
	return spec
}

func assertRemovalAuthorityUnknown(t *testing.T, r RemovalResult) {
	t.Helper()
	if r.Running != nil || r.Stopped != nil || r.FutureLoginMayStart != nil || r.RuntimeStateVerified || r.LoadedOriginVerified || r.ManagerRequestAttempted || r.StopRequested || r.EnablementChanged || r.ReloadRequested || r.ScanningRequested || r.DataRemoved || r.ExecutableRemoved || r.DirectoriesRemoved || r.LockRemoved {
		t.Fatal("descriptor removal widened its scope or invented runtime evidence", r)
	}
}

func TestServiceRemovalExactAndAbsentRetryPreserveOtherObjects(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			spec := removalFixture(t, platform)
			t.Setenv("PATH", "")
			data := spec.Service.Paths.StateDir
			inventory := filepath.Join(data, "inventory.db")
			quarantine := filepath.Join(data, "quarantine")
			if err := os.Mkdir(quarantine, 0700); err != nil {
				t.Fatal(err)
			}
			restored := filepath.Join(quarantine, "restore-record")
			for _, name := range []string{inventory, restored} {
				if err := os.WriteFile(name, []byte("generated preserved sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			keptFiles := []string{spec.Service.Executable, spec.Service.Paths.ConfigFile, inventory, restored, filepath.Join(spec.Directory, "."+ManagedLabel+".lock")}
			keptDirs := []string{spec.HomeDir, filepath.Dir(spec.Directory), spec.Directory, data, quarantine}
			infos := map[string]os.FileInfo{}
			contents := map[string][]byte{}
			for _, name := range append(append([]string{}, keptFiles...), keptDirs...) {
				info, err := os.Lstat(name)
				if err != nil {
					t.Fatal(err)
				}
				infos[name] = info
				if !info.IsDir() {
					contents[name], err = os.ReadFile(name)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := removeDescriptor(context.Background(), spec, removalHooks{})
			if err != nil || r.Contract != RemovalContract || r.Action != "uninstall" || r.ArtifactStatus != "absent" || r.RemovalStatus != "removed" || !r.RemovalAttempted || !r.UnlinkCompleted || r.RemovalObserved == nil || !*r.RemovalObserved || r.DescriptorAbsent == nil || !*r.DescriptorAbsent || r.ObservationAt == nil || !r.SyncCompleted {
				t.Fatal("exact descriptor removal lacks bound evidence", r, err)
			}
			assertRemovalAuthorityUnknown(t, r)
			if r.DescriptorSHA256 == "" || r.DescriptorPath != filepath.Join(spec.Directory, ManagedLabel+map[string]string{"darwin": ".plist", "linux": ".service"}[platform]) || r.ConfigFile != spec.Service.Paths.ConfigFile || r.StateDir != data || r.Executable != spec.Service.Executable || r.RuntimeDir != spec.Service.RuntimeDir {
				t.Fatal("removal changed the frozen specification", r)
			}
			if _, err = os.Lstat(r.DescriptorPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("descriptor remains", err)
			}
			for name, before := range infos {
				after, err := os.Lstat(name)
				if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
					t.Fatal("removal changed a retained object", name, err)
				}
				if !before.IsDir() {
					actual, err := os.ReadFile(name)
					if err != nil || !reflect.DeepEqual(contents[name], actual) || !before.ModTime().Equal(after.ModTime()) {
						t.Fatal("removal changed retained file bytes", name, err)
					}
				}
			}
			retry, err := removeDescriptor(context.Background(), spec, removalHooks{unlink: func(int, string) error { t.Fatal("absent retry attempted unlink"); return nil }})
			if err != nil || retry.ArtifactStatus != "absent" || retry.RemovalStatus != "not_needed" || retry.RemovalAttempted || retry.UnlinkCompleted || retry.SyncCompleted || retry.RemovalObserved == nil || *retry.RemovalObserved || retry.DescriptorAbsent == nil || !*retry.DescriptorAbsent || retry.ObservationAt == nil || retry.DescriptorSHA256 != r.DescriptorSHA256 {
				t.Fatal("absent retry fabricated a removal", retry, err)
			}
			assertRemovalAuthorityUnknown(t, retry)
			encoded, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err = json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"running", "stopped", "future_login_may_start"} {
				v, ok := fields[name]
				if !ok || v != nil {
					t.Fatal("unknown state was omitted or converted to false", name, string(encoded))
				}
			}
			for _, name := range []string{"installation_performed", "activation_performed", "scanning_enabled"} {
				if _, ok := fields[name]; ok {
					t.Fatal("removal embeds stale descriptor action claims", name)
				}
			}
		})
	}
}

func TestServiceRemovalRequiresExistingDirectoryLockAndExactArtifact(t *testing.T) {
	for _, kind := range []string{"missing directory", "missing lock", "absent and missing lock", "different bytes", "symlink", "hardlink", "directory", "FIFO", "wide permissions", "oversize", "symlink lock", "hardlink lock", "wide lock", "busy"} {
		t.Run(kind, func(t *testing.T) {
			spec := removalFixture(t, "darwin")
			d, _ := Build(spec.Service)
			name := filepath.Join(spec.Directory, d.Filename)
			lockName := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
			var held *os.File
			var err error
			switch kind {
			case "missing directory":
				err = os.Rename(spec.Directory, spec.Directory+"-retained")
			case "missing lock", "absent and missing lock":
				err = os.Remove(lockName)
				if err == nil && kind == "absent and missing lock" {
					err = os.Remove(name)
				}
			case "different bytes":
				err = os.WriteFile(name, []byte("foreign descriptor"), 0600)
			case "wide permissions":
				err = os.Chmod(name, 0644)
			case "oversize":
				err = os.WriteFile(name, []byte(strings.Repeat("x", MaxDescriptorBytes+1)), 0600)
			case "symlink", "hardlink", "directory", "FIFO":
				if err = os.Remove(name); err == nil {
					switch kind {
					case "symlink":
						err = os.Symlink(spec.Service.Executable, name)
					case "hardlink":
						err = os.Link(spec.Service.Executable, name)
					case "directory":
						err = os.Mkdir(name, 0700)
					case "FIFO":
						err = unix.Mkfifo(name, 0600)
					}
				}
			case "symlink lock", "hardlink lock":
				if err = os.Remove(lockName); err == nil {
					if kind == "symlink lock" {
						err = os.Symlink(spec.Service.Executable, lockName)
					} else {
						err = os.Link(spec.Service.Executable, lockName)
					}
				}
			case "wide lock":
				err = os.Chmod(lockName, 0644)
			case "busy":
				held, err = os.OpenFile(lockName, os.O_RDWR, 0)
				if err == nil {
					defer held.Close()
					err = unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(name)
			started := time.Now()
			r, err := removeDescriptor(context.Background(), spec, removalHooks{unlink: func(int, string) error { t.Fatal("refusal reached unlink"); return nil }})
			if err == nil || r.RemovalAttempted || r.UnlinkCompleted || r.RemovalObserved != nil || r.DescriptorAbsent != nil || r.SyncCompleted || r.RemovalStatus != "not_requested" || time.Since(started) > time.Second {
				t.Fatal("unsafe/missing scope reached removal or blocked", r, err)
			}
			assertRemovalAuthorityUnknown(t, r)
			if strings.Contains(kind, "missing") {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing scope lost its error identity", err)
				}
			} else if kind == "busy" && !errors.Is(err, ErrArtifactBusy) {
				t.Fatal("busy scope lost its error identity", err)
			}
			if before != nil {
				after, statErr := os.Lstat(name)
				if statErr != nil || !os.SameFile(before, after) {
					t.Fatal("refusal removed/replaced the named object", statErr)
				}
			}
			if strings.Contains(kind, "missing lock") {
				if _, statErr := os.Lstat(lockName); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatal("refusal initialized lock", statErr)
				}
			}
			if kind == "missing directory" {
				if _, statErr := os.Lstat(spec.Directory); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatal("refusal initialized directory", statErr)
				}
			}
		})
	}
}

func TestServiceRemovalRevalidatesBeforeUnlink(t *testing.T) {
	for _, kind := range []string{"descriptor replacement", "same file changed", "lock replacement", "directory replacement", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			spec := removalFixture(t, "darwin")
			d, _ := Build(spec.Service)
			name := filepath.Join(spec.Directory, d.Filename)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := removalHooks{beforeUnlink: func() {
				var err error
				switch kind {
				case "descriptor replacement":
					if err = os.Rename(name, name+"-retained"); err == nil {
						err = os.WriteFile(name, []byte(d.Content), 0600)
					}
				case "same file changed":
					info, statErr := os.Stat(name)
					if statErr != nil {
						t.Fatal(statErr)
					}
					if err = os.WriteFile(name, []byte(strings.Repeat("x", len(d.Content))), 0600); err == nil {
						err = os.Chtimes(name, info.ModTime(), info.ModTime())
					}
				case "lock replacement":
					lock := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
					if err = os.Rename(lock, lock+"-retained"); err == nil {
						err = os.WriteFile(lock, nil, 0600)
					}
				case "directory replacement":
					if err = os.Rename(spec.Directory, spec.Directory+"-retained"); err == nil {
						err = os.Mkdir(spec.Directory, 0700)
					}
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
			}, unlink: func(int, string) error { t.Fatal("changed scope reached unlink"); return nil }}
			r, err := removeDescriptor(ctx, spec, hooks)
			if err == nil || r.RemovalAttempted || r.UnlinkCompleted || r.RemovalObserved != nil || r.SyncCompleted {
				t.Fatal("changed scope was removed", r, err)
			}
			if kind == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation identity lost", err)
			}
			if kind == "directory replacement" {
				name = filepath.Join(spec.Directory+"-retained", d.Filename)
			}
			if _, err = os.Lstat(name); err != nil {
				t.Fatal("selected or replacement descriptor was lost", err)
			}
		})
	}
}

func TestServiceRemovalPartialOutcomesRetainSeparateEvidence(t *testing.T) {
	for _, kind := range []string{"unlink failure", "unlink executed then error", "sync failure", "cancel after unlink", "cancel after sync", "replacement after unlink", "replacement after sync"} {
		t.Run(kind, func(t *testing.T) {
			spec := removalFixture(t, "darwin")
			d, _ := Build(spec.Service)
			name := filepath.Join(spec.Directory, d.Filename)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := removalHooks{}
			switch kind {
			case "unlink failure":
				hooks.unlink = func(int, string) error { return unix.EIO }
			case "unlink executed then error":
				hooks.unlink = func(fd int, name string) error {
					if err := unix.Unlinkat(fd, name, 0); err != nil {
						return err
					}
					return unix.EIO
				}
			case "sync failure":
				hooks.syncParent = func(*os.File) error { return unix.EIO }
			case "cancel after unlink":
				hooks.afterUnlink = cancel
			case "cancel after sync":
				hooks.syncParent = func(f *os.File) error { err := f.Sync(); cancel(); return err }
			case "replacement after unlink":
				hooks.afterUnlink = func() {
					if err := os.WriteFile(name, []byte("foreign replacement"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "replacement after sync":
				hooks.syncParent = func(f *os.File) error {
					if err := f.Sync(); err != nil {
						return err
					}
					return os.WriteFile(name, []byte("foreign replacement"), 0600)
				}
			}
			r, err := removeDescriptor(ctx, spec, hooks)
			if !errors.Is(err, ErrArtifactRemoval) || !r.RemovalAttempted || r.DescriptorPath != name || r.DescriptorSHA256 == "" || !strings.Contains(err.Error(), r.DescriptorSHA256) || !strings.Contains(err.Error(), "descriptor") {
				t.Fatal("partial removal lost exact inspection scope", r, err)
			}
			assertRemovalAuthorityUnknown(t, r)
			if strings.HasPrefix(kind, "cancel") && !errors.Is(err, context.Canceled) {
				t.Fatal("late cancellation identity lost", err)
			}
			if (strings.Contains(kind, "failure") || kind == "unlink executed then error") && !errors.Is(err, unix.EIO) {
				t.Fatal("operation failure identity lost", err)
			}
			wantUnlink := kind != "unlink failure" && kind != "unlink executed then error"
			if r.UnlinkCompleted != wantUnlink {
				t.Fatal("syscall evidence was inferred from later absence", r)
			}
			wantSync := kind == "cancel after sync" || kind == "replacement after sync"
			if r.SyncCompleted != wantSync {
				t.Fatal("sync API completion evidence lost or invented", r)
			}
			switch kind {
			case "unlink failure":
				if r.RemovalObserved == nil || *r.RemovalObserved || r.DescriptorAbsent == nil || *r.DescriptorAbsent || r.RemovalStatus != "unknown" {
					t.Fatal("failed unlink was called a removal", r)
				}
			case "cancel after unlink":
				if r.RemovalObserved != nil || r.DescriptorAbsent != nil || r.ObservationAt != nil {
					t.Fatal("canceled postchecks invented observations", r)
				}
			case "replacement after unlink", "replacement after sync":
				if r.RemovalObserved == nil || !*r.RemovalObserved || r.DescriptorAbsent == nil || *r.DescriptorAbsent || r.RemovalStatus != "unknown" || r.ArtifactStatus != "unexamined" {
					t.Fatal("replacement erased selected removal or claimed lasting absence", r)
				}
				actual, readErr := os.ReadFile(name)
				if readErr != nil || string(actual) != "foreign replacement" {
					t.Fatal("post-unlink replacement was touched", readErr)
				}
			default:
				if r.RemovalObserved == nil || !*r.RemovalObserved || r.DescriptorAbsent == nil || !*r.DescriptorAbsent || r.ObservationAt == nil || r.RemovalStatus != "removed" {
					t.Fatal("known checked removal was discarded after error", r)
				}
			}
		})
	}
}

func TestServiceRemovalUsesOriginalDeadlineAndNativePublicScope(t *testing.T) {
	spec := removalFixture(t, "darwin")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	r, err := removeDescriptor(ctx, spec, removalHooks{beforeUnlink: func() { <-ctx.Done() }})
	if !errors.Is(err, context.DeadlineExceeded) || r.RemovalAttempted || time.Now().After(deadline.Add(500*time.Millisecond)) {
		t.Fatal("removal renewed the original operation deadline", r, err)
	}
	wrong := spec
	wrong.Service.GOOS = "linux"
	if runtime.GOOS == "linux" {
		wrong.Service.GOOS = "darwin"
	}
	r, err = Uninstall(context.Background(), wrong)
	if !errors.Is(err, ErrSpec) || r.Contract != "" || r.RemovalAttempted {
		t.Fatal("public removal accepted another native platform", r, err)
	}
	for _, change := range []func(*InstallSpec){
		func(s *InstallSpec) { s.Directory = filepath.Join(s.HomeDir, "unsupported") },
		func(s *InstallSpec) { s.HomeDir += "/../home" },
		func(s *InstallSpec) { s.Service.Executable = "/invalid\nexecutable" },
	} {
		invalid := spec
		change(&invalid)
		r, err = removeDescriptor(context.Background(), invalid, removalHooks{})
		if !errors.Is(err, ErrSpec) || r.Contract != "" {
			t.Fatal("invalid scope opened metadata", r, err)
		}
	}
}
