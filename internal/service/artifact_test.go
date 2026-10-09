//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"golang.org/x/sys/unix"
)

func serviceInstallFixture(t *testing.T, platform string) (InstallSpec, artifactHooks) {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(home, "data $%\"'\\Ω")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(home, "rydd $%Ω")
	if err := os.WriteFile(executable, []byte("generated executable sentinel"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "config.toml"), []byte("generated configuration sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	spec := InstallSpec{Service: Spec{GOOS: platform, Executable: executable, Paths: config.Paths{ConfigFile: filepath.Join(data, "config.toml"), StateDir: data}, RuntimeDir: filepath.Join(home, "runtime")}, HomeDir: home, Directory: filepath.Join(home, "Library", "LaunchAgents")}
	hooks := artifactHooks{}
	if platform == "linux" {
		base := filepath.Join(home, "config $%\"'\\Ω")
		if err := os.Mkdir(base, 0700); err != nil {
			t.Fatal(err)
		}
		spec.Directory = filepath.Join(base, "systemd", "user")
		spec.BusSocket = filepath.Join(home, "bus")
		hooks.manager = func(ctx context.Context, socket string) (ManagerObservation, error) {
			if socket != spec.BusSocket {
				t.Fatal("changed manager socket")
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second {
				t.Fatal("missing shared deadline")
			}
			now := time.Now().UTC()
			return ManagerObservation{Status: "observed", ObservedAt: &now, BusSocket: socket, UnitPath: []string{spec.Directory, "/usr/lib/systemd/user"}}, nil
		}
	}
	return spec, hooks
}

func TestServiceArtifactInstallStatusAndExactRetry(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			spec, hooks := serviceInstallFixture(t, platform)
			beforeConfig, _ := os.ReadFile(spec.Service.Paths.ConfigFile)
			beforeExecutable, _ := os.ReadFile(spec.Service.Executable)
			missing, err := lifecycle(context.Background(), spec, false, hooks)
			if err != nil || missing.ArtifactStatus != "absent" || missing.Publication != "not_requested" || len(missing.DirectoriesCreated) != 0 {
				t.Fatal("missing offline artifact initialized or misreported", missing, err)
			}
			if _, err = os.Stat(spec.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("status created directory", err)
			}
			saved, err := lifecycle(context.Background(), spec, true, hooks)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Contract != LifecycleContract || saved.Publication != "saved" || saved.ArtifactStatus != "exact" || !saved.SyncCompleted || !saved.InstallationPerformed || saved.ActivationPerformed || saved.ScanningEnabled || !saved.LockCreated || len(saved.DirectoriesCreated) != 2 || saved.FutureLoginMayStart != (platform == "darwin") {
				t.Fatal("publication/authority differs", saved)
			}
			data, err := os.ReadFile(saved.DescriptorPath)
			if err != nil || string(data) != saved.Content {
				t.Fatal("published bytes differ", err)
			}
			firstInfo, err := os.Lstat(saved.DescriptorPath)
			if err != nil {
				t.Fatal(err)
			}
			retry, err := lifecycle(context.Background(), spec, true, hooks)
			if err != nil || retry.Publication != "not_needed" || retry.InstallationPerformed || retry.SyncCompleted || retry.LockCreated || len(retry.DirectoriesCreated) != 0 || retry.DescriptorSHA256 != saved.DescriptorSHA256 {
				t.Fatal("exact retry changed publication", retry, err)
			}
			retryInfo, _ := os.Lstat(saved.DescriptorPath)
			if !os.SameFile(firstInfo, retryInfo) || !firstInfo.ModTime().Equal(retryInfo.ModTime()) {
				t.Fatal("retry replaced exact artifact")
			}
			status, err := lifecycle(context.Background(), spec, false, hooks)
			if err != nil || status.ArtifactStatus != "exact" || status.InstallationPerformed || status.Publication != "not_requested" {
				t.Fatal("status claims an operation", status, err)
			}
			afterConfig, _ := os.ReadFile(spec.Service.Paths.ConfigFile)
			afterExecutable, _ := os.ReadFile(spec.Service.Executable)
			if !reflect.DeepEqual(beforeConfig, afterConfig) || !reflect.DeepEqual(beforeExecutable, afterExecutable) {
				t.Fatal("installation changed config or selected executable")
			}
			if _, err = os.Stat(spec.Service.RuntimeDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("installation initialized worker runtime", err)
			}
		})
	}
}

func TestServiceArtifactForeignObjectsAndUnsupportedPlacementRefuse(t *testing.T) {
	for _, kind := range []string{"different bytes", "symlink", "hardlink", "wide permissions", "directory"} {
		t.Run(kind, func(t *testing.T) {
			spec, hooks := serviceInstallFixture(t, "darwin")
			if err := os.MkdirAll(spec.Directory, 0700); err != nil {
				t.Fatal(err)
			}
			descriptor, err := Build(spec.Service)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(spec.Directory, descriptor.Filename)
			switch kind {
			case "different bytes":
				err = os.WriteFile(name, []byte("foreign sentinel"), 0600)
			case "symlink":
				err = os.Symlink(spec.Service.Executable, name)
			case "hardlink":
				err = os.Link(spec.Service.Executable, name)
			case "wide permissions":
				err = os.WriteFile(name, []byte(descriptor.Content), 0644)
			case "directory":
				err = os.Mkdir(name, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(name)
			result, err := lifecycle(context.Background(), spec, true, hooks)
			if !errors.Is(err, ErrArtifactConflict) || result.InstallationPerformed || result.Publication != "not_requested" {
				t.Fatal("foreign artifact accepted", result, err)
			}
			after, _ := os.Lstat(name)
			if !os.SameFile(before, after) {
				t.Fatal("refusal replaced foreign artifact")
			}
			if _, err = os.Lstat(filepath.Join(spec.Directory, "."+ManagedLabel+".lock")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("foreign refusal created lock", err)
			}
		})
	}
	for _, kind := range []string{"wrong Mac directory", "wrong Linux suffix", "missing base", "symlink base", "writable base", "manager path mismatch"} {
		t.Run(kind, func(t *testing.T) {
			platform := "darwin"
			if strings.HasPrefix(kind, "wrong Linux") || kind == "manager path mismatch" {
				platform = "linux"
			}
			spec, hooks := serviceInstallFixture(t, platform)
			switch kind {
			case "wrong Mac directory":
				spec.Directory = filepath.Join(spec.HomeDir, "other")
			case "wrong Linux suffix":
				spec.Directory = filepath.Join(spec.HomeDir, "other")
			case "missing base":
				spec.HomeDir = filepath.Join(spec.HomeDir, "missing")
				spec.Directory = filepath.Join(spec.HomeDir, "Library", "LaunchAgents")
			case "symlink base":
				link := filepath.Join(spec.HomeDir, "link")
				if err := os.Symlink(spec.HomeDir, link); err != nil {
					t.Fatal(err)
				}
				spec.HomeDir = link
				spec.Directory = filepath.Join(link, "Library", "LaunchAgents")
			case "writable base":
				if err := os.Chmod(spec.HomeDir, 0777); err != nil {
					t.Fatal(err)
				}
			case "manager path mismatch":
				hooks.manager = func(context.Context, string) (ManagerObservation, error) {
					return ManagerObservation{Status: "observed", UnitPath: []string{"/unrelated/systemd/user"}}, nil
				}
			}
			result, err := lifecycle(context.Background(), spec, true, hooks)
			if err == nil || result.InstallationPerformed || result.Publication == "saved" || len(result.DirectoriesCreated) != 0 {
				t.Fatal("unsupported placement changed files", result, err)
			}
		})
	}
}

func TestServiceArtifactDestinationRaceAndPublicationUncertainty(t *testing.T) {
	for _, kind := range []string{"destination race", "cancel before", "cancel after", "sync failure", "rename uncertainty", "directory replaced", "lock replaced"} {
		t.Run(kind, func(t *testing.T) {
			spec, hooks := serviceInstallFixture(t, "darwin")
			descriptor, _ := Build(spec.Service)
			name := filepath.Join(spec.Directory, descriptor.Filename)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "destination race":
				hooks.beforePublish = func() {
					if err := os.WriteFile(name, []byte("racing foreign sentinel"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "cancel before":
				hooks.beforePublish = cancel
			case "cancel after":
				hooks.afterPublish = cancel
			case "sync failure":
				hooks.syncParent = func(*os.File) error { return unix.EIO }
			case "rename uncertainty":
				hooks.publish = func(oldFD int, old string, newFD int, new string) error {
					if err := publishServiceExclusive(oldFD, old, newFD, new); err != nil {
						return err
					}
					return unix.EIO
				}
			case "directory replaced":
				hooks.beforePublish = func() {
					if err := os.Rename(spec.Directory, spec.Directory+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(spec.Directory, 0700); err != nil {
						t.Fatal(err)
					}
				}
			case "lock replaced":
				hooks.beforePublish = func() {
					lock := filepath.Join(spec.Directory, "."+ManagedLabel+".lock")
					if err := os.Rename(lock, lock+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(lock, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			result, err := lifecycle(ctx, spec, true, hooks)
			if err == nil || result.InstallationPerformed || result.SyncCompleted {
				t.Fatal("failure claimed completed publication", result, err)
			}
			if kind == "cancel after" || kind == "sync failure" || kind == "rename uncertainty" {
				if !errors.Is(err, ErrArtifactPublication) || result.Publication != "uncertain" || result.DescriptorPath != name || result.DescriptorSHA256 == "" {
					t.Fatal("uncertain publication lost candidate scope", result, err)
				}
				status, statusErr := lifecycle(context.Background(), spec, false, artifactHooks{})
				if statusErr != nil || status.ArtifactStatus != "exact" {
					t.Fatal("candidate cannot be inspected offline", status, statusErr)
				}
			}
			if kind == "cancel before" || kind == "cancel after" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation identity lost", err)
				}
			}
			if kind == "destination race" {
				data, _ := os.ReadFile(name)
				if string(data) != "racing foreign sentinel" || !errors.Is(err, ErrArtifactConflict) {
					t.Fatal("exclusive publication replaced racing destination", err)
				}
			}
		})
	}
}

func TestServiceArtifactBusyLockAndDebrisBounds(t *testing.T) {
	spec, hooks := serviceInstallFixture(t, "darwin")
	saved, err := lifecycle(context.Background(), spec, true, hooks)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(spec.Directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent := os.NewFile(uintptr(fd), spec.Directory)
	defer parent.Close()
	lock, _, err := serviceArtifactLock(context.Background(), parent)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := lifecycle(context.Background(), spec, true, hooks); !errors.Is(err, ErrArtifactBusy) || result.InstallationPerformed {
		t.Fatal("busy publisher accepted", result, err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(saved.DescriptorPath); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		name := filepath.Join(spec.Directory, "."+ManagedLabel+"-"+strings.Repeat("x", i+1)+".tmp")
		if err = os.WriteFile(name, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := lifecycle(context.Background(), spec, true, hooks); !errors.Is(err, ErrLifecycleBounds) || result.InstallationPerformed {
		t.Fatal("debris capacity bypassed", result, err)
	}
	if _, err = os.Stat(saved.DescriptorPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("bounded refusal published artifact", err)
	}
}

func TestServiceArtifactLinuxStatusSeparatesManagerVisibility(t *testing.T) {
	spec, hooks := serviceInstallFixture(t, "linux")
	if _, err := lifecycle(context.Background(), spec, true, hooks); err != nil {
		t.Fatal(err)
	}
	hooks.manager = func(context.Context, string) (ManagerObservation, error) {
		return ManagerObservation{Status: "unavailable", BusSocket: spec.BusSocket, UnitPath: []string{}}, ErrManagerUnavailable
	}
	status, err := lifecycle(context.Background(), spec, false, hooks)
	if err != nil || status.ArtifactStatus != "exact" || status.Manager.Status != "unavailable" || status.Manager.DirectoryInUnitPath != nil || status.InstallationPerformed || status.ActivationPerformed {
		t.Fatal("offline status fabricated manager visibility", status, err)
	}
	hooks.manager = func(context.Context, string) (ManagerObservation, error) {
		return ManagerObservation{Status: "observed", UnitPath: []string{"/different/systemd/user"}}, nil
	}
	status, err = lifecycle(context.Background(), spec, false, hooks)
	if err != nil || status.ArtifactStatus != "exact" || status.Manager.DirectoryInUnitPath == nil || *status.Manager.DirectoryInUnitPath {
		t.Fatal("status confused exact artifact with manager path", status, err)
	}
	if result, err := lifecycle(context.Background(), spec, true, hooks); !errors.Is(err, ErrManagerPath) || result.InstallationPerformed || result.Publication != "not_requested" {
		t.Fatal("install ignored manager mismatch", result, err)
	}
}
