//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestServiceLoginLinkUnsupportedPlatformBeforeFilesystem(t *testing.T) {
	spec, _ := serviceInstallFixture(t, "darwin")
	for _, call := range []func(context.Context, InstallSpec) (LoginLinkResult, error){EnableLoginLink, DisableLoginLink} {
		r, err := call(context.Background(), spec)
		if !errors.Is(err, ErrSpec) || r.Contract != "" || r.ChangeAttempted {
			t.Fatal("unsupported platform reached filesystem/manager", r, err)
		}
		if _, err = os.Lstat(spec.Directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("unsupported platform created descriptor scope", err)
		}
	}
	if runtime.GOOS == "darwin" {
		spec.Service.GOOS = "linux"
		spec.Directory = filepath.Join(spec.HomeDir, "systemd", "user")
		if _, err := EnableLoginLink(context.Background(), spec); !errors.Is(err, ErrSpec) {
			t.Fatal("native macOS accepted Linux link controls", err)
		}
	}
}
