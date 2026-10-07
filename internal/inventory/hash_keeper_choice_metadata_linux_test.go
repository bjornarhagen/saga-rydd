package inventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Native Linux CI supplies an isolated mount namespace. All source files are
// generated and completely hashed before any mount; checking reads no bodies.
func TestBindMountHashKeeperChoiceMetadata(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	for _, kind := range []string{"file_between", "parent_between", "root_between", "file_during_check", "parent_during_check"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 2)
			path := string(m.saved.Record.Evidence.Keeper.PathBytes)
			if strings.HasPrefix(kind, "parent_") {
				path = filepath.Dir(path)
			} else if kind == "root_between" {
				path = m.f.root
			}
			var before unix.Stat_t
			if err := unix.Lstat(path, &before); err != nil {
				t.Fatal(err)
			}
			bind := func() {
				if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
					if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
						t.Skip("requires mount privileges inside an isolated namespace")
					}
					t.Fatal(err)
				}
				// Register after fixture creation so unmount happens before the
				// temporary root and its generated sources are removed.
				t.Cleanup(func() {
					if err := unix.Unmount(path, 0); err != nil {
						t.Error(err)
					}
				})
				var after unix.Stat_t
				if err := unix.Lstat(path, &after); err != nil || before.Dev != after.Dev || before.Ino != after.Ino {
					t.Fatal("self-bind fixture did not preserve ordinary identity", after, err)
				}
			}
			hooks := hashKeeperChoiceMetadataHooks{}
			if strings.HasSuffix(kind, "_during_check") {
				hooks.beforeTargetFinalCheck = func(i int) {
					if i == 0 {
						bind()
					}
				}
			} else {
				bind()
			}
			r, err := m.request.check(context.Background(), m.f.scanner, hooks)
			requireHashChoiceMetadataBlocked(t, r, err)
			if len(r.Targets) != 2 || r.Targets[0].Role != "keeper" || r.Targets[0].Status != "blocked" {
				t.Fatal("changed mount passed the exact selected metadata check", r)
			}
			if r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 {
				t.Fatal("metadata mount refusal requested selected bodies", r)
			}
		})
	}
}
