package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"golang.org/x/sys/unix"
)

func dismissalStorageFixture(t *testing.T) (SelectionSnapshot, string, string, string) {
	t.Helper()
	_, saved := dismissalSelectionFixture(t)
	sourceRoot := t.TempDir()
	project := filepath.Join(sourceRoot, "a")
	target, manifest := filepath.Join(project, "node_modules"), filepath.Join(project, "package.json")
	if err := os.MkdirAll(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("generated original manifest, not a SQLite database"), 0600); err != nil {
		t.Fatal(err)
	}
	saved.Roots[0].PathBytes = []byte(sourceRoot)
	f := &saved.Evidence.Findings[0]
	f.Path, f.PathBytes = target, []byte(target)
	f.ManifestPath, f.ManifestPathBytes = manifest, []byte(manifest)
	f.Measurement.Path, f.Measurement.PathBytes = target, []byte(target)
	for i, path := range []string{target, manifest} {
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		binding := &saved.Targets[0].Target
		if i != 0 {
			binding = &saved.Targets[0].Manifest
		}
		binding.Device, binding.Inode = fmt.Sprint(st.Dev), fmt.Sprint(st.Ino)
		if i == 0 {
			f.Device, f.Inode = binding.Device, binding.Inode
		}
	}
	base := filepath.Join(t.TempDir(), "manual", "selected-inventory")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	return saved, base, target, manifest
}

func TestDismissalInventoryStorageKnownManifestNames(t *testing.T) {
	for _, name := range []string{Filename, Filename + "-journal", Filename + "-wal", Filename + "-shm", "writer.lock"} {
		for _, hardlink := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_hardlink_%t", name, hardlink), func(t *testing.T) {
				saved, base, _, manifest := dismissalStorageFixture(t)
				private := filepath.Join(base, name)
				var err error
				if hardlink {
					err = os.Link(manifest, private)
				} else {
					err = os.Rename(manifest, private)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = CheckDismissalInventoryStorage(context.Background(), base, saved); !errors.Is(err, localfs.ErrObjectAlias) {
					t.Fatal("selected manifest reached a private file slot without metadata refusal", err)
				}
			})
		}
	}
}

func TestDismissalInventoryStorageKnownDirectoryNames(t *testing.T) {
	for _, parent := range []bool{false, true} {
		t.Run(fmt.Sprint(parent), func(t *testing.T) {
			saved, base, target, _ := dismissalStorageFixture(t)
			private := base
			if err := os.Remove(base); err != nil {
				t.Fatal(err)
			}
			if parent {
				private = filepath.Dir(base)
				if err := os.Remove(private); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Rename(target, private); err != nil {
				t.Fatal(err)
			}
			if err := CheckDismissalInventoryStorage(context.Background(), base, saved); !errors.Is(err, localfs.ErrObjectAlias) {
				t.Fatal("selected directory reached private traversal scope without metadata refusal", err)
			}
		})
	}
}

func TestDismissalInventoryStorageOfflineSourceUnknownScopeAndCancellation(t *testing.T) {
	saved, base, target, manifest := dismissalStorageFixture(t)
	for _, path := range []string{target, manifest} {
		if err := os.Rename(path, path+".offline"); err != nil {
			t.Fatal(err)
		}
	}
	// The metadata guard is independent of SQLite parsing and source availability.
	if err := os.WriteFile(filepath.Join(base, Filename), []byte("generated distinct private non-SQLite bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CheckDismissalInventoryStorage(context.Background(), base, saved); err != nil {
		t.Fatal("guard opened source paths or database contents", err)
	}
	if err := CheckDismissalInventoryStorage(context.Background(), filepath.Join(base, "missing"), saved); err != nil {
		t.Fatal("missing private names caused an implicit open or initialization", err)
	}
	unknown, err := CanonicalDismissalSelection(saved)
	if err != nil {
		t.Fatal(err)
	}
	unknown.Targets[0].Manifest.Inode = ""
	if err = CheckDismissalInventoryStorage(context.Background(), base, unknown); !errors.Is(err, ErrDismissalSelection) {
		t.Fatal("unknown historical identity was treated as protected", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = CheckDismissalInventoryStorage(ctx, base, saved); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled metadata guard succeeded", err)
	}
}
