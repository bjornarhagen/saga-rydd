package inventory

import (
	"context"
	"fmt"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// Native CI invokes only this test inside a private mount namespace. Ordinary
// dev tests require neither root nor mount capabilities.
func TestBindMountBoundary(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	t.Run("root_streams", testRootStreamsBindMount)
	for _, kind := range []string{"directory", "manifest"} {
		t.Run("live_"+kind, func(t *testing.T) {
			s, target := liveFixture(t)
			path := string(target.Finding.PathBytes)
			if kind == "manifest" {
				path = string(target.Finding.ManifestPathBytes)
			}
			// Bind the object to itself: device/inode stay equal, mount ID changes.
			if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
				t.Fatal(err)
			}
			defer unix.Unmount(path, 0)
			r, err := s.Verify(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code != "mount_boundary" {
				t.Fatal(r, err)
			}
		})
	}
	t.Run("input_lock", func(t *testing.T) {
		s, target := inspectionFixture(t, nil)
		path := filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json")
		if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
			t.Fatal(err)
		}
		defer unix.Unmount(path, 0)
		r, err := s.Inspect(context.Background(), []state.LiveTarget{target})
		if err != nil || r.Status != "blocked" || r.Targets[0].Code != "mount_boundary" {
			t.Fatal(r, err)
		}
	})
	for _, kind := range []string{"file", "directory"} {
		t.Run("tree_"+kind, func(t *testing.T) {
			s, target := treeFixture(t, nil)
			path := filepath.Join(string(target.Finding.PathBytes), "fixture-dep")
			if kind == "file" {
				path = filepath.Join(path, "bin.js")
			}
			if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
				t.Fatal(err)
			}
			defer unix.Unmount(path, 0)
			r, err := s.InspectTree(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code != "mount_boundary" {
				t.Fatal(r, err)
			}
		})
	}
	for _, atDestination := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovery_destination_%t", atDestination), func(t *testing.T) {
			request := recoveryFixture(t)
			path := string(request.SourcePathBytes)
			if atDestination {
				path = string(request.DestinationPathBytes)
				if err := os.Rename(string(request.SourcePathBytes), path); err != nil {
					t.Fatal(err)
				}
			}
			if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
				t.Fatal(err)
			}
			defer unix.Unmount(path, 0)
			report, err := ObserveRecovery(context.Background(), request)
			requireRecoveryUnknown(t, report, err)
			location := report.SourceLocation
			if atDestination {
				location = report.DestinationLocation
			}
			if location.Status != "blocked" || location.Code != "mount_boundary" || location.ObjectIdentity != nil {
				t.Fatal("recovery observation crossed an exact-child bind mount", report)
			}
		})
	}
	s, j, root := scannerFixture(t)
	source := t.TempDir()
	write(t, filepath.Join(source, "outside"))
	target := filepath.Join(root, "mounted")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	defer unix.Unmount(target, 0)
	b := next(t, s, j)
	if len(b.Entries) != 1 || b.Entries[0].SkipReason == "" {
		t.Fatal("same-filesystem bind mount not excluded", b)
	}
	j.Path = []byte("mounted")
	b, err := s.Next(context.Background(), j)
	if err != nil || b.Fault == "" || len(b.Entries) != 0 {
		t.Fatal("crossed bind mount", b, err)
	}
}
