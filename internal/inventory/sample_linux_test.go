package inventory

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// Like TestBindMountBoundary, this requires an isolated native mount namespace.
// Binding an object to itself preserves device/inode while changing its mount.
func TestBindMountBoundaryFileSamples(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	for _, kind := range []string{"file", "parent", "file_during_read"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			path := string(targets[0].File.PathBytes)
			if kind == "parent" {
				path = filepath.Dir(path)
			}
			bind := func() {
				if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
					if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
						t.Skip("requires mount privileges inside an isolated namespace")
					}
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := unix.Unmount(path, 0); err != nil {
						t.Error(err)
					}
				})
			}
			hooks := fileSampleHooks{}
			if kind == "file_during_read" {
				hooks.beforeFinalCheck = bind
			} else {
				bind()
			}
			r, err := s.observeFileSamples(context.Background(), targets, hooks)
			item := requireSampleBlocked(t, r, err)
			if item.Code != "mount_boundary" {
				t.Fatal("same-inode bind mount accepted", item)
			}
			if kind != "file_during_read" && item.ReadBytes != 0 {
				t.Fatal("cross-mount file read", item)
			}
		})
	}
}
