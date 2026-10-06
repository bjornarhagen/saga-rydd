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

// Native CI runs this inside an isolated mount namespace. An exact self-bind
// preserves device/inode, so identity checks alone cannot reject the new mount.
func TestBindMountBoundaryFullHash(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	for _, kind := range []string{"file_before", "parent_before", "file_between", "parent_between", "root_between", "file_during_read", "parent_during_read"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, fullHashContents(256*1024))
			session := fullHashSession(t, s, targets[0])
			path := string(targets[0].File.PathBytes)
			if strings.HasPrefix(kind, "parent_") {
				path = filepath.Dir(path)
			}
			if kind == "root_between" {
				path = string(targets[0].Root.PathBytes)
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
			var offset int64
			if strings.HasSuffix(kind, "_between") {
				p, _, err := session.Step(context.Background(), 65)
				if err != nil || p.Offset != 65 {
					t.Fatal(p, err)
				}
				offset = p.Offset
			}
			hooks := fileHashHooks{}
			if strings.HasSuffix(kind, "_during_read") {
				hooks.beforeFinalCheck = bind
			} else {
				bind()
			}
			p, usage, err := session.step(context.Background(), 64*1024, hooks)
			requireFullHashInvalid(t, p, usage, err)
			if p.Offset != offset || p.Code == "" {
				t.Fatal("same-inode mount committed new hash progress", p, usage)
			}
			if strings.HasSuffix(kind, "_during_read") {
				if usage.ReadBytes != 64*1024 || usage.RequestedBytes != 64*1024 {
					t.Fatal("mount race did not exercise post-read revalidation", usage)
				}
			} else if usage.ReadBytes != 0 || usage.RequestedBytes != 0 {
				t.Fatal("hash read across a preexisting mount boundary", usage)
			}
		})
	}
}
