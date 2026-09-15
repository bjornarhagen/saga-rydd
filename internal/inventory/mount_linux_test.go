package inventory

import (
	"context"
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
