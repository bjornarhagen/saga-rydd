package inventory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func scannerFixture(t *testing.T) (*Scanner, state.Job, string) {
	t.Helper()
	root := t.TempDir()
	s, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Kind: state.ScanKind}, root
}
func write(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
}
func next(t *testing.T, s *Scanner, j state.Job) state.ScanBatch {
	t.Helper()
	b, err := s.Next(context.Background(), j)
	if err != nil || b.Fault != "" {
		t.Fatal(b.Fault, err)
	}
	return b
}

func TestBoundedStreamAndRestart(t *testing.T) {
	s, j, root := scannerFixture(t)
	for i := 0; i < 301; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("entry-%03d", i)))
	}
	b := next(t, s, j)
	if len(b.Entries) != state.MaxBatchEntries || b.Complete {
		t.Fatal("unbounded or premature pass")
	}
	oldGeneration := b.Generation
	j.Cursor = b.Cursor
	j.RootIdentity = b.Identity
	s.Close()
	s, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seen := map[string]bool{}
	var generation int64
	for batches := 0; batches < 5; batches++ {
		b = next(t, s, j)
		if generation == 0 {
			generation = b.Generation
			if generation == oldGeneration {
				t.Fatal("restart reused pass")
			}
		}
		if b.Generation != generation || len(b.Entries) > state.MaxBatchEntries {
			t.Fatal("lost stream or unbounded batch")
		}
		for _, e := range b.Entries {
			if seen[string(e.Path)] {
				t.Fatal("duplicate within continuous enumeration")
			}
			seen[string(e.Path)] = true
		}
		if b.Complete {
			if len(seen) != 301 {
				t.Fatal(len(seen))
			}
			return
		}
		j.Cursor = b.Cursor
	}
	t.Fatal("directory did not finish")
}

func TestMetadataSymlinksSpecialFilesAndExclusions(t *testing.T) {
	s, j, root := scannerFixture(t)
	outside := t.TempDir()
	write(t, filepath.Join(outside, "secret"))
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "file"))
	if err := os.Link(filepath.Join(root, "file"), filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(root, "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	for _, name := range []string{".git", "excluded", "state"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
		write(t, filepath.Join(root, name, "keep"))
	}
	s.Close()
	s, err = New([]string{root}, []string{filepath.Join(root, "excluded")}, []string{filepath.Join(root, "state")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := next(t, s, j)
	entries := map[string]state.Entry{}
	for _, e := range b.Entries {
		entries[string(e.Path)] = e
	}
	if entries["link"].Kind != "symlink" || entries["fifo"].Kind != "other" || entries["file"].Inode != entries["hardlink"].Inode || entries["sparse"].Size != 1<<20 || entries["sparse"].Allocated >= entries["sparse"].Size {
		t.Fatal(entries)
	}
	for _, name := range []string{".git", "excluded", "state"} {
		if entries[name].SkipReason == "" {
			t.Fatal("missing exclusion", name)
		}
	}
	j.Path = []byte("link")
	b, err = s.Next(context.Background(), j)
	if err != nil || b.Fault == "" || len(b.Entries) != 0 {
		t.Fatal("followed symlink", b, err)
	}
}

func TestRootReplacementCancellationAndMutation(t *testing.T) {
	s, j, root := scannerFixture(t)
	for i := 0; i < 140; i++ {
		write(t, filepath.Join(root, fmt.Sprint(i)))
	}
	b := next(t, s, j)
	j.Cursor = b.Cursor
	j.RootIdentity = b.Identity
	write(t, filepath.Join(root, "new"))
	b = next(t, s, j)
	if b.Generation == int64(0) {
		t.Fatal("missing generation")
	}
	// A changed directory restarts the stream; it never completes the old pass.
	if string(b.Cursor) == string(j.Cursor) || b.Complete {
		t.Fatal("mutation reused cursor")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Next(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := os.Rename(root, root+"-old"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Rename(root+"-old", root) })
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(root)
	b, err := s.Next(context.Background(), j)
	if err != nil || b.Fault == "" || len(b.Entries) != 0 {
		t.Fatal("replacement root accepted", b, err)
	}
}

func TestUnavailablePermissionAndPhysicalAliases(t *testing.T) {
	s, j, root := scannerFixture(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if other, err := New([]string{root, alias}, nil, nil); err == nil {
		other.Close()
		t.Fatal("physical root alias accepted")
	}
	j.Path = []byte("missing")
	b, err := s.Next(context.Background(), j)
	if err != nil || b.Fault == "" || b.Complete {
		t.Fatal(b, err)
	}
	if os.Geteuid() == 0 {
		t.Skip("permission denial requires unprivileged user")
	}
	if err := os.Mkdir(filepath.Join(root, "denied"), 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(filepath.Join(root, "denied"), 0700)
	j.Path = []byte("denied")
	b, err = s.Next(context.Background(), j)
	if err != nil || b.Fault == "" || b.Complete {
		t.Fatal(b, err)
	}
}

func TestNonUTF8Name(t *testing.T) {
	s, j, root := scannerFixture(t)
	name := string([]byte{'x', 0xff})
	if err := os.WriteFile(filepath.Join(root, name), nil, 0600); err != nil {
		if errors.Is(err, unix.EILSEQ) || errors.Is(err, unix.EINVAL) {
			t.Skip("filesystem rejects non-UTF-8 names")
		}
		t.Fatal(err)
	}
	b := next(t, s, j)
	if len(b.Entries) != 1 || string(b.Entries[0].Path) != name {
		t.Fatal(b)
	}
}

func TestCloseDoesNotWaitForBlockedFilesystemCall(t *testing.T) {
	s, j, _ := scannerFixture(t)
	func() {
		s.mu.Lock()
		defer s.mu.Unlock() // Model Next waiting inside a kernel call.
		done := make(chan struct{})
		go func() { s.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close blocked shutdown")
		}
	}()
	if _, err := s.Next(context.Background(), j); err == nil {
		t.Fatal("closed scanner accepted work")
	}
}

func TestPrivateFileIdentityExclusion(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "private")
	write(t, original)
	if err := os.Link(original, filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	s, err := New([]string{root}, nil, []string{original})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := next(t, s, state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte(".")})
	for _, e := range b.Entries {
		if e.SkipReason == "" {
			t.Fatal("private inode alias not excluded", e)
		}
	}
}
