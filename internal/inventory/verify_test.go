package inventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func liveFixture(t *testing.T) (*Scanner, state.LiveTarget) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "project", "node_modules")
	if err = os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(root, "project", "package.json"), filepath.Join(path, "keep.txt")} {
		if err = os.WriteFile(p, []byte("original"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	stamp := func(path string) unix.Stat_t {
		t.Helper()
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	rootStamp := stamp(root)
	f, err := s.openAbsolute(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	volume, _, err := s.filesystem(int(f.Fd()))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	target := state.LiveTarget{Root: state.RootBinding{ID: 1, PathBytes: []byte(root), Fingerprint: rootFingerprint(root, volume, rootStamp), Revision: 1}}
	for _, p := range []string{".", "project"} {
		target.Ancestors = append(target.Ancestors, observation(p, stamp(filepath.Join(root, p))))
	}
	module := observation("project/node_modules", stamp(path))
	manifest := observation("project/package.json", stamp(filepath.Join(root, "project", "package.json")))
	target.Finding = state.Finding{ID: "node-modules-v1:1:3", PathBytes: []byte(path), ManifestPathBytes: []byte(filepath.Join(root, "project", "package.json")), DirectoryModifiedAt: time.Unix(0, module.MtimeNS).UTC(), ManifestModifiedAt: time.Unix(0, manifest.MtimeNS).UTC()}
	target.Binding = state.TargetBinding{FindingID: target.Finding.ID, Target: state.EntryBinding{Device: module.Device, Inode: module.Inode, ChangedNS: module.CtimeNS}, Manifest: state.EntryBinding{Device: manifest.Device, Inode: manifest.Inode, ChangedNS: manifest.CtimeNS}}
	return s, target
}

func TestLiveMetadataChanges(t *testing.T) {
	for _, kind := range []string{"unchanged", "target_symlink", "ancestor_symlink", "root_symlink", "target_replaced", "manifest_edit", "manifest_symlink", "manifest_fifo", "missing", "permission", "excluded", "excluded_child", "scope_removed", "child_contents"} {
		t.Run(kind, func(t *testing.T) {
			s, target := liveFixture(t)
			path, root := string(target.Finding.PathBytes), string(target.Root.PathBytes)
			manifest := string(target.Finding.ManifestPathBytes)
			var err error
			switch kind {
			case "target_symlink", "ancestor_symlink", "root_symlink":
				p := path
				if kind == "ancestor_symlink" {
					p = filepath.Dir(path)
				}
				if kind == "root_symlink" {
					p = root
				}
				if err = os.Rename(p, p+".old"); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(p+".old", p)
			case "target_replaced":
				if err = os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				err = os.Mkdir(path, 0700)
			case "manifest_edit":
				err = os.WriteFile(manifest, []byte("modified"), 0600)
			case "manifest_symlink", "manifest_fifo":
				if err = os.Rename(manifest, manifest+".old"); err != nil {
					t.Fatal(err)
				}
				if kind == "manifest_symlink" {
					err = os.Symlink(manifest+".old", manifest)
				} else {
					err = unix.Mkfifo(manifest, 0600)
				}
			case "missing":
				err = os.Rename(root, root+".offline")
			case "permission":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses permissions")
				}
				err = os.Chmod(filepath.Dir(path), 0)
				defer os.Chmod(filepath.Dir(path), 0700)
			case "excluded":
				s.excludes = append(s.excludes, path)
			case "excluded_child":
				s.excludes = append(s.excludes, filepath.Join(path, "keep.txt"))
			case "scope_removed":
				s.roots = map[string]bool{}
			case "child_contents":
				err = os.WriteFile(filepath.Join(path, "keep.txt"), []byte("modified"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Verify(context.Background(), []state.LiveTarget{target})
			want := "blocked"
			if kind == "unchanged" || kind == "child_contents" {
				want = "metadata_matches"
			}
			if err != nil || r.Status != want || r.Executable || r.CurrentStateVerified || len(r.Targets) != 1 {
				t.Fatal(r, err)
			}
		})
	}
}

func TestLiveMetadataSwapsDuringCheck(t *testing.T) {
	for _, kind := range []string{"target", "ancestor", "root", "manifest"} {
		t.Run(kind, func(t *testing.T) {
			s, target := liveFixture(t)
			p := string(target.Finding.PathBytes)
			switch kind {
			case "ancestor":
				p = filepath.Dir(p)
			case "root":
				p = string(target.Root.PathBytes)
			case "manifest":
				p = string(target.Finding.ManifestPathBytes)
			}
			err := s.verifyTarget(context.Background(), target, func() {
				if err := os.Rename(p, p+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(p+".old", p); err != nil {
					t.Fatal(err)
				}
			})
			if err == nil {
				t.Fatal("accepted swapped path")
			}
		})
	}
}

func TestLiveMetadataLimitsAndCancellation(t *testing.T) {
	s, target := liveFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Verify(ctx, []state.LiveTarget{target}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Verify(context.Background(), make([]state.LiveTarget, 21)); err == nil {
		t.Fatal("target limit ignored")
	}
	target.Finding.PathBytes = []byte(string(target.Root.PathBytes) + "/" + strings.Repeat("x/", 256) + "node_modules")
	target.Finding.ManifestPathBytes = []byte(filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package.json"))
	r, err := s.Verify(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "blocked" || r.Targets[0].Code != "path_limit" {
		t.Fatal(r, err)
	}
}
