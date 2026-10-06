package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func treeFixture(t *testing.T, setup func(project, tree string)) (*Scanner, state.LiveTarget) {
	t.Helper()
	return inspectionFixture(t, func(project, tree string) {
		if err := os.Remove(filepath.Join(tree, "keep.txt")); err != nil {
			t.Fatal(err)
		}
		for _, dir := range []string{"fixture-dep", ".bin"} {
			if err := os.Mkdir(filepath.Join(tree, dir), 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(tree, "fixture-dep", "bin.js"), []byte("fixture executable body"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("../fixture-dep/bin.js", filepath.Join(tree, ".bin", "fixture")); err != nil {
			t.Fatal(err)
		}
		if setup != nil {
			setup(project, tree)
		}
	})
}

func TestTreeMetadataObservationAndLocalEdits(t *testing.T) {
	s, target := treeFixture(t, nil)
	inspect := func() TreeEvidence {
		t.Helper()
		r, err := s.InspectTree(context.Background(), []state.LiveTarget{target})
		if err != nil || r.Status != "inputs_and_tree_observed" || r.Executable || r.CurrentStateVerified || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) != 1 || r.Targets[0].Tree == nil {
			t.Fatal(r, err)
		}
		return *r.Targets[0].Tree
	}
	first := inspect()
	if first.Status != "metadata_observed" || first.Entries != 4 || first.Directories != 2 || first.RegularFiles != 1 || first.InternalBinLinks != 1 || len(first.MetadataSHA256) != 64 {
		t.Fatal(first)
	}
	// Directory atime from listing and file atime from an independent read must
	// not change the digest; it describes metadata, not contents or access time.
	if _, err := os.ReadFile(filepath.Join(string(target.Finding.PathBytes), "fixture-dep", "bin.js")); err != nil {
		t.Fatal(err)
	}
	if again := inspect(); again.MetadataSHA256 != first.MetadataSHA256 {
		t.Fatal("unstable metadata digest", first, again)
	}
	// An edit before this request is observable as new metadata, but there is
	// no saved-tree baseline or pristine-content claim in an existing plan.
	if err := os.WriteFile(filepath.Join(string(target.Finding.PathBytes), "fixture-dep", "bin.js"), []byte("locally edited executable"), 0700); err != nil {
		t.Fatal(err)
	}
	if edited := inspect(); edited.MetadataSHA256 == first.MetadataSHA256 {
		t.Fatal("digest omitted changed file metadata")
	}
}

func TestTreeSupportedLayouts(t *testing.T) {
	for _, kind := range []string{"hidden_lock", "scoped_nested", "unusual_name", "ordinary_node_modules_name", "optional_absent"} {
		t.Run(kind, func(t *testing.T) {
			s, target := treeFixture(t, func(project, tree string) {
				switch kind {
				case "hidden_lock":
					// Deliberately invalid JSON: a hidden installed lock is metadata
					// only and must never be used as a content-integrity baseline.
					if err := os.WriteFile(filepath.Join(tree, ".package-lock.json"), []byte("unread fixture"), 0600); err != nil {
						t.Fatal(err)
					}
				case "scoped_nested":
					pkg := filepath.Join(tree, "fixture-dep", "node_modules", "@scope", "child")
					if err := os.MkdirAll(pkg, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(pkg, "bin.js"), []byte("nested body"), 0700); err != nil {
						t.Fatal(err)
					}
					bins := filepath.Join(tree, "fixture-dep", "node_modules", ".bin")
					if err := os.Mkdir(bins, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink("../@scope/child/bin.js", filepath.Join(bins, "child")); err != nil {
						t.Fatal(err)
					}
					addLockedPackage(t, project, "node_modules/fixture-dep/node_modules/@scope/child")
				case "unusual_name":
					if err := os.WriteFile(filepath.Join(tree, "fixture-dep", "odd\n\tname"), []byte("ordinary fixture"), 0600); err != nil {
						t.Fatal(err)
					}
				case "ordinary_node_modules_name":
					p := filepath.Join(tree, "fixture-dep", "fixtures", "node_modules")
					if err := os.MkdirAll(p, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(p, "ordinary.txt"), []byte("fixture contents"), 0600); err != nil {
						t.Fatal(err)
					}
				case "optional_absent":
					addLockedPackage(t, project, "node_modules/absent")
				}
			})
			r, err := s.InspectTree(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "inputs_and_tree_observed" || r.Targets[0].Tree == nil {
				t.Fatal(r, err)
			}
		})
	}
}

func addLockedPackage(t *testing.T, project, path string) {
	t.Helper()
	lockPath := filepath.Join(project, "package-lock.json")
	contents, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	var lock map[string]any
	if err = json.Unmarshal(contents, &lock); err != nil {
		t.Fatal(err)
	}
	packages := lock["packages"].(map[string]any)
	entry := map[string]any{}
	for key, value := range packages["node_modules/fixture-dep"].(map[string]any) {
		entry[key] = value
	}
	packages[path] = entry
	contents, err = json.Marshal(lock)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(lockPath, contents, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestTreeUnsupportedObjectsAndLinks(t *testing.T) {
	for _, kind := range []string{"top_file", "unlisted_package", "fifo", "hardlink_inside", "hardlink_outside", "protected", "scope_child", "package_symlink", "bin_regular", "absolute_link", "outside_link", "dangling_link", "non_executable", "link_chain", "missing_dotdot", "file_dotdot", "trailing_slash", "excluded"} {
		t.Run(kind, func(t *testing.T) {
			s, target := treeFixture(t, func(project, tree string) {
				pkg := filepath.Join(tree, "fixture-dep")
				bin := filepath.Join(tree, ".bin", "fixture")
				var err error
				switch kind {
				case "top_file":
					err = os.WriteFile(filepath.Join(tree, "custom.txt"), []byte("private fixture"), 0600)
				case "unlisted_package":
					err = os.Mkdir(filepath.Join(tree, "unlisted"), 0700)
				case "fifo":
					err = unix.Mkfifo(filepath.Join(pkg, "pipe"), 0600)
				case "hardlink_inside":
					err = os.Link(filepath.Join(pkg, "bin.js"), filepath.Join(pkg, "copy"))
				case "hardlink_outside":
					err = os.Link(filepath.Join(pkg, "bin.js"), filepath.Join(project, "original"))
				case "protected":
					err = os.Mkdir(filepath.Join(pkg, ".git"), 0700)
				case "scope_child":
					addLockedPackage(t, project, "node_modules/@scope/listed")
					err = os.MkdirAll(filepath.Join(tree, "@scope", "custom"), 0700)
				case "package_symlink":
					if err = os.Rename(pkg, filepath.Join(project, "package-original")); err == nil {
						err = os.Symlink("../package-original", pkg)
					}
				case "non_executable":
					err = os.Chmod(filepath.Join(pkg, "bin.js"), 0600)
				case "bin_regular", "absolute_link", "outside_link", "dangling_link", "link_chain", "missing_dotdot", "file_dotdot", "trailing_slash":
					if err = os.Remove(bin); err != nil {
						t.Fatal(err)
					}
					text := map[string]string{"absolute_link": filepath.Join(pkg, "bin.js"), "outside_link": "../../package.json", "dangling_link": "../fixture-dep/missing", "link_chain": "other", "missing_dotdot": "../fixture-dep/missing/../bin.js", "file_dotdot": "../fixture-dep/bin.js/../bin.js", "trailing_slash": "../fixture-dep/bin.js/"}[kind]
					if kind == "bin_regular" {
						err = os.WriteFile(bin, []byte("custom executable"), 0700)
					} else {
						err = os.Symlink(text, bin)
					}
					if kind == "link_chain" && err == nil {
						err = os.Symlink("../fixture-dep/bin.js", filepath.Join(tree, ".bin", "other"))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			if kind == "excluded" {
				s.excludes = append(s.excludes, filepath.Join(string(target.Finding.PathBytes), "fixture-dep", "bin.js"))
			}
			r, err := s.InspectTree(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code == "" || r.Targets[0].Tree != nil || r.Targets[0].Inputs != nil || strings.Contains(r.Targets[0].Message, "private fixture") {
				t.Fatal(r, err)
			}
		})
	}
}

func TestTreeMutationBetweenMetadataPasses(t *testing.T) {
	for _, kind := range []string{"edit_file", "replace_file", "swap_directory", "swap_link", "add_file", "remove_file", "change_mode"} {
		t.Run(kind, func(t *testing.T) {
			s, target := treeFixture(t, nil)
			tree := string(target.Finding.PathBytes)
			_, _, err := s.inspectTargetWithTree(context.Background(), target, true, func() {
				file := filepath.Join(tree, "fixture-dep", "bin.js")
				var mutationErr error
				switch kind {
				case "edit_file":
					mutationErr = os.WriteFile(file, []byte("new fixture contents"), 0700)
				case "replace_file":
					if mutationErr = os.Rename(file, file+".old"); mutationErr == nil {
						mutationErr = os.WriteFile(file, []byte("replacement fixture"), 0700)
					}
				case "swap_directory":
					pkg := filepath.Dir(file)
					if mutationErr = os.Rename(pkg, pkg+".old"); mutationErr == nil {
						mutationErr = os.Symlink(pkg+".old", pkg)
					}
				case "swap_link":
					link := filepath.Join(tree, ".bin", "fixture")
					if mutationErr = os.Remove(link); mutationErr == nil {
						mutationErr = os.Symlink("../../package.json", link)
					}
				case "add_file":
					mutationErr = os.WriteFile(filepath.Join(filepath.Dir(file), "new"), []byte("added fixture"), 0600)
				case "remove_file":
					mutationErr = os.Remove(file)
				case "change_mode":
					mutationErr = os.Chmod(file, 0600)
				}
				if mutationErr != nil {
					t.Fatal(mutationErr)
				}
			})
			if err == nil {
				t.Fatal("accepted mutation after the first metadata pass")
			}
		})
	}
}

func TestTreeBoundsAndCancellation(t *testing.T) {
	for _, kind := range []string{"entries", "depth", "paths"} {
		t.Run(kind, func(t *testing.T) {
			s, target := treeFixture(t, func(project, tree string) {
				pkg := filepath.Join(tree, "fixture-dep")
				if kind == "depth" {
					if err := os.MkdirAll(filepath.Join(pkg, strings.Repeat("d/", TreeDepthLimit)), 0700); err != nil {
						t.Fatal(err)
					}
					return
				}
				if kind == "paths" {
					// Long prefixes make the retained byte cap meaningful
					// before either the entry or individual-path cap is reached.
					// Keep absolute fixture paths below macOS PATH_MAX too.
					pkg = filepath.Join(pkg, strings.Repeat("a", 220), strings.Repeat("b", 220), strings.Repeat("c", 220))
					if err := os.MkdirAll(pkg, 0700); err != nil {
						t.Fatal(err)
					}
				}
				files := TreeEntryLimit
				if kind == "paths" {
					files = 7500
				}
				for i := range files {
					if err := os.WriteFile(filepath.Join(pkg, strconv.Itoa(i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			})
			r, err := s.InspectTree(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code != "tree_limit" || r.Targets[0].Tree != nil {
				t.Fatal(r, err)
			}
		})
	}
	s, target := treeFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.InspectTree(ctx, []state.LiveTarget{target}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.InspectTree(context.Background(), make([]state.LiveTarget, 21)); err == nil {
		t.Fatal("ignored target count")
	}
}
