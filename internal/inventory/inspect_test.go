package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const inspectManifest = `{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}}`

func inspectLock() string {
	return `{"name":"fixture","version":"1.0.0","lockfileVersion":3,"packages":{"":{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}},"node_modules/fixture-dep":{"version":"1.0.0","resolved":"https://registry.npmjs.org/fixture-dep/-/fixture-dep-1.0.0.tgz","integrity":"sha512-` + strings.Repeat("A", 86) + `=="}}}`
}

func inspectionFixture(t *testing.T, setup func(project, target string)) (*Scanner, state.LiveTarget) {
	t.Helper()
	return liveFixtureWithSetup(t, func(root, target string) {
		project := filepath.Dir(target)
		for name, contents := range map[string]string{"package.json": inspectManifest, "package-lock.json": inspectLock()} {
			if err := os.WriteFile(filepath.Join(project, name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if setup != nil {
			setup(project, target)
		}
	})
}

func TestProjectInputInspection(t *testing.T) {
	s, target := inspectionFixture(t, nil)
	r, err := s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.CurrentStateVerified || r.Executable || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) != 1 {
		t.Fatal(r, err)
	}
	inputs := r.Targets[0].Inputs
	if inputs == nil || inputs.LockfileVersion != 3 || inputs.LockedPackages != 1 || len(inputs.Files) != 2 {
		t.Fatal(r)
	}
	for i, contents := range []string{inspectManifest, inspectLock()} {
		digest := sha256.Sum256([]byte(contents))
		if inputs.Files[i].Bytes != len(contents) || inputs.Files[i].SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal(inputs)
		}
	}
	// Existing saved plans have no lock-content baseline. This input observation
	// must not be presented as equality with bytes seen by the scanner.
	lockPath := filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json")
	newLock := inspectLock() + "\n"
	if err := os.WriteFile(lockPath, []byte(newLock), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.Targets[0].Inputs.Files[1].SHA256 == inputs.Files[1].SHA256 {
		t.Fatal(r, err)
	}
	// Local dependency edits are deliberately unknown in this input-only slice.
	if err := os.WriteFile(filepath.Join(string(target.Finding.PathBytes), "keep.txt"), []byte("local edits"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" {
		t.Fatal(r, err)
	}
}

func TestProjectInputRefusals(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "fifo", "directory", "oversize_manifest", "oversize_lock", "excluded_lock", "invalid", "duplicate", "v1", ".npmrc", "npm-shrinkwrap.json", "patches", "binding.gyp"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, func(project, target string) {
				p := filepath.Join(project, "package-lock.json")
				var err error
				switch kind {
				case "missing", "symlink", "fifo", "directory":
					if err = os.Remove(p); err != nil {
						t.Fatal(err)
					}
					switch kind {
					case "symlink":
						err = os.Symlink("package.json", p)
					case "fifo":
						err = unix.Mkfifo(p, 0600)
					case "directory":
						err = os.Mkdir(p, 0700)
					}
				case "oversize_manifest":
					err = os.Truncate(filepath.Join(project, "package.json"), ManifestInputLimit+1)
				case "oversize_lock":
					err = os.Truncate(p, LockInputLimit+1)
				case "invalid":
					err = os.WriteFile(p, []byte("not JSON"), 0600)
				case "duplicate":
					err = os.WriteFile(p, []byte(`{"lockfileVersion":3,"lockfileVersion":2}`), 0600)
				case "v1":
					err = os.WriteFile(p, []byte(`{"lockfileVersion":1}`), 0600)
				case "excluded_lock":
				default:
					// These bytes are deliberately not JSON. Presence blocks before
					// opening or parsing config files that may contain secrets.
					err = os.WriteFile(filepath.Join(project, kind), []byte("private fixture setting"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			if kind == "excluded_lock" {
				s.excludes = append(s.excludes, filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json"))
			}
			r, err := s.Inspect(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code == "" || r.Targets[0].Inputs != nil || strings.Contains(r.Targets[0].Message, "private fixture setting") {
				t.Fatal(r, err)
			}
		})
	}
}

func TestProjectInputsChangedDuringInspection(t *testing.T) {
	for _, kind := range []string{"edit_lock", "replace_lock", "symlink_lock", "edit_manifest", "config_appears", "path_swap"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, nil)
			project := filepath.Dir(string(target.Finding.PathBytes))
			_, err := s.inspectTarget(context.Background(), target, func() {
				lock := filepath.Join(project, "package-lock.json")
				var mutationErr error
				switch kind {
				case "edit_lock":
					mutationErr = os.WriteFile(lock, []byte(inspectLock()+"\n"), 0600)
				case "edit_manifest":
					mutationErr = os.WriteFile(filepath.Join(project, "package.json"), []byte(inspectManifest+"\n"), 0600)
				case "replace_lock", "symlink_lock":
					if mutationErr = os.Rename(lock, lock+".old"); mutationErr != nil {
						t.Fatal(mutationErr)
					}
					if kind == "replace_lock" {
						mutationErr = os.WriteFile(lock, []byte(inspectLock()), 0600)
					} else {
						mutationErr = os.Symlink(lock+".old", lock)
					}
				case "config_appears":
					mutationErr = os.WriteFile(filepath.Join(project, ".npmrc"), []byte("private fixture setting"), 0600)
				case "path_swap":
					if mutationErr = os.Rename(project, project+".old"); mutationErr != nil {
						t.Fatal(mutationErr)
					}
					mutationErr = os.Symlink(project+".old", project)
				}
				if mutationErr != nil {
					t.Fatal(mutationErr)
				}
			})
			if err == nil {
				t.Fatal("accepted input/path mutation after content read")
			}
		})
	}
}

func TestProjectInputLimitsAndCancellation(t *testing.T) {
	s, target := inspectionFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Inspect(ctx, []state.LiveTarget{target}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Inspect(context.Background(), make([]state.LiveTarget, 21)); err == nil {
		t.Fatal("ignored target limit")
	}
}
