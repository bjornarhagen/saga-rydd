//go:build darwin || linux

// Package renameprobe is an isolated native API experiment. Nothing in the
// application imports it, and every operation uses a generated test fixture.
package renameprobe

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

type moveResult struct {
	Renamed bool
}

type stageError struct {
	Stage string
	Err   error
}

func (e *stageError) Error() string { return fmt.Sprintf("%s: %v", e.Stage, e.Err) }
func (e *stageError) Unwrap() error { return e.Err }

// syncRename only explores API ordering and error propagation. It deliberately
// has no approval, scope, source-identity guard, journal or recovery mechanism.
// Renamed means the syscall returned success; it is not a durability claim.
func syncRename(fromFD int, from string, toFD int, to string, sync func(int) error, rename func(int, string, int, string) error) (moveResult, error) {
	var result moveResult
	for _, stage := range []struct {
		name string
		fd   int
	}{{"sync_source_before", fromFD}, {"sync_destination_before", toFD}} {
		if err := sync(stage.fd); err != nil {
			return result, &stageError{stage.name, err}
		}
	}
	if err := rename(fromFD, from, toFD, to); err != nil {
		return result, &stageError{"rename", err}
	}
	result.Renamed = true
	for _, stage := range []struct {
		name string
		fd   int
	}{{"sync_source_after", fromFD}, {"sync_destination_after", toFD}} {
		if err := sync(stage.fd); err != nil {
			return result, &stageError{stage.name, err}
		}
	}
	return result, nil
}

type fixture struct {
	root               string
	source, quarantine string
	sourceFD, destFD   int
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	f := fixture{root: t.TempDir()}
	f.source, f.quarantine = filepath.Join(f.root, "source"), filepath.Join(f.root, "quarantine")
	for _, path := range []string{f.source, f.quarantine} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	f.sourceFD = openDirectory(t, f.source)
	f.destFD = openDirectory(t, f.quarantine)
	var sourceStat, destStat unix.Stat_t
	if err := unix.Fstat(f.sourceFD, &sourceStat); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fstat(f.destFD, &destStat); err != nil {
		t.Fatal(err)
	}
	if sourceStat.Dev != destStat.Dev {
		t.Fatal("generated parents are not on the same device")
	}
	return f
}

func openDirectory(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
	})
	return fd
}

func writeFixture(t *testing.T, path, contents string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(contents); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeObject(t *testing.T, parent, name, contents string, directory bool) {
	t.Helper()
	path := filepath.Join(parent, name)
	if directory {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		writeFixture(t, filepath.Join(path, "payload"), contents)
		if err := unix.Fsync(openDirectory(t, path)); err != nil {
			t.Fatal(err)
		}
	} else {
		writeFixture(t, path, contents)
	}
}

func namedStat(t *testing.T, fd int, name string) unix.Stat_t {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	return st
}

func sameIdentity(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode&unix.S_IFMT == b.Mode&unix.S_IFMT
}

func contentsEqual(t *testing.T, parent, name, want string, directory bool) {
	t.Helper()
	path := filepath.Join(parent, name)
	if directory {
		path = filepath.Join(path, "payload")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatal("fixture contents changed")
	}
}

func requireAbsent(t *testing.T, fd int, name string) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("expected absent fixture name, got %v", err)
	}
}

func TestNoReplaceRoundTrip(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory_%t", directory), func(t *testing.T) {
			f := newFixture(t)
			const name = "selected\tfixture\n"
			makeObject(t, f.source, name, "original fixture payload", directory)
			before := namedStat(t, f.sourceFD, name)
			flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
			if directory {
				flags |= unix.O_DIRECTORY
			}
			heldFD, err := unix.Openat(f.sourceFD, name, flags, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(heldFD)
			result, err := syncRename(f.sourceFD, name, f.destFD, "slot", unix.Fsync, renameNoReplace)
			if err != nil || !result.Renamed {
				t.Fatalf("quarantine fixture: renamed=%t error=%v", result.Renamed, err)
			}
			after := namedStat(t, f.destFD, "slot")
			var held unix.Stat_t
			if err := unix.Fstat(heldFD, &held); err != nil {
				t.Fatal(err)
			}
			if !sameIdentity(before, after) || !sameIdentity(before, held) {
				t.Fatal("rename did not preserve the observed fixture identity")
			}
			requireAbsent(t, f.sourceFD, name)
			contentsEqual(t, f.quarantine, "slot", "original fixture payload", directory)
			result, err = syncRename(f.destFD, "slot", f.sourceFD, name, unix.Fsync, renameNoReplace)
			if err != nil || !result.Renamed {
				t.Fatalf("restore fixture: renamed=%t error=%v", result.Renamed, err)
			}
			restored := namedStat(t, f.sourceFD, name)
			if !sameIdentity(before, restored) {
				t.Fatal("round trip did not preserve fixture identity")
			}
			requireAbsent(t, f.destFD, "slot")
			contentsEqual(t, f.source, name, "original fixture payload", directory)
			t.Logf("identity_preserved=true ctime_changed_on_move=%t ctime_changed_on_round_trip=%t", ctime(before) != ctime(after), ctime(before) != ctime(restored))
		})
	}
}

func TestNoReplaceDestinationCollision(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory_%t", directory), func(t *testing.T) {
			f := newFixture(t)
			makeObject(t, f.source, "selected", "original", directory)
			makeObject(t, f.quarantine, "slot", "existing destination", directory)
			source, destination := namedStat(t, f.sourceFD, "selected"), namedStat(t, f.destFD, "slot")
			result, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", unix.Fsync, renameNoReplace)
			if result.Renamed || !errors.Is(err, unix.EEXIST) {
				t.Fatalf("expected no-overwrite refusal: renamed=%t error=%v", result.Renamed, err)
			}
			if !sameIdentity(source, namedStat(t, f.sourceFD, "selected")) || !sameIdentity(destination, namedStat(t, f.destFD, "slot")) {
				t.Fatal("collision changed either object identity")
			}
			contentsEqual(t, f.source, "selected", "original", directory)
			contentsEqual(t, f.quarantine, "slot", "existing destination", directory)
		})
	}
	t.Run("dangling_destination_link", func(t *testing.T) {
		f := newFixture(t)
		makeObject(t, f.source, "selected", "original", false)
		if err := os.Symlink("missing", filepath.Join(f.quarantine, "slot")); err != nil {
			t.Fatal(err)
		}
		destination := namedStat(t, f.destFD, "slot")
		result, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", unix.Fsync, renameNoReplace)
		if result.Renamed || !errors.Is(err, unix.EEXIST) {
			t.Fatalf("expected link collision refusal: renamed=%t error=%v", result.Renamed, err)
		}
		if !sameIdentity(destination, namedStat(t, f.destFD, "slot")) {
			t.Fatal("collision replaced destination link")
		}
		link, err := os.Readlink(filepath.Join(f.quarantine, "slot"))
		if err != nil || link != "missing" {
			t.Fatal("collision changed destination link text")
		}
		contentsEqual(t, f.source, "selected", "original", false)
	})
}

func TestHeldSourceDoesNotGuardNamedRename(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory_%t", directory), func(t *testing.T) {
			f := newFixture(t)
			makeObject(t, f.source, "selected", "reviewed original", directory)
			makeObject(t, f.source, "replacement", "unreviewed replacement", directory)
			observed := namedStat(t, f.sourceFD, "selected")
			flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
			if directory {
				flags |= unix.O_DIRECTORY
			}
			heldFD, err := unix.Openat(f.sourceFD, "selected", flags, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(heldFD)
			// Deterministically simulate a competing writer after the last check.
			if err := renameNoReplace(f.sourceFD, "selected", f.sourceFD, "parked"); err != nil {
				t.Fatal(err)
			}
			if err := renameNoReplace(f.sourceFD, "replacement", f.sourceFD, "selected"); err != nil {
				t.Fatal(err)
			}
			result, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", unix.Fsync, renameNoReplace)
			if err != nil || !result.Renamed {
				t.Fatalf("named-source replacement probe: renamed=%t error=%v", result.Renamed, err)
			}
			moved := namedStat(t, f.destFD, "slot")
			var held unix.Stat_t
			if err := unix.Fstat(heldFD, &held); err != nil {
				t.Fatal(err)
			}
			if sameIdentity(observed, moved) || !sameIdentity(observed, held) || !sameIdentity(observed, namedStat(t, f.sourceFD, "parked")) {
				t.Fatal("probe did not reproduce the named-source identity gap")
			}
			contentsEqual(t, f.source, "parked", "reviewed original", directory)
			contentsEqual(t, f.quarantine, "slot", "unreviewed replacement", directory)
			t.Log("named_source_race_reproduced=true post_move_identity_detects_mismatch=true pre_move_identity_not_enforced_by_api=true")
		})
	}
}

func TestRestoreCollisionPreservesBothObjects(t *testing.T) {
	f := newFixture(t)
	makeObject(t, f.source, "selected", "original retained payload", true)
	original := namedStat(t, f.sourceFD, "selected")
	if _, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", unix.Fsync, renameNoReplace); err != nil {
		t.Fatal(err)
	}
	makeObject(t, f.source, "selected", "new project payload", true)
	replacement := namedStat(t, f.sourceFD, "selected")
	result, err := syncRename(f.destFD, "slot", f.sourceFD, "selected", unix.Fsync, renameNoReplace)
	if result.Renamed || !errors.Is(err, unix.EEXIST) {
		t.Fatalf("expected restore collision refusal: renamed=%t error=%v", result.Renamed, err)
	}
	if !sameIdentity(original, namedStat(t, f.destFD, "slot")) || !sameIdentity(replacement, namedStat(t, f.sourceFD, "selected")) {
		t.Fatal("restore collision changed either object identity")
	}
	contentsEqual(t, f.quarantine, "slot", "original retained payload", true)
	contentsEqual(t, f.source, "selected", "new project payload", true)
}

func TestHeldParentsSurvivePathReplacement(t *testing.T) {
	f := newFixture(t)
	makeObject(t, f.source, "selected", "held-parent original", false)
	observed := namedStat(t, f.sourceFD, "selected")
	for _, parent := range []string{f.source, f.quarantine} {
		if err := os.Rename(parent, parent+"-held"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(parent, 0700); err != nil {
			t.Fatal(err)
		}
	}
	makeObject(t, f.source, "selected", "decoy original", false)
	makeObject(t, f.quarantine, "slot", "decoy destination", false)
	result, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", unix.Fsync, renameNoReplace)
	if err != nil || !result.Renamed {
		t.Fatalf("held-parent probe: renamed=%t error=%v", result.Renamed, err)
	}
	if !sameIdentity(observed, namedStat(t, f.destFD, "slot")) {
		t.Fatal("relative rename used substituted parent path")
	}
	contentsEqual(t, f.source, "selected", "decoy original", false)
	contentsEqual(t, f.quarantine, "slot", "decoy destination", false)
	contentsEqual(t, f.quarantine+"-held", "slot", "held-parent original", false)
	t.Log("held_parent_binding_preserved=true ancestor_scope_requires_separate_revalidation=true")
}

func TestSyncErrorsPreserveUnknownOutcome(t *testing.T) {
	stages := []string{"sync_source_before", "sync_destination_before", "sync_source_after", "sync_destination_after"}
	for failAt, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t)
			makeObject(t, f.source, "selected", "original", false)
			observed := namedStat(t, f.sourceFD, "selected")
			calls, renameCalls := 0, 0
			sync := func(fd int) error {
				calls++
				if calls == failAt+1 {
					return unix.EIO
				}
				return unix.Fsync(fd)
			}
			rename := func(fromFD int, from string, toFD int, to string) error {
				renameCalls++
				return renameNoReplace(fromFD, from, toFD, to)
			}
			result, err := syncRename(f.sourceFD, "selected", f.destFD, "slot", sync, rename)
			var detail *stageError
			if !errors.Is(err, unix.EIO) || !errors.As(err, &detail) || detail.Stage != stage {
				t.Fatalf("sync error lost its stage or cause: %v", err)
			}
			if failAt < 2 {
				if result.Renamed || renameCalls != 0 || !sameIdentity(observed, namedStat(t, f.sourceFD, "selected")) {
					t.Fatal("pre-sync failure did not prevent the rename")
				}
				requireAbsent(t, f.destFD, "slot")
				contentsEqual(t, f.source, "selected", "original", false)
			} else {
				if !result.Renamed || renameCalls != 1 || !sameIdentity(observed, namedStat(t, f.destFD, "slot")) {
					t.Fatal("post-sync failure lost evidence that the rename occurred")
				}
				requireAbsent(t, f.sourceFD, "selected")
				contentsEqual(t, f.quarantine, "slot", "original", false)
			}
		})
	}
}

func TestNativeDirectorySyncAndErrors(t *testing.T) {
	f := newFixture(t)
	if err := unix.Fsync(f.sourceFD); err != nil {
		t.Fatalf("fixture directory fsync unavailable: %v", err)
	}
	if err := unix.Fsync(f.destFD); err != nil {
		t.Fatalf("fixture quarantine directory fsync unavailable: %v", err)
	}
	logFilesystem(t, f.sourceFD)
	probeStrongerSync(t, f.sourceFD)
	if err := unix.Fsync(-1); !errors.Is(err, unix.EBADF) {
		t.Fatalf("invalid-descriptor fsync error lost: %v", err)
	}
	if err := renameNoReplace(-1, "selected", f.destFD, "slot"); !errors.Is(err, unix.EBADF) {
		t.Fatalf("invalid-descriptor rename error lost: %v", err)
	}
	if err := renameNoReplace(f.sourceFD, "missing", f.destFD, "slot"); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("missing-source rename error lost: %v", err)
	}
}
