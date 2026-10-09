package plans

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func dismissalBindingAt(t *testing.T, path string) state.EntryBinding {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatal(err)
	}
	// The historical ctime deliberately differs from the current live value.
	// Identity screening must still reject a moved object before any body read.
	return state.EntryBinding{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), ChangedNS: 1, Generation: 7}
}

func dismissalAliasSelection(t *testing.T, path string, manifest bool) state.SelectionSnapshot {
	t.Helper()
	s := dismissalSelection()
	b := dismissalBindingAt(t, path)
	if manifest {
		s.Targets[0].Manifest = b
	} else {
		s.Targets[0].Target = b
		s.Evidence.Findings[0].Device, s.Evidence.Findings[0].Inode = b.Device, b.Inode
	}
	return s
}

func TestDismissalStorageMovedManifestAliasesRefuseBeforeSQLite(t *testing.T) {
	ctx := context.Background()
	for _, slot := range []string{filename, filename + "-journal", filename + "-wal", filename + "-shm", "writer.lock"} {
		t.Run(slot, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			dir := filepath.Join(base, "plans")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(t.TempDir(), "generated-manifest")
			body := []byte("generated selected manifest body; this is not SQLite\n")
			if err := os.WriteFile(source, body, 0600); err != nil {
				t.Fatal(err)
			}
			selection := dismissalAliasSelection(t, source, true)
			request, err := NewDismissalRequest([]byte("/fixture"), selection)
			if err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(dir, slot)
			if err = os.Rename(source, destination); err != nil {
				t.Fatal(err)
			}
			if saved, e := SaveDismissal(ctx, base, request); !errors.Is(e, localfs.ErrObjectAlias) || saved.ID != "" {
				t.Fatal("save did not refuse the known moved identity before storage access", saved, e)
			}
			if matches, e := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{selection}); !errors.Is(e, localfs.ErrObjectAlias) || matches != nil {
				t.Fatal("matcher did not refuse before SQLite", matches, e)
			}
			// A legitimate report can lack canonical dismissal evidence but still
			// retain a known manifest identity. It must receive the same guard.
			partial := state.SelectionSnapshot{Targets: selection.Targets}
			if matches, e := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{partial}); !errors.Is(e, localfs.ErrObjectAlias) || matches != nil {
				t.Fatal("partial identity escaped screening", matches, e)
			}
			got, e := os.ReadFile(destination)
			if e != nil || !bytes.Equal(got, body) {
				t.Fatal("guard changed the selected body", e)
			}
			entries, e := os.ReadDir(dir)
			if e != nil || len(entries) != 1 || entries[0].Name() != slot {
				t.Fatal("guard initialized private storage", entries, e)
			}
			var st unix.Stat_t
			if e = unix.Lstat(destination, &st); e != nil || st.Nlink != 1 {
				t.Fatal("fixture was not a single-link moved alias", e, st.Nlink)
			}
		})
	}
}

func TestDismissalStorageDirectoryAliasesRefuseBeforePermissionsOrInitialization(t *testing.T) {
	ctx := context.Background()
	for _, slot := range []string{"base", "plans"} {
		t.Run(slot, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			path := base
			if slot == "plans" {
				path = filepath.Join(base, "plans")
			}
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0755); err != nil {
				t.Fatal(err)
			}
			selection := dismissalAliasSelection(t, path, false)
			request, err := NewDismissalRequest([]byte("/fixture"), selection)
			if err != nil {
				t.Fatal(err)
			}
			if saved, e := SaveDismissal(ctx, base, request); !errors.Is(e, localfs.ErrObjectAlias) || saved.ID != "" {
				t.Fatal("directory alias reached private initialization", saved, e)
			}
			if matches, e := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{selection}); !errors.Is(e, localfs.ErrObjectAlias) || matches != nil {
				t.Fatal(matches, e)
			}
			info, e := os.Stat(path)
			if e != nil || info.Mode().Perm() != 0755 {
				t.Fatal("selected directory permissions changed", info, e)
			}
			entries, e := os.ReadDir(path)
			if e != nil || len(entries) != 0 {
				t.Fatal("selected directory gained private files", entries, e)
			}
		})
	}
}

func TestDismissalStorageUnknownIdentityBoundsAndCancellation(t *testing.T) {
	ctx := context.Background()
	missing := filepath.Join(t.TempDir(), "missing")
	unknown := state.SelectionSnapshot{Targets: []state.TargetBinding{{Target: state.EntryBinding{Inode: "0"}, Manifest: state.EntryBinding{Device: "1"}}}}
	if err := CheckDismissalStorage(ctx, missing, []state.SelectionSnapshot{unknown, {}}); err != nil {
		t.Fatal("unknown identities must not require a usable identity", err)
	}
	if matches, err := DismissedFindings(ctx, missing, []byte("/fixture"), []state.SelectionSnapshot{unknown, {}}); err != nil || !reflect.DeepEqual(matches, []bool{false, false}) {
		t.Fatal("unknown findings should stay visible", matches, err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unknown-identity reader initialized storage", err)
	}
	base := filepath.Join(t.TempDir(), "state")
	if _, err := SaveDismissal(ctx, base, dismissalRequest(t)); err != nil {
		t.Fatal(err)
	}
	if matches, err := DismissedFindings(ctx, base, []byte("/fixture"), []state.SelectionSnapshot{unknown}); err != nil || !reflect.DeepEqual(matches, []bool{false}) {
		t.Fatal("unknown identity hid a present-store finding", matches, err)
	}
	for _, selections := range [][]state.SelectionSnapshot{
		make([]state.SelectionSnapshot, state.PreviewTargetLimit+1),
		{{Roots: make([]state.RootBinding, 2)}},
		{{Targets: make([]state.TargetBinding, 2)}},
		{{Evidence: state.FindingReport{Findings: make([]state.Finding, 2)}}},
		{{Targets: []state.TargetBinding{{Manifest: state.EntryBinding{Device: strings.Repeat("1", 1025), Inode: "2"}}}}},
	} {
		if err := CheckDismissalStorage(ctx, missing, selections); !errors.Is(err, ErrDismissalRequest) {
			t.Fatal("oversize scope was not refused", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := CheckDismissalStorage(canceled, base, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDismissalUndoGuardsSavedManifestBeforeWriterOpen(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	source := filepath.Join(t.TempDir(), "generated-manifest")
	body := []byte("generated selected source remains a single link after rename\n")
	if err := os.WriteFile(source, body, 0600); err != nil {
		t.Fatal(err)
	}
	request, err := NewDismissalRequest([]byte("/fixture"), dismissalAliasSelection(t, source, true))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := SaveDismissal(ctx, base, request)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(base, "plans", "writer.lock")
	if err = os.Rename(lock, lock+".offline"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(source, lock); err != nil {
		t.Fatal(err)
	}
	if result, e := UndoDismissal(ctx, base, saved.ID); !errors.Is(e, localfs.ErrObjectAlias) || result.ID != "" {
		t.Fatal("known saved alias reached the undo writer", result, e)
	}
	got, err := os.ReadFile(lock)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatal("undo changed selected body", err)
	}
	again, err := ShowDismissal(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal("failed undo mutated saved review data", again, err)
	}
}
