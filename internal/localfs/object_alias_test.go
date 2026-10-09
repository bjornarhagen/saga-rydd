package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func objectAliasFixtureIdentity(t *testing.T, path string) ObjectIdentity {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatal(err)
	}
	return ObjectIdentity{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino)}
}

func TestKnownObjectAliasesRenameAndHardlink(t *testing.T) {
	for _, operation := range []string{"rename", "hardlink"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			source, private := filepath.Join(dir, "package.json"), filepath.Join(dir, "state.sqlite3")
			if err := os.WriteFile(source, []byte("generated non-SQLite body"), 0600); err != nil {
				t.Fatal(err)
			}
			identity := objectAliasFixtureIdentity(t, source)
			var err error
			if operation == "rename" {
				err = os.Rename(source, private)
			} else {
				err = os.Link(source, private)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = CheckKnownObjectAliases(context.Background(), []string{filepath.Join(dir, "missing"), private}, []ObjectIdentity{identity}); !errors.Is(err, ErrObjectAlias) {
				t.Fatal("retained dev/inode was not refused before any body open", err)
			}
		})
	}
}

func TestKnownObjectAliasesMissingNamesAndNoSymlinkFollowing(t *testing.T) {
	dir := t.TempDir()
	source, private, link := filepath.Join(dir, "package.json"), filepath.Join(dir, "state.sqlite3"), filepath.Join(dir, "link")
	if err := os.WriteFile(source, []byte("generated original"), 0600); err != nil {
		t.Fatal(err)
	}
	identity := objectAliasFixtureIdentity(t, source)
	if err := os.WriteFile(private, []byte("generated distinct private bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	if err := CheckKnownObjectAliases(context.Background(), []string{private, link, filepath.Join(dir, "missing")}, []ObjectIdentity{identity}); err != nil {
		t.Fatal("metadata guard followed a link or opened the non-database body", err)
	}
	if err := CheckPrivateFile(link); err == nil {
		t.Fatal("separate private-file protection accepted a symlink")
	}
	if err := os.Rename(source, source+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := CheckKnownObjectAliases(context.Background(), []string{private, filepath.Join(dir, "missing")}, []ObjectIdentity{identity}); err != nil {
		t.Fatal("guard required the historical source path to be available", err)
	}
}

func TestKnownObjectAliasesBoundsUnknownIdentityAndCancellation(t *testing.T) {
	known := ObjectIdentity{Device: "1", Inode: "2"}
	for _, tc := range []struct {
		paths []string
		ids   []ObjectIdentity
	}{
		{nil, []ObjectIdentity{known}},
		{[]string{"/generated/missing"}, nil},
		{make([]string, 65), []ObjectIdentity{known}},
		{[]string{"/generated/missing"}, make([]ObjectIdentity, 129)},
		{[]string{""}, []ObjectIdentity{known}},
		{[]string{strings.Repeat("a", 4097)}, []ObjectIdentity{known}},
		{[]string{"bad\x00path"}, []ObjectIdentity{known}},
		{[]string{"/generated/missing"}, []ObjectIdentity{{Device: "1", Inode: "0"}}},
		{[]string{"/generated/missing"}, []ObjectIdentity{{Device: "", Inode: "2"}}},
	} {
		if err := CheckKnownObjectAliases(context.Background(), tc.paths, tc.ids); err == nil || errors.Is(err, ErrObjectAlias) {
			t.Fatal("invalid bounded input was accepted or mislabeled as an observed alias", tc, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := CheckKnownObjectAliases(ctx, []string{"/generated/missing"}, []ObjectIdentity{known}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled probe did not refuse", err)
	}
}
