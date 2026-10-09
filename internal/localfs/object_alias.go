package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

var ErrObjectAlias = errors.New("private storage name aliases a known selected object")

// ObjectIdentity uses the saved scanner's decimal device/inode representation.
// Ctime is intentionally absent: moving an object can change its ctime while
// retaining the identity whose body must not be opened as private storage.
type ObjectIdentity struct {
	Device string
	Inode  string
}

// CheckKnownObjectAliases probes at most 64 named paths against at most 128
// usable saved identities. Only Lstat metadata is read; missing names are fine.
// This detects known aliases at probe time. It authenticates neither ancestor
// scope nor namespace ownership and does not replace private-storage checks.
func CheckKnownObjectAliases(ctx context.Context, paths []string, identities []ObjectIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(paths) < 1 || len(paths) > 64 || len(identities) < 1 || len(identities) > 128 {
		return errors.New("object alias checks require 1–64 paths and 1–128 known identities")
	}
	known := make(map[ObjectIdentity]bool, len(identities))
	for _, identity := range identities {
		if err := ctx.Err(); err != nil {
			return err
		}
		if identity.Device == "" || identity.Inode == "" || identity.Inode == "0" || len(identity.Device) > 1024 || len(identity.Inode) > 1024 || strings.ContainsRune(identity.Device, 0) || strings.ContainsRune(identity.Inode, 0) || strings.TrimSpace(identity.Device) != identity.Device || strings.TrimSpace(identity.Inode) != identity.Inode {
			return errors.New("object alias checks require usable device/inode identities")
		}
		known[identity] = true
	}
	for _, path := range paths {
		if len(path) < 1 || len(path) > 4096 || strings.ContainsRune(path, 0) {
			return errors.New("object alias check path is invalid")
		}
	}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		var st unix.Stat_t
		err := unix.Lstat(path, &st)
		if canceled := ctx.Err(); canceled != nil {
			return canceled
		}
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if known[ObjectIdentity{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino)}] {
			return fmt.Errorf("%q: %w", path, ErrObjectAlias)
		}
	}
	return ctx.Err()
}
