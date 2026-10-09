package state

import (
	"context"
	"path/filepath"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

// CheckDismissalInventoryStorage detects known selected-object aliases before
// a derived inventory reader can open SQLite or its sidecars. It performs only
// metadata probes of the private names, never source-path or file-body reads.
// A successful probe is not namespace authentication or a reusable open token.
func CheckDismissalInventoryStorage(ctx context.Context, base string, saved SelectionSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := CanonicalDismissalSelection(saved)
	if err != nil {
		return err
	}
	if !canonicalPath([]byte(base)) {
		return ErrDismissalSelection
	}
	binding := canonical.Targets[0]
	identities := []localfs.ObjectIdentity{
		{Device: binding.Target.Device, Inode: binding.Target.Inode},
		{Device: binding.Manifest.Device, Inode: binding.Manifest.Inode},
	}
	db := filepath.Join(base, Filename)
	return localfs.CheckKnownObjectAliases(ctx, []string{
		filepath.Dir(base), base, db, db + "-journal", db + "-wal", db + "-shm", filepath.Join(base, "writer.lock"),
	}, identities)
}
