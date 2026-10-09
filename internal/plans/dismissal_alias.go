package plans

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// CheckDismissalStorage checks only private storage metadata, before SQLite or
// initialization can read a known selected object through a storage alias.
// Partial scopes retain their available identities even if they cannot support
// dismissal. Missing paths are allowed. This is not authentication against a
// same-user namespace replacement between metadata checks and SQLite opening.
func CheckDismissalStorage(ctx context.Context, base string, selections []state.SelectionSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !dismissalRoot([]byte(base)) || len(selections) > state.PreviewTargetLimit {
		return ErrDismissalRequest
	}
	identities := make([]localfs.ObjectIdentity, 0, 2*len(selections))
	for _, selection := range selections {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(selection.Roots) > 1 || len(selection.Targets) > 1 || len(selection.Evidence.Findings) > 1 {
			return ErrDismissalRequest
		}
		for _, target := range selection.Targets {
			for _, binding := range []state.EntryBinding{target.Target, target.Manifest} {
				if len(binding.Device) > 1024 || len(binding.Inode) > 1024 || strings.ContainsRune(binding.Device, 0) || strings.ContainsRune(binding.Inode, 0) {
					return ErrDismissalRequest
				}
				if binding.Device != "" && binding.Inode != "" && binding.Inode != "0" {
					identities = append(identities, localfs.ObjectIdentity{Device: binding.Device, Inode: binding.Inode})
				}
			}
		}
	}
	if len(identities) == 0 {
		return ctx.Err()
	}
	dir := filepath.Join(base, "plans")
	database := filepath.Join(dir, filename)
	paths := []string{base, dir, database, database + "-journal", database + "-wal", database + "-shm", filepath.Join(dir, "writer.lock")}
	return localfs.CheckKnownObjectAliases(ctx, paths, identities)
}
