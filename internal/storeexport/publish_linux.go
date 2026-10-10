//go:build linux

package storeexport

import "golang.org/x/sys/unix"

func publishNoReplace(parent int, source, destination string) error {
	return unix.Renameat2(parent, source, parent, destination, unix.RENAME_NOREPLACE)
}
