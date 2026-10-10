//go:build darwin

package storeexport

import "golang.org/x/sys/unix"

func publishNoReplace(parent int, source, destination string) error {
	return unix.RenameatxNp(parent, source, parent, destination, unix.RENAME_EXCL)
}
