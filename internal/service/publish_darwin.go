//go:build darwin

package service

import "golang.org/x/sys/unix"

func publishServiceExclusive(oldFD int, oldName string, newFD int, newName string) error {
	return unix.RenameatxNp(oldFD, oldName, newFD, newName, unix.RENAME_EXCL)
}
