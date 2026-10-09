//go:build linux

package service

import "golang.org/x/sys/unix"

func publishServiceExclusive(oldFD int, oldName string, newFD int, newName string) error {
	return unix.Renameat2(oldFD, oldName, newFD, newName, unix.RENAME_NOREPLACE)
}
