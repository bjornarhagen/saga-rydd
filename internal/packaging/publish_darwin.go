//go:build darwin

package packaging

import "golang.org/x/sys/unix"

func publishExclusive(old, new string) error {
	return unix.RenameatxNp(unix.AT_FDCWD, old, unix.AT_FDCWD, new, unix.RENAME_EXCL)
}
