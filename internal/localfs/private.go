// Package localfs manages Rydd-owned files. It never changes scan-root permissions.
package localfs

import (
	"fmt"
	"os"
)

// EnsurePrivateDir creates an application-owned directory, rejecting a symlink
// or an existing shared directory instead of changing someone else's permissions.
func EnsurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return CheckPrivateDir(path)
}

func CheckPrivateDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q must be a real directory", path)
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%q must be private (mode 0700); choose a dedicated data directory", path)
	}
	return nil
}

func CheckPrivateFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%q must be a regular file, not a symlink or special file", path)
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%q must be private (mode 0600)", path)
	}
	return nil
}
