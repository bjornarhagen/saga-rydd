package cli

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// FICLONE is used directly. A byte-copy fallback would not provide native
// clone acceptance evidence.
func duplicateGateCloneFile(parent, source, destination string) error {
	dir, err := unix.Open(parent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dir)
	src, err := unix.Openat(dir, source, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(src)
	dst, err := unix.Openat(dir, destination, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	var sourceStat, destinationStat unix.Stat_t
	if err = unix.Fstat(src, &sourceStat); err == nil {
		err = unix.Fstat(dst, &destinationStat)
	}
	if err == nil && (sourceStat.Mode&unix.S_IFMT != unix.S_IFREG || destinationStat.Mode&unix.S_IFMT != unix.S_IFREG || sourceStat.Nlink != 1 || destinationStat.Nlink != 1 || sourceStat.Dev != destinationStat.Dev || sourceStat.Ino == destinationStat.Ino) {
		err = fmt.Errorf("generated clone descriptors are not distinct same-filesystem regular files")
	}
	if err == nil {
		err = unix.IoctlFileClone(dst, src)
		// Whole-file cloning uses no caller-authored range or alignment. With
		// verified regular same-filesystem descriptors, these errors indicate
		// unavailable clone support. EBADF remains a failure because it can
		// also report a descriptor-mode error.
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EINVAL) {
			err = &duplicateGateCloneUnavailable{api: "FICLONE", cause: err}
		}
	}
	closeErr := unix.Close(dst)
	if err == nil {
		err = closeErr
	}
	return err
}
