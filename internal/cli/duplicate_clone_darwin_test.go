package cli

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Only generated regular files are cloned. Holding both descriptors avoids
// treating CLONE_NOFOLLOW as an ancestor-path guard; that flag alone can clone
// a source symlink rather than refusing it.
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
	var st unix.Stat_t
	if err = unix.Fstat(src, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 {
		return fmt.Errorf("generated clone source is not a single-link regular file")
	}
	err = unix.Fclonefileat(src, dir, destination, 0)
	if errors.Is(err, unix.ENOTSUP) {
		return &duplicateGateCloneUnavailable{api: "fclonefileat", cause: err}
	}
	return err
}
