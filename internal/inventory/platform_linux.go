package inventory

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func timestamps(s *unix.Stat_t) (int64, int64) { return s.Mtim.Nano(), s.Ctim.Nano() }

func dataless(s unix.Stat_t) bool { return false }

func filesystem(fd int) (string, string, error) {
	var fs unix.Statfs_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return "", "", err
	}
	// Deliberately narrow until provider/network behavior has native evidence.
	switch uint64(fs.Type) {
	case 0xef53, 0x9123683e, 0x58465342, 0x01021994, 0x794c7630: // ext, btrfs, xfs, tmpfs, overlay (fixtures)
	default:
		return "", "", fmt.Errorf("unsupported filesystem type %#x", fs.Type)
	}
	var st unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &st); err != nil {
		return "", "", err
	}
	if st.Mask&unix.STATX_MNT_ID == 0 {
		return "", "", fmt.Errorf("mount identity unavailable")
	}
	return fmt.Sprintf("%x:%v", fs.Type, fs.Fsid), fmt.Sprint(st.Mnt_id), nil
}
