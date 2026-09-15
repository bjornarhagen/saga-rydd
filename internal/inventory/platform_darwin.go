package inventory

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func timestamps(s *unix.Stat_t) (int64, int64) { return s.Mtim.Nano(), s.Ctim.Nano() }

// SF_DATALESS is documented in Apple's sys/stat.h and TN3150. This check
// supplements protected cloud paths; real provider behavior remains unverified.
func dataless(s unix.Stat_t) bool { return s.Flags&0x40000000 != 0 }

func (s *Scanner) filesystem(fd int) (string, string, error) {
	var fs unix.Statfs_t
	s.metrics.filesystem.Add(1)
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return "", "", err
	}
	name := unix.ByteSliceToString(fs.Fstypename[:])
	if name != "apfs" && name != "hfs" {
		return "", "", fmt.Errorf("unsupported filesystem %q", name)
	}
	return fmt.Sprintf("%s:%v", name, fs.Fsid), unix.ByteSliceToString(fs.Mntonname[:]), nil
}
