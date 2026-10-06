package renameprobe

import (
	"testing"

	"golang.org/x/sys/unix"
)

func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.RenameatxNp(fromFD, from, toFD, to, unix.RENAME_EXCL)
}

func ctime(st unix.Stat_t) int64 { return st.Ctim.Nano() }

func logFilesystem(t *testing.T, fd int) {
	t.Helper()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		t.Fatal(err)
	}
	t.Logf("platform=darwin filesystem_type=%s directory_fsync_supported=true", unix.ByteSliceToString(fs.Fstypename[:]))
}

func probeStrongerSync(t *testing.T, fd int) {
	t.Helper()
	_, err := unix.FcntlInt(uintptr(fd), unix.F_FULLFSYNC, 0)
	if err != nil {
		t.Logf("directory_fullfsync_supported=false error=%v power_loss_durability_tested=false", err)
		return
	}
	t.Log("directory_fullfsync_supported=true power_loss_durability_tested=false")
}
