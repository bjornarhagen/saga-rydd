package renameprobe

import (
	"testing"

	"golang.org/x/sys/unix"
)

func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.Renameat2(fromFD, from, toFD, to, unix.RENAME_NOREPLACE)
}

func ctime(st unix.Stat_t) int64 { return st.Ctim.Nano() }

func logFilesystem(t *testing.T, fd int) {
	t.Helper()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		t.Fatal(err)
	}
	t.Logf("platform=linux filesystem_type=%#x directory_fsync_supported=true", fs.Type)
}

func probeStrongerSync(t *testing.T, fd int) {
	t.Helper()
	t.Log("power_loss_durability_tested=false")
}
