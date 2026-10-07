package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const hashConfigByteLimit = 1 << 20

func hashConfigTimes(st unix.Stat_t) (int64, int64) {
	return st.Mtim.Nano(), st.Ctim.Nano()
}

func hashSelectedIdentity(st unix.Stat_t, proposal inventory.HashProposal) bool {
	device, inode := fmt.Sprint(st.Dev), fmt.Sprint(st.Ino)
	for _, target := range proposal.Targets {
		if target.File.Device == device && target.File.Inode == inode {
			return true
		}
		for _, ancestor := range target.Ancestors {
			if ancestor.Device == device && ancestor.Inode == inode {
				return true
			}
		}
	}
	return false
}

func validHashConfigFile(st unix.Stat_t, proposal inventory.HashProposal) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&0077 == 0 && st.Nlink == 1 && st.Uid == uint32(os.Geteuid()) && st.Size >= 0 && st.Size <= hashConfigByteLimit && !hashSelectedIdentity(st, proposal)
}

func sameHashConfigStamp(a, b unix.Stat_t) bool {
	am, ac := hashConfigTimes(a)
	bm, bc := hashConfigTimes(b)
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Nlink == b.Nlink && a.Uid == b.Uid && a.Size == b.Size && am == bm && ac == bc
}

// Parse bytes from the same checked descriptor. Loading configuration must not
// accidentally read a selected file through a private path or hardlink alias.
func loadHashRunConfig(ctx context.Context, paths config.Paths, proposal inventory.HashProposal) (config.Config, error) {
	cfg := config.Default()
	if err := ctx.Err(); err != nil {
		return cfg, err
	}
	fd, err := unix.Open(paths.ConfigFile, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, fmt.Errorf("open hashing configuration: %w", err)
	}
	file := os.NewFile(uintptr(fd), paths.ConfigFile)
	defer file.Close()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return cfg, err
	}
	if !validHashConfigFile(before, proposal) {
		return cfg, errors.New("hashing configuration must be a privately owned, single-link regular file of at most 1 MiB and cannot alias selected evidence")
	}
	var body bytes.Buffer
	chunk := make([]byte, 32<<10)
	for int64(body.Len()) < before.Size {
		if err := ctx.Err(); err != nil {
			return cfg, err
		}
		request := min(int64(len(chunk)), before.Size-int64(body.Len()))
		n, readErr := file.Read(chunk[:request])
		body.Write(chunk[:n])
		if readErr != nil && readErr != io.EOF {
			return cfg, readErr
		}
		if readErr == io.EOF && int64(body.Len()) != before.Size {
			return cfg, errors.New("hashing configuration changed while being read")
		}
		if n == 0 {
			return cfg, io.ErrNoProgress
		}
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(fd, &held); err != nil {
		return cfg, err
	}
	if err := unix.Lstat(paths.ConfigFile, &named); err != nil {
		return cfg, err
	}
	if !validHashConfigFile(held, proposal) || !validHashConfigFile(named, proposal) || !sameHashConfigStamp(before, held) || !sameHashConfigStamp(held, named) {
		return cfg, errors.New("hashing configuration changed while being read")
	}
	if err := ctx.Err(); err != nil {
		return cfg, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return cfg, err
	}
	return config.Decode(body.Bytes(), home)
}

// This metadata-only check runs before SQLite reads the derived source store.
// It adds the selected-evidence and link checks absent from ordinary readers.
// SQLite still uses its own path-based API; this is not namespace authentication.
func checkHashSourceStorage(ctx context.Context, dir string, proposal inventory.HashProposal) error {
	for _, path := range []string{filepath.Dir(dir), dir} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := localfs.CheckOwnedDir(path); err != nil {
			return err
		}
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			return err
		}
		if hashSelectedIdentity(st, proposal) {
			return errors.New("manual inventory directory aliases selected source evidence")
		}
	}
	db := filepath.Join(dir, state.Filename)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		var st unix.Stat_t
		err := unix.Lstat(db+suffix, &st)
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || hashSelectedIdentity(st, proposal) {
			return errors.New("manual inventory files must be private, single-link regular files and cannot alias selected source evidence")
		}
	}
	return nil
}
