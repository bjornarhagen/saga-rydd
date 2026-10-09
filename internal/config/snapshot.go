//go:build darwin || linux

package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

var ErrConfigChanged = errors.New("configuration changed; load it again before starting work")

// Snapshot is an exact captured configuration, not a live permission token.
// It retains no descriptor and Check rereads bounded bytes without parsing.
type Snapshot struct {
	path   string
	stamp  unix.Stat_t
	parent unix.Stat_t
	digest [32]byte
	valid  bool
}

type heldConfig struct {
	file, parent *os.File
	path, name   string
	stamp        unix.Stat_t
	parentStamp  unix.Stat_t
}

func LoadSnapshot(ctx context.Context, path, home string) (Config, Snapshot, error) {
	data, held, err := readStableConfig(ctx, path, nil)
	if err != nil {
		return Config{}, Snapshot{}, err
	}
	defer held.close()
	cfg, err := Decode(data, home)
	if err != nil {
		return Config{}, Snapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return Config{}, Snapshot{}, err
	}
	return cfg, held.snapshot(data), nil
}

func (s Snapshot) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.valid {
		return ErrConfigChanged
	}
	data, held, err := readStableConfig(ctx, s.path, nil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %w", ErrConfigChanged, err)
	}
	defer held.close()
	if !sameConfigStamp(s.stamp, held.stamp) || !sameConfigParent(s.parent, held.parentStamp) || s.digest != sha256.Sum256(data) {
		return ErrConfigChanged
	}
	return ctx.Err()
}

func (h *heldConfig) snapshot(data []byte) Snapshot {
	return Snapshot{path: h.path, stamp: h.stamp, parent: h.parentStamp, digest: sha256.Sum256(data), valid: true}
}

func (h *heldConfig) close() {
	_ = h.file.Close()
	_ = h.parent.Close()
}

func configStoragePath(path string) bool {
	return path != "" && len(path) <= 4096 && !strings.ContainsRune(path, 0) && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func privateConfigParent(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid())
}

func privateConfigFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 && st.Size >= 0 && st.Size <= maxConfigBytes
}

func sameConfigStamp(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Nlink == b.Nlink && a.Uid == b.Uid && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func sameConfigParent(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid
}

func (h *heldConfig) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var held, named, parent, namedParent unix.Stat_t
	if err := unix.Fstat(int(h.file.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(h.parent.Fd()), h.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if err := unix.Fstat(int(h.parent.Fd()), &parent); err != nil {
		return err
	}
	if err := unix.Lstat(filepath.Dir(h.path), &namedParent); err != nil {
		return err
	}
	if !privateConfigFile(held) || !privateConfigFile(named) || !sameConfigStamp(h.stamp, held) || !sameConfigStamp(held, named) || !privateConfigParent(parent) || !privateConfigParent(namedParent) || !sameConfigParent(h.parentStamp, parent) || !sameConfigParent(parent, namedParent) {
		return ErrConfigChanged
	}
	return ctx.Err()
}

func readStableConfig(ctx context.Context, path string, afterChunk func(int)) ([]byte, *heldConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !configStoragePath(path) {
		return nil, nil, errors.New("configuration path must be one absolute canonical path of at most 4096 bytes")
	}
	parentFD, err := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	parent := os.NewFile(uintptr(parentFD), filepath.Dir(path))
	failParent := func(err error) ([]byte, *heldConfig, error) { _ = parent.Close(); return nil, nil, err }
	var parentStamp unix.Stat_t
	if err = unix.Fstat(parentFD, &parentStamp); err != nil {
		return failParent(err)
	}
	if !privateConfigParent(parentStamp) {
		return failParent(errors.New("configuration parent must be a private, owned directory"))
	}
	fd, err := unix.Openat(parentFD, filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return failParent(err)
	}
	file := os.NewFile(uintptr(fd), path)
	fail := func(err error) ([]byte, *heldConfig, error) { _ = file.Close(); return failParent(err) }
	var before unix.Stat_t
	if err = unix.Fstat(fd, &before); err != nil {
		return fail(err)
	}
	if !privateConfigFile(before) {
		return fail(errors.New("configuration must be a private, owned single-link regular file of at most 1 MiB"))
	}
	h := &heldConfig{file: file, parent: parent, path: path, name: filepath.Base(path), stamp: before, parentStamp: parentStamp}
	if err = h.check(ctx); err != nil {
		return fail(err)
	}
	var body bytes.Buffer
	chunk := make([]byte, 32<<10)
	for int64(body.Len()) < before.Size {
		if err = ctx.Err(); err != nil {
			return fail(err)
		}
		next := min(int64(len(chunk)), before.Size-int64(body.Len()))
		n, readErr := file.Read(chunk[:next])
		body.Write(chunk[:n])
		if n > 0 && afterChunk != nil {
			afterChunk(body.Len())
		}
		if readErr != nil && readErr != io.EOF {
			return fail(readErr)
		}
		if readErr == io.EOF && int64(body.Len()) != before.Size {
			return fail(ErrConfigChanged)
		}
		if n == 0 {
			return fail(io.ErrNoProgress)
		}
	}
	if err = h.check(ctx); err != nil {
		return fail(err)
	}
	return body.Bytes(), h, nil
}
