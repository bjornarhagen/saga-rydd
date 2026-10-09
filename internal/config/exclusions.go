//go:build darwin || linux

package config

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sys/unix"
)

const ExclusionPathLimit = 128
const ExclusionDirectoryEntryLimit = 256

var ErrExclusionInput = errors.New("exclusion requires add or remove with one valid absolute path")
var ErrExclusionBounds = errors.New("exclusion editor supports at most 128 roots and 128 exclusions in a 1 MiB configuration")
var ErrExclusionPublication = errors.New("configuration publication or its reply is uncertain; inspect exclude --list before another change")

type ExclusionResult struct {
	Action             string   `json:"action"`
	Path               string   `json:"path,omitempty"`
	PathBytes          []byte   `json:"path_bytes,omitempty"`
	ConfigFile         string   `json:"config_file"`
	ConfigFileBytes    []byte   `json:"config_file_bytes"`
	StateDir           string   `json:"state_dir"`
	StateDirBytes      []byte   `json:"state_dir_bytes"`
	Exclusions         []string `json:"exclusions"`
	ExclusionPathBytes [][]byte `json:"exclusion_path_bytes"`
	ConfigSHA256       string   `json:"config_sha256"`
	Changed            bool     `json:"changed"`
	Publication        string   `json:"publication"`
	SyncCompleted      bool     `json:"sync_completed"`
}

type exclusionHooks struct {
	afterReadChunk func(int)
	beforePublish  func()
	afterPublish   func()
	syncParent     func(*os.File) error
	rename         func(int, string, int, string) error
}

func validExclusionPath(path string) bool {
	return path != "" && len(path) <= 4096 && utf8.ValidString(path) && !strings.ContainsRune(path, 0) && filepath.IsAbs(path)
}

func decodeEditorConfig(data []byte, home string) (Config, error) {
	_, normalized, err := decodeEditorConfigs(data, home)
	return normalized, err
}

func decodeEditorConfigs(data []byte, home string) (Config, Config, error) {
	c := Default()
	if len(data) > maxConfigBytes {
		return c, Config{}, ErrExclusionBounds
	}
	if err := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&c); err != nil {
		return c, Config{}, fmt.Errorf("decode config: %w", err)
	}
	// Check counts before Validate's quadratic root and exclusion comparisons.
	if len(c.Roots) > ExclusionPathLimit || len(c.Excludes) > ExclusionPathLimit {
		return c, Config{}, ErrExclusionBounds
	}
	for _, paths := range [][]string{c.Roots, c.Excludes} {
		for _, path := range paths {
			if len(path) > 4096 || !utf8.ValidString(path) || strings.ContainsRune(path, 0) {
				return c, Config{}, ErrExclusionBounds
			}
		}
	}
	// Validate normalizes its slices. Keep literal settings independent so an
	// unrelated ~/ root or exclusion retains its portable on-disk value.
	normalized := c
	normalized.Roots = append([]string{}, c.Roots...)
	normalized.Excludes = append([]string{}, c.Excludes...)
	if err := normalized.Validate(home); err != nil {
		return c, Config{}, err
	}
	for _, paths := range [][]string{normalized.Roots, normalized.Excludes} {
		for _, path := range paths {
			if !validExclusionPath(path) {
				return c, Config{}, ErrExclusionBounds
			}
		}
	}
	return c, normalized, nil
}

func exclusionResult(paths Paths, action, path string, exclusions []string, data []byte) ExclusionResult {
	r := ExclusionResult{Action: action, Path: path, ConfigFile: paths.ConfigFile, ConfigFileBytes: []byte(paths.ConfigFile), StateDir: paths.StateDir, StateDirBytes: []byte(paths.StateDir), Exclusions: append([]string{}, exclusions...), ExclusionPathBytes: make([][]byte, len(exclusions)), ConfigSHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Publication: "not_requested"}
	if path != "" {
		r.PathBytes = []byte(path)
	}
	for i, exclusion := range exclusions {
		r.ExclusionPathBytes[i] = []byte(exclusion)
	}
	return r
}

// ListExclusions reads an existing bounded configuration. It creates no lock,
// directory or database and does not inspect any configured source path.
func ListExclusions(ctx context.Context, paths Paths, home string) (ExclusionResult, error) {
	data, held, err := readStableConfig(ctx, paths.ConfigFile, nil)
	if err != nil {
		return ExclusionResult{}, err
	}
	defer held.close()
	cfg, err := decodeEditorConfig(data, home)
	if err != nil {
		return ExclusionResult{}, err
	}
	if err = held.check(ctx); err != nil {
		return ExclusionResult{}, err
	}
	return exclusionResult(paths, "list", "", cfg.Excludes, data), nil
}

// EditExclusion coordinates with the existing global state writer lock. Only
// later invocations use the resulting settings; active manual work is unchanged.
// A returned candidate with ErrExclusionPublication must be inspected, not
// treated as proof either that publication succeeded or that nothing changed.
func EditExclusion(ctx context.Context, paths Paths, home, action, path string) (ExclusionResult, error) {
	return editExclusion(ctx, paths, home, action, path, exclusionHooks{})
}

func editExclusion(ctx context.Context, paths Paths, home, action, path string, hooks exclusionHooks) (ExclusionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExclusionResult{}, err
	}
	if action != "add" && action != "remove" || !validExclusionPath(path) {
		return ExclusionResult{}, ErrExclusionInput
	}
	path = filepath.Clean(path)
	if !configStoragePath(paths.StateDir) {
		return ExclusionResult{}, errors.New("configured state directory must be one absolute canonical path")
	}
	lock, err := localfs.AcquireExistingLock(ctx, paths.StateDir)
	if err != nil {
		return ExclusionResult{}, fmt.Errorf("acquire existing configured state writer lock: %w", err)
	}
	defer lock.Close()
	data, held, err := readStableConfig(ctx, paths.ConfigFile, hooks.afterReadChunk)
	if err != nil {
		return ExclusionResult{}, err
	}
	defer held.close()
	if lock.Identity() == (localfs.ObjectIdentity{Device: fmt.Sprint(held.stamp.Dev), Inode: fmt.Sprint(held.stamp.Ino)}) {
		return ExclusionResult{}, errors.New("configuration cannot be the held writer lock object")
	}
	raw, cfg, err := decodeEditorConfigs(data, home)
	if err != nil {
		return ExclusionResult{}, err
	}
	next := make([]string, 0, len(cfg.Excludes)+1)
	nextRaw := make([]string, 0, len(raw.Excludes)+1)
	found := false
	for i, exclusion := range cfg.Excludes {
		if exclusion == path {
			found = true
			if action == "remove" {
				continue
			}
		}
		next = append(next, exclusion)
		nextRaw = append(nextRaw, raw.Excludes[i])
	}
	changed := action == "add" && !found || action == "remove" && found
	if !changed {
		if err = held.check(ctx); err != nil {
			return ExclusionResult{}, err
		}
		if err = lock.Check(ctx); err != nil {
			return ExclusionResult{}, err
		}
		r := exclusionResult(paths, action, path, cfg.Excludes, data)
		r.Publication = "not_needed"
		return r, nil
	}
	if action == "add" {
		if len(next) >= ExclusionPathLimit {
			return ExclusionResult{}, ErrExclusionBounds
		}
		next = append(next, path)
		nextRaw = append(nextRaw, path)
	}
	cfg.Excludes = next
	if err = cfg.Validate(home); err != nil {
		return ExclusionResult{}, fmt.Errorf("%w: %w", ErrExclusionInput, err)
	}
	raw.Excludes = nextRaw
	candidate, err := toml.Marshal(raw)
	if err != nil {
		return ExclusionResult{}, err
	}
	if len(candidate) > maxConfigBytes {
		return ExclusionResult{}, ErrExclusionBounds
	}
	r := exclusionResult(paths, action, path, cfg.Excludes, candidate)
	r.Changed = true
	return publishExclusions(ctx, held, lock, candidate, r, hooks)
}

func publishExclusions(ctx context.Context, original *heldConfig, lock *localfs.ExistingLock, data []byte, result ExclusionResult, hooks exclusionHooks) (ExclusionResult, error) {
	if err := original.check(ctx); err != nil {
		return ExclusionResult{}, err
	}
	if err := lock.Check(ctx); err != nil {
		return ExclusionResult{}, err
	}
	if err := checkExclusionDirectoryBudget(ctx, original); err != nil {
		return ExclusionResult{}, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ExclusionResult{}, err
	}
	name := ".config-exclusion-" + hex.EncodeToString(nonce[:]) + ".tmp"
	parentFD := int(original.parent.Fd())
	fd, err := unix.Openat(parentFD, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return ExclusionResult{}, err
	}
	temp := os.NewFile(uintptr(fd), filepath.Join(filepath.Dir(original.path), name))
	defer temp.Close()
	var initial unix.Stat_t
	if err = unix.Fstat(fd, &initial); err != nil {
		return ExclusionResult{}, err
	}
	defer func() {
		var named unix.Stat_t
		if unix.Fstatat(parentFD, name, &named, unix.AT_SYMLINK_NOFOLLOW) == nil && named.Dev == initial.Dev && named.Ino == initial.Ino {
			_ = unix.Unlinkat(parentFD, name, 0)
		}
	}()
	for offset := 0; offset < len(data); {
		if err = ctx.Err(); err != nil {
			return ExclusionResult{}, err
		}
		n, writeErr := temp.Write(data[offset:min(offset+(32<<10), len(data))])
		offset += n
		if writeErr != nil {
			return ExclusionResult{}, writeErr
		}
		if n == 0 {
			return ExclusionResult{}, io.ErrNoProgress
		}
	}
	if err = ctx.Err(); err != nil {
		return ExclusionResult{}, err
	}
	if err = temp.Sync(); err != nil {
		return ExclusionResult{}, err
	}
	var prepared unix.Stat_t
	if err = unix.Fstat(fd, &prepared); err != nil {
		return ExclusionResult{}, err
	}
	if !privateConfigFile(prepared) || prepared.Size != int64(len(data)) {
		return ExclusionResult{}, ErrConfigChanged
	}
	if hooks.beforePublish != nil {
		hooks.beforePublish()
	}
	if err = original.check(ctx); err != nil {
		return ExclusionResult{}, err
	}
	if err = lock.Check(ctx); err != nil {
		return ExclusionResult{}, err
	}
	var held, named unix.Stat_t
	if err = unix.Fstat(fd, &held); err != nil {
		return ExclusionResult{}, err
	}
	if err = unix.Fstatat(parentFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ExclusionResult{}, err
	}
	if !sameConfigStamp(prepared, held) || !sameConfigStamp(held, named) {
		return ExclusionResult{}, ErrConfigChanged
	}
	if err = ctx.Err(); err != nil {
		return ExclusionResult{}, err
	}
	rename := unix.Renameat
	if hooks.rename != nil {
		rename = hooks.rename
	}
	// Once replacement is attempted, even its error cannot prove no publication.
	if err = rename(parentFD, name, parentFD, original.name); err != nil {
		return uncertainExclusion(result, errors.Join(err, ctx.Err()))
	}
	var published unix.Stat_t
	if err = unix.Fstat(fd, &published); err != nil {
		return uncertainExclusion(result, err)
	}
	if hooks.afterPublish != nil {
		hooks.afterPublish()
	}
	if err = checkPublishedConfig(ctx, original, temp, published); err != nil {
		return uncertainExclusion(result, err)
	}
	if err = lock.Check(ctx); err != nil {
		return uncertainExclusion(result, err)
	}
	syncParent := func(file *os.File) error { return file.Sync() }
	if hooks.syncParent != nil {
		syncParent = hooks.syncParent
	}
	if err = syncParent(original.parent); err != nil {
		return uncertainExclusion(result, err)
	}
	if err = checkPublishedConfig(ctx, original, temp, published); err != nil {
		return uncertainExclusion(result, err)
	}
	if err = lock.Check(ctx); err != nil {
		return uncertainExclusion(result, err)
	}
	if err = temp.Close(); err != nil {
		return uncertainExclusion(result, err)
	}
	if err = ctx.Err(); err != nil {
		return uncertainExclusion(result, err)
	}
	result.Publication, result.SyncCompleted = "saved", true
	return result, nil
}

func checkPublishedConfig(ctx context.Context, original *heldConfig, temp *os.File, expected unix.Stat_t) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var held, named, parent, namedParent unix.Stat_t
	if err := unix.Fstat(int(temp.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(original.parent.Fd()), original.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if err := unix.Fstat(int(original.parent.Fd()), &parent); err != nil {
		return err
	}
	if err := unix.Lstat(filepath.Dir(original.path), &namedParent); err != nil {
		return err
	}
	if !privateConfigFile(held) || !privateConfigFile(named) || !sameConfigStamp(expected, held) || !sameConfigStamp(held, named) || !privateConfigParent(parent) || !privateConfigParent(namedParent) || !sameConfigParent(original.parentStamp, parent) || !sameConfigParent(parent, namedParent) {
		return ErrConfigChanged
	}
	return ctx.Err()
}

func uncertainExclusion(result ExclusionResult, cause error) (ExclusionResult, error) {
	result.Publication = "uncertain"
	return result, fmt.Errorf("%w: %s %q in %q, candidate SHA-256 %s; inspect exclude --list: %w", ErrExclusionPublication, result.Action, string(result.PathBytes), string(result.ConfigFileBytes), result.ConfigSHA256, cause)
}

// An interrupted publication may leave its private temporary behind. Count only
// immediate private names, without opening them or cleaning historical artifacts.
// A fresh descriptor avoids sharing enumeration offsets with the held parent.
func checkExclusionDirectoryBudget(ctx context.Context, original *heldConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, err := unix.Openat(int(original.parent.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(fd), filepath.Dir(original.path))
	defer dir.Close()
	var stamp unix.Stat_t
	if err = unix.Fstat(fd, &stamp); err != nil {
		return err
	}
	if !privateConfigParent(stamp) || !sameConfigParent(original.parentStamp, stamp) {
		return ErrConfigChanged
	}
	count := 0
	for count < ExclusionDirectoryEntryLimit {
		if err = ctx.Err(); err != nil {
			return err
		}
		names, readErr := dir.Readdirnames(min(64, ExclusionDirectoryEntryLimit-count))
		count += len(names)
		if err = ctx.Err(); err != nil {
			return err
		}
		if count >= ExclusionDirectoryEntryLimit {
			return fmt.Errorf("%w: private configuration directory has at least %d entries; no temporary file was created", ErrExclusionBounds, ExclusionDirectoryEntryLimit)
		}
		if readErr != nil {
			if readErr == io.EOF {
				return original.check(ctx)
			}
			return readErr
		}
		if len(names) == 0 {
			return io.ErrNoProgress
		}
	}
	return ErrExclusionBounds
}
