// Package inventory reads metadata only. It never opens ordinary file contents.
// A single bounded directory stream is retained between committed batches.
package inventory

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

type Scanner struct {
	mu           sync.Mutex
	closed       atomic.Bool
	roots        map[string]bool
	excludes     []string
	protectedIDs map[string]bool
	current      *stream
}
type stream struct {
	file       *os.File
	jobID      int64
	cursor     []byte
	generation int64
	stamp      unix.Stat_t
}

func New(roots, excludes, privatePaths []string) (*Scanner, error) {
	s := &Scanner{roots: make(map[string]bool), protectedIDs: make(map[string]bool)}
	protected := append([]string{}, excludes...)
	protected = append(protected, privatePaths...)
	protected = append(protected, "/proc", "/sys", "/dev", "/run", "/System", "/Library", "/usr", "/bin", "/sbin", "/etc", "/private/etc")
	if home, err := os.UserHomeDir(); err == nil {
		protected = append(protected, filepath.Join(home, "Library"))
	}
	for _, p := range protected {
		var st unix.Stat_t
		if unix.Stat(p, &st) == nil {
			s.protectedIDs[objectID(st)] = true
		}
		s.excludes = append(s.excludes, filepath.Clean(p))
		if canonical, err := filepath.EvalSymlinks(p); err == nil {
			s.excludes = append(s.excludes, canonical)
		}
	}
	var canonicalRoots []string
	var infos []os.FileInfo
	for _, root := range roots {
		s.roots[root] = true
		canonical, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		} // Unavailable roots become saved job errors.
		info, err := os.Stat(canonical)
		if err != nil {
			continue
		}
		for i, prior := range canonicalRoots {
			if config.Within(canonical, prior) || config.Within(prior, canonical) || os.SameFile(info, infos[i]) {
				return nil, errors.New("scan roots physically overlap or alias each other")
			}
		}
		canonicalRoots = append(canonicalRoots, canonical)
		infos = append(infos, info)
	}
	return s, nil
}

func (s *Scanner) reset() {
	if s.current != nil {
		s.current.file.Close()
		s.current = nil
	}
}

// Close must not block worker shutdown behind an uninterruptible filesystem
// call. An in-flight Next releases its descriptor when that call returns.
func (s *Scanner) Close() {
	s.closed.Store(true)
	if s.mu.TryLock() {
		s.reset()
		s.mu.Unlock()
	}
}

func (s *Scanner) openat(fd int, name string) (int, error) {
	var before, after unix.Stat_t
	if err := unix.Fstatat(fd, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return -1, err
	}
	if dataless(before) || s.protectedIDs[objectID(before)] {
		return -1, errors.New("dataless or protected directory")
	}
	next, err := unix.Openat(fd, name, openFlags, 0)
	if err != nil {
		return -1, err
	}
	if err := unix.Fstat(next, &after); err != nil {
		unix.Close(next)
		return -1, err
	}
	if dataless(after) || objectID(before) != objectID(after) {
		unix.Close(next)
		return -1, errors.New("directory changed while opening")
	}
	return next, nil
}

func (s *Scanner) excluded(path string) bool {
	for _, p := range s.excludes {
		if config.Within(path, p) {
			return true
		}
	}
	switch filepath.Base(path) {
	case ".git", ".Trash", ".Trashes", ".snapshots", "Backups.backupdb":
		return true
	}
	return false
}

func validPath(path string) bool {
	return path != "" && len(path) <= 4096 && !filepath.IsAbs(path) && filepath.Clean(path) == path && path != ".." && !strings.HasPrefix(path, "../") && !strings.ContainsRune(path, 0)
}

const openFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK

// Open every component relative to a directory descriptor. Only configured root
// aliases are resolved, before opening; descendants are never followed.
func (s *Scanner) openAbsolute(ctx context.Context, path string) (*os.File, error) {
	fd, err := unix.Open("/", openFlags, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			unix.Close(fd)
			return nil, err
		}
		next, err := s.openat(fd, part)
		unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), path), nil
}

func objectID(st unix.Stat_t) string { return fmt.Sprintf("%d:%d", st.Dev, st.Ino) }

func statFile(f *os.File) (unix.Stat_t, error) {
	var st unix.Stat_t
	err := unix.Fstat(int(f.Fd()), &st)
	return st, err
}
func sameStamp(a, b unix.Stat_t) bool {
	am, ac := timestamps(&a)
	bm, bc := timestamps(&b)
	return a.Dev == b.Dev && a.Ino == b.Ino && am == bm && ac == bc
}

func observation(path string, st unix.Stat_t) state.Entry {
	kind := "other"
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		kind = "file"
	case unix.S_IFDIR:
		kind = "directory"
	case unix.S_IFLNK:
		kind = "symlink"
	}
	mtime, ctime := timestamps(&st)
	return state.Entry{Path: []byte(path), Kind: kind, Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), Size: max(st.Size, 0), Allocated: max(st.Blocks, 0) * 512, MtimeNS: mtime, CtimeNS: ctime}
}

func (s *Scanner) open(ctx context.Context, j state.Job) (*os.File, string, string, error) {
	root := string(j.RootPath)
	path := string(j.Path)
	if !s.roots[root] || !validPath(path) {
		return nil, "", "", errors.New("invalid scan scope")
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, "", "", err
	}
	if s.excluded(root) || s.excluded(canonical) || s.excluded(filepath.Join(root, path)) || s.excluded(filepath.Join(canonical, path)) {
		return nil, "", "", errors.New("directory is excluded or protected")
	}
	f, err := s.openAbsolute(ctx, canonical)
	if err != nil {
		return nil, "", "", err
	}
	fail := func(err error) (*os.File, string, string, error) { f.Close(); return nil, "", "", err }
	st, err := statFile(f)
	if err != nil {
		return fail(err)
	}
	volume, mount, err := filesystem(int(f.Fd()))
	if err != nil {
		return fail(err)
	}
	// Mount IDs are for live boundary checks, not persistent identity: they
	// change when the same filesystem is mounted again or in another namespace.
	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("%x:%s:%d:%d", []byte(canonical), volume, st.Dev, st.Ino)))
	identity := fmt.Sprintf("v1:%x", fingerprint)
	if j.RootIdentity != "" && j.RootIdentity != identity {
		return fail(errors.New("root or volume identity changed; previous inventory retained"))
	}
	if path != "." {
		for _, part := range strings.Split(path, "/") {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			fd, err := s.openat(int(f.Fd()), part)
			if err != nil {
				return fail(err)
			}
			f.Close()
			f = os.NewFile(uintptr(fd), path)
			v, m, err := filesystem(fd)
			if err != nil {
				return fail(err)
			}
			if v != volume || m != mount {
				return fail(errors.New("nested mount boundary"))
			}
		}
	}
	return f, identity, canonical, nil
}

// Next returns at most 128 child observations. A process restart or lost cursor
// restarts enumeration with a new generation; directory order/offsets are never
// treated as portable durable cursors. Upserts make replay idempotent.
func (s *Scanner) Next(ctx context.Context, j state.Job) (state.ScanBatch, error) {
	s.mu.Lock()
	defer func() {
		if s.closed.Load() {
			s.reset()
		}
		s.mu.Unlock()
	}()
	if s.closed.Load() {
		return state.ScanBatch{}, errors.New("scanner closed")
	}
	fault := func(err error) (state.ScanBatch, error) {
		s.reset()
		if ctx.Err() != nil {
			return state.ScanBatch{}, ctx.Err()
		}
		message := err.Error()
		if len(message) > 2048 {
			message = message[:2048]
		}
		return state.ScanBatch{Fault: message}, nil
	}
	f, identity, canonical, err := s.open(ctx, j)
	if err != nil {
		return fault(err)
	}
	st, err := statFile(f)
	if err != nil {
		f.Close()
		return fault(err)
	}
	if s.current != nil && (s.current.jobID != j.ID || !bytes.Equal(s.current.cursor, j.Cursor) || !sameStamp(s.current.stamp, st)) {
		s.reset()
	}
	if s.current == nil {
		var token [8]byte
		if _, err := rand.Read(token[:]); err != nil {
			f.Close()
			return fault(err)
		}
		generation := int64(binary.BigEndian.Uint64(token[:]) & 0x7fffffffffffffff)
		if generation == 0 {
			generation = 1
		}
		s.current = &stream{file: f, jobID: j.ID, generation: generation, stamp: st}
	} else {
		f.Close()
	}
	stream := s.current
	parentVolume, parentMount, err := filesystem(int(stream.file.Fd()))
	if err != nil {
		return fault(err)
	}
	b := state.ScanBatch{Identity: identity, Generation: stream.generation, Directory: observation(string(j.Path), st)}
	names, readErr := stream.file.Readdirnames(state.MaxBatchEntries)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fault(readErr)
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return fault(err)
		}
		if name == "." || name == ".." || strings.ContainsRune(name, '/') {
			return fault(errors.New("invalid directory entry"))
		}
		var child unix.Stat_t
		if err := unix.Fstatat(int(stream.file.Fd()), name, &child, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return fault(err)
		}
		path := filepath.Join(string(j.Path), name)
		if !validPath(path) {
			return fault(errors.New("entry path exceeds supported bounds"))
		}
		e := observation(path, child)
		if dataless(child) {
			e.SkipReason = "dataless placeholder"
		}
		if s.protectedIDs[objectID(child)] || s.excluded(filepath.Join(string(j.RootPath), path)) || s.excluded(filepath.Join(canonical, path)) {
			e.SkipReason = "excluded or protected"
		}
		if e.Kind == "directory" && e.SkipReason == "" {
			fd, err := s.openat(int(stream.file.Fd()), name)
			if err != nil {
				e.SkipReason = "directory unavailable"
			} else {
				v, m, err := filesystem(fd)
				unix.Close(fd)
				if err != nil || v != parentVolume || m != parentMount {
					e.SkipReason = "unsupported filesystem or mount boundary"
				}
			}
		}
		b.Entries = append(b.Entries, e)
	}
	// Reopen the path as well as fstat'ing the stream: a renamed/replaced
	// ancestor must not let an old descriptor certify the current pathname.
	check, _, _, err := s.open(ctx, j)
	if err != nil {
		return fault(err)
	}
	end, err := statFile(check)
	check.Close()
	if err != nil {
		return fault(err)
	}
	current, err := statFile(stream.file)
	if err != nil {
		return fault(err)
	}
	if !sameStamp(stream.stamp, end) || !sameStamp(stream.stamp, current) {
		return fault(errors.New("directory changed during enumeration; retry required"))
	}
	b.Complete = errors.Is(readErr, io.EOF)
	if b.Complete {
		s.reset()
	} else {
		b.Cursor = make([]byte, 16)
		binary.BigEndian.PutUint64(b.Cursor, uint64(stream.generation))
		if _, err := rand.Read(b.Cursor[8:]); err != nil {
			return fault(err)
		}
		stream.cursor = bytes.Clone(b.Cursor)
	}
	return b, nil
}
