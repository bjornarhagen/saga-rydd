//go:build darwin || linux

package storeexport

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type heldDir struct {
	file       *os.File
	path, name string
	parent     *heldDir
	stamp      unix.Stat_t
	ownsParent bool
}

func sameObject(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid
}
func sameFile(a, b unix.Stat_t) bool {
	return sameObject(a, b) && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
func privateDir(s unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFDIR && s.Mode&0077 == 0 && s.Uid == uint32(os.Geteuid())
}
func privateFile(s unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFREG && s.Mode&0077 == 0 && s.Uid == uint32(os.Geteuid()) && s.Nlink == 1 && s.Size >= 0
}
func (d *heldDir) fd() int { return int(d.file.Fd()) }

func openDir(path string) (*heldDir, error) {
	if err := cleanPath(path); err != nil {
		return nil, err
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	d := &heldDir{file: os.NewFile(uintptr(fd), "/"), path: "/"}
	if err = unix.Fstat(fd, &d.stamp); err != nil {
		return nil, errors.Join(err, d.close())
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, e := d.child(part)
		if e != nil {
			return nil, errors.Join(e, d.close())
		}
		next.ownsParent = true
		d = next
	}
	if !privateDir(d.stamp) {
		return nil, errors.Join(ErrProfile, d.close())
	}
	return d, nil
}

func (d *heldDir) child(name string) (*heldDir, error) {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return nil, ErrProfile
	}
	fd, err := unix.Openat(d.fd(), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	c := &heldDir{file: os.NewFile(uintptr(fd), filepath.Join(d.path, name)), path: filepath.Join(d.path, name), name: name, parent: d}
	if err = unix.Fstat(fd, &c.stamp); err == nil {
		err = c.check()
	}
	if err != nil {
		return nil, errors.Join(err, c.close())
	}
	return c, nil
}

func (d *heldDir) check() error {
	if d == nil || d.file == nil {
		return ErrChanged
	}
	if d.parent != nil {
		if err := d.parent.check(); err != nil {
			return err
		}
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(d.fd(), &held); err != nil {
		return err
	}
	var err error
	if d.parent == nil {
		err = unix.Lstat("/", &named)
	} else {
		err = unix.Fstatat(d.parent.fd(), d.name, &named, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return err
	}
	if held.Mode&unix.S_IFMT != unix.S_IFDIR || !sameObject(d.stamp, held) || !sameObject(held, named) {
		return ErrChanged
	}
	return nil
}

func (d *heldDir) close() error {
	if d == nil {
		return nil
	}
	var err error
	if d.file != nil {
		err = d.file.Close()
		d.file = nil
	}
	if d.ownsParent {
		err = errors.Join(err, d.parent.close())
		d.ownsParent = false
	}
	return err
}

func (d *heldDir) stat(name string) (unix.Stat_t, error) {
	var s unix.Stat_t
	if err := d.check(); err != nil {
		return s, err
	}
	err := unix.Fstatat(d.fd(), name, &s, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil && !privateFile(s) {
		err = ErrProfile
	}
	return s, err
}

func (d *heldDir) names(g *guard, maximum int) (names []string, err error) {
	if err = g.check(); err != nil {
		return nil, err
	}
	if err = d.check(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(d.fd(), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), d.path)
	defer func() { joined(&err, f.Close()) }()
	for i := 0; i < 257; i++ {
		if err = g.check(); err != nil {
			return nil, err
		}
		rows, e := f.ReadDir(1)
		if err = g.check(); err != nil {
			return nil, err
		}
		if len(rows) > 1 {
			return nil, ErrBounds
		}
		if len(rows) == 1 {
			names = append(names, rows[0].Name())
			if len(names) > maximum {
				return nil, ErrBounds
			}
		}
		if e == io.EOF {
			if err = d.check(); err != nil {
				return nil, err
			}
			return names, nil
		}
		if e != nil {
			return nil, e
		}
		if len(rows) == 0 {
			return nil, io.ErrNoProgress
		}
	}
	return nil, ErrBounds
}

func (d *heldDir) exact(g *guard, expected ...string) error {
	names, err := d.names(g, len(expected))
	if err != nil {
		return err
	}
	if len(names) != len(expected) {
		return ErrChanged
	}
	want := map[string]bool{}
	for _, name := range expected {
		want[name] = true
	}
	for _, name := range names {
		if !want[name] {
			return ErrChanged
		}
		delete(want, name)
	}
	return nil
}

// stageAttemptError reports only the lexical inspection path after successful
// mkdir. The path does not prove that the created object remains at that name.
// No cleanup or reuse of an unproved child is authorized by this metadata.
type stageAttemptError struct {
	inspectionPath string
	cause          error
}

func (e *stageAttemptError) Error() string {
	return "plans export created a stage but its identity was not confirmed"
}
func (e *stageAttemptError) Unwrap() error { return e.cause }

func retainCandidateStage(result *ExportResult, stage *heldDir, err error) {
	if err == nil && stage != nil {
		result.Candidate = stage.path
		result.CandidateRetained = true
		result.CandidateIdentityObserved = true
		return
	}
	var attempt *stageAttemptError
	if errors.As(err, &attempt) {
		result.Candidate = attempt.inspectionPath
		result.CandidateRetained = true
		result.CandidateIdentityObserved = false
	}
}

func retainScratchStage(result *VerificationResult, stage *heldDir, err error) {
	if err == nil && stage != nil {
		result.Scratch = stage.path
		result.ScratchRetained = true
		result.ScratchIdentityObserved = true
		return
	}
	var attempt *stageAttemptError
	if errors.As(err, &attempt) {
		result.Scratch = attempt.inspectionPath
		result.ScratchRetained = true
		result.ScratchIdentityObserved = false
	}
}

func createStage(parent *heldDir, g *guard) (*heldDir, error) {
	return createStageWithOpener(parent, g, (*heldDir).child)
}

// The private opener seam permits generated post-mkdir refusal witnesses. The
// production opener still performs the same no-follow held/name proof.
func createStageWithOpener(parent *heldDir, g *guard, openChild func(*heldDir, string) (*heldDir, error)) (*heldDir, error) {
	if openChild == nil {
		return nil, ErrProfile
	}
	if _, err := parent.names(g, maxParentNames); err != nil {
		return nil, err
	}
	for i := 0; i < 16; i++ {
		if err := g.check(); err != nil {
			return nil, err
		}
		var token [8]byte
		if _, err := rand.Read(token[:]); err != nil {
			return nil, err
		}
		if err := g.check(); err != nil {
			return nil, err
		}
		name := ".rydd-export-" + hex.EncodeToString(token[:])
		if err := parent.check(); err != nil {
			return nil, err
		}
		if err := unix.Mkdirat(parent.fd(), name, 0700); errors.Is(err, unix.EEXIST) {
			continue
		} else if err != nil {
			return nil, err
		}
		inspectionPath := filepath.Join(parent.path, name)
		stage, err := openChild(parent, name)
		if err == nil && (stage == nil || stage.parent != parent || stage.name != name || stage.path != inspectionPath) {
			err = ErrChanged
		}
		if err == nil {
			err = stage.check()
		}
		if err == nil && (!privateDir(stage.stamp) || stage.stamp.Mode&0777 != 0700) {
			err = ErrProfile
		}
		if err != nil {
			return nil, &stageAttemptError{inspectionPath, errors.Join(err, stage.close())} // Never unlink an unproved created name.
		}
		return stage, nil
	}
	return nil, ErrBounds
}

func createFile(d *heldDir, name string) (*os.File, unix.Stat_t, error) {
	var s unix.Stat_t
	if err := d.check(); err != nil {
		return nil, s, err
	}
	fd, err := unix.Openat(d.fd(), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, s, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(d.path, name))
	if err = unix.Fstat(fd, &s); err != nil || !privateFile(s) || s.Mode&0777 != 0600 {
		if err == nil {
			err = ErrProfile
		}
		return nil, s, errors.Join(err, f.Close())
	}
	return f, s, nil
}

func openFile(d *heldDir, name string, expected *unix.Stat_t) (*os.File, unix.Stat_t, error) {
	s, err := d.stat(name)
	if err != nil {
		return nil, s, err
	}
	if expected != nil && !sameFile(*expected, s) {
		return nil, s, ErrChanged
	}
	fd, err := unix.Openat(d.fd(), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, s, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(d.path, name))
	var held unix.Stat_t
	if err = unix.Fstat(fd, &held); err != nil || !privateFile(held) || !sameFile(s, held) {
		if err == nil {
			err = ErrChanged
		}
		return nil, s, errors.Join(err, f.Close())
	}
	return f, held, nil
}

// streamFile reads exactly the admitted length plus one EOF sentinel. Output
// allocation is optional and only used for the <=64KiB manifest.
func streamFile(d *heldDir, name string, expected *unix.Stat_t, max int64, g *guard, out *os.File, keep bool) (digest string, body []byte, pin unix.Stat_t, err error) {
	if err = g.check(); err != nil {
		return
	}
	f, pin, e := openFile(d, name, expected)
	if e != nil {
		err = e
		return
	}
	defer func() { joined(&err, f.Close()) }()
	if pin.Size > max || pin.Size > maxDatabaseBytes {
		err = ErrBounds
		return
	}
	if keep && max > maxManifestBytes {
		err = ErrBounds
		return
	}
	if keep {
		body = make([]byte, 0, int(pin.Size))
	}
	h := sha256.New()
	buf := make([]byte, bufferBytes)
	left := pin.Size
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if err = g.check(); err != nil {
			return
		}
		want := int64(len(buf))
		if left < want {
			want = left
		}
		if left == 0 {
			want = 1
		}
		n, readErr := f.Read(buf[:int(want)])
		if err = g.check(); err != nil {
			return
		}
		if n < 0 || int64(n) > want {
			err = ErrBounds
			return
		}
		if left == 0 {
			if n != 0 || readErr != io.EOF {
				err = ErrChanged
				return
			}
			var held, named unix.Stat_t
			if err = unix.Fstat(int(f.Fd()), &held); err != nil {
				return
			}
			if named, err = d.stat(name); err != nil {
				return
			}
			if !sameFile(pin, held) || !sameFile(held, named) {
				err = ErrChanged
				return
			}
			digest = hex.EncodeToString(h.Sum(nil))
			return
		}
		if n == 0 {
			if readErr != nil {
				err = readErr
			} else {
				err = io.ErrNoProgress
			}
			return
		}
		left -= int64(n)
		_, _ = h.Write(buf[:n])
		if keep {
			body = append(body, buf[:n]...)
		}
		if out != nil {
			if err = g.check(); err != nil {
				return
			}
			written, writeErr := out.Write(buf[:n])
			if writeErr != nil {
				err = writeErr
				return
			}
			if written != n {
				err = io.ErrShortWrite
				return
			}
			if err = g.check(); err != nil {
				return
			}
		}
		if readErr != nil {
			err = readErr
			return
		}
	}
	err = ErrBounds
	return
}

// rollbackHeader checks only the twenty-byte SQLite signature/version prefix.
// It does not establish structural integrity or safe restoration. At most
// twenty positive bounded reads are allowed, with fresh guards and final held/
// named metadata checks; entered filesystem calls remain cooperative.
func rollbackHeader(d *heldDir, expected unix.Stat_t, g *guard) (err error) {
	if err = g.check(); err != nil {
		return
	}
	f, pin, err := openFile(d, "plans.sqlite3", &expected)
	if err != nil {
		return
	}
	defer func() { joined(&err, f.Close()); joined(&err, g.check()) }()
	if pin.Size < 20 || pin.Size > maxDatabaseBytes || pin.Mode&0777 != 0600 {
		return ErrProfile
	}
	var header [20]byte
	used := 0
	for attempts := 0; attempts < 20 && used < len(header); attempts++ {
		if err = g.check(); err != nil {
			return
		}
		n, readErr := f.Read(header[used:])
		if err = g.check(); err != nil {
			return
		}
		if n < 0 || n > len(header)-used {
			return ErrBounds
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		used += n
	}
	if used != len(header) {
		return ErrBounds
	}
	if string(header[:16]) != "SQLite format 3\x00" || header[18] != 1 || header[19] != 1 {
		return ErrProfile
	}
	if err = g.check(); err != nil {
		return
	}
	var actual unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &actual); err != nil {
		return
	}
	named, err := d.stat("plans.sqlite3")
	if err != nil {
		return err
	}
	if !sameFile(pin, actual) || !sameFile(actual, named) {
		return ErrChanged
	}
	return g.check()
}

// createdOutputPin checks only bounded metadata for an already held, exclusively
// created output. Neither the lexical name nor this observation grants cleanup
// authority or authenticates future same-user namespace changes.
func createdOutputPin(d *heldDir, name string, f *os.File, creation unix.Stat_t, size int64) (pin unix.Stat_t, err error) {
	if size < 0 || size > maxDatabaseBytes {
		return pin, ErrBounds
	}
	if f == nil || !privateFile(creation) || creation.Mode&0777 != 0600 {
		return pin, ErrChanged
	}
	if err = d.check(); err != nil {
		return
	}
	if err = unix.Fstat(int(f.Fd()), &pin); err != nil {
		return
	}
	if !privateFile(pin) || !sameObject(creation, pin) || pin.Mode&0777 != 0600 || pin.Size != size {
		return pin, ErrChanged
	}
	named, err := d.stat(name)
	if err != nil {
		return pin, err
	}
	if !sameFile(pin, named) {
		return pin, ErrChanged
	}
	return
}

func copyFile(from *heldDir, name string, expected unix.Stat_t, to *heldDir, g *guard) (digest string, pin unix.Stat_t, err error) {
	f, creationPin, err := createFile(to, "plans.sqlite3")
	if err != nil {
		return
	}
	defer func() { joined(&err, f.Close()) }()
	digest, _, _, err = streamFile(from, name, &expected, maxDatabaseBytes, g, f, false)
	if err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	pin, err = createdOutputPin(to, "plans.sqlite3", f, creationPin, expected.Size)
	return
}

func writeFile(d *heldDir, name string, body []byte, g *guard) (pin unix.Stat_t, err error) {
	if len(body) > maxManifestBytes {
		return pin, ErrBounds
	}
	if err = g.check(); err != nil {
		return
	}
	f, creationPin, err := createFile(d, name)
	if err != nil {
		return
	}
	defer func() { joined(&err, f.Close()) }()
	n, e := f.Write(body)
	if e != nil {
		err = e
		return
	}
	if n != len(body) {
		err = io.ErrShortWrite
		return
	}
	if err = g.check(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	pin, err = createdOutputPin(d, name, f, creationPin, int64(len(body)))
	return
}

// removeScratch is the entire removal scope. The engine must be known closed,
// and only the exact copied file and empty exact child can be unlinked.
func removeScratch(scratch *heldDir, pin unix.Stat_t, g *guard) error {
	if err := scratch.exact(g, "plans.sqlite3"); err != nil {
		return err
	}
	named, err := scratch.stat("plans.sqlite3")
	if err != nil {
		return err
	}
	if !sameFile(pin, named) {
		return ErrChanged
	}
	if err = g.check(); err != nil {
		return err
	}
	if err = unix.Unlinkat(scratch.fd(), "plans.sqlite3", 0); err != nil {
		return err
	}
	if err = scratch.exact(g); err != nil {
		return err
	}
	if err = scratch.check(); err != nil {
		return err
	}
	if err = g.check(); err != nil {
		return err
	}
	return unix.Unlinkat(scratch.parent.fd(), scratch.name, unix.AT_REMOVEDIR)
}

type sourceStamp struct {
	main unix.Stat_t
	wal  *unix.Stat_t
}

func sourceMetadata(d *heldDir) (sourceStamp, error) {
	var result sourceStamp
	main, err := d.stat("plans.sqlite3")
	if err != nil {
		return result, err
	}
	result.main = main
	if main.Size > maxDatabaseBytes {
		return result, ErrBounds
	}
	if _, err = d.stat("plans.sqlite3-journal"); !errors.Is(err, unix.ENOENT) {
		if err == nil {
			err = ErrProfile
		}
		return result, err
	}
	for _, name := range []string{"plans.sqlite3-wal", "plans.sqlite3-shm"} {
		s, e := d.stat(name)
		if errors.Is(e, unix.ENOENT) {
			continue
		}
		if e != nil {
			return result, e
		}
		if s.Size > maxSidecarBytes {
			return result, ErrBounds
		}
		if name == "plans.sqlite3-wal" {
			result.wal = &s
			if s.Size > maxDatabaseBytes-main.Size {
				return result, ErrBounds
			}
		}
	}
	return result, nil
}

func unchangedSource(before, after sourceStamp) bool {
	if !sameFile(before.main, after.main) || (before.wal == nil) != (after.wal == nil) {
		return false
	}
	return before.wal == nil || sameFile(*before.wal, *after.wal)
}
