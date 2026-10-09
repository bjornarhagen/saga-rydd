package powerinfo

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const resolveScope = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV

type linuxSource struct {
	root      int
	rootPath  string
	class     *os.File
	rootID    unix.Stat_t
	classID   unix.Stat_t
	rootMount uint64
}

type linuxSupply struct{ fd int }

func platformOpen(ctx context.Context) (source, error) {
	return openLinuxSource(ctx, "/sys", true)
}

// Alternate paths and relaxed magic verification exist only as private
// generated-fixture seams. Observe never accepts a caller-provided root.
func openLinuxSource(ctx context.Context, path string, requireSysfs bool) (source, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	root, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, sourceError{"sysfs_root_unavailable"}
	}
	s := &linuxSource{root: root, rootPath: path}
	failed := true
	defer func() {
		if failed {
			_ = s.close()
		}
	}()
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if err := unix.Fstat(root, &s.rootID); err != nil || s.rootID.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, sourceError{"sysfs_root_unavailable"}
	}
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(root, &fs); err != nil {
		return nil, sourceError{"sysfs_root_unavailable"}
	}
	if requireSysfs && uint64(fs.Type) != unix.SYSFS_MAGIC {
		return nil, sourceError{"sysfs_filesystem_unconfirmed"}
	}
	if s.rootMount, err = mountID(ctx, root); err != nil {
		return nil, err
	}
	class, err := openRelativeDirectory(ctx, root, "class/power_supply")
	if err != nil {
		return nil, err
	}
	s.class = os.NewFile(uintptr(class), "power-supply-class")
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if err := unix.Fstat(class, &s.classID); err != nil {
		return nil, sourceError{"power_supply_class_unavailable"}
	}
	failed = false
	return s, nil
}

func openRelativeDirectory(ctx context.Context, root int, name string) (int, error) {
	if err := canceled(ctx); err != nil {
		return -1, err
	}
	fd, err := unix.Openat2(root, name, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: resolveScope})
	if err != nil {
		return -1, resolutionError(err)
	}
	return fd, nil
}

func resolutionError(err error) error {
	switch {
	case errors.Is(err, unix.ENOSYS), errors.Is(err, unix.EINVAL):
		return sourceError{"resolver_unsupported"}
	case errors.Is(err, unix.EXDEV), errors.Is(err, unix.ELOOP), errors.Is(err, unix.EAGAIN):
		return sourceError{"resolution_refused"}
	default:
		return sourceError{"provider_unavailable"}
	}
}

func (s *linuxSource) names(ctx context.Context, limit int) ([]string, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	names, err := s.class.Readdirnames(limit)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return names, err
}

func sameDirectory(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode&unix.S_IFMT == unix.S_IFDIR && b.Mode&unix.S_IFMT == unix.S_IFDIR
}

func mountID(ctx context.Context, fd int) (uint64, error) {
	if err := canceled(ctx); err != nil {
		return 0, err
	}
	var st unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &st); err != nil || st.Mask&unix.STATX_MNT_ID == 0 {
		return 0, sourceError{"mount_identity_unavailable"}
	}
	return st.Mnt_id, nil
}

func (s *linuxSource) checkClass(ctx context.Context) error {
	fd, err := openRelativeDirectory(ctx, s.root, "class/power_supply")
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := canceled(ctx); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || !sameDirectory(st, s.classID) {
		return sourceError{"scope_changed"}
	}
	return nil
}

func (s *linuxSource) provider(ctx context.Context, name string) (supply, error) {
	if len(name) == 0 || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, sourceError{"resolution_refused"}
	}
	if err := s.checkClass(ctx); err != nil {
		return nil, err
	}
	fd, err := openRelativeDirectory(ctx, s.root, "class/power_supply/"+name)
	if err != nil {
		return nil, err
	}
	if err := s.checkClass(ctx); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &linuxSupply{fd: fd}, nil
}

func (s *linuxSource) check(ctx context.Context) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	// Opening the named root for comparison reads no attribute body. The
	// fixed production name is /sys; fixtures retain their generated name.
	var st unix.Stat_t
	if err := unix.Fstat(s.root, &st); err != nil || !sameDirectory(st, s.rootID) {
		return sourceError{"scope_changed"}
	}
	if err := canceled(ctx); err != nil {
		return err
	}
	named, err := unix.Open(s.rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return sourceError{"scope_changed"}
	}
	defer func() {
		if named >= 0 {
			_ = unix.Close(named)
		}
	}()
	if err := canceled(ctx); err != nil {
		return err
	}
	if err := unix.Fstat(named, &st); err != nil || !sameDirectory(st, s.rootID) {
		return sourceError{"scope_changed"}
	}
	if mount, err := mountID(ctx, named); err != nil || mount != s.rootMount {
		return sourceError{"scope_changed"}
	}
	// Close the temporary named-root descriptor before checkClass opens its
	// temporary descriptor; the observation retains at most four FDs.
	owned := named
	named = -1
	if err := unix.Close(owned); err != nil {
		return sourceError{"scope_changed"}
	}
	return s.checkClass(ctx)
}

func (s *linuxSource) close() error {
	var classErr, rootErr error
	if s.class != nil {
		classErr = s.class.Close()
		s.class = nil
	}
	if s.root >= 0 {
		rootErr = unix.Close(s.root)
		s.root = -1
	}
	return errors.Join(classErr, rootErr)
}

func (p *linuxSupply) attribute(ctx context.Context, name string) ([]byte, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	fd, err := unix.Openat2(p.fd, name, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK | unix.O_NOFOLLOW, Resolve: resolveScope | unix.RESOLVE_NO_SYMLINKS})
	if err != nil {
		return nil, resolutionError(err)
	}
	defer unix.Close(fd)
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, sourceError{"attribute_kind_refused"}
	}
	// sysfs can report PAGE_SIZE in st.Size. Bound returned bytes rather
	// than treating that metadata size as the actual attribute length.
	return readAttribute(ctx, func(b []byte) (int, error) { return unix.Read(fd, b) })
}

func readAttribute(ctx context.Context, read func([]byte) (int, error)) ([]byte, error) {
	var buf [MaxAttributeBytes + 1]byte
	used := 0
	// Each positive short read consumes at least one byte. At most 64 such
	// reads plus EOF can succeed; the 65th byte is an overflow sentinel.
	for calls := 0; calls < MaxAttributeBytes+1; calls++ {
		if err := canceled(ctx); err != nil {
			return append([]byte(nil), buf[:used]...), err
		}
		n, err := read(buf[used:])
		if n < 0 || n > len(buf)-used {
			return append([]byte(nil), buf[:used]...), sourceError{"malformed_attribute"}
		}
		used += n
		if used > MaxAttributeBytes {
			return append([]byte(nil), buf[:used]...), sourceError{"attribute_byte_limit"}
		}
		if err != nil {
			return append([]byte(nil), buf[:used]...), err
		}
		if n == 0 {
			return append([]byte(nil), buf[:used]...), nil
		}
	}
	return append([]byte(nil), buf[:used]...), sourceError{"attribute_attempt_limit"}
}

func (p *linuxSupply) close() error {
	if p.fd < 0 {
		return nil
	}
	err := unix.Close(p.fd)
	p.fd = -1
	return err
}
