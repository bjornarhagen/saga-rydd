package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	kernelIOStatLimit   = 4096
	kernelIOStatusLimit = 4096
	kernelIOSelfLimit   = 11 // maximum signed 32-bit PID plus a truncation sentinel
	kernelIODataLimit   = 1024
	kernelIOReadCalls   = 4
	procResolve         = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV
)

type procIdentity struct {
	pid, parent int64
	start       uint64
}
type linuxKernelIO struct {
	fd       int
	device   uint64
	inode    uint64
	identity procIdentity
	ops      procIOOps
}

// This private seam lets fixtures count the exact fixed-profile operations.
// The production implementation opens only /proc and retained relative names.
type procIOOps interface {
	open(string, int, uint32) (int, error)
	openat2(int, string, *unix.OpenHow) (int, error)
	readlinkat(int, string, []byte) (int, error)
	fstat(int, *unix.Stat_t) error
	fstatfs(int, *unix.Statfs_t) error
	read(int, []byte) (int, error)
	close(int) error
}
type nativeProcIOOps struct{}

func (nativeProcIOOps) open(p string, f int, m uint32) (int, error) { return unix.Open(p, f, m) }
func (nativeProcIOOps) openat2(d int, p string, h *unix.OpenHow) (int, error) {
	return unix.Openat2(d, p, h)
}
func (nativeProcIOOps) readlinkat(d int, p string, b []byte) (int, error) {
	return unix.Readlinkat(d, p, b)
}
func (nativeProcIOOps) fstat(d int, s *unix.Stat_t) error     { return unix.Fstat(d, s) }
func (nativeProcIOOps) fstatfs(d int, s *unix.Statfs_t) error { return unix.Fstatfs(d, s) }
func (nativeProcIOOps) read(d int, b []byte) (int, error)     { return unix.Read(d, b) }
func (nativeProcIOOps) close(d int) error                     { return unix.Close(d) }

func openKernelIO(ctx context.Context, c *child) (kernelIOSource, string, string) {
	return openKernelIOWithOps(ctx, c, nativeProcIOOps{})
}
func openKernelIOWithOps(ctx context.Context, c *child, ops procIOOps) (kernelIOSource, string, string) {
	if reason := kernelIOContextReason(ctx); reason != "" {
		return nil, "unavailable", reason
	}
	if childReaped(c.done) {
		return nil, "unavailable", "worker_reaped"
	}
	root, err := ops.open("/proc", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "unavailable", "procfs_unavailable"
	}
	defer ops.close(root)
	var rootStat unix.Stat_t
	var fs unix.Statfs_t
	if ops.fstat(root, &rootStat) != nil || ops.fstatfs(root, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC || rootStat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, "unavailable", "procfs_unavailable"
	}
	if reason := kernelIOContextReason(ctx); reason != "" {
		return nil, "unavailable", reason
	}
	// /proc can show an ancestor PID namespace. Equal bare PID/PPID numbers
	// alone do not bind the directory to this harness's child. The kernel emits
	// NSpid values from the proc mount's namespace through the task's namespace;
	// exactly one caller value establishes an aligned view before child lookup.
	if reason := verifyProcNamespace(ctx, root, uint64(rootStat.Dev), ops); reason != "" {
		return nil, "unavailable", reason
	}
	if reason := kernelIOContextReason(ctx); reason != "" {
		return nil, "unavailable", reason
	}
	fd, err := ops.openat2(root, strconv.Itoa(c.cmd.Process.Pid), &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW, Resolve: procResolve})
	if err != nil {
		return nil, "unavailable", procOpenReason(err)
	}
	source := &linuxKernelIO{fd: fd, device: uint64(rootStat.Dev), ops: ops}
	keep := false
	defer func() {
		if !keep {
			source.close()
		}
	}()
	var st unix.Stat_t
	if ops.fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(st.Dev) != source.device || ops.fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return nil, "unavailable", "process_scope_unavailable"
	}
	source.inode = st.Ino
	data, reason := source.readFile(ctx, c.done, "stat", kernelIOStatLimit)
	if reason != "" {
		return nil, "unavailable", reason
	}
	identity, ok := parseProcIdentity(data)
	if !ok || identity.pid != int64(c.cmd.Process.Pid) || identity.parent != int64(os.Getpid()) {
		return nil, "unavailable", "process_identity_unavailable"
	}
	if reason := kernelIOContextReason(ctx); reason != "" {
		return nil, "unavailable", reason
	}
	source.identity, keep = identity, true
	return source, "unavailable", "not_sampled"
}

func verifyProcNamespace(ctx context.Context, root int, device uint64, ops procIOOps) string {
	if reason := kernelIOContextReason(ctx); reason != "" {
		return reason
	}
	link := make([]byte, kernelIOSelfLimit)
	n, err := ops.readlinkat(root, "self", link)
	if reason := kernelIOContextReason(ctx); reason != "" {
		return reason
	}
	if err != nil || n < 1 || n >= len(link) {
		return "process_namespace_unavailable"
	}
	pid, ok := procUnsigned(string(link[:n]))
	if !ok || pid != uint64(os.Getpid()) {
		return "process_namespace_unavailable"
	}
	fd, err := ops.openat2(root, string(link[:n]), &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW, Resolve: procResolve})
	if err != nil {
		return "process_namespace_unavailable"
	}
	defer ops.close(fd)
	var st unix.Stat_t
	var fs unix.Statfs_t
	if ops.fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(st.Dev) != device || ops.fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return "process_namespace_unavailable"
	}
	caller := linuxKernelIO{fd: fd, device: device, ops: ops}
	data, reason := caller.readFile(ctx, nil, "status", kernelIOStatusLimit)
	if reason != "" {
		return reason
	}
	if !parseProcNamespace(data, pid) {
		return "process_namespace_unavailable"
	}
	return kernelIOContextReason(ctx)
}

func parseProcNamespace(data []byte, pid uint64) bool {
	if len(data) == 0 || len(data) > kernelIOStatusLimit || data[len(data)-1] != '\n' {
		return false
	}
	found := false
	for _, line := range bytes.Split(data[:len(data)-1], []byte{'\n'}) {
		if !bytes.HasPrefix(line, []byte("NSpid:")) {
			continue
		}
		if found {
			return false
		}
		found = true
		// Reject nested/ancestor views even if their numeric values coincide.
		fields := bytes.Split(line, []byte{'\t'})
		if len(fields) != 2 || string(fields[0]) != "NSpid:" {
			return false
		}
		n, ok := procUnsigned(string(fields[1]))
		if !ok || n != pid {
			return false
		}
	}
	return found
}

func procOpenReason(err error) string {
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		return "resolution_unsupported"
	}
	return "process_scope_unavailable"
}

func (s *linuxKernelIO) close() {
	if s.fd >= 0 {
		s.ops.close(s.fd)
		s.fd = -1
	}
}

func (s *linuxKernelIO) heldScope(ctx context.Context, done <-chan struct{}) string {
	if reason := kernelIOContextReason(ctx); reason != "" {
		return reason
	}
	if childReaped(done) {
		return "worker_reaped"
	}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if s.fd < 0 || s.ops.fstat(s.fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || uint64(st.Dev) != s.device || st.Ino != s.inode || s.ops.fstatfs(s.fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return "process_scope_unavailable"
	}
	if reason := kernelIOContextReason(ctx); reason != "" {
		return reason
	}
	if childReaped(done) {
		return "worker_reaped"
	}
	return ""
}

func (s *linuxKernelIO) readFile(ctx context.Context, done <-chan struct{}, name string, limit int) ([]byte, string) {
	if reason := kernelIOContextReason(ctx); reason != "" {
		return nil, reason
	}
	if childReaped(done) {
		return nil, "worker_reaped"
	}
	expectedLimit := 0
	switch name {
	case "stat":
		expectedLimit = kernelIOStatLimit
	case "io":
		expectedLimit = kernelIODataLimit
	case "status":
		expectedLimit = kernelIOStatusLimit
	}
	if expectedLimit == 0 || limit != expectedLimit {
		return nil, "attribute_shape_unavailable"
	}
	fd, err := s.ops.openat2(s.fd, name, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK, Resolve: procResolve})
	if err != nil {
		return nil, procOpenReason(err)
	}
	defer s.ops.close(fd)
	var st unix.Stat_t
	var fs unix.Statfs_t
	if s.ops.fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || uint64(st.Dev) != s.device || s.ops.fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return nil, "attribute_shape_unavailable"
	}
	return readProcBytes(ctx, done, limit, func(b []byte) (int, error) { return s.ops.read(fd, b) })
}

func readProcBytes(ctx context.Context, done <-chan struct{}, limit int, read func([]byte) (int, error)) ([]byte, string) {
	if limit < 1 || limit > kernelIOStatLimit {
		return nil, "attribute_shape_unavailable"
	}
	data := make([]byte, limit+1)
	size := 0
	for i := 0; i < kernelIOReadCalls; i++ {
		if reason := kernelIOContextReason(ctx); reason != "" {
			return nil, reason
		}
		if childReaped(done) {
			return nil, "worker_reaped"
		}
		n, err := read(data[size:])
		if reason := kernelIOContextReason(ctx); reason != "" {
			return nil, reason
		}
		if childReaped(done) {
			return nil, "worker_reaped"
		}
		if n < 0 || n > len(data)-size {
			return nil, "attribute_read_unavailable"
		}
		size += n
		if size > limit {
			return nil, "attribute_too_large"
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, "attribute_read_unavailable"
		}
		if n == 0 {
			return data[:size], ""
		}
	}
	return nil, "attribute_read_limit"
}

func (s *linuxKernelIO) read(ctx context.Context, done <-chan struct{}) (kernelIOCounters, string) {
	if reason := s.heldScope(ctx, done); reason != "" {
		return kernelIOCounters{}, reason
	}
	before, reason := s.readFile(ctx, done, "stat", kernelIOStatLimit)
	if reason != "" {
		return kernelIOCounters{}, reason
	}
	identity, ok := parseProcIdentity(before)
	if !ok || identity != s.identity {
		return kernelIOCounters{}, "process_identity_changed"
	}
	data, reason := s.readFile(ctx, done, "io", kernelIODataLimit)
	if reason != "" {
		return kernelIOCounters{}, reason
	}
	counters, ok := parseProcIO(data)
	if !ok {
		return kernelIOCounters{}, "counter_profile_unsupported"
	}
	after, reason := s.readFile(ctx, done, "stat", kernelIOStatLimit)
	if reason != "" {
		return kernelIOCounters{}, reason
	}
	identity, ok = parseProcIdentity(after)
	if !ok || identity != s.identity {
		return kernelIOCounters{}, "process_identity_changed"
	}
	if reason := s.heldScope(ctx, done); reason != "" {
		return kernelIOCounters{}, reason
	}
	return counters, ""
}

func procUnsigned(s string) (uint64, bool) {
	if s == "" || len(s) > 20 || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

func parseProcIdentity(data []byte) (procIdentity, bool) {
	if len(data) == 0 || len(data) > kernelIOStatLimit || data[len(data)-1] != '\n' {
		return procIdentity{}, false
	}
	open, close := bytes.IndexByte(data, '('), bytes.LastIndexByte(data, ')')
	if open < 2 || close <= open || close-open > 65 || close+2 >= len(data) || data[open-1] != ' ' || data[close+1] != ' ' {
		return procIdentity{}, false
	}
	pid, ok := procUnsigned(string(data[:open-1]))
	if !ok || pid == 0 || pid > uint64(^uint32(0)>>1) {
		return procIdentity{}, false
	}
	fields := strings.Fields(string(data[close+2:]))
	if len(fields) != 50 || len(fields[0]) != 1 || !strings.Contains("RSDTtKWPI", fields[0]) {
		return procIdentity{}, false
	}
	// Profile: 52 stat fields. Only PID, PPID and start ticks become identity;
	// the remaining numeric fields are syntax-checked and never published.
	for _, f := range fields[1:] {
		if strings.HasPrefix(f, "-") {
			f = f[1:]
		}
		if _, valid := procUnsigned(f); !valid {
			return procIdentity{}, false
		}
	}
	parent, parentOK := procUnsigned(fields[1])
	start, startOK := procUnsigned(fields[19])
	if !parentOK || parent == 0 || parent > uint64(^uint32(0)>>1) || !startOK {
		return procIdentity{}, false
	}
	return procIdentity{int64(pid), int64(parent), start}, true
}

func parseProcIO(data []byte) (kernelIOCounters, bool) {
	if len(data) == 0 || len(data) > kernelIODataLimit || data[len(data)-1] != '\n' {
		return kernelIOCounters{}, false
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	if len(lines) != 7 {
		return kernelIOCounters{}, false
	}
	var c kernelIOCounters
	fields := map[string]*uint64{"rchar": &c.RChar, "wchar": &c.WChar, "syscr": &c.SysCR, "syscw": &c.SysCW, "read_bytes": &c.ReadBytes, "write_bytes": &c.WriteBytes, "cancelled_write_bytes": &c.CancelledWriteBytes}
	for _, line := range lines {
		parts := bytes.Split(line, []byte(": "))
		if len(parts) != 2 {
			return kernelIOCounters{}, false
		}
		field, ok := fields[string(parts[0])]
		if !ok || field == nil {
			return kernelIOCounters{}, false
		}
		n, ok := procUnsigned(string(parts[1]))
		if !ok {
			return kernelIOCounters{}, false
		}
		*field = n
		fields[string(parts[0])] = nil
	}
	return c, true
}
