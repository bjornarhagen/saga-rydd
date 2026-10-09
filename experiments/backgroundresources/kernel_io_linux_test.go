package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func fixtureProcStat(pid, parent int64, start uint64, comm string) []byte {
	fields := make([]string, 50)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[1], fields[19] = "S", strconv.FormatInt(parent, 10), strconv.FormatUint(start, 10)
	return []byte(fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " ")))
}
func fixtureProcIO(c kernelIOCounters) []byte {
	return []byte(fmt.Sprintf("rchar: %d\nwchar: %d\nsyscr: %d\nsyscw: %d\nread_bytes: %d\nwrite_bytes: %d\ncancelled_write_bytes: %d\n", c.RChar, c.WChar, c.SysCR, c.SysCW, c.ReadBytes, c.WriteBytes, c.CancelledWriteBytes))
}

func TestKernelIOProcParsersStrictProfile(t *testing.T) {
	want := procIdentity{123, 456, math.MaxUint64}
	valid := fixtureProcStat(want.pid, want.parent, want.start, "name) (with\nnewline")
	if got, ok := parseProcIdentity(valid); !ok || got != want {
		t.Fatal("valid comm/ticks refused", got, ok)
	}
	for _, data := range [][]byte{nil, valid[:len(valid)-1], bytes.Replace(valid, []byte("123 ("), []byte("0123 ("), 1), bytes.Replace(valid, []byte(" S "), []byte(" Z "), 1), bytes.Replace(valid, []byte(" S "), []byte(" ? "), 1), append(append([]byte{}, valid[:len(valid)-1]...), []byte(" 0\n")...), bytes.Replace(valid, []byte(" S 456"), []byte(" S 0"), 1), bytes.Replace(valid, []byte(" S 456"), []byte(" S -456"), 1), fixtureProcStat(123, 456, 1, strings.Repeat("x", 65)), append(bytes.Repeat([]byte{'a'}, kernelIOStatLimit), '\n')} {
		if _, ok := parseProcIdentity(data); ok {
			t.Fatal("invalid stat accepted", string(data))
		}
	}
	maximum := kernelIOCounters{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	validIO := fixtureProcIO(maximum)
	if got, ok := parseProcIO(validIO); !ok || got != maximum {
		t.Fatal("maximum counters refused", got, ok)
	}
	for _, data := range [][]byte{nil, validIO[:len(validIO)-1], append(append([]byte{}, validIO...), []byte("other: 1\n")...), bytes.Replace(validIO, []byte("syscw:"), []byte("syscr:"), 1), bytes.Replace(validIO, []byte("rchar: "), []byte("rchar:\t"), 1), bytes.Replace(validIO, []byte("18446744073709551615"), []byte("18446744073709551616"), 1), bytes.Replace(validIO, []byte("18446744073709551615"), []byte("+1"), 1), bytes.Replace(validIO, []byte("18446744073709551615"), []byte("-1"), 1), bytes.Replace(validIO, []byte("18446744073709551615"), []byte("01"), 1), bytes.Replace(validIO, []byte("18446744073709551615"), []byte("1 "), 1), append(bytes.Repeat([]byte{'a'}, kernelIODataLimit), '\n')} {
		if _, ok := parseProcIO(data); ok {
			t.Fatal("invalid io accepted", string(data))
		}
	}
}

func TestKernelIOProcReadBoundsAndFinalChecks(t *testing.T) {
	for _, mode := range []string{"exact", "overflow", "partial", "eintr", "error", "invalid_count", "cancel", "reap", "bad_limit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			calls := 0
			limit := 4
			if mode == "bad_limit" {
				limit = -1
			}
			data, reason := readProcBytes(ctx, done, limit, func(b []byte) (int, error) {
				calls++
				switch mode {
				case "exact":
					if calls == 1 {
						return copy(b, []byte("data")), nil
					}
					return 0, nil
				case "overflow":
					return copy(b, []byte("large")), nil
				case "partial":
					b[0] = 'x'
					return 1, nil
				case "eintr":
					return 0, unix.EINTR
				case "error":
					return 0, unix.EIO
				case "invalid_count":
					return len(b) + 1, nil
				case "cancel":
					cancel()
					return copy(b, []byte("data")), nil
				case "reap":
					close(done)
					return copy(b, []byte("data")), nil
				default:
					t.Fatal("unexpected read")
					return 0, nil
				}
			})
			want := map[string]string{"exact": "", "overflow": "attribute_too_large", "partial": "attribute_read_limit", "eintr": "attribute_read_limit", "error": "attribute_read_unavailable", "invalid_count": "attribute_read_unavailable", "cancel": "context_canceled", "reap": "worker_reaped", "bad_limit": "attribute_shape_unavailable"}[mode]
			if reason != want || calls > kernelIOReadCalls || (reason != "" && data != nil) || (mode == "exact" && string(data) != "data") || (mode == "bad_limit" && calls != 0) {
				t.Fatal(string(data), reason, calls)
			}
		})
	}
}

type fixtureProcFile struct {
	name     string
	data     []byte
	offset   int
	mode     uint32
	dev, ino uint64
}
type fixtureProcCall struct {
	parent int
	name   string
	how    unix.OpenHow
}
type fixtureProcOps struct {
	files                                        map[int]*fixtureProcFile
	next, peak                                   int
	rootOpens, attrReads, stats, fsstats, closes int
	calls                                        []fixtureProcCall
	statData, ioData                             []byte
	openError                                    error
	wrongFS, wrongScope, wrongAttr               bool
	afterStat                                    []byte
	statOpens                                    int
	statusData                                   []byte
	selfData                                     string
	selfOpens, linkCalls, childOpens             int
	statusReadFn                                 func([]byte) (int, error)
	readLengths                                  []int
}

func newFixtureProcOps() *fixtureProcOps {
	return &fixtureProcOps{files: map[int]*fixtureProcFile{}, next: 10, statData: fixtureProcStat(123, int64(os.Getpid()), 7, "generated"), ioData: fixtureProcIO(kernelIOCounters{1, 2, 3, 4, 5, 6, 7}), selfData: strconv.Itoa(os.Getpid()), statusData: []byte(fmt.Sprintf("Name:\tgenerated\nNSpid:\t%d\n", os.Getpid()))}
}
func (o *fixtureProcOps) readlinkat(parent int, name string, b []byte) (int, error) {
	o.linkCalls++
	if o.files[parent] == nil || o.files[parent].name != "root" || name != "self" || len(b) != kernelIOSelfLimit {
		return 0, unix.EINVAL
	}
	return copy(b, o.selfData), nil
}
func (o *fixtureProcOps) add(f *fixtureProcFile) int {
	fd := o.next
	o.next++
	o.files[fd] = f
	o.peak = max(o.peak, len(o.files))
	return fd
}
func (o *fixtureProcOps) open(p string, f int, m uint32) (int, error) {
	o.rootOpens++
	if p != "/proc" || f != unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW || m != 0 {
		return -1, unix.EINVAL
	}
	return o.add(&fixtureProcFile{name: "root", mode: unix.S_IFDIR, dev: 1, ino: 1}), nil
}
func (o *fixtureProcOps) openat2(parent int, name string, h *unix.OpenHow) (int, error) {
	o.calls = append(o.calls, fixtureProcCall{parent, name, *h})
	if o.openError != nil {
		return -1, o.openError
	}
	dir := o.files[parent]
	if dir == nil {
		return -1, unix.EBADF
	}
	if h.Resolve != procResolve {
		return -1, unix.EINVAL
	}
	if dir.name == "root" {
		if o.selfOpens == 0 {
			if name != strconv.Itoa(os.Getpid()) || h.Flags != unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW {
				return -1, unix.EINVAL
			}
			o.selfOpens++
			return o.add(&fixtureProcFile{name: "caller", mode: unix.S_IFDIR, dev: 1, ino: 4}), nil
		}
		if name != "123" || h.Flags != unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW {
			return -1, unix.EINVAL
		}
		o.childOpens++
		return o.add(&fixtureProcFile{name: "process", mode: unix.S_IFDIR, dev: 1, ino: 2}), nil
	}
	if dir.name == "caller" && name == "status" && h.Flags == unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK {
		return o.add(&fixtureProcFile{name: name, data: o.statusData, mode: unix.S_IFREG, dev: 1, ino: 5}), nil
	}
	if dir.name != "process" || (name != "stat" && name != "io") || h.Flags != unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK {
		return -1, unix.EINVAL
	}
	data := o.ioData
	if name == "stat" {
		o.statOpens++
		data = o.statData
		if o.statOpens >= 3 && o.afterStat != nil {
			data = o.afterStat
		}
	}
	return o.add(&fixtureProcFile{name: name, data: data, mode: unix.S_IFREG, dev: 1, ino: 3}), nil
}
func (o *fixtureProcOps) fstat(fd int, st *unix.Stat_t) error {
	o.stats++
	f := o.files[fd]
	if f == nil {
		return unix.EBADF
	}
	st.Dev, st.Ino, st.Mode = f.dev, f.ino, f.mode
	if f.name == "process" && o.wrongScope {
		st.Ino++
	}
	if (f.name == "stat" || f.name == "io" || f.name == "status") && o.wrongAttr {
		st.Mode = unix.S_IFIFO
	}
	return nil
}
func (o *fixtureProcOps) fstatfs(fd int, st *unix.Statfs_t) error {
	o.fsstats++
	if o.files[fd] == nil {
		return unix.EBADF
	}
	st.Type = unix.PROC_SUPER_MAGIC
	if o.wrongFS {
		st.Type = unix.TMPFS_MAGIC
	}
	return nil
}
func (o *fixtureProcOps) read(fd int, b []byte) (int, error) {
	o.attrReads++
	o.readLengths = append(o.readLengths, len(b))
	f := o.files[fd]
	if f == nil {
		return 0, unix.EBADF
	}
	if f.name == "status" && o.statusReadFn != nil {
		return o.statusReadFn(b)
	}
	n := copy(b, f.data[f.offset:])
	f.offset += n
	return n, nil
}

func TestKernelIOCallerNamespaceBindingRefusesForeignAndMalformedViews(t *testing.T) {
	pid := strconv.Itoa(os.Getpid())
	for _, mode := range []string{"foreign", "foreign_equal_numbers", "missing", "duplicate", "overflow", "trailing", "no_eof", "too_large", "partial_reads", "interrupted_reads", "wrong_self", "self_suffix", "self_overflow", "self_truncated"} {
		t.Run(mode, func(t *testing.T) {
			ops := newFixtureProcOps()
			switch mode {
			case "foreign":
				ops.statusData = []byte("Name:\tfixture\nNSpid:\t42\t" + pid + "\n")
			case "foreign_equal_numbers":
				ops.statusData = []byte("NSpid:\t" + pid + "\t" + pid + "\n")
			case "missing":
				ops.statusData = []byte("Name:\tfixture\n")
			case "duplicate":
				ops.statusData = []byte("NSpid:\t" + pid + "\nNSpid:\t" + pid + "\n")
			case "overflow":
				ops.statusData = []byte("NSpid:\t18446744073709551616\n")
			case "trailing":
				ops.statusData = []byte("NSpid:\t" + pid + " \n")
			case "no_eof":
				ops.statusData = ops.statusData[:len(ops.statusData)-1]
			case "too_large":
				ops.statusData = append(ops.statusData, bytes.Repeat([]byte{'x'}, kernelIOStatusLimit)...)
			case "partial_reads":
				ops.statusReadFn = func(b []byte) (int, error) { b[0] = 'x'; return 1, nil }
			case "interrupted_reads":
				ops.statusReadFn = func([]byte) (int, error) { return 0, unix.EINTR }
			case "wrong_self":
				ops.selfData = strconv.Itoa(os.Getpid() + 1)
			case "self_suffix":
				ops.selfData = pid + "/"
			case "self_overflow":
				ops.selfData = "18446744073709551616"
			case "self_truncated":
				ops.selfData = strings.Repeat("1", kernelIOSelfLimit)
			}
			source, status, reason := openKernelIOWithOps(context.Background(), fixtureOwnedChild(), ops)
			if source != nil || status != "unavailable" || reason == "" || ops.childOpens != 0 || ops.statOpens != 0 || len(ops.files) != 0 || ops.peak > 3 || ops.linkCalls != 1 || ops.attrReads > kernelIOReadCalls {
				t.Fatal("foreign/unknown proc view reached child lookup or leaked", status, reason, ops)
			}
			if mode == "foreign" || mode == "foreign_equal_numbers" || mode == "missing" || mode == "duplicate" || mode == "overflow" || mode == "trailing" || mode == "no_eof" {
				if reason != "process_namespace_unavailable" {
					t.Fatal("namespace profile reason", reason)
				}
			}
			for _, length := range ops.readLengths {
				if length > kernelIOStatusLimit+1 {
					t.Fatal("setup read buffer exceeded cap", length)
				}
			}
		})
	}
	// An exact-cap status with one complete namespace value is accepted only
	// after a separate EOF read. Nothing relies on the file's reported st_size.
	ops := newFixtureProcOps()
	prefix := []byte("NSpid:\t" + pid + "\n")
	ops.statusData = append(prefix, append(bytes.Repeat([]byte{'x'}, kernelIOStatusLimit-len(prefix)-1), '\n')...)
	source, _, reason := openKernelIOWithOps(context.Background(), fixtureOwnedChild(), ops)
	if source == nil || reason != "not_sampled" || ops.peak != 3 || ops.childOpens != 1 {
		t.Fatal("exact-cap EOF binding failed", reason, ops)
	}
	source.close()
	if len(ops.files) != 0 {
		t.Fatal("exact-cap binding leaked")
	}
}

func TestKernelIOBindingCancellationAndReapedNotification(t *testing.T) {
	for _, mode := range []string{"before", "during_status", "reaped"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ops, c := newFixtureProcOps(), fixtureOwnedChild()
			if mode == "before" {
				cancel()
			}
			if mode == "reaped" {
				close(c.done)
			}
			if mode == "during_status" {
				ops.statusReadFn = func(b []byte) (int, error) { cancel(); return copy(b, ops.statusData), nil }
			}
			source, status, reason := openKernelIOWithOps(ctx, c, ops)
			want := "context_canceled"
			if mode == "reaped" {
				want = "worker_reaped"
			}
			if source != nil || status != "unavailable" || reason != want || ops.childOpens != 0 || len(ops.files) != 0 || ops.peak > 3 {
				t.Fatal("binding crossed cancellation/reap", status, reason, ops)
			}
			if mode != "during_status" && (ops.rootOpens != 0 || ops.linkCalls != 0) {
				t.Fatal("canceled/reaped binding started probe", ops)
			}
		})
	}
}
func (o *fixtureProcOps) close(fd int) error {
	if o.files[fd] == nil {
		return unix.EBADF
	}
	o.closes++
	delete(o.files, fd)
	return nil
}
func fixtureOwnedChild() *child {
	return &child{cmd: &exec.Cmd{Process: &os.Process{Pid: 123}}, done: make(chan struct{})}
}

func TestKernelIOHeldProcScopeBoundsAndNoNumericReopen(t *testing.T) {
	ops := newFixtureProcOps()
	c := fixtureOwnedChild()
	source, status, reason := openKernelIOWithOps(context.Background(), c, ops)
	if source == nil || status != "unavailable" || reason != "not_sampled" || ops.peak != 3 || len(ops.files) != 1 || ops.rootOpens != 1 || len(ops.calls) != 4 || ops.linkCalls != 1 || ops.childOpens != 1 || ops.selfOpens != 1 || ops.attrReads != 4 || ops.stats != 5 || ops.fsstats != 5 || ops.closes != 4 {
		t.Fatal("binding profile", status, reason, ops)
	}
	bound := source.(*linuxKernelIO)
	if bound.identity != (procIdentity{123, int64(os.Getpid()), 7}) || bound.inode != 2 {
		t.Fatal("identity not bound", bound)
	}
	priorReads, priorStats, priorFS, priorCalls, priorCloses := ops.attrReads, ops.stats, ops.fsstats, len(ops.calls), ops.closes
	got, reason := source.read(context.Background(), c.done)
	if reason != "" || got != (kernelIOCounters{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatal(got, reason)
	}
	if ops.attrReads-priorReads != 6 || ops.stats-priorStats != 5 || ops.fsstats-priorFS != 5 || len(ops.calls)-priorCalls != 3 || ops.closes-priorCloses != 3 || len(ops.files) != 1 {
		t.Fatal("sample operation profile changed", ops)
	}
	for i, call := range ops.calls[priorCalls:] {
		if call.parent != bound.fd || call.name != []string{"stat", "io", "stat"}[i] {
			t.Fatal("numeric scope reopened", call)
		}
	}
	close(c.done)
	beforeOpens, beforeReads, beforeStats := len(ops.calls), ops.attrReads, ops.stats
	if _, reason := source.read(context.Background(), c.done); reason != "worker_reaped" || len(ops.calls) != beforeOpens || ops.attrReads != beforeReads || ops.stats != beforeStats {
		t.Fatal("reaped source performed operations", reason, ops)
	}
	source.close()
	source.close()
	if len(ops.files) != 0 {
		t.Fatal("retained source descriptor leaked", ops.files)
	}
}

func TestKernelIOProcRefusalsCloseAndNeverFallback(t *testing.T) {
	for _, mode := range []string{"fs", "resolution", "identity", "changed_before", "changed_after", "held_inode", "attribute", "io_profile", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			ops := newFixtureProcOps()
			c := fixtureOwnedChild()
			if mode == "fs" {
				ops.wrongFS = true
			}
			if mode == "resolution" {
				ops.openError = unix.ENOSYS
			}
			if mode == "identity" {
				ops.statData = fixtureProcStat(124, int64(os.Getpid()), 7, "other")
			}
			source, _, reason := openKernelIOWithOps(context.Background(), c, ops)
			if mode == "fs" || mode == "resolution" || mode == "identity" {
				if source != nil || reason == "" || len(ops.files) != 0 || ops.rootOpens != 1 {
					t.Fatal("failed binding retained/reopened descriptor", reason, ops)
				}
				return
			}
			if source == nil {
				t.Fatal("initial binding failed", reason)
			}
			switch mode {
			case "changed_before":
				ops.statData = fixtureProcStat(123, int64(os.Getpid()), 8, "replacement")
			case "changed_after":
				ops.afterStat = fixtureProcStat(123, int64(os.Getpid()), 8, "replacement")
			case "held_inode":
				ops.wrongScope = true
			case "attribute":
				ops.wrongAttr = true
			case "io_profile":
				ops.ioData = append(ops.ioData, []byte("future: 1\n")...)
			case "symlink":
				ops.openError = unix.ELOOP
			}
			tk := newKernelIOTracker(source, c.done, time.Now().Add(-time.Second), time.Now, "unavailable", "not_sampled")
			got := tk.sample(context.Background())
			if got.Status != "unavailable" || got.Counters != nil || got.Reason == "" || len(ops.files) != 0 || tk.summary().SampledDelta != nil {
				t.Fatal("unsafe positive or leaked scope", got, ops)
			}
			opens := len(ops.calls)
			if retry := tk.sample(context.Background()); retry.Status != "unavailable" || len(ops.calls) != opens || ops.rootOpens != 1 {
				t.Fatal("refusal started weaker fallback", retry, ops)
			}
		})
	}
}

func TestKernelIONativeNoFollowGeneratedAttribute(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "original"), []byte("private body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("original", filepath.Join(base, "io")); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	spy := &nativeProcOpenSpy{}
	s := &linuxKernelIO{fd: fd, ops: spy}
	if data, reason := s.readFile(context.Background(), make(chan struct{}), "io", kernelIODataLimit); data != nil || reason == "" {
		t.Fatal("generated symlink was followed", string(data), reason)
	}
	if spy.opens != 1 || !errors.Is(spy.lastError, unix.ELOOP) {
		t.Fatal("native no-follow rejection was not observed", spy.opens, spy.lastError)
	}
	if data, reason := s.readFile(context.Background(), make(chan struct{}), "../original", kernelIODataLimit); data != nil || reason != "attribute_shape_unavailable" {
		t.Fatal("unlisted relative path accepted", reason)
	}
	if spy.opens != 1 {
		t.Fatal("unlisted path reached native resolution")
	}
}

type nativeProcOpenSpy struct {
	nativeProcIOOps
	opens     int
	lastError error
}

func (o *nativeProcOpenSpy) openat2(parent int, name string, h *unix.OpenHow) (int, error) {
	o.opens++
	fd, err := o.nativeProcIOOps.openat2(parent, name, h)
	o.lastError = err
	return fd, err
}

func TestKernelIOOwnedLinuxChildHelper(t *testing.T) {
	if os.Getenv("RYDD_KERNEL_IO_CHILD") == "" {
		t.Skip("disposable process only")
	}
	base := os.Getenv("RYDD_KERNEL_IO_FIXTURE")
	if err := os.WriteFile(filepath.Join(base, "ready"), []byte("ready"), 0600); err != nil {
		os.Exit(21)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(base, "go")); err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			os.Exit(22)
		}
		time.Sleep(5 * time.Millisecond)
	}
	body := bytes.Repeat([]byte{'x'}, 64*1024)
	if err := os.WriteFile(filepath.Join(base, "body"), body, 0600); err != nil {
		os.Exit(23)
	}
	for i := 0; i < 4; i++ {
		got, err := os.ReadFile(filepath.Join(base, "body"))
		if err != nil || !bytes.Equal(got, body) {
			os.Exit(24)
		}
	}
	if err := os.WriteFile(filepath.Join(base, "finished"), []byte("finished"), 0600); err != nil {
		os.Exit(25)
	}
	time.Sleep(5 * time.Second)
	os.Exit(0)
}

func TestKernelIONativeOwnedChildCountersAndReapedDescriptor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base := t.TempDir()
	c, err := startWorker(ctx, os.Args[0], []string{"-test.run=^TestKernelIOOwnedLinuxChildHelper$"}, append(os.Environ(), "RYDD_KERNEL_IO_CHILD=1", "RYDD_KERNEL_IO_FIXTURE="+base), base, 128)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.terminate)
	if c.kernelIO == nil {
		t.Fatal("native proc tracker missing")
	}
	if c.kernelIO.source == nil {
		t.Fatal("native proc binding unavailable", c.kernelIO.summary())
	}
	fd := c.kernelIO.source.(*linuxKernelIO).fd
	waitFixtureMarker(t, c, func() bool { _, err := os.Stat(filepath.Join(base, "ready")); return err == nil })
	first := c.kernelIO.sample(ctx)
	if first.Status != "observed" || first.Counters == nil {
		t.Fatal("initial owned child sample unavailable", first)
	}
	if err := os.WriteFile(filepath.Join(base, "go"), []byte("go"), 0600); err != nil {
		t.Fatal(err)
	}
	waitFixtureMarker(t, c, func() bool { _, err := os.Stat(filepath.Join(base, "finished")); return err == nil })
	last := c.kernelIO.sample(ctx)
	if last.Status != "observed" || last.Counters == nil {
		t.Fatal("second owned child sample unavailable", last)
	}
	delta := c.kernelIO.summary().SampledDelta
	if delta == nil || delta.RChar < 4*64*1024 || delta.WChar < 64*1024 || delta.SysCR < 4 || delta.SysCW < 1 {
		t.Fatal("known generated successful read/write calls absent", delta)
	}
	// Cached generated reads may have read_bytes == 0. Never demand physical
	// traffic, invent a full-lifetime delta, or net off cancelled write bytes.
	c.terminate()
	if c.cmd.ProcessState == nil || c.kernelIO.sample(ctx).Reason != "worker_reaped" || c.kernelIO.summary().FullLifetime {
		t.Fatal("post-reap read/full-lifetime claim")
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); !errors.Is(err, unix.EBADF) {
		t.Fatal("retained process descriptor not closed", err)
	}
	t.Logf("owned generated child sampled delta: rchar=%d wchar=%d syscr=%d syscw=%d read_bytes=%d write_bytes=%d (kernel accounting only)", delta.RChar, delta.WChar, delta.SysCR, delta.SysCW, delta.ReadBytes, delta.WriteBytes)
}
