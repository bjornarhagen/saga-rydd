package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func rootStreamFixture(t *testing.T, counts []int, options ...Option) (*Scanner, []state.Job) {
	t.Helper()
	base := t.TempDir()
	roots := make([]string, len(counts))
	jobs := make([]state.Job, len(counts))
	for i, count := range counts {
		roots[i] = filepath.Join(base, fmt.Sprintf("root-%d", i))
		if err := os.Mkdir(roots[i], 0700); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < count; n++ {
			write(t, filepath.Join(roots[i], fmt.Sprintf("file-%04d", n)))
		}
		jobs[i] = state.Job{ID: int64(i + 1), RootID: int64(i + 1), RootPath: []byte(roots[i]), Path: []byte("."), Kind: state.ScanKind}
	}
	options = append(options, WithRootStreams(MaxRootStreams))
	s, err := New(roots, nil, nil, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, jobs
}

func TestRootStreamsInterleavedWideAndSmallComplete(t *testing.T) {
	counts := []int{301, 257, 3}
	s, jobs := rootStreamFixture(t, counts)
	seen := make([]map[string]bool, len(jobs))
	gens := make([]int64, len(jobs))
	complete := make([]bool, len(jobs))
	for i := range jobs {
		seen[i] = map[string]bool{}
	}
	for turn := 0; turn < 12; turn++ {
		i := turn % len(jobs)
		if complete[i] {
			continue
		}
		b := next(t, s, jobs[i])
		if gens[i] == 0 {
			gens[i] = b.Generation
		}
		if gens[i] != b.Generation {
			t.Fatal("rotation replayed directory", i)
		}
		for _, e := range b.Entries {
			if seen[i][string(e.Path)] {
				t.Fatal("replayed committed name", i, string(e.Path))
			}
			seen[i][string(e.Path)] = true
		}
		jobs[i].Cursor = b.Cursor
		jobs[i].RootIdentity = b.Identity
		complete[i] = b.Complete
		if i == 2 && turn == 2 && (len(b.Entries) != 3 || complete[0] || complete[1]) {
			t.Fatal("small root waited for wide roots")
		}
		if turn == 5 && (!complete[2] || complete[0] || complete[1]) {
			t.Fatal("small root EOF waited for wide roots")
		}
	}
	for i, count := range counts {
		if !complete[i] || len(seen[i]) != count {
			t.Fatal(i, complete[i], len(seen[i]))
		}
	}
	if len(s.rootStreams) != 0 || s.Metrics().EntryInspections != 561 {
		t.Fatal("stream leak or missing inspections", len(s.rootStreams), s.Metrics())
	}
}

func TestRootStreamsPendingNamesAndSharedPacing(t *testing.T) {
	s, jobs := rootStreamFixture(t, []int{7, 7}, WithEntryRate(20))
	seen := []map[string]bool{{}, {}}
	gens := make([]int64, 2)
	completed := make([]bool, 2)
	started := time.Now()
	for turn := 0; turn < 40; turn++ {
		i := turn % 2
		if completed[i] {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		b, err := s.Next(ctx, jobs[i])
		cancel()
		if err != nil || b.Fault != "" {
			t.Fatal(b, err)
		}
		if gens[i] == 0 {
			gens[i] = b.Generation
		}
		if b.Generation != gens[i] {
			t.Fatal("lost parked generation", i)
		}
		for _, e := range b.Entries {
			if seen[i][string(e.Path)] {
				t.Fatal("lost pending buffer", i)
			}
			seen[i][string(e.Path)] = true
		}
		jobs[i].Cursor = b.Cursor
		completed[i] = b.Complete
		if turn == 1 && (len(s.rootStreams) != 2 || len(s.rootStreams[string(jobs[0].RootPath)].pending) == 0 || len(s.rootStreams[string(jobs[1].RootPath)].pending) == 0) {
			t.Fatal("did not retain both unread buffers")
		}
		if completed[0] && completed[1] {
			break
		}
	}
	if !completed[0] || !completed[1] || len(seen[0]) != 7 || len(seen[1]) != 7 || s.Metrics().EntryInspections != 14 || time.Since(started) < 650*time.Millisecond {
		t.Fatal("rotation bypassed shared pacing", completed, seen, s.Metrics(), time.Since(started))
	}
}

func TestRootStreamsSelectedFailureAndRelease(t *testing.T) {
	for _, failure := range []string{"permit", "cancel", "cursor", "mutation", "invalid_permit"} {
		t.Run(failure, func(t *testing.T) {
			s, jobs := rootStreamFixture(t, []int{301, 301})
			for i := range jobs {
				b := next(t, s, jobs[i])
				jobs[i].Cursor = b.Cursor
				jobs[i].RootIdentity = b.Identity
			}
			other := s.rootStreams[string(jobs[1].RootPath)]
			generation := other.generation
			selectedGeneration := s.rootStreams[string(jobs[0].RootPath)].generation
			var b state.ScanBatch
			var err error
			switch failure {
			case "permit":
				b, err = s.NextPermitted(context.Background(), jobs[0], func(context.Context, APICallKind) error { return errors.New("denied fixture") })
			case "invalid_permit":
				b, err = s.NextPermitted(context.Background(), jobs[0], nil)
			case "cancel":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				b, err = s.NextPermitted(ctx, jobs[0], func(context.Context, APICallKind) error { return nil })
			case "cursor":
				jobs[0].Cursor = []byte("unknown")
				b, err = s.Next(context.Background(), jobs[0])
			case "mutation":
				write(t, filepath.Join(string(jobs[0].RootPath), "new"))
				stamp := time.Now().Add(time.Hour)
				if err := os.Chtimes(string(jobs[0].RootPath), stamp, stamp); err != nil {
					t.Fatal(err)
				}
				b, err = s.Next(context.Background(), jobs[0])
			}
			if failure == "permit" || failure == "invalid_permit" || failure == "cancel" {
				if !errors.Is(err, ErrAPIPermitDenied) || b.Generation != 0 || s.rootStreams[string(jobs[0].RootPath)] != nil {
					t.Fatal(b, err)
				}
			} else if err != nil || b.Generation == selectedGeneration {
				t.Fatal(b, err)
			}
			if s.rootStreams[string(jobs[1].RootPath)] != other {
				t.Fatal("selected reset discarded other root")
			}
			b = next(t, s, jobs[1])
			if b.Generation != generation || len(b.Entries) != 128 {
				t.Fatal("other root replayed prefix", b)
			}
			fd := int(other.file.Fd())
			s.ReleaseRoot(string(jobs[1].RootPath))
			var st unix.Stat_t
			if s.rootStreams[string(jobs[1].RootPath)] != nil || unix.Fstat(fd, &st) != unix.EBADF {
				t.Fatal("dormant stream descriptor retained")
			}
		})
	}
}

func TestRootStreamsAdmissionAndNamesBeforeSourceCalls(t *testing.T) {
	paths := make([]string, MaxRootStreams+1)
	for i := range paths {
		paths[i] = fmt.Sprintf("/fixture/%d", i)
	}
	for _, roots := range [][]string{paths, nil, {"relative"}, {"/duplicate", "/duplicate"}, {"/fixture/../other"}} {
		calls := 0
		_, err := NewPermitted(context.Background(), roots, nil, nil, func(context.Context, APICallKind) error { calls++; return nil }, WithRootStreams(MaxRootStreams))
		if err == nil || calls != 0 {
			t.Fatal("invalid admission reached source", roots, calls, err)
		}
	}
	for _, names := range [][]string{{".."}, {""}, {"a/b"}, {"a\x00b"}, {strings.Repeat("a", MaxRetainedNameBytes+1)}, make([]string, state.MaxBatchEntries+1)} {
		if supportedPendingNames(".", names) {
			t.Fatal("unsupported names retained")
		}
	}
	if supportedPendingNames(strings.Repeat("a", 4090), []string{"1234567"}) || !supportedPendingNames(".", []string{"valid\xff"}) {
		t.Fatal("relative-path or byte filename bounds")
	}
}

type rootStreamNativeEvidence struct {
	Roots            int   `json:"roots"`
	PendingNames     int   `json:"pending_names"`
	OpenDelta        int   `json:"open_fd_delta"`
	AfterDelta       int   `json:"closed_fd_delta"`
	RootDirectoryFDs int   `json:"root_directory_fds"`
	AfterRootFDs     int   `json:"after_root_directory_fds"`
	PeakRSSBytes     int64 `json:"child_peak_rss_bytes"`
	RaceInstrumented bool  `json:"race_instrumented"`
	RSSLimitApplied  bool  `json:"rss_limit_applied"`
}

func fixtureRetainedDirectoryFDs() int {
	// The pinned Go Darwin Readdirnames implementation uses fdopendir on a
	// duplicated descriptor. closedir and File.Close release both descriptors.
	if runtime.GOOS == "darwin" {
		return 2 * MaxRootStreams
	}
	return MaxRootStreams
}

func fixtureOpenFDCount(t *testing.T) int {
	t.Helper()
	count := 0
	for fd := 0; fd < 256; fd++ {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			count++
		} else if err != unix.EBADF {
			t.Fatal(err)
		}
	}
	return count
}

func fixtureRootDirectoryFDCounts(t *testing.T, roots map[string]int) (int, map[string]int) {
	t.Helper()
	total := 0
	counts := make(map[string]int, len(roots))
	for fd := 0; fd < 256; fd++ {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			if err != unix.EBADF {
				t.Fatal(err)
			}
			continue
		}
		identity := fmt.Sprint(st.Dev, ":", st.Ino)
		if _, selected := roots[identity]; selected && st.Mode&unix.S_IFMT == unix.S_IFDIR {
			counts[identity]++
			total++
		}
	}
	return total, counts
}

func TestRootDirectoryFDCountExcludesOtherDirectories(t *testing.T) {
	root, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	other, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var st unix.Stat_t
	if err := unix.Fstat(int(root.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	identity := fmt.Sprint(st.Dev, ":", st.Ino)
	roots := map[string]int{identity: 1}
	if total, counts := fixtureRootDirectoryFDCounts(t, roots); total != 1 || counts[identity] != 1 {
		t.Fatal("unrelated directory charged to selected root", total, counts)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if total, _ := fixtureRootDirectoryFDCounts(t, roots); total != 0 {
		t.Fatal("closed selected directory still counted", total)
	}
}

func TestRootStreamsNativeChild(t *testing.T) {
	if os.Getenv("RYDD_TEST_ROOT_STREAM_CHILD") != "1" {
		t.Skip("disposable child only")
	}
	counts := make([]int, MaxRootStreams)
	for i := range counts {
		counts[i] = 129
	}
	s, jobs := rootStreamFixture(t, counts)
	for _, job := range jobs {
		for i := 0; i < 129; i++ {
			name := fmt.Sprintf("file-%04d", i)
			if err := os.Rename(filepath.Join(string(job.RootPath), name), filepath.Join(string(job.RootPath), strings.Repeat("x", 190)+name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	roots := make(map[string]int, len(jobs))
	for _, job := range jobs {
		var st unix.Stat_t
		if err := unix.Stat(string(job.RootPath), &st); err != nil {
			t.Fatal(err)
		}
		roots[fmt.Sprint(st.Dev, ":", st.Ino)] = fixtureRetainedDirectoryFDs() / MaxRootStreams
	}
	if len(roots) != len(jobs) {
		t.Fatal("generated roots have repeated identities")
	}
	if total, _ := fixtureRootDirectoryFDCounts(t, roots); total != 0 {
		t.Fatal("generated root descriptor open before scanning", total)
	}
	before := fixtureOpenFDCount(t)
	var fds []int
	pending := 0
	for _, job := range jobs {
		// This fixture measures retained buffers and descriptors. Yield before
		// consuming a child without depending on a short wall-clock timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		b, err := s.NextPermittedPaced(ctx, job,
			func(context.Context, APICallKind) error { return nil },
			func(context.Context, int64) (bool, error) { return false, nil })
		cancel()
		if err != nil || b.Fault != "" || b.Complete || len(b.Entries) != 0 {
			t.Fatal(b, err)
		}
		stream := s.rootStreams[string(job.RootPath)]
		if stream == nil || len(stream.pending) != 128 {
			t.Fatal("max unread buffer not retained")
		}
		pending += len(stream.pending)
		fd := int(stream.file.Fd())
		if fd >= 256 {
			t.Fatal("fixture descriptor count range exceeded", fd)
		}
		fds = append(fds, fd)
	}
	openDelta := fixtureOpenFDCount(t) - before
	rootFDs, rootCounts := fixtureRootDirectoryFDCounts(t, roots)
	for identity, expected := range roots {
		if rootCounts[identity] != expected {
			t.Fatal("retained directory descriptor count", rootCounts[identity], expected)
		}
	}
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	rss := usage.Maxrss
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	s.Close()
	for _, fd := range fds {
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != unix.EBADF {
			t.Fatal("retained FD not closed", fd)
		}
	}
	afterDelta := fixtureOpenFDCount(t) - before
	afterRootFDs, _ := fixtureRootDirectoryFDCounts(t, roots)
	// Raw process deltas include unrelated runtime/race descriptors opened after
	// the baseline. Exact generated directory identities establish ownership,
	// including Darwin's fdopendir duplicate; keep raw deltas as diagnostics.
	if rootFDs != fixtureRetainedDirectoryFDs() || afterRootFDs != 0 || rss <= 0 || (!rootStreamRaceInstrumented && rss >= 100<<20) {
		t.Fatal("bounded fixture envelope failed", rootFDs, afterRootFDs, openDelta, afterDelta, rss)
	}
	evidence := rootStreamNativeEvidence{Roots: len(jobs), PendingNames: pending, OpenDelta: openDelta, AfterDelta: afterDelta, RootDirectoryFDs: rootFDs, AfterRootFDs: afterRootFDs, PeakRSSBytes: rss, RaceInstrumented: rootStreamRaceInstrumented, RSSLimitApplied: !rootStreamRaceInstrumented}
	if err := json.NewEncoder(os.Stdout).Encode(evidence); err != nil {
		t.Fatal(err)
	}
}

func TestRootStreamsNativeDisposableProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRootStreamsNativeChild$")
	command.Env = append(os.Environ(), "RYDD_TEST_ROOT_STREAM_CHILD=1")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(output))
	}
	var evidence rootStreamNativeEvidence
	if err = json.NewDecoder(bytes.NewReader(output)).Decode(&evidence); err != nil || evidence.Roots != 32 || evidence.PendingNames != 4096 || evidence.RootDirectoryFDs != fixtureRetainedDirectoryFDs() || evidence.AfterRootFDs != 0 || evidence.PeakRSSBytes <= 0 || evidence.RaceInstrumented != rootStreamRaceInstrumented || evidence.RSSLimitApplied == rootStreamRaceInstrumented {
		t.Fatal(evidence, err, string(output))
	}
	t.Logf("Generated %s child: %+v; finite fixture evidence only", runtime.GOOS, evidence)
}
