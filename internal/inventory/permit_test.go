package inventory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func scannerAPIAttempts(m Metrics) uint64 {
	return m.StatCalls + m.DirectoryOpenCalls + m.DirectoryReadCalls + m.FilesystemStatCalls + m.MountIdentityCalls + m.PathResolutionCalls
}

func scannerAPIKinds(m Metrics) map[APICallKind]uint64 {
	return map[APICallKind]uint64{APIStat: m.StatCalls, APIDirectoryOpen: m.DirectoryOpenCalls, APIDirectoryRead: m.DirectoryReadCalls, APIFilesystemStat: m.FilesystemStatCalls, APIMountIdentity: m.MountIdentityCalls, APIPathResolution: m.PathResolutionCalls}
}

func assertEmptyDeniedBatch(t *testing.T, b state.ScanBatch, err, cause error) {
	t.Helper()
	if !errors.Is(err, ErrAPIPermitDenied) || (cause != nil && !errors.Is(err, cause)) || !reflect.DeepEqual(b, state.ScanBatch{}) {
		t.Fatal("denial became source evidence or lost its cause", b, err)
	}
}

func TestScannerPermitStartupBoundsAndEveryDenial(t *testing.T) {
	root := t.TempDir()
	excludes := []string{filepath.Join(root, "absent excluded")}
	private := []string{filepath.Join(root, "absent private")}
	allowance, err := StartupAPIAttemptAllowance([]string{root}, excludes, private)
	if err != nil || allowance != 30 {
		t.Fatal("startup bound differs", allowance, err)
	}
	var calls []APICallKind
	s, err := NewPermitted(context.Background(), []string{root}, excludes, private, func(_ context.Context, kind APICallKind) error {
		calls = append(calls, kind)
		return nil
	})
	if err != nil || scannerAPIAttempts(s.Metrics()) != uint64(len(calls)) || int64(len(calls)) > allowance {
		t.Fatal("constructor attempts disagree", calls, err)
	}
	s.Close()
	blocked := errors.New("generated reservation exhausted")
	for denied := 1; denied <= len(calls); denied++ {
		invoked := 0
		s, err := NewPermitted(context.Background(), []string{root}, excludes, private, func(context.Context, APICallKind) error {
			invoked++
			if invoked == denied {
				return blocked
			}
			return nil
		})
		if s != nil || !errors.Is(err, ErrAPIPermitDenied) || !errors.Is(err, blocked) || invoked != denied {
			t.Fatal("constructor ignored denial or published partial scope", denied, invoked, s, err)
		}
	}
	maxRoots := make([]string, MaxAPIAttemptAllowance/2-12)
	if n, err := StartupAPIAttemptAllowance(maxRoots, nil, nil); err != nil || n != MaxAPIAttemptAllowance {
		t.Fatal("exact startup ceiling refused", n, err)
	}
	maxRoots = append(maxRoots, root)
	callback := false
	s, err = NewPermitted(context.Background(), maxRoots, nil, nil, func(context.Context, APICallKind) error { callback = true; return nil }, func(*Scanner) error { callback = true; return nil })
	if err == nil || s != nil || callback {
		t.Fatal("oversized startup reached a callback", s, err, callback)
	}
	if s, err := NewPermitted(context.Background(), []string{root}, nil, nil, nil); s != nil || !errors.Is(err, ErrAPIPermitDenied) {
		t.Fatal("missing permit accepted", s, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	callback = false
	s, err = NewPermitted(ctx, []string{root}, nil, nil, func(context.Context, APICallKind) error { callback = true; return nil })
	if s != nil || callback || !errors.Is(err, ErrAPIPermitDenied) || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled construction reached source admission", s, err, callback)
	}
}

func TestScannerPermitFreezesInputsAndRejectsLateConstructionCancel(t *testing.T) {
	root, replacement := t.TempDir(), t.TempDir()
	roots := []string{root}
	excludes := []string{filepath.Join(root, "excluded")}
	private := []string{filepath.Join(root, "private")}
	s, err := NewPermitted(context.Background(), roots, excludes, private, func(context.Context, APICallKind) error {
		roots[0], excludes[0], private[0] = replacement, replacement, replacement
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.roots[root] || s.roots[replacement] || !s.excluded(filepath.Join(root, "excluded")) || !s.excluded(filepath.Join(root, "private")) || s.excluded(replacement) {
		t.Fatal("callback replaced captured scope", s.roots, s.excludes)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	allowance := int64(scannerAPIAttempts(probe.Metrics()))
	probe.Close()
	used := int64(0)
	late, err := NewPermitted(ctx, []string{root}, nil, nil, func(context.Context, APICallKind) error {
		used++
		if used == allowance {
			cancel() // Nil admits this last attempt; final publication must fail.
		}
		return nil
	})
	if late != nil || !errors.Is(err, ErrAPIPermitDenied) || !errors.Is(err, context.Canceled) {
		t.Fatal("late canceled construction published scope", used, late, err)
	}
}

func TestScannerPermitEveryNextDenialDiscardsTentativeEvidence(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "file"))
	if err := os.Mkdir(filepath.Join(root, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	job := state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Cursor: []byte("prior saved cursor"), Kind: state.ScanKind}
	baseline, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var trace []APICallKind
	b, err := baseline.NextPermitted(context.Background(), job, func(_ context.Context, kind APICallKind) error { trace = append(trace, kind); return nil })
	baseline.Close()
	if err != nil || b.Fault != "" || len(b.Entries) != 2 {
		t.Fatal(b, err)
	}
	blocked := errors.New("generated quota expiry")
	for denied := 1; denied <= len(trace); denied++ {
		s, err := New([]string{root}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		before := scannerAPIAttempts(s.Metrics())
		beforeKinds := scannerAPIKinds(s.Metrics())
		invoked, admitted := 0, 0
		b, err := s.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error {
			invoked++
			if invoked == denied {
				return blocked
			}
			admitted++
			return nil
		})
		assertEmptyDeniedBatch(t, b, err, blocked)
		if s.current != nil || scannerAPIAttempts(s.Metrics())-before != uint64(admitted) || !bytes.Equal(job.Cursor, []byte("prior saved cursor")) {
			t.Fatal("denial counted a call, retained tentative stream or changed saved cursor", denied, admitted, s.Metrics())
		}
		wantKinds := map[APICallKind]uint64{}
		for _, kind := range trace[:denied-1] {
			wantKinds[kind]++
		}
		for kind, count := range scannerAPIKinds(s.Metrics()) {
			if count-beforeKinds[kind] != wantKinds[kind] {
				t.Fatal("per-kind denied accounting differs", denied, kind, count-beforeKinds[kind], wantKinds[kind])
			}
		}
		seen := map[string]bool{}
		retry := job
		complete := false
		for step := 0; step < 4; step++ {
			b, err := s.NextPermitted(context.Background(), retry, func(context.Context, APICallKind) error { return nil })
			if err != nil || b.Fault != "" {
				t.Fatal("retry failed", denied, b, err)
			}
			for _, entry := range b.Entries {
				seen[string(entry.Path)] = true
			}
			if b.Complete {
				complete = true
				break
			}
			retry.Cursor = b.Cursor
		}
		s.Close()
		if !complete || len(seen) != 2 || !seen["child"] || !seen["file"] {
			t.Fatal("denial lost retry evidence", denied, seen)
		}
	}
}

func TestScannerPermitWindowExpiryRollbackAndLateCancellation(t *testing.T) {
	for _, reason := range []string{"UTC expiry", "wall rollback", "cancellation", "late cancellation"} {
		t.Run(reason, func(t *testing.T) {
			s, job, root := scannerFixture(t)
			write(t, filepath.Join(root, "file"))
			start := time.Date(2026, 10, 9, 23, 59, 59, 0, time.UTC)
			wall, highWater, expires := start, start, start.Add(time.Second)
			refusal := errors.New("generated " + reason)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			limit := 2
			if reason == "late cancellation" {
				probe, err := New([]string{root}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				limit = 0
				_, err = probe.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error { limit++; return nil })
				probe.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			admitted := 0
			before := scannerAPIAttempts(s.Metrics())
			b, err := s.NextPermitted(ctx, job, func(context.Context, APICallKind) error {
				if wall.Before(highWater) || !wall.Before(expires) {
					return refusal
				}
				highWater = wall
				admitted++
				if admitted == limit {
					switch reason {
					case "UTC expiry":
						wall = expires
					case "wall rollback":
						wall = start.Add(-time.Second)
					default:
						cancel(refusal)
					}
				}
				return nil
			})
			assertEmptyDeniedBatch(t, b, err, refusal)
			if admitted != limit || scannerAPIAttempts(s.Metrics())-before != uint64(limit) || s.current != nil {
				t.Fatal("expiry/cancel consumed extra calls or retained evidence", reason, admitted, s.Metrics())
			}
		})
	}
}

func TestScannerPermitMissingOrClosedDiscardsExistingStream(t *testing.T) {
	s, job, root := scannerFixture(t)
	for i := 0; i < state.MaxBatchEntries+1; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("file-%03d", i)))
	}
	first := next(t, s, job)
	if first.Complete || s.current == nil {
		t.Fatal("fixture did not retain a bounded stream", first)
	}
	before := scannerAPIAttempts(s.Metrics())
	b, err := s.NextPermitted(context.Background(), job, nil)
	assertEmptyDeniedBatch(t, b, err, nil)
	if s.current != nil || scannerAPIAttempts(s.Metrics()) != before {
		t.Fatal("missing callback retained stream or accessed source")
	}
	s.Close()
	b, err = s.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error { t.Fatal("closed scanner invoked callback"); return nil })
	assertEmptyDeniedBatch(t, b, err, nil)
	if scannerAPIAttempts(s.Metrics()) != before {
		t.Fatal("closed scanner accessed source")
	}
}

func TestScannerPermitFailuresCancellationAndCallerJobMutation(t *testing.T) {
	s, job, root := scannerFixture(t)
	job.Path = []byte("missing")
	before := scannerAPIAttempts(s.Metrics())
	admitted := 0
	b, err := s.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error { admitted++; return nil })
	if err != nil || b.Fault == "" || b.Complete || scannerAPIAttempts(s.Metrics())-before != uint64(admitted) {
		t.Fatal("admitted failure was not counted", b, err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	windowExpired := errors.New("generated UTC reservation window expired")
	cancel(windowExpired)
	before = scannerAPIAttempts(s.Metrics())
	b, err = s.NextPermitted(ctx, job, func(context.Context, APICallKind) error { t.Fatal("canceled permit invoked"); return nil })
	assertEmptyDeniedBatch(t, b, err, windowExpired)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("custom cause lost cancellation identity", err)
	}
	if scannerAPIAttempts(s.Metrics()) != before {
		t.Fatal("canceled call was counted")
	}
	write(t, filepath.Join(root, "file"))
	job.Path, job.Cursor = []byte("."), []byte("old")
	b, err = s.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error {
		job.RootPath[0], job.Path[0], job.Cursor[0] = '!', '!', '!'
		return nil
	})
	if err != nil || b.Fault != "" || len(b.Entries) != 1 || string(b.Entries[0].Path) != "file" {
		t.Fatal("callback altered frozen job", b, err)
	}
}

func TestScannerPermitPlatformMountAndUnrelatedHelpers(t *testing.T) {
	s, _, root := scannerFixture(t)
	f, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	blocked := errors.New("generated mount permit refused")
	var kinds []APICallKind
	g := &apiGuard{ctx: context.Background(), permit: func(_ context.Context, kind APICallKind) error {
		kinds = append(kinds, kind)
		if kind == APIMountIdentity {
			return blocked
		}
		return nil
	}}
	before := s.Metrics()
	_, _, err = s.filesystemGuarded(g, int(f.Fd()))
	after := s.Metrics()
	if after.FilesystemStatCalls != before.FilesystemStatCalls+1 || after.MountIdentityCalls != before.MountIdentityCalls {
		t.Fatal("denied platform call was counted", before, after)
	}
	if runtime.GOOS == "linux" {
		if !errors.Is(err, ErrAPIPermitDenied) || !errors.Is(err, blocked) || !reflect.DeepEqual(kinds, []APICallKind{APIFilesystemStat, APIMountIdentity}) {
			t.Fatal(kinds, err)
		}
	} else if err != nil || !reflect.DeepEqual(kinds, []APICallKind{APIFilesystemStat}) {
		t.Fatal(kinds, err)
	}
	// An unmetered live helper must not see another operation's callback.
	count := len(kinds)
	if _, _, err := s.filesystem(int(f.Fd())); err != nil || len(kinds) != count {
		t.Fatal("permit leaked to unrelated helper", kinds, err)
	}
}

func TestScannerPermitPacedPartialAndConcurrentIsolation(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"one", "two", "three"} {
		write(t, filepath.Join(root, name))
	}
	s, err := New([]string{root}, nil, nil, WithEntryRate(10))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job := state.Job{ID: 1, RootID: 1, RootPath: []byte(root), Path: []byte("."), Kind: state.ScanKind}
	seen := map[string]bool{}
	partials := 0
	for step := 0; step < 12; step++ {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		b, err := s.NextPermitted(ctx, job, func(context.Context, APICallKind) error { return nil })
		cancel()
		if err != nil || b.Fault != "" {
			t.Fatal(b, err)
		}
		for _, entry := range b.Entries {
			if seen[string(entry.Path)] {
				t.Fatal("paced partial replayed entry", entry)
			}
			seen[string(entry.Path)] = true
		}
		if b.Complete {
			break
		}
		partials++
		job.Cursor = b.Cursor
	}
	if len(seen) != 3 || partials == 0 {
		t.Fatal("permit broke paced partials", seen, partials)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var callbacks atomic.Uint64
	go func() {
		_, err := s.NextPermitted(context.Background(), job, func(context.Context, APICallKind) error {
			if callbacks.Add(1) == 1 {
				close(entered)
				<-release
			}
			return nil
		})
		done <- err
	}()
	<-entered
	if _, err := s.resolve(root); err != nil || callbacks.Load() != 1 {
		close(release)
		t.Fatal("operation permit leaked to concurrent resolution", err, callbacks.Load())
	}
	_ = s.Metrics() // Does not need the scanner mutex held by NextPermitted.
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
