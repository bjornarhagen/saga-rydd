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
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestAPIPacingPureBoundsAndEarlyScopeRefusals(t *testing.T) {
	bounds, err := APIPacingWorkBounds([]string{"/generated/offline"}, []string{"/generated/excluded"}, []string{"/generated/private"})
	if err != nil || bounds.Startup != 30 || bounds.MinimumNext != 32794 || bounds.MaximumNext != 33556 || bounds.FinalValidation != 16393 || bounds.Entry != 6 || bounds.MaximumNext > MaxAPIAttemptAllowance {
		t.Fatal("pacing work bounds differ", bounds, err)
	}
	// The maximum clean component count follows independently from the byte
	// ceiling: /a repeated 2048 times and a/ repeated 2047 times plus a.
	abs := strings.Repeat("/a", 2048)
	rel := strings.Repeat("a/", 2047) + "a"
	if len(abs) != 4096 || len(rel) != 4095 || !pacedAbsolutePath(abs) || !validPath(rel) {
		t.Fatal("supported byte/component boundary differs")
	}
	if bounds.MinimumNext != int64(6*2048+10*2048+26) || bounds.MaximumNext != int64(6*2048+10*2048+20+128*6) {
		t.Fatal("full traversal/validation capacity is understated")
	}
	maxRoots := make([]string, MaxAPIAttemptAllowance/2-12)
	for i := range maxRoots {
		maxRoots[i] = "/offline"
	}
	if b, err := APIPacingWorkBounds(maxRoots, nil, nil); err != nil || b.Startup != MaxAPIAttemptAllowance {
		t.Fatal("exact startup ceiling refused", b, err)
	}
	maxRoots = append(maxRoots, "invalid but count must refuse first")
	if b, err := APIPacingWorkBounds(maxRoots, nil, nil); !errors.Is(err, ErrAPIPacingInput) || !reflect.DeepEqual(b, APIPacingBounds{}) {
		t.Fatal("oversized pure scope accepted", b, err)
	}
	for _, path := range []string{"", "relative", "/a/../b", "/a\x00b", abs + "x"} {
		callback := false
		permit := func(context.Context, APICallKind) error { callback = true; return nil }
		option := func(*Scanner) error { callback = true; return nil }
		s, err := NewPermittedPaced(context.Background(), []string{path}, nil, nil, permit, option)
		if s != nil || !errors.Is(err, ErrAPIPacingInput) || callback {
			t.Fatal("invalid paced root reached callbacks", len(path), s, err, callback)
		}
	}
	guard := &apiGuard{paced: true}
	if err := guard.pacedRoot(abs + "x"); !errors.Is(err, ErrAPIPacingProfile) || !errors.Is(err, ErrAPIPermitDenied) {
		t.Fatal("canonical expansion was not an explicit profile refusal", err)
	}
	if err := (&apiGuard{}).pacedRoot(abs + "x"); err != nil {
		t.Fatal("ordinary guard acquired the optional profile", err)
	}
}

func TestAPIPacingActualTraversalCallBounds(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "first", "second", "child"), 0700); err != nil {
		t.Fatal(err)
	}
	var startup []APICallKind
	s, err := NewPermittedPaced(context.Background(), []string{root}, nil, nil, func(_ context.Context, kind APICallKind) error {
		startup = append(startup, kind)
		return nil
	}, WithRootStreams(2))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	bounds, err := APIPacingWorkBounds([]string{root}, nil, nil)
	if err != nil || int64(len(startup)) > bounds.Startup || scannerAPIAttempts(s.Metrics()) != uint64(len(startup)) {
		t.Fatal("startup trace exceeds pure bounds", len(startup), bounds, err)
	}
	var calls []APICallKind
	checks := 0
	b, err := s.NextPermittedPaced(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte("first/second")}, func(_ context.Context, kind APICallKind) error {
		calls = append(calls, kind)
		return nil
	}, func(_ context.Context, required int64) (bool, error) {
		checks++
		if required != 16399 {
			t.Fatal("entry omitted final validation capacity", required)
		}
		return true, nil
	})
	if err != nil || b.Fault != "" || len(b.Entries) != 1 || checks != 1 {
		t.Fatal("generated traversal failed", b, err, checks)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	r := len(strings.Split(strings.TrimPrefix(canonical, "/"), "/"))
	// Independent primitive count for this two-component relative traversal:
	// Linux opens need stat/open/stat plus fsstat/mount; Darwin has fsstat only.
	want := 6*r + 10*2 + 26
	if runtime.GOOS == "darwin" {
		want = 6*r + 8*2 + 20
	}
	if len(calls) != want || scannerAPIAttempts(s.Metrics()) != uint64(len(startup)+len(calls)) {
		t.Fatal("actual traversal/revalidation trace differs", len(calls), want, calls)
	}
	seen := map[APICallKind]bool{}
	for _, kind := range calls {
		seen[kind] = true
	}
	for _, kind := range []APICallKind{APIStat, APIDirectoryOpen, APIDirectoryRead, APIFilesystemStat, APIPathResolution} {
		if !seen[kind] {
			t.Fatal("unmetered primitive", kind)
		}
	}
	if runtime.GOOS == "linux" && !seen[APIMountIdentity] {
		t.Fatal("unmetered Linux mount observation")
	}
	if !b.Complete {
		last := b
		b, err = s.NextPermittedPaced(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte("first/second"), Cursor: last.Cursor}, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) {
			t.Fatal("EOF reached an entry boundary")
			return false, nil
		})
		if err != nil || b.Fault != "" || !b.Complete || b.Generation != last.Generation || len(b.Entries) != 0 {
			t.Fatal("separate EOF did not finish the observed generation", b, err)
		}
	}
}

func TestAPIPacingYieldRetainsNamesAcrossRootSwitches(t *testing.T) {
	a, other := t.TempDir(), t.TempDir()
	want := make(map[string]bool)
	for i := range 139 {
		name := fmt.Sprintf("file-%03d", i)
		want[name] = true
		if err := os.WriteFile(filepath.Join(a, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(other, "other"), []byte("other"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := NewPermittedPaced(context.Background(), []string{a, other}, nil, nil, func(context.Context, APICallKind) error { return nil }, WithRootStreams(2))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	j := state.Job{ID: 1, RootPath: []byte(a), Path: []byte(".")}
	yielded, checks := false, 0
	var tail []APICallKind
	b, err := s.NextPermittedPaced(context.Background(), j, func(_ context.Context, kind APICallKind) error {
		if yielded {
			tail = append(tail, kind)
		}
		return nil
	}, func(context.Context, int64) (bool, error) { checks++; yielded = checks == 3; return !yielded, nil })
	if err != nil || b.Fault != "" || b.Complete || len(b.Entries) != 2 || len(b.Cursor) != 16 || len(tail) == 0 || tail[0] != APIPathResolution {
		t.Fatal("yield skipped validation or fabricated completion", b, err, tail)
	}
	generation := b.Generation
	seen := map[string]bool{}
	for _, e := range b.Entries {
		seen[string(e.Path)] = true
	}
	parked := s.rootStreams[a]
	if parked == nil || len(parked.pending) != 126 {
		t.Fatal("yield consumed unobserved names")
	}
	firstPending := parked.pending[0]
	// Ordinary methods must not reuse an operation-local capacity callback.
	otherBatch, err := s.NextPermitted(context.Background(), state.Job{ID: 2, RootPath: []byte(other), Path: []byte(".")}, func(context.Context, APICallKind) error { return nil })
	if err != nil || otherBatch.Fault != "" || len(otherBatch.Entries) != 1 || checks != 3 {
		t.Fatal("pacing callback leaked to another root/method", otherBatch, err, checks)
	}
	j.Cursor = bytes.Clone(b.Cursor)
	before := s.Metrics()
	b, err = s.NextPermittedPaced(context.Background(), j, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) { return false, nil })
	if err != nil || b.Fault != "" || b.Complete || len(b.Entries) != 0 || b.Generation != generation || s.rootStreams[a].pending[0] != firstPending || s.Metrics().EntryInspections != before.EntryInspections || s.Metrics().DirectoryReadCalls != before.DirectoryReadCalls {
		t.Fatal("zero-entry yield consumed names or restarted enumeration", b, err)
	}
	j.Cursor = bytes.Clone(b.Cursor)
	for turn := 0; turn < 60; turn++ {
		checks := 0
		b, err = s.NextPermittedPaced(context.Background(), j, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) { checks++; return checks <= 3, nil })
		if err != nil || b.Fault != "" || b.Generation != generation {
			t.Fatal("resumed stream changed generation or failed", b, err)
		}
		for _, e := range b.Entries {
			path := string(e.Path)
			if !want[path] || seen[path] {
				t.Fatal("yield lost/replayed an observed name", path)
			}
			seen[path] = true
		}
		if b.Complete {
			break
		}
		j.Cursor = bytes.Clone(b.Cursor)
	}
	if len(seen) != len(want) || !b.Complete {
		t.Fatal("bounded resumed traversal did not finish", len(seen), len(want))
	}
	for name := range want {
		body, err := os.ReadFile(filepath.Join(a, name))
		if err != nil || string(body) != name {
			t.Fatal("metadata work changed generated body", name, err)
		}
	}
}

func TestAPIPacingCapacityAndLatePermitErrorsDiscardTentativeBatch(t *testing.T) {
	for _, mode := range []string{"capacity_error", "capacity_cancel", "late_validation", "closed"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			for _, name := range []string{"one", "two", "three"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s, err := NewPermittedPaced(context.Background(), []string{root}, nil, nil, func(context.Context, APICallKind) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			blocked := errors.New("generated capacity or tail refusal")
			checks, yielded := 0, false
			b, err := s.NextPermittedPaced(ctx, state.Job{ID: 1, RootPath: []byte(root), Path: []byte(".")}, func(_ context.Context, kind APICallKind) error {
				if yielded && kind == APIStat {
					return blocked
				}
				return nil
			}, func(context.Context, int64) (bool, error) {
				checks++
				if checks == 1 {
					return true, nil
				}
				switch mode {
				case "capacity_error":
					return false, blocked
				case "capacity_cancel":
					cancel()
					return false, nil
				case "closed":
					s.Close()
					return false, nil
				default:
					yielded = true
					return false, nil
				}
			})
			var cause error
			if mode == "capacity_error" || mode == "late_validation" {
				cause = blocked
			} else if mode == "capacity_cancel" {
				cause = context.Canceled
			}
			assertEmptyDeniedBatch(t, b, err, cause)
			if checks != 2 || s.current != nil {
				t.Fatal("denied partial state escaped", checks)
			}
			if mode != "closed" {
				b, err = s.NextPermittedPaced(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte(".")}, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) { return true, nil })
				if err != nil || b.Fault != "" || len(b.Entries) != 3 {
					t.Fatal("denied tentative names were committed/lost", b, err)
				}
				if !b.Complete {
					last := b
					b, err = s.NextPermittedPaced(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte("."), Cursor: last.Cursor}, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) {
						t.Fatal("EOF reached an entry boundary")
						return false, nil
					})
					if err != nil || b.Fault != "" || !b.Complete || b.Generation != last.Generation || len(b.Entries) != 0 {
						t.Fatal("retry did not reach confirmed EOF", b, err)
					}
				}
			}
		})
	}
}

func TestAPIPacingEarlyNextRefusalsAndFailedCalls(t *testing.T) {
	root := t.TempDir()
	s, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job := state.Job{ID: 1, RootPath: []byte(root), Path: []byte(".")}
	for _, mode := range []string{"nil_context", "nil_permit", "nil_capacity", "relative", "overlong", "root", "canceled"} {
		ctx := context.Background()
		called := false
		permit := APIPermit(func(context.Context, APICallKind) error { called = true; return nil })
		capacity := APIEntryCapacity(func(context.Context, int64) (bool, error) { called = true; return true, nil })
		j := job
		switch mode {
		case "nil_context":
			ctx = nil
		case "nil_permit":
			permit = nil
		case "nil_capacity":
			capacity = nil
		case "relative":
			j.Path = []byte("../escape")
		case "overlong":
			j.Path = bytes.Repeat([]byte("a"), 4097)
		case "root":
			j.RootPath = []byte("relative")
		case "canceled":
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		before := s.Metrics()
		b, err := s.NextPermittedPaced(ctx, j, permit, capacity)
		assertEmptyDeniedBatch(t, b, err, nil)
		if called || scannerAPIAttempts(s.Metrics()) != scannerAPIAttempts(before) {
			t.Fatal("early refusal admitted a source API", mode)
		}
	}
	// A genuine missing directory remains a source fault. Both admitted resolve
	// and the failed guarded stat attempt count; it is not a rate denial.
	before := s.Metrics()
	calls := 0
	missing := job
	missing.Path = []byte("absent")
	b, err := s.NextPermittedPaced(context.Background(), missing, func(context.Context, APICallKind) error { calls++; return nil }, func(context.Context, int64) (bool, error) {
		t.Fatal("missing directory reached entry admission")
		return false, nil
	})
	if err != nil || b.Fault == "" || b.Complete || calls == 0 || scannerAPIAttempts(s.Metrics())-scannerAPIAttempts(before) != uint64(calls) {
		t.Fatal("admitted failed source call vanished or became a quota refusal", b, err, calls)
	}
}

func TestAPIPacingFrozenInputsAndOrdinaryMethods(t *testing.T) {
	root := t.TempDir()
	name := "control\n\tfile"
	if err := os.WriteFile(filepath.Join(root, name), []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	roots := []string{root}
	mutated := false
	s, err := NewPermittedPaced(context.Background(), roots, nil, nil, func(context.Context, APICallKind) error {
		if !mutated {
			roots[0] = "/not-the-generated-root"
			mutated = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job := state.Job{ID: 1, RootPath: []byte(root), Path: []byte(".")}
	b, err := s.NextPermittedPaced(context.Background(), job, func(context.Context, APICallKind) error { return nil }, func(context.Context, int64) (bool, error) {
		job.RootPath[0] = 'x'
		job.Path[0] = 'x'
		return false, nil
	})
	if err != nil || b.Fault != "" || b.Complete || len(b.Entries) != 0 {
		t.Fatal("caller mutation widened/falsified scope", b, err)
	}
	// The ordinary method does not retain the previous operation's callback or
	// bounded-profile mode; it resumes the exact stream with its saved cursor.
	b, err = s.Next(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte("."), Cursor: bytes.Clone(b.Cursor)})
	if err != nil || b.Fault != "" || len(b.Entries) != 1 || !bytes.Equal(b.Entries[0].Path, []byte(name)) {
		t.Fatal("ordinary method inherited paced callback or lost raw path", b, err)
	}
	if !b.Complete {
		b, err = s.Next(context.Background(), state.Job{ID: 1, RootPath: []byte(root), Path: []byte("."), Cursor: bytes.Clone(b.Cursor)})
		if err != nil || b.Fault != "" || !b.Complete || len(b.Entries) != 0 {
			t.Fatal("ordinary method failed to reach confirmed EOF", b, err)
		}
	}
}
