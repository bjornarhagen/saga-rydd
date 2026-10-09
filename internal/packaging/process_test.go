package packaging

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCandidateCompilerOutputAndDeadlineAreBounded(t *testing.T) {
	output := &boundedOutput{limit: 1024}
	n, err := io.Copy(output, io.LimitReader(strings.NewReader(strings.Repeat("x", 122880)), 122880))
	if err != nil || n != 122880 || !output.overflowed() || output.buffer.Len() != 1024 {
		t.Fatal(n, err, output.buffer.Len())
	}
	dir := t.TempDir()
	for _, test := range []struct {
		name, script string
		limit        time.Duration
		want         error
	}{{"overflow", "while :; do printf 'generated-compiler-output-0123456789'; done", time.Second, ErrBuild}, {"deadline", "sleep 30 & wait", 50 * time.Millisecond, context.DeadlineExceeded}, {"failure", "printf 'private-path-canary' >&2; exit 7", time.Second, ErrBuild}} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(dir, test.name)
			if err := os.WriteFile(file, []byte("#!/bin/sh\n"+test.script+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			err := runBuild(context.Background(), file, dir, []string{"PATH=/usr/bin:/bin"}, nil, test.limit)
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "private-path-canary") || time.Since(started) > 3*time.Second {
				t.Fatal(err, time.Since(started))
			}
		})
	}
}

func TestCandidateCompileRequiresExplicitCachesBeforeToolAccess(t *testing.T) {
	t.Setenv("GOCACHE", "")
	t.Setenv("GOMODCACHE", "")
	if err := compile(context.Background(), t.TempDir(), filepath.Join(t.TempDir(), "rydd"), targets[0], "v1"); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestCandidateCompilerCancellationStopsChildProcessGroup(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "compiler")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n(while :; do printf x >> heartbeat; sleep 0.01; done) &\nwait\n"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "heartbeat")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runBuild(ctx, script, dir, []string{"PATH=/usr/bin:/bin"}, nil, 5*time.Second) }()
	// Native executable admission can take longer than a short deadline. Wait
	// for actual generated child work before testing cancellation of that work.
	readyBy := time.Now().Add(3 * time.Second)
	for {
		body, err := os.ReadFile(path)
		if err == nil && len(body) > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatal("generated compiler exited before child readiness", err)
		default:
		}
		if time.Now().After(readyBy) {
			cancel()
			<-done
			t.Fatal("generated compiler child did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil || len(first) == 0 {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(first) {
		t.Fatal("compiler descendant continued after cancellation", err)
	}
}
