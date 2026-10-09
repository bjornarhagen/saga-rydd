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
	}{{"overflow", "while :; do printf 'generated-compiler-output-0123456789'; done", time.Second, ErrBuild}, {"deadline", "exec /bin/sleep 30", 50 * time.Millisecond, context.DeadlineExceeded}, {"failure", "printf 'private-path-canary' >&2; exit 7", time.Second, ErrBuild}} {
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

func TestCandidateCompilerCancellationStopsAndReapsDirectChild(t *testing.T) {
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "heartbeat")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = runBuild(ctx, executable, dir, []string{"RYDD_GENERATED_COMPILER_HEARTBEAT=" + path}, []string{"-test.run=^TestCandidateCompilerDirectChildHelper$"}, 5*time.Second)
		close(done)
	}()
	// Every assertion path cancels and joins the exact owned child operation.
	defer func() { cancel(); <-done }()
	readyBy := time.NewTimer(3 * time.Second)
	defer readyBy.Stop()
	for {
		body, readErr := os.ReadFile(path)
		if readErr == nil && len(body) > 0 {
			break
		}
		select {
		case <-done:
			t.Fatal("generated direct compiler exited before readiness", runErr)
		case <-readyBy.C:
			t.Fatal("generated direct compiler did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if !errors.Is(runErr, context.Canceled) {
		t.Fatal(runErr)
	}
	first, err := os.ReadFile(path)
	if err != nil || len(first) == 0 {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(first) {
		t.Fatal("direct compiler continued after cancellation and Wait", err)
	}
}

func TestCandidateCompilerDirectChildHelper(t *testing.T) {
	path := os.Getenv("RYDD_GENERATED_COMPILER_HEARTBEAT")
	if path == "" {
		return
	}
	for {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("x")); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
