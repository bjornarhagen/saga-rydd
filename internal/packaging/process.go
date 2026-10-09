package packaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

const buildLimit = 2 * time.Minute
const outputLimit = 16 << 10

var ErrBuild = errors.New("fixed candidate compilation failed")

// compile uses only the invoking toolchain and explicitly supplied module cache.
// It neither installs a toolchain nor resolves dependencies over the network.
// Build tools and this developer repository remain trusted inputs.
func compile(ctx context.Context, repoDir, binary string, t target, version string) error {
	cache := os.Getenv("GOCACHE")
	modules := os.Getenv("GOMODCACHE")
	if !filepath.IsAbs(cache) || !filepath.IsAbs(modules) {
		return fmt.Errorf("%w: explicit absolute GOCACHE and GOMODCACHE are required", ErrInput)
	}
	work := filepath.Join(filepath.Dir(binary), "tool-work")
	for _, name := range []string{"tmp", "telemetry", "gopath"} {
		if err := os.MkdirAll(filepath.Join(work, name), 0700); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(work, "telemetry", "mode"), []byte("off"), 0600); err != nil {
		return err
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"), "GOROOT=" + runtime.GOROOT(),
		"GOCACHE=" + cache, "GOMODCACHE=" + modules, "GOCACHEPROG=",
		"GOPATH=" + filepath.Join(work, "gopath"), "GOTMPDIR=" + filepath.Join(work, "tmp"),
		"TMPDIR=" + filepath.Join(work, "tmp"), "TEST_TELEMETRY_DIR=" + filepath.Join(work, "telemetry"),
		"GO_TELEMETRY_CHILD=", "GO_TELEMETRY_CHILD_UPLOAD=",
		"GOTOOLCHAIN=local", "GOENV=off", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=-mod=readonly", "GO111MODULE=on",
		"CGO_ENABLED=0", "GOOS=" + t.os, "GOARCH=" + t.arch, "GOAMD64=v1", "GOARM64=v8.0", "GODEBUG=",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_COUNT=2", "GIT_CONFIG_KEY_0=core.fsmonitor", "GIT_CONFIG_VALUE_0=false", "GIT_CONFIG_KEY_1=core.hooksPath", "GIT_CONFIG_VALUE_1=/dev/null", "GIT_TERMINAL_PROMPT=0",
	}
	return runBuild(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), repoDir, env,
		[]string{"build", "-trimpath", "-buildvcs=true", "-pgo=off", "-ldflags=" + linkerFlags(version), "-o", binary, "./cmd/rydd"}, buildLimit)
}

func runBuild(parent context.Context, executable, directory string, env, args []string, limit time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, limit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir, cmd.Env = directory, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	stdout := &boundedOutput{limit: outputLimit, cancel: cancel}
	stderr := &boundedOutput{limit: outputLimit, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run() // Run waits and reaps after cancellation, including output overflow.
	// Never signal a numeric group after Run has reaped its leader. WaitDelay
	// bounds output-pipe waits; a tool leaving those pipes open is refused.
	// Cancellation handles the trusted live compiler group, not arbitrary
	// detached descendants or authenticated process-group provenance.
	if errors.Is(err, exec.ErrWaitDelay) {
		return fmt.Errorf("%w: compiler output did not finish", ErrBuild)
	}
	if stdout.overflowed() || stderr.overflowed() {
		return fmt.Errorf("%w: compiler output exceeded the retained limit", ErrBuild)
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return ErrBuild
	} // Raw compiler output can contain private developer paths.
	return nil
}

type boundedOutput struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	remaining := b.limit - b.buffer.Len()
	if len(p) > remaining {
		p = p[:remaining]
		b.overflow = true
		if b.cancel != nil {
			b.cancel()
		}
	}
	_, err := b.buffer.Write(p)
	return n, err
}

func (b *boundedOutput) overflowed() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.overflow }
