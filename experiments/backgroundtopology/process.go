package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var errChildOutput = errors.New("generated child output exceeded its declared limit")
var errChild = errors.New("generated child failed; inspect retained private logs")

type usage struct {
	Available bool   `json:"available"`
	RSSScope  string `json:"peak_rss_scope"`
	UserNS    int64  `json:"user_cpu_ns"`
	SystemNS  int64  `json:"system_cpu_ns"`
	RSS       int64  `json:"peak_rss_bytes"`
	ElapsedNS int64  `json:"launch_to_wait_return_ns"`
}

// A named buffer avoids promoting ReaderFrom, which could bypass Write's cap.
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
	if len(p) > b.limit-b.buffer.Len() {
		b.overflow = true
		b.cancel()
		return len(p), nil
	}
	return b.buffer.Write(p)
}
func (b *boundedOutput) value() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buffer.Bytes()...), b.overflow
}

type child struct {
	cmd             *exec.Cmd
	cancel          context.CancelFunc
	done            chan struct{}
	out, diagnostic *boundedOutput
	started         time.Time
	err             error
	usage           usage
	forced          atomic.Bool
}

func start(ctx context.Context, binary string, args, env []string, limit int) (*child, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &child{cancel: cancel, done: make(chan struct{}), started: time.Now()}
	c.out = &boundedOutput{limit: limit, cancel: cancel}
	c.diagnostic = &boundedOutput{limit: 65536, cancel: cancel}
	c.cmd = exec.CommandContext(ctx, binary, args...)
	// Preserve the default guarded os.Process handle; record cancellation intent.
	c.cmd.Cancel = func() error { c.forced.Store(true); return c.cmd.Process.Kill() }
	c.cmd.Env = env
	c.cmd.Stdout = c.out
	c.cmd.Stderr = c.diagnostic
	c.cmd.WaitDelay = time.Second
	if err := c.cmd.Start(); err != nil {
		cancel()
		return nil, errChild
	}
	go func() {
		c.err = c.cmd.Wait()
		c.usage.ElapsedNS = time.Since(c.started).Nanoseconds()
		c.usage.RSSScope = "individual_child_lifetime"
		cancel()
		if c.cmd.ProcessState != nil {
			if r, ok := c.cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
				c.usage.UserNS = timevalNS(r.Utime)
				c.usage.SystemNS = timevalNS(r.Stime)
				c.usage.RSS = r.Maxrss
				if runtime.GOOS == "linux" {
					if c.usage.RSS > math.MaxInt64/1024 {
						c.usage.RSS = -1
					} else {
						c.usage.RSS *= 1024
					}
				}
			}
		}
		c.usage.Available = c.usage.UserNS >= 0 && c.usage.SystemNS >= 0 && c.usage.RSS > 0
		close(c.done)
	}()
	return c, nil
}
func (c *child) join(ctx context.Context) error {
	select {
	case <-c.done:
	case <-ctx.Done():
		c.forced.Store(true)
		c.cancel()
		<-c.done
	}
	_, a := c.out.value()
	_, b := c.diagnostic.value()
	if a || b {
		return errChildOutput
	}
	if c.err != nil || !c.usage.Available {
		if ctx.Err() != nil && c.cmd.ProcessState != nil {
			if status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
				return errors.Join(errChild, ctx.Err())
			}
		}
		return errChild
	}
	return nil
}
func (c *child) cleanup() {
	select {
	case <-c.done:
		return
	default:
		c.forced.Store(true)
		c.cancel()
		<-c.done
	}
}

func timevalNS(t syscall.Timeval) int64 {
	sec, usec := int64(t.Sec), int64(t.Usec)
	if sec < 0 || usec < 0 || usec >= 1000000 || sec > math.MaxInt64/1000000000 {
		return -1
	}
	n := sec * 1000000000
	if usec > (math.MaxInt64-n)/1000 {
		return -1
	}
	return n + usec*1000
}

func (u usage) MarshalJSON() ([]byte, error) {
	var user, system, rss *int64
	if u.Available {
		user = &u.UserNS
		system = &u.SystemNS
		rss = &u.RSS
	}
	return json.Marshal(struct {
		Available bool   `json:"available"`
		User      *int64 `json:"user_cpu_ns"`
		System    *int64 `json:"system_cpu_ns"`
		RSS       *int64 `json:"peak_rss_bytes"`
		ElapsedNS int64  `json:"launch_to_wait_or_phase_elapsed_ns"`
		RSSScope  string `json:"peak_rss_scope"`
	}{u.Available, user, system, rss, u.ElapsedNS, u.RSSScope})
}
