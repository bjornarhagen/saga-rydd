package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTopologyStreamingOracle(t *testing.T) {
	for _, shape := range []string{"wide", "deep", "healthy"} {
		t.Run(shape, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "node_modules")
			n := 256
			if shape == "healthy" {
				n = 512
			}
			spec := topology{shape, n}
			if err := generate(context.Background(), root, spec); err != nil {
				t.Fatal(err)
			}
			before := filepath.Join(base, "before.sqlite")
			a, err := inspect(context.Background(), root, before, spec)
			if err != nil {
				t.Fatal(err)
			}
			dirs := spec.levels()
			if shape == "healthy" {
				dirs++
			}
			if a.Files != int64(n) || a.Directories != int64(dirs) || a.Logical <= 0 || a.BodiesSHA256 == "" {
				t.Fatal(a)
			}
			after := filepath.Join(base, "after.sqlite")
			b, err := inspect(context.Background(), root, after, spec)
			if err != nil || a != b {
				t.Fatal(a, b, err)
			}
			if err = sameOracle(context.Background(), before, after); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestTopologyOracleRejectsActualNamespaceChanges(t *testing.T) {
	for _, change := range []string{"body", "extra", "missing", "hardlink", "directory"} {
		t.Run(change, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "node_modules")
			spec := topology{"wide", 256}
			if err := generate(context.Background(), root, spec); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "body":
				if err := os.WriteFile(spec.path(root, 1), []byte("wrong"), 0600); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(root, "extra"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(spec.path(root, 1)); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Remove(spec.path(root, 2)); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(spec.path(root, 1), spec.path(root, 2)); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(filepath.Join(root, "extra"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := inspect(context.Background(), root, filepath.Join(base, "oracle.sqlite"), spec); !errors.Is(err, errOracle) {
				t.Fatal("unexpected namespace accepted", err)
			}
		})
	}
}
func TestTopologySameOracleDetectsDeletedRows(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "node_modules")
	spec := topology{"wide", 256}
	if err := generate(context.Background(), root, spec); err != nil {
		t.Fatal(err)
	}
	a, b := filepath.Join(base, "a.sqlite"), filepath.Join(base, "b.sqlite")
	for _, name := range []string{a, b} {
		if _, err := inspect(context.Background(), root, name, spec); err != nil {
			t.Fatal(err)
		}
	}
	db, err := openOracleForTest(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DELETE FROM observations WHERE ordinal=1"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if !errors.Is(sameOracle(context.Background(), a, b), errOracle) {
		t.Fatal("deleted oracle row accepted")
	}
}
func TestTopologyPilotBoundsBeforeCreation(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "uncreated")
	o := options{Binary: "/trusted/rydd", Output: out, Shape: "wide", Files: 4096, Seconds: 600}
	for _, mutate := range []func(*options){func(o *options) { o.Files = 1000000 }, func(o *options) { o.Shape = "other" }, func(o *options) { o.Seconds = 3600 }, func(o *options) { o.Binary = "relative" }, func(o *options) { o.Files = 65 }} {
		c := o
		mutate(&c)
		if _, err := run(context.Background(), c); !errors.Is(err, errProfile) {
			t.Fatal(err)
		}
		if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refused profile created output", err)
		}
	}
}
func TestTopologyOutputBoundCannotBypassWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &boundedOutput{limit: 8, cancel: cancel}
	if _, err := io.Copy(b, strings.NewReader(strings.Repeat("x", 32))); err != nil {
		t.Fatal(err)
	}
	data, overflow := b.value()
	if !overflow || len(data) > 8 || ctx.Err() == nil {
		t.Fatal(len(data), overflow, ctx.Err())
	}
}
func TestTopologyNativeUsageChecked(t *testing.T) {
	if timevalNS(syscall.Timeval{Sec: 1, Usec: 2}) != 1000002000 || timevalNS(syscall.Timeval{Sec: -1}) != -1 || timevalNS(syscall.Timeval{Usec: 1000000}) != -1 || timevalNS(syscall.Timeval{Sec: math.MaxInt64}) != -1 {
		t.Fatal("invalid native counters accepted")
	}
}
func TestTopologyChildHelper(t *testing.T) {
	mode := os.Getenv("RYDD_TOPOLOGY_TEST_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "overflow":
		os.Stdout.Write([]byte(strings.Repeat("x", 8192)))
	case "wait":
		<-time.After(time.Minute)
	}
	os.Exit(0)
}
func TestTopologyChildBoundAndCancel(t *testing.T) {
	for _, mode := range []string{"overflow", "wait"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			c, err := start(ctx, os.Args[0], []string{"-test.run=^TestTopologyChildHelper$"}, append(os.Environ(), "RYDD_TOPOLOGY_TEST_HELPER="+mode), 64)
			if err != nil {
				t.Fatal(err)
			}
			defer c.cleanup()
			if err = c.join(ctx); !errors.Is(err, errChild) && !errors.Is(err, errChildOutput) {
				t.Fatal("failed child accepted", err)
			}
			if mode == "wait" && !c.forced.Load() {
				t.Fatal("forced cancellation intent omitted")
			}
			if c.usage.ElapsedNS > int64(2*time.Second) {
				t.Fatal("child not bounded", c.usage)
			}
		})
	}
}

func openOracleForTest(path string) (*sql.DB, error) { return sql.Open("sqlite", path) }

func TestTopologyChildCancellationAndReapedBoundary(t *testing.T) {
	t.Run("cancel_owned_waiting_child", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, err := start(ctx, os.Args[0], []string{"-test.run=^TestTopologyChildHelper$"}, append(os.Environ(), "RYDD_TOPOLOGY_TEST_HELPER=wait"), 64)
		if err != nil {
			t.Fatal(err)
		}
		defer c.cleanup()
		cancel()
		until, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if !errors.Is(c.join(until), errChild) {
			t.Fatal("canceled child accepted")
		}
		status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatal("owned child cancellation was not reaped SIGKILL", status)
		}
	})
	t.Run("cancel_after_owned_wait_return", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, err := start(ctx, os.Args[0], []string{"-test.run=^TestTopologyChildHelper$"}, append(os.Environ(), "RYDD_TOPOLOGY_TEST_HELPER=exit"), 64)
		if err != nil {
			t.Fatal(err)
		}
		defer c.cleanup()
		until, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if err = c.join(until); err != nil {
			t.Fatal(err)
		}
		cancel()
		if err = c.cmd.Process.Kill(); !errors.Is(err, os.ErrProcessDone) {
			t.Fatal("reaped child handle was still killable", err)
		}
		if !c.cmd.ProcessState.Exited() || c.cmd.ProcessState.ExitCode() != 0 {
			t.Fatal("original child status changed")
		}
	})
}
func TestTopologyReceiptFailureIsReported(t *testing.T) {
	base := t.TempDir()
	if err := saveJSON(base, map[string]int{"observed": 1}); !errors.Is(err, errReceipt) {
		t.Fatal("receipt publication failure ignored", err)
	}
}

func TestTopologyUnavailableUsageRemainsNull(t *testing.T) {
	b, err := json.Marshal(usage{ElapsedNS: 1})
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Available bool
		User      *int64 `json:"user_cpu_ns"`
		System    *int64 `json:"system_cpu_ns"`
		RSS       *int64 `json:"peak_rss_bytes"`
	}
	if json.Unmarshal(b, &v) != nil || v.Available || v.User != nil || v.System != nil || v.RSS != nil {
		t.Fatal("unknown resource observation became zero", string(b))
	}
}

func TestTopologyUnexpectedDeepChainRefusesBeforeDescent(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "node_modules")
	spec := topology{"wide", 256}
	if err := generate(context.Background(), root, spec); err != nil {
		t.Fatal(err)
	}
	dir := root
	for i := 0; i < 100; i++ {
		dir = filepath.Join(dir, "x")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	v, err := inspect(context.Background(), root, filepath.Join(base, "oracle.sqlite"), spec)
	if !errors.Is(err, errOracle) || v.Directories != 1 {
		t.Fatal("unexpected chain was descended before rejection", v.Directories, err)
	}
}

func TestTopologySamplingExpiryPreservesReceiptFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !samplingEnded(ctx, context.Canceled) || !samplingEnded(ctx, errors.Join(errChild, context.Canceled)) {
		t.Fatal("ordinary expiry refused")
	}
	for _, err := range []error{errReceipt, errors.Join(context.Canceled, errReceipt), errors.Join(context.Canceled, errChildOutput), errors.Join(context.Canceled, errProfile)} {
		if samplingEnded(ctx, err) {
			t.Fatal("stronger failure swallowed by sampling expiry", err)
		}
	}
}

func TestTopologyReadinessRetainsReceiptErrorAtExpiry(t *testing.T) {
	for _, failure := range []error{errReceipt, errChildOutput, errProfile} {
		ctx, cancel := context.WithCancel(context.Background())
		r := runner{}
		c := child{done: make(chan struct{})}
		calls := 0
		err := r.readyWithStatus(ctx, &c, false, func(context.Context) (statusView, error) {
			calls++
			cancel()
			return statusView{}, errors.Join(context.Canceled, failure)
		})
		if !errors.Is(err, failure) || calls != 1 || samplingEnded(ctx, err) {
			t.Fatal("readiness swallowed stronger status failure", err, calls)
		}
		cancel()
	}
}
