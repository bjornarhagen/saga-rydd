package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const sampleCrashHelperEnv = "SAGA_SAMPLE_STORE_CRASH_HELPER"

type sampleCrashConfig struct {
	Mode     string `json:"mode"`
	Base     string `json:"base"`
	StateDir string `json:"state_dir"`
	Root     string `json:"root"`
	BatchID  string `json:"batch_id"`
}

type sampleCrashOutput struct {
	mu       sync.Mutex
	body     [65536]byte
	used     int
	overflow bool
}

func (b *sampleCrashOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := copy(b.body[b.used:], p)
	b.used += n
	if n != len(p) {
		b.overflow = true
	}
	return len(p), nil
}

// Only the four explicit generated helper invocations exercise this parent.
// A normal package run needs no helper environment and returns without a skip.
func TestSampleStoreCrashHelper(t *testing.T) {
	path := os.Getenv(sampleCrashHelperEnv)
	if path == "" {
		return
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal("invalid generated helper configuration", err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var before, after, named unix.Stat_t
	if err = unix.Fstat(fd, &before); err != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&0777 != 0600 || before.Uid != uint32(os.Geteuid()) || before.Nlink != 1 || before.Size < 0 || before.Size > 16384 {
		t.Fatal("invalid generated helper configuration", err)
	}
	body, err := io.ReadAll(io.LimitReader(f, 16385))
	if err != nil || int64(len(body)) != before.Size {
		t.Fatal("incomplete generated helper configuration", err)
	}
	if err = unix.Fstat(fd, &after); err != nil {
		t.Fatal(err)
	}
	if err = unix.Lstat(path, &named); err != nil {
		t.Fatal(err)
	}
	stamp := newHashLiveStamp(before)
	if !stamp.matches(after) || !stamp.matches(named) || after.Uid != before.Uid || named.Uid != before.Uid || after.Gid != before.Gid || named.Gid != before.Gid {
		t.Fatal("generated helper configuration identity changed")
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	var c sampleCrashConfig
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, body) {
		t.Fatal("noncanonical generated helper configuration", err)
	}
	if !sampleHex(c.BatchID) {
		t.Fatal("invalid generated batch identity")
	}
	s, err := OpenSampleWriter(context.Background(), c.Base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.now = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	source, err := state.OpenReader(context.Background(), c.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	scanner, err := New([]string{c.Root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	h := sampleStoreHooks{}
	switch c.Mode {
	case "before_reserve_commit":
		h.beforeReserveCommit = func() { os.Exit(81) }
	case "after_reserve":
		h.afterReserve = func() { os.Exit(82) }
	case "after_actual_read":
		h.sample.afterRead = func(n int) {
			if n != 32768 {
				t.Fatal("helper did not complete its actual first Pread", n)
			}
			os.Exit(83)
		}
	case "after_settle_commit":
		h.afterSettleCommit = func() { os.Exit(84) }
	default:
		t.Fatal("unsupported generated crash point")
	}
	result, err := s.runSamples(context.Background(), c.BatchID, source, scanner, h)
	t.Fatal("generated crash hook did not exit", result, err)
}

func TestSampleStoreCrashReserveReadAndSettle(t *testing.T) {
	for _, mode := range []string{"before_reserve_commit", "after_reserve", "after_actual_read", "after_settle_commit"} {
		t.Run(mode, func(t *testing.T) {
			f := sampleStoreFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.source.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "generated-helper.json")
			body, err := json.Marshal(sampleCrashConfig{mode, f.base, f.stateDir, f.root, f.batch.BatchID})
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSampleStoreCrashHelper$", "-test.timeout=15s")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, sampleCrashHelperEnv+"=") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, sampleCrashHelperEnv+"="+path)
			var captured sampleCrashOutput
			cmd.Stdout = &captured
			cmd.Stderr = &captured
			err = cmd.Run()
			output := captured.body[:captured.used]
			if ctx.Err() != nil || captured.overflow {
				t.Fatal("bounded crash helper did not finish", ctx.Err(), captured.overflow)
			}
			want := map[string]int{"before_reserve_commit": 81, "after_reserve": 82, "after_actual_read": 83, "after_settle_commit": 84}[mode]
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != want {
				t.Fatalf("generated crash %s exit=%v, expected%d; %s", mode, err, want, output)
			}
			// These expected nonzero helper exits are crash facts, not evidence
			// that the sampler or controller completed successfully.
			s, err := OpenSampleWriter(context.Background(), f.base)
			if err != nil {
				t.Fatal("process loss did not release private writer lock", err)
			}
			defer s.Close()
			s.now = func() time.Time { t.Fatal("saved recovery sampled clock"); return time.Time{} }
			before := sampleSaved(t, s)
			if mode == "before_reserve_commit" {
				if before.Attempt != nil || before.Budget.TotalReservedBytes != 0 {
					t.Fatal("uncommitted reserve survived", before)
				}
			} else if before.Budget.TotalReservedBytes != 98304 {
				t.Fatal("durable grant disappeared", before)
			}
			after, err := s.Recover(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "before_reserve_commit":
				if after.Attempt != nil {
					t.Fatal(after)
				}
			case "after_reserve", "after_actual_read":
				if after.Attempt.Status != "interrupted_unknown" || after.Attempt.RequestedBytes != nil || after.Attempt.ReadBytes != nil || after.Attempt.ElapsedNS != nil || after.Budget.TotalUnknownReservedBytes != 98304 || after.Budget.TotalRequestedBytes != 0 || after.Budget.TotalOutstandingReservedBytes != 0 {
					t.Fatal("unknown usage was invented or refunded", after)
				}
			case "after_settle_commit":
				if after.Attempt.Status != "settled" || *after.Attempt.RequestedBytes != 98304 || *after.Attempt.ReadBytes != 98304 || after.Budget.TotalUnknownReservedBytes != 0 {
					t.Fatal("known settlement became unknown", after)
				}
			}
			again, err := s.Recover(context.Background())
			if err != nil || !reflect.DeepEqual(after, again) {
				t.Fatal("second recovery changed charge", again, err)
			}
			if mode != "before_reserve_commit" {
				// No source/scanner is available to recovery; any saved batch
				// report must contain historical evidence/accounting only.
				if after.Status != "settled" && after.Status != "interrupted_unknown" {
					t.Fatal(fmt.Sprint(after.Status))
				}
			}
		})
	}
}
