package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestPrioritySettingPartialAndUnknownReplies(t *testing.T) {
	for _, mode := range []string{"accepted", "failed", "before_unavailable", "readback_unavailable", "preserve", "cancel_before", "cancel_after_before", "cancel_after_request"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel_before" {
				cancel()
			}
			gets, sets := 0, 0
			setting := requestPrioritySetting(ctx, "fixture", 10, func() (int, error) {
				gets++
				if mode == "before_unavailable" || (mode == "readback_unavailable" && gets == 2) {
					return 0, errors.New("untrusted native detail")
				}
				if mode == "preserve" {
					return 19, nil
				}
				if gets == 1 {
					if mode == "cancel_after_before" {
						cancel()
					}
					return 0, nil
				}
				if mode == "failed" {
					return 0, nil
				}
				return 10, nil
			}, func(value int) error {
				sets++
				if value != 10 {
					t.Fatal(value)
				}
				if mode == "cancel_after_request" {
					cancel()
				}
				if mode == "failed" {
					return errors.New("untrusted native detail")
				}
				return nil
			}, func(value int) bool { return value >= 10 })
			data, err := json.Marshal(setting)
			if err != nil || strings.Contains(string(data), "untrusted") {
				t.Fatal(setting, err)
			}
			switch mode {
			case "accepted", "failed":
				want := mode == "accepted"
				if gets != 2 || sets != 1 || !setting.Attempted || setting.Accepted == nil || *setting.Accepted != want || setting.Observed == nil {
					t.Fatal(setting, gets, sets)
				}
			case "before_unavailable", "cancel_before":
				if sets != 0 || setting.Attempted || setting.Accepted != nil || setting.Before != nil || setting.Observed != nil {
					t.Fatal(setting, gets, sets)
				}
			case "preserve":
				if gets != 1 || sets != 0 || setting.Attempted || setting.Accepted != nil || setting.Observed == nil || *setting.Observed != 19 {
					t.Fatal("conservative existing setting was raised", setting)
				}
			case "cancel_after_before":
				if gets != 1 || sets != 0 || setting.Attempted || setting.Accepted != nil || setting.Before == nil {
					t.Fatal(setting, gets, sets)
				}
			case "readback_unavailable", "cancel_after_request":
				if sets != 1 || !setting.Attempted || setting.Accepted == nil || !*setting.Accepted || setting.Observed != nil || setting.Before == nil {
					t.Fatal("acceptance became observed scheduling", setting, gets, sets)
				}
			}
		})
	}
}

func TestPriorityChunkOrderingCancellationAndPanic(t *testing.T) {
	for _, mode := range []string{"normal", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan outcome, 1)
			var requested, handled atomic.Int32
			stop := startChunkWithPriority(ctx, time.Second, state.Job{}, func(context.Context, state.Job) (Result, error) {
				if requested.Load() != 1 {
					return Result{}, errors.New("handler preceded priority bookkeeping")
				}
				handled.Add(1)
				if mode == "panic" {
					panic("fixture")
				}
				return Result{Done: true}, nil
			}, done, func(context.Context) ThreadPriorityObservation {
				requested.Add(1)
				if mode == "cancel" {
					cancel()
				}
				return newThreadPriorityObservation("fixture")
			})
			defer stop()
			select {
			case result := <-done:
				if requested.Load() != 1 || (mode == "cancel" && (handled.Load() != 0 || !errors.Is(result.err, context.Canceled))) || (mode == "normal" && (!result.result.Done || result.err != nil)) || (mode == "panic" && (!result.panicked || result.err == nil)) {
					t.Fatal(mode, result, requested.Load(), handled.Load())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("priority chunk did not finish")
			}
		})
	}
}

func TestPriorityWorkerControlsAndLiveHistoricalObservation(t *testing.T) {
	dir, cfg := fixture(t)
	cfg.Roots = []string{t.TempDir()}
	entered := make(chan struct{}, 1)
	var sourceCalls atomic.Int32
	options := Options{ExperimentalScan: true, WorkDuration: time.Second, Interval: time.Second}
	options.priorityRequest = func(context.Context) ThreadPriorityObservation {
		r := newThreadPriorityObservation("fixture")
		accepted := false
		r.CPU = &PrioritySetting{Policy: "nice_at_least_10", Target: 10, Attempted: true, Accepted: &accepted, Reason: "fixture_request_failed"}
		return r
	}
	options.scannerNew = func(ctx context.Context, roots, excludes, private []string, permit inventory.APIPermit, _ ...inventory.Option) (*inventory.Scanner, error) {
		sourceCalls.Add(1)
		if err := permit(ctx, inventory.APIStat); err != nil {
			return nil, err
		}
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	initial, done := start(t, dir, cfg, options)
	if initial.Priority != nil {
		t.Fatal("priority request was invented before dispatch", initial)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("failed priority request prevented conservative source work")
	}
	current := control(t, dir, "status")
	if current.Priority == nil || current.Priority.CPU == nil || current.Priority.CPU.Accepted == nil || *current.Priority.CPU.Accepted || current.Priority.CPU.Observed != nil || current.Priority.WholeProcessVerified || current.Priority.PhysicalIOVerified || current.Priority.PhysicalPowerVerified {
		t.Fatal(current)
	}
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	current = control(t, dir, "status")
	if sourceCalls.Load() != 1 || current.Metadata.TotalCharges.OutstandingReserved != 0 || current.Priority == nil {
		t.Fatal(current, sourceCalls.Load())
	}
	control(t, dir, "stop")
	waitExit(t, done)

	idleDir, idleCfg := fixture(t)
	var idleRequests atomic.Int32
	initial, done = start(t, idleDir, idleCfg, Options{priorityRequest: func(context.Context) ThreadPriorityObservation {
		idleRequests.Add(1)
		return newThreadPriorityObservation("fixture")
	}})
	if initial.Priority != nil || control(t, idleDir, "status").Priority != nil || idleRequests.Load() != 0 {
		t.Fatal("idle mode requested native scheduling")
	}
	control(t, idleDir, "stop")
	waitExit(t, done)
}

// Native requests run only in this disposable child, never on the test runner's
// threads. The locked request thread exits without unlocking even on panic.
func TestPriorityNativeChild(t *testing.T) {
	if os.Getenv("RYDD_TEST_PRIORITY_CHILD") != "1" {
		return
	}
	done := make(chan ThreadPriorityObservation, 1)
	go func() {
		runtime.LockOSThread()
		r := requestThreadPriority(context.Background())
		done <- r
	}()
	r := <-done
	if r.Contract != ThreadPriorityContract || r.Scope != "experimental_source_handler_os_thread" || r.WholeProcessVerified || r.EffectiveSchedulingVerified || r.PhysicalIOVerified || r.PhysicalPowerVerified || !r.NewThreadsMayInherit {
		t.Fatal(r)
	}
	if runtime.GOOS == "linux" {
		if r.CPU == nil || r.CPU.Observed == nil || *r.CPU.Observed < 10 {
			t.Fatal("native low nice setting was not observed", r)
		}
		if r.IO == nil || r.IO.Observed == nil || *r.IO.Observed>>13 != 3 {
			t.Fatal("native I/O idle setting was not observed", r)
		}
	} else if r.Background == nil || r.Background.Observed == nil || *r.Background.Observed != 1 {
		t.Fatal("native thread background setting was not observed", r)
	}
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		t.Fatal(err)
	}
}

func TestPriorityNativeDisposableProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPriorityNativeChild$")
	cmd.Env = append(os.Environ(), "RYDD_TEST_PRIORITY_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native disposable scheduling fixture: %v\n%s", err, output)
	}
	var r ThreadPriorityObservation
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	if err := decoder.Decode(&r); err != nil || r.Platform != runtime.GOOS || r.CheckedAt.IsZero() || r.ThreadDisposition != "locked_until_handler_goroutine_exit" {
		t.Fatal(r, err, string(output))
	}
}
