//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestServiceClientCompletedCancellationRetainsProcessOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	completedCtx, completedCancel := context.WithCancel(ctx)
	defer completedCancel()
	args := []string{"-test.run=^TestServiceManagerProcessFixture$"}
	completed := serviceClientCommand(completedCtx, os.Args[0], args, append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE=reply"))
	var output bytes.Buffer
	completed.Stdout = &output
	if err := completed.Run(); err != nil || completed.ProcessState == nil || !completed.ProcessState.Success() || output.Len() == 0 {
		t.Fatal("generated direct client was not reaped", err)
	}
	reaped := completed.ProcessState
	ready := filepath.Join(t.TempDir(), "ready")
	runningCtx, runningCancel := context.WithCancel(ctx)
	running := serviceClientCommand(runningCtx, os.Args[0], args, append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE=ready-wait", "RYDD_SERVICE_PROCESS_READY="+ready))
	if err := running.Start(); err != nil {
		runningCancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = running.Wait(); close(done) }()
	t.Cleanup(func() {
		runningCancel()
		<-done
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		body, err := os.ReadFile(ready)
		if err == nil && string(body) == "ready" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("generated direct client did not reach its running marker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// This uses the Process object's terminal state, never a raw numeric PID
	// or group signal after reap. The other owned client must remain live.
	if err := completed.Cancel(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatal("terminal client cancellation bypassed Process state", err)
	}
	completedCancel()
	if completed.ProcessState != reaped || !reaped.Success() {
		t.Fatal("completed ProcessState changed")
	}
	select {
	case <-done:
		t.Fatal("completed client cancellation ended another owned client", waitErr)
	default:
	}
	runningCancel()
	<-done
	status, ok := running.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("running direct client was not killed and reaped", waitErr, status)
	}
}

func TestServiceGetterAndRuntimeCancellationAfterRunningMarker(t *testing.T) {
	for _, kind := range []string{"getter", "runtime"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ready := filepath.Join(t.TempDir(), "ready")
			env := append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE=ready-wait", "RYDD_SERVICE_PROCESS_READY="+ready)
			type outcome struct {
				reply runtimeReply
				err   error
			}
			done := make(chan struct{})
			var got outcome
			go func() {
				if kind == "runtime" {
					reply, err := runRuntimeProcessWithEnv(ctx, os.Args[0], []string{"-test.run=^TestServiceManagerProcessFixture$"}, env)
					got = outcome{reply, err}
				} else {
					data, err := runManagerGetterProcess(ctx, os.Args[0], []string{"-test.run=^TestServiceManagerProcessFixture$"}, env)
					got = outcome{runtimeReply{data: data}, err}
				}
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			deadline := time.Now().Add(3 * time.Second)
			for {
				body, err := os.ReadFile(ready)
				if err == nil && string(body) == "ready" {
					break
				}
				if !time.Now().Before(deadline) {
					t.Fatal("generated selected client did not reach its running marker")
				}
				time.Sleep(5 * time.Millisecond)
			}
			canceledAt := time.Now()
			cancel()
			<-done
			if !errors.Is(got.err, context.Canceled) || len(got.reply.data) != 0 || kind == "runtime" && !got.reply.started || time.Since(canceledAt) > time.Second {
				t.Fatal("canceled direct client lost scope, start evidence or bounded output", got.err, got.reply.started)
			}
		})
	}
}

func TestServiceRuntimeBoundedProcessStartEvidence(t *testing.T) {
	for _, mode := range []string{"reply", "overflow", "stderr overflow", "sleep", "invalid-mode"} {
		t.Run(mode, func(t *testing.T) {
			deadline := 3 * time.Second
			if mode == "sleep" {
				deadline = 150 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			started := time.Now()
			reply, err := runRuntimeProcessWithEnv(ctx, os.Args[0], []string{"-test.run=^TestServiceManagerProcessFixture$"}, append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE="+mode))
			if !reply.started {
				t.Fatal("started client was reported as never dispatched", err)
			}
			if mode == "reply" {
				if err != nil || len(reply.data) == 0 {
					t.Fatal("generated client success lost", err)
				}
				return
			}
			if err == nil || len(reply.data) != 0 {
				t.Fatal("failed client published a positive body", err)
			}
			switch mode {
			case "sleep":
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
					t.Fatal("client was not killed/reaped within original deadline", err, time.Since(started))
				}
			case "overflow", "stderr overflow":
				if !errors.Is(err, ErrLifecycleBounds) {
					t.Fatal("output bound cause lost", err)
				}
			default:
				if !errors.Is(err, ErrManagerUnavailable) {
					t.Fatal("opaque failure cause lost", err)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if reply, err := runRuntimeProcess(ctx, os.Args[0], nil); !errors.Is(err, context.Canceled) || reply.started || len(reply.data) != 0 {
		t.Fatal("pre-start cancellation claimed a dispatch", reply, err)
	}
	if reply, err := runRuntimeProcess(context.Background(), filepath.Join(t.TempDir(), "missing client"), nil); !errors.Is(err, ErrManagerUnavailable) || reply.started {
		t.Fatal("missing executable claimed a dispatch", reply, err)
	}
}

func TestServiceRuntimeStrictTypedProtocol(t *testing.T) {
	for _, data := range []string{
		`{"type":"o","data":null}`,
		`{"type":"o","data":[null]}`,
		`{"type":"o","data":["/one","/two"]}`,
		`{"type":"o","type":"o","data":["/one"]}`,
		`{"type":"o","data":["/one"],"extra":1}`,
		`{"type":"o","data":["/one"]} {}`,
	} {
		if _, err := decodeRuntimeObject([]byte(data)); !errors.Is(err, ErrManagerProtocol) {
			t.Fatal("unsupported typed response accepted", data, err)
		}
	}
	for _, data := range []string{
		`{"type":"a{sv}","data":[null]}`,
		`{"type":"a{sv}","data":[{"Id":{"type":"s"}}]}`,
		`{"type":"a{sv}","data":[{"Id":{"type":"s","data":"a","data":"b"}}]}`,
		`{"type":"a{sv}","data":[{"Id":{"type":"s","data":"a","extra":1}}]}`,
	} {
		if _, err := decodeRuntimeProperties([]byte(data)); !errors.Is(err, ErrManagerProtocol) {
			t.Fatal("unsupported property dictionary accepted", data, err)
		}
	}
	for _, path := range []string{"/org/freedesktop/systemd1/job/0", "/org/freedesktop/systemd1/job/01", "/org/freedesktop/systemd1/job/4294967296", "/foreign/job/1", "/org/freedesktop/systemd1/job/1/extra"} {
		if runtimeJobPath(path) {
			t.Fatal("foreign/noncanonical job reference accepted", path)
		}
	}
	if !runtimeUniqueName(":1.12345") || runtimeUniqueName("org.freedesktop.systemd1") || runtimeUniqueName(":1.2;unix:path=/other") {
		t.Fatal("unique owner validation accepted routing data")
	}
}

func TestServiceRuntimeUnknownMacClientOutcome(t *testing.T) {
	for _, kind := range []string{"failed after start", "output overflow", "canceled after start", "not started"} {
		t.Run(kind, func(t *testing.T) {
			spec := runtimeFixture(t, "darwin")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := runtimeTestHooks(func(context.Context, string, []string) (runtimeReply, error) {
				switch kind {
				case "failed after start":
					return runtimeReply{started: true}, ErrManagerUnavailable
				case "output overflow":
					return runtimeReply{started: true, data: []byte(strings.Repeat("x", 65537))}, nil
				case "canceled after start":
					cancel()
					return runtimeReply{started: true}, context.Canceled
				default:
					return runtimeReply{}, ErrManagerUnavailable
				}
			})
			r, err := requestRuntime(ctx, spec, "stop", hooks)
			if err == nil || r.RequestAccepted != nil || r.ReplyObservedAt != nil || r.Stopped != nil || r.Running != nil {
				t.Fatal("unknown stop became accepted/stopped", r, err)
			}
			if kind == "not started" {
				if r.RequestAttempted || errors.Is(err, ErrRuntimeOutcome) {
					t.Fatal("failed process start claimed manager action", r, err)
				}
			} else if !r.RequestAttempted || r.RequestStatus != "unknown" || !errors.Is(err, ErrRuntimeOutcome) {
				t.Fatal("lost attempted unknown command", r, err)
			}
			if kind == "canceled after start" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost", err)
			}
		})
	}
}
