//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
