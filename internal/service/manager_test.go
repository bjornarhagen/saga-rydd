//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func generatedServiceSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rs-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "bus")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return socket
}

func TestServiceManagerTypedLocalUnitPathAndLiteralAddress(t *testing.T) {
	socket := generatedServiceSocket(t)
	paths := []string{"/generated home/config $%\"'\\Ω/systemd/user", "/usr/lib/systemd/user"}
	data, err := json.Marshal(map[string]any{"type": "v", "data": []any{map[string]any{"type": "as", "data": paths}}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	run := func(ctx context.Context, args []string) ([]byte, error) {
		calls++
		if !reflect.DeepEqual(args[1:], []string{"--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.DBus.Properties", "Get", "ss", "org.freedesktop.systemd1.Manager", "UnitPath"}) || args[0] != "--address=unix:path="+socket {
			t.Fatal("getter gained activation or default/remote bus routing", args)
		}
		return data, nil
	}
	observed, err := observeUnitPathWithRunner(context.Background(), socket, run)
	if err != nil || calls != 1 || observed.Status != "observed" || observed.ObservedAt == nil || !reflect.DeepEqual(observed.UnitPath, paths) || observed.DirectoryInUnitPath != nil {
		t.Fatal("typed paths changed or visibility fabricated", observed, calls, err)
	}
	if got := dbusAddressPath("/tmp/a\\ b%,=;Ω"); got != "/tmp/a%5C%20b%25%2C%3D%3B%CE%A9" {
		t.Fatal("literal socket address gained another transport or property", got)
	}
}

func TestServiceManagerStrictRepliesAndSocketGuards(t *testing.T) {
	for name, reply := range map[string]string{
		"missing data":    `{"type":"v"}`,
		"null array":      `{"type":"v","data":[{"type":"as","data":null}]}`,
		"wrong signature": `{"type":"as","data":[]}`,
		"extra variants":  `{"type":"v","data":[{"type":"as","data":[]},{"type":"as","data":[]}]}`,
		"unknown key":     `{"type":"v","data":[{"type":"as","data":[],"extra":1}]}`,
		"duplicate key":   `{"type":"v","data":[{"type":"as","type":"as","data":[]}]}`,
		"trailing":        `{"type":"v","data":[{"type":"as","data":[]}]} {}`,
		"relative path":   `{"type":"v","data":[{"type":"as","data":["relative"]}]}`,
		"duplicate path":  `{"type":"v","data":[{"type":"as","data":["/same","/same"]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeManagerUnitPath([]byte(reply)); !errors.Is(err, ErrManagerProtocol) {
				t.Fatal("unsupported reply accepted", err)
			}
		})
	}
	if paths, err := decodeManagerUnitPath([]byte(`{"type":"v","data":[{"type":"as","data":[]}]}`)); err != nil || paths == nil || len(paths) != 0 {
		t.Fatal("known empty manager array refused", paths, err)
	}
	if _, err := decodeManagerUnitPath([]byte(strings.Repeat(" ", 65537))); !errors.Is(err, ErrLifecycleBounds) {
		t.Fatal("output bound bypassed", err)
	}
	for _, kind := range []string{"missing", "regular", "symlink", "changed", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			socket := generatedServiceSocket(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			switch kind {
			case "missing":
				socket += "-missing"
			case "regular":
				socket = filepath.Join(filepath.Dir(socket), "regular")
				if err := os.WriteFile(socket, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := socket + "-link"
				if err := os.Symlink(socket, link); err != nil {
					t.Fatal(err)
				}
				socket = link
			case "canceled":
				cancel()
			}
			run := func(context.Context, []string) ([]byte, error) {
				calls++
				if kind == "changed" {
					if err := os.Rename(socket, socket+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(socket, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				return []byte(`{"type":"v","data":[{"type":"as","data":[]}]}`), nil
			}
			observed, err := observeUnitPathWithRunner(ctx, socket, run)
			if err == nil || observed.Status == "observed" || observed.ObservedAt != nil {
				t.Fatal("unsafe socket yielded observation", observed, err)
			}
			if kind != "changed" && calls != 0 {
				t.Fatal("unsafe socket reached child")
			}
			if kind == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost", err)
			}
		})
	}
}

func TestServiceManagerProcessFixture(t *testing.T) {
	mode := os.Getenv("RYDD_SERVICE_PROCESS_FIXTURE")
	if mode == "" {
		return
	}
	switch mode {
	case "reply":
		fmt.Print(`{"type":"v","data":[{"type":"as","data":["/literal path/$%\"'\\Ω/systemd/user"]}]}`)
		os.Exit(0)
	case "overflow":
		for range 128 {
			fmt.Print(strings.Repeat("x", 1024))
		}
		os.Exit(0)
	case "stderr overflow":
		for range 32 {
			fmt.Fprint(os.Stderr, strings.Repeat("x", 1024))
		}
		os.Exit(0)
	case "sleep":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "ready-wait":
		if err := os.WriteFile(os.Getenv("RYDD_SERVICE_PROCESS_READY"), []byte("ready"), 0600); err != nil {
			os.Exit(4)
		}
		time.Sleep(5 * time.Second)
		os.Exit(0)
	default:
		os.Exit(3)
	}
}

func TestServiceManagerBoundedGeneratedProcesses(t *testing.T) {
	for _, mode := range []string{"reply", "overflow", "stderr overflow", "sleep"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if mode != "sleep" {
				ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
			}
			started := time.Now()
			data, err := runManagerGetterProcess(ctx, os.Args[0], []string{"-test.run=^TestServiceManagerProcessFixture$"}, append(os.Environ(), "RYDD_SERVICE_PROCESS_FIXTURE="+mode))
			if mode == "reply" {
				if err != nil {
					t.Fatal(err)
				}
				paths, err := decodeManagerUnitPath(data)
				if err != nil || len(paths) != 1 || paths[0] != "/literal path/$%\"'\\Ω/systemd/user" {
					t.Fatal("generated process changed typed path", paths, err)
				}
			} else {
				if err == nil || len(data) != 0 {
					t.Fatal("process failure returned positive output", err, string(data))
				}
				if mode == "sleep" {
					if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
						t.Fatal("deadline did not kill and reap child", err, time.Since(started))
					}
				} else if !errors.Is(err, ErrLifecycleBounds) {
					t.Fatal("output limit lost", err)
				}
			}
		})
	}
}
