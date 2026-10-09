package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestControlIOErrorObservedContextAndNativeIdentity(t *testing.T) {
	for _, kind := range []string{"live", "canceled", "expired"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var expected error
			switch kind {
			case "canceled":
				ended, cancel := context.WithCancel(ctx)
				cancel()
				ctx, expected = ended, context.Canceled
			case "expired":
				ended, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				<-ended.Done()
				ctx, expected = ended, context.DeadlineExceeded
			}
			native := &net.OpError{Op: "read", Net: "unix", Err: net.ErrClosed}
			got := controlIOError(ctx, native)
			if !errors.Is(got, net.ErrClosed) {
				t.Fatal("native closed error was lost", got)
			}
			var original *net.OpError
			if !errors.As(got, &original) || original != native {
				t.Fatal("native error identity was replaced", got)
			}
			var network net.Error
			if !errors.As(got, &network) || network.Timeout() != (kind == "expired") {
				t.Fatal("observed deadline did not precede the closed error", got)
			}
			if expected == nil {
				if got != native || errors.Is(got, context.Canceled) || errors.Is(got, context.DeadlineExceeded) {
					t.Fatal("live context invented a cancellation or timeout", got)
				}
			} else if !errors.Is(got, expected) {
				t.Fatal("observed context cause was lost", got)
			}
		})
	}
}

func controlCancellationSocket(t *testing.T) (string, *net.UnixListener) {
	t.Helper()
	// A short generated runtime path works within Darwin's Unix socket limit.
	runtimeDir, err := os.MkdirTemp("/tmp", "rydd-control-cause-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(runtimeDir); err != nil {
			t.Error(err)
		}
	})
	t.Setenv("RYDD_RUNTIME_DIR", runtimeDir)
	dir := t.TempDir()
	path, err := Endpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return dir, listener
}

func TestSendEndedContextBeforeDial(t *testing.T) {
	for _, kind := range []string{"canceled", "expired"} {
		t.Run(kind, func(t *testing.T) {
			dir, _ := controlCancellationSocket(t)
			var ctx context.Context
			var cancel context.CancelFunc
			var expected error
			if kind == "canceled" {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
				expected = context.Canceled
			} else {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				<-ctx.Done()
				expected = context.DeadlineExceeded
			}
			status, err := Send(ctx, dir, "status")
			var native *net.OpError
			if !errors.Is(err, expected) || !errors.As(err, &native) || !reflect.DeepEqual(status, Snapshot{}) {
				t.Fatal("failed dial lost context/native evidence or returned status", status, err)
			}
		})
	}
}

func TestSendParentCancellationDeadlineAndLivePeerClose(t *testing.T) {
	for _, kind := range []string{"canceled", "deadline", "live_peer_close", "success"} {
		t.Run(kind, func(t *testing.T) {
			dir, listener := controlCancellationSocket(t)
			ctx, cancel := context.WithCancel(context.Background())
			if kind == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			}
			var connMu sync.Mutex
			var accepted *net.UnixConn
			serverDone := make(chan struct{})
			requestRead := make(chan struct{})
			clientDone := make(chan struct{})
			var serverErr, clientErr error
			var status Snapshot
			// Register cleanup before either goroutine can own a connection.
			t.Cleanup(func() {
				cancel()
				_ = listener.Close()
				connMu.Lock()
				if accepted != nil {
					_ = accepted.Close()
				}
				connMu.Unlock()
				for _, child := range []struct {
					name string
					done <-chan struct{}
				}{{"client", clientDone}, {"server", serverDone}} {
					select {
					case <-child.done:
					case <-time.After(3 * time.Second):
						t.Errorf("generated %s did not finish", child.name)
					}
				}
			})
			go func() {
				defer close(serverDone)
				conn, err := listener.AcceptUnix()
				if err != nil {
					serverErr = err
					return
				}
				connMu.Lock()
				accepted = conn
				connMu.Unlock()
				defer conn.Close()
				if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					serverErr = err
					return
				}
				var request Request
				if err = json.NewDecoder(io.LimitReader(conn, 4096)).Decode(&request); err != nil {
					serverErr = err
					return
				}
				if request.Version != protocolVersion || request.Command != "status" {
					serverErr = errors.New("generated server received a different control request")
					return
				}
				close(requestRead)
				if kind == "live_peer_close" {
					return
				}
				if kind == "success" {
					serverErr = json.NewEncoder(conn).Encode(Response{Version: protocolVersion, OK: true, Status: Snapshot{Paused: true, WaitReason: "paused"}})
					return
				}
				var byteAfterRequest [1]byte
				_, err = conn.Read(byteAfterRequest[:])
				if err != io.EOF {
					serverErr = errors.New("client did not close its generated connection after cancellation")
				}
			}()
			go func() {
				defer close(clientDone)
				status, clientErr = Send(ctx, dir, "status")
			}()
			select {
			case <-requestRead:
			case <-serverDone:
				select {
				case <-requestRead:
				default:
					t.Fatal("generated server failed before the request", serverErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("generated server did not receive the control request")
			}
			if kind == "canceled" {
				cancel()
			}
			select {
			case <-clientDone:
			case <-time.After(3 * time.Second):
				t.Fatal("control client did not finish within its existing bound")
			}
			select {
			case <-serverDone:
			case <-time.After(3 * time.Second):
				t.Fatal("generated server did not finish")
			}
			if serverErr != nil {
				t.Fatal(serverErr)
			}
			if kind == "success" {
				if clientErr != nil || !reflect.DeepEqual(status, Snapshot{Paused: true, WaitReason: "paused"}) {
					t.Fatal("successful response changed", status, clientErr)
				}
				return
			}
			if !reflect.DeepEqual(status, Snapshot{}) || clientErr == nil || !strings.Contains(clientErr.Error(), "control response unavailable (retrying pause/resume/stop is safe)") {
				t.Fatal("failed response lost guidance or returned status", status, clientErr)
			}
			switch kind {
			case "canceled":
				if !errors.Is(clientErr, context.Canceled) || !errors.Is(clientErr, net.ErrClosed) {
					t.Fatal("parent cancellation or native close cause was lost", clientErr)
				}
			case "deadline":
				<-ctx.Done()
				var network net.Error
				// SetDeadline can report its native timeout before the context
				// timer fires. Only an already-observed ctx.Err is joined.
				if ctx.Err() != context.DeadlineExceeded || !errors.As(clientErr, &network) || !network.Timeout() {
					t.Fatal("actual parent deadline lost its timeout evidence", clientErr)
				}
			case "live_peer_close":
				if ctx.Err() != nil || errors.Is(clientErr, context.Canceled) || errors.Is(clientErr, context.DeadlineExceeded) || !errors.Is(clientErr, io.EOF) {
					t.Fatal("live peer close invented context failure", clientErr)
				}
				var network net.Error
				if errors.As(clientErr, &network) && network.Timeout() {
					t.Fatal("live peer close became a timeout", clientErr)
				}
			}
		})
	}
}
