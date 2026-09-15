package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"golang.org/x/sys/unix"
)

const protocolVersion = 1
const controlTimeout = 2 * time.Second

var ErrNotRunning = errors.New("worker is not running; start rydd daemon")

type Request struct {
	Version int    `json:"version"`
	Command string `json:"command"`
}
type Response struct {
	Version int      `json:"version"`
	OK      bool     `json:"ok"`
	Error   string   `json:"error,omitempty"`
	Status  Snapshot `json:"status"`
}
type controlCall struct {
	request Request
	ctx     context.Context
	reply   chan Response
}

// Endpoint avoids macOS's short Unix socket path limit without moving persistent
// state. Canonical paths make aliases point to the same endpoint. No files are
// created by this function or by client commands.
func Endpoint(stateDir string) (string, error) {
	canonical, err := filepath.EvalSymlinks(stateDir)
	if err != nil {
		return "", err
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return "", err
	}
	base := os.Getenv("RYDD_RUNTIME_DIR")
	if base == "" {
		base = "/tmp"
	}
	if !filepath.IsAbs(base) {
		return "", errors.New("RYDD_RUNTIME_DIR must be absolute")
	}
	hash := sha256.Sum256([]byte(canonical))
	path := filepath.Join(base, fmt.Sprintf("rydd-%d-%x", os.Geteuid(), hash[:16]), "control.sock")
	if len(path) > 100 {
		return "", errors.New("control socket path too long; use a shorter RYDD_RUNTIME_DIR")
	}
	return path, nil
}

func sameUser(conn *net.UnixConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var peerErr error
	if err := raw.Control(func(fd uintptr) {
		uid, e := peerUID(int(fd))
		peerErr = e
		if e == nil && uid != uint32(os.Geteuid()) {
			peerErr = errors.New("control peer belongs to another user")
		}
	}); err != nil {
		return err
	}
	return peerErr
}

func checkSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0077 != 0 || st.Uid != uint32(os.Geteuid()) {
		return errors.New("control endpoint is not a private socket owned by this user")
	}
	return nil
}

func Send(ctx context.Context, dir, command string) (Snapshot, error) {
	var result Snapshot
	path, err := Endpoint(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, ErrNotRunning
		}
		return result, err
	}
	if err := localfs.CheckOwnedDir(filepath.Dir(path)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, ErrNotRunning
		}
		return result, err
	}
	if err := checkSocket(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, ErrNotRunning
		}
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, controlTimeout)
	defer cancel()
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		if errors.Is(err, unix.ECONNREFUSED) || errors.Is(err, os.ErrNotExist) {
			return result, ErrNotRunning
		}
		return result, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return result, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := sameUser(conn.(*net.UnixConn)); err != nil {
		return result, err
	}
	if err := json.NewEncoder(conn).Encode(Request{protocolVersion, command}); err != nil {
		return result, err
	}
	var response Response
	if err := json.NewDecoder(io.LimitReader(conn, 8192)).Decode(&response); err != nil {
		return result, fmt.Errorf("control response unavailable (retrying pause/resume/stop is safe): %w", err)
	}
	if response.Version != protocolVersion {
		return result, errors.New("unsupported worker control protocol")
	}
	if !response.OK {
		return result, errors.New(response.Error)
	}
	return response.Status, nil
}

type server struct {
	listener   *net.UnixListener
	path       string
	info       os.FileInfo
	acceptDone chan struct{}
	handlers   sync.WaitGroup
	errors     chan error
}

// listen is called only while the Store's exclusive writer lock is held.
func listen(dir string, calls chan<- controlCall) (*server, error) {
	path, err := Endpoint(dir)
	if err != nil {
		return nil, err
	}
	runtimeDir := filepath.Dir(path)
	if err := localfs.EnsurePrivateDir(runtimeDir); err != nil {
		return nil, err
	}
	if err := localfs.CheckOwnedDir(runtimeDir); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); err == nil {
		if err := checkSocket(path); err != nil {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		l.Close()
		return nil, err
	}
	l.SetUnlinkOnClose(false)
	s := &server{listener: l, path: path, info: info, acceptDone: make(chan struct{}), errors: make(chan error, 1)}
	go s.accept(calls)
	return s, nil
}

func (s *server) accept(calls chan<- controlCall) {
	defer close(s.acceptDone)
	limit := make(chan struct{}, 8)
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.errors <- err
			}
			return
		}
		select {
		case limit <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		s.handlers.Add(1)
		go func() { defer s.handlers.Done(); defer func() { <-limit }(); defer conn.Close(); serve(conn, calls) }()
	}
}

func serve(conn *net.UnixConn, calls chan<- controlCall) {
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return
	}
	if sameUser(conn) != nil {
		return
	}
	line, err := bufio.NewReader(io.LimitReader(conn, 4097)).ReadBytes('\n')
	respond := func(r Response) { _ = json.NewEncoder(conn).Encode(r) }
	if err != nil || len(line) > 4096 {
		respond(Response{Version: protocolVersion, Error: "request must be one JSON line of at most 4096 bytes"})
		return
	}
	var req Request
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		respond(Response{Version: protocolVersion, Error: "invalid control request"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF || req.Version != protocolVersion {
		respond(Response{Version: protocolVersion, Error: "unsupported protocol or trailing request data"})
		return
	}
	call := controlCall{req, ctx, make(chan Response, 1)}
	select {
	case calls <- call:
	case <-ctx.Done():
		return
	}
	select {
	case result := <-call.reply:
		respond(result)
	case <-ctx.Done():
		return
	}
}

func (s *server) Close() {
	_ = s.listener.Close()
	<-s.acceptDone
	s.handlers.Wait()
	// Do not unlink a replaced endpoint. Leave unrelated runtime files untouched.
	if info, err := os.Lstat(s.path); err == nil && os.SameFile(info, s.info) {
		_ = os.Remove(s.path)
	}
	_ = os.Remove(filepath.Dir(s.path))
}
