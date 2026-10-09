//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var (
	ErrManagerUnavailable = errors.New("the selected local user manager is unavailable")
	ErrManagerProtocol    = errors.New("the selected user manager response is unsupported")
	ErrManagerPath        = errors.New("the selected directory is absent from the observed user manager UnitPath")
)

type ManagerObservation struct {
	Status              string     `json:"status"`
	ObservedAt          *time.Time `json:"observed_at"`
	BusSocket           string     `json:"bus_socket"`
	UnitPath            []string   `json:"unit_path"`
	DirectoryInUnitPath *bool      `json:"directory_in_unit_path"`
}

type managerRunner func(context.Context, []string) ([]byte, error)

func observeUnitPath(ctx context.Context, socket string) (ManagerObservation, error) {
	return observeUnitPathWithRunner(ctx, socket, runManagerGetter)
}

func observeUnitPathWithRunner(ctx context.Context, socket string, run managerRunner) (ManagerObservation, error) {
	r := ManagerObservation{Status: "unavailable", BusSocket: socket, UnitPath: []string{}}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	if err := validatePath("bus socket", socket); err != nil {
		return r, err
	}
	if len(socket) > 107 {
		return r, ErrLifecycleBounds
	}
	chain, err := openServiceBase(ctx, filepath.Dir(socket))
	if err != nil {
		return r, errors.Join(ErrManagerUnavailable, err)
	}
	defer chain.close()
	parent := chain.last()
	var before, after unix.Stat_t
	if err = unix.Fstatat(int(parent.Fd()), filepath.Base(socket), &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return r, errors.Join(ErrManagerUnavailable, err)
	}
	if before.Mode&unix.S_IFMT != unix.S_IFSOCK || before.Uid != uint32(os.Geteuid()) || chain.stamps[len(chain.stamps)-1].Mode&0077 != 0 {
		return r, ErrManagerUnavailable
	}
	args := []string{"--address=unix:path=" + dbusAddressPath(socket), "--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.DBus.Properties", "Get", "ss", "org.freedesktop.systemd1.Manager", "UnitPath"}
	data, err := run(ctx, args)
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if err != nil {
		return r, err
	}
	paths, err := decodeManagerUnitPath(data)
	if err != nil {
		return r, err
	}
	if err = chain.check(ctx); err != nil {
		return r, errors.Join(ErrManagerUnavailable, err)
	}
	if err = unix.Fstatat(int(parent.Fd()), filepath.Base(socket), &after, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return r, errors.Join(ErrManagerUnavailable, err)
	}
	if !sameServiceStamp(before, after) {
		return r, ErrManagerUnavailable
	}
	now := time.Now().UTC()
	r.Status = "observed"
	r.ObservedAt = &now
	r.UnitPath = paths
	return r, ctx.Err()
}

// D-Bus addresses percent-encode bytes outside their literal subset. This is an
// address argument, never shell text; only the unix:path transport is selected.
func dbusAddressPath(path string) string {
	const digits = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(path); i++ {
		b := path[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("_-/.", rune(b)) {
			out.WriteByte(b)
		} else {
			out.WriteByte('%')
			out.WriteByte(digits[b>>4])
			out.WriteByte(digits[b&15])
		}
	}
	return out.String()
}

func runManagerGetter(ctx context.Context, args []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	executable, err := exec.LookPath("busctl")
	if err != nil {
		return nil, ErrManagerUnavailable
	}
	return runManagerGetterProcess(ctx, executable, args, nil)
}

func runManagerGetterProcess(ctx context.Context, executable string, args, env []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(childCtx, executable, args...)
	if env != nil {
		cmd.Env = env
	}
	cmd.SysProcAttr = &unix.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		err := unix.Kill(-cmd.Process.Pid, unix.SIGKILL)
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 100 * time.Millisecond
	stdout := &managerOutput{limit: 64 << 10, cancel: cancel}
	stderr := &managerOutput{limit: 16 << 10, cancel: cancel}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if stdout.exceeded || stderr.exceeded {
		return nil, ErrLifecycleBounds
	}
	if err != nil {
		return nil, ErrManagerUnavailable
	}
	return stdout.data.Bytes(), nil
}

type managerOutput struct {
	data     bytes.Buffer
	limit    int
	cancel   context.CancelFunc
	exceeded bool
}

func (w *managerOutput) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.data.Len() {
		w.exceeded = true
		w.cancel()
		return 0, ErrLifecycleBounds
	}
	return w.data.Write(p)
}

func decodeManagerUnitPath(data []byte) ([]string, error) {
	if len(data) > 64<<10 {
		return nil, ErrLifecycleBounds
	}
	// call Properties.Get returns one variant in the message's data array.
	type variant struct {
		Type string   `json:"type"`
		Data []string `json:"data"`
	}
	var reply struct {
		Type string    `json:"type"`
		Data []variant `json:"data"`
	}
	if err := uniqueManagerJSON(data); err != nil {
		return nil, ErrManagerProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return nil, ErrManagerProtocol
	}
	if reply.Type != "v" || len(reply.Data) != 1 || reply.Data[0].Type != "as" || reply.Data[0].Data == nil {
		return nil, ErrManagerProtocol
	}
	paths := reply.Data[0].Data
	if len(paths) > 64 {
		return nil, ErrLifecycleBounds
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if validatePath("manager UnitPath", path) != nil || seen[path] {
			return nil, ErrManagerProtocol
		}
		seen[path] = true
	}
	return paths, nil
}
func uniqueManagerJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	var value func(int) error
	value = func(depth int) error {
		if depth > 8 {
			return ErrManagerProtocol
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				token, err = d.Token()
				if err != nil {
					return err
				}
				key, ok := token.(string)
				if !ok || seen[key] {
					return ErrManagerProtocol
				}
				seen[key] = true
				if err = value(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err = value(depth + 1); err != nil {
					return err
				}
			}
		default:
			return ErrManagerProtocol
		}
		_, err = d.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return ErrManagerProtocol
	}
	return nil
}
