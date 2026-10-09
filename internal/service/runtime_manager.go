//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Each invocation owns this bounded session. Separate busctl connections retain
// the captured socket name/identity and unique destination owner. This remains a
// cooperating current-user namespace, not bus or same-user authentication.
type runtimeBus struct {
	chain   *serviceChain
	socket  string
	stamp   unix.Stat_t
	program string
	run     runtimeRunner
	calls   int
	bytes   int
}

func openRuntimeBus(ctx context.Context, socket, program string, run runtimeRunner) (*runtimeBus, error) {
	if len(socket) > 107 {
		return nil, ErrLifecycleBounds
	}
	chain, err := openServiceBase(ctx, filepath.Dir(socket))
	if err != nil {
		return nil, errors.Join(ErrManagerUnavailable, err)
	}
	b := &runtimeBus{chain: chain, socket: socket, program: program, run: run}
	if err = unix.Fstatat(int(chain.last().Fd()), filepath.Base(socket), &b.stamp, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		chain.close()
		return nil, errors.Join(ErrManagerUnavailable, err)
	}
	if b.stamp.Mode&unix.S_IFMT != unix.S_IFSOCK || b.stamp.Uid != uint32(os.Geteuid()) || chain.stamps[len(chain.stamps)-1].Mode&0077 != 0 {
		chain.close()
		return nil, ErrManagerUnavailable
	}
	return b, nil
}

func (b *runtimeBus) close() { b.chain.close() }

func (b *runtimeBus) check(ctx context.Context) error {
	if err := b.chain.check(ctx); err != nil {
		return errors.Join(ErrManagerUnavailable, err)
	}
	var named unix.Stat_t
	if err := unix.Fstatat(int(b.chain.last().Fd()), filepath.Base(b.socket), &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errors.Join(ErrManagerUnavailable, err)
	}
	if !sameServiceStamp(b.stamp, named) {
		return ErrManagerUnavailable
	}
	return ctx.Err()
}

func (b *runtimeBus) call(ctx context.Context, dest, object, iface, method, signature string, values ...string) (runtimeReply, error) {
	if b.calls >= 7 {
		return runtimeReply{}, ErrLifecycleBounds
	}
	if err := b.check(ctx); err != nil {
		return runtimeReply{}, err
	}
	args := []string{"--address=unix:path=" + dbusAddressPath(b.socket), "--auto-start=no", "--allow-interactive-authorization=no", "--timeout=2s", "--json=short", "--no-pager", "call", dest, object, iface, method, signature}
	args = append(args, values...)
	b.calls++
	childCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	reply, err := b.run(childCtx, b.program, args)
	cancel()
	b.bytes += len(reply.data)
	if len(reply.data) > 64<<10 || b.bytes > 448<<10 {
		return reply, errors.Join(ErrLifecycleBounds, ctx.Err())
	}
	if err != nil {
		return reply, errors.Join(err, ctx.Err())
	}
	if !reply.started {
		return reply, ErrManagerProtocol
	}
	return reply, b.check(ctx)
}

func (b *runtimeBus) owner(ctx context.Context) (string, error) {
	reply, err := b.call(ctx, "org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus", "GetNameOwner", "s", "org.freedesktop.systemd1")
	if err != nil {
		return "", err
	}
	owner, err := decodeRuntimeString(reply.data)
	if err != nil || !runtimeUniqueName(owner) {
		return "", ErrManagerProtocol
	}
	return owner, nil
}

func (b *runtimeBus) checkOwner(ctx context.Context, want string) error {
	owner, err := b.owner(ctx)
	if err != nil {
		return err
	}
	if owner != want {
		return ErrRuntimeBinding
	}
	return nil
}

func bindRuntimeUnit(ctx context.Context, b *runtimeBus, d Descriptor, r *RuntimeResult) error {
	owner, err := b.owner(ctx)
	if err != nil {
		return err
	}
	r.ManagerUniqueName = owner
	reply, err := b.call(ctx, owner, "/org/freedesktop/systemd1", "org.freedesktop.DBus.Properties", "Get", "ss", "org.freedesktop.systemd1.Manager", "UnitPath")
	if err != nil {
		return err
	}
	paths, err := decodeManagerUnitPath(reply.data)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	inPath := false
	for _, path := range paths {
		if path == r.Directory {
			inPath = true
		}
	}
	r.Manager = ManagerObservation{Status: "observed", ObservedAt: &now, BusSocket: b.socket, UnitPath: paths, DirectoryInUnitPath: &inPath}
	if !inPath {
		return ErrManagerPath
	}
	reply, err = b.call(ctx, owner, "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "LoadUnit", "s", d.Filename)
	r.UnitLoadAttempted = reply.started
	if err != nil {
		return err
	}
	object, err := decodeRuntimeObject(reply.data)
	if err != nil || !runtimeObjectPath(object) || !strings.HasPrefix(object, "/org/freedesktop/systemd1/unit/") {
		return ErrManagerProtocol
	}
	r.UnitObjectPath = object
	reply, err = b.call(ctx, owner, object, "org.freedesktop.DBus.Properties", "GetAll", "s", "org.freedesktop.systemd1.Unit")
	if err != nil {
		return err
	}
	unit, err := decodeRuntimeProperties(reply.data)
	if err != nil {
		return err
	}
	reply, err = b.call(ctx, owner, object, "org.freedesktop.DBus.Properties", "GetAll", "s", "org.freedesktop.systemd1.Service")
	if err != nil {
		return err
	}
	service, err := decodeRuntimeProperties(reply.data)
	if err != nil {
		return err
	}
	if err = matchRuntimeProperties(d, r.DescriptorPath, unit, service); err != nil {
		if errors.Is(err, ErrRuntimeBinding) {
			matched := false
			r.LoadedBindingMatched = &matched
		}
		return err
	}
	matched := true
	now = time.Now().UTC()
	r.LoadedBindingMatched = &matched
	r.BindingCheckedAt = &now
	return ctx.Err()
}

type runtimeVariant struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func decodeRuntimeMessage(data []byte, signature string) (json.RawMessage, error) {
	if len(data) > 64<<10 {
		return nil, ErrLifecycleBounds
	}
	if uniqueManagerJSON(data) != nil {
		return nil, ErrManagerProtocol
	}
	var reply struct {
		Type string            `json:"type"`
		Data []json.RawMessage `json:"data"`
	}
	if runtimeJSON(data, &reply) != nil || reply.Type != signature || len(reply.Data) != 1 || bytes.Equal(reply.Data[0], []byte("null")) {
		return nil, ErrManagerProtocol
	}
	return reply.Data[0], nil
}

func runtimeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return ErrManagerProtocol
	}
	return nil
}

func decodeRuntimeString(data []byte) (string, error) {
	raw, err := decodeRuntimeMessage(data, "s")
	var value string
	if err != nil || runtimeJSON(raw, &value) != nil {
		return "", ErrManagerProtocol
	}
	return value, nil
}

func decodeRuntimeObject(data []byte) (string, error) {
	raw, err := decodeRuntimeMessage(data, "o")
	var value string
	if err != nil || runtimeJSON(raw, &value) != nil {
		return "", ErrManagerProtocol
	}
	return value, nil
}

func decodeRuntimeProperties(data []byte) (map[string]runtimeVariant, error) {
	raw, err := decodeRuntimeMessage(data, "a{sv}")
	if err != nil {
		return nil, err
	}
	var properties map[string]runtimeVariant
	if runtimeJSON(raw, &properties) != nil || properties == nil || len(properties) > 512 {
		return nil, ErrManagerProtocol
	}
	for key, value := range properties {
		if key == "" || len(key) > 128 || value.Type == "" || len(value.Type) > 128 || len(value.Data) == 0 {
			return nil, ErrManagerProtocol
		}
	}
	return properties, nil
}

func matchRuntimeProperties(d Descriptor, descriptorPath string, unit, service map[string]runtimeVariant) error {
	for _, check := range []struct {
		properties map[string]runtimeVariant
		name       string
		want       string
	}{
		{unit, "Id", d.Filename}, {unit, "FragmentPath", descriptorPath},
		{unit, "LoadState", "loaded"}, {unit, "SourcePath", ""}, {unit, "Following", ""},
		{service, "Type", "exec"}, {service, "Restart", "on-failure"},
		{service, "RootDirectory", ""}, {service, "RootImage", ""},
	} {
		value, err := runtimeProperty[string](check.properties, check.name, "s")
		if err != nil {
			return err
		}
		if value != check.want {
			return ErrRuntimeBinding
		}
	}
	for _, name := range []string{"NeedDaemonReload", "Transient"} {
		value, err := runtimeProperty[bool](unit, name, "b")
		if err != nil {
			return err
		}
		if value {
			return ErrRuntimeBinding
		}
	}
	for _, check := range []struct {
		properties map[string]runtimeVariant
		name       string
		signature  string
	}{
		{unit, "DropInPaths", "as"},
		{service, "EnvironmentFiles", "a(sb)"}, {service, "PassEnvironment", "as"}, {service, "UnsetEnvironment", "as"},
		{service, "ExecConditionEx", "a(sasasttttuii)"}, {service, "ExecStartPreEx", "a(sasasttttuii)"},
		{service, "ExecStartPostEx", "a(sasasttttuii)"}, {service, "ExecStopEx", "a(sasasttttuii)"},
		{service, "ExecStopPostEx", "a(sasasttttuii)"}, {service, "ExecReloadEx", "a(sasasttttuii)"},
	} {
		values, err := runtimeProperty[[]json.RawMessage](check.properties, check.name, check.signature)
		if err != nil || values == nil {
			return ErrManagerProtocol
		}
		if len(values) != 0 {
			return ErrRuntimeBinding
		}
	}
	for name, want := range map[string]uint64{"RestartUSec": 30_000_000, "TimeoutStopUSec": 10_000_000} {
		value, err := runtimeProperty[uint64](service, name, "t")
		if err != nil {
			return err
		}
		if value != want {
			return ErrRuntimeBinding
		}
	}
	environment, err := runtimeProperty[[]string](service, "Environment", "as")
	if err != nil || environment == nil {
		return ErrManagerProtocol
	}
	wantEnv := make([]string, 0, len(d.Env))
	for key, value := range d.Env {
		wantEnv = append(wantEnv, key+"="+value)
	}
	sort.Strings(wantEnv)
	sort.Strings(environment)
	if !reflect.DeepEqual(environment, wantEnv) {
		return ErrRuntimeBinding
	}
	commands, err := runtimeProperty[[][]json.RawMessage](service, "ExecStartEx", "a(sasasttttuii)")
	if err != nil || commands == nil {
		return ErrManagerProtocol
	}
	if len(commands) != 1 {
		return ErrRuntimeBinding
	}
	command := commands[0]
	if len(command) != 10 {
		return ErrManagerProtocol
	}
	var path string
	var argv, flags []string
	if runtimeJSON(command[0], &path) != nil || runtimeJSON(command[1], &argv) != nil || runtimeJSON(command[2], &flags) != nil || argv == nil || flags == nil {
		return ErrManagerProtocol
	}
	if path != d.Executable || !reflect.DeepEqual(argv, d.Argv) || !reflect.DeepEqual(flags, []string{"no-env-expand"}) {
		return ErrRuntimeBinding
	}
	// Runtime timestamps/PID/status can legitimately be nonzero after earlier
	// requests. Validate the fixed wire shape but do not publish them as state.
	for i := 3; i < 7; i++ {
		var value uint64
		if runtimeJSON(command[i], &value) != nil || bytes.Equal(command[i], []byte("null")) {
			return ErrManagerProtocol
		}
	}
	var pid uint32
	if runtimeJSON(command[7], &pid) != nil || bytes.Equal(command[7], []byte("null")) {
		return ErrManagerProtocol
	}
	for i := 8; i < 10; i++ {
		var value int32
		if runtimeJSON(command[i], &value) != nil || bytes.Equal(command[i], []byte("null")) {
			return ErrManagerProtocol
		}
	}
	return nil
}

func runtimeProperty[T any](properties map[string]runtimeVariant, name, signature string) (T, error) {
	var value T
	property, ok := properties[name]
	if !ok || property.Type != signature || len(property.Data) == 0 || bytes.Equal(property.Data, []byte("null")) || runtimeJSON(property.Data, &value) != nil {
		return value, ErrManagerProtocol
	}
	return value, nil
}

func runtimeUniqueName(value string) bool {
	if len(value) > 64 || !strings.HasPrefix(value, ":") {
		return false
	}
	parts := strings.Split(value[1:], ".")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || strings.ContainsFunc(part, func(r rune) bool { return r < '0' || r > '9' }) {
			return false
		}
	}
	return true
}

func runtimeObjectPath(value string) bool {
	if len(value) > MaxPathBytes || value == "/" || !strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return false
	}
	for _, part := range strings.Split(value[1:], "/") {
		if part == "" || strings.ContainsFunc(part, func(r rune) bool {
			return !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_'
		}) {
			return false
		}
	}
	return true
}

func runtimeJobPath(value string) bool {
	const prefix = "/org/freedesktop/systemd1/job/"
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	id := strings.TrimPrefix(value, prefix)
	n, err := strconv.ParseUint(id, 10, 32)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == id
}
