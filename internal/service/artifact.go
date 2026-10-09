//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const LifecycleContract = "service_artifact_lifecycle_v1"

var (
	ErrArtifactConflict    = errors.New("service artifact is foreign or differs from the exact descriptor")
	ErrArtifactChanged     = errors.New("service artifact or directory changed")
	ErrArtifactBusy        = errors.New("another service artifact publisher is running")
	ErrArtifactPublication = errors.New("service artifact publication is uncertain; inspect service status with the exact scope")
	ErrLifecycleBounds     = errors.New("service lifecycle operation exceeds its bounds")
)

type InstallSpec struct {
	Service   Spec
	HomeDir   string
	Directory string
	BusSocket string
}

type LifecycleResult struct {
	Descriptor
	Action              string             `json:"action"`
	Directory           string             `json:"directory"`
	DescriptorPath      string             `json:"descriptor_path"`
	DescriptorSHA256    string             `json:"descriptor_sha256"`
	ArtifactStatus      string             `json:"artifact_status"`
	Publication         string             `json:"publication"`
	SyncCompleted       bool               `json:"sync_completed"`
	DirectoriesCreated  []string           `json:"directories_created"`
	LockCreated         bool               `json:"lock_created"`
	FutureLoginMayStart bool               `json:"future_login_may_start"`
	Manager             ManagerObservation `json:"manager"`
}

// Install publishes only the exact descriptor. It never enables, loads or starts
// a manager unit. Login-directory publication can affect a later macOS login.
func Install(ctx context.Context, spec InstallSpec) (LifecycleResult, error) {
	return lifecycle(ctx, spec, true, artifactHooks{})
}

// Inspect compares the named artifact to the exact intended descriptor. It does
// not initialize directories or infer enablement/running state from publication.
func Inspect(ctx context.Context, spec InstallSpec) (LifecycleResult, error) {
	return lifecycle(ctx, spec, false, artifactHooks{})
}

type artifactHooks struct {
	manager       func(context.Context, string) (ManagerObservation, error)
	beforePublish func()
	afterPublish  func()
	publish       func(int, string, int, string) error
	syncParent    func(*os.File) error
}

func lifecycle(ctx context.Context, spec InstallSpec, install bool, hooks artifactHooks) (LifecycleResult, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	descriptor, err := Build(spec.Service)
	if err != nil {
		return LifecycleResult{}, err
	}
	for _, p := range []string{spec.HomeDir, spec.Directory} {
		if err := validatePath("installation", p); err != nil {
			return LifecycleResult{}, err
		}
		if len(strings.Split(strings.TrimPrefix(p, "/"), "/")) > 64 {
			return LifecycleResult{}, ErrLifecycleBounds
		}
	}
	base := spec.HomeDir
	if spec.Service.GOOS == "darwin" {
		if spec.Directory != filepath.Join(spec.HomeDir, "Library", "LaunchAgents") {
			return LifecycleResult{}, fmt.Errorf("%w: macOS requires the selected home's Library/LaunchAgents directory", ErrSpec)
		}
	} else {
		if filepath.Base(spec.Directory) != "user" || filepath.Base(filepath.Dir(spec.Directory)) != "systemd" {
			return LifecycleResult{}, fmt.Errorf("%w: Linux requires an exact systemd/user directory", ErrSpec)
		}
		base = filepath.Dir(filepath.Dir(spec.Directory))
		if err := validatePath("bus socket", spec.BusSocket); err != nil {
			return LifecycleResult{}, err
		}
	}
	descriptor.Contract = LifecycleContract
	digest := sha256.Sum256([]byte(descriptor.Content))
	r := LifecycleResult{Descriptor: descriptor, Action: "status", Directory: spec.Directory, DescriptorPath: filepath.Join(spec.Directory, descriptor.Filename), DescriptorSHA256: hex.EncodeToString(digest[:]), ArtifactStatus: "unexamined", Publication: "not_requested", DirectoriesCreated: []string{}, FutureLoginMayStart: spec.Service.GOOS == "darwin", Manager: ManagerObservation{Status: "not_checked", UnitPath: []string{}}}
	if install {
		r.Action = "install"
	}
	if spec.Service.GOOS == "linux" {
		probe := observeUnitPath
		if hooks.manager != nil {
			probe = hooks.manager
		}
		r.Manager, err = probe(ctx, spec.BusSocket)
		if err != nil {
			if install || !errors.Is(err, ErrManagerUnavailable) || errors.Is(err, ErrLifecycleBounds) || ctx.Err() != nil {
				return r, err
			}
		} else {
			inPath := false
			for _, p := range r.Manager.UnitPath {
				if p == spec.Directory {
					inPath = true
				}
			}
			r.Manager.DirectoryInUnitPath = &inPath
			if install && !inPath {
				return r, ErrManagerPath
			}
		}
	}
	chain, err := openServiceBase(ctx, base)
	if err != nil {
		return r, err
	}
	defer chain.close()
	for _, name := range []string{filepath.Base(filepath.Dir(spec.Directory)), filepath.Base(spec.Directory)} {
		childPath := filepath.Join(chain.last().Name(), name)
		created, err := chain.child(ctx, name, install)
		if created {
			r.DirectoriesCreated = append(r.DirectoriesCreated, childPath)
		}
		if err != nil {
			if !install && errors.Is(err, os.ErrNotExist) {
				r.ArtifactStatus = "absent"
				return r, errors.Join(chain.check(ctx), ctx.Err())
			}
			return r, err
		}
	}
	parent := chain.last()
	if chain.stamps[len(chain.stamps)-1].Uid != uint32(os.Geteuid()) || chain.stamps[len(chain.stamps)-1].Mode&0022 != 0 {
		return r, ErrArtifactConflict
	}
	status, err := readArtifact(ctx, parent, descriptor.Filename, []byte(descriptor.Content))
	if err != nil {
		return r, err
	}
	r.ArtifactStatus = status
	if !install {
		return r, errors.Join(chain.check(ctx), ctx.Err())
	}
	lock, created, err := serviceArtifactLock(ctx, parent)
	r.LockCreated = created
	if err != nil {
		return r, err
	}
	defer lock.Close()
	var lockStamp unix.Stat_t
	if err = unix.Fstat(int(lock.Fd()), &lockStamp); err != nil {
		return r, err
	}
	if err = chain.check(ctx); err != nil {
		return r, err
	}
	status, err = readArtifact(ctx, parent, descriptor.Filename, []byte(descriptor.Content))
	if err != nil {
		return r, err
	}
	r.ArtifactStatus = status
	if status == "exact" {
		r.Publication = "not_needed"
		return r, errors.Join(checkServiceLock(parent, lock, lockStamp), chain.check(ctx), ctx.Err())
	}
	if err = checkServiceDirectoryBounds(ctx, parent); err != nil {
		return r, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return r, err
	}
	name := "." + ManagedLabel + "-" + hex.EncodeToString(nonce[:]) + ".tmp"
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return r, err
	}
	temp := os.NewFile(uintptr(fd), filepath.Join(spec.Directory, name))
	defer temp.Close()
	var initial unix.Stat_t
	if err = unix.Fstat(fd, &initial); err != nil {
		return r, err
	}
	defer func() {
		var named unix.Stat_t
		if unix.Fstatat(int(parent.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW) == nil && sameServiceIdentity(initial, named) {
			_ = unix.Unlinkat(int(parent.Fd()), name, 0)
		}
	}()
	data := []byte(descriptor.Content)
	for offset := 0; offset < len(data); {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		n, writeErr := temp.Write(data[offset:min(offset+(32<<10), len(data))])
		offset += n
		if writeErr != nil {
			return r, writeErr
		}
		if n == 0 {
			return r, io.ErrNoProgress
		}
	}
	if err = temp.Sync(); err != nil {
		return r, err
	}
	var prepared unix.Stat_t
	if err = unix.Fstat(fd, &prepared); err != nil {
		return r, err
	}
	if !serviceFile(prepared) || prepared.Size != int64(len(data)) {
		return r, ErrArtifactChanged
	}
	if hooks.beforePublish != nil {
		hooks.beforePublish()
	}
	if err = chain.check(ctx); err != nil {
		return r, err
	}
	if err = checkServiceLock(parent, lock, lockStamp); err != nil {
		return r, err
	}
	var held, named unix.Stat_t
	if err = unix.Fstat(fd, &held); err != nil {
		return r, err
	}
	if err = unix.Fstatat(int(parent.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return r, err
	}
	if !sameServiceStamp(prepared, held) || !sameServiceStamp(held, named) {
		return r, ErrArtifactChanged
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	publish := publishServiceExclusive
	if hooks.publish != nil {
		publish = hooks.publish
	}
	if err = publish(int(parent.Fd()), name, int(parent.Fd()), descriptor.Filename); err != nil {
		// Destination-exclusive EEXIST proves no replacement by this operation.
		if errors.Is(err, unix.EEXIST) {
			return r, ErrArtifactConflict
		}
		return uncertainArtifact(r, errors.Join(err, ctx.Err()))
	}
	if hooks.afterPublish != nil {
		hooks.afterPublish()
	}
	if err = chain.check(ctx); err != nil {
		return uncertainArtifact(r, err)
	}
	if err = checkServiceLock(parent, lock, lockStamp); err != nil {
		return uncertainArtifact(r, err)
	}
	if _, err = readArtifact(ctx, parent, descriptor.Filename, data); err != nil {
		return uncertainArtifact(r, err)
	}
	var published unix.Stat_t
	if err = unix.Fstatat(int(parent.Fd()), descriptor.Filename, &published, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return uncertainArtifact(r, err)
	}
	if !sameServiceIdentity(prepared, published) {
		return uncertainArtifact(r, ErrArtifactChanged)
	}
	syncParent := func(f *os.File) error { return f.Sync() }
	if hooks.syncParent != nil {
		syncParent = hooks.syncParent
	}
	if err = syncParent(parent); err != nil {
		return uncertainArtifact(r, err)
	}
	if err = chain.check(ctx); err != nil {
		return uncertainArtifact(r, err)
	}
	if err = checkServiceLock(parent, lock, lockStamp); err != nil {
		return uncertainArtifact(r, err)
	}
	if _, err = readArtifact(ctx, parent, descriptor.Filename, data); err != nil {
		return uncertainArtifact(r, err)
	}
	if err = temp.Close(); err != nil {
		return uncertainArtifact(r, err)
	}
	if err = ctx.Err(); err != nil {
		return uncertainArtifact(r, err)
	}
	r.ArtifactStatus = "exact"
	r.Publication = "saved"
	r.SyncCompleted = true
	r.InstallationPerformed = true
	return r, nil
}

func uncertainArtifact(r LifecycleResult, err error) (LifecycleResult, error) {
	r.ArtifactStatus = "unexamined"
	r.Publication = "uncertain"
	return r, fmt.Errorf("%w: descriptor %q, candidate SHA-256 %s: %w", ErrArtifactPublication, r.DescriptorPath, r.DescriptorSHA256, err)
}

type serviceChain struct {
	files  []*os.File
	stamps []unix.Stat_t
}

func (c *serviceChain) last() *os.File { return c.files[len(c.files)-1] }
func (c *serviceChain) close() {
	for i := len(c.files) - 1; i >= 0; i-- {
		_ = c.files[i].Close()
	}
}
func openServiceBase(ctx context.Context, base string) (*serviceChain, error) {
	parts := strings.Split(strings.TrimPrefix(base, "/"), "/")
	if len(parts) > 64 {
		return nil, ErrLifecycleBounds
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	c := &serviceChain{files: []*os.File{os.NewFile(uintptr(fd), "/")}}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		c.close()
		return nil, err
	}
	c.stamps = append(c.stamps, st)
	for _, name := range parts {
		if name == "" {
			continue
		}
		if _, err = c.child(ctx, name, false); err != nil {
			c.close()
			return nil, err
		}
	}
	if st = c.stamps[len(c.stamps)-1]; st.Uid != uint32(os.Geteuid()) || st.Mode&0022 != 0 {
		c.close()
		return nil, ErrArtifactConflict
	}
	return c, nil
}
func (c *serviceChain) child(ctx context.Context, name string, create bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p := c.last()
	created := false
	fd, err := unix.Openat(int(p.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if create && errors.Is(err, unix.ENOENT) {
		if err = unix.Mkdirat(int(p.Fd()), name, 0700); err == nil {
			created = true
		} else if !errors.Is(err, unix.EEXIST) {
			return false, err
		}
		if created {
			if err = p.Sync(); err != nil {
				return true, err
			}
		}
		fd, err = unix.Openat(int(p.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	}
	if err != nil {
		return created, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(p.Name(), name))
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		_ = f.Close()
		return created, err
	}
	// Root-owned sticky ancestors such as /tmp cannot replace another user's
	// private child. This is cooperating-user scope, not namespace authentication.
	if !serviceDir(st) {
		_ = f.Close()
		return created, ErrArtifactConflict
	}
	c.files = append(c.files, f)
	c.stamps = append(c.stamps, st)
	return created, c.check(ctx)
}
func serviceDir(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && (st.Uid == 0 || st.Uid == uint32(os.Geteuid())) && (st.Mode&0022 == 0 || (st.Uid == 0 && st.Mode&unix.S_ISVTX != 0))
}
func sameServiceIdentity(a, b unix.Stat_t) bool { return a.Dev == b.Dev && a.Ino == b.Ino }
func sameServiceStamp(a, b unix.Stat_t) bool {
	return sameServiceIdentity(a, b) && a.Mode == b.Mode && a.Uid == b.Uid && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
func serviceFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 && st.Mode&0077 == 0
}
func (c *serviceChain) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, f := range c.files {
		var held, named unix.Stat_t
		if err := unix.Fstat(int(f.Fd()), &held); err != nil {
			return err
		}
		if err := unix.Lstat(f.Name(), &named); err != nil {
			return err
		}
		original := c.stamps[i]
		if !serviceDir(held) || !serviceDir(named) || !sameServiceIdentity(original, held) || !sameServiceIdentity(held, named) || original.Mode != held.Mode || original.Uid != held.Uid {
			return ErrArtifactChanged
		}
	}
	return ctx.Err()
}
func readArtifact(ctx context.Context, parent *os.File, name string, want []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return "absent", nil
	}
	if err != nil {
		return "", errors.Join(ErrArtifactConflict, err)
	}
	f := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), name))
	defer f.Close()
	var before, after, named unix.Stat_t
	if err = unix.Fstat(fd, &before); err != nil {
		return "", err
	}
	if !serviceFile(before) || before.Size > MaxDescriptorBytes {
		return "", ErrArtifactConflict
	}
	var data bytes.Buffer
	buffer := make([]byte, 32<<10)
	for {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		n, readErr := f.Read(buffer)
		if data.Len()+n > MaxDescriptorBytes {
			return "", ErrLifecycleBounds
		}
		data.Write(buffer[:n])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", readErr
		}
		if n == 0 {
			return "", io.ErrNoProgress
		}
	}
	if err = unix.Fstat(fd, &after); err != nil {
		return "", err
	}
	if err = unix.Fstatat(int(parent.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return "", err
	}
	if !serviceFile(after) || !serviceFile(named) || !sameServiceStamp(before, after) || !sameServiceStamp(after, named) {
		return "", ErrArtifactChanged
	}
	if !bytes.Equal(data.Bytes(), want) {
		return "", ErrArtifactConflict
	}
	return "exact", errors.Join(f.Close(), ctx.Err())
}
func serviceArtifactLock(ctx context.Context, parent *os.File) (*os.File, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	name := "." + ManagedLabel + ".lock"
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	created := err == nil
	if errors.Is(err, unix.EEXIST) {
		fd, err = unix.Openat(int(parent.Fd()), name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	}
	if err != nil {
		return nil, created, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), name))
	fail := func(err error) (*os.File, bool, error) { _ = f.Close(); return nil, created, err }
	if err = checkServiceLock(parent, f); err != nil {
		return fail(err)
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			err = ErrArtifactBusy
		}
		return fail(err)
	}
	if err = checkServiceLock(parent, f); err != nil {
		return fail(err)
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return f, created, nil
}
func checkServiceLock(parent, file *os.File, expected ...unix.Stat_t) error {
	var held, named unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(int(parent.Fd()), "."+ManagedLabel+".lock", &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !serviceFile(held) || !serviceFile(named) || !sameServiceStamp(held, named) || (len(expected) > 0 && !sameServiceStamp(expected[0], held)) {
		return ErrArtifactChanged
	}
	return nil
}

func checkServiceDirectoryBounds(ctx context.Context, parent *os.File) error {
	fd, err := unix.Openat(int(parent.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(fd), parent.Name())
	defer dir.Close()
	total, artifacts := 0, 0
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		names, readErr := dir.Readdirnames(64)
		total += len(names)
		for _, name := range names {
			if strings.HasPrefix(name, "."+ManagedLabel+"-") && strings.HasSuffix(name, ".tmp") {
				artifacts++
			}
		}
		if total > 1024 || artifacts >= 128 {
			return ErrLifecycleBounds
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
