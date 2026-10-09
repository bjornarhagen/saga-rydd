// Package packaging creates local candidate artifacts, never installed files.
package packaging

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"debug/macho"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
)

const (
	Contract                = "candidate_package_v1"
	Toolchain               = "go1.27.1"
	BinaryByteLimit   int64 = 128 << 20
	MetadataByteLimit       = 16 << 10
	operationLimit          = 10 * time.Minute
)

var (
	ErrInput       = errors.New("candidate packaging requires a finite version and an absolute repository path")
	ErrArtifact    = errors.New("candidate binary does not match the fixed build contract")
	ErrExists      = errors.New("candidate destination or interrupted staging already exists; inspect it before using another candidate label")
	ErrPublication = errors.New("candidate publication needs inspection")
)

type target struct{ os, arch string }

var targets = []target{{"darwin", "amd64"}, {"darwin", "arm64"}, {"linux", "amd64"}, {"linux", "arm64"}}

type Artifact struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type Result struct {
	Contract             string     `json:"contract"`
	Version              string     `json:"version"`
	Directory            string     `json:"directory"`
	Archives             []Artifact `json:"archives"`
	PublicationAttempted bool       `json:"publication_attempted"`
	RenameCompleted      bool       `json:"rename_completed"`
	SyncCompleted        bool       `json:"sync_completed"`
	ReleaseAccepted      bool       `json:"release_accepted"`
}

type BuildInfo struct {
	Contract                    string               `json:"contract"`
	BuildMetadata               buildmetadata.Record `json:"build_metadata"`
	BinarySHA256                string               `json:"binary_sha256"`
	BinaryBytes                 int64                `json:"binary_bytes"`
	ArtifactKind                string               `json:"artifact_kind"`
	SourceAuthenticated         bool                 `json:"source_authenticated"`
	NativeCompatibilityVerified bool                 `json:"native_compatibility_verified"`
	DistributionSigned          bool                 `json:"distribution_signed"`
	ReleaseAccepted             bool                 `json:"release_accepted"`
}

type hooks struct {
	build         func(context.Context, string, string, target, string) error
	beforeArchive func(int, string)
	publish       func(string, string) error
	sync          func(*os.File) error
	afterPublish  func()
}

// Build freshly compiles only cmd/rydd. One fixed staging directory bounds
// cooperating crash leftovers; it is never imported or automatically recovered.
// The final directory rename is developer-artifact publication, not source-file
// action authority. All four archives and checksums exist before publication.
func Build(ctx context.Context, repoDir, version string) (Result, error) {
	return build(ctx, repoDir, version, hooks{})
}

func build(ctx context.Context, repoDir, version string, h hooks) (Result, error) {
	if ctx == nil || !buildmetadata.ValidVersion(version) || !filepath.IsAbs(repoDir) || filepath.Clean(repoDir) != repoDir || len(repoDir) > 4096 || !utf8.ValidString(repoDir) || strings.ContainsRune(repoDir, 0) {
		return Result{}, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	r := Result{Contract: Contract, Version: version, Directory: filepath.Join("dist", "candidates", version), Archives: []Artifact{}}
	parent := filepath.Join(repoDir, "dist", "candidates")
	for _, p := range []string{repoDir, filepath.Join(repoDir, "dist"), parent} {
		_, err := directory(p, p != repoDir)
		if err != nil {
			return r, err
		}
		// Repeat these two parent syncs on retries as well. A prior call may
		// have created the entry but failed its parent sync before returning.
		if p != repoDir {
			if err := syncDirectory(filepath.Dir(p), h.sync); err != nil {
				return r, err
			}
		}
	}
	final := filepath.Join(repoDir, r.Directory)
	if _, err := os.Lstat(final); err == nil {
		return r, ErrExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return r, err
	}
	stage := filepath.Join(parent, ".staging")
	if err := os.Mkdir(stage, 0700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return r, ErrExists
		}
		return r, err
	}
	removeStage := true
	defer func() {
		if removeStage {
			_ = os.RemoveAll(stage)
		}
	}()
	buildBinary := h.build
	if buildBinary == nil {
		buildBinary = compile
	}
	var first *buildmetadata.Record
	for i, t := range targets {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		dir := filepath.Join(stage, t.os+"-"+t.arch)
		if err := os.Mkdir(dir, 0700); err != nil {
			return r, err
		}
		binary := filepath.Join(dir, "rydd")
		if err := buildBinary(ctx, repoDir, binary, t, version); err != nil {
			return r, err
		}
		if h.beforeArchive != nil {
			h.beforeArchive(i, binary)
		}
		info, err := inspect(ctx, binary, t, version)
		if err != nil {
			return r, err
		}
		if first != nil && (first.SourceStatus != info.BuildMetadata.SourceStatus || !reflect.DeepEqual(first.Revision, info.BuildMetadata.Revision)) {
			return r, fmt.Errorf("%w: declared VCS context changed between targets", ErrArtifact)
		}
		v := info.BuildMetadata
		first = &v
		name := "rydd-" + version + "-" + t.os + "-" + t.arch + ".tar.gz"
		a, err := archive(ctx, filepath.Join(stage, name), binary, info)
		if err != nil {
			return r, err
		}
		r.Archives = append(r.Archives, a)
		if err = os.RemoveAll(dir); err != nil {
			return r, err
		}
	}
	var checksums strings.Builder
	for _, a := range r.Archives {
		fmt.Fprintf(&checksums, "%s  %s\n", a.SHA256, a.Name)
	}
	if err := writeFile(ctx, filepath.Join(stage, "SHA256SUMS"), []byte(checksums.String())); err != nil {
		return r, err
	}
	if err := syncDirectory(stage, h.sync); err != nil {
		return r, err
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	publish := h.publish
	if publish == nil {
		publish = publishExclusive
	}
	r.PublicationAttempted = true
	if err := publish(stage, final); err != nil {
		// An uncertain rename must not remove a candidate or its staging input.
		removeStage = false
		return r, fmt.Errorf("%w: inspect %s and dist/candidates/.staging: %w", ErrPublication, r.Directory, err)
	}
	r.RenameCompleted = true
	removeStage = false
	if h.afterPublish != nil {
		h.afterPublish()
	}
	if err := ctx.Err(); err != nil {
		return r, fmt.Errorf("%w: inspect %s: %w", ErrPublication, r.Directory, err)
	}
	if err := syncDirectory(parent, h.sync); err != nil {
		return r, fmt.Errorf("%w: inspect %s: %w", ErrPublication, r.Directory, err)
	}
	r.SyncCompleted = true
	if err := ctx.Err(); err != nil {
		return r, fmt.Errorf("%w: inspect %s: %w", ErrPublication, r.Directory, err)
	}
	return r, nil
}

func directory(path string, create bool) (bool, error) {
	created := false
	if create {
		if err := os.Mkdir(path, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return false, err
		} else if err == nil {
			created = true
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return created, fmt.Errorf("%w: output directory unavailable", ErrInput)
	}
	return created, nil
}

func linkerFlags(version string) string {
	return "-X=github.com/bjornarhagen/saga-rydd/internal/buildmetadata.Version=" + version
}

func inspect(ctx context.Context, path string, t target, version string) (BuildInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return BuildInfo{}, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > BinaryByteLimit {
		return BuildInfo{}, ErrArtifact
	}
	reader := io.NewSectionReader(f, 0, before.Size())
	info, err := buildinfo.Read(reader)
	if err != nil || len(info.Settings) > 64 || len(info.Deps) > 128 || info.Path != "github.com/bjornarhagen/saga-rydd/cmd/rydd" || info.GoVersion != Toolchain {
		return BuildInfo{}, ErrArtifact
	}
	settings := map[string]string{}
	for _, s := range info.Settings {
		if _, exists := settings[s.Key]; exists {
			return BuildInfo{}, ErrArtifact
		}
		settings[s.Key] = s.Value
	}
	if settings["GOOS"] != t.os || settings["GOARCH"] != t.arch || settings["CGO_ENABLED"] != "0" || settings["-trimpath"] != "true" {
		return BuildInfo{}, ErrArtifact
	}
	if t.arch == "amd64" && settings["GOAMD64"] != "v1" || t.arch == "arm64" && settings["GOARM64"] != "v8.0" {
		return BuildInfo{}, ErrArtifact
	}
	// Go intentionally omits recorded linker flags when trimpath is enabled.
	// The candidate version is the fixed recipe input, not an independently
	// extracted cross-target runtime observation. Native tests execute --version.
	if t.os == "darwin" {
		m, err := macho.NewFile(reader)
		if err != nil {
			return BuildInfo{}, ErrArtifact
		}
		defer m.Close()
		cpu := macho.CpuAmd64
		if t.arch == "arm64" {
			cpu = macho.CpuArm64
		}
		if m.Cpu != cpu || m.Type != macho.TypeExec {
			return BuildInfo{}, ErrArtifact
		}
	} else {
		e, err := elf.NewFile(reader)
		if err != nil {
			return BuildInfo{}, ErrArtifact
		}
		defer e.Close()
		machine := elf.EM_X86_64
		if t.arch == "arm64" {
			machine = elf.EM_AARCH64
		}
		if e.Machine != machine || e.Type != elf.ET_EXEC {
			return BuildInfo{}, ErrArtifact
		}
		for _, p := range e.Progs {
			if p.Type == elf.PT_INTERP {
				return BuildInfo{}, ErrArtifact
			}
		}
		libs, err := e.ImportedLibraries()
		if err != nil || len(libs) != 0 {
			return BuildInfo{}, ErrArtifact
		}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return BuildInfo{}, err
	}
	digest := sha256.New()
	n, err := copyBounded(ctx, digest, f, before.Size())
	if err != nil || n != before.Size() {
		return BuildInfo{}, errors.Join(ErrArtifact, err)
	}
	after, err := f.Stat()
	named, nameErr := os.Lstat(path)
	if err != nil || nameErr != nil || !sameFile(before, after) || !sameFile(before, named) {
		return BuildInfo{}, ErrArtifact
	}
	return BuildInfo{Contract: Contract, BuildMetadata: buildmetadata.FromBuildInfo(info, version), BinarySHA256: hex.EncodeToString(digest.Sum(nil)), BinaryBytes: n, ArtifactKind: "local_candidate"}, nil
}

func sameFile(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime() == b.ModTime()
}

func archive(ctx context.Context, path, binary string, info BuildInfo) (Artifact, error) {
	metadata, err := json.Marshal(info)
	if err != nil || len(metadata) > MetadataByteLimit {
		return Artifact{}, ErrArtifact
	}
	metadata = append(metadata, '\n')
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Artifact{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	digest := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(f, digest))
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	if err = tw.WriteHeader(&tar.Header{Name: "rydd", Mode: 0755, Size: info.BinaryBytes, Format: tar.FormatUSTAR}); err != nil {
		return Artifact{}, err
	}
	source, err := os.Open(binary)
	if err != nil {
		return Artifact{}, err
	}
	readDigest := sha256.New()
	n, copyErr := copyBounded(ctx, io.MultiWriter(tw, readDigest), source, info.BinaryBytes)
	closeErr := source.Close()
	if copyErr != nil || closeErr != nil || n != info.BinaryBytes || hex.EncodeToString(readDigest.Sum(nil)) != info.BinarySHA256 {
		return Artifact{}, errors.Join(ErrArtifact, copyErr, closeErr)
	}
	if err = tw.WriteHeader(&tar.Header{Name: "BUILDINFO.json", Mode: 0644, Size: int64(len(metadata)), Format: tar.FormatUSTAR}); err != nil {
		return Artifact{}, err
	}
	if _, err = tw.Write(metadata); err != nil {
		return Artifact{}, err
	}
	if err = tw.Close(); err != nil {
		return Artifact{}, err
	}
	if err = gz.Close(); err != nil {
		return Artifact{}, err
	}
	if err = ctx.Err(); err != nil {
		return Artifact{}, err
	}
	if err = f.Sync(); err != nil {
		return Artifact{}, err
	}
	st, err := f.Stat()
	if err != nil {
		return Artifact{}, err
	}
	if err = f.Close(); err != nil {
		return Artifact{}, err
	}
	closed = true
	return Artifact{Name: filepath.Base(path), SHA256: hex.EncodeToString(digest.Sum(nil)), Bytes: st.Size()}, nil
}

func copyBounded(ctx context.Context, out io.Writer, in io.Reader, limit int64) (int64, error) {
	var total int64
	buffer := make([]byte, 64<<10)
	for total < limit {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := in.Read(buffer[:min(int64(len(buffer)), limit-total)])
		if n > 0 {
			written, e := out.Write(buffer[:n])
			total += int64(written)
			if e != nil {
				return total, e
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF && total == limit {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	var extra [1]byte
	if err := ctx.Err(); err != nil {
		return total, err
	}
	n, err := in.Read(extra[:])
	if n != 0 || err != io.EOF {
		return total, ErrArtifact
	}
	return total, nil
}

func writeFile(ctx context.Context, path string, data []byte) error {
	if len(data) > MetadataByteLimit {
		return ErrArtifact
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(writeErr, syncErr, closeErr, ctx.Err())
}

func syncDirectory(path string, hook func(*os.File) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if hook == nil {
		hook = func(f *os.File) error { return f.Sync() }
	}
	err = hook(f)
	return errors.Join(err, f.Close())
}
