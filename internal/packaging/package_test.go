package packaging

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
)

// Real binaries exercise the fixed build and parsers once; subsequent cases
// clone these generated cooperating build artifacts into fresh staging inputs.
func TestCandidatePackagesFreshFourTargetsAndPublication(t *testing.T) {
	provided := os.Getenv("RYDD_PACKAGE_TEST_BINARIES")
	if provided == "" && os.Getenv("RYDD_PACKAGE_BUILD_TEST") != "1" {
		t.Skip("genuine candidate binaries require explicit disposable build opt-in or generated four-target inputs")
	}
	if provided != "" && (!filepath.IsAbs(provided) || filepath.Clean(provided) != provided) {
		t.Fatal("generated input directory must be absolute and clean")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	version := "fixture-v1"
	pool := t.TempDir()
	binaries := map[target]string{}
	for _, target := range targets {
		dir := filepath.Join(pool, target.os+"-"+target.arch)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		binary := filepath.Join(dir, "rydd")
		if provided == "" {
			if err := compile(context.Background(), root, binary, target, version); err != nil {
				t.Fatalf("compile %s/%s: %v", target.os, target.arch, err)
			}
		} else if err := copyFixture(filepath.Join(provided, "rydd-"+target.os+"-"+target.arch), binary); err != nil {
			t.Fatal(err)
		}
		if _, err := inspect(context.Background(), binary, target, version); err != nil {
			t.Fatalf("inspect %s/%s: %v", target.os, target.arch, err)
		}
		binaries[target] = binary
	}
	t.Run("native_linked_version_agrees_with_recipe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binaries[target{runtime.GOOS, runtime.GOARCH}], "--json", "--version")
		cmd.Env = []string{}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.WaitDelay = time.Second
		output := &boundedOutput{limit: MetadataByteLimit}
		cmd.Stdout = output
		cmd.Stderr = output
		if err := cmd.Run(); err != nil || output.overflowed() {
			t.Fatal(err)
		}
		var reply struct {
			Version  string               `json:"version"`
			Metadata buildmetadata.Record `json:"build_metadata"`
		}
		if err := json.Unmarshal(output.buffer.Bytes(), &reply); err != nil || reply.Version != version || reply.Metadata.Version != version || reply.Metadata.VersionSource != "linked_value" || !reply.Metadata.ExecutableLabelVerified {
			t.Fatal(reply, err)
		}
	})
	copyBuild := func(_ context.Context, _ string, to string, target target, _ string) error {
		return copyFixture(binaries[target], to)
	}
	newRepo := func() string {
		p := filepath.Join(t.TempDir(), "repository")
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Run("deterministic_flat_archives_and_no_stale_inputs", func(t *testing.T) {
		var first Result
		for iteration := 0; iteration < 2; iteration++ {
			repo := newRepo()
			if err := os.Mkdir(filepath.Join(repo, "dist"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, "dist", "rydd-linux-amd64"), []byte("stale-canary"), 0600); err != nil {
				t.Fatal(err)
			}
			calls := []target{}
			r, err := build(context.Background(), repo, version, hooks{build: func(ctx context.Context, r, b string, t target, v string) error {
				calls = append(calls, t)
				return copyBuild(ctx, r, b, t, v)
			}})
			if err != nil || !r.RenameCompleted || !r.SyncCompleted || !r.PublicationAttempted || r.ReleaseAccepted || !reflect.DeepEqual(calls, targets) {
				t.Fatal(r, err, calls)
			}
			checkCandidate(t, filepath.Join(repo, r.Directory), r)
			if iteration == 0 {
				first = r
			} else if !reflect.DeepEqual(first, r) {
				t.Fatal("same fixed bytes produced different archives", first, r)
			}
			if _, err := build(context.Background(), repo, version, hooks{build: copyBuild}); !errors.Is(err, ErrExists) {
				t.Fatal(err)
			}
		}
	})
	t.Run("failed_third_build_has_no_final_set", func(t *testing.T) {
		repo := newRepo()
		calls := 0
		r, err := build(context.Background(), repo, version, hooks{build: func(ctx context.Context, r, b string, target target, v string) error {
			calls++
			if calls == 3 {
				return ErrBuild
			}
			return copyBuild(ctx, r, b, target, v)
		}})
		if !errors.Is(err, ErrBuild) || r.PublicationAttempted || calls != 3 {
			t.Fatal(r, err, calls)
		}
		assertAbsent(t, filepath.Join(repo, "dist", "candidates", version))
		assertAbsent(t, filepath.Join(repo, "dist", "candidates", ".staging"))
	})
	t.Run("wrong_target_and_mutated_binary_refuse", func(t *testing.T) {
		for _, wrong := range []bool{true, false} {
			repo := newRepo()
			h := hooks{build: copyBuild}
			h.beforeArchive = func(i int, path string) {
				if i != 0 {
					return
				}
				if wrong {
					if err := copyFixture(binaries[targets[2]], path); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.WriteFile(path, []byte("not-a-build"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := build(context.Background(), repo, version, h)
			if !errors.Is(err, ErrArtifact) || r.PublicationAttempted {
				t.Fatal(r, err)
			}
			assertAbsent(t, filepath.Join(repo, "dist", "candidates", version))
		}
	})
	t.Run("cancel_before_publication_has_no_final_set", func(t *testing.T) {
		repo := newRepo()
		ctx, cancel := context.WithCancel(context.Background())
		r, err := build(ctx, repo, version, hooks{build: copyBuild, beforeArchive: func(i int, _ string) {
			if i == 1 {
				cancel()
			}
		}})
		if !errors.Is(err, context.Canceled) || r.PublicationAttempted {
			t.Fatal(r, err)
		}
		assertAbsent(t, filepath.Join(repo, "dist", "candidates", version))
		assertAbsent(t, filepath.Join(repo, "dist", "candidates", ".staging"))
	})
	t.Run("exclusive_destination_and_uncertain_rename_retains_staging", func(t *testing.T) {
		repo := newRepo()
		r, err := build(context.Background(), repo, version, hooks{build: copyBuild, publish: func(old, next string) error {
			if err := os.Mkdir(next, 0700); err != nil {
				return err
			}
			return publishExclusive(old, next)
		}})
		if !errors.Is(err, ErrPublication) || !r.PublicationAttempted || r.RenameCompleted || r.SyncCompleted {
			t.Fatal(r, err)
		}
		entries, _ := os.ReadDir(filepath.Join(repo, "dist", "candidates", version))
		if len(entries) != 0 {
			t.Fatal("existing destination was overwritten")
		}
		if _, err := os.Stat(filepath.Join(repo, "dist", "candidates", ".staging", "SHA256SUMS")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("post_rename_cancel_or_sync_failure_retains_complete_set", func(t *testing.T) {
		for _, cancelReply := range []bool{true, false} {
			repo := newRepo()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := hooks{build: copyBuild}
			if cancelReply {
				h.afterPublish = cancel
			} else {
				h.sync = func(f *os.File) error {
					if filepath.Base(f.Name()) == "candidates" {
						return io.ErrUnexpectedEOF
					}
					return f.Sync()
				}
			}
			r, err := build(ctx, repo, version, h)
			if !errors.Is(err, ErrPublication) || !r.RenameCompleted || r.SyncCompleted || r.ReleaseAccepted {
				t.Fatal(r, err)
			}
			checkCandidate(t, filepath.Join(repo, r.Directory), r)
		}
	})
	t.Run("failed_directory_sync_is_repeated_before_retry", func(t *testing.T) {
		repo := newRepo()
		if _, err := build(context.Background(), repo, version, hooks{build: copyBuild, sync: func(f *os.File) error {
			if f.Name() == repo {
				return io.ErrUnexpectedEOF
			}
			return f.Sync()
		}}); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
		assertAbsent(t, filepath.Join(repo, "dist", "candidates", version))
		parents := []string{}
		r, err := build(context.Background(), repo, version, hooks{build: copyBuild, sync: func(f *os.File) error { parents = append(parents, f.Name()); return f.Sync() }})
		if err != nil || !r.SyncCompleted || len(parents) != 4 || parents[0] != repo || parents[1] != filepath.Join(repo, "dist") {
			t.Fatal(r, err, parents)
		}
		checkCandidate(t, filepath.Join(repo, r.Directory), r)
	})
}

func checkCandidate(t *testing.T, path string, result Result) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 5 {
		t.Fatal(entries, err)
	}
	var sums strings.Builder
	for i, a := range result.Archives {
		if a.Name != "rydd-"+result.Version+"-"+targets[i].os+"-"+targets[i].arch+".tar.gz" {
			t.Fatal(a)
		}
		f, err := os.Open(filepath.Join(path, a.Name))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(f, BinaryByteLimit+MetadataByteLimit+1<<20))
		f.Close()
		if err != nil || n != a.Bytes || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
			t.Fatal(a, err)
		}
		fmt.Fprintf(&sums, "%s  %s\n", a.SHA256, a.Name)
		f, err = os.Open(filepath.Join(path, a.Name))
		if err != nil {
			t.Fatal(err)
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		if !gz.ModTime.IsZero() || gz.Name != "" || gz.Comment != "" || len(gz.Extra) != 0 || gz.OS != 255 {
			t.Fatal(gz.Header)
		}
		tr := tar.NewReader(gz)
		binary, err := tr.Next()
		if err != nil || binary.Name != "rydd" || binary.Mode != 0755 || binary.Size <= 0 || binary.Size > BinaryByteLimit || binary.Uid != 0 || binary.Gid != 0 || !binary.ModTime.Equal(time.Unix(0, 0)) {
			t.Fatal(binary, err)
		}
		digest := sha256.New()
		if _, err := io.Copy(digest, io.LimitReader(tr, BinaryByteLimit+1)); err != nil {
			t.Fatal(err)
		}
		metadata, err := tr.Next()
		if err != nil || metadata.Name != "BUILDINFO.json" || metadata.Mode != 0644 || metadata.Size > MetadataByteLimit || metadata.Uid != 0 || metadata.Gid != 0 {
			t.Fatal(metadata, err)
		}
		body, err := io.ReadAll(io.LimitReader(tr, MetadataByteLimit+1))
		if err != nil {
			t.Fatal(err)
		}
		var info BuildInfo
		if err := json.Unmarshal(body, &info); err != nil {
			t.Fatal(err)
		}
		if info.BinarySHA256 != hex.EncodeToString(digest.Sum(nil)) || info.BinaryBytes != binary.Size || info.BuildMetadata.Version != result.Version || info.BuildMetadata.VersionSource != "candidate_recipe" || info.BuildMetadata.ExecutableLabelVerified || info.BuildMetadata.GOOS != targets[i].os || info.BuildMetadata.GOARCH != targets[i].arch || info.SourceAuthenticated || info.ReleaseAccepted || info.NativeCompatibilityVerified || info.DistributionSigned || info.ArtifactKind != "local_candidate" {
			t.Fatal(info)
		}
		if _, err := tr.Next(); err != io.EOF {
			t.Fatal("unexpected archive member", err)
		}
		gz.Close()
		f.Close()
	}
	got, err := os.ReadFile(filepath.Join(path, "SHA256SUMS"))
	if err != nil || string(got) != sums.String() {
		t.Fatal(string(got), err)
	}
}

func copyFixture(from, to string) error {
	f, err := os.Open(from)
	if err != nil {
		return err
	}
	defer f.Close()
	g, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		return err
	}
	_, err = io.Copy(g, f)
	return errors.Join(err, g.Close())
}
func assertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(path, err)
	}
}

func TestCandidateInputBoundsAndInterruptedStagingRefuse(t *testing.T) {
	for _, v := range []string{"../x", ".", "..", strings.Repeat("x", 65), "a b"} {
		if _, err := Build(context.Background(), t.TempDir(), v); !errors.Is(err, ErrInput) {
			t.Fatal(v, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := t.TempDir()
	if _, err := Build(ctx, repo, "v1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertAbsent(t, filepath.Join(repo, "dist"))
	for _, slot := range []string{".staging", "v1"} {
		repo := t.TempDir()
		p := filepath.Join(repo, "dist", "candidates")
		if err := os.MkdirAll(filepath.Join(p, slot), 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := Build(context.Background(), repo, "v1"); !errors.Is(err, ErrExists) {
			t.Fatal(slot, err)
		}
	}
	repo = t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(repo, "dist")); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), repo, "v1"); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	assertAbsent(t, filepath.Join(outside, "candidates"))
}

func TestCandidateBoundedStreamRejectsShortExtraAndCanceledInput(t *testing.T) {
	for _, v := range []struct {
		body  string
		limit int64
	}{{"x", 2}, {"xxx", 2}} {
		if _, err := copyBounded(context.Background(), io.Discard, strings.NewReader(v.body), v.limit); err == nil {
			t.Fatal(v)
		}
	}
	if _, err := copyBounded(context.Background(), shortWriter{}, strings.NewReader("abc"), 3); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, err := copyBounded(ctx, io.Discard, bytes.NewReader([]byte("abc")), 3); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal(n, err)
	}
	path := filepath.Join(t.TempDir(), "large")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(BinaryByteLimit + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := inspect(context.Background(), path, targets[0], "v1"); !errors.Is(err, ErrArtifact) {
		t.Fatal(err)
	}
}

func TestCandidateArchiveDeterminismAndChangedInputRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary")
	body := []byte(strings.Repeat("generated-build-artifact", 4096))
	if err := os.WriteFile(path, body, 0700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	info := BuildInfo{Contract: Contract, BuildMetadata: buildmetadata.FromBuildInfo(nil, "fixture-v1"), BinarySHA256: hex.EncodeToString(digest[:]), BinaryBytes: int64(len(body)), ArtifactKind: "local_candidate"}
	var first []byte
	for i := 0; i < 2; i++ {
		archivePath := filepath.Join(t.TempDir(), "fixture.tar.gz")
		a, err := archive(context.Background(), archivePath, path, info)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(archivePath)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(got)) != a.Bytes {
			t.Fatal(a)
		}
		if i == 0 {
			first = got
		} else if !bytes.Equal(first, got) {
			t.Fatal("archive depended on path or time")
		}
	}
	body[0] ^= 1
	if err := os.WriteFile(path, body, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := archive(context.Background(), filepath.Join(t.TempDir(), "changed.tar.gz"), path, info); !errors.Is(err, ErrArtifact) {
		t.Fatal(err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
