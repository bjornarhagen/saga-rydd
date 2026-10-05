package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/regeneration"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const (
	ManifestInputLimit = 256 * 1024
	LockInputLimit     = 2 * 1024 * 1024
)

type InputFileEvidence struct {
	Name   string `json:"name"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type InputEvidence struct {
	LockfileVersion int                 `json:"lockfile_version"`
	LockedPackages  int                 `json:"locked_packages"`
	Files           []InputFileEvidence `json:"files"`
}

type InspectionResult struct {
	FindingID string         `json:"finding_id"`
	Status    string         `json:"status"`
	Code      string         `json:"code,omitempty"`
	Message   string         `json:"message"`
	Inputs    *InputEvidence `json:"inputs,omitempty"`
}

type InspectionReport struct {
	Status                    string             `json:"status"`
	Source                    string             `json:"source"`
	CheckedAt                 time.Time          `json:"checked_at"`
	CurrentStateVerified      bool               `json:"current_state_verified"`
	Executable                bool               `json:"executable"`
	RegenerationVerified      bool               `json:"regeneration_verified"`
	DependencyContentsChecked bool               `json:"dependency_contents_checked"`
	LocalDependencyEdits      string             `json:"local_dependency_edits"`
	Targets                   []InspectionResult `json:"targets"`
}

// Inspect observes selected project inputs. Digests describe this request only;
// saved plans contain no lock-content baseline or verified regeneration proof.
func (s *Scanner) Inspect(ctx context.Context, targets []state.LiveTarget) (InspectionReport, error) {
	r := InspectionReport{Status: "inputs_observed", Source: "live_project_inputs", LocalDependencyEdits: "unknown", Targets: []InspectionResult{}}
	if len(targets) < 1 || len(targets) > state.PreviewTargetLimit {
		return InspectionReport{}, errors.New("input inspection requires 1–20 exact targets")
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return InspectionReport{}, err
		}
		item := InspectionResult{FindingID: target.Finding.ID, Status: "inputs_observed", Message: "The manifest and npm lock inputs fit the supported format during this check. Local dependency edits and successful reinstall remain unknown."}
		evidence, err := s.inspectTarget(ctx, target, nil)
		if err != nil {
			if ctx.Err() != nil {
				return InspectionReport{}, ctx.Err()
			}
			item.Status, r.Status = "blocked", "blocked"
			var failure liveError
			if errors.As(err, &failure) {
				item.Code, item.Message = failure.code, failure.message
			} else {
				item.Code, item.Message = "path_unavailable", "A selected path or project input is missing, inaccessible or unsupported. Review the folder."
			}
		} else {
			item.Inputs = &evidence
		}
		r.Targets = append(r.Targets, item)
	}
	r.CheckedAt = time.Now().UTC()
	return r, nil
}

// These names signal an unsupported install configuration. Only presence is
// checked; in particular, .npmrc may contain credentials and is never read.
var unsupportedInputHints = []string{
	"npm-shrinkwrap.json", "yarn.lock", "pnpm-lock.yaml", "bun.lock", "bun.lockb",
	".npmrc", "patches", ".yarn", "pnpm-workspace.yaml", "lerna.json", "binding.gyp",
}

type heldInput struct {
	file  *os.File
	name  string
	stamp unix.Stat_t
}

type inputReader struct {
	scanner *Scanner
	ctx     context.Context
	parent  int
	mount   string
	project string
	files   []heldInput
}

func (s *Scanner) inspectTarget(ctx context.Context, target state.LiveTarget, beforeRecheck func()) (InputEvidence, error) {
	reader := inputReader{scanner: s, ctx: ctx, project: filepath.Dir(string(target.Finding.PathBytes))}
	defer reader.close()
	var evidence InputEvidence
	err := s.verifyTargetWithVisit(ctx, target, func(parent int, mount string) error {
		reader.parent, reader.mount = parent, mount
		if err := reader.checkHints(); err != nil {
			return err
		}
		manifest, err := reader.read("package.json", ManifestInputLimit)
		if err != nil {
			return err
		}
		lock, err := reader.read("package-lock.json", LockInputLimit)
		if err != nil {
			return err
		}
		summary, err := regeneration.Analyze(manifest, lock)
		if err != nil {
			var failure regeneration.Error
			if errors.As(err, &failure) {
				return blocked(failure.Code, failure.Message)
			}
			return err
		}
		evidence = InputEvidence{LockfileVersion: summary.LockfileVersion, LockedPackages: summary.LockedPackages, Files: []InputFileEvidence{inputDigest("package.json", manifest), inputDigest("package-lock.json", lock)}}
		return nil
	}, reader.recheck, beforeRecheck)
	if err != nil {
		return InputEvidence{}, err
	}
	return evidence, nil
}

func inputDigest(name string, contents []byte) InputFileEvidence {
	digest := sha256.Sum256(contents)
	return InputFileEvidence{Name: name, Bytes: len(contents), SHA256: hex.EncodeToString(digest[:])}
}

func (r *inputReader) close() {
	for _, file := range r.files {
		_ = file.file.Close()
	}
}

func (r *inputReader) checkHints() error {
	for _, name := range unsupportedInputHints {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		var st unix.Stat_t
		err := unix.Fstatat(r.parent, name, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			return blocked("input_unsupported", "The project contains "+name+", which this input check does not support. Review its install configuration.")
		}
		if !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return nil
}

func (r *inputReader) read(name string, limit int) ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.scanner.excluded(filepath.Join(r.project, name)) {
		return nil, blocked("scope_excluded", "A required project input is currently excluded or protected.")
	}
	var before unix.Stat_t
	if err := unix.Fstatat(r.parent, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, blocked("input_missing", "The project needs package.json and package-lock.json for this supported input check.")
		}
		return nil, err
	}
	if !r.supportedFile(before) {
		return nil, blocked("input_unsupported", "A required input is a symlink, placeholder, protected object or unsupported file type.")
	}
	if before.Size > int64(limit) || before.Size < 0 {
		return nil, blocked("input_limit", "A project input exceeds the bounded read limit. Review it separately.")
	}
	if err := verifyNamedMount(r.parent, name, r.mount); err != nil {
		return nil, blocked("mount_boundary", "A project input crosses a mount boundary or lacks mount evidence.")
	}
	fd, err := unix.Openat(r.parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	r.files = append(r.files, heldInput{file: file, name: name, stamp: before})
	var opened unix.Stat_t
	if err = unix.Fstat(fd, &opened); err != nil {
		return nil, err
	}
	if !r.supportedFile(opened) || !sameInputStamp(before, opened) {
		return nil, blocked("input_changed_during_check", "A project input changed while opening. Review it and retry.")
	}
	v, m, err := r.scanner.filesystem(fd)
	if err != nil || v == "" || m != r.mount {
		return nil, blocked("mount_boundary", "An opened input crosses a mount boundary or lacks filesystem evidence.")
	}
	contents := make([]byte, 0, int(before.Size))
	var chunk [8192]byte
	for {
		if err = r.ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := file.Read(chunk[:min(len(chunk), limit+1-len(contents))])
		contents = append(contents, chunk[:n]...)
		if len(contents) > limit {
			return nil, blocked("input_limit", "A project input exceeded the bounded read limit during inspection.")
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if int64(len(contents)) != before.Size {
		return nil, blocked("input_changed_during_check", "A project input changed size during inspection.")
	}
	return contents, r.recheckFiles()
}

func sameInputStamp(a, b unix.Stat_t) bool {
	return sameStamp(a, b) && a.Mode == b.Mode && a.Size == b.Size && a.Nlink == b.Nlink
}

func (r *inputReader) supportedFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && !dataless(st) && !r.scanner.protectedIDs[objectID(st)]
}

func (r *inputReader) recheckFiles() error {
	for _, input := range r.files {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		var held, named unix.Stat_t
		if err := unix.Fstat(int(input.file.Fd()), &held); err != nil {
			return err
		}
		if err := unix.Fstatat(r.parent, input.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return blocked("input_changed_during_check", "A project input became unavailable during inspection.")
		}
		if !r.supportedFile(held) || !r.supportedFile(named) || !sameInputStamp(input.stamp, held) || !sameInputStamp(input.stamp, named) {
			return blocked("input_changed_during_check", "A project input or its named link changed during inspection.")
		}
		if err := verifyNamedMount(r.parent, input.name, r.mount); err != nil {
			return blocked("mount_boundary", "A project input mount changed during inspection.")
		}
	}
	return nil
}

func (r *inputReader) recheck() error {
	if err := r.recheckFiles(); err != nil {
		return err
	}
	return r.checkHints()
}
