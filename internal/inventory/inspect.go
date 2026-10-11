package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
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
	Tree      *TreeEvidence  `json:"tree,omitempty"`
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
	return s.inspect(ctx, targets, false)
}

// InspectTree additionally observes a bounded installed layout. Neither method
// reads ordinary dependency contents or establishes that they are unmodified.
func (s *Scanner) InspectTree(ctx context.Context, targets []state.LiveTarget) (InspectionReport, error) {
	return s.inspect(ctx, targets, true)
}

// InspectWithUsage preserves input inspection and separately returns every
// issued input-file read allowance and actual returned byte, including failures.
// This is per-call diagnostics, not a durable quota or source-read authority.
func (s *Scanner) InspectWithUsage(ctx context.Context, targets []state.LiveTarget) (InspectionReport, FileReadUsage, error) {
	return s.inspectWithUsage(ctx, targets, false, inspectUsageHooks{})
}

// InspectTreeWithUsage counts only project input-file Read attempts. Directory
// listings and link text are excluded, and dependency contents stay unchecked.
func (s *Scanner) InspectTreeWithUsage(ctx context.Context, targets []state.LiveTarget) (InspectionReport, FileReadUsage, error) {
	return s.inspectWithUsage(ctx, targets, true, inspectUsageHooks{})
}

// Hooks are private per-call generated-fixture observers. They never supply
// successful source payloads or survive a call; public entry points use nil hooks.
type inspectUsageHooks struct {
	beforeRead        func(*os.File, int)
	afterRead         func(int, int)
	beforeRecheck     func()
	beforeFinalReturn func()
}

func (s *Scanner) inspectWithUsage(ctx context.Context, targets []state.LiveTarget, tree bool, hooks inspectUsageHooks) (report InspectionReport, usage FileReadUsage, err error) {
	started := time.Now()
	defer func() {
		// Nested input/path/tree descriptor close attempts have already run.
		// Their legacy ignored close errors remain unchanged.
		if hooks.beforeFinalReturn != nil {
			hooks.beforeFinalReturn()
		}
		if ended := ctx.Err(); ended != nil {
			report = InspectionReport{}
			err = errors.Join(err, ended)
		}
		usage.Elapsed = time.Since(started)
	}()
	report, err = s.inspectCore(ctx, targets, tree, &usage, hooks)
	return report, usage, err
}

func (s *Scanner) inspect(ctx context.Context, targets []state.LiveTarget, tree bool) (InspectionReport, error) {
	return s.inspectCore(ctx, targets, tree, nil, inspectUsageHooks{})
}

func (s *Scanner) inspectCore(ctx context.Context, targets []state.LiveTarget, tree bool, usage *FileReadUsage, hooks inspectUsageHooks) (InspectionReport, error) {
	r := InspectionReport{Status: "inputs_observed", Source: "live_project_inputs", LocalDependencyEdits: "unknown", Targets: []InspectionResult{}}
	status := r.Status
	if tree {
		status, r.Status, r.Source = "inputs_and_tree_observed", "inputs_and_tree_observed", "live_project_inputs_and_tree_metadata"
	}
	if len(targets) < 1 || len(targets) > state.PreviewTargetLimit {
		return InspectionReport{}, errors.New("input inspection requires 1–20 exact targets")
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return InspectionReport{}, err
		}
		item := InspectionResult{FindingID: target.Finding.ID, Status: status, Message: "The manifest and npm lock inputs fit the supported format during this check. Local dependency edits and successful reinstall remain unknown."}
		if tree {
			item.Message = "The project inputs and observed dependency layout fit the supported format during this check. File contents, local edits and successful reinstall remain unverified."
		}
		evidence, treeEvidence, err := s.inspectTargetWithTreeUsage(ctx, target, tree, hooks.beforeRecheck, usage, hooks)
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
			item.Tree = treeEvidence
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
	usage   *FileReadUsage
	hooks   inspectUsageHooks
}

func (s *Scanner) inspectTarget(ctx context.Context, target state.LiveTarget, beforeRecheck func()) (InputEvidence, error) {
	evidence, _, err := s.inspectTargetWithTree(ctx, target, false, beforeRecheck)
	return evidence, err
}

func (s *Scanner) inspectTargetWithTree(ctx context.Context, target state.LiveTarget, tree bool, beforeRecheck func()) (InputEvidence, *TreeEvidence, error) {
	return s.inspectTargetWithTreeUsage(ctx, target, tree, beforeRecheck, nil, inspectUsageHooks{})
}

func (s *Scanner) inspectTargetWithTreeUsage(ctx context.Context, target state.LiveTarget, tree bool, beforeRecheck func(), usage *FileReadUsage, hooks inspectUsageHooks) (InputEvidence, *TreeEvidence, error) {
	reader := inputReader{scanner: s, ctx: ctx, project: filepath.Dir(string(target.Finding.PathBytes)), usage: usage, hooks: hooks}
	defer reader.close()
	var evidence InputEvidence
	var treeEvidence *TreeEvidence
	var treeCheck func() error
	err := s.verifyTargetWithVisit(ctx, target, func(parent, targetFD int, mount string) error {
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
		var summary regeneration.Summary
		var layout regeneration.Layout
		if tree {
			summary, layout, err = regeneration.AnalyzeLayout(manifest, lock)
		} else {
			summary, err = regeneration.Analyze(manifest, lock)
		}
		if err != nil {
			var failure regeneration.Error
			if errors.As(err, &failure) {
				return blocked(failure.Code, failure.Message)
			}
			return err
		}
		evidence = InputEvidence{LockfileVersion: summary.LockfileVersion, LockedPackages: summary.LockedPackages, Files: []InputFileEvidence{inputDigest("package.json", manifest), inputDigest("package-lock.json", lock)}}
		if tree {
			walker := newTreeReader(s, ctx, targetFD, string(target.Finding.PathBytes), mount, layout)
			observed, err := walker.observe()
			if err != nil {
				return err
			}
			treeEvidence = &observed
			treeCheck = walker.recheck
		}
		return nil
	}, func() error {
		if treeCheck != nil {
			if err := treeCheck(); err != nil {
				return err
			}
		}
		return reader.recheck()
	}, beforeRecheck)
	if err != nil {
		return InputEvidence{}, nil, err
	}
	return evidence, treeEvidence, nil
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
		want := min(len(chunk), limit+1-len(contents))
		if r.usage != nil && !addInspectionUsage(&r.usage.RequestedBytes, int64(want)) {
			return nil, blocked("input_usage_limit", "Input read accounting exceeded its fixed integer bound.")
		}
		if r.hooks.beforeRead != nil {
			r.hooks.beforeRead(file, want)
		}
		n, readErr := file.Read(chunk[:want])
		if r.usage != nil && (n < 0 || n > want || !addInspectionUsage(&r.usage.ReadBytes, int64(n))) {
			return nil, blocked("input_usage_limit", "Input read accounting exceeded its fixed integer bound.")
		}
		if r.hooks.afterRead != nil {
			r.hooks.afterRead(want, n)
		}
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

func addInspectionUsage(counter *int64, amount int64) bool {
	if amount < 0 || *counter < 0 || amount > math.MaxInt64-*counter {
		return false
	}
	*counter += amount
	return true
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
