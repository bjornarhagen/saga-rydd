package inventory

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var ErrAPIPacingInput = errors.New("invalid bounded scanner API pacing input")
var ErrAPIPacingProfile = errors.New("path is outside the bounded scanner API pacing profile")

const pacedAPIPathBytes = 4096
const pacedAPIComponents = pacedAPIPathBytes / 2

// Linux performs the larger number of guarded calls: three per absolute
// component, five per relative component, and six for a directory child.
// Both supported platforms use these conservative bounds. An API attempt can
// perform multiple syscalls; these are neither physical I/O nor latency bounds.
const pacedAPIFinalValidation int64 = 3*pacedAPIComponents + 5*pacedAPIComponents + 9
const pacedAPIChild int64 = 6
const pacedAPIMinimumNext int64 = 6*pacedAPIComponents + 10*pacedAPIComponents + 26
const pacedAPIMaximumNext int64 = pacedAPIMinimumNext + (state.MaxBatchEntries-1)*pacedAPIChild

// APIPacingBounds describes finite call-count capacity, not permission or a
// measured execution time. Startup is needed only while constructing a scanner.
// MinimumNext includes setup, one worst-case child and full final validation;
// MaximumNext includes all 128 worst-case children. Earlier failure uses fewer
// attempts. Unavailable/slow filesystems still have cooperative latency limits.
type APIPacingBounds struct {
	Startup         int64
	MinimumNext     int64
	MaximumNext     int64
	FinalValidation int64
	Entry           int64
}

// APIPacingWorkBounds performs no filesystem access or callbacks. Canonical
// roots are not known here: the paced methods enforce the same 4096-byte ceiling
// after resolution, before opening their components. Relative jobs share that
// ceiling. A canonical alias outside it is an explicit profile refusal.
func APIPacingWorkBounds(roots, excludes, privatePaths []string) (APIPacingBounds, error) {
	startup, err := StartupAPIAttemptAllowance(roots, excludes, privatePaths)
	if err != nil {
		return APIPacingBounds{}, errors.Join(ErrAPIPacingInput, err)
	}
	for _, root := range roots {
		if !pacedAbsolutePath(root) {
			return APIPacingBounds{}, ErrAPIPacingInput
		}
	}
	return APIPacingBounds{Startup: startup, MinimumNext: pacedAPIMinimumNext,
		MaximumNext: pacedAPIMaximumNext, FinalValidation: pacedAPIFinalValidation, Entry: pacedAPIChild}, nil
}

func pacedAbsolutePath(path string) bool {
	return len(path) > 0 && len(path) <= pacedAPIPathBytes && filepath.IsAbs(path) &&
		filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
}

func (g *apiGuard) pacedRoot(path string) error {
	if g != nil && g.paced && !pacedAbsolutePath(path) {
		return deniedAPI(ErrAPIPacingProfile)
	}
	return nil
}

// APIEntryCapacity performs a non-consuming admission check for the requested
// number of future calls. False means ordinary rate yielding before this child:
// pending names remain unread and final validation still runs. Errors deny the
// entire tentative batch. The caller must retain clock observations and check
// cancellation/deadlines without counting an API that has not been attempted.
// This callback is operation-local and is never retained by the scanner.
type APIEntryCapacity func(context.Context, int64) (bool, error)

// NewPermittedPaced adds the finite path profile to NewPermitted. The caller
// must check spacing/work capacity before reserving or starting source work;
// this method provides bounds and per-call admission, not a rate controller.
func NewPermittedPaced(ctx context.Context, roots, excludes, privatePaths []string, permit APIPermit, options ...Option) (*Scanner, error) {
	if _, err := APIPacingWorkBounds(roots, excludes, privatePaths); err != nil {
		return nil, err
	}
	if ctx == nil || permit == nil {
		return nil, deniedAPI(errors.New("context and permit are required"))
	}
	return newScanner(roots, excludes, privatePaths, &apiGuard{ctx: ctx, permit: permit, paced: true}, options...)
}

// NextPermittedPaced adds a clean entry-boundary yield to NextPermitted. Every
// API, including final validation, still needs permit admission. Unsupported
// paths and callback errors return no tentative evidence and reset only the
// selected stream. Neither yielding nor a positive result ensures eventual
// completion under slow calls, mutation, repeated interruption or insufficient
// caller capacity. Ordinary Next/NextPermitted and live hash methods are unchanged.
func (s *Scanner) NextPermittedPaced(ctx context.Context, job state.Job, permit APIPermit, capacity APIEntryCapacity) (state.ScanBatch, error) {
	if ctx == nil || permit == nil || capacity == nil {
		return s.denyPacedNext(job, errors.New("context, permit and entry capacity are required"))
	}
	if !pacedAbsolutePath(string(job.RootPath)) || !validPath(string(job.Path)) {
		return s.denyPacedNext(job, ErrAPIPacingProfile)
	}
	job.RootPath = bytes.Clone(job.RootPath)
	job.Path = bytes.Clone(job.Path)
	job.Cursor = bytes.Clone(job.Cursor)
	return s.next(ctx, job, &apiGuard{ctx: ctx, permit: permit, paced: true, capacity: capacity})
}

func (s *Scanner) denyPacedNext(job state.Job, err error) (state.ScanBatch, error) {
	s.mu.Lock()
	s.selectRoot(string(job.RootPath))
	s.reset()
	s.mu.Unlock()
	return state.ScanBatch{}, deniedAPI(err)
}
