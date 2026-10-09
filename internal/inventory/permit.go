package inventory

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// APICallKind names one scanner API attempt, not the syscalls or physical I/O
// that the API may perform internally.
type APICallKind string

const (
	APIStat                APICallKind = "stat"
	APIDirectoryOpen       APICallKind = "directory_open"
	APIDirectoryRead       APICallKind = "directory_read"
	APIFilesystemStat      APICallKind = "filesystem_stat"
	APIMountIdentity       APICallKind = "mount_identity"
	APIPathResolution      APICallKind = "path_resolution"
	MaxAPIAttemptAllowance int64       = 65536
)

// APIPermit admits exactly one attempt when it returns nil. The caller owns
// reservation, expiry and usage accounting; this callback supplies no durable
// quota or authority by itself. It must return any cancellation it observes.
// An admitted attempt is counted immediately before calling the API, even when
// that API fails. Cancellation cannot be atomic with entering a kernel call.
type APIPermit func(context.Context, APICallKind) error

var ErrAPIPermitDenied = errors.New("scanner API attempt was not permitted")

// StartupAPIAttemptAllowance bounds the calls made by construction, including
// failed calls. Eleven fixed protected paths and one optional home-Library path
// each need a stat and resolution attempt; each root needs at most two calls.
// The optional slot is reserved even when home lookup fails. Unused allowance
// is not a refund; settlement belongs to the caller's durable budget.
func StartupAPIAttemptAllowance(roots, excludes, privatePaths []string) (int64, error) {
	const fixed = 12
	limit := MaxAPIAttemptAllowance / 2
	total := int64(fixed)
	for _, count := range []int{len(roots), len(excludes), len(privatePaths)} {
		if int64(count) > limit-total {
			return 0, errors.New("scanner startup allowance exceeds 65536 API attempts")
		}
		total += int64(count)
	}
	return 2 * total, nil
}

type apiGuard struct {
	ctx    context.Context
	permit APIPermit
}

func deniedAPI(err error) error {
	return fmt.Errorf("%w: %w", ErrAPIPermitDenied, err)
}

func (g *apiGuard) check(s *Scanner) error {
	if g == nil {
		return nil
	}
	if err := context.Cause(g.ctx); err != nil {
		return deniedAPI(errors.Join(g.ctx.Err(), err))
	}
	if s.closed.Load() {
		return deniedAPI(errors.New("scanner closed"))
	}
	return nil
}

func (g *apiGuard) before(s *Scanner, kind APICallKind) error {
	if g == nil {
		return nil
	}
	if err := g.check(s); err != nil {
		return err
	}
	if err := g.permit(g.ctx, kind); err != nil {
		return deniedAPI(err)
	}
	return nil
}

// NewPermitted constructs a scanner only after every source API attempt is
// admitted. Inputs are frozen before callbacks. No partially constructed
// scanner escapes on denial; ordinary source errors retain New's behavior.
func NewPermitted(ctx context.Context, roots, excludes, privatePaths []string, permit APIPermit, options ...Option) (*Scanner, error) {
	if _, err := StartupAPIAttemptAllowance(roots, excludes, privatePaths); err != nil {
		return nil, err
	}
	if ctx == nil || permit == nil {
		return nil, deniedAPI(errors.New("context and permit are required"))
	}
	return newScanner(roots, excludes, privatePaths, &apiGuard{ctx: ctx, permit: permit}, options...)
}

// NextPermitted bounds only this operation's source API attempts. The callback
// is never retained on Scanner or used by unrelated live observations. A denied
// operation returns no tentative batch, fault, skip or completion evidence and
// discards only the selected root's stream; the caller retains its previously saved job cursor.
// A small allowance may prevent progress on deep paths. This API guarantees no
// eventual progress for every depth, and does not meter closes or database work.
func (s *Scanner) NextPermitted(ctx context.Context, job state.Job, permit APIPermit) (state.ScanBatch, error) {
	if ctx == nil || permit == nil {
		s.mu.Lock()
		s.selectRoot(string(job.RootPath))
		s.reset()
		s.mu.Unlock()
		return state.ScanBatch{}, deniedAPI(errors.New("context and permit are required"))
	}
	job.RootPath = bytes.Clone(job.RootPath)
	job.Path = bytes.Clone(job.Path)
	job.Cursor = bytes.Clone(job.Cursor)
	return s.next(ctx, job, &apiGuard{ctx: ctx, permit: permit})
}
