package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

const (
	FileHashContract            = "full_file_sha256_v1"
	FileHashStepByteLimit int64 = 1 << 20
	fileHashStepTimeout         = 5 * time.Second
)

// FileReadUsage survives errors and cancellation. RequestedBytes charges every
// issued read, including short or failed reads; ReadBytes counts returned bytes.
// These per-call figures are not a durable reservation or daily quota ledger.
type FileReadUsage struct {
	RequestedBytes int64         `json:"requested_bytes"`
	ReadBytes      int64         `json:"read_bytes"`
	Elapsed        time.Duration `json:"elapsed_ns"`
}

type FullHashProgress struct {
	Status                    string    `json:"status"`
	Source                    string    `json:"source"`
	Contract                  string    `json:"contract"`
	InventoryID               string    `json:"inventory_id"`
	RootID                    int64     `json:"root_id"`
	FileID                    int64     `json:"file_id"`
	PathBytes                 []byte    `json:"path_bytes"`
	LogicalBytes              int64     `json:"logical_bytes"`
	Offset                    int64     `json:"offset"`
	CheckedAt                 time.Time `json:"checked_at"`
	SHA256                    string    `json:"sha256,omitempty"`
	Code                      string    `json:"code,omitempty"`
	Message                   string    `json:"message"`
	ProvenanceVerified        bool      `json:"provenance_verified"`
	ContentVerified           bool      `json:"content_verified"`
	CurrentStateVerified      bool      `json:"current_state_verified"`
	DuplicatesVerified        bool      `json:"duplicates_verified"`
	Executable                bool      `json:"executable"`
	EstimatedReclaimableBytes *int64    `json:"estimated_reclaimable_bytes"`
}

// FullHashSession is an opaque, in-memory continuation. Copies of the wrapper
// share the same private state and serialize their steps. No descriptors remain
// open between steps. Private SHA state can retain buffered source bytes; it is
// never returned, serialized or persisted. Process loss loses this continuation.
type FullHashSession struct{ core *fullHashCore }

type fullHashCore struct {
	mu            sync.Mutex
	scanner       *Scanner
	target        SavedFileTarget
	checkpoint    fullHashCheckpoint
	checked       atomic.Pointer[FullHashProgress]
	invalidated   bool
	code, message string
}

type fullHashCheckpoint struct {
	digest             hash.Cloner
	offset             int64
	stamp              unix.Stat_t
	volume, mount      string
	baseline, complete bool
	checkedAt          time.Time
}

// Private seams support deterministic cancellation, path-change and soft-yield
// fixtures. A positive readPhaseLimit only shortens the normal soft window.
type fileHashHooks struct {
	afterOpen        func()
	afterRead        func(int)
	beforeFinalCheck func()
	readPhaseLimit   time.Duration
}

// NewFullHashSession freezes one exact saved selection without opening source
// paths. This explicit library request provides no provenance or read-consent
// verification. Production source reads need a separately approved exact scope.
func (s *Scanner) NewFullHashSession(target SavedFileTarget) (*FullHashSession, error) {
	if s == nil || s.closed.Load() {
		return nil, errors.New("scanner closed or unavailable")
	}
	if err := boundedSampleRequest([]SavedFileTarget{target}); err != nil {
		return nil, err
	}
	if err := validateSampleTarget(target); err != nil {
		return nil, err
	}
	digest, ok := sha256.New().(hash.Cloner)
	if !ok {
		return nil, errors.New("SHA-256 in-memory cloning is unsupported")
	}
	// Confirm the runtime can clone before a session can issue any source read.
	if _, err := digest.Clone(); err != nil {
		return nil, errors.New("SHA-256 in-memory cloning is unsupported")
	}
	target.Root.PathBytes = bytes.Clone(target.Root.PathBytes)
	target.File.PathBytes = bytes.Clone(target.File.PathBytes)
	target.File.Path = string(target.File.PathBytes)
	copy := append(target.Ancestors[:0:0], target.Ancestors...)
	for i := range copy {
		copy[i].Path = bytes.Clone(copy[i].Path)
	}
	target.Ancestors = copy
	core := &fullHashCore{scanner: s, target: target, checkpoint: fullHashCheckpoint{digest: digest}}
	core.publishCheckedProgress()
	return &FullHashSession{core: core}, nil
}

// Step reads sequentially from the last checked offset, with an allowance of
// 1–1 MiB, reads of at most 32 KiB and a cooperative five-second total deadline.
// Half the remaining time after opening is reserved for final path checks.
// Internal soft yields can publish checked partial progress; cancellation
// publishes no tentative progress or digest, while retaining read accounting.
// Changed or uncertain evidence invalidates the session. A completed call must
// revalidate the file again before returning its ordinary SHA256(file bytes).
// Metadata cannot establish one atomic snapshot across writers or slices.
func (s *FullHashSession) Step(ctx context.Context, allowance int64) (FullHashProgress, FileReadUsage, error) {
	return s.step(ctx, allowance, fileHashHooks{})
}

func (s *FullHashSession) step(ctx context.Context, allowance int64, hooks fileHashHooks) (progress FullHashProgress, usage FileReadUsage, err error) {
	started := time.Now()
	defer func() { usage.Elapsed = time.Since(started) }()
	if s == nil || s.core == nil {
		return progress, usage, errors.New("full hash session is uninitialized")
	}
	core := s.core
	if allowance < 1 || allowance > FileHashStepByteLimit {
		return core.checkedProgress(), usage, errors.New("full hash allowance must be 1–1048576 requested bytes")
	}
	ctx, cancel := context.WithTimeout(ctx, fileHashStepTimeout)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return core.checkedProgress(), usage, err
	}
	// Waiting never starts a goroutine and remains bounded by the same context
	// as filesystem work. A copied wrapper cannot fork the mutable hash state.
	for !core.mu.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return core.checkedProgress(), usage, ctx.Err()
		case <-timer.C:
		}
	}
	defer core.mu.Unlock()
	previous := core.checkpoint
	committed := false
	defer func() {
		// Cancellation after final checks still vetoes tentative publication.
		// Keep this check under the gate so another wrapper cannot see a state
		// which this call then rolls back.
		if canceled := ctx.Err(); canceled != nil {
			if committed {
				core.checkpoint = previous
			}
			progress, err = core.progress(false), canceled
		}
		// Waiters can read only this immutable, digest-free checked snapshot.
		// Publish after the cancellation veto and before releasing the gate.
		core.publishCheckedProgress()
	}()
	progress = core.progress(false)
	if err = ctx.Err(); err != nil {
		return progress, usage, err
	}
	if core.invalidated {
		return progress, usage, blocked(core.code, core.message)
	}
	if core.scanner.closed.Load() {
		err = blocked("scanner_closed", "The scanner closed before full-file hashing completed.")
		core.invalidate(err)
		return core.progress(false), usage, err
	}
	candidate := previous
	candidate.digest, err = previous.digest.Clone()
	if err != nil {
		err = blocked("hash_state_unsupported", "SHA-256 in-memory state could not be cloned. No digest is available.")
		core.invalidate(err)
		return core.progress(false), usage, err
	}
	err = core.scanner.withSavedRegularFile(ctx, core.target, func(fd int, opened unix.Stat_t, volume, mount string) error {
		if previous.baseline && (!sameInputStamp(previous.stamp, opened) || previous.volume != volume || previous.mount != mount) {
			return blocked("file_changed", "The file's identity, metadata, link count or mount differs from the checked continuation.")
		}
		candidate.stamp, candidate.volume, candidate.mount, candidate.baseline = opened, volume, mount, true
		if hooks.afterOpen != nil {
			hooks.afterOpen()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		deadline, _ := ctx.Deadline()
		window := max(time.Until(deadline)/2, 0)
		if hooks.readPhaseLimit > 0 {
			window = min(window, hooks.readPhaseLimit)
		}
		readUntil := time.Now().Add(window)
		var buffer [32 * 1024]byte
		for candidate.offset < opened.Size && usage.RequestedBytes < allowance {
			if err := ctx.Err(); err != nil {
				return err
			}
			if core.scanner.closed.Load() {
				return blocked("scanner_closed", "The scanner closed before full-file hashing completed.")
			}
			if !time.Now().Before(readUntil) {
				break
			}
			want := min(int64(len(buffer)), opened.Size-candidate.offset, allowance-usage.RequestedBytes)
			usage.RequestedBytes += want
			n, readErr := unix.Pread(fd, buffer[:int(want)], candidate.offset)
			if n > 0 {
				usage.ReadBytes += int64(n)
				_, _ = candidate.digest.Write(buffer[:n])
				candidate.offset += int64(n)
			}
			if hooks.afterRead != nil {
				hooks.afterRead(max(n, 0))
			}
			if readErr != nil || int64(n) != want {
				return blocked("hash_incomplete", "A selected byte range could not be read completely. The continuation is invalidated and no digest is available.")
			}
		}
		return ctx.Err()
	}, hooks.beforeFinalCheck)
	if ctx.Err() != nil {
		return core.progress(false), usage, ctx.Err()
	}
	if err != nil {
		core.invalidate(err)
		return core.progress(false), usage, err
	}
	candidate.complete = candidate.offset == core.target.File.Size
	candidate.checkedAt = time.Now().UTC()
	core.checkpoint, committed = candidate, true
	progress = core.progress(true)
	return progress, usage, nil
}

func (c *fullHashCore) invalidate(err error) {
	c.invalidated = true
	c.checkpoint.digest = nil
	c.code, c.message = "path_unavailable", "The selected file or path is unavailable or unsupported. The continuation is invalidated and no digest is available."
	var failure liveError
	if errors.As(err, &failure) {
		c.code, c.message = failure.code, failure.message
	}
}

func (c *fullHashCore) publishCheckedProgress() {
	progress := c.progress(false)
	c.checked.Store(&progress)
}

func (c *fullHashCore) checkedProgress() FullHashProgress {
	stored := c.checked.Load()
	if stored == nil {
		return FullHashProgress{}
	}
	progress := *stored
	progress.PathBytes = bytes.Clone(progress.PathBytes)
	return progress
}

func (c *fullHashCore) progress(publishDigest bool) FullHashProgress {
	p := FullHashProgress{Status: "partial", Source: "live_full_file_hash", Contract: FileHashContract, InventoryID: c.target.InventoryID, RootID: c.target.Root.ID, FileID: c.target.File.ID, PathBytes: bytes.Clone(c.target.File.PathBytes), LogicalBytes: c.target.File.Size, Offset: c.checkpoint.offset, CheckedAt: c.checkpoint.checkedAt, Message: "Checked progress is retained only in this process. No full-file digest is available yet."}
	if c.invalidated {
		p.Status, p.Code, p.Message = "invalidated", c.code, c.message
	} else if publishDigest && c.checkpoint.complete {
		p.Status, p.SHA256, p.Message = "hash_observed", hex.EncodeToString(c.checkpoint.digest.Sum(nil)), "All selected file bytes were read and path metadata was rechecked. This observation does not prove duplicate files, current contents or safe removal."
	}
	return p
}
