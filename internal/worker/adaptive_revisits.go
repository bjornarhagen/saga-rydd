package worker

import (
	"context"
	"path/filepath"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// AdaptiveRevisitSnapshot is a cached, bounded summary of saved scheduling
// evidence. It has no paths, per-root history or current-content claim.
type AdaptiveRevisitSnapshot struct {
	PolicyEnabled bool      `json:"policy_enabled"`
	CachedAt      time.Time `json:"cached_at"`
	state.AdaptiveRevisitStatus
}

type adaptiveRevisitPolicy struct {
	enabled bool
	scope   state.AdaptiveRevisitScope
	rootIDs []int64
	startup int
	cached  *AdaptiveRevisitSnapshot
}

// These lexical protections match the scanner's fixed detailed/skip profile.
// A scanner policy change must also revise the adaptive scope profile.
func adaptiveRevisitDigest(roots, excludes, privatePaths []string, home string) (string, error) {
	clean := func(paths []string) []string {
		result := make([]string, len(paths))
		for i, path := range paths {
			result[i] = filepath.Clean(path)
		}
		return result
	}
	protected := append(clean(privatePaths), "/proc", "/sys", "/dev", "/run", "/System", "/Library", "/usr", "/bin", "/sbin", "/etc", "/private/etc")
	if home != "" {
		protected = append(protected, filepath.Join(home, "Library"))
	}
	return state.AdaptiveRevisitScopeDigest(clean(roots), clean(excludes), protected)
}

func (p *adaptiveRevisitPolicy) configure(ctx context.Context, w *state.Store, roots state.FairInventoryRoots, enabled bool, digest string, now time.Time) error {
	scope, err := w.ConfigureAdaptiveRevisits(ctx, roots, enabled, digest, now)
	if err != nil {
		return err
	}
	p.enabled, p.scope = enabled, scope
	if enabled {
		p.rootIDs = scope.RootIDs()
	}
	return p.refresh(ctx, w, now)
}

func (p *adaptiveRevisitPolicy) refresh(ctx context.Context, w *state.Store, now time.Time) error {
	if !p.enabled {
		return nil
	}
	status, err := w.AdaptiveRevisitStatus(ctx, p.scope)
	if err != nil {
		return err
	}
	p.cached = &AdaptiveRevisitSnapshot{PolicyEnabled: true, CachedAt: now.Round(0).UTC(), AdaptiveRevisitStatus: status}
	return nil
}

func (p *adaptiveRevisitPolicy) snapshot() *AdaptiveRevisitSnapshot {
	if p.cached == nil {
		return nil
	}
	copy := *p.cached
	return &copy
}

func (p *adaptiveRevisitPolicy) startupPending() bool {
	return p.enabled && p.startup < len(p.rootIDs)
}

// An initial drained root is seeded without learning. Existing unfinished or
// future work stays ahead of seeding. One exact root per turn yields controls.
func (p *adaptiveRevisitPolicy) initializeNext(ctx context.Context, w *state.Store, now time.Time) error {
	if !p.startupPending() {
		return nil
	}
	if _, err := w.FinalizeAdaptiveRevisit(ctx, p.scope, p.rootIDs[p.startup], now); err != nil {
		return err
	}
	p.startup++
	return p.refresh(ctx, w, now)
}

func (p *adaptiveRevisitPolicy) finalize(ctx context.Context, w *state.Store, root int64, now time.Time, fixedInterval time.Duration) error {
	if !p.enabled {
		_, err := w.ScheduleInventoryRevisit(ctx, root, now, fixedInterval)
		return err
	}
	if _, err := w.FinalizeAdaptiveRevisit(ctx, p.scope, root, now); err != nil {
		return err
	}
	return p.refresh(ctx, w, now)
}

func (p *adaptiveRevisitPolicy) commit(ctx context.Context, w *state.Store, job state.Job, batch state.ScanBatch, now time.Time) error {
	if !p.enabled {
		return w.CommitScan(ctx, job, batch)
	}
	return w.CommitAdaptiveScan(ctx, p.scope, job, batch, now)
}

func (p *adaptiveRevisitPolicy) pending(ctx context.Context, w *state.Store, due time.Time, fixedInterval time.Duration) (bool, error) {
	if p.enabled {
		return w.AdaptiveRevisitPending(ctx, p.scope, due)
	}
	return w.InventoryRevisitPending(ctx, due, fixedInterval)
}
