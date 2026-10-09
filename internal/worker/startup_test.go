package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestStartupCheckOwnsWriterAndRefusesBeforeRootsRecoveryOrReady(t *testing.T) {
	dir, cfg := fixture(t)
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(ctx, 1, "fixture", nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	job, err := w.ClaimJob(ctx, []string{"fixture"}, time.Now().Add(-time.Minute), time.Second)
	if err != nil || job == nil {
		t.Fatal(job, err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Roots = []string{"/different-generated-root-must-not-be-synced"}
	refused := errors.New("generated stale configuration")
	checked, ready, dispatched := false, false, false
	err = Run(ctx, dir, cfg, Options{
		StartupCheck: func(ctx context.Context) error {
			checked = true
			lock, err := localfs.AcquireLock(dir)
			if lock != nil {
				lock.Close()
			}
			if !errors.Is(err, localfs.ErrLocked) {
				t.Fatal("startup check did not own the state writer lock", err)
			}
			return refused
		},
		Ready: func(Snapshot) { ready = true },
		Handlers: map[string]Handler{"fixture": func(context.Context, state.Job) (Result, error) {
			dispatched = true
			return Result{Done: true}, nil
		}},
	})
	if !errors.Is(err, refused) || !checked || ready || dispatched {
		t.Fatal("stale startup reached work or readiness", err, checked, ready, dispatched)
	}
	r, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(ctx)
	if err != nil || summary.RunningJobs != 1 || summary.PendingJobs != 0 {
		t.Fatal("startup refusal recovered an old running job", summary, err)
	}
	report, err := r.LargestFiles(ctx, 20, "")
	if err != nil || len(report.Roots) != 1 || string(report.Roots[0].PathBytes) != "/synthetic" {
		t.Fatal("startup refusal synchronized changed roots", report.Roots, err)
	}
	lock, err := localfs.AcquireLock(dir)
	if err != nil {
		t.Fatal("startup refusal retained its writer lock", err)
	}
	lock.Close()
}
