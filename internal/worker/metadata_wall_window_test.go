package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestMetadataWallWorkBoundaryWithFrozenElapsed(t *testing.T) {
	for _, test := range []struct {
		name    string
		started time.Time
		expires time.Duration
		work    time.Duration
		bound   time.Duration
	}{
		{"work", time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC), time.Hour, 30 * time.Second, 30 * time.Second},
		{"lease", time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC), 10 * time.Second, 30 * time.Second, 10 * time.Second},
		{"UTC", time.Date(2026, 10, 9, 23, 59, 50, 0, time.UTC), 10 * time.Second, 30 * time.Second, 10 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			wall, elapsed := test.started, test.started
			r := state.MetadataReservation{StartedAt: test.started, ClockHighWater: test.started, ExpiresAt: test.started.Add(test.expires), Allowance: 3}
			w := metadataWindowWithClocks(r, test.work, func() time.Time { return wall }, func() time.Time { return elapsed })
			wall = test.started.Add(test.bound - time.Nanosecond)
			if err := w.permit(context.Background(), inventory.APIStat); err != nil {
				t.Fatal("valid final instant denied", err)
			}
			wall = test.started.Add(test.bound)
			if err := w.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataExpired) {
				t.Fatal("exact boundary admitted after frozen elapsed clock", err)
			}
			if observed, highWater := w.usage(); observed != 1 || !highWater.Equal(wall) {
				t.Fatal("expiry counted an attempt or lost forward high-water", observed, highWater)
			}
			wall = wall.Add(-time.Nanosecond)
			if err := w.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataRollback) {
				t.Fatal("rollback revived the expired window", err)
			}
		})
	}
}

func TestMetadataWallWorkDelayedConstructionHasNoNewWindow(t *testing.T) {
	base := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	r := state.MetadataReservation{StartedAt: base, ClockHighWater: base, ExpiresAt: base.Add(time.Hour), Allowance: 3}
	wall, elapsed := base.Add(20*time.Second), base
	w := metadataWindowWithClocks(r, 30*time.Second, func() time.Time { return wall }, func() time.Time { return elapsed })
	elapsed = base.Add(10*time.Second - time.Nanosecond)
	if err := w.permit(context.Background(), inventory.APIStat); err != nil {
		t.Fatal(err)
	}
	elapsed = base.Add(10 * time.Second)
	if err := w.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataExpired) {
		t.Fatal("delayed construction granted another full elapsed window", err)
	}
	wall, elapsed = base.Add(30*time.Second), base
	w = metadataWindowWithClocks(r, 30*time.Second, func() time.Time { return wall }, func() time.Time { return elapsed })
	if err := w.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataExpired) {
		t.Fatal("expired reservation received a fresh window", err)
	}
	if observed, _ := w.usage(); observed != 0 {
		t.Fatal(observed)
	}
}

func TestMetadataWallWorkConstructionToNextSharesBothDeadlines(t *testing.T) {
	for _, clock := range []string{"wall", "elapsed"} {
		t.Run(clock, func(t *testing.T) {
			startup, wall, elapsed := windowFixture()
			base := *wall
			*wall, *elapsed = base.Add(2*time.Second), base.Add(2*time.Second)
			if err := startup.permit(context.Background(), inventory.APIStat); err != nil {
				t.Fatal(err)
			}
			r := startup.reservation
			r.StartedAt, r.ClockHighWater, r.ExpiresAt = *wall, *wall, base.Add(time.Hour)
			next := metadataWindowWithClocks(r, time.Minute, func() time.Time { return *wall }, func() time.Time { return *elapsed })
			next.inheritHighWater(startup)
			advance := wall
			if clock == "elapsed" {
				advance = elapsed
			}
			*advance = base.Add(30*time.Second - time.Nanosecond)
			if err := next.permit(context.Background(), inventory.APIStat); err != nil {
				t.Fatal("valid shared final instant denied", err)
			}
			*advance = base.Add(30 * time.Second)
			if err := next.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataExpired) {
				t.Fatal("Next extended preceding source work", clock, err)
			}
			if observed, _ := next.usage(); observed != 1 {
				t.Fatal("shared expiry counted an attempt", observed)
			}
		})
	}
}

func TestMetadataWallWorkGeneratedSourceDenialAndKnownUnusedSettlement(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "keep.bin")
	if err := os.WriteFile(path, []byte("generated original"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "state")
	store, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	if err = store.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	job, err := store.ClaimJob(ctx, []string{state.ScanKind}, base, time.Minute)
	if err != nil || job == nil {
		t.Fatal(job, err)
	}
	allowance, err := inventory.StartupAPIAttemptAllowance([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	limit := allowance + inventory.MaxAPIAttemptAllowance
	startupReservation, err := store.ReserveMetadata(ctx, base, state.MetadataStartup, nil, allowance, limit)
	if err != nil {
		t.Fatal(err)
	}
	nextReservation, err := store.ReserveMetadata(ctx, base, state.MetadataNext, job, inventory.MaxAPIAttemptAllowance, limit)
	if err != nil {
		t.Fatal(err)
	}
	wall, elapsed := base, base
	startup := metadataWindowWithClocks(startupReservation, 30*time.Second, func() time.Time { return wall }, func() time.Time { return elapsed })
	next := metadataWindowWithClocks(nextReservation, time.Minute, func() time.Time { return wall }, func() time.Time { return elapsed })
	scanner, err := inventory.NewPermitted(ctx, []string{root}, nil, nil, startup.permit)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	next.inheritHighWater(startup)
	resolutions := 0
	batch, err := scanner.NextPermitted(ctx, *job, func(ctx context.Context, kind inventory.APICallKind) error {
		if kind == inventory.APIPathResolution {
			resolutions++
			if resolutions == 2 {
				// Tentative entry evidence exists before final pathname checks.
				// A wall gap expires the shared window while elapsed is frozen.
				wall = base.Add(30 * time.Second)
			}
		}
		return next.permit(ctx, kind)
	})
	if !errors.Is(err, inventory.ErrAPIPermitDenied) || !errors.Is(err, errMetadataExpired) || !reflect.DeepEqual(batch, state.ScanBatch{}) || scanner.Metrics().EntryInspections != 1 {
		t.Fatal("work expiry became tentative source coverage", batch, err, scanner.Metrics())
	}
	if err = settleMetadata(ctx, store, []*metadataWindow{startup, next}, false, base); err != nil {
		t.Fatal(err)
	}
	if err = store.FinishJob(ctx, *job, false, job.Cursor, time.Unix(0, 1), ""); err != nil {
		t.Fatal(err)
	}
	budget, err := store.MetadataBudget(ctx, wall, limit)
	startupObserved, _ := startup.usage()
	nextObserved, _ := next.usage()
	if err != nil || budget.ClockHighWater == nil || !budget.ClockHighWater.Equal(wall) || budget.TotalCharges == nil {
		t.Fatal(budget, err)
	}
	charges := budget.TotalCharges
	if charges.Reserved != limit || charges.Observed != startupObserved+nextObserved || charges.KnownUnusedReserved != limit-charges.Observed || charges.UnknownReserved != 0 || charges.OutstandingReserved != 0 {
		t.Fatal("expired source allowance was refunded or became unknown", charges)
	}
	summary, err := store.Summary(ctx)
	if err != nil || summary.Entries != 0 || summary.CompleteDirectories != 0 || summary.DirectoryErrors != 0 || summary.SkippedEntries != 0 || summary.PendingJobs != 1 || summary.RunningJobs != 0 {
		t.Fatal("work expiry changed coverage or lost saved progress", summary, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := state.OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reopened, err := reader.MetadataBudget(ctx, wall.Add(-time.Nanosecond), limit)
	if err != nil || reopened.Reason != "clock_rollback" || reopened.TotalCharges == nil || !reflect.DeepEqual(reopened.TotalCharges, charges) {
		t.Fatal("saved expiry high-water or charges were lost", reopened, err)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "generated original" {
		t.Fatal("generated source body changed", content, err)
	}
}
