package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func windowFixture() (*metadataWindow, *time.Time, *time.Time) {
	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	elapsed := wall
	r := state.MetadataReservation{Scope: state.MetadataNext, Allowance: 3,
		StartedAt: wall, ClockHighWater: wall, ExpiresAt: wall.Add(time.Minute)}
	w := metadataWindowWithClocks(r, 30*time.Second, func() time.Time { return wall }, func() time.Time { return elapsed })
	return w, &wall, &elapsed
}

func TestMetadataWindowAdmissionBoundAndDenials(t *testing.T) {
	w, _, _ := windowFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, entry := range []struct {
		ctx  context.Context
		kind inventory.APICallKind
		want error
	}{
		{ctx, inventory.APIStat, context.Canceled},
		{context.Background(), "unknown", state.ErrMetadataInvalid},
		{nil, inventory.APIStat, state.ErrMetadataInvalid},
	} {
		if err := w.permit(entry.ctx, entry.kind); !errors.Is(err, entry.want) {
			t.Fatal(err)
		}
	}
	if n, _ := w.usage(); n != 0 {
		t.Fatal("denials were counted", n)
	}
	for i := 0; i < 3; i++ {
		if err := w.permit(context.Background(), inventory.APIStat); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.permit(context.Background(), inventory.APIDirectoryOpen); !errors.Is(err, errMetadataExhausted) {
		t.Fatal(err)
	}
	if n, _ := w.usage(); n != 3 {
		t.Fatal(n)
	}
}

func TestMetadataWindowWallLeaseMidnightAndElapsedExpiry(t *testing.T) {
	for _, test := range []string{"local_rollback", "durable_rollback", "lease_expiry", "midnight", "elapsed_expiry", "work_deadline"} {
		t.Run(test, func(t *testing.T) {
			w, wall, elapsed := windowFixture()
			want := errMetadataExpired
			switch test {
			case "local_rollback":
				*wall = wall.Add(time.Second)
				if err := w.permit(context.Background(), inventory.APIStat); err != nil {
					t.Fatal(err)
				}
				*wall = wall.Add(-time.Nanosecond)
				want = errMetadataRollback
			case "durable_rollback":
				*wall = wall.Add(-time.Nanosecond)
				want = errMetadataRollback
			case "lease_expiry":
				*wall = w.reservation.ExpiresAt
			case "midnight":
				w.reservation.ExpiresAt = time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
				*wall = w.reservation.ExpiresAt
			case "elapsed_expiry", "work_deadline":
				*elapsed = elapsed.Add(30 * time.Second)
				// A frozen wall clock cannot extend the independent deadline.
			}
			before, _ := w.usage()
			if err := w.permit(context.Background(), inventory.APIStat); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if n, high := w.usage(); n != before || high.Before(w.reservation.ClockHighWater) {
				t.Fatal(n, high)
			}
		})
	}
}

func TestMetadataWindowConcurrentAllowance(t *testing.T) {
	w, _, _ := windowFixture()
	w.reservation.Allowance = 17
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.permit(context.Background(), inventory.APIStat) }()
	}
	wg.Wait()
	if n, _ := w.usage(); n != 17 {
		t.Fatal(n)
	}
}

func TestMetadataWindowConstructionToNextRollbackFence(t *testing.T) {
	startup, wall, elapsed := windowFixture()
	base := *wall
	next := metadataWindowWithClocks(startup.reservation, time.Minute, func() time.Time { return *wall }, func() time.Time { return *elapsed })
	*wall = base.Add(2 * time.Second)
	if err := startup.permit(context.Background(), inventory.APIStat); err != nil {
		t.Fatal(err)
	}
	// Both reservations came before construction. Its observed time must fence
	// the following operation, even when Next's own window has no observations.
	next.inheritHighWater(startup)
	*wall = base.Add(time.Second)
	if err := next.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataRollback) {
		t.Fatal(err)
	}
	if attempts, highWater := next.usage(); attempts != 0 || !highWater.Equal(base.Add(2*time.Second)) {
		t.Fatal("rollback admitted a Next attempt", attempts, highWater)
	}
	// An expiry denial still records its high-water; transfer must retain it.
	*wall = startup.reservation.ExpiresAt
	if err := startup.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataExpired) {
		t.Fatal(err)
	}
	next.inheritHighWater(startup)
	*wall = startup.reservation.ExpiresAt.Add(-time.Nanosecond)
	if err := next.permit(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataRollback) {
		t.Fatal(err)
	}
	if attempts, highWater := next.usage(); attempts != 0 || !highWater.Equal(startup.reservation.ExpiresAt) {
		t.Fatal(attempts, highWater)
	}
}

func TestMetadataReadinessRequiresWholeOperation(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name                 string
		limit, used, startup int64
		deferred             bool
	}{
		{"exact", 65566, 0, 30, false}, {"startup_missing", 65565, 0, 30, true},
		{"next_exact", 65537, 1, 0, false}, {"next_missing", 65536, 1, 0, true},
		{"reduced_limit", 100, 200, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := state.MetadataBudget{Available: true, Limit: test.limit, DayCharges: &state.MetadataCharges{Reserved: test.used}}
			at, reason, err := metadataReadiness(b, now, test.startup)
			if err != nil || (reason != "") != test.deferred {
				t.Fatal(at, reason, err)
			}
			if test.deferred && (reason != "daily_metadata_limit" || !at.Equal(time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC))) {
				t.Fatal(at, reason)
			}
		})
	}
	future := now.Add(time.Hour)
	if at, reason, err := metadataReadiness(state.MetadataBudget{Available: true, Reason: "clock_rollback", NextAllowedAt: &future}, now, 30); err != nil || reason != "clock_rollback" || !at.Equal(future) {
		t.Fatal(at, reason, err)
	}
	if _, _, err := metadataReadiness(state.MetadataBudget{Available: true, Reason: "settlement_required"}, now, 30); !errors.Is(err, state.ErrMetadataOutstanding) {
		t.Fatal(err)
	}
}

func TestMetadataSettlementPersistsPermitHighWater(t *testing.T) {
	dir, _ := fixture(t)
	ctx := context.Background()
	store, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	r, err := store.ReserveMetadata(ctx, now, state.MetadataStartup, nil, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := now
	wall := now
	w := metadataWindowWithClocks(r, time.Minute, func() time.Time { return wall }, func() time.Time { return elapsed })
	if err := w.permit(ctx, inventory.APIStat); err != nil {
		t.Fatal(err)
	}
	wall = now.AddDate(0, 0, 1)
	if err := w.permit(ctx, inventory.APIStat); !errors.Is(err, errMetadataExpired) {
		t.Fatal(err)
	}
	if err := settleMetadata(ctx, store, []*metadataWindow{w}, false, now); err != nil {
		t.Fatal(err)
	}
	b, err := store.MetadataBudget(ctx, now, 100)
	if err != nil || b.Reason != "clock_rollback" || b.ClockHighWater == nil || !b.ClockHighWater.Equal(wall) || b.TotalCharges.Reserved != 3 || b.TotalCharges.Observed != 1 || b.TotalCharges.KnownUnusedReserved != 2 {
		t.Fatal(b, err)
	}
}

func TestMetadataQuotaBlockedControlsWithoutConstruction(t *testing.T) {
	dir, c := fixture(t)
	// The Next allowance fits, but construction plus Next does not.
	c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance
	options := Options{ExperimentalScan: true, scannerNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		return nil, errors.New("quota bypassed construction")
	}}
	initial, done := start(t, dir, c, options)
	if initial.InventoryMetrics != nil || initial.Metadata == nil || initial.Metadata.Status != "untracked" {
		t.Fatal(initial)
	}
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_metadata_limit" })
	control(t, dir, "pause")
	control(t, dir, "resume")
	s := control(t, dir, "status")
	if s.ActiveJob != 0 || s.InventoryMetrics != nil || s.Metadata.TotalCharges != nil || s.Dispatch == nil || s.Dispatch.Used != 0 {
		t.Fatal("quota admitted source/dispatch work", s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestMetadataWorkerRollbackKeepsControlsAndSourceBlocked(t *testing.T) {
	dir, c := fixture(t)
	ctx := context.Background()
	store, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(time.Hour)
	r, err := store.ReserveMetadata(ctx, future, state.MetadataStartup, nil, 3, c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SettleMetadata(ctx, r, 0, future); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, done := start(t, dir, c, Options{ExperimentalScan: true, scannerNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
		return nil, errors.New("clock rollback admitted source construction")
	}})
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "clock_rollback" })
	control(t, dir, "pause")
	control(t, dir, "resume")
	s := control(t, dir, "status")
	if s.ActiveJob != 0 || s.InventoryMetrics != nil || s.Metadata.TotalCharges.Reserved != 3 || s.Metadata.NextAllowedAt == nil || !s.Metadata.NextAllowedAt.Equal(future) || s.Dispatch.Used != 0 {
		t.Fatal("rollback extended permission or admitted a dispatch", s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestMetadataScannerCancellationAndPanicSettlement(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "denied", "exhausted", "cancel_startup", "panic_startup", "denied_startup"} {
		t.Run(mode, func(t *testing.T) {
			dir, c := fixture(t)
			// One complete construction+Next charge, no room for another.
			c.Scan.MetadataAttemptsPerDay = inventory.MaxAPIAttemptAllowance + 30
			entered := make(chan struct{}, 1)
			options := Options{ExperimentalScan: true, Interval: time.Millisecond}
			options.scannerNew = func(ctx context.Context, roots, excludes, private []string, permit inventory.APIPermit, _ ...inventory.Option) (*inventory.Scanner, error) {
				// Control status must be available before the first source attempt.
				s, err := Send(ctx, dir, "status")
				if err != nil {
					return nil, err
				}
				if s.Metadata == nil || s.Metadata.TotalCharges == nil || s.Metadata.TotalCharges.OutstandingReserved != c.Scan.MetadataAttemptsPerDay {
					return nil, errors.New("source lacks durable startup and Next reservations")
				}
				if err := permit(ctx, inventory.APIStat); err != nil {
					return nil, err
				}
				if strings.HasSuffix(mode, "_startup") {
					entered <- struct{}{}
					switch mode {
					case "panic_startup":
						panic("private fixture text must not escape")
					case "denied_startup":
						return nil, errors.Join(inventory.ErrAPIPermitDenied, errMetadataExpired)
					default:
						<-ctx.Done()
						return nil, ctx.Err()
					}
				}
				return &inventory.Scanner{}, nil
			}
			options.scannerNext = func(ctx context.Context, _ *inventory.Scanner, _ state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
				if err := permit(ctx, inventory.APIStat); err != nil {
					return state.ScanBatch{}, err
				}
				entered <- struct{}{}
				switch mode {
				case "panic":
					panic("private fixture text must not escape")
				case "denied":
					return state.ScanBatch{Complete: true}, errors.Join(inventory.ErrAPIPermitDenied, errMetadataExpired)
				case "exhausted":
					for i := int64(1); i <= inventory.MaxAPIAttemptAllowance; i++ {
						if err := permit(ctx, inventory.APIStat); err != nil {
							return state.ScanBatch{Complete: true}, errors.Join(inventory.ErrAPIPermitDenied, err)
						}
					}
					return state.ScanBatch{}, errors.New("allowance was not bounded")
				default:
					<-ctx.Done()
					return state.ScanBatch{}, ctx.Err()
				}
			}
			_, done := start(t, dir, c, options)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("source did not run")
			}
			if strings.HasPrefix(mode, "cancel") {
				control(t, dir, "pause")
			}
			waitUntil(t, func() bool {
				s := control(t, dir, "status")
				return s.ActiveJob == 0 && s.Metadata.TotalCharges.OutstandingReserved == 0
			})
			s := control(t, dir, "status")
			charges := s.Metadata.TotalCharges
			if charges.Reserved != c.Scan.MetadataAttemptsPerDay {
				t.Fatal(charges)
			}
			if strings.HasPrefix(mode, "panic") {
				if charges.UnknownReserved != charges.Reserved || charges.Observed != 0 {
					t.Fatal(charges)
				}
			} else {
				want := int64(2)
				if strings.HasSuffix(mode, "_startup") {
					want = 1
				}
				if mode == "exhausted" {
					want = inventory.MaxAPIAttemptAllowance + 1
				}
				if charges.UnknownReserved != 0 || charges.Observed != want || charges.KnownUnusedReserved != charges.Reserved-want {
					t.Fatal(charges)
				}
			}
			control(t, dir, "stop")
			waitExit(t, done)
			r, err := state.OpenReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			summary, err := r.Summary(context.Background())
			if err != nil || summary.Entries != 0 || summary.CompleteDirectories != 0 || summary.SkippedEntries != 0 {
				t.Fatal("interruption committed tentative coverage", summary, err)
			}
			if !strings.HasPrefix(mode, "panic") && summary.DirectoryErrors != 0 {
				t.Fatal("permit denial became a filesystem fault", summary)
			}
		})
	}
}

func TestMetadataScannerPublishCloseRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		var holder scannerHolder
		scanner := &inventory.Scanner{}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); holder.publish(scanner) }()
		go func() { defer wg.Done(); holder.close() }()
		wg.Wait()
		attempts := 0
		_, err := scanner.NextPermitted(context.Background(), state.Job{}, func(context.Context, inventory.APICallKind) error { attempts++; return nil })
		if !errors.Is(err, inventory.ErrAPIPermitDenied) || attempts != 0 {
			t.Fatal("late scanner publication escaped close", attempts, err)
		}
	}
}

func TestMetadataStartupBoundsRefuseBeforeSource(t *testing.T) {
	dir, c := fixture(t)
	private := make([]string, inventory.MaxAPIAttemptAllowance/2)
	for i := range private {
		private[i] = fmt.Sprintf("/generated/%d", i)
	}
	if err := Run(context.Background(), dir, c, Options{ExperimentalScan: true, PrivatePaths: private}); err == nil {
		t.Fatal("unbounded construction inputs admitted")
	}
	c.Scan.MetadataAttemptsPerDay = 0
	if err := Run(context.Background(), dir, c, Options{ExperimentalScan: true}); !errors.Is(err, state.ErrMetadataInvalid) {
		t.Fatal("zero cap fell back to a default", err)
	}
}
