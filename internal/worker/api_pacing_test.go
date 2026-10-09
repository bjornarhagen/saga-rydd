package worker

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func pacingFixture(t *testing.T, rate int, work time.Duration) (*sourceAPIPacer, *time.Time, *time.Time) {
	t.Helper()
	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	elapsed := wall
	bounds, err := inventory.APIPacingWorkBounds([]string{"/generated"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := newSourceAPIPacer(rate, work, 100000, bounds, func() time.Time { return wall }, func() time.Time { return elapsed })
	if err != nil {
		t.Fatal(err)
	}
	return p, &wall, &elapsed
}

func pacingWindow(p *sourceAPIPacer, wall time.Time, allowance int64, work time.Duration) *metadataWindow {
	r := state.MetadataReservation{Scope: state.MetadataNext, StartedAt: wall, ClockHighWater: wall, ExpiresAt: wall.Add(time.Hour), Allowance: allowance}
	return metadataWindowWithClocks(r, work, p.wallNow, p.elapsedNow)
}

func TestAPIPacingCapacityAndDefaultProfile(t *testing.T) {
	for _, item := range []struct {
		rate  int
		work  time.Duration
		ready bool
	}{
		{100, 30 * time.Second, false}, {1000, 30 * time.Second, false}, {5000, 30 * time.Second, true}, {100000, time.Second, true}, {1, 24 * time.Hour, true},
	} {
		p, _, _ := pacingFixture(t, item.rate, item.work)
		if p.capacity(true) != item.ready || p.snapshot().CapacityReady != item.ready {
			t.Fatal(item, p.snapshot())
		}
		if p.spacing != (time.Second+time.Duration(item.rate)-1)/time.Duration(item.rate) {
			t.Fatal(p.spacing)
		}
		p.blockProfile()
		if p.capacity(false) || p.reason() != "api_profile_blocked" {
			t.Fatal(p.snapshot())
		}
	}
	p, _, _ := pacingFixture(t, 0, time.Second)
	if p != nil || !p.capacity(true) || p.snapshot() != nil || p.blockedReason(true) != "" {
		t.Fatal("default changed", p)
	}
	if _, err := newSourceAPIPacer(100001, time.Second, 10, inventory.APIPacingBounds{}, time.Now, time.Now); !errors.Is(err, inventory.ErrAPIPacingInput) {
		t.Fatal(err)
	}
}

func TestAPIPacingSharedSpacingClocksAndNoCatchup(t *testing.T) {
	p, wall, elapsed := pacingFixture(t, 100000, time.Second)
	w := pacingWindow(p, *wall, 20, time.Second)
	kinds := []inventory.APICallKind{inventory.APIStat, inventory.APIDirectoryOpen, inventory.APIDirectoryRead, inventory.APIFilesystemStat, inventory.APIMountIdentity, inventory.APIPathResolution}
	for _, kind := range kinds {
		if err := p.permit(w)(context.Background(), kind); err != nil {
			t.Fatal(err)
		}
		*wall = wall.Add(p.spacing)
		*elapsed = elapsed.Add(p.spacing)
	}
	if used, _ := w.usage(); used != 6 {
		t.Fatal(used)
	}
	// A different operation/window still has the same spacing history.
	next := pacingWindow(p, *wall, 20, time.Second)
	if err := p.permit(next)(context.Background(), inventory.APIStat); err != nil {
		t.Fatal(err)
	}
	*wall = wall.Add(10 * time.Second) // wall progress cannot erase the elapsed wait.
	if delay, err := p.delay(*wall, *elapsed); err != nil || delay != p.spacing {
		t.Fatal(delay, err)
	}
	*elapsed = elapsed.Add(10 * time.Second)
	if delay, err := p.delay(*wall, *elapsed); err != nil || delay != 0 {
		t.Fatal(delay, err)
	}
	// The next grant starts a new interval, without accumulated credit.
	p.lastWall, p.lastElapsed = *wall, *elapsed
	if delay, err := p.delay(*wall, *elapsed); err != nil || delay != p.spacing {
		t.Fatal(delay, err)
	}
	*elapsed = elapsed.Add(-time.Nanosecond)
	if _, err := p.delay(*wall, *elapsed); !errors.Is(err, errMetadataRollback) {
		t.Fatal(err)
	}
}

func TestAPIPacingCancellationAndNonconsumingHighWater(t *testing.T) {
	p, wall, elapsed := pacingFixture(t, 1, time.Second)
	w := pacingWindow(p, *wall, 10, time.Second)
	if err := p.permit(w)(context.Background(), inventory.APIStat); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	finished := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("paced permit did not return after cancellation")
		}
	})
	go func() { defer close(finished); finished <- p.permit(w)(ctx, inventory.APIStat) }()
	waitUntil(t, func() bool { return p.snapshot().Waiting })
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second || p.snapshot().Waiting || p.snapshot().WaitNS == 0 {
		t.Fatal(p.snapshot())
	}
	if used, _ := w.usage(); used != 1 {
		t.Fatal("wait counted an unattempted API", used)
	}
	// Capacity refuses without admitting a call, but must retain its clock
	// observation for the eventual durable settlement.
	future := w.reservation.ExpiresAt
	*wall = future
	*elapsed = elapsed.Add(time.Second)
	if _, err := p.entryCapacity(w)(context.Background(), 16399); !errors.Is(err, errMetadataExpired) {
		t.Fatal(err)
	}
	if used, high := w.usage(); used != 1 || !high.Equal(future) {
		t.Fatal(used, high)
	}
}

func TestAPIPacingEntryCapacityYieldsAndRejectsRollback(t *testing.T) {
	p, wall, elapsed := pacingFixture(t, 5000, time.Second)
	w := pacingWindow(p, *wall, 65536, time.Second)
	if ready, err := p.entryCapacity(w)(context.Background(), 16399); err != nil || ready {
		t.Fatal(ready, err)
	}
	if used, _ := w.usage(); used != 0 {
		t.Fatal(used)
	}
	*wall = wall.Add(time.Millisecond)
	if ready, err := p.entryCapacity(w)(context.Background(), 6); err != nil || !ready {
		t.Fatal(ready, err)
	}
	*elapsed = elapsed.Add(-time.Nanosecond)
	if _, err := p.entryCapacity(w)(context.Background(), 6); !errors.Is(err, errMetadataRollback) {
		t.Fatal(err)
	}
	if used, _ := w.usage(); used != 0 {
		t.Fatal(used)
	}
}

func TestAPIPacingZeroAdmissionSettlementRetainsHighWater(t *testing.T) {
	dir, _ := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	p, wall, _ := pacingFixture(t, 100000, time.Second)
	r, err := w.ReserveMetadata(context.Background(), *wall, state.MetadataStartup, nil, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	window := metadataWindowWithClocks(r, time.Second, p.wallNow, p.elapsedNow)
	*wall = wall.AddDate(0, 0, 1)
	if _, err := p.entryCapacity(window)(context.Background(), 6); !errors.Is(err, errMetadataExpired) {
		t.Fatal(err)
	}
	if err := settleMetadata(context.Background(), w, []*metadataWindow{window}, false, r.StartedAt); err != nil {
		t.Fatal(err)
	}
	b, err := w.MetadataBudget(context.Background(), r.StartedAt, 100)
	if err != nil || b.ClockHighWater == nil || !b.ClockHighWater.Equal(*wall) || b.TotalCharges.Observed != 0 || b.TotalCharges.KnownUnusedReserved != 3 || b.Reason != "clock_rollback" {
		t.Fatal(b, err)
	}
}

func TestAPIPacingDeniedObservationsFenceFollowingWindow(t *testing.T) {
	for _, mode := range []string{"expired", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			p, wall, elapsed := pacingFixture(t, 100000, time.Second)
			base := *wall
			old := pacingWindow(p, base, 10, time.Second)
			*wall = base.Add(p.spacing)
			*elapsed = base.Add(2 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := errMetadataExpired
			if mode == "canceled" {
				cancel()
				want = context.Canceled
			}
			if _, err := p.entryCapacity(old)(ctx, 6); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if count, high := old.usage(); count != 0 || !high.Equal(*wall) {
				t.Fatal(count, high)
			}
			// A fresh window is valid in its own clock domain, but cannot erase
			// the lifetime pacer's refused later elapsed observation.
			*elapsed = base.Add(time.Second)
			fresh := pacingWindow(p, *wall, 10, time.Second)
			if err := p.permit(fresh)(context.Background(), inventory.APIStat); !errors.Is(err, errMetadataRollback) {
				t.Fatal("fresh window erased refused clock observation", err)
			}
			if count, _ := fresh.usage(); count != 0 {
				t.Fatal(count)
			}
			cancel()
			if err := p.permit(fresh)(ctx, inventory.APIStat); !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost priority over shared rollback", err)
			}
		})
	}
}

func TestAPIPacingCompactSnapshotAndSaturation(t *testing.T) {
	p, _, _ := pacingFixture(t, 100000, time.Second)
	p.waitNS.Store(math.MaxUint64 - 2)
	p.addWait(3)
	a := p.snapshot()
	if a.WaitNS != math.MaxUint64 {
		t.Fatal(a)
	}
	a.RatePerSecond = 1
	if p.snapshot().RatePerSecond != 100000 {
		t.Fatal("snapshot aliased state")
	}
	raw, err := json.Marshal(map[string]any{"api_pacing": p.snapshot()})
	if err != nil || len(raw) > 150 {
		t.Fatal(len(raw), string(raw), err)
	}
	var fields map[string]any
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields["api_pacing"], map[string]any{"rate_per_second": float64(100000), "capacity_ready": true, "wait_ns": float64(math.MaxUint64), "waiting": false}) {
		t.Fatal(fields)
	}
}
