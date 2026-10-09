package inventory

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

type hashPacingClock struct {
	wall, elapsed time.Time
	waits         []time.Duration
	onWait        func(context.Context, time.Duration) error
}

func newHashPacingClock() *hashPacingClock {
	return &hashPacingClock{wall: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC), elapsed: time.Unix(1, 0)}
}
func (c *hashPacingClock) advance(d time.Duration) {
	c.wall = c.wall.Add(d)
	c.elapsed = c.elapsed.Add(d)
}
func (c *hashPacingClock) pacer(rate int64) *hashReadPacer {
	return newHashReadPacer(rate, func() time.Time { return c.wall }, func() time.Time { return c.elapsed }, func(ctx context.Context, d time.Duration) error {
		c.waits = append(c.waits, d)
		if c.onWait != nil {
			return c.onWait(ctx, d)
		}
		c.advance(d)
		return nil
	})
}

func TestHashReadPacingCapacity(t *testing.T) {
	for _, tc := range []struct {
		rate, min int64
		window    time.Duration
		want      error
	}{
		{1, 64, 5 * time.Second, ErrHashReadPacingCapacity}, {64, 64, 5 * time.Second, nil},
		{1, 1, 5 * time.Second, nil}, {1, 0, 0, nil}, {1 << 30, 64, time.Nanosecond, ErrHashReadPacingCapacity},
		{0, 64, 5 * time.Second, ErrHashReadPacingInput}, {1<<30 + 1, 64, 5 * time.Second, ErrHashReadPacingInput},
		{math.MaxInt64, 64, 5 * time.Second, ErrHashReadPacingInput}, {1, 65, 5 * time.Second, ErrHashReadPacingInput},
		{1, -1, 5 * time.Second, ErrHashReadPacingInput}, {1, 0, -1, ErrHashReadPacingInput}, {1, 0, 5*time.Second + 1, ErrHashReadPacingInput},
	} {
		if err := CheckHashReadPacingCapacity(tc.rate, tc.min, tc.window); !errors.Is(err, tc.want) {
			t.Fatalf("capacity(%d,%d,%s): %v, want %v", tc.rate, tc.min, tc.window, err, tc.want)
		}
	}
	// Independently verify ceiling division at irregular rates.
	if got := hashReadPacingCost(3, 1); got != 333333334*time.Nanosecond {
		t.Fatal(got)
	}
	if got := hashReadPacingCost(1<<30, 32768); got != 30518*time.Nanosecond {
		t.Fatal(got)
	}
}

func TestHashReadPacingEveryRequestPrepaysAndSizesDurableQuanta(t *testing.T) {
	c := newHashPacingClock()
	p := c.pacer(1024)
	ctx := context.Background()
	if err := p.beginPhase(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		want, left, expected int64
		cost                 time.Duration
	}{{32768, 65536, 1024, time.Second}, {32768, 64512, 512, time.Second / 2}, {128, 128, 128, time.Second / 8}} {
		before := c.elapsed
		got, err := p.request(ctx, tc.want, tc.left)
		if err != nil || got != tc.expected || c.elapsed.Sub(before) != tc.cost {
			t.Fatal("request was not prospectively prepaid", got, err, c.elapsed.Sub(before), tc)
		}
	}
	if len(c.waits) == 0 || p.observation().ObservedWait == nil || *p.observation().ObservedWait != 1625*time.Millisecond {
		t.Fatal(p.observation(), c.waits)
	}
	c.advance(370 * time.Millisecond)
	got, err := p.request(ctx, 64, 100)
	if err != nil || got != 0 || !p.yielded {
		t.Fatal("insufficient durable quantum did not soft yield", got, err, p.observation())
	}
	// A final one-byte tail may complete without inventing a 64-byte request.
	c = newHashPacingClock()
	p = c.pacer(1)
	if err = p.beginPhase(ctx, 2500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	got, err = p.request(ctx, 1, 1)
	if err != nil || got != 1 || p.waited != time.Second {
		t.Fatal(got, err, p.observation())
	}
}

func TestHashReadPacingDeniedWaitRetainsHighWaterAndStickyCause(t *testing.T) {
	for _, reason := range []string{"cancel", "rollback", "elapsed_rollback", "expiry", "day", "wall_window", "elapsed_window"} {
		t.Run(reason, func(t *testing.T) {
			c := newHashPacingClock()
			p := c.pacer(1024)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			expiry := c.wall.Add(time.Hour)
			if reason == "expiry" {
				expiry = c.wall.Add(50 * time.Millisecond)
			}
			day := c.wall.Format(time.DateOnly)
			if reason == "day" {
				c.wall = time.Date(2026, 10, 7, 23, 59, 59, 990000000, time.UTC)
				p = c.pacer(1024)
				expiry = c.wall.Add(time.Hour)
				day = c.wall.Format(time.DateOnly)
			}
			p.bind(expiry, c.wall, day, cancel)
			if err := p.beginPhase(ctx, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			var peak time.Time
			var want error
			c.onWait = func(ctx context.Context, d time.Duration) error {
				c.advance(d)
				peak = c.wall
				switch reason {
				case "cancel":
					cancel(context.Canceled)
					want = context.Canceled
				case "rollback":
					c.wall = c.wall.Add(-time.Second)
					want = ErrHashReadClockRollback
				case "elapsed_rollback":
					c.elapsed = c.elapsed.Add(-time.Second)
					want = ErrHashReadClockRollback
				case "expiry":
					c.wall = expiry
					peak = c.wall
					want = ErrHashReadExpired
				case "day":
					want = ErrHashReadReservationDay
				case "wall_window":
					c.wall = c.wall.Add(6 * time.Second)
					peak = c.wall
					want = context.DeadlineExceeded
				case "elapsed_window":
					c.elapsed = c.elapsed.Add(6 * time.Second)
					want = context.DeadlineExceeded
				}
				return nil
			}
			got, err := p.request(ctx, 1024, 1024)
			if got != 0 || !errors.Is(err, want) || !errors.Is(context.Cause(ctx), want) {
				t.Fatal("denied wait admitted a read or lost its cause", got, err, context.Cause(ctx), want)
			}
			if reason != "rollback" && p.maxWall.Before(peak) {
				t.Fatal("denied wall observation was lost", p.maxWall, peak)
			}
			if reason == "elapsed_rollback" && p.observation().ObservedWait != nil {
				t.Fatal("invalid elapsed measurement claimed known wait", p.observation())
			}
			old := p.refusal
			c.onWait = nil
			c.wall = p.maxWall.Add(time.Millisecond)
			c.elapsed = p.lastElapsed.Add(time.Millisecond)
			_, _, err = p.observe(context.Background())
			if !errors.Is(err, old) {
				t.Fatal("clock catch-up cleared refusal", err, old)
			}
		})
	}
}

func TestHashReadPacingNoCatchupCreditAndIndependentClockSpacing(t *testing.T) {
	c := newHashPacingClock()
	p := c.pacer(1024)
	ctx := context.Background()
	c.advance(time.Second)
	if err := p.beginPhase(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	before := c.elapsed
	if n, err := p.request(ctx, 64, 64); err != nil || n != 64 || c.elapsed.Sub(before) != time.Second/16 {
		t.Fatal("idle time became credit", n, err, c.elapsed.Sub(before))
	}
	// A forward wall change inside the work window does not pay elapsed cost.
	c = newHashPacingClock()
	p = c.pacer(1024)
	_ = p.beginPhase(ctx, 2*time.Second)
	c.onWait = func(ctx context.Context, d time.Duration) error {
		c.elapsed = c.elapsed.Add(d)
		c.wall = c.wall.Add(d + time.Millisecond)
		return nil
	}
	before = c.elapsed
	if n, err := p.request(ctx, 64, 64); err != nil || n != 64 || c.elapsed.Sub(before) != time.Second/16 {
		t.Fatal("wall advance bypassed elapsed spacing", n, err, c.elapsed.Sub(before))
	}
}

func TestHashReadPacingFullHashYieldValidatesAndRetainsPrefix(t *testing.T) {
	data := fullHashContents(4096)
	scanner, targets := sampleFixture(t, data)
	session := fullHashSession(t, scanner, targets[0])
	c := newHashPacingClock()
	p := c.pacer(1024)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	p.bind(c.wall.Add(time.Hour), c.wall, c.wall.Format(time.DateOnly), cancel)
	p1, usage, err := session.step(ctx, 4096, fileHashHooks{pacing: p})
	if err != nil || p1.Status != "partial" || p1.Offset < 64 || p1.Offset%64 != 0 || usage.RequestedBytes != p1.Offset || !p.yielded {
		t.Fatal("soft yield lost checked aligned progress", p1, usage, err, p.observation())
	}
	requireFullHashClaims(t, p1)
	p2, _, err := session.Step(context.Background(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	requireFullHashDigest(t, p2, data)
}

func TestHashReadPacingMutationWhileWaitingAndAdmittedShortRead(t *testing.T) {
	for _, mode := range []string{"rewrite", "truncate"} {
		t.Run(mode, func(t *testing.T) {
			data := fullHashContents(1024)
			scanner, targets := sampleFixture(t, data)
			session := fullHashSession(t, scanner, targets[0])
			c := newHashPacingClock()
			p := c.pacer(1 << 20)
			changed := false
			c.onWait = func(ctx context.Context, d time.Duration) error {
				c.advance(d)
				if !changed {
					changed = true
					path := string(targets[0].File.PathBytes)
					var err error
					if mode == "truncate" {
						err = os.Truncate(path, 1)
					} else {
						err = os.WriteFile(path, bytes.Repeat([]byte("x"), 1024), 0600)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			progress, usage, err := session.step(context.Background(), 1024, fileHashHooks{pacing: p})
			requireFullHashInvalid(t, progress, usage, err)
			if usage.RequestedBytes != 1024 || !changed || len(c.waits) == 0 {
				t.Fatal("admitted mutated/failed request was not fully counted", usage, changed)
			}
			if mode == "truncate" && usage.ReadBytes != 1 {
				t.Fatal("short read accounting lost", usage)
			}
		})
	}
}

func TestHashReadPacingOriginalCapacityRefusalNoReservationAndTailEmpty(t *testing.T) {
	for _, size := range []int{1, 64, 4096} {
		t.Run(string(rune('a'+size%26)), func(t *testing.T) {
			data := fullHashContents(size)
			f, req := hashReadFixture(t, data)
			consent := hashReadApprove(t, f, req)
			before := hashStoreSnapshot(t, f.store)
			c := newHashPacingClock()
			c.wall = f.store.now()
			f.store.now = func() time.Time { return c.wall }
			p := c.pacer(1)
			result, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hashStoreHooks{pacing: p})
			if size >= 64 {
				if !errors.Is(err, ErrHashReadPacingCapacity) || result.ReservedBytes != 0 || result.Usage.RequestedBytes != 0 || result.Code != "read_pacing_capacity" {
					t.Fatal(result, err)
				}
				after := hashStoreSnapshot(t, f.store)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("capacity refusal altered work/charge/consent", before, after)
				}
			} else {
				if err != nil || result.Status != "hash_observed" {
					t.Fatal(result, err)
				}
				requireFullHashDigest(t, result.Progress, data)
				if size == 0 && len(c.waits) != 0 {
					t.Fatal("empty file waited for imaginary body read")
				}
			}
			if result.ReadPacing == nil || result.ReadPacing.Contract != HashReadPacingContract || result.ReadPacing.RequestedBytesPerSecond != 1 {
				t.Fatal(result)
			}
		})
	}
}

func TestHashReadPacingOriginalWaitCancellationSettlesFullChargeAndHighWater(t *testing.T) {
	data := fullHashContents(1024)
	f, req := hashReadFixture(t, data)
	consent := hashReadApprove(t, f, req)
	c := newHashPacingClock()
	c.wall = f.store.now()
	f.store.now = func() time.Time { return c.wall }
	p := c.pacer(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.onWait = func(ctx context.Context, d time.Duration) error { c.advance(d); cancel(); return ctx.Err() }
	result, err := f.store.runConsented(ctx, consent.ID, f.source, f.scanner, hashStoreHooks{pacing: p})
	if !errors.Is(err, context.Canceled) || result.ReservedBytes != 1024 || result.Usage.RequestedBytes != 0 || result.DurableOffset != 0 || result.Progress.SHA256 != "" {
		t.Fatal(result, err)
	}
	saved := hashStoreSnapshot(t, f.store)
	if saved.Work[0].Status != "pending" || saved.Work[0].LatestAttempt.Status != "settled" || saved.Budget.TotalReservedBytes != 1024 || saved.Budget.TotalReadBytes != 0 || saved.Budget.TotalUnknownReservedBytes != 0 || saved.ReadConsent.ID != consent.ID || saved.ReadConsent.ClockHighWater != p.maxWall {
		t.Fatal("canceled wait corrupted evidence/accounting or lost observed wall", saved, p.maxWall)
	}
}

func TestHashReadPacingEmptyFullHashHasNoBodyWait(t *testing.T) {
	scanner, targets := sampleFixture(t, []byte{})
	session := fullHashSession(t, scanner, targets[0])
	c := newHashPacingClock()
	p := c.pacer(1)
	result, usage, err := session.step(context.Background(), 1, fileHashHooks{pacing: p})
	if err != nil || usage.RequestedBytes != 0 || usage.ReadBytes != 0 || len(c.waits) != 0 {
		t.Fatal(result, usage, err, c.waits)
	}
	requireFullHashDigest(t, result, []byte{})
}

func TestHashReadPacingCloseDuringWaitAdmitsNoBodyRead(t *testing.T) {
	scanner, targets := sampleFixture(t, fullHashContents(1024))
	session := fullHashSession(t, scanner, targets[0])
	c := newHashPacingClock()
	p := c.pacer(1024)
	c.onWait = func(ctx context.Context, d time.Duration) error { c.advance(d); scanner.Close(); return nil }
	result, usage, err := session.step(context.Background(), 1024, fileHashHooks{pacing: p})
	if err == nil || result.Code != "scanner_closed" || result.SHA256 != "" || usage.RequestedBytes != 0 || usage.ReadBytes != 0 {
		t.Fatal(result, usage, err)
	}
}

func TestHashReadPacingReservationRechecksRemainingCapacityWithoutCharge(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh"}[fresh], func(t *testing.T) {
			c := newHashPacingClock()
			p := c.pacer(64)
			if !fresh {
				f, req := hashReadFixture(t, fullHashContents(1024))
				consent := hashReadApprove(t, f, req)
				c.wall = f.store.now()
				f.store.now = func() time.Time { return c.wall }
				p = c.pacer(64)
				before := hashStoreSnapshot(t, f.store)
				result, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hashStoreHooks{pacing: p, beforeReserveCommit: func() { c.advance(4500 * time.Millisecond) }})
				if !errors.Is(err, ErrHashReadPacingCapacity) || result.ReservedBytes != 0 || result.Usage.RequestedBytes != 0 || result.Code != "read_pacing_capacity" {
					t.Fatal(result, err)
				}
				saved := hashStoreSnapshot(t, f.store)
				if !reflect.DeepEqual(saved.Work, before.Work) || !reflect.DeepEqual(saved.Budget, before.Budget) || saved.ReadConsent.ID != consent.ID || saved.ReadConsent.ClockHighWater != p.maxWall || len(c.waits) != 0 {
					t.Fatal("late capacity refusal mutated queue/charge or lost wall", saved, p.maxWall)
				}
			} else {
				f := freshRunFiles(t, 1024, 2, "2", "1")
				w := f.open(t)
				c.wall = w.now()
				w.now = func() time.Time { return c.wall }
				p = c.pacer(64)
				before := requireFreshRunSaved(t, f, w)
				result, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{pacing: p, beforeReserveCommit: func() { c.advance(4500 * time.Millisecond) }})
				if !errors.Is(err, ErrHashReadPacingCapacity) || result.ReservedBytes != 0 || result.Usage.RequestedBytes != 0 || result.Code != "read_pacing_capacity" {
					t.Fatal(result, err)
				}
				saved := requireFreshRunSaved(t, f, w)
				if !reflect.DeepEqual(saved.Progress, before.Progress) || !reflect.DeepEqual(saved.FreshBudget, before.FreshBudget) || saved.ReadConsent.ID != f.consent.ID || saved.ReadConsent.ClockHighWater != p.maxWall || len(c.waits) != 0 {
					t.Fatal("fresh late capacity refusal mutated queue/charge or lost wall", saved, p.maxWall)
				}
				f.checkOriginal(t, w)
			}
		})
	}
}

func TestHashReadPacingOriginalAndFreshWallRefusalsSettleWithoutProgress(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		for _, reason := range []string{"expiry", "day", "rollback", "post_read_expiry"} {
			t.Run(map[bool]string{false: "original", true: "fresh"}[fresh]+"/"+reason, func(t *testing.T) {
				c := newHashPacingClock()
				var expiry time.Time
				var p *hashReadPacer
				var peak time.Time
				mutate := func() {
					switch reason {
					case "expiry", "post_read_expiry":
						c.wall = expiry.Add(time.Millisecond)
					case "day":
						c.advance(25 * time.Millisecond)
					case "rollback":
						c.wall = c.wall.Add(-time.Second)
					}
					peak = c.wall
				}
				if !fresh {
					f, req := hashReadFixture(t, fullHashContents(1024))
					consent := hashReadApprove(t, f, req)
					c.wall = f.store.now()
					expiry = consent.Approval.ExpiresAt
					if reason == "day" {
						c.wall = time.Date(c.wall.Year(), c.wall.Month(), c.wall.Day(), 23, 59, 59, 990000000, time.UTC)
					}
					f.store.now = func() time.Time { return c.wall }
					p = c.pacer(1 << 20)
					c.onWait = func(ctx context.Context, d time.Duration) error {
						c.advance(d)
						if reason != "post_read_expiry" {
							mutate()
						}
						return nil
					}
					hooks := hashStoreHooks{pacing: p}
					if reason == "post_read_expiry" {
						hooks.file.afterRead = func(int) { mutate() }
					}
					result, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hooks)
					expected := map[string]error{"expiry": ErrHashReadExpired, "post_read_expiry": ErrHashReadExpired, "day": ErrHashReadReservationDay, "rollback": ErrHashReadClockRollback}[reason]
					if !errors.Is(err, expected) || result.ReservedBytes != 1024 || result.DurableOffset != 0 || result.Status != "refused" || result.Progress.SHA256 != "" {
						t.Fatal(result, err, expected)
					}
					saved := hashStoreSnapshot(t, f.store)
					if saved.Work[0].Status != "pending" || saved.Work[0].LatestAttempt.Status != "settled" || saved.Budget.TotalReservedBytes != 1024 || saved.Budget.TotalUnknownReservedBytes != 0 || saved.ReadConsent.ID != consent.ID || saved.ReadConsent.ClockHighWater.Before(p.maxWall) {
						t.Fatal(saved, p.maxWall, peak)
					}
					if reason == "post_read_expiry" {
						if saved.Budget.TotalReadBytes != 1024 || result.Usage.ReadBytes != 1024 {
							t.Fatal(saved.Budget, result.Usage)
						}
					} else if saved.Budget.TotalRequestedBytes != 0 || result.Usage.RequestedBytes != 0 {
						t.Fatal("denied wait admitted body bytes", saved.Budget, result.Usage)
					}
				} else {
					f := freshRunFiles(t, 1024, 2, "2", "1")
					w := f.open(t)
					c.wall = w.now()
					expiry = f.consent.Approval.ExpiresAt
					if reason == "day" {
						c.wall = time.Date(c.wall.Year(), c.wall.Month(), c.wall.Day(), 23, 59, 59, 990000000, time.UTC)
					}
					w.now = func() time.Time { return c.wall }
					p = c.pacer(1 << 20)
					c.onWait = func(ctx context.Context, d time.Duration) error {
						c.advance(d)
						if reason != "post_read_expiry" {
							mutate()
						}
						return nil
					}
					hooks := hashFreshRunHooks{pacing: p}
					if reason == "post_read_expiry" {
						hooks.file.afterRead = func(int) { mutate() }
					}
					result, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
					expected := map[string]error{"expiry": ErrHashReadExpired, "post_read_expiry": ErrHashReadExpired, "day": ErrHashReadReservationDay, "rollback": ErrHashReadClockRollback}[reason]
					if !errors.Is(err, expected) || result.ReservedBytes != 1024 || result.DurableOffset != 0 || result.Status != "refused" || result.Progress.SHA256 != "" {
						t.Fatal(result, err, expected)
					}
					saved := requireFreshRunSaved(t, f, w)
					if saved.Progress[0].Status != "pending" || saved.Progress[0].LatestAttempt.Status != "settled" || saved.FreshBudget.TotalReservedBytes != 1024 || saved.FreshBudget.TotalUnknownReservedBytes != 0 || saved.ReadConsent.ID != f.consent.ID || saved.ReadConsent.ClockHighWater.Before(p.maxWall) {
						t.Fatal(saved, p.maxWall, peak)
					}
					if reason == "post_read_expiry" {
						if saved.FreshBudget.TotalReadBytes != 1024 || result.Usage.ReadBytes != 1024 {
							t.Fatal(saved.FreshBudget, result.Usage)
						}
					} else if saved.FreshBudget.TotalRequestedBytes != 0 || result.Usage.RequestedBytes != 0 {
						t.Fatal("fresh denied wait admitted body bytes", saved.FreshBudget, result.Usage)
					}
					f.checkOriginal(t, w)
				}
			})
		}
	}
}

func TestHashReadPacingActualOriginalFreshVariantsPreserveIdentities(t *testing.T) {
	data := fullHashContents(4096)
	f, req := hashReadFixture(t, data)
	consent := hashReadApprove(t, f, req)
	base, start := f.store.now(), time.Now()
	f.store.now = func() time.Time { return base.Add(time.Since(start)) }
	result, err := f.store.RunConsentedPaced(context.Background(), consent.ID, f.source, f.scanner, 1<<20)
	if err != nil || result.ApprovalID != consent.ID || result.ReservedBytes != 4096 || result.ReadPacing == nil || result.ReadPacing.ObservedWait == nil || *result.ReadPacing.ObservedWait <= 0 {
		t.Fatal(result, err)
	}
	requireFullHashDigest(t, result.Progress, data)
	originalSaved := hashStoreSnapshot(t, f.store)
	if !reflect.DeepEqual(originalSaved.ReadConsent.Approval, consent.Approval) {
		t.Fatal("pacing changed original immutable consent", originalSaved.ReadConsent)
	}
	fresh := freshRunFiles(t, 4096, 2, "2", "1")
	w := fresh.open(t)
	base, start = w.now(), time.Now()
	w.now = func() time.Time { return base.Add(time.Since(start)) }
	for ordinal := 1; ordinal <= 2; ordinal++ {
		r, e := w.RunFreshConsentedPaced(context.Background(), fresh.consent.ID, fresh.fresh.m.f.scanner, 1<<20)
		if e != nil || r.Ordinal != ordinal || r.ApprovalID != fresh.consent.ID || r.JobID != fresh.fresh.job.ID || r.JobKey != fresh.fresh.job.Record.JobKey || r.RequestID != fresh.fresh.request.ID() || r.ReservedBytes != 4096 || r.ReadPacing == nil || r.ReadPacing.ObservedWait == nil || *r.ReadPacing.ObservedWait <= 0 {
			t.Fatal(r, e)
		}
		requireFullHashDigest(t, r.Progress, data)
	}
	saved := requireFreshRunSaved(t, fresh, w)
	if !reflect.DeepEqual(saved.ReadConsent.Approval, fresh.consent.Approval) || saved.Comparison.Status != "historical_hashes_match" {
		t.Fatal("pacing changed approval or historical comparison", saved)
	}
	fresh.checkOriginal(t, w)
}

func TestHashReadPacingEveryControllerClockCaptureFencesTransientRollback(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		for _, stage := range []string{"reservation", "after_reserve"} {
			t.Run(map[bool]string{false: "original", true: "fresh"}[fresh]+"/"+stage, func(t *testing.T) {
				var base time.Time
				armed := false
				calls := 0
				clock := func() time.Time {
					if !armed {
						return base
					}
					calls++
					if stage == "reservation" && calls == 1 {
						return base.Add(time.Second)
					}
					if (stage == "reservation" && calls == 2) || (stage == "after_reserve" && calls == 1) {
						return base.Add(3 * time.Second)
					}
					return base.Add(2 * time.Second)
				}
				c := newHashPacingClock()
				if !fresh {
					f, req := hashReadFixture(t, fullHashContents(1024))
					consent := hashReadApprove(t, f, req)
					base = f.store.now()
					f.store.now = clock
					p := newHashReadPacer(1024, clock, func() time.Time { return c.elapsed }, func(ctx context.Context, d time.Duration) error {
						t.Fatal("a denied clock sequence entered a pacing wait")
						return nil
					})
					hooks := hashStoreHooks{pacing: p}
					if stage == "reservation" {
						hooks.beforeReserveCommit = func() { armed = true }
					} else {
						hooks.afterReserve = func() { armed = true }
					}
					result, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hooks)
					if !errors.Is(err, ErrHashReadClockRollback) || result.Status != "refused" || result.Usage.RequestedBytes != 0 || result.Progress.SHA256 != "" || result.DurableOffset != 0 {
						t.Fatal(result, err)
					}
					saved := hashStoreSnapshot(t, f.store)
					if saved.ReadConsent.ClockHighWater != base.Add(3*time.Second) || saved.ReadConsent.ID != consent.ID || saved.Work[0].Status != "pending" {
						t.Fatal("transient sampled peak was lost", saved, p.maxWall, calls)
					}
					if stage == "reservation" {
						if result.ReservedBytes != 0 || saved.Budget != nil || saved.Work[0].LatestAttempt != nil {
							t.Fatal("known rollback published charge", result, saved)
						}
					} else if result.ReservedBytes != 1024 || saved.Budget.TotalReservedBytes != 1024 || saved.Work[0].LatestAttempt.Status != "settled" {
						t.Fatal("committed full charge was lost", result, saved)
					}
				} else {
					f := freshRunFiles(t, 1024, 2, "2", "1")
					w := f.open(t)
					base = w.now()
					w.now = clock
					p := newHashReadPacer(1024, clock, func() time.Time { return c.elapsed }, func(ctx context.Context, d time.Duration) error {
						t.Fatal("a fresh denied clock sequence entered a pacing wait")
						return nil
					})
					hooks := hashFreshRunHooks{pacing: p}
					if stage == "reservation" {
						hooks.beforeReserveCommit = func() { armed = true }
					} else {
						hooks.afterReserve = func() { armed = true }
					}
					result, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
					if !errors.Is(err, ErrHashReadClockRollback) || result.Status != "refused" || result.Usage.RequestedBytes != 0 || result.Progress.SHA256 != "" || result.DurableOffset != 0 {
						t.Fatal(result, err)
					}
					saved := requireFreshRunSaved(t, f, w)
					if saved.ReadConsent.ClockHighWater != base.Add(3*time.Second) || saved.ReadConsent.ID != f.consent.ID || saved.Progress[0].Status != "pending" {
						t.Fatal("fresh transient sampled peak was lost", saved, p.maxWall, calls)
					}
					if stage == "reservation" {
						if result.ReservedBytes != 0 || saved.FreshBudget.TotalReservedBytes != 0 || saved.Progress[0].LatestAttempt != nil {
							t.Fatal("fresh known rollback published charge", result, saved)
						}
					} else if result.ReservedBytes != 1024 || saved.FreshBudget.TotalReservedBytes != 1024 || saved.Progress[0].LatestAttempt.Status != "settled" {
						t.Fatal("fresh committed full charge was lost", result, saved)
					}
					f.checkOriginal(t, w)
				}
			})
		}
	}
}

func TestHashReadPacingElapsedRegressionOutsideWaitHasUnknownDuration(t *testing.T) {
	for _, stage := range []string{"preflight", "final"} {
		t.Run(stage, func(t *testing.T) {
			c := newHashPacingClock()
			p := c.pacer(1024)
			if stage == "final" {
				if err := p.beginPhase(context.Background(), 2*time.Second); err != nil {
					t.Fatal(err)
				}
				if _, err := p.request(context.Background(), 64, 64); err != nil {
					t.Fatal(err)
				}
			}
			c.elapsed = c.elapsed.Add(-time.Second)
			_, _, err := p.observe(context.Background())
			if !errors.Is(err, ErrHashReadClockRollback) || p.observation().ObservedWait != nil {
				t.Fatal("unusable elapsed clock claimed known wait", p.observation(), err)
			}
		})
	}
}

func TestHashReadPacingUnpredictableHeldSetupYieldsQualifiedChargedZeroProgress(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh"}[fresh], func(t *testing.T) {
			c := newHashPacingClock()
			if !fresh {
				f, req := hashReadFixture(t, fullHashContents(1024))
				consent := hashReadApprove(t, f, req)
				c.wall = f.store.now()
				f.store.now = func() time.Time { return c.wall }
				p := c.pacer(64)
				result, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hashStoreHooks{pacing: p, file: fileHashHooks{afterOpen: func() { c.advance(4900 * time.Millisecond) }}})
				if err != nil || result.Code != "pacing_window_exhausted" || result.Status != "pending" || result.ReservedBytes != 1024 || result.Usage.RequestedBytes != 0 || result.DurableOffset != 0 || result.ReadPacing == nil || !result.ReadPacing.Yielded {
					t.Fatal(result, err)
				}
				saved := hashStoreSnapshot(t, f.store)
				if saved.Budget.TotalReservedBytes != 1024 || saved.Budget.TotalReadBytes != 0 || saved.Budget.TotalUnknownReservedBytes != 0 || saved.Work[0].LatestAttempt.Status != "settled" {
					t.Fatal(saved)
				}
			} else {
				f := freshRunFiles(t, 1024, 2, "2", "1")
				w := f.open(t)
				c.wall = w.now()
				w.now = func() time.Time { return c.wall }
				p := c.pacer(64)
				result, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{pacing: p, file: fileHashHooks{afterOpen: func() { c.advance(4900 * time.Millisecond) }}})
				if err != nil || result.Code != "pacing_window_exhausted" || result.Status != "pending" || result.ReservedBytes != 1024 || result.Usage.RequestedBytes != 0 || result.DurableOffset != 0 || result.ReadPacing == nil || !result.ReadPacing.Yielded {
					t.Fatal(result, err)
				}
				saved := requireFreshRunSaved(t, f, w)
				if saved.FreshBudget.TotalReservedBytes != 1024 || saved.FreshBudget.TotalReadBytes != 0 || saved.FreshBudget.TotalUnknownReservedBytes != 0 || saved.Progress[0].LatestAttempt.Status != "settled" {
					t.Fatal(saved)
				}
				f.checkOriginal(t, w)
			}
		})
	}
}

func TestHashReadPacingOriginalFreshReopenPreservesPrefixWithoutSavedPacingDebt(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh"}[fresh], func(t *testing.T) {
			data := fullHashContents(4096)
			c := newHashPacingClock()
			if !fresh {
				f, req := hashReadFixture(t, data)
				req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = 8192, 16384
				consent := hashReadApprove(t, f, req)
				c.wall = f.store.now()
				f.store.now = func() time.Time { return c.wall }
				p := c.pacer(1024)
				first, err := f.store.runConsented(context.Background(), consent.ID, f.source, f.scanner, hashStoreHooks{pacing: p})
				if err != nil || first.Status != "pending" || first.DurableOffset < 64 || first.DurableOffset >= 4096 || first.DurableOffset%64 != 0 || first.Usage.ReadBytes != first.DurableOffset || first.ReservedBytes != 4096 {
					t.Fatal(first, err)
				}
				f.reopen(t)
				base, start := c.wall, time.Now()
				f.store.now = func() time.Time { return base.Add(time.Since(start)) }
				second, err := f.store.RunConsentedPaced(context.Background(), consent.ID, f.source, f.scanner, 1<<20)
				if err != nil || second.ReadPacing.RequestedBytesPerSecond != 1<<20 || second.ReservedBytes != 4096-first.DurableOffset || second.Usage.ReadBytes != 4096-first.DurableOffset {
					t.Fatal(second, err)
				}
				requireFullHashDigest(t, second.Progress, data)
				saved := hashStoreSnapshot(t, f.store)
				if saved.ReadConsent.ID != consent.ID || !reflect.DeepEqual(saved.ReadConsent.Approval, consent.Approval) || saved.Budget.TotalReadBytes != 4096 || saved.Budget.TotalReservedBytes != 4096+second.ReservedBytes {
					t.Fatal(saved)
				}
			} else {
				f := freshRunFiles(t, 4096, 2, "2", "1")
				w := f.open(t)
				c.wall = w.now()
				w.now = func() time.Time { return c.wall }
				p := c.pacer(1024)
				first, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{pacing: p})
				if err != nil || first.Status != "pending" || first.DurableOffset < 64 || first.DurableOffset >= 4096 || first.DurableOffset%64 != 0 || first.Usage.ReadBytes != first.DurableOffset || first.ReservedBytes != 4096 {
					t.Fatal(first, err)
				}
				w = f.open(t)
				base, start := c.wall, time.Now()
				w.now = func() time.Time { return base.Add(time.Since(start)) }
				// The finite queue rotates at reservation: copy2 precedes keeper1's saved
				// continuation. Two explicit invocations must preserve that exact order.
				for _, ordinal := range []int{2, 1} {
					second, err := w.RunFreshConsentedPaced(context.Background(), f.consent.ID, f.fresh.m.f.scanner, 1<<20)
					if err != nil || second.Ordinal != ordinal || second.ReadPacing.RequestedBytesPerSecond != 1<<20 {
						t.Fatal(second, err)
					}
					requireFullHashDigest(t, second.Progress, data)
				}
				saved := requireFreshRunSaved(t, f, w)
				if saved.ReadConsent.ID != f.consent.ID || !reflect.DeepEqual(saved.ReadConsent.Approval, f.consent.Approval) || saved.FreshBudget.TotalReadBytes != 8192 || saved.FreshBudget.TotalUnknownReservedBytes != 0 {
					t.Fatal(saved)
				}
				f.checkOriginal(t, w)
			}
		})
	}
}

func TestHashReadPacingFreshEarlyWindowRefusalSavesLatestObservedClock(t *testing.T) {
	f := freshRunFiles(t, 1024, 2, "2", "1")
	w := f.open(t)
	expiry := f.consent.Approval.ExpiresAt
	calls := 0
	clock := func() time.Time {
		calls++
		if calls <= 2 {
			return expiry.Add(-7 * time.Second)
		}
		if calls == 3 {
			return expiry.Add(-3 * time.Second)
		}
		return expiry.Add(-4 * time.Second)
	}
	w.now = clock
	c := newHashPacingClock()
	p := newHashReadPacer(1<<20, clock, func() time.Time { return c.elapsed }, func(context.Context, time.Duration) error { t.Fatal("short window entered a wait"); return nil })
	result, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{pacing: p})
	if !errors.Is(err, ErrHashFreshReadWindow) || result.Code != "fresh_read_window_too_short" || result.ReservedBytes != 0 || result.Usage.RequestedBytes != 0 {
		t.Fatal(result, err, calls)
	}
	saved := requireFreshRunSaved(t, f, w)
	if saved.ReadConsent.ClockHighWater != expiry.Add(-3*time.Second) || saved.ReadConsent.ExpiredObserved || saved.FreshBudget.TotalReservedBytes != 0 || saved.Progress[0].LatestAttempt != nil {
		t.Fatal("early refusal lost clock or fabricated usage/expiry", saved, calls)
	}
	w.now = func() time.Time { return expiry.Add(-4 * time.Second) }
	retry, err := w.RunFreshConsentedPaced(context.Background(), f.consent.ID, f.fresh.m.f.scanner, 1<<20)
	if !errors.Is(err, ErrHashReadClockRollback) || retry.ReservedBytes != 0 || retry.Usage.RequestedBytes != 0 {
		t.Fatal("intermediate rollback cleared refusal", retry, err)
	}
	f.checkOriginal(t, w)
}
