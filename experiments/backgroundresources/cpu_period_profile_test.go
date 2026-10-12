package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestBackgroundResourceCPUPeriodInputsAndUTCBoundaries(t *testing.T) {
	good := options{binary: "explicit", profile: cpuPeriodPartialProfile, seconds: 60, roots: 1, files: 1}
	if !good.valid() {
		t.Fatal("fixed positive profile refused")
	}
	for _, test := range []struct {
		name   string
		change func(*options)
	}{
		{"binary", func(o *options) { o.binary = "" }},
		{"short", func(o *options) { o.seconds = 59 }},
		{"long", func(o *options) { o.seconds = 61 }},
		{"roots", func(o *options) { o.roots = 2 }},
		{"files", func(o *options) { o.files = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := good
			test.change(&o)
			if o.valid() {
				t.Fatal("changed geometry admitted")
			}
			if _, err := run(context.Background(), o); !errors.Is(err, errInput) {
				t.Fatal("invalid input passed prefixture validation", err)
			}
		})
	}
	boundary := time.Date(2026, 10, 12, 10, 57, 0, 0, time.UTC)
	p, err := admitCPUPeriodProfile(boundary)
	if err != nil || p.reason() != "cpu_day_charge_partial" {
		t.Fatal("exact180seconds refused or wrong controlling slot", p, err)
	}
	for _, test := range []struct {
		name string
		at   time.Time
	}{
		{"short_margin", boundary.Add(time.Nanosecond)},
		{"zero", time.Time{}},
		{"outside_ns", time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := admitCPUPeriodProfile(test.at); !errors.Is(err, errCPUPeriodBoundary) {
				t.Fatal("invalid temporal admission accepted", err)
			}
		})
	}
	t.Run("prelaunch_margin", func(t *testing.T) {
		if err := p.checkUTC(boundary.Add(time.Second), true); !errors.Is(err, errCPUPeriodBoundary) {
			t.Fatal("late launch admitted", err)
		}
	})
	t.Run("unchanged_slot", func(t *testing.T) {
		if err := p.checkUTC(boundary.Add(time.Second), false); err != nil {
			t.Fatal(err)
		}
		for _, at := range []time.Time{boundary.Add(3 * time.Minute), boundary.Add(-time.Hour)} {
			if !errors.Is(p.checkUTC(at, false), errCPUPeriodBoundary) {
				t.Fatal("changed UTC slot admitted")
			}
		}
	})
	t.Run("equal_boundaries", func(t *testing.T) {
		p, err := admitCPUPeriodProfile(time.Date(2026, 10, 12, 23, 0, 0, 0, time.UTC))
		if err != nil || p.reason() != "cpu_hour_charge_partial" {
			t.Fatal("equal-slot production ordering lost", p, err)
		}
	})
}

func cpuPeriodProtocolFixture(t *testing.T) (*state.Store, state.CPULimitedSessionMarker, *cpuPeriodProfile, cpuPeriodPair) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 10, 12, 10, 3, 0, 0, time.UTC)
	p, err := admitCPUPeriodProfile(at)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "state")
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := w.SyncRoots(ctx, []string{filepath.Join(dir, "generated-root")}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ActivateCPUCharges(ctx, at); err != nil {
		t.Fatal(err)
	}
	limits := state.CPUChargeLimits{HourNS: int64(time.Second), DayNS: int64(time.Second)}
	if _, err := w.ActivateCPUChargeAdmission(ctx, at, limits); err != nil {
		t.Fatal(err)
	}
	cpu := int64(10 * time.Millisecond)
	marker, c, a, err := w.BeginCPUSessionLimited(ctx, state.CPULimitedSessionStart{Start: state.CPUSessionStart{Nonce: strings.Repeat("a", 64), Instance: strings.Repeat("b", 32), ObservedAt: at.Add(time.Millisecond), SelfCPUNS: &cpu}, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	before, err := w.CPUChargeAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := w.CPUCharges(ctx)
	if err != nil || !reflect.DeepEqual(saved, c) {
		t.Fatal("real saved session differs", err)
	}
	after, err := w.CPUChargeAdmission(ctx)
	if err != nil || !reflect.DeepEqual(after, a) {
		t.Fatal("real saved period differs", err)
	}
	pair, err := p.validatePair(before, saved, after)
	if err != nil || !pair.active() {
		t.Fatal("genuine active pair refused", err)
	}
	return w, marker, p, pair
}

func copyCPUPeriodPair(t *testing.T, pair cpuPeriodPair) cpuPeriodPair {
	t.Helper()
	type exported struct {
		CPU    state.CPUChargeState
		Period state.CPUChargeAdmissionState
	}
	raw, err := json.Marshal(exported{pair.cpu, pair.period})
	if err != nil {
		t.Fatal(err)
	}
	var copy exported
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return cpuPeriodPair{copy.CPU, copy.Period}
}

func TestBackgroundResourceCPUPeriodSavedPairRefusals(t *testing.T) {
	_, _, p, base := cpuPeriodProtocolFixture(t)
	for _, test := range []struct {
		name   string
		change func(*cpuPeriodPair)
	}{
		{"generation", func(q *cpuPeriodPair) { q.cpu.Generation++ }},
		{"ordinal", func(q *cpuPeriodPair) { q.cpu.Session.LastOrdinal++; q.period.CPUOrdinal++ }},
		{"missing_counter", func(q *cpuPeriodPair) { q.cpu.Session.LastSelfCPUNS = nil }},
		{"missing_start_counter", func(q *cpuPeriodPair) {
			q.cpu.Session.Start.SelfCPUNS = nil
			q.period.BeginRequest.Start.SelfCPUNS = nil
		}},
		{"recovered", func(q *cpuPeriodPair) { q.cpu.RecoveredSessions = 1 }},
		{"baseline", func(q *cpuPeriodPair) { q.period.ActivationBaselineCPUNS = 1 }},
		{"hour_conservation", func(q *cpuPeriodPair) { q.period.Hour.AssignedCPUNS++ }},
		{"day_conservation", func(q *cpuPeriodPair) { q.period.Day.AssignedCPUNS++ }},
		{"retired", func(q *cpuPeriodPair) { q.period.Hour.RetiredAssignedCPUNS++ }},
		{"rollover", func(q *cpuPeriodPair) {
			q.period.Hour.StartNS += int64(time.Hour)
			q.period.Hour.NextBoundaryNS += int64(time.Hour)
		}},
		{"complete_hour", func(q *cpuPeriodPair) { q.period.Hour.PartialTracking = false }},
		{"complete_day", func(q *cpuPeriodPair) { q.period.Day.PartialTracking = false }},
		{"unknown_slot", func(q *cpuPeriodPair) { q.period.Day.BlockingUnknown = true }},
		{"zero_limit", func(q *cpuPeriodPair) { q.period.Limits.DayNS = 0; q.period.BeginRequest.Limits.DayNS = 0 }},
		{"gap", func(q *cpuPeriodPair) { q.period.TrackingGapOpen = true }},
		{"changed_policy", func(q *cpuPeriodPair) { q.period.PolicyRevision++ }},
		{"start_binding", func(q *cpuPeriodPair) { q.period.BeginRequest.Start.Nonce = strings.Repeat("c", 64) }},
		{"physical", func(q *cpuPeriodPair) { q.period.PhysicalPeriodCPUVerified = true }},
		{"permission", func(q *cpuPeriodPair) { q.period.WorkPermissionGranted = true }},
		{"full_lifetime", func(q *cpuPeriodPair) { q.cpu.FullProcessLifetimeVerified = true }},
		{"overlap_claim", func(q *cpuPeriodPair) { q.cpu.PrefixOverlapPossible = false }},
		{"closed_active", func(q *cpuPeriodPair) { q.cpu.ClosedSessions = 1; q.cpu.UnknownTailSessions = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := copyCPUPeriodPair(t, base)
			test.change(&q)
			digest, err := cpuPeriodDigest(q.cpu)
			if err != nil {
				t.Fatal(err)
			}
			q.period.CPUStateDigest = digest
			pair, err := p.validatePair(q.period, q.cpu, q.period)
			if err == nil && pair.active() {
				t.Fatal("mutated coherent-digest pair admitted")
			}
		})
	}
	t.Run("digest", func(t *testing.T) {
		q := copyCPUPeriodPair(t, base)
		q.period.CPUStateDigest = "cpu-state-v1-" + strings.Repeat("0", 64)
		if _, err := p.validatePair(q.period, q.cpu, q.period); !errors.Is(err, errCPUPeriodEvidence) {
			t.Fatal(err)
		}
	})
	t.Run("changed_between_reads", func(t *testing.T) {
		after := copyCPUPeriodPair(t, base).period
		after.ClockHighWater = &base.cpu.Session.Start.ObservedAt
		after.PolicyRevision++
		if _, err := p.validatePair(base.period, base.cpu, after); !errors.Is(err, errCPUPeriodEvidence) {
			t.Fatal("changing period fence admitted", err)
		}
	})
}

func TestBackgroundResourceCPUPeriodRealFinishAndTail(t *testing.T) {
	w, marker, p, active := cpuPeriodProtocolFixture(t)
	finalCPU, elapsed := int64(30*time.Millisecond), int64(5*time.Second)
	c, a, err := w.FinishCPUSessionLimited(context.Background(), marker, state.CPUSessionSample{Ordinal: 1, ObservedAt: active.cpu.Session.Start.ObservedAt.Add(5 * time.Second), SelfCPUNS: &finalCPU, ElapsedNS: &elapsed})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := p.validatePair(a, c, a)
	if err != nil || !finished.finishedFrom(active) || c.ClosedSessions != 1 || c.UnknownTailSessions != 1 || c.RecoveredSessions != 0 {
		t.Fatal("genuine final accounting refused", err)
	}
	for _, test := range []struct {
		name   string
		change func(*cpuPeriodPair)
	}{
		{"tail_count", func(q *cpuPeriodPair) { q.cpu.UnknownTailSessions = 0 }},
		{"closure", func(q *cpuPeriodPair) { q.cpu.Session.ClosureReason = "interrupted_session" }},
		{"not_final", func(q *cpuPeriodPair) { q.cpu.Session.LastSampleFinal = false }},
		{"wrong_start", func(q *cpuPeriodPair) { q.cpu.Session.Start.Nonce = strings.Repeat("d", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := copyCPUPeriodPair(t, finished)
			test.change(&q)
			if q.finishedFrom(active) {
				t.Fatal("invalid final transition admitted")
			}
		})
	}
	if total, tail, err := terminalCPUTail(finalCPU, usage{UserCPUNS: finalCPU, SystemCPUNS: int64(time.Millisecond)}); err != nil || total != finalCPU+int64(time.Millisecond) || tail != int64(time.Millisecond) {
		t.Fatal(total, tail, err)
	}
	if _, tail, err := terminalCPUTail(finalCPU, usage{UserCPUNS: finalCPU}); err != nil || tail != 0 {
		t.Fatal("observed zero tail fabricated as unknown", tail, err)
	}
	for _, test := range []struct {
		name     string
		saved    int64
		observed usage
	}{
		{"negative_saved", -1, usage{}},
		{"negative_user", 0, usage{UserCPUNS: -1}},
		{"negative_system", 0, usage{SystemCPUNS: -1}},
		{"deficit", finalCPU, usage{UserCPUNS: finalCPU - 1}},
		{"overflow", 0, usage{UserCPUNS: math.MaxInt64, SystemCPUNS: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := terminalCPUTail(test.saved, test.observed); !errors.Is(err, errCPUPeriodEvidence) {
				t.Fatal("incomparable terminal CPU accepted", err)
			}
		})
	}
}

func TestBackgroundResourceCPUPeriodNoWorkShapeRefusals(t *testing.T) {
	w, _, p, _ := cpuPeriodProtocolFixture(t)
	ctx := context.Background()
	var base statusView
	var err error
	base.State, err = w.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	base.Dispatch, err = w.DispatchBudget(ctx, time.Unix(0, p.hour).UTC(), config.Default().Scan.MaxScanChunksPerDay)
	if err != nil {
		t.Fatal(err)
	}
	base.Metadata, err = w.MetadataBudget(ctx, time.Unix(0, p.hour).UTC(), config.Default().Scan.MetadataAttemptsPerDay)
	if err != nil {
		t.Fatal(err)
	}
	base.CPUFeedback, err = w.CPUFeedback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	base.Worker.State = "running"
	base.Worker.Live = &worker.Snapshot{Paused: true}
	if base.Dispatch.NextAllowed.IsZero() || !p.noWork(base, true) {
		t.Fatal("computed unreserved dispatch time or genuine untracked absence refused")
	}
	for _, test := range []struct {
		name   string
		change func(*statusView)
	}{
		{"entries", func(s *statusView) { s.State.Entries++ }},
		{"queue", func(s *statusView) { s.State.PendingJobs++ }},
		{"dispatch", func(s *statusView) { s.Dispatch.Used++ }},
		{"metadata_zero_record", func(s *statusView) { s.Metadata.TotalCharges = &state.MetadataCharges{} }},
		{"feedback_window", func(s *statusView) { s.CPUFeedback.Window = &state.CPUWindowRecord{} }},
		{"scanner_zero_metrics", func(s *statusView) { s.Worker.Live.InventoryMetrics = &inventory.Metrics{} }},
		{"mode", func(s *statusView) { s.Worker.Live.InventoryMode = &worker.InventoryModeSnapshot{} }},
		{"active", func(s *statusView) { s.Worker.Live.ActiveJob = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := base
			live := *base.Worker.Live
			s.Worker.Live = &live
			test.change(&s)
			if p.noWork(s, true) {
				t.Fatal("inventory/dispatch evidence admitted")
			}
		})
	}
	base.Worker.State, base.Worker.Live = "not-running", nil
	if !p.noWork(base, false) {
		t.Fatal("genuine stopped no-work view refused")
	}
}

func TestBackgroundResourceCPUPeriodReplyQualificationsAndPrivacy(t *testing.T) {
	_, _, _, pair := cpuPeriodProtocolFixture(t)
	for _, mode := range []string{"cpu_charges", "cpu_period_charges"} {
		var saved any = pair.cpu
		if mode == "cpu_period_charges" {
			saved = pair.period
		}
		encode := func(extra map[string]any) []byte {
			t.Helper()
			extra["saved_state"] = saved
			raw, err := json.Marshal(map[string]any{mode: extra})
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
		decode := func(raw []byte) error {
			if mode == "cpu_charges" {
				_, err := decodeCPUSessionReply(raw)
				return err
			}
			_, err := decodeCPUPeriodReply(raw)
			return err
		}
		if err := decode(encode(map[string]any{})); err != nil {
			t.Fatal(err)
		}
		flags := []string{"current_permission_evaluated", "hourly_limit_enforced", "daily_limit_enforced", "physical_power_measured"}
		if mode == "cpu_period_charges" {
			flags = []string{"current_permission_evaluated", "work_permission_granted", "physical_period_cpu_verified", "global_cpu_quota_verified", "full_process_cpu_verified"}
		}
		for _, field := range flags {
			t.Run(mode+"/"+field, func(t *testing.T) {
				if !errors.Is(decode(encode(map[string]any{field: true})), errCPUPeriodEvidence) {
					t.Fatal("saved-view authority qualification lost")
				}
			})
		}
		for _, test := range []struct {
			name string
			raw  []byte
		}{{"missing", []byte(`{}`)}, {"null", []byte(`{"` + mode + `":{"saved_state":null}}`)}, {"type", []byte(`{"` + mode + `":{"saved_state":[]}}`)}, {"overflow", bytes.Repeat([]byte(" "), cliOutputLimit+1)}} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				if !errors.Is(decode(test.raw), errCPUPeriodEvidence) {
					t.Fatal("missing/malformed reply accepted")
				}
			})
		}
	}
	private := "/private/generated/nonce-and-instance"
	pair.cpu.Session.Start.Nonce, pair.cpu.Session.Start.Instance = private, private
	view := result{CPUPeriod: &cpuPeriodProfileResult{Contract: "generated_positive_cpu_period_partial_v1", PartialTracking: true, Paused: pair.projection()}}
	raw, err := json.Marshal(view)
	if err != nil || bytes.Contains(raw, []byte(private)) || bytes.Contains(raw, []byte("nonce")) || bytes.Contains(raw, []byte("instance")) || view.CPUPeriod.LedgerFullLifetimeCoverage || view.CPUPeriod.LimitExhaustionVerified || view.CPUPeriod.UTCRolloverVerified || view.CPUPeriod.RestartVerified || view.HourlyTargetAccepted || view.PhysicalPowerAccepted {
		t.Fatal("private or unaccepted claims entered public projection", err)
	}
}

func TestBackgroundResourceCPUPeriodExistingProfilesCompatibility(t *testing.T) {
	for _, profile := range []string{"defaults_no_power", "short_fixture"} {
		t.Run(profile, func(t *testing.T) {
			for _, o := range []options{{binary: "explicit", profile: profile, seconds: 30, roots: 1, files: 1}, {binary: "explicit", profile: profile, seconds: 3600, roots: 32, files: 1024}} {
				if !o.valid() {
					t.Fatal("existing profile bounds changed")
				}
			}
			raw, err := json.Marshal(result{Profile: profile})
			if err != nil || bytes.Contains(raw, []byte("cpu_period_profile")) {
				t.Fatal("new projection changed existing reply", err)
			}
		})
	}
	before, c := config.Default(), config.Default()
	configureCPUPeriodProfile(&c)
	if !c.Scan.CPUSessionCharges || c.Scan.CPUChargeSecondsPerHour != 1 || c.Scan.CPUChargeSecondsPerDay != 1 {
		t.Fatal("fixed positive profile not selected")
	}
	c.Scan.CPUSessionCharges, c.Scan.CPUChargeSecondsPerHour, c.Scan.CPUChargeSecondsPerDay = before.Scan.CPUSessionCharges, before.Scan.CPUChargeSecondsPerHour, before.Scan.CPUChargeSecondsPerDay
	if !reflect.DeepEqual(c, before) {
		t.Fatal("new profile changed unrelated settings")
	}
}
