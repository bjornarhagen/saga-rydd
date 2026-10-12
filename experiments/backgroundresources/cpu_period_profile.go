package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const cpuPeriodPartialProfile = "positive_period_partial"

var errCPUPeriodBoundary = errors.New("positive CPU profile UTC boundary preflight refused")
var errCPUPeriodEvidence = errors.New("positive CPU profile saved accounting evidence refused")

type cpuPeriodProfile struct {
	hour, day, nextHour, nextDay int64
	paused                       *cpuPeriodPair
}

type cpuPeriodPair struct {
	cpu    state.CPUChargeState
	period state.CPUChargeAdmissionState
}

// Only this projection enters public results. Raw replies, nonces and worker
// instances stay in the harness's private command files.
type cpuPeriodProjection struct {
	Generation          int64                 `json:"generation"`
	Ordinal             int64                 `json:"ordinal"`
	ChargedCPUNS        int64                 `json:"charged_cpu_ns"`
	LastSelfCPUNS       int64                 `json:"last_self_cpu_ns"`
	Status              string                `json:"status"`
	ClosedSessions      int64                 `json:"closed_sessions"`
	UnknownTailSessions int64                 `json:"unknown_tail_sessions"`
	RecoveredSessions   int64                 `json:"recovered_sessions"`
	Hour                state.CPUChargePeriod `json:"hour"`
	Day                 state.CPUChargePeriod `json:"day"`
}

type cpuPeriodProfileResult struct {
	Contract                     string              `json:"contract"`
	NoWorkScope                  string              `json:"no_work_scope"`
	PartialTracking              bool                `json:"partial_tracking"`
	ExpectedWaitReason           string              `json:"expected_wait_reason"`
	PartialSetupBlocked          bool                `json:"partial_setup_blocked"`
	PausedAccountingUnchanged    bool                `json:"paused_accounting_unchanged"`
	FinishedAccountingConserved  bool                `json:"finished_accounting_conserved"`
	Paused                       cpuPeriodProjection `json:"paused"`
	Final                        cpuPeriodProjection `json:"final"`
	TerminalWorkerCPUNS          int64               `json:"terminal_worker_cpu_ns"`
	ObservedTerminalMinusSavedNS int64               `json:"observed_terminal_minus_saved_cpu_ns"`
	LedgerFullLifetimeCoverage   bool                `json:"ledger_full_lifetime_coverage"`
	LimitExhaustionVerified      bool                `json:"limit_exhaustion_verified"`
	UTCRolloverVerified          bool                `json:"utc_rollover_verified"`
	RestartVerified              bool                `json:"restart_verified"`
}

func cpuPeriodUTC(now time.Time) (cpuPeriodProfile, error) {
	wall := now.Round(0).UTC()
	valid := func(t time.Time) bool { return t.UnixNano() > 0 && time.Unix(0, t.UnixNano()).Equal(t) }
	if !valid(wall) {
		return cpuPeriodProfile{}, errCPUPeriodBoundary
	}
	hour := wall.Truncate(time.Hour)
	day := time.Date(wall.Year(), wall.Month(), wall.Day(), 0, 0, 0, 0, time.UTC)
	nextHour, nextDay := hour.Add(time.Hour), day.Add(24*time.Hour)
	if !valid(hour) || !valid(day) || !valid(nextHour) || !valid(nextDay) {
		return cpuPeriodProfile{}, errCPUPeriodBoundary
	}
	return cpuPeriodProfile{hour: hour.UnixNano(), day: day.UnixNano(), nextHour: nextHour.UnixNano(), nextDay: nextDay.UnixNano()}, nil
}

func admitCPUPeriodProfile(now time.Time) (*cpuPeriodProfile, error) {
	p, err := cpuPeriodUTC(now)
	if err != nil {
		return nil, err
	}
	if err := p.checkUTC(now, true); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *cpuPeriodProfile) checkUTC(now time.Time, margin bool) error {
	current, err := cpuPeriodUTC(now)
	if err != nil || current.hour != p.hour || current.day != p.day || current.nextHour != p.nextHour || current.nextDay != p.nextDay {
		return errCPUPeriodBoundary
	}
	if margin && (time.Unix(0, p.nextHour).Sub(now) < 180*time.Second || time.Unix(0, p.nextDay).Sub(now) < 180*time.Second) {
		return errCPUPeriodBoundary
	}
	return nil
}

func (p *cpuPeriodProfile) reason() string {
	// The production wait visits hour first and replaces only a longer wait.
	if p.nextDay > p.nextHour {
		return "cpu_day_charge_partial"
	}
	return "cpu_hour_charge_partial"
}

func configureCPUPeriodProfile(c *config.Config) {
	c.Scan.CPUSessionCharges = true
	c.Scan.CPUChargeSecondsPerHour, c.Scan.CPUChargeSecondsPerDay = 1, 1
}

func (p *cpuPeriodProfile) noWork(s statusView, live bool) bool {
	if s.State.EnabledRoots != 1 || s.State.Entries != 0 || s.State.PendingJobs != 0 || s.State.RunningJobs != 0 || s.State.CompleteDirectories != 0 || s.State.DirectoryErrors != 0 || s.State.SkippedEntries != 0 || s.Dispatch.Used != 0 || s.Metadata.Contract != state.MetadataBudgetContract || !s.Metadata.Available || s.Metadata.Status != "untracked" || s.Metadata.DayCharges != nil || s.Metadata.TotalCharges != nil || s.Metadata.TrackingStartedAt != nil || s.Metadata.ClockHighWater != nil || s.CPUFeedback.Contract != state.CPUFeedbackContract || !s.CPUFeedback.Available || s.CPUFeedback.Status != "untracked" || s.CPUFeedback.Window != nil {
		return false
	}
	if !live {
		return s.Worker.Live == nil && s.Worker.State == "not-running"
	}
	w := s.Worker.Live
	return s.State.Schema == 16 && s.Worker.State == "running" && w != nil && w.ActiveJob == 0 && w.InventoryMode == nil && w.InventoryMetrics == nil && w.Dispatch == nil && w.RecoveredJobs == 0
}

func cpuPeriodDigest(c state.CPUChargeState) (string, error) {
	encoded, err := json.Marshal(c)
	if err != nil || len(encoded) > state.CPUChargesMaxJSONBytes {
		return "", errCPUPeriodEvidence
	}
	digest := sha256.Sum256(encoded)
	return "cpu-state-v1-" + hex.EncodeToString(digest[:]), nil
}

func (p *cpuPeriodProfile) validatePair(before state.CPUChargeAdmissionState, c state.CPUChargeState, after state.CPUChargeAdmissionState) (cpuPeriodPair, error) {
	a := before
	digest, err := cpuPeriodDigest(c)
	if err != nil || !reflect.DeepEqual(before, after) || c.Contract != state.CPUChargesContract || c.Scope != state.CPUChargesScope || !c.Available || !c.PrefixOverlapPossible || c.UniqueProcessCPUVerified || c.FullProcessLifetimeVerified || c.WorkPermissionGranted || c.Generation != 1 || c.Session == nil || c.Session.Generation != 1 || c.Session.LastSelfCPUNS == nil || c.ChargedCPUNS < 0 || c.ChargedCPUNS != *c.Session.LastSelfCPUNS || c.Session.SessionChargedCPUNS != c.ChargedCPUNS || c.RecoveredSessions != 0 || a.Contract != state.CPUChargeAdmissionContract || a.Scope != state.CPUChargeAdmissionScope || a.Version != 1 || !a.Available || !a.EndpointAssignedCharges || !a.PrefixOverlapPossible || a.CurrentWorkPermissionEvaluated || a.WorkPermissionGranted || a.PhysicalPeriodCPUVerified || a.GlobalCPUQuotaVerified || a.FullProcessCPUVerified || a.CPUGeneration != c.Generation || a.CPUOrdinal != c.Session.LastOrdinal || a.CPUStateDigest != digest || a.PolicyRevision != 1 || a.BeginAppliedRevision != 1 || a.BeginRequest == nil || a.BeginRequest.ExpectedPolicyRevision != 0 || a.BeginRequest.Start.ExpectedGeneration != 0 || !reflect.DeepEqual(a.BeginRequest.Start, c.Session.Start) || a.Limits != (state.CPUChargeLimits{HourNS: int64(time.Second), DayNS: int64(time.Second)}) || a.BeginRequest.Limits != a.Limits || a.ActivationBaselineCPUNS != 0 || a.TrackingGapOpen || a.LatestGap != nil || a.Hour.StartNS != p.hour || a.Hour.NextBoundaryNS != p.nextHour || a.Day.StartNS != p.day || a.Day.NextBoundaryNS != p.nextDay {
		return cpuPeriodPair{}, errCPUPeriodEvidence
	}
	for _, slot := range []state.CPUChargePeriod{a.Hour, a.Day} {
		if !slot.PartialTracking || slot.BlockingUnknown || slot.RetiredAssignedCPUNS != 0 || slot.AssignedCPUNS != c.ChargedCPUNS {
			return cpuPeriodPair{}, errCPUPeriodEvidence
		}
	}
	return cpuPeriodPair{c, a}, nil
}

func (q cpuPeriodPair) active() bool {
	r := q.cpu.Session
	return r != nil && q.cpu.Status == "active" && r.Status == "active" && q.cpu.OpenTailUnobserved && q.cpu.ClosedSessions == 0 && q.cpu.UnknownTailSessions == 0 && r.LastOrdinal == 0 && r.LastSample == nil && !r.LastSampleFinal && r.ClosedAt == nil && r.ClosureReason == "" && r.Start.SelfCPUNS != nil && r.InitialPrefixChargeNS == *r.Start.SelfCPUNS && r.InitialPrefixChargeNS == q.cpu.ChargedCPUNS
}

func (q cpuPeriodPair) finishedFrom(paused cpuPeriodPair) bool {
	r := q.cpu.Session
	return paused.active() && r != nil && r.LastSelfCPUNS != nil && q.cpu.Status == "finished" && r.Status == "finished" && !q.cpu.OpenTailUnobserved && q.cpu.ClosedSessions == 1 && q.cpu.UnknownTailSessions == 1 && r.LastOrdinal == 1 && r.LastSample != nil && r.LastSample.Ordinal == 1 && r.LastSample.SelfCPUNS != nil && *r.LastSample.SelfCPUNS == *r.LastSelfCPUNS && r.LastSample.ElapsedNS != nil && *r.LastSample.ElapsedNS > 0 && r.LastSampleFinal && r.ClosedAt != nil && r.ClosureReason == "graceful_tail_unobserved" && reflect.DeepEqual(r.Start, paused.cpu.Session.Start) && r.InitialPrefixChargeNS == paused.cpu.Session.InitialPrefixChargeNS && q.cpu.ChargedCPUNS >= paused.cpu.ChargedCPUNS
}

func (q cpuPeriodPair) projection() cpuPeriodProjection {
	return cpuPeriodProjection{q.cpu.Generation, q.period.CPUOrdinal, q.cpu.ChargedCPUNS, *q.cpu.Session.LastSelfCPUNS, q.cpu.Status, q.cpu.ClosedSessions, q.cpu.UnknownTailSessions, q.cpu.RecoveredSessions, q.period.Hour, q.period.Day}
}

func decodeCPUPeriodReply(raw []byte) (state.CPUChargeAdmissionState, error) {
	var reply struct {
		Report *struct {
			Saved      *state.CPUChargeAdmissionState `json:"saved_state"`
			Current    bool                           `json:"current_permission_evaluated"`
			Permission bool                           `json:"work_permission_granted"`
			Physical   bool                           `json:"physical_period_cpu_verified"`
			Global     bool                           `json:"global_cpu_quota_verified"`
			Full       bool                           `json:"full_process_cpu_verified"`
		} `json:"cpu_period_charges"`
	}
	if len(raw) > cliOutputLimit || json.Unmarshal(raw, &reply) != nil || reply.Report == nil || reply.Report.Saved == nil || reply.Report.Current || reply.Report.Permission || reply.Report.Physical || reply.Report.Global || reply.Report.Full {
		return state.CPUChargeAdmissionState{}, errCPUPeriodEvidence
	}
	return *reply.Report.Saved, nil
}

func decodeCPUSessionReply(raw []byte) (state.CPUChargeState, error) {
	var reply struct {
		Report *struct {
			Saved    *state.CPUChargeState `json:"saved_state"`
			Current  bool                  `json:"current_permission_evaluated"`
			Hour     bool                  `json:"hourly_limit_enforced"`
			Day      bool                  `json:"daily_limit_enforced"`
			Physical bool                  `json:"physical_power_measured"`
		} `json:"cpu_charges"`
	}
	if len(raw) > cliOutputLimit || json.Unmarshal(raw, &reply) != nil || reply.Report == nil || reply.Report.Saved == nil || reply.Report.Current || reply.Report.Hour || reply.Report.Day || reply.Report.Physical {
		return state.CPUChargeState{}, errCPUPeriodEvidence
	}
	return *reply.Report.Saved, nil
}

func (r *runner) readCPUPeriodPair(ctx context.Context, stage string) (cpuPeriodPair, error) {
	if err := r.periodProfile.checkUTC(time.Now(), false); err != nil {
		return cpuPeriodPair{}, err
	}
	beforeRaw, err := r.command(ctx, stage+"-period-before", "status", "--cpu-period-charges")
	if err != nil {
		return cpuPeriodPair{}, err
	}
	before, err := decodeCPUPeriodReply(beforeRaw)
	if err != nil {
		return cpuPeriodPair{}, err
	}
	cpuRaw, err := r.command(ctx, stage+"-cpu", "status", "--cpu-charges")
	if err != nil {
		return cpuPeriodPair{}, err
	}
	cpu, err := decodeCPUSessionReply(cpuRaw)
	if err != nil {
		return cpuPeriodPair{}, err
	}
	afterRaw, err := r.command(ctx, stage+"-period-after", "status", "--cpu-period-charges")
	if err != nil {
		return cpuPeriodPair{}, err
	}
	after, err := decodeCPUPeriodReply(afterRaw)
	if err != nil {
		return cpuPeriodPair{}, err
	}
	if err := r.periodProfile.checkUTC(time.Now(), false); err != nil {
		return cpuPeriodPair{}, err
	}
	return r.periodProfile.validatePair(before, cpu, after)
}

func (r *runner) pauseCPUPeriodProfile(ctx context.Context) error {
	snapshot, err := r.control(ctx, "pause")
	if err != nil || !snapshot.Paused {
		return errors.New("worker pause was not acknowledged")
	}
	settleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	before, err := r.untilStatus(settleCtx, func(s statusView) bool { return r.periodProfile.noWork(s, true) && s.Worker.Live.Paused })
	cancel()
	if err != nil {
		return err
	}
	if err := r.sample(ctx, before); err != nil {
		return err
	}
	first, err := r.readCPUPeriodPair(ctx, "paused-before")
	if err != nil {
		return err
	}
	if !first.active() {
		return errCPUPeriodEvidence
	}
	report, err := r.command(ctx, "report", "report", "--limit", "10")
	if err != nil {
		return err
	}
	var saved struct {
		Report struct {
			Source  string            `json:"source"`
			Current bool              `json:"current_state_verified"`
			Files   []json.RawMessage `json:"files"`
		} `json:"report"`
	}
	if json.Unmarshal(report, &saved) != nil || saved.Report.Source != "saved_inventory" || saved.Report.Current || len(saved.Report.Files) != 0 {
		return errProtocol
	}
	if err := r.waitUntil(ctx, time.Now().Add(2*time.Second)); err != nil {
		return err
	}
	after, err := r.status(ctx)
	if err != nil {
		return err
	}
	if !r.periodProfile.noWork(after, true) || !after.Worker.Live.Paused {
		return errors.New("paused positive CPU profile admitted inventory work")
	}
	second, err := r.readCPUPeriodPair(ctx, "paused-after")
	if err != nil {
		return err
	}
	if !second.active() || !reflect.DeepEqual(first, second) {
		return errCPUPeriodEvidence
	}
	if err := r.sample(ctx, after); err != nil {
		return err
	}
	r.periodProfile.paused = &second
	r.result.CPUPeriod.Paused = second.projection()
	r.result.CPUPeriod.PausedAccountingUnchanged, r.result.PauseVerified = true, true
	resumed, err := r.control(ctx, "resume")
	if err != nil || resumed.Paused {
		return errors.New("worker resume was not acknowledged")
	}
	r.result.ResumeVerified = true
	return nil
}

func terminalCPUTail(saved int64, observed usage) (int64, int64, error) {
	if saved < 0 || observed.UserCPUNS < 0 || observed.SystemCPUNS < 0 || observed.UserCPUNS > math.MaxInt64-observed.SystemCPUNS {
		return 0, 0, errCPUPeriodEvidence
	}
	total := observed.UserCPUNS + observed.SystemCPUNS
	if saved > total {
		return 0, 0, errCPUPeriodEvidence
	}
	return total, total - saved, nil
}

func (r *runner) finishCPUPeriodProfile(ctx context.Context, final statusView, observed usage) error {
	if final.State.Schema != 16 || !r.periodProfile.noWork(final, false) || r.periodProfile.paused == nil {
		return errCPUPeriodEvidence
	}
	pair, err := r.readCPUPeriodPair(ctx, "finished")
	if err != nil {
		return err
	}
	if !pair.finishedFrom(*r.periodProfile.paused) {
		return errCPUPeriodEvidence
	}
	total, tail, err := terminalCPUTail(*pair.cpu.Session.LastSelfCPUNS, observed)
	if err != nil {
		return err
	}
	r.result.CPUPeriod.Final = pair.projection()
	r.result.CPUPeriod.TerminalWorkerCPUNS, r.result.CPUPeriod.ObservedTerminalMinusSavedNS = total, tail
	r.result.CPUPeriod.FinishedAccountingConserved = true
	return nil
}
