package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// All permission and physical-accounting fields describe what this saved-only
// command does not evaluate. Frozen limits are historical policy evidence.
type CPUPeriodChargesReport struct {
	SavedState                 state.CPUChargeAdmissionState `json:"saved_state"`
	CurrentPermissionEvaluated bool                          `json:"current_permission_evaluated"`
	WorkPermissionGranted      bool                          `json:"work_permission_granted"`
	PhysicalPeriodCPUVerified  bool                          `json:"physical_period_cpu_verified"`
	GlobalCPUQuotaVerified     bool                          `json:"global_cpu_quota_verified"`
	FullProcessCPUVerified     bool                          `json:"full_process_cpu_verified"`
}

func cpuPeriodChargesStatusMode(args []string) (bool, error) {
	requested := false
	for _, a := range args {
		if strings.HasPrefix(a, "--cpu-period-charges") || strings.HasPrefix(a, "-cpu-period-charges") {
			requested = true
		}
	}
	if !requested {
		return false, nil
	}
	if len(args) != 1 || args[0] != "--cpu-period-charges" {
		return false, usageError{errors.New("status --cpu-period-charges is an exclusive saved-only mode; it takes no other arguments")}
	}
	return true, nil
}

func savedCPUPeriodCharges(ctx context.Context, paths config.Paths) (CPUPeriodChargesReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return CPUPeriodChargesReport{}, err
	}
	saved, readErr := r.CPUChargeAdmission(ctx)
	// The read transaction and reader close before either output or diagnostics.
	err = errors.Join(readErr, r.Close(), ctx.Err())
	if err != nil {
		return CPUPeriodChargesReport{}, err
	}
	return CPUPeriodChargesReport{SavedState: saved}, nil
}

func cpuPeriodChargesStatusError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, state.ErrCPUChargeAdmissionCorrupt) || errors.Is(err, state.ErrCPUChargesCorrupt) {
		return state.ErrCPUChargeAdmissionCorrupt
	}
	if errors.Is(err, state.ErrCPUChargeAdmissionUnavailable) {
		return state.ErrCPUChargeAdmissionUnavailable
	}
	return err
}

func printCPUPeriodChargesReport(out io.Writer, r CPUPeriodChargesReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved UTC CPU period charges", "")
	s := r.SavedState
	if !s.Available {
		printResultBanner(guard, "CPU PERIOD ACCOUNTING NOT ACTIVATED")
		printWrapped(guard, "This existing state store has no activated UTC-period charge ledger. Earlier session charges or CPU feedback may exist. This view does not activate accounting or start a worker.", "")
	} else {
		printResultBanner(guard, "ENDPOINT-ASSIGNED CPU CHARGES - SAVED HISTORY")
		printCPUChargesTime(guard, "Saved activation time", s.ActivatedAt)
		printCPUChargesTime(guard, "Saved clock high-water", s.ClockHighWater)
		printField(guard, "Saved policy revision", s.PolicyRevision)
		printField(guard, "Saved CPU generation", s.CPUGeneration)
		printField(guard, "Saved CPU sample ordinal", s.CPUOrdinal)
		printField(guard, "Activation baseline charge", time.Duration(s.ActivationBaselineCPUNS))
		printCPUPeriodChargesSlot(guard, "Saved UTC hour", s.Hour, s.Limits.HourNS)
		printCPUPeriodChargesSlot(guard, "Saved UTC day", s.Day, s.Limits.DayNS)
		printField(guard, "Saved tracking gap open", s.TrackingGapOpen)
		printCPUChargesTime(guard, "Saved gap opened at", s.GapOpenedAt)
		printCPUChargesTime(guard, "Saved gap closed at", s.GapClosedAt)
	}
	printWrapped(guard, "These are fixed UTC slots saved at observation time. Charges are assigned to the observation endpoint. A spanning charge can be assigned to a later slot, and process SELF prefixes can overlap. Retired charges are earlier slot assignments; the activation baseline predates period accounting. Partial tracking and unknown evidence do not represent zero CPU usage.", "")
	printWrapped(guard, "This reads saved joint accounting only. It samples no current CPU or admission clock, loads no configuration, contacts no worker and activates or recovers nothing. Frozen limits are saved policy values. This view calculates no remaining quota, current work permission or time until work. Physical period CPU, global quotas, full-process coverage and cleanup permission are not verified.", "")
	if guard.err != nil {
		return fmt.Errorf("write saved CPU period accounting: %w", guard.err)
	}
	return nil
}

func printCPUPeriodChargesSlot(out io.Writer, label string, s state.CPUChargePeriod, limit int64) {
	printWrapped(out, label, "")
	printField(out, "Saved slot start", time.Unix(0, s.StartNS).UTC().Format(time.RFC3339Nano))
	printField(out, "Saved next UTC boundary", time.Unix(0, s.NextBoundaryNS).UTC().Format(time.RFC3339Nano))
	printField(out, "Assigned CPU charge", time.Duration(s.AssignedCPUNS))
	printField(out, "Retired CPU charge", time.Duration(s.RetiredAssignedCPUNS))
	printField(out, "Frozen charge limit", time.Duration(limit))
	printField(out, "Saved partial tracking", s.PartialTracking)
	printField(out, "Saved blocking unknown", s.BlockingUnknown)
}
