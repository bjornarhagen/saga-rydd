package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type CPUChargesReport struct {
	SavedState                 state.CPUChargeState `json:"saved_state"`
	CurrentPermissionEvaluated bool                 `json:"current_permission_evaluated"`
	HourlyLimitEnforced        bool                 `json:"hourly_limit_enforced"`
	DailyLimitEnforced         bool                 `json:"daily_limit_enforced"`
	PhysicalPowerMeasured      bool                 `json:"physical_power_measured"`
}

// Reject ambiguous modes before paths or storage are inspected.
func cpuChargesStatusMode(args []string) (bool, error) {
	requested := false
	for _, a := range args {
		if strings.HasPrefix(a, "--cpu-charges") || strings.HasPrefix(a, "-cpu-charges") {
			requested = true
		}
	}
	if !requested {
		return false, nil
	}
	if len(args) != 1 || args[0] != "--cpu-charges" {
		return false, usageError{errors.New("status --cpu-charges is an exclusive saved-only mode; it takes no other arguments")}
	}
	return true, nil
}

func cpuChargesStatusPaths(dataDir string) (config.Paths, error) {
	if filepath.IsAbs(dataDir) {
		return config.PathsFor(runtime.GOOS, "", dataDir, os.Getenv)
	}
	return config.ResolvePaths(dataDir)
}

func savedCPUCharges(ctx context.Context, paths config.Paths) (CPUChargesReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return CPUChargesReport{}, err
	}
	saved, readErr := r.CPUCharges(ctx)
	// Close the reader before returning either a record or a diagnostic.
	err = errors.Join(readErr, r.Close(), ctx.Err())
	if err != nil {
		return CPUChargesReport{}, err
	}
	return CPUChargesReport{SavedState: saved}, nil
}

func cpuChargesStatusError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, state.ErrCPUChargesCorrupt) {
		return state.ErrCPUChargesCorrupt
	}
	if errors.Is(err, state.ErrCPUChargesUnavailable) {
		return state.ErrCPUChargesUnavailable
	}
	return err
}

func printCPUChargesReport(out io.Writer, r CPUChargesReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved SELF CPU charges", "")
	s := r.SavedState
	if !s.Available {
		printResultBanner(guard, "CPU CHARGE ACCOUNTING NOT ACTIVATED")
		printWrapped(guard, "This existing state store has no activated session-charge ledger. Earlier CPU feedback may exist. This view does not activate accounting or start a worker.", "")
	} else {
		printResultBanner(guard, "CONSERVATIVE CPU CHARGES - SAVED HISTORY")
		printField(guard, "Saved session state", s.Status)
		printField(guard, "Run generation", s.Generation)
		printField(guard, "CPU charged", time.Duration(s.ChargedCPUNS))
		printField(guard, "Closed sessions", s.ClosedSessions)
		printField(guard, "Sessions with unknown tails", s.UnknownTailSessions)
		printField(guard, "Recovered sessions", s.RecoveredSessions)
		printField(guard, "Open tail unobserved", s.OpenTailUnobserved)
		printCPUChargesTime(guard, "Activated at", s.ActivatedAt)
		printCPUChargesTime(guard, "Saved clock high-water", s.ClockHighWater)
		printCPUChargesTime(guard, "Saved admission deadline", s.NextAllowedAt)
		if q := s.Session; q != nil {
			printField(guard, "Session request nonce", q.Start.Nonce)
			printField(guard, "Initial SELF prefix charge", time.Duration(q.InitialPrefixChargeNS))
			printField(guard, "Session CPU charged", time.Duration(q.SessionChargedCPUNS))
			printField(guard, "Last sample ordinal", q.LastOrdinal)
			printField(guard, "Last sample was final", q.LastSampleFinal)
			printCPUChargesTime(guard, "Session closed at", q.ClosedAt)
			printField(guard, "Saved closure reason", q.ClosureReason)
			printField(guard, "Last added charge", time.Duration(q.LastChargeNS))
			printField(guard, "Last recorded backoff", time.Duration(q.LastBackoffNS))
			printField(guard, "Last backoff was capped", q.LastBackoffCapped)
		}
	}
	printWrapped(guard, "Each Run session can charge a cumulative process SELF prefix again. Prefixes can overlap. SELF covers the embedding process's threads and excludes children. Startup before the first saved observation, publication and exit tails can remain unobserved. These charges are neither unique lifetime CPU nor a complete upper bound.", "")
	printWrapped(guard, "This reads saved accounting only. It samples no current CPU or admission clock, loads no configuration, contacts no worker and recovers no session. Saved deadlines do not establish current permission or remaining wait. Hourly and daily CPU limits, physical power and cleanup permission are not evaluated or established.", "")
	if guard.err != nil {
		return fmt.Errorf("write saved CPU charge accounting: %w", guard.err)
	}
	return nil
}

func printCPUChargesTime(out io.Writer, label string, t *time.Time) {
	value := "Not recorded"
	if t != nil {
		value = t.UTC().Format(time.RFC3339Nano)
	}
	printField(out, label, value)
}
