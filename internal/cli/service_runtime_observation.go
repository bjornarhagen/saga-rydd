package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/service"
)

type serviceRuntimeObserver func(context.Context, service.InstallSpec) (service.RuntimeObservation, error)

func runServiceRuntimeObservation(ctx context.Context, spec service.InstallSpec) (service.RuntimeObservation, error) {
	return serviceRuntimeObservationWithObserver(ctx, spec, service.ObserveRuntime)
}

func serviceRuntimeObservationWithObserver(ctx context.Context, spec service.InstallSpec, observe serviceRuntimeObserver) (service.RuntimeObservation, error) {
	if err := ctx.Err(); err != nil {
		return service.RuntimeObservation{}, err
	}
	r, err := observe(ctx, spec)
	if errors.Is(err, service.ErrSpec) {
		return r, usageError{err}
	}
	return r, err
}

func serviceObservationString(value *string) string {
	if value == nil {
		return "NOT RECORDED"
	}
	return strconv.Quote(*value)
}

func serviceObservationTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "NOT RECORDED"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func printServiceRuntimeObservation(out io.Writer, r service.RuntimeObservation) error {
	w := &reviewOutput{writer: out}
	matched, pid := "NOT RECORDED", "NOT RECORDED"
	if r.LoadedBindingMatched != nil {
		matched = strconv.FormatBool(*r.LoadedBindingMatched)
	}
	if r.MainPID != nil {
		pid = strconv.FormatUint(uint64(*r.MainPID), 10)
	}
	if _, err := fmt.Fprintf(w, "SERVICE RUNTIME OBSERVATION\n\nObservation: %s\nDeclared manager profile: %s\nManaged label: %q\nDescriptor: %q\nArtifact observation: %s\nLoaded settings match: %s\nLookup attempted: %t\nLookup rechecked: %t\n\nHISTORICAL MANAGER DECLARATIONS\n\nActive state: %s\nSubstate: %s\nService result: %s\nMain PID: %s\nInvocation ID: %s (%s)\nUnit observed at: %s\nService observed at: %s\nBinding checked at: %s\nObservation finished at: %s\nManager calls attempted: %d\nManager reply bytes: %d\n\nRunning state: UNKNOWN\nStopped state: UNKNOWN\nApplication readiness: UNKNOWN\n\n", r.Status, r.ManagerProfile, r.Label, r.DescriptorPath, r.ArtifactStatus, matched, r.LookupAttempted, r.LookupRechecked, serviceObservationString(r.ActiveState), serviceObservationString(r.SubState), serviceObservationString(r.ServiceResult), pid, serviceObservationString(r.InvocationID), r.InvocationIDStatus, serviceObservationTime(r.UnitObservedAt), serviceObservationTime(r.ServiceObservedAt), serviceObservationTime(r.BindingCheckedAt), serviceObservationTime(r.FinishedAt), r.ManagerCalls, r.ManagerReplyBytes); err != nil {
		return err
	}
	if r.Platform == "darwin" {
		if _, err := fmt.Fprintln(w, "This macOS profile is unsupported. No artifact or launchctl probe was made."); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(w, "These are sequential historical manager declarations. They do not prove a\ncurrent process, loaded-file origin, shutdown or application readiness.\nConnecting to the selected local broker socket can activate broker infrastructure."); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(w, "No unit loading, start/stop, reload, enablement, worker IPC or source-content\nread was requested. Configuration, inventory/history and executable contents\nwere not opened. No report or Rydd state was saved. Native installed-manager\nacceptance remains open.")
	return err
}

func serviceRuntimeObservationReplyMessage(r service.RuntimeObservation) string {
	return fmt.Sprintf("Service runtime observation reply did not finish for managed label %q at %q (observation: %s; manager calls attempted: %d). Running, stopped and readiness remain unknown. No report or Rydd state was saved, and no unit loading, start/stop or source read was requested", r.Label, r.DescriptorPath, r.Status, r.ManagerCalls)
}
