package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func runServiceRuntime(ctx context.Context, action string, spec service.InstallSpec) (service.RuntimeResult, error) {
	var result service.RuntimeResult
	var err error
	switch action {
	case "start":
		result, err = service.Start(ctx, spec)
	case "stop":
		result, err = service.Stop(ctx, spec)
	default:
		return result, usageError{errors.New("use service start or stop")}
	}
	if errors.Is(err, service.ErrSpec) {
		return result, usageError{err}
	}
	return result, err
}

func printServiceRuntime(out io.Writer, r service.RuntimeResult) error {
	w := &reviewOutput{writer: out}
	accepted := "Unknown"
	if r.RequestAccepted != nil {
		if *r.RequestAccepted {
			accepted = "Client reported acceptance"
		} else {
			accepted = "Client reported refusal"
		}
	}
	if _, err := fmt.Fprintf(w, "SERVICE %s REQUEST\n\nManaged label: %q\nDescriptor: %q\nRecorded request: %s\nRequest attempted: %t\nAcceptance evidence: %s\nRunning state: Unknown\nStopped state: Unknown\n\n", strings.ToUpper(r.Action), r.Label, r.DescriptorPath, r.RequestStatus, r.RequestAttempted, accepted); err != nil {
		return err
	}
	if r.JobPath != nil {
		if _, err := fmt.Fprintf(w, "Historical queued job: %q\nThe job reply does not prove readiness or completed shutdown.\n", *r.JobPath); err != nil {
			return err
		}
	}
	if r.Platform == "darwin" {
		if _, err := fmt.Fprintln(w, "launchctl replies are opaque. Client success does not authenticate a\nloaded registration. This addresses the fixed current-user label in the\ncooperating user's managed service namespace."); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(w, "Loaded settings are declared manager evidence, not loaded-file\nauthentication. One fixed manager request can affect normal dependencies.\nConnecting to the selected local broker socket can activate the broker."); err != nil {
			return err
		}
	}
	if r.UnitLoadAttempted {
		if _, err := fmt.Fprintln(w, "Preflight requested unit loading; manager bookkeeping can change."); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(w, "No scanning, enablement or reload was requested. Configuration, inventory\nand the selected executable were preserved. Start uses plain idle daemon;\nits normal configuration/root/recovery bookkeeping can change. Stop leaves\nthe descriptor and its future-login effects in place. Paths can change\nafter checks. Inspect uncertain requests before deciding to retry. Native\ninstalled-manager/login/logout acceptance remains open.")
	return err
}
