package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func runServiceRemoval(ctx context.Context, spec service.InstallSpec) (service.RemovalResult, error) {
	r, err := service.Uninstall(ctx, spec)
	if errors.Is(err, service.ErrSpec) {
		return r, usageError{err}
	}
	return r, err
}

func printServiceRemoval(out io.Writer, r service.RemovalResult) error {
	w := &reviewOutput{writer: out}
	_, err := fmt.Fprintf(w, "SERVICE DESCRIPTOR REMOVAL\n\nDescriptor: %q\nRecorded removal: %s\nUnlink attempted: %t\nUnlink completed: %t\nSelected-file removal observed: %s\nDescriptor name absent: %s\nDirectory sync completed: %t\nRunning state: Unknown\nStopped state: Unknown\n\n", r.DescriptorPath, r.RemovalStatus, r.RemovalAttempted, r.UnlinkCompleted, removalEvidence(r.RemovalObserved), removalEvidence(r.DescriptorAbsent), r.SyncCompleted)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, "This command removes only the exact managed descriptor. It makes no stop,\ndisable, reload or manager request. A loaded service can continue running;\nregistration, enablement and future-login state remain unknown. Configuration,\ninventory/history, quarantine, executable, directories and coordinator lock\nwere preserved. Paths can change after checks. Current absence on a retry\ndoes not prove a previous command removed the selected file.\n\nThe Rydd stop adapter requires the exact descriptor. Request stop before\nuninstall when wanted; accepted stop does not prove shutdown. After removal,\nuse manager-specific inspection or an exact reinstall before that adapter.\nInspect uncertain removal results with the same scope before retrying. Native\ninstalled-manager/login/logout acceptance remains open.")
	return err
}

func removalEvidence(value *bool) string {
	if value == nil {
		return "Unknown"
	}
	if *value {
		return "Observed"
	}
	return "Not observed"
}
