package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func runServiceLoginLink(ctx context.Context, action string, spec service.InstallSpec) (service.LoginLinkResult, error) {
	var result service.LoginLinkResult
	var err error
	switch action {
	case "enable-login":
		result, err = service.EnableLoginLink(ctx, spec)
	case "disable-login":
		result, err = service.DisableLoginLink(ctx, spec)
	default:
		return result, usageError{errors.New("use service enable-login or disable-login")}
	}
	if errors.Is(err, service.ErrSpec) {
		return result, usageError{err}
	}
	return result, err
}

func printServiceLoginLink(out io.Writer, r service.LoginLinkResult) error {
	w := &reviewOutput{writer: out}
	if _, err := fmt.Fprintf(w, "SERVICE %s LINK\n\nSelected link change: %s\nChange attempted: %t\nChange syscall completed: %t\nLink parent sync completed: %t\nCreated directory parent sync completed: %t\n\nSelected link: %q\nExact target: %q\nLast checked selected link: %s\nLink present at that check: %s\nLink checked at: %s\nRemoved held link observed: %s\nDescriptor: %q\nDescriptor SHA-256: %s\n\n", strings.ToUpper(r.Action), r.ChangeStatus, r.ChangeAttempted, r.ChangeCompleted, r.SyncCompleted, r.DirectorySyncCompleted, r.LinkPath, r.LinkTarget, r.LinkStatus, serviceLinkBool(r.LinkPresent), serviceLinkTime(r.LinkObservedAt), removalEvidence(r.RemovalObserved), r.DescriptorPath, r.DescriptorSHA256); err != nil {
		return err
	}
	for _, directory := range r.DirectoriesCreated {
		if _, err := fmt.Fprintf(w, "Created directory: %q\n", directory); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Loaded settings matched at preflight: %s\nBinding checked at: %s\nGlobal enablement: Unknown\nRunning state: Unknown\nStopped state: Unknown\nFuture-login behavior: Unknown\n\n", serviceLinkBool(r.LoadedBindingMatched), serviceLinkTime(r.BindingCheckedAt)); err != nil {
		return err
	}
	if r.UnitLoadAttempted {
		if _, err := fmt.Fprintln(w, "Preflight requested unit loading; manager bookkeeping can change."); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(w, "This command addresses one fixed Linux default.target dependency link.\nIts creator is unknown. An identical manually created link at this exact\nname is within the selected scope; other links are preserved. Checked\nlink evidence is historical. It does not prove effective enablement,\nloaded origin, current target dependencies or runtime state. A loaded\ntarget may require a later load or reload to use a changed dependency.\n\nNo manager enable, disable, start, stop or reload request was issued.\nConnecting to the selected local broker socket can activate the broker.\nThe descriptor, configuration, inventory/history, executable and existing\ndirectories and lock were preserved. No scanning or cleanup was requested.\nDisable this link before uninstalling the descriptor. Use the same\nexecutable, data directory and service directory to inspect the artifact\nand exact selected link before deciding to retry an uncertain change.\nNative installed-manager/login/logout acceptance remains open.")
	return err
}

func serviceLinkBool(value *bool) string {
	if value == nil {
		return "Unknown"
	}
	return strconv.FormatBool(*value)
}

func serviceLinkTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "Not recorded"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func serviceLoginLinkReplyMessage(r service.LoginLinkResult) string {
	var message strings.Builder
	fmt.Fprintf(&message, "Selected login link change status is %s at %q, exact target %q. Change attempted: %t; syscall completed: %t; link parent sync completed: %t; created directory parent sync completed: %t. Last checked link state: %s; present: %s; checked at: %s. Descriptor: %q; SHA-256: %s. Inspect this exact link and service artifact with the same executable, data directory and service directory before deciding to retry. Global enablement, runtime and future-login behavior remain unknown; link origin is unverified", r.ChangeStatus, r.LinkPath, r.LinkTarget, r.ChangeAttempted, r.ChangeCompleted, r.SyncCompleted, r.DirectorySyncCompleted, r.LinkStatus, serviceLinkBool(r.LinkPresent), serviceLinkTime(r.LinkObservedAt), r.DescriptorPath, r.DescriptorSHA256)
	for _, directory := range r.DirectoriesCreated {
		fmt.Fprintf(&message, "\nCreated directory: %q", directory)
	}
	if r.UnitLoadAttempted {
		message.WriteString("; preflight unit loading was attempted and manager bookkeeping can change")
	}
	message.WriteString(". No manager enable, disable, start, stop or reload request was issued; data, executable, descriptor, existing directories, lock and other links were preserved")
	return message.String()
}
