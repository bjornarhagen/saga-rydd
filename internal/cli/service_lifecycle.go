package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

type serviceHost struct {
	GOOS, HomeDir, XDGConfigHome, XDGRuntimeDir string
	UID                                         int
}

// The manager's directory is independent of the child's selected data directory.
// This resolver supplies strings only; Install/Inspect own validation and I/O.
func serviceInstallSpec(paths config.Paths, executable, directory string, host serviceHost) service.InstallSpec {
	if directory == "" {
		if host.GOOS == "darwin" {
			directory = filepath.Join(host.HomeDir, "Library", "LaunchAgents")
		} else {
			base := host.XDGConfigHome
			if base == "" {
				base = filepath.Join(host.HomeDir, ".config")
			}
			directory = filepath.Join(base, "systemd", "user")
		}
	}
	busSocket := ""
	if host.GOOS == "linux" {
		base := host.XDGRuntimeDir
		if base == "" {
			base = filepath.Join("/run/user", strconv.Itoa(host.UID))
		}
		busSocket = filepath.Join(base, "bus")
	}
	runtimeDir := os.Getenv("RYDD_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = "/tmp"
	}
	return service.InstallSpec{
		Service: service.Spec{GOOS: host.GOOS, Executable: executable, Paths: paths, RuntimeDir: runtimeDir},
		HomeDir: host.HomeDir, Directory: directory, BusSocket: busSocket,
	}
}

func serviceLifecycleArguments(args []string) (action, executable, directory string, err error) {
	if len(args) == 0 || (args[0] != "install" && args[0] != "status" && args[0] != "start" && args[0] != "stop") {
		return "", "", "", usageError{errors.New("use service preview, install, status, start or stop with --executable ABSOLUTE_PATH")}
	}
	action = args[0]
	flags := flag.NewFlagSet("service "+action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&executable, "executable", "", "exact trusted executable")
	flags.StringVar(&directory, "directory", "", "exact supported user-service directory")
	if parseErr := flags.Parse(args[1:]); parseErr != nil {
		return "", "", "", usageError{parseErr}
	}
	counts := map[string]int{}
	for _, argument := range args[1:] {
		for _, name := range []string{"executable", "directory"} {
			if argument == "--"+name || argument == "-"+name || strings.HasPrefix(argument, "--"+name+"=") || strings.HasPrefix(argument, "-"+name+"=") {
				counts[name]++
			}
		}
	}
	if flags.NArg() != 0 || counts["executable"] != 1 || counts["directory"] > 1 || executable == "" || (counts["directory"] == 1 && directory == "") {
		return "", "", "", usageError{errors.New("service requires one --executable ABSOLUTE_PATH and at most one --directory ABSOLUTE_PATH")}
	}
	return action, executable, directory, nil
}

func dispatchService(ctx context.Context, args []string, paths config.Paths) (any, error) {
	if len(args) > 0 && args[0] == "preview" {
		return servicePreview(ctx, args, paths)
	}
	action, executable, directory, err := serviceLifecycleArguments(args)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, errors.New("user home directory is unavailable for service placement")
	}
	host := serviceHost{GOOS: runtime.GOOS, HomeDir: homeDir, XDGConfigHome: os.Getenv("XDG_CONFIG_HOME"), XDGRuntimeDir: os.Getenv("XDG_RUNTIME_DIR"), UID: os.Geteuid()}
	spec := serviceInstallSpec(paths, executable, directory, host)
	if action == "start" || action == "stop" {
		return runServiceRuntime(ctx, action, spec)
	}
	return runServiceLifecycle(ctx, action, spec)
}

func runServiceLifecycle(ctx context.Context, action string, spec service.InstallSpec) (service.LifecycleResult, error) {
	var result service.LifecycleResult
	var err error
	if action == "install" {
		result, err = service.Install(ctx, spec)
	} else {
		result, err = service.Inspect(ctx, spec)
	}
	if errors.Is(err, service.ErrSpec) {
		return result, usageError{err}
	}
	return result, err
}

func printServiceResult(out io.Writer, result any) error {
	if descriptor, ok := result.(service.Descriptor); ok {
		return printServicePreview(out, descriptor)
	}
	if request, ok := result.(service.RuntimeResult); ok {
		return printServiceRuntime(out, request)
	}
	r, ok := result.(service.LifecycleResult)
	if !ok {
		return errors.New("service result is unavailable")
	}
	w := &reviewOutput{writer: out}
	membership := "Not checked"
	if r.Manager.DirectoryInUnitPath != nil {
		membership = strconv.FormatBool(*r.Manager.DirectoryInUnitPath)
	}
	_, err := fmt.Fprintf(w, "SERVICE ARTIFACT %s\n\nDescriptor: %q\nArtifact state: %s\nPublication: %s\nDirectory sync in this command: %t\nManager UnitPath observation: %s\nDirectory in manager UnitPath: %s\nCoordinator lock created: %t\n\nNo enable, start or scanning command was issued. Runtime state and\nenablement were not checked. Configuration, inventory/history and the\nselected executable were preserved. Their contents were not checked,\nand selected paths can later change.\n", strings.ToUpper(r.Action), r.DescriptorPath, r.ArtifactStatus, r.Publication, r.SyncCompleted, r.Manager.Status, membership, r.LockCreated)
	if err != nil {
		return err
	}
	if r.FutureLoginMayStart && (r.ArtifactStatus == "exact" || r.Publication == "uncertain") {
		if _, err = fmt.Fprintln(w, "This macOS login-directory publication can start the idle worker at a\nfuture login. Plain daemon loads configuration and updates root/recovery\nbookkeeping; it does not enable the scanner."); err != nil {
			return err
		}
	}
	for _, directory := range r.DirectoriesCreated {
		if _, err = fmt.Fprintf(w, "Created directory: %q\n", directory); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(w, "Use the same executable, data directory and service directory for status.\nNative user-manager/login/logout acceptance remains open.")
	return err
}

func serviceReplyMessage(result any) string {
	if r, ok := result.(service.RuntimeResult); ok && r.DescriptorPath != "" {
		message := fmt.Sprintf("Service %s request status is %s for managed label %q. Request attempted: %t. Running/stopped state is unverified; do not automatically retry an uncertain request. Descriptor: %q. Configuration, inventory/history and executable were preserved", r.Action, r.RequestStatus, r.Label, r.RequestAttempted, r.DescriptorPath)
		if r.UnitLoadAttempted {
			message += "; preflight unit loading was attempted and manager bookkeeping can change"
		}
		return message
	}
	if r, ok := result.(service.LifecycleResult); ok && r.DescriptorPath != "" {
		var message strings.Builder
		fmt.Fprintf(&message, "Service artifact publication is %s at %q. Inspect service status with the same scope before retrying; no enable/start/scanning command was issued", r.Publication, r.DescriptorPath)
		for _, directory := range r.DirectoriesCreated {
			fmt.Fprintf(&message, "\nCreated directory: %q", directory)
		}
		if r.LockCreated {
			fmt.Fprintf(&message, "\nCreated coordinator lock in service directory: %q", r.Directory)
		}
		return message.String()
	}
	return "Service preview changed no service or state"
}

func serviceErrorCode(err error) string {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	case errors.Is(err, service.ErrRuntimeOutcome):
		return "service_outcome_unknown"
	case errors.Is(err, service.ErrRuntimeBinding):
		return "service_runtime_binding"
	case errors.Is(err, service.ErrArtifactPublication):
		return "service_outcome_unknown"
	case errors.Is(err, service.ErrArtifactConflict):
		return "service_artifact_conflict"
	case errors.Is(err, service.ErrArtifactChanged):
		return "service_artifact_changed"
	case errors.Is(err, service.ErrArtifactBusy):
		return "service_artifact_busy"
	case errors.Is(err, service.ErrManagerUnavailable):
		return "service_manager_unavailable"
	case errors.Is(err, service.ErrManagerProtocol):
		return "service_manager_protocol"
	case errors.Is(err, service.ErrManagerPath):
		return "service_manager_path"
	case errors.Is(err, service.ErrLifecycleBounds):
		return "service_bounds"
	case errors.Is(err, os.ErrNotExist):
		return "not_found"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	default:
		return "command_failed"
	}
}

// Retain publication stages even when an operation fails. External helper
// output is never included in this envelope's error message.
func serviceMachineFailure(out, errOut io.Writer, result any, err error) int {
	var usage usageError
	if errors.As(err, &usage) {
		return machineFailure(out, errOut, "service", "invalid_arguments", err.Error(), 2)
	}
	code := serviceErrorCode(err)
	message := "Service operation failed; inspect the recorded result before retrying"
	if code == "canceled" {
		message = "Service operation was canceled; inspect the recorded result before retrying"
	}
	envelope := map[string]any{"api_version": APIVersion, "ok": false, "command": "service", "error": map[string]string{"code": code, "message": message}}
	if r, ok := result.(service.LifecycleResult); ok && r.DescriptorPath != "" {
		envelope["service"] = r
	}
	if r, ok := result.(service.RuntimeResult); ok && r.DescriptorPath != "" {
		envelope["service"] = r
	}
	// Use zero only to distinguish successful emission from a failed write;
	// this operation still returns failure. Normal error envelopes keep stderr
	// empty; the envelope already carries all partial publication stages.
	if emit(out, errOut, envelope, 0) != 0 {
		fmt.Fprintln(errOut, serviceReplyMessage(result))
	}
	return 1
}
