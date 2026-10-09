package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// The saved request ID describes an attempted root selection, not a receipt or
// proof of the enabled set. Never render an underlying SQL or transport cause.
type rootAdmissionCLIError struct {
	code, message, requestID, publication        string
	prior, summary, configCommand, reportCommand string
	exit                                         int
	cause                                        error
}

func (e rootAdmissionCLIError) Error() string { return e.message }
func (e rootAdmissionCLIError) Unwrap() error { return e.cause }

func rootAdmissionDiagnostic(command string, paths config.Paths, manualRoot []byte, err error) (rootAdmissionCLIError, bool) {
	matched := false
	for _, sentinel := range []error{state.ErrRootAdmissionInput, state.ErrRootAdmissionCapacity, state.ErrRootAdmissionCorrupt, state.ErrRootAdmissionUnavailable, state.ErrRootAdmissionPublication} {
		matched = matched || errors.Is(err, sentinel)
	}
	if !matched {
		return rootAdmissionCLIError{}, false
	}
	e := rootAdmissionCLIError{exit: 1, cause: err}
	var summary string
	switch {
	case errors.Is(err, state.ErrRootAdmissionPublication):
		e.code, e.publication = "root_outcome_unknown", "unknown"
		summary = "Root synchronization outcome is unknown. The requested enabled-root selection may have been saved."
		var publication *state.RootAdmissionPublicationError
		if errors.As(err, &publication) && publication != nil && state.ValidRootAdmissionRequestID(publication.RequestID) {
			e.requestID = publication.RequestID
		}
	case errors.Is(err, state.ErrRootAdmissionInput):
		e.code, e.exit = "invalid_arguments", 2
		summary = fmt.Sprintf("Root synchronization requires 1–%d unique canonical absolute paths, each at most %d bytes without NUL. The requested root selection was not applied.", state.RootAdmissionInputLimit, state.RootAdmissionPathBytes)
	case errors.Is(err, state.ErrRootAdmissionCapacity):
		e.code = "root_capacity_reached"
		summary = fmt.Sprintf("The requested additions cannot fit within this inventory's %d-record root admission limit. Disabled roots also use capacity. The requested root selection was not applied. Existing roots and saved history remain available; valid existing roots can be reactivated without adding a record.", state.RootAdmissionLimit)
	case errors.Is(err, state.ErrRootAdmissionCorrupt):
		e.code = "root_admission_invalid"
		summary = "Saved root evidence is malformed or ambiguous. The requested root selection was not applied, and saved records were not repaired."
	default:
		e.code = "root_admission_unavailable"
		summary = "Root synchronization could not access the existing state. The requested root selection was not applied."
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		e.code, e.exit = "canceled", 1
		if e.publication == "" {
			summary = "Root synchronization was canceled. The requested root selection was not applied."
		} else {
			summary = "Root synchronization was canceled. " + summary
		}
	}
	var prior string
	switch command {
	case "init":
		prior = "Configuration was saved, but state setup did not complete. "
	case "state init":
		prior = "State setup did not complete. "
	case "scan":
		prior = "Metadata scanning did not start. A prior compact/detailed scan-mode setting may already have completed. "
	case "daemon":
		prior = "Worker startup did not complete. "
	}
	prior += "Earlier state initialization or migration may already have completed. "
	inspect := commandPrefix(paths) + " report"
	if command == "scan" && len(manualRoot) > 0 && len(manualRoot) <= state.RootAdmissionPathBytes {
		inspect += " -d " + shellQuote(string(manualRoot))
	}
	e.prior, e.summary = prior, summary
	e.configCommand, e.reportCommand = commandPrefix(paths)+" config check", inspect+" --json"
	e.message = prior + summary
	if e.requestID != "" {
		e.message += " Request: " + e.requestID + ". This ID does not prove which roots are enabled."
	}
	e.message += " Inspect current configuration with " + e.configCommand + " and the bounded saved view with " + e.reportCommand + " before deciding whether to repeat this explicit command."
	return e, true
}

func printRootAdmissionDiagnostic(out io.Writer, e rootAdmissionCLIError) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, e.prior+e.summary, "")
	if e.requestID != "" {
		fmt.Fprintln(guard, "Request:", e.requestID)
		printWrapped(guard, "This ID does not prove which roots are enabled.", "")
	}
	printWrapped(guard, "Inspect current configuration and the bounded saved view before deciding whether to repeat this explicit command:", "")
	fmt.Fprintln(guard, "  "+e.configCommand)
	fmt.Fprintln(guard, "  "+e.reportCommand)
	return guard.err
}

func rootAdmissionMachineFailure(out, errOut io.Writer, command string, e rootAdmissionCLIError) int {
	detail := map[string]string{"code": e.code, "message": e.message}
	if e.publication != "" {
		detail["publication_outcome"] = e.publication
	}
	if e.requestID != "" {
		detail["request_id"] = e.requestID
	}
	if json.NewEncoder(&reviewOutput{writer: out}).Encode(map[string]any{"api_version": APIVersion, "ok": false, "command": command, "error": detail}) != nil {
		fmt.Fprintln(errOut, "The root synchronization reply could not be written.")
		_ = printRootAdmissionDiagnostic(errOut, e)
		return 1
	}
	return e.exit
}
