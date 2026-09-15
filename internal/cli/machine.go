package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

const APIVersion = 1

type usageError struct{ error }

// JSON is accepted before/after the command without consuming flag values or
// tokens after --. Both output modes share operation implementations.
func Run(ctx context.Context, args []string, out, errOut io.Writer) int {
	filtered := make([]string, 0, len(args))
	machine := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			filtered = append(filtered, args[i:]...)
			break
		}
		if a == "--json" {
			machine = true
			continue
		}
		filtered = append(filtered, a)
		if (a == "--data-dir" || a == "--root" || a == "--exclude") && i+1 < len(args) {
			i++
			filtered = append(filtered, args[i])
		}
	}
	if !machine {
		return runHuman(ctx, args, out, errOut)
	}
	return runMachine(ctx, filtered, out, errOut)
}

func emit(out, errOut io.Writer, value any, code int) int {
	if err := json.NewEncoder(out).Encode(value); err != nil {
		fmt.Fprintf(errOut, "write JSON: %v\n", err)
		return 1
	}
	return code
}
func machineFailure(out, errOut io.Writer, command, code, message string, exit int) int {
	return emit(out, errOut, map[string]any{"api_version": APIVersion, "ok": false, "command": command, "error": map[string]string{"code": code, "message": message}}, exit)
}
func operationFailure(out, errOut io.Writer, command string, err error) int {
	var usage usageError
	if errors.As(err, &usage) {
		return machineFailure(out, errOut, command, "invalid_arguments", err.Error(), 2)
	}
	code := "command_failed"
	switch {
	case errors.Is(err, worker.ErrNotRunning):
		code = "worker_not_running"
	case errors.Is(err, localfs.ErrLocked):
		code = "writer_busy"
	case errors.Is(err, os.ErrNotExist):
		code = "not_found"
	case errors.Is(err, os.ErrExist):
		code = "already_exists"
	case errors.Is(err, os.ErrPermission):
		code = "permission_denied"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = "canceled"
	}
	return machineFailure(out, errOut, command, code, err.Error(), 1)
}

func runMachine(ctx context.Context, args []string, out, errOut io.Writer) int {
	f := flag.NewFlagSet("rydd", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	dataDir := f.String("data-dir", "", "state directory")
	version := f.Bool("version", false, "")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return emit(out, errOut, capabilities(), 0)
		}
		return machineFailure(out, errOut, "", "invalid_arguments", err.Error(), 2)
	}
	if *version {
		return emit(out, errOut, map[string]any{"api_version": APIVersion, "ok": true, "command": "version", "version": "dev"}, 0)
	}
	a := f.Args()
	if len(a) == 0 {
		return emit(out, errOut, capabilities(), 0)
	}
	command := a[0]
	if (command == "config" || command == "state") && len(a) > 1 {
		command += " " + a[1]
	}
	invalid := func(message string) int { return machineFailure(out, errOut, command, "invalid_arguments", message, 2) }
	if command == "capabilities" {
		if len(a) != 1 {
			return invalid("capabilities takes no arguments")
		}
		return emit(out, errOut, capabilities(), 0)
	}
	if command == "daemon" {
		return machineFailure(out, errOut, command, "unsupported_output", "daemon is a foreground text command; use status --json for observations", 2)
	}
	switch command {
	case "status", "pause", "resume", "stop":
		if len(a) != 1 {
			return invalid("unexpected arguments")
		}
	case "config check", "state init":
		if len(a) != 2 {
			return invalid("unexpected arguments")
		}
	case "init":
	default:
		return invalid("unknown command; use capabilities --json")
	}
	paths, err := config.ResolvePaths(*dataDir)
	if err != nil {
		return operationFailure(out, errOut, command, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return operationFailure(out, errOut, command, err)
	}
	result := map[string]any{"api_version": APIVersion, "ok": true, "command": command}
	switch command {
	case "status":
		var data bytes.Buffer
		err = status(ctx, []string{"--json"}, paths, home, &data, io.Discard)
		if err == nil {
			err = json.Unmarshal(data.Bytes(), &result)
		}
	case "pause", "resume", "stop":
		var snapshot worker.Snapshot
		snapshot, err = worker.Send(ctx, paths.StateDir, command)
		result["worker"] = snapshot
		result["acknowledged"] = err == nil
	case "config check":
		var c config.Config
		c, err = config.Load(paths.ConfigFile, home)
		result["config_file"] = paths.ConfigFile
		result["roots"] = c.Roots
		result["exclusions"] = c.Excludes
	case "state init":
		var c config.Config
		c, err = config.Load(paths.ConfigFile, home)
		if err == nil {
			err = initializeState(ctx, paths, c)
		}
		result["state_dir"] = paths.StateDir
	case "init":
		err = initialize(ctx, a[1:], paths, home, io.Discard, io.Discard)
		result["config_file"] = paths.ConfigFile
		result["state_dir"] = paths.StateDir
	}
	if err != nil {
		return operationFailure(out, errOut, command, err)
	}
	return emit(out, errOut, result, 0)
}

func capabilities() map[string]any {
	type command struct {
		Name      string   `json:"name"`
		JSON      bool     `json:"json"`
		Effect    string   `json:"effect"`
		Arguments []string `json:"arguments"`
	}
	return map[string]any{
		"api_version": APIVersion, "ok": true, "command": "capabilities", "noninteractive": true,
		"commands": []command{
			{"init", true, "writes_configuration_and_state", []string{"--root PATH (repeatable)", "--exclude PATH (repeatable)"}},
			{"config check", true, "read_only", []string{}}, {"state init", true, "writes_state", []string{}},
			{"status", true, "read_only", []string{}}, {"pause", true, "writes_state", []string{}}, {"resume", true, "writes_state", []string{}},
			{"stop", true, "stops_worker", []string{}}, {"daemon", false, "runs_worker", []string{"--experimental-scan"}}, {"capabilities", true, "read_only", []string{}},
		},
		"exit_codes":  map[string]string{"0": "success", "1": "operation_failed", "2": "invalid_usage_or_output"},
		"error_codes": []string{"invalid_arguments", "unsupported_output", "worker_not_running", "writer_busy", "not_found", "already_exists", "permission_denied", "canceled", "command_failed"},
		"features":    map[string]bool{"experimental_inventory": true, "durable_dispatch_limits": true, "wal_backpressure": true, "entry_rate_limit": true, "metadata_api_counters": true, "metadata_rate_limit": false, "cpu_limit": false, "power_controls": false, "findings": false, "duplicates": false, "cleanup": false, "service_installation": false},
	}
}
