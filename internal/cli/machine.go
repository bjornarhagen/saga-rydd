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
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

const APIVersion = 1

type usageError struct{ error }

// JSON is accepted before/after the command without consuming flag values or
// tokens after --. Both output modes share operation implementations.
func Run(ctx context.Context, args []string, out, errOut io.Writer) int {
	return runWithInput(ctx, args, os.Stdin, out, errOut)
}

// Input injection is private: non-file test readers must not block. Native
// stdin uses a cancellable descriptor reader in the guided review command.
func runWithInput(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	filtered := make([]string, 0, len(args))
	machine := false
	command := ""
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
		if command == "" && len(a) > 0 && a[0] != '-' {
			command = a
		}
		if (a == "--data-dir" || a == "-data-dir" || a == "--root" || a == "--exclude" || a == "--limit" || a == "--cursor" || a == "--directory" || a == "-directory" || a == "-d" || a == "--d" || a == "-s" || a == "--sleep" || a == "--min-age-days" || a == "--min-size-bytes" || a == "--batches" || a == "--show" || a == "-show" || a == "--from" || a == "-from" || a == "--observe" || a == "--check" || a == "--approve" || a == "-approve" || a == "--run" || a == "-run" || a == "--max-day-bytes" || a == "-max-day-bytes" || a == "--max-total-bytes" || a == "-max-total-bytes" || a == "--revoke" || a == "-revoke" || a == "--verify" || a == "--inspect" || a == "--capture" || a == "--compare" || a == "--work" || a == "-work" || (command == "hashes" && (a == "--preview" || a == "-preview" || a == "--keeper" || a == "-keeper" || a == "--choice" || a == "-choice")) || (command == "hash" && (a == "--save-choice" || a == "-save-choice" || a == "--check-choice" || a == "-check-choice" || a == "--request-choice" || a == "-request-choice" || a == "--save-choice-job" || a == "-save-choice-job" || a == "--job-key" || a == "-job-key" || a == "--show-job" || a == "-show-job" || a == "--approve-job" || a == "-approve-job" || a == "--show-job-read" || a == "-show-job-read" || a == "--revoke-job" || a == "-revoke-job" || a == "--run-job" || a == "-run-job" || a == "--keeper" || a == "-keeper"))) && i+1 < len(args) {
			i++
			filtered = append(filtered, args[i])
		}
	}
	if !machine {
		return runHuman(ctx, args, in, out, errOut)
	}
	return runMachine(ctx, filtered, out, errOut)
}

func emit(out, errOut io.Writer, value any, code int) int {
	if err := json.NewEncoder(&reviewOutput{writer: out}).Encode(value); err != nil {
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
	if errors.As(err, &usage) || errors.Is(err, inventory.ErrHashReadLimits) || errors.Is(err, inventory.ErrHashReadConfirmation) || errors.Is(err, inventory.ErrHashSelectionID) || errors.Is(err, inventory.ErrHashKeeperRequest) || errors.Is(err, inventory.ErrHashKeeperChoiceID) || errors.Is(err, inventory.ErrHashFreshJobKey) || errors.Is(err, inventory.ErrHashFreshJobID) || errors.Is(err, inventory.ErrHashFreshReadConfirmation) || errors.Is(err, inventory.ErrHashFreshReadLimits) {
		return machineFailure(out, errOut, command, "invalid_arguments", err.Error(), 2)
	}
	code := "command_failed"
	var deferred hashRunDeferredError
	switch {
	case errors.As(err, &deferred):
		code = deferred.code
	case errors.Is(err, inventory.ErrHashKeeperChoiceMetadataLocator), errors.Is(err, inventory.ErrHashKeeperChoiceMetadataRequest):
		code = "hash_choice_metadata_unavailable"
	case errors.Is(err, inventory.ErrHashKeeperChoiceFreshRequestLocator), errors.Is(err, inventory.ErrHashKeeperChoiceFreshRequestEvidence):
		code = "hash_choice_request_unavailable"
	case errors.Is(err, inventory.ErrHashFreshRunBinding):
		code = "fresh_read_consent_required"
	case errors.Is(err, inventory.ErrHashFreshProgressCorrupt):
		code = "fresh_hash_progress_invalid"
	case errors.Is(err, inventory.ErrHashFreshReadWindow):
		code = "fresh_read_window_too_short"
	case errors.Is(err, inventory.ErrHashFreshReadBinding):
		code = "fresh_read_consent_required"
	case errors.Is(err, inventory.ErrHashFreshReadApprovalConflict):
		code = "fresh_read_consent_conflict"
	case errors.Is(err, inventory.ErrHashFreshReadApprovalMissing):
		code = "not_found"
	case errors.Is(err, inventory.ErrHashFreshReadCorrupt):
		code = "fresh_read_consent_invalid"
	case errors.Is(err, inventory.ErrHashFreshJobEvidence):
		code = "hash_fresh_job_evidence_changed"
	case errors.Is(err, inventory.ErrHashFreshJobConflict):
		code = "hash_fresh_job_conflict"
	case errors.Is(err, inventory.ErrHashFreshJobCapacity):
		code = "hash_fresh_job_capacity"
	case errors.Is(err, inventory.ErrHashFreshJobCorrupt):
		code = "hash_fresh_job_invalid"
	case errors.Is(err, inventory.ErrHashKeeperChoiceEvidence):
		code = "hash_choice_evidence_changed"
	case errors.Is(err, inventory.ErrHashKeeperChoiceCorrupt):
		code = "hash_choice_invalid"
	case errors.Is(err, inventory.ErrHashKeeperChoiceCapacity):
		code = "hash_choice_capacity"
	case errors.Is(err, inventory.ErrHashStoreCorrupt):
		code = "hash_invalid"
	case errors.Is(err, inventory.ErrHashKeeperSelection):
		code = "hash_preview_unavailable"
	case errors.Is(err, inventory.ErrHashKeeperIdentity):
		code = "hash_preview_identity_ambiguous"
	case errors.Is(err, inventory.ErrHashSelectionConflict):
		code = "already_exists"
	case errors.Is(err, inventory.ErrHashReadApprovalConflict):
		code = "already_exists"
	case errors.Is(err, inventory.ErrHashReadApprovalMissing):
		code = "not_found"
	case errors.Is(err, inventory.ErrHashReadExpired):
		code = "read_consent_expired"
	case errors.Is(err, inventory.ErrHashReadRevoked):
		code = "read_consent_revoked"
	case errors.Is(err, inventory.ErrHashReadClockRollback):
		code = "clock_rollback"
	case errors.Is(err, inventory.ErrHashReadBinding):
		code = "read_consent_required"
	case errors.Is(err, inventory.ErrHashInventoryChanged):
		code = "hash_inventory_changed"
	case errors.Is(err, inventory.ErrHashRecoveryRequired):
		code = "hash_recovery_required"
	case errors.Is(err, plans.ErrReviewEvidence):
		code = "review_evidence_changed"
	case errors.Is(err, plans.ErrReviewTerminal):
		code = "review_unavailable"
	case errors.Is(err, plans.ErrReviewCorrupt):
		code = "review_invalid"
	case errors.Is(err, plans.ErrJournalCorrupt):
		code = "journal_outcome_unknown"
	case errors.Is(err, plans.ErrObservationConflict):
		code = "observation_conflict"
	case errors.Is(err, plans.ErrObservationCorrupt):
		code = "observation_invalid"
	case errors.Is(err, plans.ErrObservationEvidence):
		code = "observation_unavailable"
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
	if command == "review" {
		return machineFailure(out, errOut, command, "unsupported_output", "review prompts in text mode; use report --candidates --json, plan or hashes commands for machine output", 2)
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
	case "init", "report", "scan", "measure", "plan", "journal", "hashes", "hash":
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
	case "scan":
		var r ScanReport
		r, err = scan(ctx, a[1:], paths, io.Discard)
		result["scan"] = r
	case "measure":
		var r MeasureReport
		r, err = measure(ctx, a[1:], paths)
		result["measure"] = r
	case "plan":
		var r any
		r, err = plan(ctx, a[1:], paths)
		result["plan"] = r
	case "journal":
		var r any
		r, err = journal(ctx, a[1:], paths)
		result["journal"] = r
	case "hashes":
		var r any
		r, err = hashes(ctx, a[1:], paths)
		result["hashes"] = r
	case "hash":
		var r any
		r, err = hash(ctx, a[1:], paths)
		result["hash"] = r
	case "report":
		var r any
		r, err = report(ctx, a[1:], paths)
		result["report"] = r
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
	exitCode := emit(out, errOut, result, 0)
	if command == "hash" {
		switch choice := result["hash"].(type) {
		case inventory.SavedHashKeeperChoice:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Historical choice %s was saved. Reopen it with hashes --choice %s; no cleanup was authorized.\n", choice.ID, choice.ID)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Historical choice %s was saved, but its reply was canceled. Reopen it with hashes --choice %s: %v\n", choice.ID, choice.ID, ctx.Err())
				return 1
			}
		case inventory.HashKeeperChoiceMetadataReport:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Metadata screen reply for choice %s failed; no screen result was saved.\n", choice.ChoiceID)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Metadata screen reply for choice %s was canceled; no screen result was saved: %v\n", choice.ChoiceID, ctx.Err())
				return 1
			}
		case inventory.HashKeeperChoiceFreshRequestReport:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Fresh-read request reply for choice %s failed; no request was saved.\n", choice.ChoiceID)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Fresh-read request reply for choice %s was canceled; no request was saved: %v\n", choice.ChoiceID, ctx.Err())
				return 1
			}
		case HashFreshJobKeyResult:
			if exitCode == 0 && ctx.Err() != nil {
				fmt.Fprintf(errOut, "Fresh job key reply was canceled; nothing was saved: %v\n", ctx.Err())
				return 1
			}
		case HashFreshStepReport:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Fresh job %s step reply failed. Inspect hash --show-job %s before another explicit run.\n", choice.Result.JobID, choice.Result.JobID)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Fresh job %s step reply was canceled. Inspect hash --show-job %s before another explicit run: %v\n", choice.Result.JobID, choice.Result.JobID, ctx.Err())
				return 1
			}
		case HashFreshReadConsentResult:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Fresh consent %s exists. Inspect hash --show-job-read %s before retrying the same request.\n", choice.ReadConsent.ID, choice.ReadConsent.ID)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Fresh consent %s reply was canceled. Inspect hash --show-job-read %s: %v\n", choice.ReadConsent.ID, choice.ReadConsent.ID, ctx.Err())
				return 1
			}
		case HashFreshChoiceJobResult:
			if exitCode != 0 {
				fmt.Fprintf(errOut, "Fresh job %s exists. Inspect hash --show-job %s or repeat the same choice with --job-key %s; no source read was authorized.\n", choice.Job.ID, choice.Job.ID, choice.Job.Record.JobKey)
			} else if ctx.Err() != nil {
				fmt.Fprintf(errOut, "Fresh job %s reply was canceled. Inspect hash --show-job %s or repeat the same choice with --job-key %s: %v\n", choice.Job.ID, choice.Job.ID, choice.Job.Record.JobKey, ctx.Err())
				return 1
			}
		}
	}
	return exitCode
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
		"interactive_text_commands": []string{"review"},
		"review_contract":           map[string]any{"name": plans.ReviewContract, "validity_hours": 24, "scope": "review_consent_only", "executable": false, "renewed_approval_required_for_execution": true},
		"commands": []command{
			{"init", true, "writes_configuration_and_state", []string{"--root PATH (repeatable)", "--exclude PATH (repeatable)"}},
			{"config check", true, "read_only", []string{}}, {"state init", true, "writes_state", []string{}},
			{"scan", true, "scans_metadata_and_writes_isolated_state", []string{"-d PATH / --directory PATH", "-s MS / --sleep MS (default 10)", "--now (no entry delay)", "--compact / --detailed (saved manual inventory mode)"}},
			{"measure", true, "writes_derived_state", []string{"-d PATH / --directory PATH (exact manual compact root)", "--batches N (1–1000; default 128; five-second budget)"}},
			{"review", false, "prompts_and_writes_unapproved_selection", []string{"-d PATH / --directory PATH (exact manual scan root)", "--min-age-days N (1–36500; default 90)", "numbered candidate subset then explicit unapproved save; no cleanup", "--hashes (exclusive saved historical group/keeper/copy review; no source reads or saved choice by default)", "--hashes --save-choice (explicit save confirmation preserves exact historical roles; no read consent or cleanup)", "text prompts only; use hashes for finite JSON"}},
			{"plan", true, "read_only_or_writes_saved_plan", []string{"--preview / --save (exactly one for a new selection)", "--show PLAN_ID (reopen without directory or selection options)", "--check PLAN_ID [-d PATH] (compare with saved inventory; no approval)", "--verify PLAN_ID [-d PATH] (read selected live metadata; no contents or cleanup)", "--inspect PLAN_ID [--tree] [-d PATH] (read bounded npm project inputs; optional tree metadata listings, no ordinary dependency contents; reinstall remains unverified)", "--capture PLAN_ID [-d PATH] (save one immutable input/tree observation; no approval)", "--compare OBSERVATION_ID [-d PATH] (read-only input/tree comparison with captured baseline; local edits remain unknown)", "--approve PLAN_ID [-d PATH] --confirm-project-review --confirm-quarantine (24-hour review consent only; cannot execute cleanup)", "--revoke PLAN_ID (revoke review consent without inventory)", "-d PATH / --directory PATH (exact manual scan root)", "--min-age-days N (1–36500; default 90)", "FINDING_ID... (1–20 unique IDs; options first)"}},
			{"journal", true, "read_only", []string{"--show INTENT_ID (saved preparation/history only; no source operations)", "--observe INTENT_ID (current recovery-location metadata; no operations or saved changes)"}},
			{"hashes", true, "read_only", []string{"--work WORK_ID (optional saved work ID 1–20; whole-selection budget remains visible)", "--groups (matching completed historical hashes)", "--preview SELECTION_ID --keeper WORK_ID COPY_ID... (ephemeral possible roles for an explicit matching subset; no saved decision, approval or savings)", "--choice CHOICE_ID (reopen exact immutable historical roles; no evidence refresh or approval)", "modes are mutually exclusive; flags precede copy IDs", "no source/inventory/configuration reads, recovery or dispatch"}},
			{"hash", true, "saved_records_or_guarded_content_read", []string{"--select -d ROOT --from REPORT_JSON FILE_ID... (one existing same-size JSON page, at most 1 MiB; 1–20 unique exact saved file IDs; metadata only)", "--show SELECTION_ID (full saved proposal; no source/inventory reads)", "--save-choice SELECTION_ID --keeper WORK_ID COPY_ID... (immutable historical roles; at most 128 choices of 256 KiB; exact retries preserve ID/time; no source reads, read consent or cleanup)", "--check-choice CHOICE_ID (2–20 exact saved roles; sequential metadata checks under a cooperative five-second context; current configuration and saved inventory read; zero selected file-body reads; no saved result, content verification or approval)", "--request-choice CHOICE_ID (saved-only unapproved fresh full-read request; exact ordered roles/frozen evidence; no job, persistence, source/configuration access or consent)", "--new-job-key (generate only an explicit unsaved retry key; no storage access)", "--save-choice-job CHOICE_ID --job-key KEY (independent unapproved job; exact-key retry; at most 128 jobs of 2 MiB; pending zero progress; no source/configuration access, consent or recovery)", "--show-job JOB_ID (existing saved fresh work/context; no migration/recovery/source access)", "--approve-job JOB_ID --confirm-content-read --max-day-bytes N --max-total-bytes N (separate fresh 24-hour consent, fixed 1 MiB step; exact job/key/request; explicit caps; no content read or recovery)", "--show-job-read APPROVAL_ID (saved fresh consent only; current permission not evaluated)", "--revoke-job APPROVAL_ID (fresh consent only; no source access)", "--run-job APPROVAL_ID (one guarded fresh step under one five-second context; at most 1 MiB; new SHA/accounting; exact-job recovery only)", "--approve SELECTION_ID --confirm-content-read --max-day-bytes N --max-total-bytes N (fixed 24-hour full-file read consent; no content read)", "--run APPROVAL_ID (one guarded step, at most 1 MiB under a cooperative five-second context; frozen scope only)", "--revoke APPROVAL_ID (saved revocation without source/inventory reads)", "no cleanup, automatic loop or implicit retry"}},
			{"report", true, "read_only", []string{"--limit N (1–200)", "--cursor TOKEN", "-d PATH / --directory PATH (saved folder size; combine with --candidates or --same-size for exact manual scan root)", "--candidates [--min-age-days N] [--cursor TOKEN] (old node_modules review)", "--same-size [--min-size-bytes N] [--limit N] [--cursor TOKEN] (saved size bands; contents unchecked)"}},
			{"status", true, "read_only", []string{}}, {"pause", true, "writes_state", []string{}}, {"resume", true, "writes_state", []string{}},
			{"stop", true, "stops_worker", []string{}}, {"daemon", false, "runs_worker", []string{"--experimental-scan"}}, {"capabilities", true, "read_only", []string{}},
		},
		"exit_codes":  map[string]string{"0": "success", "1": "operation_failed", "2": "invalid_usage_or_output"},
		"error_codes": []string{"invalid_arguments", "unsupported_output", "worker_not_running", "writer_busy", "not_found", "already_exists", "permission_denied", "canceled", "command_failed", "review_evidence_changed", "review_unavailable", "review_invalid", "observation_conflict", "observation_invalid", "observation_unavailable", "journal_outcome_unknown", "hash_invalid", "hash_preview_unavailable", "hash_preview_identity_ambiguous", "hash_choice_evidence_changed", "hash_choice_invalid", "hash_choice_capacity", "hash_choice_metadata_unavailable", "hash_choice_request_unavailable", "hash_fresh_job_evidence_changed", "hash_fresh_job_conflict", "hash_fresh_job_capacity", "hash_fresh_job_invalid", "fresh_read_consent_required", "fresh_read_consent_conflict", "fresh_read_consent_invalid", "fresh_hash_progress_invalid", "fresh_read_window_too_short", "read_consent_required", "read_consent_expired", "read_consent_revoked", "clock_rollback", "daily_byte_limit", "lifetime_byte_limit", "durable_quantum", "hash_inventory_changed", "hash_recovery_required"},
		"features":    map[string]bool{"compact_manual_scan": true, "manual_scan": true, "experimental_inventory": true, "durable_dispatch_limits": true, "wal_backpressure": true, "entry_rate_limit": true, "metadata_api_counters": true, "metadata_rate_limit": false, "cpu_limit": false, "power_controls": false, "file_reports": true, "directory_size_reports": true, "findings": true, "plan_previews": true, "saved_plans": true, "guided_review": true, "guided_hash_review": true, "saved_hash_choices": true, "saved_hash_choice_metadata_checks": true, "fresh_hash_choice_requests": true, "saved_fresh_hash_jobs": true, "fresh_hash_read_consent": true, "guarded_fresh_hash_steps": true, "saved_plan_checks": true, "plan_live_checks": true, "plan_input_inspection": true, "plan_tree_inspection": true, "plan_observation_capture": true, "plan_observation_comparison": true, "plan_approval": true, "journal_records": true, "journal_location_observations": true, "same_size_candidates": true, "saved_hash_reports": true, "saved_hash_groups": true, "hash_keeper_previews": true, "saved_hash_proposals": true, "hash_read_consent": true, "guarded_hash_steps": true, "full_hashing": true, "duplicates": false, "cleanup": false, "service_installation": false},
	}
}
