// Package cli implements commands without starting implicit background work.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

const help = `Saga — Rydd
A quiet storage cleanup companion for macOS and Linux.

Usage: rydd [--data-dir /absolute/path] <command>

Commands:
  scan -d PATH [-s MS | --now] [--compact | --detailed] [--json]                Foreground metadata scan (default delay 10 ms)
  init --root /path [--root /another] [--exclude /path]  Create config and state
  config check                                         Validate configuration
  exclude --list / --add ABSOLUTE_PATH / --remove ABSOLUTE_PATH [--json]
                                                     Edit existing exclusions for later invocations
  state init                                           Initialize/migrate state from existing config
  status [--json]                                       Read saved state summary
  daemon [--experimental-scan]                         Run the worker (scanning opt-in for fixtures)
  service preview --executable ABSOLUTE_PATH [--json]  Preview an idle-only service descriptor
  service install/status --executable ABSOLUTE_PATH [--directory ABSOLUTE_PATH] [--json]
                                                     Publish or inspect the exact idle descriptor
  service start/stop --executable ABSOLUTE_PATH [--directory ABSOLUTE_PATH] [--json]
                                                     Request idle-service start or stop; runtime unknown
  service uninstall --executable ABSOLUTE_PATH [--directory ABSOLUTE_PATH] [--json]
                                                     Remove the exact descriptor; does not stop a service
  pause / resume                                       Persistently pause or resume work
  stop                                                 Request graceful worker shutdown
  ignore --preview -d ROOT [--min-age-days N] FINDING_ID [--json]
                                                     Preview one exact historical finding dismissal
  ignore --save --from REQUEST_JSON [--json]           Save the exact preview; no cleanup permission
  ignore --show ID / --undo ID [--json]                Show or undo a dismissal offline
  plan --preview [options] FINDING_ID...               Read-only exact-target cleanup preview
  plan --save [options] FINDING_ID...                  Save an unapproved selection
  plan --show PLAN_ID                                 Reopen a saved selection
  plan --check PLAN_ID [-d PATH]                      Compare saved selection with inventory
  plan --verify PLAN_ID [-d PATH]                     Check selected live metadata; no cleanup
  plan --inspect PLAN_ID [--tree] [-d PATH]           Read bounded npm project inputs; no cleanup
    --tree                                            List bounded tree metadata; no ordinary file contents
  plan --capture PLAN_ID [-d PATH]                   Save immutable input/tree observation; no approval
  plan --compare OBSERVATION_ID [-d PATH]            Read-only comparison with captured observation
  plan --approve PLAN_ID [options]                   Record 24-hour review consent; no cleanup
    --confirm-project-review                         Confirm activity, local edits and reinstall review
    --confirm-quarantine                             Accept same-filesystem quarantine without purge
  plan --revoke PLAN_ID                               Revoke review consent; no inventory needed
  journal --show INTENT_ID [--json]                 Read saved preparation/history; no operations
  journal --observe INTENT_ID [--json]              Observe recovery locations; no operations
  hashes [--work WORK_ID | --groups] [--json]       Read saved hash observations or historical matches
  hashes --preview SELECTION_ID --keeper WORK_ID COPY_ID... [--json]
                                                Preview possible roles from saved historical hashes
  hashes --choice CHOICE_ID [--json]                  Reopen an immutable historical keeper/copy choice
  hash --save-choice SELECTION_ID --keeper WORK_ID COPY_ID... [--json]
                                                     Save historical roles; no source reads or cleanup
  hash --check-choice CHOICE_ID [--json]              Screen exact metadata; no file bodies or approval
  hash --request-choice CHOICE_ID [--json]            Review a fresh-read request; nothing is saved
  hash --new-job-key [--json]                        Generate a fresh job retry key; nothing is saved
  hash --save-choice-job CHOICE_ID --job-key KEY      Save an independent unapproved fresh job
  hash --show-job JOB_ID [--json]                    Show saved fresh work; no source access
  hash --approve-job JOB_ID --confirm-content-read --max-day-bytes N --max-total-bytes N
                                                    Save separate fresh consent; no content read
  hash --show-job-read APPROVAL_ID [--json]           Show saved fresh consent; no clock evaluation
  hash --revoke-job APPROVAL_ID [--json]              Revoke fresh consent; no source access
  hash --run-job APPROVAL_ID [--json]                 Run one guarded fresh step; at most 1 MiB
  hash --select -d ROOT --from REPORT_JSON FILE_ID... Save unapproved hash metadata; no source contents
  hash --show SELECTION_ID [--json]                 Show exact saved hash proposal; no source reads
  hash --approve SELECTION_ID --confirm-content-read --max-day-bytes N --max-total-bytes N
                                                     Record fixed full-file read consent; no content read
  hash --run APPROVAL_ID [--json]                    One guarded hash step (at most 1 MiB; no cleanup)
  hash --revoke APPROVAL_ID [--json]                 Revoke read consent; no source or inventory needed
  report --candidates [--include-dismissed] [--min-age-days N] [--cursor TOKEN] [--json]  Node modules review candidates
  report --build-output -d ROOT [--min-age-days N] [--cursor TOKEN] [--json]
                                                     Saved Cargo target layouts; review required
  report --go-cache -d ROOT [--min-age-days N] [--cursor TOKEN] [--json]
                                                     Saved Go build-cache files; review required
  docker --metadata --context NAME [--json]            Finite image/container metadata; no cleanup
  docker --cache-metadata --context NAME [--json]      Engine-embedded cache metadata; no builder pinning
  measure -d PATH [--batches N] [--json]               Resume saved compact size calculations
  review -d PATH [--min-age-days N]                   Choose a numbered subset; save unapproved evidence
  review --hashes                                     Review numbered historical hashes; no saved choice
  review --hashes --save-choice                       Review roles, then type save to preserve the choice
  report -d PATH [--json]                             Saved directory size
  report [--limit N] [--cursor TOKEN] [--json]           Largest observed files
  report --same-size [--min-size-bytes N] [--limit N] [--cursor TOKEN]    Saved size bands; contents unchecked
  capabilities [--json]                                Discover commands and supported features

Options: --help, --version; --json on finite commands

Experimental metadata scanning, charged background scanner API allowances, explicit hashing
and idle-service descriptor controls are available. Full global CPU/I/O/power limits, verified
service runtime state and cleanup remain unavailable.
`

type pathsFlag []string

func (p *pathsFlag) String() string         { return fmt.Sprint([]string(*p)) }
func (p *pathsFlag) Set(value string) error { *p = append(*p, value); return nil }

func runHuman(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("rydd", flag.ContinueOnError)
	flags.SetOutput(errOut)
	dataDir := flags.String("data-dir", "", "private directory for both config and state")
	version := flags.Bool("version", false, "show development version")
	flags.Usage = func() { fmt.Fprint(out, help) }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *version {
		fmt.Fprintln(out, "rydd dev (experimental inventory)")
		return 0
	}
	remaining := flags.Args()
	if len(remaining) == 0 {
		fmt.Fprint(out, help)
		return 0
	}
	if remaining[0] == "capabilities" && len(remaining) == 1 {
		fmt.Fprintln(out, "Rydd commands: init, config check, exclude, state init, status, scan, measure, report, docker, service, review, plan, ignore, journal, hashes, hash, pause, resume, stop, capabilities.\nAdd --json for versioned machine output on finite commands. review prompts in text mode; daemon uses foreground text output.\nOther commands are noninteractive. Exit codes: 0 success, 1 operation failed, 2 invalid usage.\nScanning is experimental. Deletion, duplicate detection, and full resource controls are unavailable.")
		return 0
	}
	paths, err := config.ResolvePaths(*dataDir)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	switch remaining[0] {
	case "service":
		var serviceResult any
		serviceResult, err = dispatchService(ctx, remaining[1:], paths)
		if err == nil {
			err = printServiceResult(out, serviceResult)
		}
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil && serviceResult != nil {
			fmt.Fprintln(errOut, serviceReplyMessage(serviceResult))
		}
	case "docker":
		var r any
		r, err = dispatchDockerMetadata(ctx, remaining[1:])
		if err == nil {
			err = printDockerResult(ctx, out, r)
		}
	case "init":
		err = initialize(ctx, remaining[1:], paths, home, out, errOut)
	case "exclude":
		var r ExcludeReport
		r, err = exclude(ctx, remaining[1:], paths, home)
		if err == nil {
			err = printExcludeResult(out, r, paths)
		}
		if err == nil && ctx.Err() != nil {
			err = fmt.Errorf("%s; reply was canceled: %w", excludeReplyMessage(r, paths), ctx.Err())
		}
	case "config":
		if len(remaining) != 2 || remaining[1] != "check" {
			fmt.Fprintln(errOut, "Usage: rydd config check")
			return 2
		}
		var c config.Config
		c, err = config.Load(paths.ConfigFile, home)
		if err == nil {
			fmt.Fprintf(out, "Configuration valid: %q\n%d root(s), %d exclusion(s). Use scan -d PATH for foreground scans, or daemon --experimental-scan for configured roots.\n", paths.ConfigFile, len(c.Roots), len(c.Excludes))
		}
	case "state":
		if len(remaining) != 2 || remaining[1] != "init" {
			fmt.Fprintln(errOut, "Usage: rydd state init")
			return 2
		}
		var c config.Config
		c, err = config.Load(paths.ConfigFile, home)
		if err == nil {
			err = initializeState(ctx, paths, c)
			if err == nil {
				fmt.Fprintf(out, "State ready: %q\n", paths.StateDir)
			}
		}
	case "scan":
		var r ScanReport
		r, err = scan(ctx, remaining[1:], paths, out)
		if err == nil {
			printScanReport(out, r)
		}
	case "measure":
		var r MeasureReport
		r, err = measure(ctx, remaining[1:], paths)
		if err == nil {
			printMeasureReport(out, r)
		}
	case "plan":
		var r any
		r, err = plan(ctx, remaining[1:], paths)
		if err == nil {
			printPlan(out, r, paths)
		}
	case "ignore":
		var r any
		r, err = ignore(ctx, remaining[1:], paths)
		if err == nil {
			err = printIgnoreResult(out, r, paths)
		}
		if err == nil && ctx.Err() != nil {
			err = fmt.Errorf("%s; reply was canceled: %w", ignoreReplyMessage(r), ctx.Err())
		}
	case "review":
		err = review(ctx, remaining[1:], paths, in, out)
	case "journal":
		var r any
		r, err = journal(ctx, remaining[1:], paths)
		if err == nil {
			printJournal(out, r)
		}
	case "hashes":
		var r any
		r, err = hashes(ctx, remaining[1:], paths)
		if err == nil {
			err = printHashes(out, r)
		}
	case "hash":
		var r any
		r, err = hash(ctx, remaining[1:], paths)
		if err == nil {
			err = printHashResult(out, r)
		}
		if err == nil && ctx.Err() != nil {
			switch choice := r.(type) {
			case inventory.SavedHashKeeperChoice:
				err = fmt.Errorf("choice %s was saved, but its reply was canceled; reopen with hashes --choice %s: %w", choice.ID, choice.ID, ctx.Err())
			case inventory.HashKeeperChoiceMetadataReport:
				err = fmt.Errorf("choice %s metadata screen reply was canceled; no screen result was saved: %w", choice.ChoiceID, ctx.Err())
			case inventory.HashKeeperChoiceFreshRequestReport:
				err = fmt.Errorf("choice %s fresh-read request reply was canceled; no request was saved: %w", choice.ChoiceID, ctx.Err())
			case HashFreshJobKeyResult:
				err = fmt.Errorf("fresh job key reply was canceled; nothing was saved: %w", ctx.Err())
			case HashFreshStepReport:
				err = fmt.Errorf("fresh job %s step reply was canceled; inspect hash --show-job %s before another explicit run: %w", choice.Result.JobID, choice.Result.JobID, ctx.Err())
			case HashFreshReadConsentResult:
				err = fmt.Errorf("fresh consent %s reply was canceled; inspect hash --show-job-read %s: %w", choice.ReadConsent.ID, choice.ReadConsent.ID, ctx.Err())
			case HashFreshChoiceJobResult:
				err = fmt.Errorf("fresh job %s reply was canceled; inspect hash --show-job %s or repeat the same choice with --job-key %s: %w", choice.Job.ID, choice.Job.ID, choice.Job.Record.JobKey, ctx.Err())
			}
		}
	case "report":
		var r savedReportDispatch
		r, err = dispatchSavedReport(ctx, remaining[1:], paths)
		if err == nil {
			if r.BuildOutput != nil || r.GoCache != nil {
				checked := &reviewOutput{writer: out}
				printSavedReport(checked, r)
				err = checked.err
				if err == nil && ctx.Err() != nil {
					category := "Cargo build-output"
					if r.GoCache != nil {
						category = "Go build-cache"
					}
					err = fmt.Errorf("%s report reply was canceled; no saved records or source files were changed: %w", category, ctx.Err())
				}
			} else {
				printSavedReport(out, r)
			}
		}
	case "status":
		err = status(ctx, remaining[1:], paths, home, out, errOut)
	case "daemon":
		scanFlags := flag.NewFlagSet("daemon", flag.ContinueOnError)
		scanFlags.SetOutput(errOut)
		experimental := scanFlags.Bool("experimental-scan", false, "enable metadata scanning for disposable fixtures")
		if e := scanFlags.Parse(remaining[1:]); e != nil || scanFlags.NArg() != 0 {
			fmt.Fprintln(errOut, "Usage: rydd daemon [--experimental-scan]")
			return 2
		}
		var c config.Config
		var startupCheck func(context.Context) error
		c, startupCheck, err = loadDaemonConfiguration(ctx, paths, home)
		if err == nil {
			err = worker.Run(ctx, paths.StateDir, c, worker.Options{ExperimentalScan: *experimental, PrivatePaths: []string{paths.ConfigFile}, StartupCheck: startupCheck, Ready: func(s worker.Snapshot) {
				fmt.Fprintf(out, "Worker ready (PID %d, paused: %t, experimental scan: %t).\n", s.PID, s.Paused, *experimental)
			}})
		}
	case "pause", "resume", "stop":
		if len(remaining) != 1 {
			fmt.Fprintln(errOut, "Control commands take no arguments.")
			return 2
		}
		var live worker.Snapshot
		live, err = worker.Send(ctx, paths.StateDir, remaining[0])
		if err == nil {
			if live.Stopping {
				fmt.Fprintln(out, "Worker is stopping.")
			} else if live.Paused {
				fmt.Fprintln(out, "Worker paused; any current chunk is being canceled.")
			} else {
				fmt.Fprintln(out, "Worker resumed.")
			}
		}
	default:
		fmt.Fprintln(errOut, "Unknown command. Run rydd --help.")
		return 2
	}
	if err != nil {
		var missing missingScanError
		var missingHash missingHashError
		var missingIgnore ignoreUnavailableError
		var missingExclude excludeUnavailableError
		var missingDocker dockerMetadataError
		if errors.As(err, &missing) || errors.As(err, &missingHash) || errors.As(err, &missingIgnore) || errors.As(err, &missingExclude) || errors.As(err, &missingDocker) {
			fmt.Fprintln(errOut, err)
		} else if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(errOut, "Not initialized or unavailable: %v\nUse rydd init --root /path for new configuration, or rydd state init with existing configuration.\n", err)
		} else {
			fmt.Fprintln(errOut, err)
		}
		var usage usageError
		if errors.As(err, &usage) || errors.Is(err, config.ErrExclusionInput) {
			return 2
		}
		return 1
	}
	return 0
}

func initialize(ctx context.Context, args []string, paths config.Paths, home string, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(errOut)
	var roots, excludes pathsFlag
	flags.Var(&roots, "root", "explicit root (repeatable)")
	flags.Var(&excludes, "exclude", "excluded subtree (repeatable)")
	if err := flags.Parse(args); err != nil {
		return usageError{err}
	}
	if flags.NArg() != 0 {
		return usageError{errors.New("unexpected init arguments")}
	}
	c := config.Default()
	c.Roots = roots
	c.Excludes = excludes
	if err := c.Validate(home); err != nil {
		return err
	}
	if err := config.Create(paths.ConfigFile, home, c); err != nil {
		return fmt.Errorf("create config (existing configuration is never overwritten): %w", err)
	}
	if err := initializeState(ctx, paths, c); err != nil {
		return fmt.Errorf("configuration saved, but state setup failed: %w; fix the cause and retry rydd state init", err)
	}
	fmt.Fprintf(out, "Configuration created: %q\nState ready: %q\nNo scanning or cleanup has started.\n", paths.ConfigFile, paths.StateDir)
	return nil
}

func initializeState(ctx context.Context, paths config.Paths, c config.Config) error {
	w, err := state.OpenWriter(ctx, paths.StateDir)
	if err != nil {
		return err
	}
	if err := w.SyncRoots(ctx, c.Roots); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func status(ctx context.Context, args []string, paths config.Paths, home string, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(errOut)
	asJSON := flags.Bool("json", false, "JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected status arguments")
	}
	c, err := config.Load(paths.ConfigFile, home)
	if err != nil {
		return err
	}
	r, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return err
	}
	defer r.Close()
	summary, err := r.Summary(ctx)
	if err != nil {
		return err
	}
	paused, err := r.Paused(ctx)
	if err != nil {
		return err
	}
	budget, err := r.DispatchBudget(ctx, time.Now(), c.Scan.MaxScanChunksPerDay)
	if err != nil {
		return err
	}
	metadata, err := r.MetadataBudget(ctx, time.Now(), c.Scan.MetadataAttemptsPerDay)
	if err != nil {
		return err
	}
	type workerStatus struct {
		State string           `json:"state"`
		Live  *worker.Snapshot `json:"live,omitempty"`
		Error string           `json:"error,omitempty"`
	}
	connection := workerStatus{State: "not-running"}
	if live, e := worker.Send(ctx, paths.StateDir, "status"); e == nil {
		connection.State = "running"
		connection.Live = &live
	} else if !errors.Is(e, worker.ErrNotRunning) {
		connection.State = "unknown"
		connection.Error = e.Error()
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(struct {
			Stage           string               `json:"stage"`
			ConfigFile      string               `json:"config_file"`
			StateDir        string               `json:"state_dir"`
			ConfiguredRoots []string             `json:"configured_roots"`
			State           state.Summary        `json:"state"`
			Paused          bool                 `json:"saved_pause"`
			Worker          workerStatus         `json:"worker"`
			Dispatch        state.DispatchBudget `json:"dispatch_budget"`
			Metadata        state.MetadataBudget `json:"scanner_metadata_budget"`
		}{"experimental-inventory", paths.ConfigFile, paths.StateDir, c.Roots, summary, paused, connection, budget, metadata})
	}
	fmt.Fprintf(out, "Saga — Rydd\nConfig: %q\nState: %q\nSchema: %d (SQLite %s)\nConfigured roots: %d; saved enabled roots: %d\nSaved observations: %d; pending jobs: %d; running jobs: %d\nDatabase: %d bytes; WAL: %d bytes\nWorker: %s; saved pause: %t\nScanning: experimental, opt-in; full resource limits not enforced.\n", paths.ConfigFile, paths.StateDir, summary.Schema, summary.SQLiteVersion, len(c.Roots), summary.EnabledRoots, summary.Entries, summary.PendingJobs, summary.RunningJobs, summary.DatabaseBytes, summary.WALBytes, connection.State, paused)
	fmt.Fprintf(out, "Completed directory passes: %d; directory errors: %d; skipped observations: %d\n", summary.CompleteDirectories, summary.DirectoryErrors, summary.SkippedEntries)
	if connection.Live != nil {
		fmt.Fprintf(out, "Worker PID: %d; paused: %t; stopping: %t; active job: %d\n", connection.Live.PID, connection.Live.Paused, connection.Live.Stopping, connection.Live.ActiveJob)
	}
	fmt.Fprintf(out, "Scan batches reserved today (%s UTC): %d/%d; budget wait: %s\n", budget.Day, budget.Used, budget.Limit, budget.Reason)
	printScannerMetadata(out, &metadata)
	if connection.Live != nil {
		fmt.Fprintf(out, "Worker wait: %s\n", connection.Live.WaitReason)
		printWorkerCPU(out, connection.Live.CPU)
		printWorkerPriority(out, connection.Live.Priority)
		if m := connection.Live.InventoryMetrics; m != nil {
			fmt.Fprintf(out, "Entry inspections: %d; limit: %d/s; throttling: %t; accumulated throttle wait: %s\n", m.EntryInspections, m.EntryRatePerSecond, m.Throttled, time.Duration(m.ThrottleWaitNS))
			fmt.Fprintf(out, "Scanner API calls this worker: stat=%d; directory open=%d; directory read=%d; filesystem stat=%d; mount identity=%d; path resolution=%d\n", m.StatCalls, m.DirectoryOpenCalls, m.DirectoryReadCalls, m.FilesystemStatCalls, m.MountIdentityCalls, m.PathResolutionCalls)
		}
	}
	if connection.Error != "" {
		fmt.Fprintf(out, "Worker connection: %q\n", connection.Error)
	}
	if summary.NeedsBackpressure {
		fmt.Fprintln(out, "WAL exceeds the checkpoint threshold; the worker checks checkpoint progress before further scanning.")
	}
	return nil
}

func printWorkerCPU(out io.Writer, cpu *worker.CPUObservation) {
	if cpu == nil {
		return
	}
	fmt.Fprintf(out, "Worker CPU observation: %s; unknown windows this worker: %d\n", cpu.Status, cpu.UnknownObservations)
	if cpu.WindowCPUNS != nil && cpu.WindowElapsedNS != nil {
		fmt.Fprintf(out, "Process CPU in completed work window: %s; elapsed window: %s\n", time.Duration(*cpu.WindowCPUNS), time.Duration(*cpu.WindowElapsedNS))
		fmt.Fprintf(out, "Added CPU dispatch wait: %s; pacing target: %d%% of one core; wait capped: %t\n", time.Duration(cpu.BackoffNS), cpu.TargetPercent, cpu.BackoffCapped)
	}
	if cpu.Status == "unknown" {
		fmt.Fprintln(out, "CPU time is unknown for this window. No new CPU wait was calculated.")
	}
	fmt.Fprintln(out, "CPU observations cover this process's user and system time during completed work windows. They exclude children, are not saved quotas, and do not enforce an hourly CPU limit.")
}
