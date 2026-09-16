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
  state init                                           Initialize/migrate state from existing config
  status [--json]                                       Read saved state summary
  daemon [--experimental-scan]                         Run the worker (scanning opt-in for fixtures)
  pause / resume                                       Persistently pause or resume work
  stop                                                 Request graceful worker shutdown
  report --candidates [--cursor TOKEN] [--json]         Node modules review candidates
  report -d PATH [--json]                             Saved directory size
  report [--limit N] [--cursor TOKEN] [--json]           Largest observed files
  capabilities [--json]                                Discover commands and supported features

Options: --help, --version; --json on finite commands

Experimental foreground metadata scanning is available for selected folders. Fine-grained
metadata/content, CPU and power budgets are not enforced yet. Service installation, duplicate
detection and cleanup are not available yet.
`

type pathsFlag []string

func (p *pathsFlag) String() string         { return fmt.Sprint([]string(*p)) }
func (p *pathsFlag) Set(value string) error { *p = append(*p, value); return nil }

func runHuman(ctx context.Context, args []string, out, errOut io.Writer) int {
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
		fmt.Fprintln(out, "Rydd commands: init, config check, state init, status, scan, report, pause, resume, stop, capabilities.\nAdd --json for versioned machine output. daemon is foreground-only and uses text output.\nAll commands are noninteractive. Exit codes: 0 success, 1 operation failed, 2 invalid usage.\nScanning is experimental. Deletion, duplicate detection, and full resource controls are unavailable.")
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
	case "init":
		err = initialize(ctx, remaining[1:], paths, home, out, errOut)
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
	case "report":
		var r state.FileReport
		r, err = report(ctx, remaining[1:], paths)
		if err == nil {
			printReport(out, r)
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
		c, err = config.Load(paths.ConfigFile, home)
		if err == nil {
			err = worker.Run(ctx, paths.StateDir, c, worker.Options{ExperimentalScan: *experimental, PrivatePaths: []string{paths.ConfigFile}, Ready: func(s worker.Snapshot) {
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
		if errors.As(err, &missing) {
			fmt.Fprintln(errOut, err)
		} else if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(errOut, "Not initialized or unavailable: %v\nUse rydd init --root /path for new configuration, or rydd state init with existing configuration.\n", err)
		} else {
			fmt.Fprintln(errOut, err)
		}
		var usage usageError
		if errors.As(err, &usage) {
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
		}{"experimental-inventory", paths.ConfigFile, paths.StateDir, c.Roots, summary, paused, connection, budget})
	}
	fmt.Fprintf(out, "Saga — Rydd\nConfig: %q\nState: %q\nSchema: %d (SQLite %s)\nConfigured roots: %d; saved enabled roots: %d\nSaved observations: %d; pending jobs: %d; running jobs: %d\nDatabase: %d bytes; WAL: %d bytes\nWorker: %s; saved pause: %t\nScanning: experimental, opt-in; full resource limits not enforced.\n", paths.ConfigFile, paths.StateDir, summary.Schema, summary.SQLiteVersion, len(c.Roots), summary.EnabledRoots, summary.Entries, summary.PendingJobs, summary.RunningJobs, summary.DatabaseBytes, summary.WALBytes, connection.State, paused)
	fmt.Fprintf(out, "Completed directory passes: %d; directory errors: %d; skipped observations: %d\n", summary.CompleteDirectories, summary.DirectoryErrors, summary.SkippedEntries)
	if connection.Live != nil {
		fmt.Fprintf(out, "Worker PID: %d; paused: %t; stopping: %t; active job: %d\n", connection.Live.PID, connection.Live.Paused, connection.Live.Stopping, connection.Live.ActiveJob)
	}
	fmt.Fprintf(out, "Scan batches reserved today (%s UTC): %d/%d; budget wait: %s\n", budget.Day, budget.Used, budget.Limit, budget.Reason)
	if connection.Live != nil {
		fmt.Fprintf(out, "Worker wait: %s\n", connection.Live.WaitReason)
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
