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

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const help = `Saga — Rydd
A quiet storage cleanup companion for macOS and Linux.

Usage: rydd [--data-dir /absolute/path] <command>

Commands:
  init --root /path [--root /another] [--exclude /path]  Create config and state
  config check                                         Validate configuration
  state init                                           Initialize/migrate state from existing config
  status [--json]                                       Read saved state summary

Options: --help, --version

Only configuration and database foundations are implemented. No scanning,
background services, duplicate detection or cleanup runs yet.
`

type pathsFlag []string

func (p *pathsFlag) String() string         { return fmt.Sprint([]string(*p)) }
func (p *pathsFlag) Set(value string) error { *p = append(*p, value); return nil }

func Run(ctx context.Context, args []string, out, errOut io.Writer) int {
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
		fmt.Fprintln(out, "rydd dev (state foundation)")
		return 0
	}
	remaining := flags.Args()
	if len(remaining) == 0 {
		fmt.Fprint(out, help)
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
			fmt.Fprintf(out, "Configuration valid: %q\n%d root(s), %d exclusion(s). Scanning is not implemented yet.\n", paths.ConfigFile, len(c.Roots), len(c.Excludes))
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
	case "status":
		err = status(ctx, remaining[1:], paths, home, out, errOut)
	default:
		fmt.Fprintln(errOut, "Unknown command. Run rydd --help.")
		return 2
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(errOut, "Not initialized or unavailable: %v\nUse rydd init --root /path for new configuration, or rydd state init with existing configuration.\n", err)
		} else {
			fmt.Fprintln(errOut, err)
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
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected init arguments")
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
	if *asJSON {
		return json.NewEncoder(out).Encode(struct {
			Stage           string        `json:"stage"`
			ConfigFile      string        `json:"config_file"`
			StateDir        string        `json:"state_dir"`
			ConfiguredRoots []string      `json:"configured_roots"`
			State           state.Summary `json:"state"`
		}{"state-foundation", paths.ConfigFile, paths.StateDir, c.Roots, summary})
	}
	fmt.Fprintf(out, "Saga — Rydd\nConfig: %q\nState: %q\nSchema: %d (SQLite %s)\nConfigured roots: %d; saved enabled roots: %d\nIndexed entries: %d; pending jobs: %d\nDatabase: %d bytes; WAL: %d bytes\nScanning: not implemented (no background worker).\n", paths.ConfigFile, paths.StateDir, summary.Schema, summary.SQLiteVersion, len(c.Roots), summary.EnabledRoots, summary.Entries, summary.PendingJobs, summary.DatabaseBytes, summary.WALBytes)
	if summary.NeedsBackpressure {
		fmt.Fprintln(out, "WAL exceeds the background-write threshold; a future worker must pause writes until checkpoint progress resumes.")
	}
	return nil
}
