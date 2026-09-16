# Saga — Rydd

A quiet storage cleanup companion for macOS and Linux. Part of Saga.

Rydd will gradually discover developer clutter and duplicate files, explain what can be removed, and help reclaim space through reviewed actions or explicitly enabled automatic policies.

**Status: experimental metadata inventory.** The worker can scan explicitly selected fixture directories in bounded, resumable batches and save metadata, skip reasons and directory reconciliation markers in SQLite. Use `scan -d PATH` for a foreground scan, or `daemon --experimental-scan` for configured roots; ordinary `daemon` stays idle. Durable dispatch cadence, a daily batch cap, child-entry pacing and WAL backpressure are enforced; saved largest-file and selected-directory size reports and review-required `node_modules` candidates are available. Fine-grained CPU/I/O/power budgets, cleanup and service installation are not implemented. Nothing runs in the background when you clone, build or initialize this repository.

For humans and AI: readable output by default, versioned JSON with `--json`, and `rydd capabilities --json` for discovery. Live status also reports scanner metadata API counters to help inspect background work. See the [CLI contract](docs/cli.md).

**Next milestone: useful read-only reports and one `node_modules` recommendation category.** We are prioritizing this MVP and controlled user feedback before finishing unattended-operation infrastructure. See the [execution priority](PROGRESS.md#execution-priority--read-only-mvp-first); numbered phases are not a strict work order. File and selected-directory size reports are available; the first candidate category has also been exercised in a controlled native project trial. See [trial lessons](docs/mvp-trial-results.md) for current limits and next steps.

## Scan a chosen folder

```sh
rydd scan -d /path/to/project          # 10 ms between entry inspections
rydd scan -d /path/to/project -s 25    # slower, 25 ms
rydd scan -d /path/to/project --now    # no deliberate delay
rydd report -d /path/to/project
rydd report -d /path/to/project --candidates --json
```

No initialization is needed for a manual scan. It runs in the foreground and saves a separate inventory per selected folder; your configured background roots remain unchanged. Ctrl+C stops it; rerun the same command to finish pending work before revisiting completed folders. Once the queue is empty, the next invocation starts a fresh pass. The interrupted directory itself restarts its listing. Filesystem protections and configured exclusions still apply. No file contents are read or deleted. `--now` can produce substantial metadata I/O. `-s` spaces child-entry inspections, not every filesystem operation.

### Try compact dependency inventory

```sh
rydd scan -d /path/to/project --compact
rydd report -d /path/to/project
```

This opt-in mode stores `node_modules` file sizes and identities compactly instead of keeping each filename in ordinary inventory. It still traverses metadata to measure size; contents are not read. The mode is saved per manual inventory, so the same scan command without flags resumes it. Use `--detailed` after pending work finishes to switch back. Existing pending detailed scans must finish before switching modes.

Logical sizes use saved directory totals. Reports check at most 10,000 compact inode records for allocated size; larger or unknown identity sets show allocated size as unknown while retaining logical totals. Directory coverage, old observations and exclusions can still make a report partial or stale. Old ordinary file records are hidden from totals immediately per measured directory, then retired in bounded batches. SQLite may reuse the freed pages rather than shrinking its file. Compact background scanning and default enablement remain pending.

Reports select the manual inventory for that exact normalized path when present. Directory-size reports otherwise use the existing configured inventory. Scoped `--candidates` requires a manual scan of that exact folder. Relative paths and `~/` are accepted; use the same global `--data-dir` across commands when overriding it. Manual inventories are separate from default `status` and unscoped reports. See [manual scan semantics](docs/cli.md#foreground-manual-scans).

## View saved results

```sh
rydd report
rydd report --limit 10 --json
rydd report --candidates --json
rydd report --directory /absolute/path/to/folder
rydd report --directory /absolute/path/to/folder --json
rydd report --limit 10 --cursor TOKEN
```

The report works while the worker is stopped and reads only saved inventory. It lists the largest observed regular files, sizes, timestamps, parent-pass freshness and saved root diagnostics. Use `--directory` to measure up to 10,000 saved entries in a selected subtree, with partial/stale/unknown labels and qualified hardlink accounting. `--candidates` selects old recorded `node_modules` and sibling `package.json` timestamps for review. It does not inspect manifest contents or establish inactivity or safe deletion. Candidate reports now explain selection outcomes (including age, missing evidence and incomplete listings) and whether more saved entries remain. Reports do not rescan paths or delete anything. Use the returned `next_cursor` for another page; keep global `--data-dir` before `report` if using a separate instance. See the [report contract](docs/cli.md#saved-file-reports).

## Project map

- [PLAN.md](PLAN.md): architecture, safety requirements and acceptance gates.
- [PROGRESS.md](PROGRESS.md): completed work, remaining tasks, validation evidence and the next handoff.
- [CONTRIBUTING.md](CONTRIBUTING.md): Docker development and native validation.
- [AGENTS.md](AGENTS.md): working instructions for coding agents.
- [SQLite decision](docs/decisions/001-sqlite-driver.md): reproducible driver comparison and resource measurements.

## Development with Docker

Host requirements: Git, a POSIX shell, and a running Docker engine with Compose v2+. No host Go installation, package manager, database server or task runner is required. Development containers run only when requested.

```sh
git clone https://github.com/bjornarhagen/saga-rydd.git
cd saga-rydd
./scripts/dev check
./scripts/dev run --help
./scripts/dev build-all
```

The first command builds the development image. Later runs reuse that image and per-user Go caches in a project-specific Docker volume. The container mounts only this checkout, not your home directory or Docker socket. Build output goes to the ignored `dist/` directory. The wrapper uses your host UID/GID to avoid root-owned files on Linux.

| Command | Purpose |
| --- | --- |
| `./scripts/dev check` | Check formatting, run vet and tests, build the CLI |
| `./scripts/dev fmt` | Format Go source |
| `./scripts/dev test` | Run tests |
| `./scripts/dev race` | Run Go race detection on the container platform |
| `./scripts/dev run --help` | Run the CLI inside Docker |
| `./scripts/dev build-all` | Cross-build macOS/Linux arm64/amd64 binaries |
| `./scripts/dev sqlite-check` | Run both SQLite candidates' transaction/crash checks |
| `./scripts/dev sqlite-bench -rows 1000000` | Compare drivers using synthetic metadata |
| `./scripts/dev shell` | Open a development shell |
| `./scripts/dev rebuild` | Refresh/rebuild the development image |
| `./scripts/dev cache-clear` | Explicitly remove this project's development caches |

The installed app will run **natively**, using launchd on macOS or a user service on Linux. Docker is the development environment. Linux containers cannot establish macOS permissions, APFS behavior, battery handling or LaunchAgent correctness; those need native checks. CI runs on both macOS and Linux.

## Try the current foundation

Inside Docker, keep disposable state in this checkout so it survives between commands:

```sh
./scripts/dev run --data-dir /workspace/.local/demo init --root /workspace
./scripts/dev run --data-dir /workspace/.local/demo config check
./scripts/dev run --data-dir /workspace/.local/demo status --json
```

This records the selected root but does not scan it. `--data-dir` is a global option and comes **before** the command. Use a dedicated directory: existing shared directories and symlinked state/config files are rejected. `init` never overwrites an existing config. After editing the config, `state init` validates it and updates registered roots while preserving existing inventory; it also retries a failed initial database setup.

Without `--data-dir`, native macOS uses `~/Library/Application Support/saga-rydd`; Linux follows XDG config/state directories. Explicit roots and exclusions are absolute paths or start with `~/`; exclusions refer to subtrees, not glob patterns. Unknown settings, overlapping roots and invalid budgets are rejected. Roots may be offline at configuration time. Experimental scanning establishes a root/filesystem fingerprint, rejects available root aliases, and retains saved inventory when a root is unavailable or its identity changes. Budget settings are saved; dispatch cadence, a daily batch cap, cooperative work timeouts, child-entry pacing and WAL backpressure are implemented so far. `scan.metadata_per_second` currently paces child-entry inspections; traversal and database operations are not yet rate limited.

### Run and control the worker

In one terminal:

```sh
./scripts/dev run --data-dir /workspace/.local/demo daemon
```

In another terminal, using the same checkout:

```sh
./scripts/dev run --data-dir /workspace/.local/demo status --json
./scripts/dev run --data-dir /workspace/.local/demo pause
./scripts/dev run --data-dir /workspace/.local/demo resume
./scripts/dev run --data-dir /workspace/.local/demo stop
```

`daemon` runs in the foreground until stopped, interrupted or sent SIGTERM. On a native build, replace `./scripts/dev run` with your binary and use a native data path. Service installation will follow in P1-09. The worker stays idle unless `--experimental-scan` is supplied each time it starts.

Pause is saved before acknowledgment and survives restart. It cancels an active chunk cooperatively; status shows whether that chunk is still draining. `stop` acknowledges a shutdown request; wait for the daemon process to exit before restarting or running `state init`. Reports work while the worker is stopped. A second worker or state writer using the same state directory is rejected. Configuration changes take effect on the next worker start.

Controls use a private Unix socket with same-user peer checks. Docker commands share a small runtime volume so separate development containers can communicate. Native sockets use a private directory under `/tmp`; set the same short, absolute `RYDD_RUNTIME_DIR` for daemon and clients if overriding it. Keep application state on a local filesystem. See [worker design](docs/worker.md) for recovery and remaining scheduler work.

## Try the experimental scanner

Try experimental scanning on a disposable fixture with `./scripts/scanner-smoke ./dist/rydd-darwin-arm64` after `./scripts/dev build-all` (choose the binary for your host). Inside Docker, run `./scripts/dev shell -c './scripts/scanner-smoke ./dist/rydd'` after `./scripts/dev check`. The script creates five synthetic entries, scans them with a short fixture cadence, checks results and stops its worker. No ordinary file contents are opened, hashed or deleted. Keep broad personal-directory scans disabled until P1-06 resource enforcement.

Each batch contains at most 128 child observations. Throttle waits yield partial batches with bounded pending names so slow entry rates can make progress. A restart re-enumerates the interrupted directory with a fresh generation and idempotent upserts. Completed directory passes have reconciliation markers; old observations remain for later stale-entry processing. Counts are historical observations, not a whole-tree percentage or reclaimable space. See [inventory design](docs/inventory.md) for filesystem support and limitations.

## Design commitments

- Slow, resumable work with explicit CPU and I/O budgets.
- Local SQLite state and readable TOML configuration.
- Explainable, freshness-aware findings with realistic savings estimates.
- Scanning only by default; automatic cleanup requires an explicitly enabled policy.
- Quarantine and restoration before permanent file removal where supported.
- Exact duplicates require full verification; matching samples never authorize deletion.

See the plan for the full scope. A public release is gated on resource benchmarks, cleanup recovery tests and a multi-week native soak.
