# Saga — Rydd

A quiet storage cleanup companion for macOS and Linux. Part of Saga.

Rydd will gradually discover developer clutter and duplicate files, explain what can be removed, and help reclaim space through reviewed actions or explicitly enabled automatic policies.

**Status: configuration and state foundation.** The CLI can initialize private configuration and SQLite state, validate settings, and show a saved status summary. Scanning, cleanup, services and automatic policies are not implemented. Nothing runs in the background when you clone, build or initialize this repository.

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

Without `--data-dir`, native macOS uses `~/Library/Application Support/saga-rydd`; Linux follows XDG config/state directories. Explicit roots and exclusions are absolute paths or start with `~/`; exclusions refer to subtrees, not glob patterns. Unknown settings, overlapping roots and invalid budgets are rejected. Roots may be offline at configuration time; physical identity, symlink aliases and availability checks belong to the upcoming scanner. Budget settings are saved now and will be enforced by that worker.

## Design commitments

- Slow, resumable work with explicit CPU and I/O budgets.
- Local SQLite state and readable TOML configuration.
- Explainable, freshness-aware findings with realistic savings estimates.
- Scanning only by default; automatic cleanup requires an explicitly enabled policy.
- Quarantine and restoration before permanent file removal where supported.
- Exact duplicates require full verification; matching samples never authorize deletion.

See the plan for the full scope. A public release is gated on resource benchmarks, cleanup recovery tests and a multi-week native soak.
