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

No initialization is needed for a manual scan. It runs in the foreground and saves a separate inventory per selected folder; your configured background roots remain unchanged. Ctrl+C stops it; rerun the same command to finish pending work before revisiting completed folders. Once all scan jobs and saved maintenance/calculations finish, the next invocation starts a fresh pass. The interrupted directory itself restarts its listing. Filesystem protections and configured exclusions still apply. No file contents are read or deleted. `--now` can produce substantial metadata I/O. `-s` spaces child-entry inspections, not every filesystem operation.

### Try compact dependency inventory

```sh
rydd scan -d /path/to/project --compact
rydd report -d /path/to/project
```

This opt-in mode stores `node_modules` file sizes and identities compactly instead of keeping each filename in ordinary inventory. It still traverses metadata to measure size; contents are not read. The mode is saved per manual inventory, so the same scan command without flags resumes it. Use `--detailed` after pending work finishes to switch back. Existing pending detailed scans must finish before switching modes.

Logical sizes use saved directory totals. After scanning, the manual command calculates cached allocated totals for the selected root and each outermost `node_modules` tree, deduplicating hardlinks across the entire scope. These calculations resume in bounded batches after interruption and are invalidated by the next saved scan batch. Reports can use matching caches even above 10,000 compact identities. Other scopes, or caches still being calculated, fall back to checking at most 10,000 compact inode records; larger or unknown identity sets then show allocated size as unknown. Completed scope calculations also cache full directory coverage and logical size. These reports can cover more than 10,000 entries; other scopes and unfinished calculations retain the synchronous cap. Old observations or exclusions can still make reports partial or stale.

To finish saved calculations after an upgrade or interruption without rescanning:

```sh
rydd measure -d /path/to/project
rydd report -d /path/to/project --candidates --min-age-days 30
```

`measure` requires an existing compact manual inventory. It runs at most 128 batches or five seconds per invocation; repeat until complete. Each batch processes at most 128 records. It writes derived database state only; source folders can be offline. Pending scans must finish first. Reports remain read-only.

Old ordinary file records are hidden from totals immediately per measured directory, then retired in bounded batches. Cache calculations temporarily store a scoped identity map in SQLite and retire it after completion. [Production fixture measurements](experiments/compactscale/README.md) compare compact and detailed state, memory, latency and interruption recovery; they do not establish unattended resource budgets. SQLite may reuse freed pages rather than shrinking its file. Compact background scanning and default enablement remain pending.

After a successful completed parent listing no longer contains a directory, its saved descendants stop contributing to directory sizes. Reports disclose excluded historical rows; incomplete or failed listings retain stale evidence. Once the manual compact scan queue finishes, resumable maintenance retires obsolete subtree records and caches in bounded batches. New scan observations invalidate pending retirement proofs, preserving reappearing directories and current replacement files. This only removes rebuildable database records; user files are untouched. Interrupted maintenance finishes before another pass or mode switch.

Reports select the manual inventory for that exact normalized path when present. Directory-size reports otherwise use the existing configured inventory. Scoped `--candidates` requires a manual scan of that exact folder. Relative paths and `~/` are accepted; use the same global `--data-dir` across commands when overriding it. Manual inventories are separate from default `status` and unscoped reports. See [manual scan semantics](docs/cli.md#foreground-manual-scans).

## View saved results

```sh
rydd report
rydd report --limit 10 --json
rydd report --candidates --json
rydd report --candidates --min-age-days 30 --json
rydd report --directory /absolute/path/to/folder
rydd report --directory /absolute/path/to/folder --json
rydd report --limit 10 --cursor TOKEN
```

The report works while the worker is stopped and reads only saved inventory. It lists the largest observed regular files, sizes, timestamps, parent-pass freshness and saved root diagnostics. Use `--directory` to read a completed scope calculation or measure up to 10,000 saved entries in a selected subtree, with partial/stale/unknown labels and qualified hardlink accounting. `--candidates` selects old recorded `node_modules` and sibling `package.json` timestamps for review. It does not inspect manifest contents or establish inactivity or safe deletion. The default age filter is 90 days; `--min-age-days N` changes it for one report only. Candidate reports now explain selection outcomes (including age, missing evidence and incomplete listings) and whether more saved entries remain. Reports do not rescan paths or delete anything. Use the returned `next_cursor` for another page; keep global `--data-dir` before `report` if using a separate instance. See the [report contract](docs/cli.md#saved-file-reports).

## View same-size saved files

```sh
rydd report --same-size -d /path/to/project
rydd report --same-size -d /path/to/project --min-size-bytes 1048576 --limit 20 --json
```

This is the first filter for duplicate discovery. It groups saved regular-file metadata by size without opening source contents. Equal sizes do not prove equal contents. The default minimum is 1 MiB. `--limit` bounds raw saved file rows per page, including rows later excluded from the report. Dependency trees named `node_modules`, skipped entries and disabled roots are excluded.

Page-local counts distinguish known device/inode objects, repeated saved aliases and unknown or conflicting identities. A size band can cross pages; boundary labels preserve even a single candidate at an edge. Use the returned cursor with the same minimum and data directory. A rebuilt inventory or changed minimum refuses the cursor. Scans can change membership between pages, so this is not a frozen export. There are no verified duplicate matches, keeper choices or savings estimates yet.

A separate library foundation supports bounded full hashing with private checkpoints and conservative byte reservations. `rydd hashes [--work WORK_ID] [--json]` reads its existing saved observations and whole-selection budget. This command does not create, recover or resume work; CLI content reads and worker integration remain pending. See [durable hashing limits](docs/inventory.md#explicit-file-observations-and-durable-hashing).

## Save an exact hashing proposal

```sh
rydd report --same-size -d /path/to/project --json > /path/to/private/same-size.json
rydd hash --select -d /path/to/project --from /path/to/private/same-size.json 12 18
rydd hash --show SELECTION_ID --json
```

Use the saved file IDs from that report and the returned selection ID. The report file contains private paths. This captures one immutable unapproved proposal and shows its complete saved evidence. Changed rows or a different selection are refused. Selection and display open no selected file contents, start no scanner and recover no hashing work. CLI read approval and execution remain pending.

## Preview an exact selection

```sh
rydd plan --preview -d /path/to/project --min-age-days 30 node-modules-v1:1:42
```

Replace the example ID with a finding ID from the candidate report for the same inventory. Select 1–20 unique IDs; put flags before IDs. The preview rechecks saved eligibility and shows exact targets, qualified sizes and the checks needed before a future quarantine action. Project activity remains unconfirmed. It is read-only, has no approval ID and cannot execute cleanup. Quarantine would support recovery but would not free disk space; purge requires a separate future approval.

## Save a selection for later review

For a guided text review of an existing manual scan:

```sh
rydd review -d /path/to/project --min-age-days 30
```

1. Read the numbered candidates and their saved size qualifications.
2. Enter unique row numbers, such as `1,3`. Check the selected paths shown again.
3. Type `save` to preserve that exact unapproved selection, or `back` or `quit`.

Each page has at most 20 candidates. `next` replaces it with a later page; `refresh` returns to the first page. Numbers reset, and selections do not carry between pages. Empty pages show rejection counts and a command to read the same saved page, preserving the folder, state location, age filter and cursor. Unselected folders stay unchanged. This does not save a keep decision or hide candidates from later reports. EOF or Ctrl+C before saving ends review without publishing a plan.

The page is frozen before input. If the selected inventory evidence changes before saving, review refuses it and asks you to start again. Incomplete but unchanged evidence can be saved as a qualified historical record. Saving never records cleanup consent, moves files or verifies current source contents. Afterward, start another scan explicitly when needed. `review` prompts in text mode; use the finite report and plan commands for JSON workflows.

```sh
rydd plan --save -d /path/to/project --min-age-days 30 node-modules-v1:1:42
rydd plan --show PLAN_ID
rydd plan --check PLAN_ID -d /path/to/project
rydd plan --verify PLAN_ID -d /path/to/project
rydd plan --inspect PLAN_ID -d /path/to/project
rydd plan --inspect PLAN_ID --tree -d /path/to/project
rydd plan --capture PLAN_ID -d /path/to/project
rydd plan --compare OBSERVATION_ID -d /path/to/project
```

Use an actual finding ID, then replace `PLAN_ID` with the returned saved plan ID. Keep the same global `--data-dir` if you override it. The saved record contains the exact selection and evidence from one inventory snapshot. Later scans cannot add targets to it. Reopening works while the source folder or its inventory is offline.

`--check` compares the selection with its saved inventory and reports changed or incomplete evidence. Use the original manual scan directory, or omit `-d` for configured inventory. The check reads saved observations only: it can match while the source folder is offline. A match does not establish safe cleanup. Scanning again changes inventory revisions and requires a new selection for review.

`--verify` also reads current metadata for the selected root, path ancestors, dependency directory and `package.json`. It blocks changed, missing, excluded or symlinked paths and descendant mount boundaries. Use a canonical scan path: root aliases accepted by scanning are rejected by this stricter check. It does not read dependency contents, inspect the whole subtree or establish that cleanup is safe. No files are moved, and the result cannot authorize a later move.

`--inspect` explicitly reads bounded `package.json` and `package-lock.json` contents after the same path checks. The first supported format is a narrow subset of npm lockfile versions 2 and 3 with public registry dependencies. It compares root dependency declarations and reports exact-byte digests. Unsupported managers, local sources, install scripts and project configuration need separate review. The command does not check installed dependency contents, detect local edits or prove that reinstall will succeed. Inspection describes this request; use capture below to save a separate observation for later comparison. See the [input inspection contract](docs/cli.md#project-input-inspection).

Add `--tree` to also inspect up to 10,000 installed entries through recursive metadata listings. It accepts lock-listed package directories, ordinary files and supported internal `.bin` links; it blocks unknown boundary entries, hardlinked files, special objects, exclusions and mount boundaries. Counts and a metadata digest describe the observed layout, including two matching metadata passes. Ordinary dependency contents remain unopened. This does not establish a complete or unmodified install, and large or unsupported trees need separate review. Input-only inspection stays unchanged.

`--capture` repeats the full input/tree check and saves one immutable observation for the exact plan only when all targets pass. It saves digests and counts, with no dependency file bodies or descendant filename list. `--show` exposes the observation ID and observed time, even while source folders or inventory are offline. Repeating capture returns the same ID and time when the observations still match. Changed observations require a new saved selection; an existing baseline is never replaced.

Use the returned `OBSERVATION_ID` with `--compare` to repeat the same checks and report a match, changed evidence or a block. Comparison writes no records. A match means the checked input bytes and tree metadata agree with that capture; it cannot detect edits made before capture or prove that dependencies can be safely reinstalled. Existing review consent is separate and remains non-executable.

Plans are private local records in a separate database under the global data directory. Saving does not approve cleanup, move files or free space. Review consent can be recorded separately as described below. Cleanup remains unavailable; Rydd does not verify project activity or regeneration safety.

Saving, checking, live verification, inspection, capture and comparison require inventory schema 9. Reports and previews can still read schemas 4–8. Existing configured state can be migrated with `state init` while its worker is stopped; a manual inventory migrates on the next explicit scan, or with `measure` when it is compact and has no pending scan. Writer commands can migrate plan-store schemas 1/2 to schema 3; capture publishes its observation atomically, with existing plans and consent unchanged. Read-only `--show`, `--check`, `--verify`, `--inspect`, `--compare` and `--preview` do not migrate storage.

## Read a saved recovery record

```sh
rydd journal --show INTENT_ID
rydd journal --show INTENT_ID --json
rydd journal --observe INTENT_ID --json
```

This reads a preparation record and its recorded history without inspecting either filesystem location. The journal foundation is available for development fixtures; there is no CLI command to prepare or execute a move. Saved outcomes are caller-supplied records, not proof of current state. Missing results remain unknown and cannot permit retry. `--observe` separately checks metadata at the two recorded locations. It writes no history and keeps the cleanup outcome unknown, even when metadata agrees. Quarantine, restoration and purge remain unavailable. See the [journal contract](docs/cli.md#saved-recovery-journal).

## Record or revoke review consent

After reviewing the exact saved selection with the project owner:

```sh
rydd plan --approve PLAN_ID -d /path/to/project --confirm-project-review --confirm-quarantine
rydd plan --show PLAN_ID
rydd plan --revoke PLAN_ID
```

The two confirmations mean the owner reviewed project activity, local dependency edits and reinstall requirements, and accepts same-filesystem quarantine without permanent deletion. Consent lasts 24 hours. The saved inventory must still match; changed or incomplete evidence requires a new selection. Retrying keeps the same approval and expiry. Revocation works even when the source folder and inventory are offline.

This version records **review consent only**. It cannot execute cleanup. A future cleanup command will require renewed approval after live checks and recovery are implemented. `--show` displays the review status separately from the original saved evidence. Revoked or expired approvals cannot be renewed for the same plan; save a new selection for another review.

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
