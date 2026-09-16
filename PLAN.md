# Saga — Rydd: implementation plan

Status: configuration, state, worker and experimental metadata inventory implemented. See [PROGRESS.md](PROGRESS.md) for verification and acceptance evidence. Largest-observed-file reporting is implemented and verified; see PROGRESS.md for evidence. Selected-directory size reports are implemented and verified with bounded saved-subtree measurement; see PROGRESS.md for evidence. The first review-required node_modules finding slice is implemented and verified. Full resource enforcement and cleanup remain unimplemented.

## Current execution priority: useful read-only MVP

**User-approved decision:** prioritize an end-to-end report and recommendation workflow now. The existing persistence, filesystem safeguards, bounded scanner, controls and pacing are sufficient foundations for controlled read-only trials. Full resource hardening is important for unattended use, but is not a prerequisite for saved reports or the first detector.

The delivery phases in section 11 remain scope/acceptance groups, not a strict dependency chain. Follow the execution order and task IDs in [PROGRESS.md](PROGRESS.md#execution-priority--read-only-mvp-first); this decision supersedes earlier resource-controls-first next steps.

1. **Saved-inventory report (P1-08b1):** largest observed files; bounded deterministic pagination; coverage, freshness and skipped/unavailable locations; equivalent human/JSON output; available while the worker is stopped or scanning is incomplete. Clearly distinguish historical observations from verified current state.
2. **Directory sizes (P1-08b2):** bounded/incremental measurement with explicit partial, stale and unknown states. Do not imply that completing a directory's direct children establishes whole-subtree completeness. Separate logical size, allocated size and potential reclaimable space; qualify hardlinks/shared storage and overlapping totals.
3. **One recommendation category (P2-01a/02a plus the necessary P2-05 explanation slice):** potentially stale `node_modules` in recognized projects. Show measured size/completeness, project identity, rule/evidence, observation freshness and regeneration instructions/caveats. Label candidates review-required. Old timestamps alone prove neither inactivity nor safe deletion; insufficient observation history must be visible. Keep the first finding model small enough to deliver this workflow without implementing all future actions/policies.
4. **Controlled real-folder feedback (MVP-TRIAL):** after synthetic/native validation, use a specifically selected development folder, read-only. Assess useful candidates, false positives, misleading sizes, overlap, freshness and whether the evidence supports decisions. Record sanitized lessons and revise the next work accordingly.
5. **User-requested scan ergonomics (P1-08c):** foreground `scan -d PATH [-s MS | --now]`, separate saved inventory per path and scoped `report -d PATH`. Preserve configuration, filesystem protections and interrupt recovery.
6. **After this feedback:** develop duplicate detection and reviewed cleanup alongside remaining resource controls. Destructive operations still require their own revalidation, approval and recovery safeguards before use.

Do not put complete CPU/metadata/battery budgets, native priority tuning, service installation, general diagnostics or the full soak ahead of this MVP unless a demonstrated dependency blocks the trial or its correctness. Minimum stale-data/measurement fixes needed for truthful reports may be pulled forward from P1-07. Preserve current safeguards and experimental scope; this plan does not authorize broad unattended personal scans or deletion. Complete resource, platform, action-safety and soak gates still apply to their respective unattended/release claims.

The controlled trial procedure is in [docs/mvp-trial.md](docs/mvp-trial.md). The [first native project trial](docs/mvp-trial-results.md) exposed a specific reporting gap: empty candidate pages do not explain selection failures. Bounded per-page rejection/coverage diagnostics (P2-05a) address that gap; review their usefulness before moving on to duplicates or cleanup, retaining the current rule pending owner feedback. CI remains on GitHub-hosted runners by user decision; keep native macOS/Linux validation.

**MVP acceptance:** on synthetic fixtures and a selected development-folder trial, a user can open the saved report, see meaningful file/folder sizes with honest completeness/freshness, and assess at least one explained cleanup-candidate category. AI receives the same evidence through the documented JSON contract. The MVP is read-only; it does not require full v1 cleanup/automation to be useful.

**Why this order:** useful outputs test whether the inventory, size model and recommendations answer real questions. Infrastructure improvements should follow observed needs as well as safety requirements, rather than delaying that feedback until every supporting feature is finished.

## 1. Product direction

Install a small terminal application, enable its background worker, and let it gradually build an inventory over days or weeks. Opening the terminal app should immediately show useful findings from its saved inventory, even while scanning is incomplete or the worker is stopped.

Primary requirements:

- macOS and Linux support.
- Equally usable by humans and AI: discoverable commands, noninteractive operation, versioned JSON, stable errors and explicit action authorization.
- Low CPU, memory, disk traffic, and battery impact; scanning speed is secondary.
- Persistent progress across restarts, sleep, and temporary volume disconnection.
- Explainable recommendations with evidence, freshness, and estimated savings.
- Assisted deletion and duplicate removal, plus explicitly enabled automatic cleanup policies.
- Local operation without uploading filenames, hashes, or file contents.

Confirmed product choices: the app is Rydd, part of the Saga collection; v1 supports opt-in automatic cleanup and focuses on developer clutter plus exact duplicate files. The installed default is scanning only; cleanup requires either approval of an individual plan or a previously approved, narrowly scoped automatic policy. The scanner is general enough to support personal-file recommendations later.

## 2. Stack and process model

Confirmed stack: **Go + SQLite + TOML configuration**, distributed as one executable. Go is a practical fit for a maintainable CLI and background service; its supported targets include macOS and Linux. The database experiment selected the pure-Go `modernc.org/sqlite` driver; see [ADR 001](docs/decisions/001-sqlite-driver.md) for measurements and limitations. The foundation supports CGO-free macOS/Linux cross-builds; future native integrations must preserve or deliberately revisit that property. [Go targets](https://go.dev/doc/install/source#environment)

Development should happen in disposable Docker containers, using the repository's `scripts/dev` wrapper; no host Go installation is required. Native macOS/Linux CI and targeted native smoke tests validate platform behavior that Linux containers cannot establish. See [CONTRIBUTING.md](CONTRIBUTING.md).

One executable has two roles:

```text
Terminal CLI ── reads reports ────────────► SQLite
     │                                      ▲
     └── control / actions / policies ► Background worker
                                            │
                                  scheduler + work queue
                                            │
                                  scanner + detectors
                                            │
                                  platform integrations
```

- One worker process for the user's default state directory, with an exclusive writer lock. Explicit separate state directories support isolated instances and tests; do not configure them to scan overlapping roots.
- One filesystem work task at a time initially. Stream directory entries and file contents with bounded buffers.
- The worker performs database writes; reports use short read transactions. A private Unix socket carries pause/resume and approved action requests. Check socket ownership and restrict access to the current user.
- Store accepted actions durably before execution; a socket disconnect must not produce ambiguous duplicate execution. Use action IDs and idempotent recovery.
- Reports remain available without the worker. Cleanup requires a running worker, with an actionable message explaining how to start it.
- Keep modules small: CLI, state, scheduler, filesystem inventory, detectors, duplicate detection, action executor, platform adapters. No general plugin framework initially.

## 3. Storage: use SQLite rather than many text files

Text files are appropriate for configuration and exports. The inventory needs indexed size/hash queries, partial updates, a persistent queue, and crash recovery. Implementing these over many JSON files would effectively create a custom database. SQLite is designed for local application storage and requires no database server. [SQLite use cases](https://www.sqlite.org/whentouse.html)

Use SQLite WAL mode so reports can read while scanning writes. Keep transactions short, set bounded busy retries, and monitor/checkpoint WAL growth. WAL has auxiliary files and belongs on a local filesystem, not a network share. [SQLite WAL](https://www.sqlite.org/wal.html)

Implemented foundation defaults: FULL durability, a 4 MiB cache per connection, memory mapping disabled, one connection per store, 1 second busy timeout, and automatic checkpointing every 1000 pages. Passive checkpoint support and a 32 MiB WAL backpressure signal are available; the scheduler now defers new inventory chunks when a passive checkpoint cannot clear pending frames. Reader commands do not initialize or migrate state. Application/schema identity rejects unrelated or newer databases.

The worker uses an OS-held writer lock, a private versioned Unix control socket, durable pause state and token-fenced job leases. Schema v3 adds bounded inventory batches, directory reconciliation watermarks and skip reasons; v4 persists dispatch reservations and cadence. It dispatches at most one cooperative chunk at a time and sleeps without queue polling when idle. Metadata scanning requires `--experimental-scan` until full budget enforcement is verified. [Worker design](docs/worker.md) and [inventory design](docs/inventory.md) specify recovery, pacing and remaining integrations; cadence alone is not CPU or I/O budget enforcement.

Default locations:

| Platform | Configuration | State database |
| --- | --- | --- |
| macOS | `~/Library/Application Support/saga-rydd/config.toml` | same directory, `state.sqlite3` |
| Linux | `$XDG_CONFIG_HOME/saga-rydd/config.toml` | `$XDG_STATE_HOME/saga-rydd/state.sqlite3` |

Linux fallbacks are `~/.config` and `~/.local/state`. Keep the database away from the installed executable so upgrading or uninstalling the executable does not accidentally lose settings and history. An explicit `--data-dir` can support a portable local folder. Follow [XDG directory conventions](https://specifications.freedesktop.org/basedir/latest/).

Initial logical tables:

| Table | Purpose |
| --- | --- |
| roots | Selected locations, volume identity, coverage and exclusions |
| entries | Paths, parent, type, device/inode, sizes, timestamps and observation generation |
| jobs | Persistent directory, measurement, hashing and detector work; next run, retries, checkpoint |
| fingerprints | Sample and full hashes, algorithm version and metadata used for validation |
| findings | Category, evidence, risk, freshness, estimated savings and dismissal state |
| actions / action_items | Approved manifests, execution state, original/quarantine locations and recovery history |
| policies | Explicitly approved automatic rules, scope, limits, version and enablement state |
| settings / schema_migrations | Worker settings and schema versions |

Index sizes, due jobs, parent paths and hashes; avoid indexing every field. Preserve filesystem path bytes and escape unusual filenames safely in terminal output. Keep a current inventory and bounded observation history rather than recording every scan event forever.

The inventory can be rebuilt, but outstanding quarantine/restore records must be preserved. Treat action history as durable data, use durable commits for action transitions, and provide a supported backup/export path. Bound logs and database growth; pause new discovery when the configured state budget is reached and report why. Never automatically purge files or restoration records to meet that budget.

The [CLI contract](docs/cli.md) defines machine output and the implemented dispatch limits. Live scanner API counters now expose traversal, enumeration, failed calls and revalidation separately. Child-entry inspections are now paced using `metadata_per_second`, with partial-batch continuation when the throttle window expires. This is not full metadata rate enforcement. The counters are not durable consumption quotas or exact syscall counts; resolution internals, database work, CPU and bytes need separate accounting before full rate enforcement. Fine-grained resource accounting remains separate from daily batch reservations.

## 4. Scanning slowly and predictably

Start with selected user-owned roots, suggested during setup: development folders and Downloads. Allow the home directory as a broader option. Whole-machine coverage means additional explicitly selected readable roots, not running as root by default.

### Proposed conservative profile

These are initial tuning values and acceptance targets, not measured claims:

| Control | Starting value / behavior |
| --- | --- |
| Work concurrency | 1 filesystem task |
| Work cadence | Up to 30 seconds of work per 5 minutes, sleeping between batches |
| Metadata pace | Up to 100 entry checks per second while active |
| File-content reading | Up to 5 MiB/s while active; 5 GiB/day by default |
| Memory | Target under 100 MiB RSS on the million-entry benchmark |
| CPU | Target under 1% of one core averaged over an hour in the gentle profile |
| Battery | Pause automatic scanning by default; configurable |
| Manual control | Pause, resume, temporary faster profile, and explicit scan budget |

Use independent budgets for directory entries, file bytes, elapsed work, and measured CPU time. If CPU use exceeds the target, increase sleep time; a byte cap alone is insufficient. Persist daily consumption so restarts do not reset the budget. Small reads and checkpoints make pause responsive. Count sampling, full hashes, verification reads and our database/log writes in diagnostics; kernel caching and metadata I/O mean these are not exact physical-device limits.

If power or load detection is unavailable, report that and keep conservative fixed pacing. Do not require idle detection to make progress. Never prevent system sleep or perform a catch-up burst after waking. Avoid frequent polling and aggressive database maintenance.

### Incremental inventory

1. Discover directories in bounded batches; persist discovered child jobs and metadata together.
2. Prioritize known high-value clutter and large files, but reserve time for other roots so small-file trees are not starved.
3. Measure dependency folders incrementally; show partial sizes until measurement completes. Avoid full content hashing inside generated dependency trees by default.
4. Revisit directories on an adaptive schedule, such as daily for changing locations and weekly/monthly for stable ones. Unchanged parent timestamps do not prove that descendants are unchanged.
5. Use metadata changes to invalidate cached hashes; treat device/inode as scoped identity hints because inodes can be reused.
6. After a completed directory reconciliation, expire entries confirmed missing. Permission failures or a disconnected volume must never mark its entire inventory as deleted.
7. Restart an interrupted directory enumeration when a portable durable cursor is unavailable; upserts are idempotent. Very large directories need a tested platform strategy that preserves bounded memory and eventual progress without assuming stable enumeration order.

Initially use periodic reconciliation. Filesystem notifications can later prioritize changed areas, but should not become the only source of correctness or require a watch per file.

Exclude system pseudo-filesystems, system-managed directories, backups, application databases, the optimizer's own state/quarantine, and network/removable mounts unless explicitly selected. Do not follow symlinks or cross nested mount points by default. Detect cloud placeholders before content reads; skip cloud-managed roots where reliable avoidance of downloads cannot be guaranteed.

Coverage reporting should show entries visited, last reconciliation, queued work, skipped roots, permission errors and stale findings. Avoid an invented percentage for a tree whose total size is still unknown.

## 5. Findings and safety classification

Use **regenerable**, **review required**, and **informational/protected** rather than claiming files are universally safe to delete. Risk and confidence are separate: strong evidence that two files match does not establish that either path is unnecessary.

Every finding should include:

- Path or Docker object identity, size, last checked time, and rule/version.
- Why it was selected and what could break if removed.
- Evidence of regeneration, where applicable; network access or unavailable packages may prevent restoration.
- Estimated reclaimable space, with overlap/shared-storage qualifications.
- Available action, recovery behavior, and options to dismiss or permanently exclude it.

### First read-only detector slice (P2-01a/P2-02a)

`report --candidates` derives versioned, review-required findings from saved inventory without a schema change. A regular-file sibling `package.json` identifies a project-like directory by filename only; its contents and lockfiles are not validated. Both its and `node_modules`' recorded mtimes must be at least 90 days old. This is a provisional review filter, not observed inactivity or a regeneration guarantee. Manifest and dependency directory must belong to the same completed saved parent pass; nested `node_modules` are suppressed. Each finding exposes local entry/inode identity, timestamps, rule version and the bounded directory measurement. There are no supported cleanup actions or summed savings. Findings/dismissals are not yet persisted independently of inventory.

Pages examine at most 1,000 saved entries and measure at most 20 candidates, each capped at 10,000 entries. A five-second command deadline bounds elapsed database work; timeout is an error, not an empty report. Measurements use separate snapshots and carry their own freshness. A future inventory rebuild may reuse IDs, so these IDs must never authorize actions. This limited detector is sufficient for an initial feedback trial; validated manifest/lockfile contents, project activity history and incremental large-subtree aggregation remain open in P2-01/P2-02.

P2-05a adds bounded first-match selection counts and saved-page coverage in human/JSON candidate reports. Counts cover only examined entries and preserve existing eligibility; missing or unconfirmed evidence is explained before age. Exhaustion of saved entries is never described as completion of the filesystem scan.

### Scan continuation priority (P1-07a)

Before generated-tree aggregation, fix restart scheduling: seed a root only when it has no pending or running inventory jobs, including delayed retries. Preserve interrupted inventory job priority across cancellation and crash recovery. A repeated manual scan finishes its saved queue; a later invocation with an empty queue starts a new pass. Expose resume/new-pass mode to humans and JSON clients. This avoids unnecessary completed-directory revisits without treating recent timestamps as evidence of an unchanged subtree. Directory enumeration still restarts within an interrupted directory; durable wide-directory continuation, adaptive refresh, stale-entry handling and explicit forced-refresh controls remain P1-07 work.

### Generated dependency trees: next storage priority

The user requested avoiding a full per-file inventory inside `node_modules`. Treat supported generated trees as one logical inventory item, retaining path, project evidence, timestamps and aggregate size/freshness. Exact size still requires incremental metadata traversal; directory metadata alone cannot supply recursive size. Add a resumable measurement job with bounded cursor/working state and honest partial/stale accounting, including hardlinks and interrupted/mutated trees. Keep descendants out of ordinary largest-file and duplicate discovery by default once that measurement path is implemented. Skipping traversal entirely would leave size unknown; do not substitute zero or claim a constant-time exact measurement. This remains planned, not implemented by the manual scan command.

Prioritize this work after P1-07a and before broader per-file duplicate indexing. P2-02b1 validates compact directory totals plus inode evidence; see [ADR 002](docs/decisions/002-generated-tree-aggregation.md) for the measured storage tradeoff and required production integration. P2-02b2 adds an opt-in manual integration: cached per-directory logical totals, compact inode evidence, bounded report checks and durable obsolete-record retirement. The experimental oracle remains unused. Full cached allocated reductions, stale descendant membership, background dispatch and default enablement remain open. Start with `node_modules`; do not assume arbitrary names such as `build` or `dist` are disposable. Folder aggregation never authorizes cleanup. Provide an explicit detailed-inventory override when adding the boundary behavior, and handle existing descendant rows/rollup overlap without destroying unrelated state.

### Initial detectors

| Detector | Evidence | Proposed action |
| --- | --- | --- |
| Old `node_modules` | Recognized project manifest, lockfile where present, observed project changes and dependency timestamps | Review; quarantine selected dependency directory |
| Known generated build output | Supported project type and specifically recognized output location | Review; quarantine selected output |
| Docker build cache / images | Docker-reported object state and usage, with pinned local context/builder | Review explicit irreversible Docker cleanup |
| Docker volumes / stopped containers | Object metadata and references | Informational initially; no cleanup in v1 |
| Exact duplicate regular files | Matching full content plus action-time verification | Select a copy to keep; quarantine chosen redundant copies |

Start with `node_modules` and one well-defined build-output ecosystem; add Python virtual environments and other package caches after their regeneration rules are tested. Names like `build`, `dist`, or `.venv` alone do not establish disposability. Neither folder mtime nor access time proves inactivity. Display “no observed changes since…” rather than “unused for…” when that is all the evidence supports.

General personal files, old installers, archives and large forgotten downloads can follow as review-required recommendations. Age and size alone never establish safe deletion.

## 6. Duplicate detection and actual savings

Pipeline:

1. Consider ordinary local files above a configurable minimum, initially 1 MiB.
2. Group by logical size; unique sizes need no content read.
3. Exclude multiple paths to the same inode from redundant-copy savings: those are already hardlinked.
4. Read small samples at fixed positions, for example 32 KiB at the start, middle and end.
5. Stream a full SHA-256 hash only for matching sample groups; samples are a filter, never proof.
6. Check metadata before and after reads. Retry changing files later rather than storing a valid-looking stale hash.
7. Before removal, revalidate both the selected keeper and the redundant copy, including byte comparison and identity checks. Skip busy or changed candidates and require renewed review when the approved plan no longer matches.

Interrupted large-file hashing needs a versioned resumable state or a fair continuation scheme so files larger than a work slice/daily allowance eventually finish. Do not repeatedly start them from zero. Action-time verification follows a separate visible budget and remains cancellable.

V1 deduplication means helping the user keep one copy and remove selected others. The user chooses the keeper; application-managed paths and unknown dependencies remain protected. Do not transparently replace files with hardlinks: subsequent edits would affect every linked path. Reflink/APFS clone approaches that preserve independent paths are a later, separately validated feature.

Track logical size and allocated blocks separately. Sparse files, existing hardlinks, shared clone extents, snapshots and Docker's shared layers mean apparent size is not guaranteed reclaimable space. Avoid summing overlapping folder findings and duplicates inside them. Show savings as estimates and distinguish Docker-internal reclamation from host filesystem free space.

## 7. Review, cleanup and recovery

Background discovery does not itself authorize deletion. The action executor requires an approved plan or a matching, explicitly enabled automatic policy.

1. Generate an immutable plan containing exact targets, identities, evidence, keeper choices, estimated savings and action types.
2. Show the plan and get explicit approval tied to that plan ID, or bind the plan to the exact previously approved automatic policy version.
3. Revalidate targets immediately before each operation. Reject excluded paths, changed roots/volumes, symlink substitutions and stale objects.
4. Use descriptor-relative filesystem operations and no-follow checks where supported. Avoid shell interpolation and protect against path replacement races. Filesystem activity cannot be universally locked out; skip cases where the required safety checks cannot be maintained.
5. Journal intent durably, perform the operation, then record success/failure. On restart reconcile interrupted actions from both filesystem and journal evidence; never blindly repeat a deletion.
6. Offer restoration with collision handling: never overwrite a newly created original path.

Default file action: move to a private, application-managed quarantine on the **same filesystem**, using rename where safe. If no suitable quarantine location exists, explain the limitation and leave the item untouched. Do not silently copy a large directory across filesystems or switch to permanent deletion.

Quarantine supports recovery but **does not free disk space**. Report quarantined bytes separately; a separately approved purge releases eligible storage and is irreversible. Automatic purge is disabled unless explicitly included in an approved policy with a retention period. Native OS Trash integration can be added after cross-platform behavior is validated.

Docker actions use Docker's supported interfaces, not filesystem deletion inside its storage. Pin and display the local Docker endpoint/context and builder; refuse remote contexts by default. Use exact object IDs where supported. When a prune command only supports a predicate, explain the dynamic scope and require approval for that predicate; do not present it as an exact frozen object list. Skip if the context or supported semantics change. Bound subprocess execution and output.

Docker cleanup is irreversible through this app and must be labeled accordingly. Unreferenced images may contain local-only work. An unattached volume can hold valuable data; Docker itself treats volumes conservatively because deleting them can destroy data. [Docker pruning documentation](https://docs.docker.com/engine/manage-resources/pruning/)

### Opt-in automatic cleanup — included in v1

Offer automatic cleanup only for a small allowlist of specifically supported regenerable caches/build artifacts. Add at least one thoroughly tested cache detector/action pair in v1 to make this feature useful. A recognized directory name alone is insufficient; automatic eligibility requires detector-specific validation of ownership, regeneration inputs and absence of unsupported/custom content. Old `node_modules` remains review-required initially.

Each policy must explicitly specify:

- Eligible detector/category and selected roots; exclusions always win.
- Age/observed-stability threshold, minimum observation history and maximum finding age.
- Maximum bytes/items per run and per day; exceeding limits pauses further actions.
- Action mode: quarantine only, or quarantine followed by automatic purge after a chosen retention period.
- Whether and when the policy may run, including battery behavior.

Enablement presents a dry-run preview, explains regeneration costs and irreversible purge, and requires explicit acceptance. No automatic purge is inferred from merely enabling quarantine. Policy edits that expand scope or shorten retention require renewed approval. Record the policy version and evidence with every action; recheck enabled status, exclusions and limits before every item and purge. Disabling a policy cancels its pending automatic actions, including scheduled purges, while preserving quarantined files for restoration.

The executor uses the same revalidation and crash-recovery pipeline for manual and policy-approved actions. A stale finding is rescanned, not deleted from old evidence. Missing prerequisites, uncertain ownership, active/changing artifacts or unsupported platform behavior stop the action. Keep duplicates, personal files, Docker objects and volumes outside automatic eligibility in v1. Provide `policy preview`, `policy enable`, `policy disable` and an activity history. Automatic actions consume resource budgets too; cleanup must not become the source of background I/O spikes.

## 8. Background service and platform compatibility

### macOS

- Install a per-user LaunchAgent; run while the user is logged in and resume at next login.
- Use background process classification and low-priority I/O, validated against the installed `launchd.plist` manual.
- Respect privacy restrictions. Show inaccessible locations; explain optional Full Disk Access only when needed for the user's chosen roots.
- Avoid materializing cloud placeholders; validate supported platform controls and skip unsupported provider cases.

Apple documents LaunchAgents as the preferred mechanism for per-user background work. [Apple launchd guide](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)

### Linux

- Install a systemd user service where available; otherwise document running `rydd daemon` under an existing supervisor.
- Use low CPU/I/O priority where supported; cgroup resource controls are supplemental because availability/delegation varies.
- Keep internal resource budgets functional without systemd or elevated privileges.
- User services depend on the user manager lifecycle; running after logout via lingering is a separate explicit setup choice.

[systemd resource control definitions](https://github.com/systemd/systemd/blob/main/man/systemd.resource-control.xml), [systemd delegation](https://systemd.io/CGROUP_DELEGATION/).

Initial release targets: macOS arm64/amd64 and Linux arm64/amd64. Select minimum OS versions and Linux libc support during packaging validation. Verify behavior on APFS and ext4; add Btrfs/XFS-specific savings support only with testing. Always respect case sensitivity, unusual filenames, permissions, filesystem boundaries and interrupted mounts.

Installation should include status, diagnostic, stop and uninstall commands. Uninstall removes the service/executable while explicitly preserving configuration, inventory and quarantine unless the user chooses otherwise.

## 9. Proposed terminal workflow

The terminal command is `rydd`; the public repository/module is `github.com/bjornarhagen/saga-rydd`.

```text
rydd init                         Choose roots, exclusions and resource profile
rydd service install              Enable background scanning
rydd status                       Worker, coverage, last activity and budgets
rydd report                       Findings grouped by category and estimated savings
rydd report --duplicates          Duplicate groups and possible keepers
rydd explain <finding-id>         Evidence, freshness, risks and restoration details
rydd ignore <finding-id>          Dismiss a finding or exclude a path
rydd pause / rydd resume           Control background work
rydd plan <finding-ids...>        Produce a reviewable cleanup plan
rydd apply <plan-id>              Review, confirm, revalidate and execute
rydd restore <action-id>          Restore quarantined files without overwriting
rydd purge <action-id>            Review and confirm permanent removal
rydd policy preview <policy-id>  Show matching targets, scope and recovery behavior
rydd policy enable <policy-id>   Review and explicitly enable automatic cleanup
rydd policy disable <policy-id>  Stop future automatic actions under this policy
rydd report --json               Machine-readable export
rydd doctor                      Explain service, permission and state problems
```

Start with readable tables, filters and prompts. A full-screen TUI can follow once scanning and cleanup are trustworthy. Reports must remain useful during partial scans and paginated for large inventories.

## 10. Existing scripts: what to reuse

Inspected, but did not execute:

- `docker-cleanup.sh` from the founder's existing development utility scripts.
- `dev-folders-cleanup.sh` from the same collection.

Reuse category ideas and explicit review prompts. Reimplement the mechanics:

- The dependency script relies on directory mtime, uses macOS-specific `stat -f`, parses paths as whitespace-separated fields, uses `eval` for input expansion, and reruns the search during deletion instead of acting on a frozen reviewed set.
- The Docker script starts an Alpine container to measure every volume and mounts the volume read-write. Background discovery should not pull images or launch helper containers. Use bounded Docker metadata calls; report unavailable sizes honestly.
- Both scripts can report success without reliably checking each operation's outcome. Record actual exit status and partial failures.
- Do not carry broad image/volume pruning into a supposedly safe automatic category.

## 11. Delivery phases and acceptance gates

These group the full product scope. Use the current execution priority above for task selection; completing every Phase 1 item is not required before the read-only Phase 2 MVP slice. Full gate completion must still be supported by evidence.

### Phase 1 — read-only foundation

Implement binary/CLI skeleton, config, SQLite migrations, root selection, exclusions, persistent scheduler, resource budgets, incremental inventory and reports. Add service adapters and one-instance enforcement.

Acceptance: resumes after termination and sleep; handles permission failures and disconnected mounts; reports open while scanning; bounded memory on wide/deep trees; no deletion capability. Benchmark SQLite driver choice and state growth here.

### Phase 2 — useful recommendations

Add `node_modules`, one recognized build-output category, at least one cache category suitable for tightly scoped automation, opt-in Docker metadata inventory, finding explanations, ignored paths and freshness. Measure folders incrementally and prevent double counting.

Acceptance: fixture projects produce explainable findings; modified or unrecognized projects are downgraded/skipped; missing Docker is harmless; Docker scans do not launch containers, pull images, or contact remote contexts.

### Phase 3 — exact duplicates

Add size grouping, sample filtering, resumable full hashes, hardlink accounting, keeper selection and estimated savings.

Acceptance: matches independently verified fixture groups; samples never authorize deletion; handles changing/very large files, hardlinks, sparse files, clones and daily-budget rollover; completes large jobs without starving other roots.

### Phase 4 — manual and opt-in automatic cleanup

Add immutable plans, revalidation, quarantine, restoration, explicit purge, durable action recovery and carefully scoped manual Docker cleanup. Add policy preview/enable/disable, category eligibility, scope/age limits, quotas, and explicitly enabled retention-based purge. This completes the planned v1 action scope, including assisted deletion/deduplication and opt-in automatic cleanup. The earlier read-only MVP is independently useful and does not wait for this phase.

Acceptance: test stale plans, symlink/path swaps, modified keepers, partial failures, crash points between journal and filesystem updates, interrupted purges, restoration collisions and full disks. Test automatic-policy scope boundaries, disabled/revoked policies, quota accounting after restart, retention clocks, and exclusion changes before purge. Any uncertain action must stop safely and provide an actionable result.

### Phase 5 — unattended soak and release

Run a 1–2 week read-only soak on representative macOS and Linux machines, then controlled cleanup trials on disposable fixtures. Release packaged binaries and clear service setup/uninstall instructions.

Acceptance: measure hour-average CPU, RSS, content reads/day, metadata operations, state/WAL growth, idle wakeups and report latency. Suggested report target: under one second for the default summary on the million-entry fixture. Tune budgets from measurements; verify that stopping/pausing works and laptop sleep remains undisturbed.

Later candidates: additional ecosystems and automatic categories, personal-file recommendations, optional notifications, native Trash, filesystem event acceleration, and reflink-based deduplication. Each needs its own safety/compatibility validation.

## 12. Decisions to settle before implementation

1. Confirmed: include opt-in automatic cleanup in v1; settle the first eligible cache category during detector design.
2. Confirmed: developer clutter plus exact duplicates as the first focus.
3. Confirmed: Go, with SQLite and TOML; develop primarily through Docker.
4. Choose initial roots/resource defaults during onboarding design and benchmarks.
5. Confirmed: Saga — Rydd; repository `saga-rydd`, CLI `rydd`.

Current milestone: useful saved reports and one review-required `node_modules` recommendation category, followed by controlled real-folder feedback. See the execution priority above and the task handoff in PROGRESS.md.
