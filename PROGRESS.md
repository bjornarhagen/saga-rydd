# Saga — Rydd: progress and handoff

This file is the canonical implementation tracker. The architecture and full acceptance gates live in [PLAN.md](PLAN.md). Keep this ledger updated in the same commit as the work it describes.

## Current state

- **Stage:** P1-01–P1-05, P1-06a and P1-08a complete; experimental metadata inventory verified on native macOS/Linux and Docker. The rest of Phase 1 remains open.
- **App / brand:** Rydd / Saga. Repository: `bjornarhagen/saga-rydd`; executable: `rydd`.
- **Confirmed:** Go, local SQLite, TOML configuration, macOS/Linux, low resource usage, developer clutter plus duplicates, opt-in automatic cleanup in v1.
- **Development:** Docker first; do not require host Go or additional host development tooling.
- **Implemented app behavior:** initialization, saved/live status, worker controls and opt-in experimental metadata scanning; private TOML/SQLite state, exclusive writer lock, bounded batches, atomic inventory/job commits and directory reconciliation watermarks. Durable dispatch cadence/daily batch reservations, WAL backpressure and versioned JSON commands are implemented. Fine-grained CPU/I/O/power enforcement, service installation, findings and cleanup remain unavailable.
- **Active task:** P1-06b1 scanner metadata API accounting and live human/JSON diagnostics. Rate enforcement follows after accounting coverage is verified.
- **Blockers:** none currently. Scanner resource targets and native service behavior remain unvalidated; the database experiment is not a scanner benchmark.

## How to use this tracker

- `[ ]` means unfinished; `[x]` means implemented and verified. Use the active-task section for in-progress work rather than checking items early.
- Preserve task IDs when splitting or extending work. Add evidence (test names, commands, PRs/commits or reports) when completing an item.
- A phase is complete only when its acceptance gate is verified, not merely when its implementation tasks are checked.
- Record blockers and exact next steps for a fresh agent. Keep machine-specific/private paths and inventory data out of the public record.

## Bootstrap

- [x] B01 — Record architecture, safety model, delivery phases and resource targets in `PLAN.md`.
- [x] B02 — Confirm Saga — Rydd naming, Go and Docker-first development.
- [x] B03 — Add agent instructions, contribution guide, stable task IDs and handoff convention.
- [x] B04 — Validate disposable Docker workflow, UID/GID handling, formatting/vet/build commands and four-target cross-builds.
- [x] B05 — Publish public GitHub repository and verify remote/default branch.
- [x] B06 — Add and verify Linux/macOS CI and Docker workflow checks against the published commit.

## Phase 1 — read-only foundation

- [x] P1-01 — Compare SQLite drivers for RSS, state size, crash behavior and macOS/Linux build portability; record decision and benchmark method. See ADR 001 and CI evidence below.
- [x] P1-02 — Implement TOML configuration, standard platform data directories, root selection, exclusions and validation. Tested in Docker and native macOS/Linux CI.
- [x] P1-03 — Implement SQLite schema/migrations, short transactions, WAL maintenance and durable-vs-rebuildable state boundaries. Tested in Docker and native macOS/Linux CI. Scheduler enforcement of WAL backpressure belongs to P1-06.
- [x] P1-04 — Implement one-worker lock, private control channel, pause/resume and saved scheduler jobs. Verified with process-kill recovery, native macOS/Linux CI, Docker controls and four-target builds; see worker evidence below.
- [x] P1-05 — Implement streaming inventory, volume identity, symlink/mount boundaries, permission errors and reconciliation. Verified on bounded fixtures, process-kill recovery and native CI including a real Linux bind mount. Stale descendant lifecycle/fair revisits remain P1-07; large-scale resource and physical/provider tests remain open gates.
- [ ] P1-06 — Implement independent metadata/read/CPU budgets, persistent daily limits, sleep/backoff and power-state fallbacks.
  - [x] P1-06a — Persist scan dispatch cadence/daily batch cap; gate discovery on WAL checkpoint progress.
  - [ ] P1-06b1 — Count scanner metadata API attempts, including traversal/revalidation and failed calls; expose bounded live counters.
  - [ ] P1-06b — Meter metadata/content/CPU, enforce daily resource consumption and add low-priority/power/sleep controls.
- [ ] P1-07 — Implement adaptive revisits, fair scheduling, large-directory continuation and stale/missing-entry handling.
- [ ] P1-08 — Implement paginated saved reports, coverage/freshness, diagnostics and bounded logs/state growth.
  - [x] P1-08a — Versioned JSON controls/status, stable machine errors and capability discovery for AI and scripts.
- [ ] P1-09 — Implement launchd/systemd user service installation, status, stop and uninstall; document other supervisors.
- [ ] P1-GATE — Verify restart/sleep recovery, disconnected volumes, permissions, concurrent reports and bounded memory on wide/deep million-entry fixtures. No deletion capability in this phase.

## Phase 2 — useful recommendations

- [ ] P2-01 — Implement evidence-based finding model, rule versions, risk/confidence, dismissal and persistent exclusions.
- [ ] P2-02 — Implement old `node_modules` detection with project recognition and incremental directory measurement.
- [ ] P2-03 — Implement one recognized build-output category and one narrowly validated cache category eligible for later automation.
- [ ] P2-04 — Implement opt-in bounded Docker metadata discovery with pinned local context/builder and no helper containers/image pulls.
- [ ] P2-05 — Implement finding explanations, regeneration caveats, partial sizes, freshness and overlap-aware totals.
- [ ] P2-GATE — Verify supported/modified/unrecognized project fixtures; missing Docker and unknown sizes are harmless; no remote Docker access or broad disposable-category assumptions.

## Phase 3 — exact duplicates

- [ ] P3-01 — Implement size grouping, minimum size, generated-tree exclusions and hardlink accounting.
- [ ] P3-02 — Implement bounded samples, full hashing and before/after metadata checks with invalidation.
- [ ] P3-03 — Implement durable continuation for hashes larger than work/day budgets, without starving other work.
- [ ] P3-04 — Implement duplicate reports, keeper selection and allocated-vs-logical savings estimates.
- [ ] P3-GATE — Verify against independently known duplicate groups; handle changed/huge files, sparse files, hardlinks, clones and budget rollover. Samples never authorize deletion.

## Phase 4 — manual and opt-in automatic cleanup

- [ ] P4-01 — Implement immutable exact-target plans with approval IDs, keeper choices, evidence and explicit action semantics.
- [ ] P4-02 — Implement action-time identity/scope/policy checks, fresh duplicate verification and descriptor-relative path safeguards.
- [ ] P4-03 — Implement same-filesystem quarantine and restoration with collision handling; unsupported cases leave originals untouched.
- [ ] P4-04 — Implement durable intent/result journal and crash reconciliation, including interrupted purges and idempotent requests.
- [ ] P4-05 — Implement separately approved permanent purge and clear quarantined/reclaimed accounting.
- [ ] P4-06 — Implement narrowly scoped manual Docker actions; explain irreversible/dynamic prune scope and reject changed contexts.
- [ ] P4-07 — Implement automatic policy preview/enable/disable, versioned approval, allowlisted category/root/stability rules and quotas.
- [ ] P4-08 — Implement explicitly enabled retention-based purge, revocation checks, exclusion precedence and resource budgeting for automatic actions.
- [ ] P4-GATE — Verify symlink/path swaps, stale plans, modified keepers, journal crash points, partial failures, full disks, restore collisions, policy revocation, restart quotas and retention clocks. Uncertain actions stop safely.

## Phase 5 — soak and release

- [ ] P5-01 — Finalize supported OS/libc/architecture matrix and package native binaries with verified service setup/uninstall.
- [ ] P5-02 — Benchmark hourly CPU, RSS, reads/day, metadata operations, state/WAL growth, idle wakeups and report latency; tune documented defaults.
- [ ] P5-03 — Complete a 1–2 week native read-only soak on representative macOS and Linux machines; publish sanitized measurements and limitations.
- [ ] P5-04 — Complete controlled manual/automatic cleanup and recovery trials on disposable native fixtures.
- [ ] P5-05 — Finish onboarding, recovery/troubleshooting documentation, release/versioning process and project license choice.
- [ ] P5-GATE — Evidence supports resource targets, reliable pause/stop/sleep behavior, recovery and all supported platforms; publish the first usable release.

## Validation evidence

- 2026-09-15: `sh -n scripts/dev scripts/tasks` and `git diff --cached --check` passed.
- 2026-09-15: `./scripts/dev check`, `./scripts/dev race`, and `./scripts/dev build-all` passed using the existing Docker engine; no host Go/tooling was installed.
- All four binaries were verified as the expected Mach-O/ELF architectures. The macOS arm64 binary ran natively: help/version succeeded and an unsupported `daemon` command returned exit status 2. Host output ownership matched the invoking user.
- Historical bootstrap tests reported **no test files**; those early results validated only the workflow. Feature tests were added with the state/config foundation below.
- The [public repository](https://github.com/bjornarhagen/saga-rydd) was created and verified with default branch `main`; the initial bootstrap commit is `9e0f884`.
- GitHub [CI run 34971463782](https://github.com/bjornarhagen/saga-rydd/actions/runs/34971463782) passed for bootstrap commit `9e0f884`: native Linux checks (24s), native macOS checks (38s), and Docker workflow/four-target cross-builds including Linux output ownership (48s).
- `docker compose ps --status running` confirmed no development containers remained running after validation. Only the reusable development image/cache volume remains.

### Configuration/state foundation

- [ADR 001](docs/decisions/001-sqlite-driver.md) records the reproducible one-million-entry SQLite comparison. Both drivers passed snapshot/rollback and forced-process-kill checks. Selected modernc used 24.3 MiB peak RSS; both database files were 252.3 MiB. These are isolated unthrottled synthetic measurements.
- Docker tests cover strict TOML/defaults/path validation, no-overwrite initialization, read-only status, migration identity/version refusal, stable root registration and rollback, connection replacement settings, non-UTF-8 path bytes, foreign keys, WAL snapshots/checkpoints, process-crash recovery and writer contention/cancellation.
- Fixed development cache ownership conflicts by separating cache directories per UID and allowing noninteractive commands through `scripts/dev shell -c`.
- `./scripts/dev check`, `./scripts/dev race`, `./scripts/dev sqlite-check` and `./scripts/dev build-all` passed. All four outputs were verified as the expected Mach-O/ELF architectures.
- The Docker-built macOS arm64 binary ran `init`, `config check`, `status --json` and `state init` natively against a private ignored fixture. It registered one root with zero scanned entries and a 61,440-byte initial database; no host Go installation was needed.
- [CI run 34974105231](https://github.com/bjornarhagen/saga-rydd/actions/runs/34974105231) passed for implementation commit `491d932`: native macOS (2m59s), native Linux (2m5s), and Docker/four-target builds (2m47s). Native jobs ran application tests/race checks plus both SQLite candidates' correctness checks and 100,000-row comparisons.
- All development containers exited. No host Go/tooling or background service was installed. A small ignored native test fixture and reusable development caches remain.

### Worker foundation

- Schema v2 migration tests preserve v1 root IDs, saved job cursors and settings; readers require explicit migration rather than changing state.
- Lock tests reject a second writer, symlinks and hardlinks and retain the lock inode across close/reopen. Worker tests cover durable pause, queue due/filter/idempotency rules, stale lease fencing, disabled/unknown jobs, chunk pacing, cancellation, panic/error backoff and bounded invalid control requests.
- `TestProcessKillRecoveryAndSIGTERM` kills a separate worker with an active lease, restarts through its stale socket, verifies the saved cursor and recovery count, and checks SIGTERM plus persistent pause across process restarts.
- `./scripts/dev check`, `./scripts/dev race` and `./scripts/dev build-all` passed. `scripts/worker-smoke` passed with the Docker-built native macOS arm64 binary and the Linux container binary. No host Go installation was used.
- Separate Docker command containers successfully paused, queried, resumed and stopped a fixture worker through the shared runtime volume. All fixture workers exited; no service was installed.
- Initial [CI run 34977696796](https://github.com/bjornarhagen/saga-rydd/actions/runs/34977696796) passed native macOS/Linux, but Docker exposed a pacing error under slower job claims: database latency shortened the gap between handler starts. Pacing now uses the actual handler start time; ten repeated pacing regressions passed locally, along with checks/race/four-target builds.
- Final [CI run 34978064639](https://github.com/bjornarhagen/saga-rydd/actions/runs/34978064639) passed for implementation commit `979257c`: native Linux (32s), native macOS (49s), Docker checks/four-target builds/native Linux binary smoke (2m37s). Native jobs ran application tests, race checks, SQLite experiments and the new worker CLI smoke. P1-04 is complete; full Phase 1 gates remain open.

### Experimental inventory

- Schema v3 adds directory reconciliation watermarks and skip reasons. `CommitScan` atomically saves metadata, child jobs and cursor/completion with a lease fence; tests inject a constraint failure after partial SQL work and verify complete rollback.
- Scanner tests verify 128-entry bounds, 301-file enumeration/restart, root replacement, cancellation, private inode aliases, exclusions, symlink refusal, FIFO metadata, sparse/hardlinked files, non-UTF-8 names where supported, and unprivileged permission failures. Scanner close does not wait behind a blocked filesystem call.
- `TestInventoryWorkerKillAndComplete` kills a worker after its first saved batch, restarts it, then verifies 342 unique observations across a 20-level fixture tree and 21 completed directory passes with an empty queue. Existing lease-crash/SIGTERM tests remain enabled.
- `./scripts/dev check`, `./scripts/dev race` and `./scripts/dev build-all` passed. The scanner smoke script passed natively on macOS arm64 and inside Linux Docker using only five synthetic entries. No host toolchain or background service was installed.
- [CI run 34985645161](https://github.com/bjornarhagen/saga-rydd/actions/runs/34985645161) passed for implementation commit `ef88200`: native macOS (1m1s), native Linux (41s, including a real bind mount in a private namespace), and Docker/four-target builds/native binary smoke (2m54s). P1-05 is complete. Cloud providers, physical remount/rebind, million-entry resource bounds and laptop sleep remain unverified phase gates.

### Dispatch limits and machine CLI

- implemented P1-06a durable dispatch cadence/daily batch reservations (schema v4), WAL checkpoint backpressure, and P1-08a versioned JSON controls/status/errors/capability discovery. Docker formatting/vet/tests/race and four-target builds passed; native macOS worker/scanner smoke passed. Tests cover worker daily cap across restart with responsive controls, UTC rollover/backward clocks, pinned WAL readers, and JSON success/error contracts. [CI run 34989487603](https://github.com/bjornarhagen/saga-rydd/actions/runs/34989487603) passed for implementation commit `f2890e7`: native Linux (1m3s), native macOS (1m6s), and Docker checks/four-target builds/native binary smoke. P1-06a and P1-08a are complete; parent tasks and Phase 1 gates remain open. The local test installation was updated and its schema migrated with saved fixture inventory preserved; the worker remains stopped.

## Handoff

**Last updated:** 2026-09-15.

**Completed this session:** P1-06b1 live scanner API accounting, with atomic counters for stat, directory open/read, filesystem/mount checks and path resolution. Startup validation, failed attempts and final revalidation are included. Human status and JSON control snapshots expose the counters; capability discovery distinguishes accounting from rate enforcement. Docker formatting/vet/tests/race and all four cross-builds passed, along with native macOS worker/scanner fixture smoke. Native CI is pending; the subitem remains unchecked until verified. No schema change or dependency added.

**Next action:** finish native CI verification for P1-06b1, then continue P1-06b rate enforcement. Cover opaque path-resolution internals and database work; add persistent consumption reservations, CPU/byte metering and low-priority/power/sleep controls. Test low metadata rates with short work deadlines: repeatedly discarding a timed-out batch must not prevent eventual progress. Live API counters reset per worker and do not themselves enforce a rate. Preserve cancellation/restart semantics and validate fixtures before unattended personal-root scanning. P1-07 covers fair/adaptive revisits, huge-directory continuation, stale descendant filtering and reviewed root rebinding. See [CLI contract](docs/cli.md) and [inventory design](docs/inventory.md).

**Remaining decisions:** first automation-eligible cache category; supported minimum OS/libc versions; tuned resource/scan-root defaults; license. Go and naming are settled.

**Do not infer:** the database benchmark is not an hourly scanner resource test; dispatch reservations do not enforce CPU/I/O consumption or battery budgets. WAL backpressure is not a strict database-size cap. Ordinary `daemon` stays idle; scanning requires `--experimental-scan` each start. The root fingerprint is a scoped identity hint, not cleanup authorization. Root completion only covers direct children, and summary counts retain old observations. Huge-directory fairness, stale descendant lifecycle, cloud-provider hydration avoidance, physical remount/rebind and million-entry resource/sleep tests remain open. No findings, cleanup or service installation exists. Future action/restore data must never be removed by inventory rebuilds. Stop the worker before `state init`; migrations preserve inventory and configuration.
