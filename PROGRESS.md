# Saga — Rydd: progress and handoff

This file is the canonical implementation tracker. The architecture and full acceptance gates live in [PLAN.md](PLAN.md). Keep this ledger updated in the same commit as the work it describes.

## Current state

- **Stage:** P1-01–P1-05, P1-06a, P1-06b1/b2 and P1-08a/b1/b2 complete; experimental metadata inventory verified on native macOS/Linux and Docker. The rest of Phase 1 remains open.
- **App / brand:** Rydd / Saga. Repository: `bjornarhagen/saga-rydd`; executable: `rydd`.
- **Confirmed:** Go, local SQLite, TOML configuration, macOS/Linux, low resource usage, developer clutter plus duplicates, opt-in automatic cleanup in v1.
- **Development:** Docker first; do not require host Go or additional host development tooling.
- **Implemented app behavior:** initialization, saved/live status, paginated largest-file and selected-directory size reports, worker controls and opt-in experimental metadata scanning; private TOML/SQLite state, exclusive writer lock, bounded batches, atomic inventory/job commits and directory reconciliation watermarks. Durable dispatch cadence/daily batch reservations, child-entry pacing with partial batches, live scanner accounting, WAL backpressure and versioned JSON commands are implemented. Fine-grained CPU/I/O/power enforcement, service installation and cleanup remain unavailable; the first review-required node_modules candidate report is available.
- **Active task:** MVP-TRIAL — P2-05b age override implemented and exercised against private saved inventory. Owner review of candidate usefulness and activity remains open; default/background rollout remains open.
- **Blockers:** no scan/report blocker. Further trial feedback and full unattended resource/service gates remain open.

## Execution priority — read-only MVP first

**Approved sequencing decision:** deliver useful reports and one recommendation category before completing unattended-operation infrastructure. The numbered phases below organize scope and acceptance; they are not a strict execution order. This section supersedes the previous resource-controls-first handoff.

Follow this order:

1. **P1-08b1:** paginated largest-observed-files report with coverage/freshness in human and JSON output; useful with the worker stopped and while scanning is incomplete.
2. **P1-08b2:** directory-size reporting with explicit partial/stale/unknown states and qualified logical/allocated sizes.
3. **P2-01a → P2-02a:** minimal finding model and one end-to-end, review-required `node_modules` detector/report. Implement the necessary explanation/freshness/overlap slice of P2-05 alongside it.
4. **MVP-TRIAL:** evaluate the reports and recommendations on an explicitly selected real development folder after fixture validation. Record sanitized observations and use them to reprioritize further work.
5. Then add duplicate detection and reviewed cleanup, alongside the remaining resource controls, according to what the trial reveals.

Do not default to low-priority scheduling, complete CPU/battery budgets, service installation or more general diagnostics as the next task. Those remain required for unattended/release readiness, but do not gate a controlled read-only MVP. Implement supporting work now only when it directly blocks report correctness, selected-root safety or the trial; document that dependency. Existing filesystem safeguards, bounded work, pause/stop, scope restrictions and truthful size/freshness reporting remain required. No deletion or automatic broad personal-folder scan is authorized by this sequencing change.

The next milestone is: **Rydd shows useful cleanup candidates with enough evidence for a person to assess them.** It is not the complete v1 release.

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
  - [x] P1-06b1 — Count scanner metadata API attempts, including traversal/revalidation and failed calls; expose bounded live counters.
  - [x] P1-06b2 — Pace child-entry inspections and preserve partial-batch progress across throttle waits; expose rate/wait diagnostics.
  - [ ] P1-06b — Meter metadata/content/CPU, enforce daily resource consumption and add low-priority/power/sleep controls.
- [ ] P1-07 — Implement adaptive revisits, fair scheduling, large-directory continuation and stale/missing-entry handling.
  - [x] P1-07a — Finish existing queued inventory work before root revisits; preserve interrupted directory priority across cancellation/recovery.
- [ ] P1-08 — Implement paginated saved reports, coverage/freshness, diagnostics and bounded logs/state growth.
  - [x] P1-08a — Versioned JSON controls/status, stable machine errors and capability discovery for AI and scripts.
  - [x] P1-08b1 — paginated largest-observed-files report, saved-inventory coverage/freshness, human/JSON parity and offline availability.
  - [x] P1-08b2 — incremental/bounded directory-size reports with partial/stale/unknown labels and logical/allocated-size qualifications.
  - [x] P1-08c — Foreground `scan -d PATH [-s MS | --now]`, isolated per-folder state, cancellation and `report -d PATH` / scoped candidate reports.
- [ ] P1-09 — Implement launchd/systemd user service installation, status, stop and uninstall; document other supervisors.
- [ ] P1-GATE — Verify restart/sleep recovery, disconnected volumes, permissions, concurrent reports and bounded memory on wide/deep million-entry fixtures. No deletion capability in this phase.

## Phase 2 — useful recommendations

- [ ] P2-01 — Implement evidence-based finding model, rule versions, risk/confidence, dismissal and persistent exclusions.
  - [x] P2-01a — minimum saved finding/report contract for one detector: identity, rule/version, evidence, review requirement, freshness and measured-size completeness; keep unsupported actions explicit.
- [ ] P2-02 — Implement old `node_modules` detection with project recognition and incremental directory measurement.
  - [x] P2-02a — First end-to-end recommendation: recognized project, potentially stale dependencies, measured/partial size, evidence and regeneration caveats in human/JSON reports. Age alone never establishes safety or actual inactivity.
  - [ ] P2-02b — **Next after P1-07a:** represent node_modules as one logical item with resumable aggregate size measurement; stop storing each descendant as ordinary inventory by default. Preserve partial/stale/unknown, hardlink accounting, scope protections and interruption recovery; support explicit detailed inventory.
  - [x] P2-02b1 — Validate compact directory totals plus inode evidence against ordinary inventory, including interruption/replay, mutation and measured database size; record a production integration design. This does not enable aggregation in the app.
  - [x] P2-02b2 — Opt-in compact manual scan persistence, cached directory logical totals, bounded allocated identity checks, pass-pinned detailed override, legacy-overlap suppression and durable bounded retirement. Verified in Docker and on native macOS; default/background enablement and full allocated reductions remain open.
  - [x] P2-02b3 — Exclude absent descendants from saved directory totals using completed ancestor membership evidence; expose exclusions, retain incomplete/error evidence, and verify compact disappearance/replacement. Docker checks/race/four-target builds and native macOS/Linux fixture smoke passed; native Linux evidence is recorded in CI run `37067698111`. This bounds report correctness work without enabling background scans or deleting saved evidence.
  - [x] P2-02b4 — Budgeted, resumable allocated reductions for manual-root/outermost node_modules scopes, with root-revision invalidation, scoped hardlink deduplication, conflict/unknown accounting and bounded fallback. Docker checks/race/four-target builds and native macOS fixture/recovery tests passed. Schema 6 is additive; schema-4/5 reports remain readable. Native Linux and 100,000-identity checks passed in CI run `37067698111`; broader resource/default rollout gates remain open.
  - [x] P2-02b5 — Retire disappeared subtree inventory, compact inode evidence and obsolete scope caches in manual compact stores through durable bounded work. Scan revisions fence fresh reappearance observations; retirement invalidates allocated coverage. Docker checks/race/builds and native macOS disappearance/replacement/recovery/storage fixtures passed. Schema 7 preserves existing state and schema-4–6 report readability. User files and future action/restore data remain untouched.
  - [ ] P2-02b6 — Gather larger production scan/reduction/retirement resource and native-platform evidence before default/background rollout: reproducible state/WAL/backlog, memory, latency and interrupted-work measurements on generated fixtures, native Linux CI, and owner feedback on selected real scopes.
  - [x] P2-02b6a — Reproduce 100,000-identity production CLI scans, rescans, shrink/disappearance/reappearance and forced scan/maintenance interruptions; measure child peak RSS, elapsed/report latency, database/WAL/pages and durable backlog on native platforms. Native macOS and Linux CI passed; sanitized measurements are in `experiments/compactscale/results`.
  - [x] P2-02b6b — One-million-identity production fixture passed on native macOS and Linux, including three SIGKILL recoveries, shrink/restore, two disappearance/reappearance cycles, exact saved reports and drained work. Sanitized results compare memory/state/latency with 100,000 identities; full CI `37211415803` passed. Default/background rollout remains open.
- [ ] P2-03 — Implement one recognized build-output category and one narrowly validated cache category eligible for later automation.
- [ ] P2-04 — Implement opt-in bounded Docker metadata discovery with pinned local context/builder and no helper containers/image pulls.
- [ ] P2-05 — Implement finding explanations, regeneration caveats, partial sizes, freshness and overlap-aware totals.
  - [x] P2-05a — explain empty candidate pages with bounded human/JSON selection diagnostics: mutually exclusive rejection counts, inspected coverage and continuation. Distinguish age rejection, missing/unsupported manifest, skips and unconfirmed/incomplete parent evidence. Preserve the current rule and review requirement; add fixture regressions.
  - [x] P2-05b — configurable candidate report age with validated bounds, explicit effective threshold and consistent pagination; evaluate a lower threshold on private saved trial evidence.
- [ ] P2-GATE — Verify supported/modified/unrecognized project fixtures; missing Docker and unknown sizes are harmless; no remote Docker access or broad disposable-category assumptions.

## Read-only MVP trial

- [ ] MVP-TRIAL — After P1-08b1/b2 and P2-01a/02a, use an explicitly selected real development folder in a controlled read-only trial. Verify report usefulness, partial/stale sizes, false positives, overlap/double counting and the evidence needed for decisions. Preserve sanitized findings and revise priorities. Fixture/native validation precedes the trial; this does not mark any full phase or release gate complete.

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

### Live scanner accounting

- P1-06b1 live scanner API accounting, with atomic counters for stat, directory open/read, filesystem/mount checks and path resolution. Startup validation, failed attempts and final revalidation are included. Human status and JSON control snapshots expose the counters; capability discovery distinguishes accounting from rate enforcement. Docker formatting/vet/tests/race and all four cross-builds passed, along with native macOS worker/scanner fixture smoke. Initial CI passed native macOS but Linux race testing exceeded the existing five-second inventory-recovery deadline (no data race reported). That correctness test now has a 30-second bound, early worker-exit detection and saved-progress diagnostics. Ten repeated Docker race runs of inventory recovery and daily-cap restart passed (36.6s total), followed by checks. [Final CI 34999962258](https://github.com/bjornarhagen/saga-rydd/actions/runs/34999962258) passed for `ca8fcf0`: native macOS (56s), native Linux (42s), and Docker checks/four-target builds/native binary smoke. P1-06b1 is complete; P1-06b and Phase 1 remain open. The local test binary was updated; configuration and saved state were preserved and the worker remains stopped. No schema change or dependency added.

### Entry pacing

- P1-06b2 child-entry pacing using the existing `metadata_per_second` setting, interruptible waits without accumulated burst credits, bounded pending names and validated partial batches. Live human/JSON status reports effective entry rate, inspection count and throttle duration/state; capabilities distinguish entry pacing from full metadata limits. Docker checks/race/four-target builds and native macOS worker/scanner smoke passed. Tests cover minimum-rate completion in one-second windows, short-window partial batches, mutation invalidation, cancellation/no catch-up, paced worker pause/resume/completion and crash recovery. The crash fixture now uses a one-batch cap before the kill, avoiding a disk-speed assumption about the observation window. [CI run 35001556510](https://github.com/bjornarhagen/saga-rydd/actions/runs/35001556510) passed for implementation commit `199e182`: native Linux (55s), native macOS (1m24s), and Docker checks/four-target builds/native binary smoke. P1-06b2 is complete; the parent resource task and Phase 1 gates remain open. The installed test binary was updated; inventory/configuration are preserved and the worker remains stopped. No dependency or schema change.

### Saved file reports

- implemented P1-08b1 `rydd report`: size/ID cursor pagination (default 20, max 200), logical/allocated sizes, observation/modification times, direct-parent generation labels, saved root diagnostics, quoted human output and JSON with byte-preserving paths. Reports work from saved state without a worker, config load or filesystem scan. Each page has one read snapshot and a five-second deadline; concurrent pages are not frozen exports. Directory aggregates, recommendations and current-ancestor verification remain unimplemented. Docker checks, race tests and four-target builds passed. The native macOS scanner smoke now verifies offline reports in both human and JSON output and passed. Tests cover ties/cursors, partial/unconfirmed parent records, disabled/unavailable roots, cancellation, invalid usage, unusual path bytes and human/JSON parity. [CI run 35017147393](https://github.com/bjornarhagen/saga-rydd/actions/runs/35017147393) passed for `e7e8a97`: native Linux (55s), native macOS (1m33s), and Docker checks/four-target builds/native offline report smoke. The installed Mac binary was updated and both report formats verified against the saved playground inventory; no new scan was run. P1-08b1 is complete; parent report/MVP gates remain open. No schema or dependency change.

### Saved directory measurements

- Previous session: implemented P1-08b2 selected-directory measurement via `rydd report --directory ABSOLUTE_PATH`, with human/JSON parity. One saved read snapshot examines at most 10,000 entries and validates up to 256 saved ancestors. Results distinguish recorded-complete, partial, stale and unknown; null sizes distinguish unknown from empty. Logical file bytes are per path; known hardlinked allocations are counted once, while conflicts/missing identities are qualified. Overflow is rejected. No filesystem scan, schema change or dependency added. Docker checks/race/four-target builds passed; native macOS fixture/report smoke passed with the expected content and allocated totals. Additional tests cover a fully recorded empty folder, read-only snapshots, excluded directories, unavailable roots and conflicting inode observations. [CI run 35019688145](https://github.com/bjornarhagen/saga-rydd/actions/runs/35019688145) passed for `437cf61`: native Linux (50s), native macOS (1m13s), and Docker checks/four-target builds/native reports (3m14s). P1-08b2 is complete; the wider reporting/MVP gates remain open. The installed Mac binary was updated and both formats verified against saved playground observations; no rescan, state migration or worker start occurred. This is bounded on-demand measurement, not persisted aggregation or global directory ranking; measurements above the cap remain explicitly partial.


### First candidate report

-  P2-01a/P2-02a implementation: `report --candidates` derives review-required old `node_modules` candidates from saved directory/manifest metadata. It includes local IDs, rule version, timestamps, regeneration cautions and bounded directory measurements. Filename-only project recognition is explicit; no manifest/content scan or action support. Pages examine 1,000 inventory entries and measure at most 20 subtrees with a five-second deadline; nested dependencies are suppressed. Docker check/race and four-target builds passed. Native macOS synthetic candidate smoke passed, including offline human/JSON evidence and expected sizes. [CI run 35022807347](https://github.com/bjornarhagen/saga-rydd/actions/runs/35022807347) passed for `52e9a7e`: native Linux (1m), native macOS (1m20s), Docker checks/four-target builds and native smoke (3m8s). The installed Mac test binary was updated and both candidate output formats verified against saved playground inventory; no personal-folder scan or worker start occurred. P2-01a/P2-02a are complete for the limited filename-recognition slice documented in PLAN; broader finding persistence, validated regeneration and phase gates remain open. No schema or dependency change.


### Empty candidate emphasis

- 2026-09-16: empty candidate pages gained a boxed uppercase headline, bold yellow on real terminals, with plain output for redirection, `NO_COLOR` and `TERM=dumb`; JSON was unchanged. Existing go-isatty was promoted to a direct dependency without adding a module/version. Docker checks/four-target builds and native macOS terminal/plain/JSON fixture checks passed. The test binary was installed without changing saved state or restarting the worker; CI results for that cosmetic follow-up were not recorded.

### Saved membership reporting

- 2026-10-02: P2-02b3 excludes historical descendants from directory totals after successful completed ancestor reconciliation, and below non-directory replacements. The same rule covers ordinary and compact contributions, including inode evidence. Human/JSON reports disclose excluded rows; selecting an absent directory returns unknown/null sizes. Incomplete/failed listings retain qualified stale evidence. No schema/dependency change or saved-evidence deletion.
- Regression tests cover compact disappearance, file/symlink replacement, nested descendants, similarly named siblings, cross-directory hardlinks, offline reopen, incomplete/failed listings, and unchanged 10,000-entry bounds including excluded rows. The existing native manual-scan smoke now exercises disappearance, ordinary/compact boundary replacement, restoration, and human/JSON diagnostics on disposable fixtures; it is already included in native macOS/Linux CI.
- `./scripts/dev fmt`, `./scripts/dev check`, `./scripts/dev race`, `./scripts/dev build-all`, `./scripts/manual-scan-smoke ./dist/rydd-darwin-arm64`, `./scripts/dev shell -c './scripts/manual-scan-smoke ./dist/rydd'`, `sh -n scripts/manual-scan-smoke` and `git diff --check` passed. Linux container results are not native Linux evidence. Native Linux CI and full compact process-kill/large-scale growth gates remain pending. No installed binary, personal inventory or running service was changed.

### Scoped allocated reductions

- 2026-10-02: P2-02b4 adds schema-6 root revisions, scoped scalar caches and resumable membership/identity scratch. Each reduction step processes at most 128 entries, inode contributions or scratch retirements; published totals deduplicate ordinary/compact identities across the manual root or an outermost node_modules tree. Every scan commit invalidates that root's caches. Manual scans finish maintenance before starting another pass; detailed-mode conversion drains/removes caches. Reports disclose cached versus bounded evidence and retain existing entry caps and freshness/unknown semantics. No dependency added.
- Docker `./scripts/dev check`, `./scripts/dev race` and `./scripts/dev build-all` passed. Tests cover 10,001 identities, shared identities across ordinary files and multiple trees, high fanout, conflicts, unknowns, overflow rollback, cursor/cancellation rollback, reopen/resume, invalidation during work, scratch retirement, mode pinning, coverage caps and schema-5 migration rollback/readability. Schema-4 report compatibility remains tested.
- `./scripts/manual-scan-smoke ./dist/rydd-darwin-arm64` and the same script inside Linux Docker passed, including a real 10,001-file fixture above the fallback identity cap and cached candidate/report evidence. Docker cross-built the state test binary; allocation/compact tests passed natively on macOS. Native validation exposed a crash-helper wait that could deadlock; it now waits on a timer and explicitly asserts SIGKILL. Corrected crash tests passed in Docker, under race detection, and natively on macOS. Shell syntax and diff checks passed. No host tooling or installed app/state was changed; native Linux CI and larger state/WAL/backlog measurements remain pending.

### Bounded subtree retirement

- 2026-10-02: P2-02b5 adds schema-7 completed-parent reconciliation cursors, durable purge phases and scan-only revisions. Manual compact scans retire absent/replaced subtree observations, compact identities and obsolete scope caches after the scan queue drains. Discovery reads at most 128 saved children; purge batches delete at most 128 payload rows from one fixed rebuildable table. New scan revisions cancel obsolete proofs, current replacement entries survive, and retirement invalidates allocated caches as coverage changes. Mode switches, new passes and reductions wait for maintenance. No dependency added and no user-file deletion implemented.
- `./scripts/dev check`, `./scripts/dev race`, `./scripts/dev build-all`, `sh -n scripts/manual-scan-smoke` and `git diff --check` passed. Fixtures cover bounded removal, reopen/resume, rollback/cancellation, delayed retries/incomplete evidence, reappearance between batches, file/symlink replacement, sibling-prefix protection, actual SIGKILL recovery and schema-6 migration rollback/readability. The allocation invalidation fixture explicitly waits for partial scratch after the new maintenance stages; its focused race check passed.
- Docker cross-built the state test binary; `./dist/state-darwin-arm64.test -test.run='^TestSubtreeRetirement' -test.v` passed natively on macOS arm64. Extended manual-scan smoke passed both natively and inside Linux Docker, including moving a real 10,001-file subtree outside the fixture root, retiring its saved observations, and confirming the fixture originals still exist.
- Native synthetic storage fixture: 10,001 identities retired in 107 maintenance transactions; checkpointed database remained 884,736 bytes (864 KiB), peak observed WAL was 4,165,352 bytes, and 179 pages became reusable. This is one bounded fixture, not a large-scale scanner resource benchmark or physical-space-reclamation guarantee. Native Linux CI, larger fixtures, repeated-growth/backlog measurements and owner feedback remain open.

### Production compact scale fixture

- 2026-10-02: P2-02b6a adds `experiments/compactscale`, a real CLI subprocess runner with owned disposable native files, independently calculated expected sizes, observed-phase SIGKILL/resume assertions, half-tree shrink/restore, two whole-tree disappearance/reappearance cycles and a separate detailed production baseline. No production behavior/schema/dependency changes. Native Linux CI now includes this 100,000-identity fixture; formatting checks cover all experiment packages.
- Docker `./scripts/dev check` passed. A 1,000-file native macOS harness run passed, as did the same fixture under `go run -race` in Linux Docker. The full native macOS arm64 100,000-file run passed every exact-total, cached-report, drained-backlog and three SIGKILL recovery assertion. [Method and results](experiments/compactscale/README.md) include sanitized raw measurements.
- Native macOS: compact state grew from 10.84 MiB after initial recovery to 13.11 MiB after rescan, then stayed at that size through shrink/restore and two disappear/reappear cycles; detailed baseline was 31.89 MiB. Maximum child RSS was 29.36 MiB; sampled WAL peaked at 5.02 MiB; completed reports took 9–13 ms. Full compact passes took 14.3–16.9 s, versus 5.9 s for detailed inventory. Disappearance freed 3,318 reusable pages and drained all work. This sparse metadata-heavy fixture demonstrates storage savings with extra maintenance latency; sampled peaks, a single fixture size and two cycles do not establish general resource bounds or long-term stability. Native Linux results are recorded below.

- [CI run 37067698111](https://github.com/bjornarhagen/saga-rydd/actions/runs/37067698111) passed for `6034ffd`: native Linux checks/race/bind-mount/CLI smoke (5m13s), native macOS checks/race/CLI smoke (6m2s), Docker checks/four-target builds/smoke (3m9s), and the new native Linux scale fixture (3m26s). Linux 100,000-identity results: compact database plateaued at 11.70 MiB after rescan versus 31.45 MiB detailed; maximum child RSS 29.63 MiB, sampled WAL peak 4.35 MiB, reports 5–6 ms, full compact passes 25.1–27.1 s versus 11.5 s detailed. Both disappearance cycles drained all work and exposed 2,958 reusable pages. Every exact-total and observed-phase SIGKILL assertion passed. Full CI also closes the pending native Linux checks for P2-02b3/b4/b5.

### Million-identity production validation

- 2026-10-04: P2-02b6b extends the same fixture to one million files across 100 packages. Commit `f44a94c` adds a manual `scale_files` CI choice; routine push/PR jobs remain at 100,000. The million-file job allows 60 minutes overall but preserves the existing ten-minute child timeout. No production code, dependency, schema or default changed.
- Docker cross-built current native macOS binaries; `./scripts/dev check` passed. The native macOS run with `-files 1000000 -cycles 2` passed every exact-size, cache, drained-work and observed-phase SIGKILL assertion. Compact state plateaued at 126.75 MiB after rescan versus 314.78 MiB detailed, with 32,412 pages reusable after each full disappearance. Maximum child RSS was 32.83 MiB versus 29.36 MiB at 100,000 files; reports took 8–15 ms. Full compact passes took 185–211 s versus 114 s detailed. Sampled compact WAL peaked at 7.34 MiB; detailed WAL reached 14.53 MiB. Measured child processes totaled 20.43 minutes, excluding fixture creation/report overhead.
- [CI run 37211415803](https://github.com/bjornarhagen/saga-rydd/actions/runs/37211415803) passed for `f44a94c`, including native macOS/Linux checks, race tests, smoke tests, Linux bind-mount checks, Docker/four-target builds and the manually selected million-file native Linux fixture. Linux compact state plateaued at 113.90 MiB versus 311.52 MiB detailed; each disappearance freed 29,121 reusable pages. Maximum child RSS was 29.83 MiB versus 29.63 MiB at 100,000 files; reports took 5–7 ms. Full compact passes took 246–268 s versus 122 s detailed. Sampled compact WAL peaked at 6.20 MiB. Measured child processes totaled 26.11 minutes. All exact-size/recovery/drained-work assertions passed.
- [Method, comparison tables and sanitized results](experiments/compactscale/README.md) retain both scales. Database size grew roughly with identities, while process RSS stayed modest and cached report latency stayed small for this fixture shape. macOS timing grew more than 10× for some stages; different-day load/cache state and size-dependent observer overhead prevent attributing that to a specific production bottleneck. No fix or timeout increase was needed. These two fixture sizes do not establish long-term, background, wide/deep or real-project behavior.

## Handoff

**Last updated:** 2026-10-04.

**Completed this session:** P2-05b adds `report --candidates --min-age-days N` for an explicitly requested lower-threshold trial. The default remains 90 days; overrides are report-only, range-checked (1–36500), disclosed in human/JSON output and preserved in continuation commands. Cursors reject changed thresholds while preserving old default-age cursors. No schema, dependency, scan-setting or cleanup-policy change. The selected private inventory was evaluated at a lower threshold and again at the default; all project-derived evidence stays in ignored owner-only files.

**Validation:** `./scripts/dev check` and `./scripts/dev build-all` passed. Regressions cover default/override selection, both timestamps, unknown/future dates, invalid bounds, unchanged review-only classification, pagination mismatch and CLI errors/output. An initial terminal assertion needed whitespace normalization for existing line wrapping; the corrected check passed. Native macOS reports exhausted saved candidate pages at both thresholds, retained partial/unknown size qualifications and left database/WAL hashes unchanged during a repeated report. Focused race checks for state/CLI candidate reports passed. Native Linux validation of this change remains pending CI.

**Limits / blockers:** no technical scan/report blocker. Owner review of age evidence and reporting usefulness is pending. A completed scan does not remove the existing report and cached-scope coverage caps; never present a capped measurement as a full-folder total. Saved sizes are not current-content guarantees or reclaimable-space estimates; age does not prove inactivity or regeneration safety. Parent compact rollout, general resource controls and unattended/service readiness remain open.

**Next action:** review the private lower-threshold candidates with the owner to assess usefulness and actual project activity. Preserve the 90-day default unless a separate decision changes it. For complete measurements of larger scopes, plan bounded resumable report coverage instead of raising synchronous limits. Keep project evidence private. No rescan, broader discovery, cleanup, default/background rollout or service work is needed by default. MVP-TRIAL and P2-02b6 remain open for feedback and broader rollout evidence.

**Remaining decisions:** first automation-eligible cache category; supported minimum OS/libc versions; tuned resource/scan-root defaults; license. Go and naming are settled.

**Do not infer:** the database benchmark is not an hourly scanner resource test; dispatch reservations do not enforce CPU/I/O consumption or battery budgets. WAL backpressure is not a strict database-size cap. Ordinary `daemon` stays idle; scanning requires `--experimental-scan` each start. The root fingerprint is a scoped identity hint, not cleanup authorization. Root completion only covers direct children, and summary counts retain old observations. Huge-directory fairness, stale descendant lifecycle, cloud-provider hydration avoidance, physical remount/rebind, wide/deep million-entry variants and sleep tests remain open. No cleanup or service installation exists. Future action/restore data must never be removed by inventory rebuilds. Stop the worker before `state init`; migrations preserve inventory and configuration.
