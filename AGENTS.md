# Working on Saga — Rydd

## Start here

1. Read `README.md`, `PROGRESS.md`, then the relevant sections of `PLAN.md`.
2. Inspect the working tree before editing. Preserve other contributors' changes.
3. Follow the current state, execution-priority section and latest handoff in `PROGRESS.md`, not numeric phase order. The read-only MVP and bounded resource/service development are delivered. The next milestone needs the cleanup scope choice, then owner-selected native acceptance trials. Do not repeat completed work or treat continuation approval as selection of a private source, service/context or source operation.
4. Pick a bounded unchecked task from that priority; record it as in progress before substantial implementation. Supporting work may be pulled forward only for a demonstrated MVP correctness/safety dependency, recorded in the handoff.

## Development

- Go is the chosen language; SQLite and TOML are the planned persistence/configuration stack.
- Use `./scripts/dev check` and the other Docker commands documented in `CONTRIBUTING.md`. Do not install Go, linters, databases or other host tooling just to work on this repository.
- The product must run natively on macOS and Linux. Container tests and cross-compilation are not evidence that native platform behavior works.
- Do not mount host home directories or the Docker socket into development containers by default. Use disposable fixtures for scanner and deletion tests.
- Keep dependencies minimal. Explain new dependencies and verify their platform/build impact. Select the SQLite driver through the planned portability/resource experiment.
- Prefer small coherent changes and run checks appropriate to them. Do not claim that a scaffold's successful test command validates features that do not exist.

## Development housekeeping

- Follow the [housekeeping and retention procedure](CONTRIBUTING.md#housekeeping-and-retention). This applies to developer artifacts, not product cleanup or personal source folders.
- Use one private, task-owned output directory and record the exact generated paths. Keep disposable binaries/fixtures separate from retained evidence and application state.
- At task completion, reap owned processes, retain complete outcomes and reconstruction information, then automatically remove that task's unused generated binaries, synthetic fixtures/state and temporary copies. Do not wait for a separate housekeeping request for these task-owned artifacts.
- Keep the current walkthrough and artifacts needed by active or planned validation. Remove superseded copies only after their unique edits/helpers are committed or reconstructable from verified retained material; names, age and hashes alone are insufficient.
- Preserve owner-selected application/trial state, original/retained notice material, complete failed/partial evidence and unexplained publication staging. Complete records may be compressed or deduplicated; do not truncate them to make a summary.
- Treat shared caches, old unclassified folders, worktrees and Docker storage as separate scoped maintenance. Keep downloaded modules for offline work. Never use blanket deletion of `.local`, `dist`, temporary folders or Docker volumes, or a broad Docker prune.
- Record removals, retained evidence and any deferred large artifacts in the handoff. Recheck exact paths, ownership/type and unexpected changes before removal; leave uncertain files for review.

## Product invariants

- No autonomous deletion without an explicitly approved, narrowly scoped policy. Findings alone are not authorization.
- Revalidate file identities, scope, policy and freshness at action time. Samples and timestamps do not prove safe deletion.
- Preserve bounded memory, resource budgets, durable progress and recovery behavior.
- Never substitute broad shell deletion/pruning for exact reviewed targets. Keep Docker actions scoped to an explicitly selected local context.
- Protect original data and restore records. Quarantine is not reclaimed disk space.
- Use filesystem/platform APIs and argument arrays; avoid parsing shell output or interpolating paths into shell commands.
- Do not implement later-phase destructive behavior as a shortcut during the read-only foundation.

## Progress and handoff

- `PROGRESS.md` is the canonical task/status ledger; `PLAN.md` is the design and acceptance reference. Update both when scope or decisions change.
- Update progress in the same change as implementation: check off only implemented and verified work, record the commands/results, and list remaining limitations.
- Use stable task IDs in commits/PRs where useful. Keep the next-action section specific enough for a fresh agent to begin without conversation history.
- Never mark a phase complete solely because code exists. Its acceptance gates, including native testing and soak periods, must have evidence.
- End each work session with a current status, validation summary, blockers and concrete next action in the handoff section. Do not include credentials, private paths or machine inventories in public files.
