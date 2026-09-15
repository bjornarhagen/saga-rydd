# Working on Saga — Rydd

## Start here

1. Read `README.md`, `PROGRESS.md`, then the relevant sections of `PLAN.md`.
2. Inspect the working tree before editing. Preserve other contributors' changes.
3. Follow the execution-priority section and handoff in `PROGRESS.md`, not numeric phase order. The approved next milestone is saved reports plus one `node_modules` recommendation category, followed by a controlled read-only trial. Do not resume resource hardening or service work by default.
4. Pick a bounded unchecked task from that priority; record it as in progress before substantial implementation. Supporting work may be pulled forward only for a demonstrated MVP correctness/safety dependency, recorded in the handoff.

## Development

- Go is the chosen language; SQLite and TOML are the planned persistence/configuration stack.
- Use `./scripts/dev check` and the other Docker commands documented in `CONTRIBUTING.md`. Do not install Go, linters, databases or other host tooling just to work on this repository.
- The product must run natively on macOS and Linux. Container tests and cross-compilation are not evidence that native platform behavior works.
- Do not mount host home directories or the Docker socket into development containers by default. Use disposable fixtures for scanner and deletion tests.
- Keep dependencies minimal. Explain new dependencies and verify their platform/build impact. Select the SQLite driver through the planned portability/resource experiment.
- Prefer small coherent changes and run checks appropriate to them. Do not claim that a scaffold's successful test command validates features that do not exist.

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
