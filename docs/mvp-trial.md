# Controlled read-only MVP trial

Status: procedure prepared; real-folder trial awaits a user-selected project.
Task: MVP-TRIAL in [PROGRESS.md](../PROGRESS.md).

## Scope and setup

Use one explicitly selected development project on the native host, ideally with an older `node_modules` directory. Record the selected root privately. Do not infer permission to scan its parent directory or the home directory. The existing installed playground configuration stays separate.

Use the installed native binary or the matching Docker-built `dist/rydd-*` binary. Do not mount the selected project into a development container. No additional host tooling is required for the CLI trial.

Create a fresh private trial directory under this checkout's ignored `.local/` directory. Run `rydd --data-dir ABSOLUTE_TRIAL_STATE init --root ABSOLUTE_SELECTED_PROJECT`. Never reuse or overwrite an existing state directory. Keep raw reports, logs, configuration and inventory private; they can expose filenames and project details.

Before starting the worker, edit only the newly generated trial configuration:

```toml
# Within the existing [scan] section; retain other generated settings.
work_seconds = 5
interval_seconds = 10
metadata_per_second = 25
max_scan_chunks_per_day = 24
```

Validate with `rydd --data-dir ABSOLUTE_TRIAL_STATE config check`. These values limit dispatch and child-entry inspection rate, not all CPU/I/O or metadata work. The daily chunk cap uses UTC dates; it is not the trial's wall-clock timer.

## Run and stop

1. Start `rydd --data-dir ABSOLUTE_TRIAL_STATE daemon --experimental-scan` as a supervised native process. Keep its PID/process handle and log in the private trial directory.
2. Set a separate five-minute elapsed-time deadline. Check `status --json` about every ten seconds, not in a tight loop. Stop sooner when the queue has drained, the dispatch cap is reached, or an error needs inspection.
3. Check that `pause` acknowledges, the active chunk drains, and `resume` allows work to continue. Record observations rather than assuming acknowledgment means a blocked filesystem call has returned.
4. Send `stop` at completion or the deadline and wait for the actual process to exit. Use a cleanup handler to request stop/termination if the supervising session fails. Never leave the experimental worker running after the trial.
5. Confirm `status --json` reports `worker.state = not-running`. Retain the saved inventory for offline reports. If the inventory is partial, inspect that result before deciding whether another explicitly bounded session is worthwhile.

A small scan budget may not reach a large dependency tree. That is a valid coverage observation, not evidence that no candidates exist. Do not increase the scope or remove pacing to manufacture a complete result.

## Inspect saved results

Use the same explicit `--data-dir` for every command:

```text
rydd --data-dir ABSOLUTE_TRIAL_STATE status --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --limit 10
rydd --data-dir ABSOLUTE_TRIAL_STATE report --limit 10 --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --directory ABSOLUTE_SELECTED_PROJECT --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --candidates
rydd --data-dir ABSOLUTE_TRIAL_STATE report --candidates --json
```

Follow candidate `next_cursor` values, including on empty pages, up to ten pages for the first inspection. Keep the worker stopped so pages have stable input. If a cursor remains after that inspection budget, record that candidate discovery was not exhausted. Each page examines at most 1,000 entries; each candidate measurement examines at most 10,000 entries. No whole-inventory result should be inferred from a single page.

For any candidate, inspect its individual directory measurement in both output formats. Do not sum overlapping scopes or interpret allocated bytes as guaranteed recoverable space. Unknown, partial and stale are substantive results.

## Questions the trial should resolve

- Does the first report expose useful information without several manual pagination steps? Record report elapsed time, page counts and whether candidates were reachable within the inspection budget.
- Are sizes and incomplete coverage understandable? Record null/partial/stale/truncated results and whether the explanation matches the saved coverage.
- Does the candidate correspond to a real dependency folder? Does the owner still use the project or modify dependencies locally? Owner feedback is distinct from metadata evidence.
- Do old manifest/directory mtimes select active projects, or miss inactive projects? A 90-day mtime filter does not establish inactivity. Fresh edits inside a dependency folder may coexist with old top-level timestamps.
- Do human and JSON results convey the same evidence and unsupported actions? Is the explanation concise enough to use?
- Did pause, resume and stop work? Record observed durations and errors. This short trial does not validate unattended resource limits or a multi-week soak.

Do not open project contents or run package managers as part of a metadata-only trial. Do not delete, quarantine, regenerate dependencies, rewrite timestamps or change the project to make a finding appear.

## Sanitized handoff

Publish only a manually reviewed summary: platform, approximate coverage, report latency, completeness labels, candidate count/false-positive feedback, control behavior and prioritized follow-up. Omit private paths, filenames, manifest contents and raw JSON/logs. Never mark the real trial complete using synthetic fixtures alone. Keep MVP-TRIAL unchecked until real observations and user feedback support the acceptance criteria.

If the trial produces no candidates, explain whether the age filter, missing project evidence, pagination or incomplete inventory accounts for that result. If the evidence cannot distinguish them, record a diagnostic gap rather than claiming the machine is clean.
