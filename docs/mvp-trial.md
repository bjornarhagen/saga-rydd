# Controlled read-only MVP trial

Status: first controlled project scan completed; see [general lessons](mvp-trial-results.md). Owner feedback and selection-diagnostic review remain open.
Task: MVP-TRIAL in [PROGRESS.md](../PROGRESS.md).

## Scope and setup

Use one explicitly selected development project on the native host, ideally with an older `node_modules` directory. Record the selected root privately. Do not infer permission to scan its parent directory or the home directory. The existing installed playground configuration stays separate.

Use the installed native binary or the matching Docker-built `dist/rydd-*` binary. Do not mount the selected project into a development container. No additional host tooling is required for the CLI trial.

## Current foreground/manual workflow

Use this workflow to evaluate guided review and saved same-size reports. Create a fresh private trial directory under this checkout's ignored `.local/` directory. Use a new absolute state path within it; never reuse or overwrite an earlier trial. Keep inventory, raw reports and process logs there. No `init` or configured worker is needed for a manual scan. In the examples below, `rydd` means the selected native binary, and the uppercase paths are placeholders for the privately recorded trial state and exact owner-selected root.

### Run a bounded scan session

1. Set a separate elapsed-time deadline of at most five minutes. Keep the foreground process handle and a private log so the session can be stopped if supervision fails.
2. Start the exact selected scan with explicit compact mode and conservative child-entry pacing:

   ```text
   rydd --data-dir ABSOLUTE_TRIAL_STATE scan -d ABSOLUTE_SELECTED_PROJECT --compact -s 40
   ```

3. Stop sooner if the scan finishes or an error needs inspection. Otherwise send Ctrl+C at the deadline. Request interruption or termination if the supervising session fails; wait for the actual process to exit. Cancellation is cooperative, so a blocked filesystem call can delay exit. Never leave the scan unattended.
4. Retain the saved result, including an interrupted or partial result. The 40 ms delay spaces child-entry inspections; it does not bound every metadata operation, CPU use or I/O. Do not remove pacing or expand the root to obtain more findings.
5. If another bounded session is needed within the authorized trial, rerun the same command with the same root and state. Pending work resumes. Once all pending scan jobs, size calculations and maintenance have finished, another invocation starts a fresh pass; it is not a report-only operation.
6. Keep scanning stopped before comparing saved reports. Worker `pause`, `resume` and `stop` do not control this foreground command. Unscoped `status` describes configured inventory, not the separate manual inventory; use the manual scan result and scoped directory report for this trial's coverage.

Compact mode is opt-in and persists for the selected manual inventory. It still traverses dependency metadata to measure size. Neither scanning nor these reports reads source file contents.

### Read and compare saved reports

Use the same global state and exact manual root for every scoped command:

```text
rydd --data-dir ABSOLUTE_TRIAL_STATE report -d ABSOLUTE_SELECTED_PROJECT
rydd --data-dir ABSOLUTE_TRIAL_STATE report -d ABSOLUTE_SELECTED_PROJECT --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --candidates -d ABSOLUTE_SELECTED_PROJECT --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --same-size -d ABSOLUTE_SELECTED_PROJECT --json
```

Compare the human and JSON candidate and same-size pages by repeating the same options without `--json`. Inspect at most ten pages per report mode for the first review. Follow `next_cursor` even when a page is empty. Preserve `--data-dir`, the manual root, report mode, threshold and page limit when requesting another page. If a cursor remains after that budget, record unfinished discovery; do not claim the whole inventory was inspected. A changed threshold starts a separate first-page report, not a continuation.

Candidate age defaults to 90 days. Any trial age override must be explicit, shown in the report and retained through pagination and guided review. Each candidate page examines at most 1,000 saved entries and shows at most 20 candidates. Completed cached measurements can cover larger scopes; uncached directory measurements retain the 10,000-entry fallback bound. Compare candidate rejection counts with `entries_examined` to explain empty pages rather than inferring no clutter.

Same-size reports default to a 1 MiB minimum and 20 raw saved regular-file rows per page, plus one lookahead. `--min-size-bytes` and `--limit` change only that report. A page can contain fewer displayed files than eligible rows because complete singleton size bands are omitted. Boundary singletons and empty filtered pages can retain continuation. Counts describe this page only, not a complete duplicate group. Dependency trees named `node_modules` are excluded. Other generated categories can still appear.

Check that unknown size stays null in JSON and is shown as unknown in human output. Preserve partial, stale, truncated and incomplete-coverage labels. Same-size `content_verified` and `current_state_verified` must remain false, with `estimated_reclaimable_bytes` null. Equal sizes, saved aliases and allocated bytes do not establish equal contents, independent storage or space that can be freed. Do not sum overlapping scopes or page-local object counts.

### Exercise guided navigation without choosing targets

```text
rydd --data-dir ABSOLUTE_TRIAL_STATE review -d ABSOLUTE_SELECTED_PROJECT
```

Use `next` when a later page exists, `refresh` to return to the first page, then `quit`. Keep the same explicit age override used for candidate reports, if any. Check that row numbers reset, empty pages explain continuation and quitting creates no saved selection or consent. Verify the inventory is unchanged after report/navigation commands; retain any comparison evidence privately.

Do not enter candidate numbers or `save` without the owner's exact choices. A later owner-selected save preserves an unapproved historical selection only; it cannot approve cleanup. Unselected rows do not create a durable keep or dismissal policy. Record whether the next available step, selection counts and size qualifications are understandable without manually copying finding IDs.

## Optional configured-worker control workflow

The earlier trial below exercises configured scanning and worker controls. Use it separately when those controls are part of the authorized trial. It does not create the exact manual inventory required by `review -d` or scoped manual candidate/same-size reports.

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

### Run and stop the configured worker

1. Start `rydd --data-dir ABSOLUTE_TRIAL_STATE daemon --experimental-scan` as a supervised native process. Keep its PID/process handle and log in the private trial directory.
2. Set a separate five-minute elapsed-time deadline. Check `status --json` about every ten seconds, not in a tight loop. Stop sooner when the queue has drained, the dispatch cap is reached, or an error needs inspection.
3. Check that `pause` acknowledges, the active chunk drains, and `resume` allows work to continue. Record observations rather than assuming acknowledgment means a blocked filesystem call has returned.
4. Send `stop` at completion or the deadline and wait for the actual process to exit. Use a cleanup handler to request stop/termination if the supervising session fails. Never leave the experimental worker running after the trial.
5. Confirm `status --json` reports `worker.state = not-running`. Retain the saved inventory for offline reports. If the inventory is partial, inspect that result before deciding whether another explicitly bounded session is worthwhile.

A small scan budget may not reach a large dependency tree. That is a valid coverage observation, not evidence that no candidates exist. Do not increase the scope or remove pacing to manufacture a complete result.

### Inspect configured inventory results

Use the same explicit `--data-dir` for every command:

```text
rydd --data-dir ABSOLUTE_TRIAL_STATE status --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --limit 10
rydd --data-dir ABSOLUTE_TRIAL_STATE report --limit 10 --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --directory ABSOLUTE_SELECTED_PROJECT --json
rydd --data-dir ABSOLUTE_TRIAL_STATE report --candidates
rydd --data-dir ABSOLUTE_TRIAL_STATE report --candidates --json
```

Follow candidate `next_cursor` values, including on empty pages, up to ten pages for the first inspection. Keep the worker stopped so pages have stable input. If a cursor remains after that inspection budget, record that candidate discovery was not exhausted. Each page examines at most 1,000 entries; uncached candidate measurements retain the 10,000-entry fallback bound. No whole-inventory result should be inferred from a single page.

For any candidate, inspect its individual directory measurement in both output formats. Do not sum overlapping scopes or interpret allocated bytes as guaranteed recoverable space. Unknown, partial and stale are substantive results.

## Questions the trial should resolve

- Does the first report expose useful information without several manual pagination steps? Record report elapsed time, page counts and whether candidates were reachable within the inspection budget.
- Are sizes and incomplete coverage understandable? Record null/partial/stale/truncated results and whether the explanation matches the saved coverage.
- Does the candidate correspond to a real dependency folder? Does the owner still use the project or modify dependencies locally? Owner feedback is distinct from metadata evidence.
- Do old manifest/directory mtimes select active projects, or miss inactive projects? A 90-day mtime filter does not establish inactivity. Fresh edits inside a dependency folder may coexist with old top-level timestamps.
- Do human and JSON results convey the same evidence and unsupported actions? Is the explanation concise enough to use?
- Did foreground interruption/resumption work, or did pause, resume and stop work in the optional worker workflow? Record observed durations and errors. This short trial does not validate unattended resource limits or a multi-week soak.

Do not open project contents or run package managers as part of a metadata-only trial. Do not delete, quarantine, regenerate dependencies, rewrite timestamps or change the project to make a finding appear.

## Sanitized handoff

Publish only manually reviewed general product lessons, validation methods and prioritized follow-up. Keep all project-derived measurements, including counts, timing and coverage observations, private alongside personal paths, filenames, inventories, manifest contents and raw JSON/logs. Never mark the real trial complete using synthetic fixtures alone. Keep MVP-TRIAL unchecked until real observations and user feedback support the acceptance criteria.

If the trial produces no candidates, explain whether the age filter, missing project evidence, pagination or incomplete inventory accounts for that result. If the evidence cannot distinguish them, record a diagnostic gap rather than claiming the machine is clean.
