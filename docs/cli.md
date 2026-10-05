# CLI contract for humans, scripts and AI

Human-readable output is the default. Add `--json` to any finite command for one JSON object on stdout, on both success and failure. Normal JSON responses leave stderr empty; failure to write the response is reported on stderr. `daemon` is a long-running foreground text command; observe it with `status --json` from another process.

```sh
rydd capabilities --json
rydd status --json
rydd pause --json
rydd resume --json
rydd stop --json
rydd --data-dir /absolute/fixture/state status --json
```

Put global `--data-dir` before the command. `--json` can go before or after the command. Commands do not prompt. `capabilities --json` works without initialization and lists supported commands, effects, arguments, features and error codes. Unimplemented duplicates and cleanup are explicitly false.

Every JSON response has `api_version: 1`, `ok` and `command`. Success fields depend on the command; status preserves its existing fields and adds `dispatch_budget`. Failures have `error.code` and a human-readable `error.message`. Match codes, not message text. Consumers must tolerate additional fields; incompatible contract changes require a new API version.

| Exit status | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Operation failed (including unavailable worker, configuration/state errors) |
| 2 | Invalid arguments or unsupported output mode |

Control responses contain `acknowledged` and a worker snapshot. A stop acknowledgement means shutdown was requested; it does not guarantee process exit. Pause is durable and requests cancellation of the current cooperative batch; an active filesystem call can still be draining. Status reads saved inventory even without a worker. Paths may contain private information; treat reports as local data.

## Human output

Scan, measure, reports and previews use a shared presentation guide inspired by simplified technical English. This is a clarity preference, not formal ASD-STE100 compliance:

- State the result first, explain its limits, then show the next available step. Use familiar words and consistent terms. Translate internal outcome codes into sentences; retain those codes in JSON.
- Use `File size`, `Allocated on disk` and `Measurement` consistently. `Complete in saved scan` describes saved coverage only. Show incomplete, outdated and unknown evidence explicitly; unknown size is never displayed as zero. These sizes are not estimates of space that cleanup would free.
- Put selected findings before selection diagnostics. Keep paths and reference IDs on separate lines. Explain preview limitations and the review needed from the owner; keep executor implementation requirements in the JSON evidence and design documentation.
- Use aligned fields at normal widths and stacked fields below 60 columns. Wrap prose to the terminal width, capped at 100 columns for readability; use 78 columns when redirected or the width is unavailable. Unicode prose uses conservative width estimates, not a full grapheme layout engine. Individual long words, quoted paths, IDs and commands are not split or shortened and can exceed the width.
- Use ASCII emphasis rules and text labels that work without color. Color is used only on a terminal and respects `NO_COLOR` (including an empty value) and `TERM=dumb`. Redirected output contains no color escape sequences. Quote paths and errors to display embedded control characters safely.
- Suggested commands single-quote ordinary paths. Paths with control characters or invalid UTF-8 use Bash/Zsh ANSI-C byte escapes, preserving even trailing newlines without sending control characters to the terminal. Those exceptional commands require Bash or Zsh.

Human wording and layout are not a scripting contract. Use `--json` for stable field names and full structured evidence. The presentation guide does not change JSON, exit codes, selection rules or action availability. Status/help formatting and a separate human detail mode are not part of this pass.

## Dispatch limits

`scan.max_scan_chunks_per_day` defaults to 288. The worker reserves each inventory chunk before starting it; reservations and the next permitted dispatch time survive restart. Cancellation or a crash does not refund a reservation. UTC midnight replenishes the count; moving the clock backwards does not refill it. The count uses a single database row. Existing configurations inherit the default.

Worker `wait_reason` reports `paused`, `running`, `idle`, `cadence`, `daily_chunk_limit`, `clock_rollback`, `wal_backpressure` or `stopping` where applicable. `dispatch_budget` is a separate saved-budget view, so its reason need not equal the worker's reason (for example, a paused worker can also have exhausted its daily quota). An empty budget reason means the budget currently permits dispatch; job retries may still delay work.

Above 32 MiB of WAL, a passive checkpoint gates new inventory chunks. If a reader pins pending pages, the worker defers scanning and retries after a minute while remaining controllable. A fully checkpointed but physically large WAL can be reused. This is backpressure, not a strict database-size cap; control/startup writes and the active chunk can still add data.

Batch reservations do not meter metadata calls, bytes or CPU. Fine-grained resource limits, battery/sleep integration and production resource measurements remain unfinished. Scanning remains experimental and opt-in on each worker start.

## Live scanner accounting

When experimental inventory is enabled, live worker snapshots include `inventory_metrics`. Human `status` prints the same counters. `capabilities --json` advertises `metadata_api_counters: true`; `metadata_rate_limit` remains false.

| Counter | Attempted scanner API calls |
| --- | --- |
| `stat_calls` | Stat, Fstat and Fstatat, including scope checks and revalidation |
| `directory_open_calls` | Open and Openat for directory descriptors |
| `directory_read_calls` | Readdirnames batches, including EOF reads |
| `filesystem_stat_calls` | Fstatfs filesystem checks |
| `mount_identity_calls` | Linux Statx mount-ID checks; zero on macOS |
| `path_resolution_calls` | EvalSymlinks calls for configured/protected paths |

Counters include startup validation, retries and failed attempts. They are fixed-size in-memory counters for one worker instance, reset on restart, and absent when no inventory scanner is running. Use the worker `instance` field when computing deltas. Each field is sampled independently; a live snapshot is not a transaction across all counters. Reading metrics does not acquire the scanner lock.

These count API attempts, not kernel syscalls or physical disk operations. In particular, EvalSymlinks can perform multiple internal metadata calls, and Readdirnames buffers directory reads. Descriptor closes, SQLite work and runtime activity are outside these counters. There is no per-path history, CPU/byte measurement or persisted metadata-consumption quota. Child-entry pacing is described below; the other API calls are not yet rate limited. Full metadata limiting must cover resolution internals and database work and preserve progress at low rates and short work deadlines.

## Child-entry pacing

The worker now uses `scan.metadata_per_second` (default 100) to space child-entry inspections. The first inspection can start immediately; subsequent starts are at least `1/rate` apart within the worker instance. Idle time does not accumulate burst credits. Failed child stat attempts also consume an inspection slot. This is an entry-inspection limit, not a limit on every metadata operation: directory traversal, mount checks, path resolution, directory-read buffering and SQLite work remain outside it. Capability discovery therefore reports `entry_rate_limit: true` and `metadata_rate_limit: false`.

`inventory_metrics` adds `entry_inspections`, `entry_rate_per_second`, `throttled` and `throttle_wait_ns`. Counts and completed wait durations reset per worker instance. The entry clock is also in memory; the separately persisted dispatch cadence and daily batch cap still apply across restarts. A continuously running worker does not perform a catch-up burst after sleep.

Throttle waits honor pause/stop cancellation. Each timed scan pass reserves half its remaining window for final path revalidation and returns a partial batch when it cannot afford another paced inspection. At most 128 unread names are retained in the single directory stream. Partial batches keep their enumeration generation and preserve pending names; a process restart or actual cancellation still restarts the directory safely with idempotent upserts. Empty partial batches are possible and still consume a dispatch reservation. A throttle yield does not mark a directory complete or record an error.

This prevents pacing alone from discarding every unfinished batch at low rates. It does not guarantee progress if kernel calls or path revalidation repeatedly exceed the work deadline, nor across continual cancellation/restarts. Resource and huge-directory acceptance gates remain open; use disposable fixtures while scanning is experimental.

## Saved file reports

`rydd report [--limit N] [--cursor TOKEN] [--json]` ranks observed regular files by logical size descending, then saved entry ID descending. The default page size is 20, maximum 200. JSON uses the standard envelope with a `report` object; capabilities advertises `file_reports: true`, with `directory_size_reports: true` and `findings: true`.

The command reads an existing database without loading configuration, contacting the worker, scanning the filesystem or changing state. Enabled roots and exclusions reflect saved inventory state; apply configuration changes through the normal state/worker workflow. Files can have disappeared or changed since observation. `source` is `saved_inventory` and `current_state_verified` is false. A report is not a deletion recommendation.

Each file includes `logical_bytes`, `allocated_bytes`, `observed_at`, `modified_at`, `skip_reason` and `parent_pass`. Parent pass labels are `unknown`, `directory_error`, `unconfirmed` (different saved generation), `partial`, or `observed_in_completed_parent_pass`. The last label describes only the direct parent's saved pass; it does not verify ancestors or current disk state. Historical/unconfirmed entries remain visible with labels instead of silently implying they are current. No reclaimable-space total is computed.

`path` is a display string. `path_bytes` is base64-encoded original absolute Unix path bytes and is authoritative when names are not valid UTF-8. Human output quotes paths and errors so embedded control characters do not execute in the terminal.

`roots` contains up to 100 enabled roots, with pending/running job counts, directory error counts, last root error and last root-directory pass time. `roots_truncated` indicates omitted root diagnostics. Root pass times cover direct children, not complete subtrees; empty queues do not prove coverage, and saved errors do not establish current availability. Observation timestamps describe freshness without inventing whole-tree completion percentages.

Pages use a cursor based on size and entry ID rather than an increasing offset. Each invocation uses one short SQLite read snapshot, capped at five seconds, returning at most one extra file to determine whether a next page exists. Index-backed file pagination bounds returned data, while root diagnostic counts still depend on inventory size. Concurrent scanning can change ordering between pages; this is not a frozen export and changes can cause omissions/repeats across invocations. Missing/empty inventories, exhausted pages and invalid cursors produce explicit empty reports or standard errors. A returned `next_cursor` should be passed unchanged with the same state directory. Selected-directory measurement is described below; the first review-candidate category is described below.

## Saved directory-size reports

Human output groups size, scan coverage, freshness and concise caveats into aligned sections. Zero-valued file-identity diagnostics are omitted from that view; JSON retains all fields and full notes. Missing scan coverage produces an actionable scan command, including custom state selection when needed, without starting a scan.

```sh
rydd report --directory /absolute/path/to/folder
rydd report --directory /absolute/path/to/folder --json
```

Directory mode replaces file pagination and cannot be combined with `--limit` or `--cursor`. It uses enabled roots saved in the database, with lexical path matching and no filesystem resolution or scanning. The selected path must lie within a saved enabled root. Missing/non-directory observations or missing ancestor observations return `unknown` with null sizes, rather than a misleading zero-byte result.

A successful completed parent listing establishes which historical entries are absent from that saved pass. Directory measurements exclude those entries and their descendants, including compact logical totals and inode evidence. Descendants below a saved non-directory replacement are also excluded. `excluded_entries` counts examined historical rows omitted from sizes/file counts; it does not count the compact files represented by those rows. Excluded rows still consume the entry budget, and saved evidence is retained. A selected directory absent from a successful completed ancestor listing returns `unknown` with null sizes. Incomplete or failed listings cannot establish absence; their unconfirmed contributions remain explicitly stale. These rules apply to directory measurements, including candidate sizes; largest-file pagination retains its existing historical-observation contract.

Reports themselves never retire records. Completed manual compact scans can subsequently remove obsolete saved subtree rows and caches through bounded maintenance. Those rows then disappear from saved reports and stop consuming coverage budget. This changes only Rydd's rebuildable database evidence, never user files or action/restore records.

JSON places the measurement in `report.directory`; file pagination fields are unused in this mode. The report uses a completed revision-matched scope summary when available. Otherwise it measures the selected directory plus at most 9,999 descendant observations in one read snapshot. Both paths retain the existing five-second command deadline. Ancestor validation is capped at 256 directory records. `truncated` means additional saved entries were omitted, and there is no cursor continuation for that synchronous fallback. For manual compact roots and outermost dependency trees, `measure -d ROOT` can prepare full scope summaries from saved inventory. The implementation bounds result processing and memory; SQLite can examine more index rows while locating the subtree. This is a selected-subtree measurement, not a ranking of all directories.

`logical_file_bytes` sums regular-file path sizes. `unique_inode_allocated_file_bytes` counts each known `(device,inode)` once in the measured portion, using the maximum observed allocation if records disagree; disagreement marks the result stale. Missing identities are counted per path and disclosed through `unknown_inodes`. Directory metadata, symlinks and other objects are excluded. Shared clones, snapshots and hardlinks outside the folder can retain space, so neither number is a reclaimable-space estimate. Do not add overlapping folder measurements.

Status describes saved evidence, never current disk state:

- `unknown`: the selected directory or an ancestor lacks a usable saved directory observation; sizes are null.
- `stale`: a saved root error, unconfirmed ancestry/entries, directory observation newer than its listing, or conflicting inode sizes make the aggregate uncertain.
- `partial`: listing gaps/errors, skipped entries, a bound being reached or incomplete ancestry prevent recorded completeness.
- `recorded_complete`: all examined saved directory listings and links meet the checks, with no truncation or known gaps. This does not establish an atomic whole-tree snapshot or current completeness.

Stale status takes precedence over partial; individual counters and notes still disclose missing/error/truncated portions. Oldest/newest observation timestamps expose age without imposing an invented inactivity threshold. Partial or stale totals can overestimate or underestimate actual current size. A fully recorded empty directory reports zero regular-file bytes; an unobserved directory reports null. No report authorizes deletion.

## Future actions

Keep discovery, review and execution as explicit commands. Machine readability is not cleanup authorization: future actions must use exact plans, revalidation and explicit user approval or an already approved narrow policy. Destructive commands and idempotency keys will be designed when action execution is implemented.

## Saved cleanup candidates

`rydd report --candidates [--min-age-days N] [--cursor TOKEN] [--json]` reports the first experimental category: potentially old `node_modules`. It cannot be combined with `--limit`; `-d` / `--directory` optionally selects an exact manual-scan root. JSON uses `report.candidates`; the standard API envelope and errors are unchanged. `capabilities` advertises `findings: true`; cleanup, duplicates and service installation remain false.

Rule `node_modules_old_metadata` version 1 requires a saved regular-file sibling `package.json`, both mtimes at least 90 days old by default, and a common completed parent listing. Recognition is explicitly `manifest_filename_only`. Contents, lockfiles, source activity and local dependency modifications are unverified. Old metadata is not proof of inactivity or safe deletion. Every finding is `review_required`, with an empty `available_actions` list.

`--min-age-days N` overrides the age filter for this report only (1–36500 days). It requires `--candidates` and does not change saved inventory or configure cleanup policy. The effective value appears in human output and JSON `minimum_age_days`. For example, `report --candidates --min-age-days 30` reviews observations at least 30 days old. Unknown and future timestamps remain ineligible.

Findings contain a local ID, rule/version, entry/root/device/inode identity, authoritative base64 path bytes, manifest path, observation/modification times and the full directory measurement contract. IDs are stable across unchanged inventory reports but not authorization and not guaranteed across inventory rebuilds. Findings derive from saved inventory; there is no persisted approval, dismissal or independent finding table yet.

Pages examine at most 1,000 inventory entries and measure at most 20 candidates, using completed scope summaries or the bounded directory fallback. Follow `next_cursor` even on an empty page; keep `--candidates` and the same `--min-age-days` on subsequent requests. Cursors reject a changed age threshold; human continuation commands preserve the override. Each directory measurement reads its own snapshot, so concurrent updates can change evidence between selection and measurement. The whole command has a five-second deadline; timeout returns an error. Nested dependency directories are suppressed, and no aggregate savings are presented. Sizes remain qualified by stale/partial/unknown status and shared-storage caveats. Empty results never mean the machine is clean.

### Candidate selection diagnostics

`report.candidates.selection_diagnostics` is an ordered array of `{code, count, explanation}`. All codes are present, including zero counts. Counts cover only examined entries on this page, sum to `entries_examined`, and assign each entry its first matching outcome in this order:

1. `not_node_modules`: different basename.
2. `nested_dependency`: nested dependency path suppressed.
3. `not_directory`: dependency path recorded as a non-directory.
4. `skipped`: dependency or manifest observation has a skip reason.
5. `manifest_missing_or_unsupported`: sibling manifest absent or not a regular file.
6. `parent_incomplete_or_error`: parent listing absent, incomplete or errored.
7. `parent_unconfirmed`: directory or manifest generation differs from the saved parent generation.
8. `timestamp_unknown`: directory or manifest mtime is nonpositive.
9. `age_not_met`: either mtime is newer than the cutoff, including future timestamps.
10. `selected`: selected review candidate; count equals this page's finding count.

Later conditions may also fail; these are first-match explanations, not an exhaustive list of issues. Human output shows nonzero counts with the same codes and explanations. Evidence failures precede age checks to avoid presenting uncertain observations as simply recent. Eligibility and rule version are unchanged.

`page_coverage` is `more_saved_entries` when `next_cursor` is present, otherwise `saved_entries_exhausted`. It describes the remainder of this saved page sequence, not filesystem scan completion. Disabled roots and entries beyond the cursor/page bounds are not counted. An empty page can still require continuation; an exhausted page does not prove that the computer is clean.

## Foreground manual scans

`scan -d PATH [-s MS | --now] [--json]` scans one selected folder without initialization or service installation. Long aliases are `--directory` and `--sleep`. Default spacing is 10 ms between child-entry inspection starts; accepted spacing is 0–60000 ms. `--now` means zero spacing. Combining `--now` with a sleep flag, or both aliases of an option, is invalid usage. Relative and home-relative paths normalize to absolute lexical paths; aliases/symlinks are not canonicalized into the same inventory key.

Each selected path gets a private store under `STATE_DIR/manual/SHA256(normalized-path)`. Background configuration and its inventory are not changed; existing config exclusions are inherited when available. Other manual stores and application state are protected from scanning. Concurrent writers to the same store are refused. Separate stores can scan concurrently, so their I/O adds up. This foreground mode does not use the daemon's cadence/daily dispatch cap, power controls, or control socket. Use Ctrl+C/SIGTERM, not `rydd stop`, to stop it. Filesystem protections, metadata-only scanning, bounded batches, leases, durability and WAL backpressure remain in effect.

Human output reports committed batch progress. JSON emits one final versioned envelope with a `scan` object: directory/authoritative path bytes, state directory, sleep milliseconds, mode (`resume` or `new_pass`), batch count, inventory summary and outcome. `queue_drained` means no queued work remains; skipped or historical observations can still make sizes incomplete. `pending_retry` means saved jobs remain (for example an unavailable directory); `wal_backpressure` means writes paused to avoid growing a pinned WAL. These are successful command results with explicit outcomes, not promises of a complete current inventory. Cancellation and operation errors use the normal failure envelope/exit code; previously committed batches remain saved.

Rerunning recovers interrupted jobs and finishes the existing queue before seeding any root revisit. Running jobs and delayed retries also prevent a fresh pass; retry backoff is preserved. Once the queue is empty, the next invocation starts a new pass. Interrupted directory listings restart from the beginning, retaining their priority ahead of their queued children; this is not an exact per-entry resume or a frozen point-in-time export. Changes in already completed directories are discovered on a later pass, not by trusting recent timestamps as proof of unchanged contents. Historical missing entries remain until stale-entry lifecycle work is implemented. It stops rather than waiting for delayed retries. No automatic rescan or background process continues after command exit.

`report -d PATH` aliases `--directory` and chooses that exact manual store when present, falling back to configured saved inventory for directory-size reports only. `report -d PATH --candidates [--min-age-days N] [--cursor TOKEN]` requires that exact manual store and lists its candidates. Folder size cannot be combined with file pagination; candidates can paginate. Reports never initiate scans. Manual results take precedence even if older than configured inventory; no inventories are merged. To address a manual store directly, use its returned `state_dir` as global `--data-dir` with normal report commands. Candidate evidence/size limits still apply, including the 10,000-entry directory measurement fallback when no completed scope summary is available.

### Opt-in compact manual inventory

`scan -d PATH --compact` enables saved compact persistence for regular files inside `node_modules`, including a selected root itself named `node_modules`. Nested dependency trees share that treatment. Directory and non-regular metadata remain available for scope/coverage diagnostics; compact mode does not retain individual regular-file paths. Scanning still uses the same metadata pacing and filesystem protections. No file contents are read or deleted.

`--detailed` selects ordinary per-file persistence. Omit both flags to retain the saved mode (initially detailed); combining the flags is invalid. A mode change is refused until inventory, retirement and allocated-reduction work finish. JSON adds `scan.compact`, `scan.retirement_batches`, `scan.subtree_retirement_batches` and `scan.allocation_batches`. These count maintenance transactions, including discovery/phase transitions as well as payload changes. The manual command drains obsolete generations, disappeared/replaced subtrees and allocated reductions in that order through bounded, cancellable transactions; restart finishes maintenance before starting a new pass. Subtree retirement and reductions wait until all inventory jobs for that root finish, including delayed retries. WAL backpressure and the existing inter-maintenance delay still apply. The background scanner currently refuses compact stores because it does not dispatch this maintenance.

Directory JSON adds `compacted_directories`, `compacted_files`, `compact_inode_entries_examined` and `allocated_size_source`. Logical bytes include cached per-directory totals, allowing many files to be represented within the existing 10,000-entry report limit. `allocated_size_source` is `cached_reduction`, `bounded_identity_check` or `unknown`. A completed cache covers the exact manual-root or outermost `node_modules` scope, combining ordinary and compact identities; it is never assembled by adding independently deduplicated subtree totals. Its revision must match the report snapshot, its compact identities must be known for allocated size to be available. Schema-8 summaries also cover full logical size and entry diagnostics above the synchronous entry cap. Cached reports check zero compact inode rows and retain conflict/stale/partial qualifications from saved evidence.

Without a matching cache, a separate 10,000-row compact identity budget bounds allocated-size checks. Missing identities or exhaustion makes `unique_inode_allocated_file_bytes` null and status partial unless stale evidence takes precedence. Logical totals remain available, with coverage qualifications; repeated-inode diagnostics then cover only checked evidence. Normal and compact inode evidence are combined, so links across storage boundaries are not counted twice. Arbitrary subfolders retain this fallback rather than borrowing another scope's cache.

Each reduction step processes at most 128 inventory rows, inode contributions or scratch-cleanup rows. Progress, identity deduplication and totals commit together. Every committed scan batch (including errors and detailed passes) invalidates all that root's scopes; incomplete results are never published as cached totals. The cache may be used while its scratch cleanup finishes. The temporary identity map can grow with scope size on disk; it is cleaned in resumable batches, not kept in memory. Cache selection and SQLite index work are not a promise that each transaction performs only 128 database operations. These are saved observations, not current filesystem verification or reclaimable-space estimates.

Generation changes replace one directory's partial totals; stale inode generations are excluded immediately. Existing ordinary file rows in that directory are also excluded immediately, before bounded retirement. Completed ancestor membership excludes disappeared subtrees from directory measurements and scoped reductions as described above. Each successful completed compact-mode directory listing queues reconciliation of its saved children in batches of at most 128. Absent children and old directory payload below a non-directory replacement become durable retirement scopes. Each purge transaction removes at most 128 payload rows from one fixed rebuildable table; current replacement entries are preserved, and an absent anchor entry is removed after its payload and descendants. Retirement checks a separate scan revision before each batch, cancelling obsolete work after any new scan commit. Deletions advance the allocated-cache revision because removing excluded rows can change report coverage too. Reductions wait for this retirement to finish.

Incomplete or failed listings do not authorize retirement. Delayed scan retries can postpone maintenance; historical evidence remains available until a later successful reconciliation. General background/detailed-inventory stale cleanup, huge-directory continuation, arbitrary-scope caches and large-scale resource validation remain unfinished. Freed database pages are reusable; retiring rows does not promise the SQLite file will shrink.

Schema 8 introduced full saved scope coverage. Writers now migrate additively to schema 9; reports still read schemas 4–8 without migration and use bounded identity checks when caches are unavailable. Stop a worker before explicitly migrating its state; manual inventories are separate stores. Older binaries cannot read schema-9 stores. Existing allocation caches are invalidated once for coverage recalculation. No application database is deleted or rebuilt by this migration, and upgrading alone does not scan, populate caches or schedule subtree retirement.

### Human candidate output

Candidate reports lead with the number of candidates on the current page, followed by candidate entries and then aligned selection counts. Nonempty pages use compact candidate entries with path, measured sizes/coverage, modification dates and reference ID. Shared qualifications appear once; complete rule, recognition, observation, diagnostic and measurement evidence remains in the unchanged `--json` report.

The next-page command preserves the selected manual directory and any custom global state location, with literal shell quoting. Empty pages can still have more results; the human summary makes that continuation explicit.

Empty human candidate pages display an uppercase result with an ASCII emphasis rule. Real terminals use bold yellow unless `NO_COLOR` is set or `TERM=dumb`; redirected output and JSON contain no color escapes.

### Complete saved size calculations

`rydd measure -d PATH [--batches N] [--json]` resumes calculations for an exact existing manual compact inventory. `--directory` aliases `-d`. It never scans the source folder, creates scan jobs or changes the configured roots. It holds the state writer lock and may migrate the selected store; reports remain read-only. An unfinished scan or inventory retirement must finish with `scan -d PATH` first.

The default invocation processes up to 128 calculation batches; `--batches` accepts 1–1000. Each transaction handles at most 128 entry, identity or scratch-retirement records, with a five-second invocation budget. Progress survives command boundaries and crashes. Repeat the same command until complete. WAL backpressure can pause progress; close readers retaining old snapshots before retrying. A canceled transaction rolls back while previous commits remain saved.

JSON `measure` includes `directory`, `state_dir`, `batches`, `complete` and `outcome` (`complete`, `batch_limit`, `time_limit` or `wal_backpressure`). Exit 0 means a successful bounded invocation; use `complete` to determine whether more work remains. Cancellation and failures use the standard error envelope.

Directory and candidate measurements add `coverage_source`: `cached_reduction` uses a completed revision-matched scope summary, while `bounded_entry_check` uses the existing synchronous fallback. For cached coverage, `entries_examined` counts all entries covered by the durable calculation and `entry_limit` is 0 because the report does not walk the subtree. `truncated` is false, but exclusions, errors, unknown identities or stale evidence can still qualify size and status. Ancestor checks remain bounded and can make a summary partial or unknown. Caches cover the manual root and outermost `node_modules` trees; arbitrary subfolders still use the bounded fallback. Source metadata and cleanup permissions are unchanged.

## Read-only cleanup plan previews

`rydd plan --preview [-d PATH] [--min-age-days N] FINDING_ID... [--json]` previews 1–20 explicitly selected, unique candidate references. Flags precede positional IDs (`--json` is accepted globally as usual). Use exactly one of `--preview`, `--save`, `--show` or `--check`. No approval or execution form is implemented. `-d` / `--directory` selects an exact manual scan root; otherwise use the selected global `--data-dir` inventory. No default selection, wildcard or recursive expansion exists.

IDs must have canonical `node-modules-v1:ROOT_ID:ENTRY_ID` syntax with positive decimal IDs. The command queries only those entries and applies the current saved candidate rule, including the selected age threshold (default 90 days). Missing, disabled, skipped or no-longer-eligible selections fail the whole request with `invalid_arguments`; duplicate/malformed IDs and more than 20 selections do too. Run a new candidate report to review changed evidence. ID reuse across rebuilt or different inventories is possible; a preview never establishes durable action identity.

The five-second report deadline and per-target measurement rules still apply. Partial, stale, truncated and unknown measurements are preserved. Selection and measurements use separate saved snapshots; they do not establish current filesystem state or an atomic action manifest.

JSON `plan` has `mode: "preview"`, `executable: false`, `approval_available: false`, `project_activity: "unconfirmed"`, `proposed_future_action: "same_filesystem_quarantine"`, `quarantine_reclaims_space: false` and `estimated_reclaimable_bytes: null`. `evidence` contains the full findings, effective age filter, timestamps, identity/path bytes and measurements. Its `page_coverage` is `selected_entries_only`; `entry_limit` equals the explicit selection count and no continuation is returned. `requirements_before_execution` and `notes` explain activity/regeneration review, immutable approvals, action-time identity/scope checks, safe quarantine, durable journaling and collision-safe restoration. Finding `available_actions` stays empty; capabilities expose `plan_previews: true` and `cleanup: false`.

The preview saves no plan and changes no inventory or action records. It performs no migration or source-folder traversal and generates no plan ID. SQLite may maintain its normal reader sidecars. Redirected output can contain private paths and inventory; keep it private. Saved unapproved selections are available separately as described below. Approval, quarantine, restore and purge remain future work. Quarantine itself does not reclaim storage, and a later purge needs separate explicit approval.


## Saved cleanup selections

```sh
rydd plan --save [-d PATH] [--min-age-days N] FINDING_ID... [--json]
rydd plan --show PLAN_ID [--json]
```

`--save` rechecks 1–20 unique explicit finding IDs using the same eligibility rule as previews. Invalid, duplicate, disabled, missing or ineligible selections fail the whole save. Flags precede IDs. A five-second context bounds inventory capture and persistence. `--show` accepts only the full returned plan ID: directory, age and finding arguments are rejected. It uses the selected global data directory, not a manual inventory directory.

The saved JSON `plan` object contains `id` and `record`. Record version 1 includes creation time, `status: "unapproved"`, proposed future same-filesystem quarantine, `project_activity: "unconfirmed"`, `executable: false`, `approval_available: false`, `quarantine_reclaims_space: false`, a null reclaimable-size estimate and `selection`. Selection includes the inventory incarnation ID, saved root paths/fingerprints/revisions, target and manifest device/inode/change-time/generation bindings, and the full qualified finding report. `path_bytes` remains authoritative for arbitrary Unix filenames. Eligibility, measurement and bindings all come from one SQLite read snapshot; capture does not acquire the scanner's writer lock or traverse source folders. Historical identity fields can be unknown; none prove safe deletion or replace action-time validation.

Schema 9 adds a durable random inventory identity. Reader commands do not migrate existing stores. Saving from schemas 4–8 fails with migration guidance; reports and previews remain available. A migration does not itself scan source folders. Configured state uses `state init` with its worker stopped. Manual stores migrate on an explicit scan, or on `measure` for an existing compact store. A freshly created inventory has a different identity even when numeric finding IDs repeat.

Records are stored separately at `plans/plans.sqlite3` under the global state directory. Directories are private (0700); database and sidecar files must be private (0600), owned regular files with one link. The plan store uses a separate writer lock, its own application/schema identity, WAL and FULL-synchronous transactions. `--show` opens it read-only and does not initialize missing storage; SQLite may maintain normal reader sidecars. Inventory replacement or source-folder unavailability does not remove saved plans. Preserve the plans directory when rebuilding inventory.

Each record is bounded to 1 MiB. Its ID is `plan-v1-` followed by the lowercase SHA-256 digest of the exact stored JSON bytes. Loading verifies that digest, record version and unapproved/non-executable state. The application exposes no plan update or deletion; database triggers also reject updates/deletes. The digest is content binding for a future approval, not an approval token or protection against deliberate same-user modification. Unsupported databases, damaged records and changed digests fail closed. Per-record and per-request work are bounded; total saved history has no retention limit yet.

Capabilities advertise `saved_plans: true`, `plan_approval: false` and `cleanup: false`; `plan` can now read evidence or write saved-plan storage depending on its mode. Existing preview JSON is unchanged. No source contents, cleanup action or approval are written. List/expiry/revocation, an interactive review flow, explicit approval, live revalidation, action journaling, quarantine and restoration remain later work. An interrupted save can have committed before its result was delivered; no automatic retry or execution follows.

## Check a saved selection

`rydd plan --check PLAN_ID [-d PATH] [--json]` compares the saved selection with one current inventory snapshot. Use the same global data directory and original manual scan directory as the save, or omit `-d` for configured inventory. The plan retains its exact targets and age threshold; new finding IDs, age overrides, duplicate directory aliases and mixed modes are rejected. Neither database is migrated or initialized. A missing store or plan is `not_found`; unsupported inventory schemas return migration guidance. Readers do not take the inventory writer lock.

The JSON `plan` contains `id` and `check`. `check.status` has these meanings:

| Status | Meaning |
| --- | --- |
| `matches_saved_inventory` | Recorded identity and evidence agree, with complete recorded measurements. Live files remain unchecked. |
| `changed` | The inventory incarnation, exact selection, root binding/revision, target/manifest identity or recorded evidence differs. Review a new selection. |
| `unverifiable` | No definite change was found, but identity or measurement evidence is incomplete, stale or unknown. This is not a match. |

Exit 0 and `ok: true` mean the comparison completed, including `changed` and `unverifiable`. Consumers must inspect `check.status`; no result grants approval. Errors and cancellation use the standard envelope. `check` also contains `source: "saved_inventory"`, `selected_targets`, an `issues` array, and `current_state_verified`, `approval_available` and `executable`, all false. Each issue has `code`, `message` and an optional `finding_id`. Codes are `different_inventory`, `selection_unavailable`, `root_evidence_changed`, `root_evidence_unknown`, `target_identity_changed`, `finding_evidence_changed` and `target_evidence_unknown`. A definite change takes precedence over unknown evidence; the entire selection is checked, with no partial successful approval.

Identity is checked before resolving numeric IDs in a different inventory. Within the original inventory, comparisons include authoritative path bytes, root fingerprints/revisions, device/inode/change-time/generation bindings, eligibility, observation/modification times and qualified measurement fields. Display strings, measurement generation time and explanatory notes are not identity evidence. A subsequent scan can invalidate the comparison through its root revision even if it found no changes to the selected folder. Unknown identities are not treated as usable equal evidence; incomplete measurements require renewed review. The existing 1–20 target, 1 MiB record, measurement and five-second limits apply.

This command performs no source-folder traversal. It can match while the source is offline, or while unobserved changes exist. It does not check current configuration/exclusions, live identity, project activity, regeneration inputs or quarantine/recovery readiness. Plan and inventory records are unchanged; SQLite can maintain normal reader sidecars. Approval and action-time verification remain separate future steps. Capabilities advertise `saved_plan_checks: true`; `plan_approval` and `cleanup` remain false.
