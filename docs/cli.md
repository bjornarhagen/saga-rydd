# CLI contract for humans, scripts and AI

Human-readable output is the default. Add `--json` to finite noninteractive commands for one JSON object on stdout, on both success and failure. Normal JSON responses leave stderr empty; failure to write the response is reported on stderr. `daemon` is a long-running foreground text command; observe it with `status --json` from another process. `review` is a guided text command and rejects JSON before reading input or opening state.

```sh
rydd capabilities --json
rydd status --json
rydd pause --json
rydd resume --json
rydd stop --json
rydd --data-dir /absolute/fixture/state status --json
```

Put global `--data-dir` before the command. `--json` can go before or after the command. Only `review` prompts. `capabilities --json` works without initialization and lists supported commands, effects, arguments, features and error codes. Its `noninteractive: true` describes machine output; `interactive_text_commands` lists `review`, whose command entry has `json: false`. Unimplemented duplicates and cleanup are explicitly false.

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

## Guided manual review

`rydd review -d PATH [--min-age-days N]` opens the existing inventory for that exact normalized manual root. It does not load a replacement inventory, scan, inspect source files, record consent or start cleanup. Missing manual inventory fails with a scan command. Directory aliases cannot be combined; age is 1–36500 days (default 90).

Discovery examines at most 1,000 saved entries and finds at most 20 candidates. Before displaying numbered rows, review captures their exact bindings and measurements in one saved inventory snapshot, then closes the reader. Human input holds no database reader, transaction or writer lock. Each database operation has a five-second deadline; input waiting does not consume that deadline.

| Input | Result |
| --- | --- |
| `1,3` or `1 3` | Select unique row numbers from this page; repeat the exact chosen paths and measurements |
| `next` | Replace the page with later saved entries; no selection carries forward |
| `refresh` | Replace the page from the start of saved entries |
| `save` after selection | Compare the frozen subset with current saved inventory, then save it as unapproved evidence |
| `back` after selection | Discard the tentative subset and return to the same frozen page |
| `quit`, EOF or Ctrl+C before publication | End without saving a plan |

Ranges, `all`, duplicate numbers and numbers outside this page are refused. Empty pages may have a continuation. Unselected folders stay unchanged for this review; no durable keep, dismissal or exclusion decision is recorded. A new page resets row numbers.

Empty pages show the existing discovery diagnostics: saved entries examined and mutually exclusive rejection counts. Their report command addresses that same page and preserves manual scope, state location, minimum age and the incoming cursor. A later scan can change its results. Nonempty numbered pages show frozen selection evidence; their selection counts are not discovery coverage.

Changed evidence, including a rebuilt inventory reusing numeric entry IDs, refuses saving. The check does not replace displayed evidence with newer records. Unchanged incomplete evidence can be saved as a qualified historical record and is explicitly labelled unverifiable. The check and publication are separate operations: an intervening change can make the record outdated, but cannot change its exact selected objects. Later checks remain necessary. No result verifies current filesystem contents or permits execution.

Input lines are bounded to 4096 bytes, including the newline. A command needs a completed line: an unterminated `save` at EOF does not confirm publication. Native terminal and pipe input wait with bounded polling and context cancellation, without changing inherited input blocking mode or leaving blocked reader goroutines. Paths are printed from authoritative raw bytes with escaped controls. `--json` returns `unsupported_output` and exit 2 before input or state access. Existing finite commands remain noninteractive.

The completion output includes commands to reopen the saved plan and start another explicit scan and review. Review exits after one saved subset. Scanning never continues automatically.

## Saved same-size file bands

`report --same-size [-d PATH] [--min-size-bytes N] [--limit N] [--cursor TOKEN] [--json]` is a metadata-only prerequisite for duplicate discovery. JSON uses `report.same_size`, with `content_verified: false`, `current_state_verified: false` and `estimated_reclaimable_bytes: null`. Equal sizes are not content matches. `same_size_candidates` is true; `duplicates` and cleanup remain false.

Minimum size is a positive integer number of bytes, default 1048576 (1 MiB). `--limit` is 1–200 raw saved regular-file rows per page, default 20, plus one lookahead. It does not limit whole size bands. `--min-size-bytes` requires `--same-size`; `--candidates` and its age filter cannot be combined with this mode. Providing both mode flags is refused even when one is explicitly false. A scoped report requires the exact existing manual scan; it never falls through to configured inventory.

One read transaction selects bounded raw rows through the existing size index, then filters disabled roots, saved skip reasons, dependency trees named `node_modules`, compact-parent overlaps and invalid saved paths. This also excludes detailed dependency inventory. Other generated categories remain future work. Parent-pass freshness is qualified per member; stale descendants and ancestor freshness are not verified. No source path is opened, configuration loaded, worker contacted or database migrated.

The cursor binds its version, inventory incarnation, minimum bytes, last raw size and entry ID. Continuation first seeks remaining IDs in the current size band, then smaller sizes using the remaining allowance. There is no whole-band count or whole-inventory grouping. Rebuilding the inventory or changing the minimum refuses continuation. Each page reads a new saved snapshot; scans may add, remove or reorder observations between pages.

Bands contain page-local `files`, `known_objects`, `repeated_saved_objects`, `unknown_identities` and `conflicting_identities`. Known aliases use saved device/inode with qualified change/generation evidence; conflicting stamps or allocation values remain uncertain. No count establishes live inode continuity, independent storage or reclaimable space. `continues_before`/`continues_after` describe raw size-band boundaries, which can include excluded rows. Boundary singletons are retained so a split pair remains visible. Interior unique-size files are omitted. Empty filtered pages can still return a cursor, and exhaustion does not prove complete scan coverage or absence of duplicates.

The human view shows each saved file ID. These IDs identify rows on this page; the human display does not freeze a selection. `hash --select` requires the complete standard JSON page and rechecks its exact selected rows. Schema 9 is required for durable inventory identity. Full-file hashing needs the separate explicit read-consent workflow below. Keeper selection and cleanup remain unavailable.

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

Findings contain a local ID, rule/version, entry/root/device/inode identity, authoritative base64 path bytes, manifest path, observation/modification times and the full directory measurement contract. IDs are stable across unchanged inventory reports but not authorization and not guaranteed across inventory rebuilds. Findings derive from saved inventory; findings themselves are not approvals. Review consent is stored separately per saved plan; dismissal and an independent finding table remain unavailable.

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

Rerunning recovers interrupted jobs and finishes the existing queue before seeding any root revisit. Running jobs and delayed retries also prevent a fresh pass; retry backoff is preserved. Compact inventories also resume saved retirement, reconciliation and allocation work after the scan queue drains. Once all saved work finishes, the next invocation starts a new pass. Interrupted directory listings restart from the beginning, retaining their priority ahead of their queued children; this is not an exact per-entry resume or a frozen point-in-time export. Changes in already completed directories are discovered on a later pass, not by trusting recent timestamps as proof of unchanged contents. Manual compact inventories retire proven absent subtrees in bounded batches; broader stale-entry lifecycle work remains open for other scopes and modes. It stops rather than waiting for delayed retries. No automatic rescan or background process continues after command exit.

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

`rydd plan --preview [-d PATH] [--min-age-days N] FINDING_ID... [--json]` previews 1–20 explicitly selected, unique candidate references. Flags precede positional IDs (`--json` is accepted globally as usual). Use exactly one of `--preview`, `--save`, `--show`, `--check`, `--verify`, `--inspect`, `--capture`, `--compare`, `--approve` or `--revoke`. Preview does not record consent or execute cleanup. `-d` / `--directory` selects an exact manual scan root; otherwise use the selected global `--data-dir` inventory. No default selection, wildcard or recursive expansion exists.

IDs must have canonical `node-modules-v1:ROOT_ID:ENTRY_ID` syntax with positive decimal IDs. The command queries only those entries and applies the current saved candidate rule, including the selected age threshold (default 90 days). Missing, disabled, skipped or no-longer-eligible selections fail the whole request with `invalid_arguments`; duplicate/malformed IDs and more than 20 selections do too. Run a new candidate report to review changed evidence. ID reuse across rebuilt or different inventories is possible; a preview never establishes durable action identity.

The five-second report deadline and per-target measurement rules still apply. Partial, stale, truncated and unknown measurements are preserved. Selection and measurements use separate saved snapshots; they do not establish current filesystem state or an atomic action manifest.

JSON `plan` has `mode: "preview"`, `executable: false`, `approval_available: false`, `project_activity: "unconfirmed"`, `proposed_future_action: "same_filesystem_quarantine"`, `quarantine_reclaims_space: false` and `estimated_reclaimable_bytes: null`. `evidence` contains the full findings, effective age filter, timestamps, identity/path bytes and measurements. Its `page_coverage` is `selected_entries_only`; `entry_limit` equals the explicit selection count and no continuation is returned. `requirements_before_execution` and `notes` explain activity/regeneration review, immutable approvals, action-time identity/scope checks, safe quarantine, durable journaling and collision-safe restoration. Finding `available_actions` stays empty; capabilities expose `plan_previews: true` and `cleanup: false`.

The preview saves no plan and changes no inventory or action records. It performs no migration or source-folder traversal and generates no plan ID. SQLite may maintain its normal reader sidecars. Redirected output can contain private paths and inventory; keep it private. Saved unapproved selections are available separately as described below. Executable approval, quarantine, restore and purge remain future work. Quarantine itself does not reclaim storage, and a later purge needs separate explicit approval.


## Saved cleanup selections

```sh
rydd plan --save [-d PATH] [--min-age-days N] FINDING_ID... [--json]
rydd plan --show PLAN_ID [--json]
```

`--save` rechecks 1–20 unique explicit finding IDs using the same eligibility rule as previews. Invalid, duplicate, disabled, missing or ineligible selections fail the whole save. Flags precede IDs. A five-second context bounds inventory capture and persistence. `--show` accepts only the full returned plan ID: directory, age and finding arguments are rejected. It uses the selected global data directory, not a manual inventory directory.

The saved JSON `plan` object contains `id` and `record`, with separate optional `review` and `observation` fields when those records exist. Record version 1 includes creation time, `status: "unapproved"`, proposed future same-filesystem quarantine, `project_activity: "unconfirmed"`, `executable: false`, `approval_available: false`, `quarantine_reclaims_space: false`, a null reclaimable-size estimate and `selection`. Selection includes the inventory incarnation ID, saved root paths/fingerprints/revisions, target and manifest device/inode/change-time/generation bindings, and the full qualified finding report. `path_bytes` remains authoritative for arbitrary Unix filenames. Eligibility, measurement and bindings all come from one SQLite read snapshot; saving the selection does not acquire the scanner's writer lock or traverse source folders. Historical identity fields can be unknown; none prove safe deletion or replace action-time validation.

Schema 9 adds a durable random inventory identity. Reader commands do not migrate existing stores. Saving from schemas 4–8 fails with migration guidance; reports and previews remain available. A migration does not itself scan source folders. Configured state uses `state init` with its worker stopped. Manual stores migrate on an explicit scan, or on `measure` for an existing compact store. A freshly created inventory has a different identity even when numeric finding IDs repeat.

Records are stored separately at `plans/plans.sqlite3` under the global state directory. Directories are private (0700); database and sidecar files must be private (0600), owned regular files with one link. The plan store uses a separate writer lock, its own application/schema identity, WAL and FULL-synchronous transactions. `--show` opens it read-only and does not initialize missing storage; SQLite may maintain normal reader sidecars. Inventory replacement or source-folder unavailability does not remove saved plans. Preserve the plans directory when rebuilding inventory.

Each record is bounded to 1 MiB. Its ID is `plan-v1-` followed by the lowercase SHA-256 digest of the exact stored JSON bytes. Loading verifies that digest, record version and unapproved/non-executable state. The application exposes no plan update or deletion; database triggers also reject updates/deletes. The digest binds review consent to exact content; it is not an approval token or protection against deliberate same-user modification. Unsupported databases, damaged records and changed digests fail closed. Per-record and per-request work are bounded; total saved history has no retention limit yet.

Capabilities advertise `saved_plans: true`, `plan_approval: true` (review consent only) and `cleanup: false`; `plan` can now read evidence or write saved-plan storage depending on its mode. Existing preview JSON is unchanged. Saving writes no source contents, cleanup action or approval. Explicit review consent and revocation are separate operations below. Listing, an interactive review flow, action journaling, quarantine and restoration remain later work. An interrupted save can have committed before its result was delivered; no automatic retry or execution follows.

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

This command performs no source-folder traversal. It can match while the source is offline, or while unobserved changes exist. It does not check current configuration/exclusions, live identity, project activity, regeneration inputs or quarantine/recovery readiness. Plan and inventory records are unchanged; SQLite can maintain normal reader sidecars. Review consent is a separate step; action-time verification remains future work. Capabilities advertise `saved_plan_checks: true` and `plan_approval: true` for review consent only; `cleanup` remains false.


## Record and revoke review consent

```sh
rydd plan --approve PLAN_ID [-d PATH] --confirm-project-review --confirm-quarantine [--json]
rydd plan --revoke PLAN_ID [--json]
rydd plan --show PLAN_ID [--json]
```

`--approve` requires both confirmations to be explicitly true. `--confirm-project-review` records the owner's review of project activity, local dependency edits and reinstall requirements. `--confirm-quarantine` accepts same-filesystem quarantine without purge, acknowledges that quarantine frees no disk space, and records review consent only with a 24-hour validity period. These are owner statements, not Rydd's verification. Plan commands remain noninteractive. Confirmations are rejected with any other mode, even if set false. Approval accepts the original manual scan directory or configured inventory, but no age override or replacement finding selection. Revoke/show require only the exact plan ID in the same global data directory.

Approval loads and verifies the exact content-addressed plan, then requires `matches_saved_inventory` with no issues from a fresh bounded inventory snapshot. This check can match with offline or unobserved live changes; the review-v1 contract cannot authorize an executor. The five-second command deadline applies. Invalid or missing confirmations use `invalid_arguments` (exit 2). Changed/unknown/different inventory evidence returns `review_evidence_changed`; expired, revoked or not-yet-valid consent returns `review_unavailable`; damaged or mismatched review records return `review_invalid` (exit 1). Missing storage or a revoke request without approval is `not_found`. No real source files are read, moved or deleted.

Plan-store schema 2 adds an immutable random store identity and separate immutable approval/revocation tables. Writers migrate schema 1 transactionally; show/check readers still accept schema 1 without migration. This is separate from inventory schema 9. The private plan-store writer lock serializes consent changes. Each decision is published by a FULL-synchronous SQLite commit, with at most one approval and one revocation per plan. Decisions are bounded to 16 KiB, include versions and content digests, and are checked against the exact plan/store/inventory/action bindings. Missing or invalid associations are never treated as new consent. Digests and private file modes do not protect against deliberate same-user modification or a restored database backup.

The JSON `plan` retains `id` and the unchanged `record`. Approve, revoke and show add `review` when consent exists. The original `record.status: "unapproved"`, activity and capability flags describe the frozen capture; they never change and are not the current review state. Use `review.status` for consent:

| Status | Meaning |
| --- | --- |
| `review_approved` | Recorded owner consent is within its 24-hour window. This does not check current eligibility or authorize execution. |
| `revoked` | A durable revocation exists. It takes precedence over clock and expiry state. |
| `expired` | Current time is at or after the approval deadline. |
| `not_yet_valid` | Current time is before creation, for example after a clock change. Consent is unavailable. |

`review.id` uses `approval-v1-` plus a SHA-256 digest. `review.approval` includes version 1, plan/store/inventory IDs, `action_contract: "same_filesystem_quarantine_review_v1"`, creation/expiry times and both owner confirmations. Optional `review.revocation` binds the approval ID, plan ID and store ID with its creation time. `review.executable` and `review.current_state_verified` remain false. No source identity or eligibility check is implied by merely showing active consent. Clock-based display depends on the system wall clock; it is not a tamper-resistant expiry service.

Retrying approval rechecks saved evidence and returns the same still-valid approval without extending its deadline. A revoked or expired approval is never renewed; save a new selection and review again. Revoke needs no inventory, source access or reconfirmation, works for expired/not-yet-valid approvals, and returns the same revocation on retry. Interrupted writes may commit before output reaches the caller; use show or retry to recover the result. Canceling the process is not revocation.

Capabilities expose `review_contract` with its name, `validity_hours: 24`, `scope: "review_consent_only"`, `executable: false` and `renewed_approval_required_for_execution: true`. `plan_approval: true` means these review records are supported. It does not enable cleanup. Future execution requires a new action contract and renewed approval, plus live scope/identity/configuration checks, durable action journals, same-filesystem quarantine and collision-safe restoration. Purge remains separately approved and unimplemented.


## Compare selected live metadata

`rydd plan --verify PLAN_ID [-d PATH] [--json]` first verifies the immutable saved plan and checks its original inventory. It uses the saved age threshold; new finding IDs, age overrides, mixed modes and owner-confirmation flags are rejected. No review consent is required for this read-only operation. The same five-second cooperative deadline covers inventory and filesystem work. Missing/corrupt storage, invalid configuration and cancellation use standard errors.

Ancestor evidence is captured with the saved-inventory check in one read transaction. The inventory incarnation and root revisions must match the frozen plan before these ancestor observations are used. Unknown or changed saved evidence stops the request before selected live paths are opened. Current configuration is loaded afresh: configured roots must remain enabled; manual verification uses its exact `-d` root and current configured exclusions when a config exists. Default protected paths, app state and excluded descendants inside the target remain protected. Invalid configuration stops the request rather than silently ignoring exclusions.

The filesystem pass holds directory descriptors for every absolute path component. It uses no-follow `openat`/`fstatat` checks, rejects dataless directory/manifest flags where available, compares the saved root fingerprint, and checks directory/ancestor and manifest device/inode/change/modification timestamps. A second pass checks held descriptors and their named links, including mount identities. The manifest is inspected as a regular file without opening its contents. Linux additionally uses its named mount ID to reject same-filesystem file bind mounts. The existing narrow local-filesystem allowlist applies. Strict verification rejects all symlink components, including root aliases that scanning can resolve; rescan/save using a canonical path instead.

The JSON `plan` contains `id`, `inventory_check` and, only when the saved check matches, `live`. Extra saved-check issue codes are `ancestor_limit` and `ancestor_evidence_unknown`. `live` contains `source: "live_metadata"`, `checked_at`, `status` (`metadata_matches` or `blocked`), and one result per selected finding. Each target has `finding_id`, `status`, `message` and an optional failure `code`: `scope_invalid`, `scope_excluded`, `evidence_unknown`, `path_limit`, `path_unavailable`, `filesystem_unsupported`, `root_changed`, `identity_changed`, `mount_boundary`, `manifest_changed` or `path_changed_during_check`. A blocked target makes the overall live result blocked. Exit 0 means the comparison completed; consumers must check both inventory and live status. Neither result is approval.

`live.current_state_verified` and `live.executable` stay false: only the named path metadata was checked. This does not recursively inspect dependency contents, validate manifest contents or lockfiles, prove regeneration safety, or ensure later filesystem stability. Editing an existing file below `node_modules` can leave the selected directory metadata unchanged and produce a metadata match. Observed swaps are rejected, but the namespace is not locked; no descriptors or reusable permission are returned to a future executor.

Work is bounded to 20 targets, 4096-byte paths, 256 absolute directory components per target and the existing saved-measurement limits. One target's handles are closed before checking the next; at most 258 descriptors are used for its traversal/recheck, apart from SQLite/runtime descriptors. No directory listings or file contents are read. Cancellation is checked between operations; a blocked kernel call can outlast the deadline. Provider hydration, physical remounts and hostile continuous namespace changes are not established safe by these tests. No plan/consent/inventory records change, though SQLite may maintain reader sidecars. Capabilities advertise `plan_live_checks: true`; cleanup remains false.


## Project input inspection

```sh
rydd plan --inspect PLAN_ID [--tree] [-d PATH] [--json]
```

Inspection explicitly reads project inputs. `--verify` retains its metadata-only contract. Without `--tree`, inspection has no recursive listings. The saved selection, age threshold, inventory snapshot, current configuration, exclusion rules, canonical path requirements and five-second cooperative deadline are the same as live verification. Review consent is independent and never required for this read-only command. Mixed modes, finding IDs, age overrides and confirmation flags are invalid usage. `--tree` is accepted only with `--inspect`, even when set false; `--inspect --tree=false` retains input-only behaviour. Changed or incomplete saved evidence prevents any input read.

After verifying the selected paths, inspection opens sibling `package.json` and `package-lock.json` through the held project directory. Inputs must be regular, supported local files with no symlinks, dataless flags, protected identities or mount boundaries. Files remain open until identities, size/mode/change times, named links and mounts are rechecked. Root/ancestor/target/manifest path checks also remain active. Observed mutations block the result. Paths and contents can change after the request; no permission or descriptors are returned for execution.

The initial supported format is deliberately narrow:

| Input rule | Supported observation |
| --- | --- |
| JSON | UTF-8 objects with no duplicate keys, including nested objects. |
| npm lock | Version 2 or 3 with a `packages` object and its empty-key root entry. |
| Root declarations | Manifest and lock root dependency maps match literally; declared direct entries exist, with an exception for explicitly optional peers. Name/version match when present in the manifest. |
| Locked sources | Public `https://registry.npmjs.org` tarball locations, concrete versions and one canonical SHA-512 integrity value. No URL credentials, query or fragment. |
| Restricted features | Workspace/local/git/URL/alias sources, links, bundles, overrides and lifecycle install hooks are unsupported. Ordinary test/build script declarations can be present. |

This validates an input shape and a declaration comparison. It does not solve npm's dependency graph or semver ranges, authenticate package contents or prove that a lock matches the installed tree. Valid npm projects can be refused by this limited contract, including projects with missing optional lock entries. Unsupported does not mean broken or unsafe.

The following project siblings block inspection by presence: `npm-shrinkwrap.json`, `yarn.lock`, `pnpm-lock.yaml`, `bun.lock`, `bun.lockb`, `.npmrc`, `patches`, `.yarn`, `pnpm-workspace.yaml`, `lerna.json` and `binding.gyp`. Their contents are never read. In particular, `.npmrc` values and credentials are never printed. User/global configuration, ancestor configuration, environment variables, CLI flags, installed Node/npm versions and download availability remain unknown.

The JSON `plan` has `id`, `inventory_check` and, only after matching saved evidence, `inspection`. Inspection has `source: "live_project_inputs"`, `checked_at`, `status` (`inputs_observed` or `blocked`) and one `targets` result per finding. Each target contains `finding_id`, `status`, `message` and optional `code`. A successful target has `inputs` with `lockfile_version`, `locked_packages` (excluding the root entry) and `files`: each file has a static `name`, `bytes` and exact-byte `sha256`. Raw manifest values, dependency names and URLs are not echoed. A blocked target has no partial input evidence and makes the overall status blocked.

Input failure codes are `input_missing`, `input_invalid`, `input_unsupported`, `input_limit` and `input_changed_during_check`, in addition to the live path-check codes above. Exit 0 means the observation completed, including a block; inspect both inventory and inspection status. Invalid arguments/storage/configuration and cancellation retain the standard errors and exit codes.

`current_state_verified`, `executable`, `regeneration_verified` and `dependency_contents_checked` remain false. `local_dependency_edits` remains `"unknown"`. The exact-byte digests describe this request only: inspection itself does not compare against a frozen observation, and even a changed lock can fit the supported shape. No ordinary dependency files or hidden installed lock are opened. Input-only mode has no recursive listing. No package manager or network request runs. Inspection changes no saved records or file contents; normal filesystem access times and SQLite reader sidecars may change.

Bounds are 256 KiB for a manifest, 2 MiB for a lock, a combined 50,000 decoded JSON values and 64 nesting levels, plus the existing 20-target/4096-byte/256-directory limits. One target's handles are closed before the next; at most 260 descriptors are used, apart from SQLite/runtime handles. Reads use 8 KiB chunks and cancellation checks between operations; blocked kernel calls can exceed the cooperative deadline. Provider hydration, physical remounts and continuously hostile namespace changes remain unverified. Capabilities expose `plan_input_inspection: true`; cleanup remains false.


### Installed-tree metadata review

```sh
rydd plan --inspect PLAN_ID --tree [-d PATH] [--json]
```

This opt-in mode adds recursive metadata listings after the same supported project-input checks. It leaves input-only inspection unchanged. Success uses `inspection.source: "live_project_inputs_and_tree_metadata"` and `status: "inputs_and_tree_observed"` for the report and each target. Each successful target adds `tree` with `status: "metadata_observed"`, `entries`, `directories`, `regular_files`, `internal_bin_links` and `metadata_sha256`. Counts exclude the selected root. Blocked targets have neither partial inputs nor tree evidence; a blocked target blocks the report. The four verification/execution flags remain false and local dependency edits remain unknown.

The initial installed-layout contract is narrow:

| Location or object | Observation allowed |
| --- | --- |
| Install boundary | Exact lock-listed package directories, required scope containers and `.bin`. |
| Root hidden lock | A regular `.package-lock.json`, inspected as metadata only; its body is never opened. |
| Package interior | Ordinary single-link regular files and directories. A direct package `node_modules` starts another install boundary; deeper fixture directories with that name stay ordinary content. |
| `.bin` entry | A relative symlink through observed directory components to a regular file with an executable mode bit inside a lock-listed package. |
| Uncertain object | Missing/absolute/outside/chained links, multiple regular-file hardlinks, special file types, protected/excluded objects and mount boundaries block the result. |

Raw symlink components are checked before normalization. A missing directory or regular file followed by `..` cannot be hidden by lexical cleaning. Symlinks are never followed by filesystem operations. Installed packages absent from the lock are refused at boundaries; lock entries absent from disk are allowed because omission or incomplete installation remains unknown. A supported layout does not establish a complete install, authentic package contents, absence of custom data inside packages or successful execution/reinstall.

Each directory stream reads at most 128 names per batch. Directory handles remain open during descent, with no-follow opens, protected identity checks and named mount checks for both files and directories. Directory stamps are checked around listing; every descendant's name, identity, mode, size, link count, change/modification times and link text must agree with a second bounded metadata pass. The enclosing saved root/ancestor/path/manifest checks run after extended tree work. These are observations; concurrent changes after an object's last check remain possible.

The digest uses a domain-separated `rydd-tree-metadata-v1` encoding of sorted relative path bytes and the checked metadata/link fields. It includes the root boundary record and excludes access times and file contents. Matching repeated digests describe matching observations, not pristine package contents or cleanup permission. This inspection mode does not load or compare a saved observation. The JSON returns counts and digests without dumping filenames or link text. Ordinary dependency bodies and the hidden lock remain unopened; no source content or saved records are changed.

Per target limits are 10,000 descendant entries, 64 relative directory levels (also at most 256 total absolute directory components), 4096-byte paths/link text and 4 MiB retained path/link bytes. The existing 20-target/input/JSON limits and shared cooperative five-second deadline apply. Handles are released between targets; at most 260 traversal/input handles are open, apart from SQLite/runtime handles. Limit exhaustion returns a block, never a partial positive observation. Cancellation uses the standard error. The synchronous pass has no durable cursor; large trees need separate review or a future resumable inspection.

Additional block codes are `tree_layout_unknown`, `tree_link_unsupported`, `tree_unsupported`, `tree_limit` and `tree_changed_during_check`, plus input/live path codes. Exit 0 still means the check completed, including a block. Capabilities add `plan_tree_inspection: true`. Cleanup, restoration and automatic policies remain unavailable.


## Capture and compare observations

```sh
rydd plan --capture PLAN_ID [-d PATH] [--json]
rydd plan --show PLAN_ID [--json]
rydd plan --compare OBSERVATION_ID [-d PATH] [--json]
```

Capture explicitly runs the full input/tree check, with the same canonical paths, saved inventory, current scope and resource limits as `--inspect --tree`. It checks saved inventory again after that work. A changed or incomplete selection or blocked target saves no observation. Cancellation before publication rolls back; publication can succeed before an interrupted response is observed, so use show or a matching retry to recover the saved ID. Only all-target success can publish a separate immutable baseline. Review consent is independent. Capture accepts no finding IDs, age override, confirmations or `--tree` modifier. Compare accepts only a full observation ID and an optional original manual root; mixed modes are invalid usage.

Each plan has at most one observation. A matching capture retry returns its original ID and observed time. Changed capture returns `observation_conflict`; use compare to review the difference, or save a new selection before another capture. Baselines are never replaced. `--show` reads the separate observation with the original plan and consent in one snapshot, without source or inventory access. This recovers the exact ID if publication succeeded before a response was lost.

Successful capture JSON `plan` contains `id`, `inventory_check`, `inspection` and `observation`. The latter has `id` and `record`. Record version 1 binds the exact `plan_id`, random `store_id`, `inventory_id`, ordered target IDs and `inspection_contract: "npm_inputs_tree_metadata_v1"`. It stores `observed_at`, the successful inspection status/source, input sizes/digests/summary, tree counts/digest and explicit verification limits. It contains no input bodies or descendant filename/link listings. A saved-evidence refusal has no `inspection` or `observation`; an inspection block has no `observation`.

Observation IDs are `observation-v1-` followed by the lowercase SHA-256 of the exact canonical stored JSON bytes. Payloads are capped at 64 KiB. Plan-store schema 3 adds one table and immutable-row triggers. Writer commands can migrate older plan stores; capture defers migration until it can publish migration and the first observation in the same FULL-synchronous transaction. Existing plan-v1 and review-v1 bytes remain unchanged. Reader commands accept schemas 1–4 without migration, and comparison does not initialize missing storage. Digests bind observations to their records; they do not protect against deliberate same-user tampering. Total saved history still has no retention limit.

Compare validates the observation, bound plan and store identity before inventory/source access. It prepares the exact saved selection, repeats the full inspection and checks saved inventory again. JSON `plan` contains `id`, `observation_id`, `inventory_check` and, when saved evidence permits the check, `comparison`. The comparison includes original `observed_at`, current `checked_at`, source, ordered targets and these statuses:

| Status | Meaning |
| --- | --- |
| `matches_observation` | The observed input bytes, input summary, tree metadata and counts match the captured baseline. |
| `changed` | At least one successful current observation differs. Review the reported change categories. |
| `blocked` | At least one target could not be inspected under the supported contract. There is no complete comparison. |

Blocked takes precedence over changed for the whole selection. Each target reports its own status, safe message and any block code. Successful targets include current input/tree summaries; changed targets also list `changes` from `manifest_bytes_changed`, `lock_bytes_changed`, `input_summary_changed`, `tree_metadata_changed` and `tree_counts_changed`. The output does not dump descendant file/link names or bodies. A manifest edit can fail the original saved metadata checks and become blocked instead of a digest difference; an unscanned valid lock edit can be reported as changed.

All `current_state_verified`, `executable`, `regeneration_verified` and `dependency_contents_checked` fields remain false; `local_dependency_edits` stays `"unknown"`. Matching ignores request times and explanatory wording for evidence equality; a comparison time before the capture, or in the future, is unusable. It does not detect edits that predate capture, check ordinary dependency file bodies, establish a complete/pristine install, prove reinstall safety or authorize cleanup. Capturing records no renewed executable consent. Files can change after either observation; these finite checks are not an atomic snapshot.

Exit 0 means a check completed, including saved-evidence refusal, changed or blocked results. Inspect statuses. Invalid usage is exit 2. Corrupt bindings/records (`observation_invalid`), conflicting capture (`observation_conflict`), unusable capture evidence (`observation_unavailable`), storage/configuration failures and cancellation use exit 1 with the standard machine error. Comparison writes no saved records or source contents; normal access times and SQLite reader sidecars may change. Capabilities add `plan_observation_capture` and `plan_observation_comparison`; cleanup remains false.


## Saved recovery journal

```sh
rydd journal --show INTENT_ID [--json]
```

This read-only command loads one preparation and its complete saved history in a five-second snapshot. It does not access source paths or inventory, initialize missing storage or migrate an older store. Only a full `journal-v1-` content ID is accepted; directory, selection and operation options are unavailable. There is no preparation, event-writing, quarantine or restore command. The library's publication API exists for disposable development fixtures and a later execution design.

JSON places the snapshot under `journal`: `intent`, `events`, `state`, `executable: false` and `current_state_verified: false`. The intent's `preparation_contract` is `preparation_only_v1`. It records exact plan/observation/store/inventory/finding bindings, authoritative base64 source/destination path bytes, supplied object/parent identities and a requirement that the destination be absent. These requirements are recorded, not checked against the live filesystem. `generation` refers to saved inventory generation; it is not a platform inode generation. Events use `evidence_source: "caller_supplied_record"`; their saved observations do not establish actual movement or current identity.

| Saved state | Meaning |
| --- | --- |
| `prepared` | Only a non-executable preparation was saved. |
| `outcome_unknown` | An attempt or unknown result was recorded. No known filesystem outcome or retry permission follows. |
| `recorded_at_source` | A caller recorded a matching source object and absent destination. |
| `recorded_at_destination` | A caller recorded an absent source and matching destination object. |
| `recorded_conflict` | A caller recorded both locations as present. Review is required. |

The human view labels outcomes as recorded observations and quotes paths. It does not offer an action. A missing result is an unknown outcome, even if the process stopped before returning. A library caller cannot record a second attempt. Reconciliation records are accepted only after unknown/conflict states, and never authorize a retry. Restoration preparation links the original immutable quarantine intent, exact reversed path bytes and last recorded object identity. Review expiry or revocation does not erase restore evidence; restoration itself remains unavailable.

Plan-store schema 4 adds immutable intents, events and chain heads. Migration and the first preparation commit together in a FULL-synchronous transaction. Each record is capped at 32 KiB, paths at 4096 bytes and each intent at 16 events. Request keys identify exact retries; changed requests conflict. Readers validate canonical payloads, bindings, contiguous sequences, predecessor IDs and head/event agreement. Invalid or incomplete history returns exit 1 with `journal_outcome_unknown`, without partially trusted record output. Missing storage/IDs and cancellation use the usual errors; invalid options use exit 2.

The journal preserves original and restoration records independently of rebuildable inventory. It has no history retention policy. Digests and immutable-row triggers detect accidental corruption and ordinary incomplete chains; coherent same-user rewriting or deletion of an event together with its matching head is outside this protection. Process-crash tests validate publication and rollback, not filesystem movement or power-loss durability. The preparation contract and review-v1 can never authorize a future executor; that needs separate versioned consent and an execution design. Native [rename experiments](../experiments/rename/README.md) demonstrate that exclusive destinations and held descriptors do not bind a renamed source name to the reviewed inode. Capabilities add `journal_records: true`; `cleanup` remains false.


## Observe recovery locations

```sh
rydd journal --observe INTENT_ID [--json]
```

This separately observes current metadata at the exact source and destination paths from a valid journal record. It leaves the saved state and history unchanged. No inventory, review approval, package-manager command, file body or recursive listing is required. Review expiry/revocation does not block access. Source or destination errors become explicit uncertain location results; corrupt journal history is rejected before filesystem access. It cannot be combined with `--show`, repeated modes, directory flags or selection options.

The JSON `journal` object contains `intent_id`, `saved_state`, `reference_kind`, `reference_id` and `observation`. The comparison reference is the last identity-bearing recorded event, or the preparation's source identity when no such event exists. This does not authenticate a caller-supplied record. Observation source is `live_recovery_metadata`, with a check time and separate source/destination location results. The overall status remains `outcome_unknown`; `current_state_verified`, `executable`, `historical_mount_verified` and `scope_verified` remain false.

A held no-follow directory chain locates each parent, using the built-in system/Library/protected-name and dataless-directory safeguards. Saved user exclusions and policy scope are not loaded or verified. The parent must match the supplied device/inode; historical parent ctime changes are disclosed because a move itself changes directory metadata. Both final names are probed repeatedly, with exact-parent stamp stability and ancestor/named-link identity and mount checks before handles close. A missing final child below the expected validated parent can be `observed_absent`. A missing ancestor cannot establish final-child absence. Symlinks, unsupported objects, inaccessible paths, changed parent identities, mount boundaries and changing paths remain blocked or unavailable. Location `status` is `observed_present`, `observed_absent`, `blocked` or `unavailable`. Present directories add observed parent/object identities and `identity_relation`: `metadata_matches_reference`, `potential_identity` or `different_identity`. `device_inode_match`, `ctime_match`, `parent_ctime_changed` and `object_ctime_changed` qualify that comparison. Block codes include `parent_identity_changed`, `ancestor_unsupported`, `object_unsupported`, `mount_boundary`, `path_changed_during_check`, `scope_excluded`, `filesystem_unsupported` and `path_unavailable`; messages contain no source values.

A matching directory's device/inode and ctime are compared with the reference; a matching device/inode with changed ctime is only a potential identity match. Observed identities contain no invented scanner generation.

These finite observations do not lock out later changes, prove inode continuity across a crash, authenticate dependency contents, establish original ancestor scope or determine that quarantine succeeded. Preparation records contain no historical mount identity, so the check cannot establish that the original mount remained unchanged. Present objects at both locations can indicate a conflict but cannot permit overwriting. No result can authorize retry, restoration or cleanup, and no result is appended to the journal. Normal access times may change. Exit 0 means the observation completed, including blocked/unavailable locations; inspect statuses. Cancellation and invalid storage use the usual failures. Capabilities add `journal_location_observations: true`; cleanup remains false.


Recovery paths are capped at 4096 bytes and 256 components each, with at most 512 held chain descriptors plus transient probes. The shared five-second context is cooperative; a blocked kernel call can exceed it. No durable cursor or stored recovery observation is created. Symlink aliases used by scanning can be refused by these stricter no-follow paths.

## Saved hash observations

```sh
rydd hashes [--work WORK_ID | --groups] [--json]
rydd --data-dir /absolute/path/to/state hashes --work 1 --json
```

`hashes` reads the one finite saved hashing selection, its work records and its reservation budget. `--work` optionally selects one canonical decimal work ID from `1` to `20` within that selection. `--groups` selects the historical matching view below; it cannot be combined with `--work`. Repeated mode flags, `--groups=false`, positional IDs, directory/source options, pagination and operation flags are rejected before storage access. Global `--data-dir` selects the hashing store; it must precede the command. The standard `--json` placement rules apply.

The command opens only existing `hashes/hashes.sqlite3` storage under the selected data directory. It does not load configuration or inventory, access source paths, initialize or migrate storage, recover interrupted work, record consent or dispatch a read. Sources and inventory can be unavailable. Readers do not take the hashing writer lock. Opening and loading use a shared cooperative five-second context. No source files or saved records are changed; normal SQLite reader sidecars may change.

JSON uses the standard envelope with a `hashes` object. It retains the saved snapshot's store/selection/inventory IDs, `source: "saved_hash_observations"`, `contract: "full_file_sha256_v1"`, budget and work. `selected_work_id` appears when `--work` filters the work list. `budget_scope: "whole_saved_selection"` always applies: filtering one work item does not filter the budget. Paths use authoritative base64 `path_bytes`; the human view quotes the full raw path and escapes controls.

| Saved work state | Meaning |
| --- | --- |
| `pending` | Work is pending in the saved queue. |
| `running` | An unsettled attempt is saved. This does not prove a process is active. |
| `complete` | A historical full-read SHA-256 observation is saved. Current files are not checked. |
| `invalidated` | Work was refused. Its previous prefix does not establish current contents. |

`durable_offset` is the saved prefix in bytes, or the full file size for a completed record. `checked_at` describes the historical check; a zero value means no checked prefix is recorded. A completed `sha256` is not duplicate detection, current-content verification or cleanup authority. All provenance/content/current-state/duplicate verification and execution flags remain false; `estimated_reclaimable_bytes` remains `null`.

`latest_attempt` distinguishes `reserved` (usage not settled), `settled` (known requested/read/elapsed usage), and `interrupted_unknown` (usage unknown after writer recovery). Unknown `observed_requested_bytes`, `observed_read_bytes` and `observed_elapsed_ns` are present as `null`; known zero stays zero. No attempt is represented by an absent `latest_attempt`. Reading a saved running record does not perform recovery.

The budget contains the saved UTC reservation day, clock high-water, charged allowances, settled known usage and interrupted charges, for that day and across all reservation days. The unchanged JSON keys `unknown_reserved_bytes` and `total_unknown_reserved_bytes` count only recovered `interrupted_unknown` attempts. A currently unsettled `reserved` attempt has null usage and remains fully charged, but is excluded from those interrupted counters until writer recovery. The human view labels them as interrupted charges. The budget describes the whole saved selection, including work hidden by `--work`. Full reservations are never refunded; known usage counters omit unknown attempts. These are not physical I/O measurements or reads per wall-clock day. The budget itself stores no limit; a separate saved read consent can state fixed approved limits. This view does not infer remaining quota.

Exit `0` means the saved snapshot was read, including a present store with no selection. Missing storage or work uses `not_found` and exit `1`; corrupt/incompatible records use `hash_invalid` without partially trusted record output. Cancellation and storage permissions use the standard failures. Invalid arguments use exit `2`; output-write failures use exit `1`. Capabilities include `saved_hash_reports: true`. Separate `hash` modes save a proposal, record read consent, perform one guarded step or revoke consent. Duplicate detection and cleanup remain false.

### Matching historical hashes

```sh
rydd hashes --groups
rydd --data-dir /absolute/path/to/state hashes --groups --json
```

This mode compares only completed full-file observations in the existing finite selection. A group contains at least two recorded paths with the same logical size and full SHA-256 digest. Groups are ordered by descending size, then digest; members retain saved work order. The size is per file. Observations can come from different times and do not establish current equality. Sources, inventory and configuration can be offline; there is no live path check, new read consent, recovery or source read.

JSON retains the standard `hashes` envelope. This mode uses `source: "saved_hash_observations"`, `contract: "historical_full_sha256_groups_v1"`, `hash_contract: "full_file_sha256_v1"`, and `scope`/`budget_scope: "whole_saved_selection"`. It includes store/selection/inventory IDs, saved budget/consent and `groups`, without changing the default observations schema. All verification/execution flags remain `false`, and `estimated_reclaimable_bytes` remains `null`.

| Whole-selection count | Meaning |
| --- | --- |
| `selected_work` | All records in the one saved selection, at most 20. |
| `completed_observations` | Records with a validated completed full-file observation. |
| `unfinished_work` | Records without a completed observation, including pending, running and invalidated states. |
| `unmatched_completed_observations` | Completed observations outside every displayed matching group. |

`groups` is always an array, including `[]` when no matches are saved. Empty groups do not establish complete scan coverage or absence of duplicate files. Each group includes `logical_bytes`, `sha256`, `members`, `saved_identities`, `repeated_saved_paths` and `conflicting_saved_identities`. Each member retains exact work/file/root IDs, authoritative `path_bytes`, its individual `checked_at`, saved device/inode/change/mtime/allocation evidence and repetition/conflict flags.

Identity counts describe recorded device/inode values. Repeated identities and conflicts are classified across the whole selection, including unfinished records and observations outside the displayed group. Differing saved size, change time, modification time, allocation or completed digests makes an identity conflicting. Group counts cover only its members; member flags retain the wider context. Multiple paths to one recorded identity can still form a group, with an explicit alias qualification. These records do not prove current hardlinks, inode continuity or independent storage.

The human view quotes paths, shows historical check times and labels sizes per file. It provides no keeper or savings recommendation. Saved budget and consent retain their existing semantics; current read permission remains unevaluated. Both the immutable selection and observed work come from one bounded read transaction. Argument/error/output contracts are the same as the default saved view. `saved_hash_groups` is true; `duplicates` and cleanup remain false.

## Save and show a hash proposal

```sh
rydd report --same-size -d /absolute/path/to/folder --json > same-size.json
rydd hash --select -d /absolute/path/to/folder --from same-size.json --json 12 13
rydd hash --show SELECTION_ID --json
```

Use saved file IDs from that exact report page. `hash --select` accepts 1–20 unique canonical positive decimal IDs and one exact manual scan root. Options must precede IDs. `--from` names one existing standard successful API version 1 `report --same-size` JSON page, not a list of IDs or a custom export. Global `--data-dir` must precede the command and must select the same data directory used for the manual scan and report. `-d` and `--directory` are aliases; options cannot be repeated. `--show` accepts one full lowercase 64-character selection ID and cannot be combined with selection or read-consent options.

The named report input is limited to 1 MiB. Opening rejects final symlinks and nonregular files, including FIFOs, and checks the held and named file identity, size and modification time after reading. Duplicate JSON keys or file IDs, unknown or missing fields, null required metadata, wrong envelopes, other report modes and altered verification qualifications are rejected. These checks do not establish report provenance. `path_bytes` is authoritative; the lossy display `path` does not replace raw bytes.

Selection opens only the existing saved inventory derived from that exact manual root. It compares every selected file field and inventory identity with the report before initializing hash storage, then captures again for publication. It refuses stale evidence, missing IDs, other roots and incomplete capture evidence. Looking up IDs alone never replaces the reviewed rows. Root and ancestor records are a new saved baseline at proposal capture; they were not displayed by the earlier same-size report. Neither operation opens source paths, loads configuration, runs a scanner, reads selected file contents or migrates inventory.

Successful new selection saves one immutable, initially unapproved proposal in global hash storage. It cannot replace an existing different selection; a conflict returns `already_exists` and exit `1`. A matching retry preserves the existing proposal, any saved read consent and work state without recovering an interrupted attempt. The selection writer can initialize hashing storage, but cannot dispatch a content read. `hash --show` opens only existing hashing storage and works when the report, source root and inventory are unavailable. It does not initialize, migrate, recover or change saved records. Reader sidecars can change as described for `hashes`.

JSON uses the standard envelope with a `hash` object: store/selection/inventory IDs, `source: "saved_hash_selection"`, the hashing contract, `source_locator` and complete `targets` containing root, file and ordered ancestor evidence. The manual locator contains `kind: "manual_inventory_v1"`, authoritative `root_path_bytes` and the derived `inventory_key`; it is not permission to read source files. Legacy library selections remain viewable with no locator. No checkpoint bytes or continuation state are exposed. Provenance/content/current-state/duplicate verification and execution flags remain false; estimated reclaimable bytes remain `null`. The human view quotes full file/root paths and every relative ancestor path, shows file IDs, sizes and dates, and labels the selection as unapproved only when no read consent record is saved. Logical and allocated sizes are not estimates of space you can free.

Report reads, inventory capture and saved publication share a cooperative five-second context. Regular filesystem calls cannot be preempted. Invalid arguments and report schemas return exit `2`; missing storage/selection uses `not_found`, corrupt hashing storage uses `hash_invalid`, and other storage/cancellation failures use the standard operation errors. Publication can finish before a canceled reply or output-write failure. Such failures do not prove the proposal was rolled back. Inspect `hashes --json` for the saved selection ID, then reopen it with `hash --show`; the command never retries implicitly. Capabilities include `saved_hash_proposals: true`.

### Saved read consent records

`hashes` and `hash --show` can include an optional `read_consent` object. It exposes the read consent ID, exact approval binding, fixed step/day/lifetime reservation limits, prior lifetime charges, recorded creation/expiry times, writer clock observations and any revocation. It contains no checkpoint or raw file contents. These saved-only commands do not record consent, evaluate current read permission or dispatch reads.

| Saved consent status | Meaning |
| --- | --- |
| `recorded` | Consent is saved. Current permission, freshness and remaining quota are unevaluated. |
| `expired_observed` | A writer recorded expiry. This reader did not make a new clock or expiry observation. |
| `revoked` | Revocation is saved. Any separate saved expiry observation remains visible. |

`current_read_permission_evaluated` stays `false`. The human view uses a saved-consent banner when the record exists, displays its exact recorded limits and expiry, and states that current permission is unevaluated. A recorded expiry date is not a countdown or a fresh permission check; the view does not compare it with the current clock. Day limits and budget counters refer to reservation days. Lifetime charges include prior, canceled and unknown reservations. Filtering `hashes --work` does not filter consent scope. No remaining quota or time to expiry is calculated, and read consent never authorizes cleanup. Missing required records for an existing consent, or corrupt/incompatible consent and lifecycle records, are refused with `hash_invalid` rather than shown as an unapproved selection.

## Approve, run one hash step and revoke read consent

```sh
rydd hash --approve SELECTION_ID --confirm-content-read --max-day-bytes N --max-total-bytes N [--json]
rydd hash --run APPROVAL_ID [--json]
rydd hash --revoke APPROVAL_ID [--json]
```

Choose exactly one mode. Selection and read consent IDs must be full lowercase 64-character IDs. Options cannot be repeated. These modes do not accept directory, report, file ID, target or step-allowance overrides. Global `--data-dir` must select the store containing the saved proposal and precede `hash`.

Approval requires explicit confirmation and both byte caps. There are no default caps. Each cap is a canonical positive decimal integer from `1` to `1125899906842624` (`1 << 50`): signs, leading zeros and fractional values are rejected. The day cap applies to the saved UTC reservation day. The total cap is an absolute lifetime ceiling for the whole selection, including charges before approval and canceled or interrupted attempts. Full reservations are never refunded. A cap below the next durable SHA-256 block can defer work without reading; caps do not guarantee completion.

`--approve` records consent for the proposal's exact store, selection, inventory and frozen manual source locator. It fixes a 24-hour expiry, a 1 MiB step ceiling and the supplied caps. It does not start a read. A matching retry returns the same approval ID, expiry and caps; it cannot renew permission or increase a limit. A different approval conflicts with `already_exists`. Legacy selections without a manual locator cannot obtain this consent.

Approval and revocation open an existing exact saved record before acquiring the selection writer. Every read-consent writer also requires existing initialized storage; if it disappears after lookup, the command refuses it rather than recreating it. Missing storage or IDs do not initialize storage. Approval and revocation do not load configuration or inventory, access source paths, recover interrupted work or read selected file contents. A valid existing hash schema can migrate when its writer opens. `--revoke` saves one immutable revocation; a matching retry preserves it. Revocation blocks future reservations after it obtains the writer lock. It cannot preempt a call already holding that lock or a blocked kernel operation.

`--run` derives only the frozen manual inventory from the proposal's authoritative root bytes and key. It loads current configuration exclusions; configured roots cannot widen this selection. Missing configuration is allowed, but invalid configuration fails. A held no-follow configuration descriptor must be a bounded, private, single-link regular file and must not match a selected file or ancestor identity before its contents are read. The derived inventory receives a private ownership, link and known-alias metadata preflight before SQLite opens it. Global state and configuration paths are protected, including their resolved aliases. Missing inventory is not replaced or initialized.

These private SQLite path checks do not authenticate records against coherent same-user rewrites or adversarial replacement between metadata preflight and SQLite open. They are not an atomic namespace snapshot. Actual source reads still require held-descriptor file, ancestor, root and mount revalidation against the exact saved evidence.

One invocation makes at most one guarded hashing step and exits. It does not loop or retry. The engine rechecks consent lifecycle, clock, fixed caps and current saved inventory before reserving the full allowance. It then checks held live evidence before reading selected-file contents and again before publishing checked progress. A refused live check can retain the full charge with zero selected-file bytes read. Each step reads at most 1 MiB of selected-file contents in chunks of at most 32 KiB. Opening, setup and the step share a cooperative five-second context; filesystem calls cannot be preempted, and bounded accounting settlement can finish after cancellation. Partial SHA-256 progress saves only complete 64-byte blocks, so a later explicit step may reread an unsaved tail.

Normal hashing-writer opening can perform metadata-only recovery of an earlier interrupted attempt even if a new read is denied. Recovery retains its full charged reservation and records unknown usage; it does not read source contents. Configuration or inventory failures can occur before that writer opens. A denied or failed run therefore does not promise that all saved records stayed unchanged.

Successful approval and revocation JSON use the standard `hash` envelope with `mode` and `read_consent`. A successful run uses `hash.mode: "run"`, the exact `approval_id`, fixed `step_byte_limit`, `budget_scope: "whole_saved_selection"`, the step `result` and saved `read_consent`. The result distinguishes observed prefix from durable saved prefix and includes known requested/read/elapsed usage and charged reservation bytes. The human view quotes the exact path and labels a completed digest as historical SHA-256. Completed observations still do not prove current contents, duplicate files, a keeper choice or safe cleanup.

Invalid options or caps return `invalid_arguments` and exit `2`. Missing records use `not_found`; corrupt records use `hash_invalid`. Consent failures use `read_consent_required`, `read_consent_expired`, `read_consent_revoked` or `clock_rollback`. Budget deferrals retain `daily_byte_limit`, `lifetime_byte_limit` or `durable_quantum`. Changed inventory uses `hash_inventory_changed`. These and other operation failures return exit `1` with the standard error-only JSON envelope; they do not fabricate zero usage or expose a digest. Publication can finish before cancellation or output failure. Inspect `hashes` and `hash --show` before another explicit run; no failed command retries implicitly.

Capabilities expose `hash_read_consent`, `guarded_hash_steps` and `full_hashing` as true. Duplicate detection, cleanup and broader resource-hardening gates remain false. Read consent authorizes only bounded full-file hashing of the frozen selection.
