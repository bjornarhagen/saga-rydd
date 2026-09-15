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

Put global `--data-dir` before the command. `--json` can go before or after the command. Commands do not prompt. `capabilities --json` works without initialization and lists supported commands, effects, arguments, features and error codes. Unimplemented findings, duplicates and cleanup are explicitly false.

Every JSON response has `api_version: 1`, `ok` and `command`. Success fields depend on the command; status preserves its existing fields and adds `dispatch_budget`. Failures have `error.code` and a human-readable `error.message`. Match codes, not message text. Consumers must tolerate additional fields; incompatible contract changes require a new API version.

| Exit status | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Operation failed (including unavailable worker, configuration/state errors) |
| 2 | Invalid arguments or unsupported output mode |

Control responses contain `acknowledged` and a worker snapshot. A stop acknowledgement means shutdown was requested; it does not guarantee process exit. Pause is durable and requests cancellation of the current cooperative batch; an active filesystem call can still be draining. Status reads saved inventory even without a worker. Paths may contain private information; treat reports as local data.

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

`rydd report [--limit N] [--cursor TOKEN] [--json]` ranks observed regular files by logical size descending, then saved entry ID descending. The default page size is 20, maximum 200. JSON uses the standard envelope with a `report` object; capabilities advertises `file_reports: true`, with `directory_size_reports: true` and findings still false.

The command reads an existing database without loading configuration, contacting the worker, scanning the filesystem or changing state. Enabled roots and exclusions reflect saved inventory state; apply configuration changes through the normal state/worker workflow. Files can have disappeared or changed since observation. `source` is `saved_inventory` and `current_state_verified` is false. A report is not a deletion recommendation.

Each file includes `logical_bytes`, `allocated_bytes`, `observed_at`, `modified_at`, `skip_reason` and `parent_pass`. Parent pass labels are `unknown`, `directory_error`, `unconfirmed` (different saved generation), `partial`, or `observed_in_completed_parent_pass`. The last label describes only the direct parent's saved pass; it does not verify ancestors or current disk state. Historical/unconfirmed entries remain visible with labels instead of silently implying they are current. No reclaimable-space total is computed.

`path` is a display string. `path_bytes` is base64-encoded original absolute Unix path bytes and is authoritative when names are not valid UTF-8. Human output quotes paths and errors so embedded control characters do not execute in the terminal.

`roots` contains up to 100 enabled roots, with pending/running job counts, directory error counts, last root error and last root-directory pass time. `roots_truncated` indicates omitted root diagnostics. Root pass times cover direct children, not complete subtrees; empty queues do not prove coverage, and saved errors do not establish current availability. Observation timestamps describe freshness without inventing whole-tree completion percentages.

Pages use a cursor based on size and entry ID rather than an increasing offset. Each invocation uses one short SQLite read snapshot, capped at five seconds, returning at most one extra file to determine whether a next page exists. Index-backed file pagination bounds returned data, while root diagnostic counts still depend on inventory size. Concurrent scanning can change ordering between pages; this is not a frozen export and changes can cause omissions/repeats across invocations. Missing/empty inventories, exhausted pages and invalid cursors produce explicit empty reports or standard errors. A returned `next_cursor` should be passed unchanged with the same state directory. Selected-directory measurement is described below; recommendations are the next MVP task.

## Saved directory-size reports

```sh
rydd report --directory /absolute/path/to/folder
rydd report --directory /absolute/path/to/folder --json
```

Directory mode replaces file pagination and cannot be combined with `--limit` or `--cursor`. It uses enabled roots saved in the database, with lexical path matching and no filesystem resolution or scanning. The selected path must lie within a saved enabled root. Missing/non-directory observations or missing ancestor observations return `unknown` with null sizes, rather than a misleading zero-byte result.

JSON places the measurement in `report.directory`; file pagination fields are unused in this mode. The report measures the selected directory plus at most 9,999 descendant observations in one read snapshot, with the existing five-second command deadline. Ancestor validation is capped at 256 directory records. `truncated` means additional saved entries were omitted, and there is currently no continuation for this measurement. The implementation bounds result processing and memory; SQLite can examine more index rows while locating the subtree. This is a selected-subtree measurement, not a ranking of all directories or a persisted incremental aggregate.

`logical_file_bytes` sums regular-file path sizes. `unique_inode_allocated_file_bytes` counts each known `(device,inode)` once in the measured portion, using the maximum observed allocation if records disagree; disagreement marks the result stale. Missing identities are counted per path and disclosed through `unknown_inodes`. Directory metadata, symlinks and other objects are excluded. Shared clones, snapshots and hardlinks outside the folder can retain space, so neither number is a reclaimable-space estimate. Do not add overlapping folder measurements.

Status describes saved evidence, never current disk state:

- `unknown`: the selected directory or an ancestor lacks a usable saved directory observation; sizes are null.
- `stale`: a saved root error, unconfirmed ancestry/entries, directory observation newer than its listing, or conflicting inode sizes make the aggregate uncertain.
- `partial`: listing gaps/errors, skipped entries, a bound being reached or incomplete ancestry prevent recorded completeness.
- `recorded_complete`: all examined saved directory listings and links meet the checks, with no truncation or known gaps. This does not establish an atomic whole-tree snapshot or current completeness.

Stale status takes precedence over partial; individual counters and notes still disclose missing/error/truncated portions. Oldest/newest observation timestamps expose age without imposing an invented inactivity threshold. Partial or stale totals can overestimate or underestimate actual current size. A fully recorded empty directory reports zero regular-file bytes; an unobserved directory reports null. No report authorizes deletion.

## Future actions

Keep discovery, review and execution as explicit commands. Machine readability is not cleanup authorization: future actions must use exact plans, revalidation and explicit user approval or an already approved narrow policy. Destructive commands and idempotency keys will be designed when action execution is implemented.
