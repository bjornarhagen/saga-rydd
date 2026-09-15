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

## Future actions

Keep discovery, review and execution as explicit commands. Machine readability is not cleanup authorization: future actions must use exact plans, revalidation and explicit user approval or an already approved narrow policy. Destructive commands and idempotency keys will be designed when action execution is implemented.
