# First use and recovery

Rydd currently supports read-only inventory, saved reports, guided review and separately approved full-file hashing. Cleanup is unavailable. A saved selection, matching metadata or matching historical hashes does not permit moving or deleting a file.

Use a native binary for your platform. [Local candidate packages](../CONTRIBUTING.md#local-candidate-packages) are available for development review; they are not accepted releases. Cloning, building and initializing the repository starts no background scan.

## Choose the folder and private state

For a first scan, choose one folder that you can review. Use a separate private state directory and the same global options for every command. `--data-dir` goes before the command. Reports and state contain private filenames; keep them outside version control.

```sh
rydd --data-dir /path/to/private/state scan -d /path/to/project
rydd --data-dir /path/to/private/state report -d /path/to/project
rydd --data-dir /path/to/private/state report -d /path/to/project --candidates
```

Manual scanning needs no `init`. It saves an inventory for that exact normalized folder. It runs in the foreground and inspects metadata without reading ordinary file contents. The default ten-millisecond delay spaces child-entry inspections; it does not limit every filesystem operation. Use `-s 25` for a longer delay. `--now` removes that deliberate delay and can produce substantial metadata I/O.

Press Ctrl+C to stop. Run the same scan command again to finish pending work. After scanning and saved maintenance finish, another invocation starts a new pass. An interrupted directory restarts its listing. Saved entries do not prove complete coverage or current contents.

Manual inventories are separate from configured background inventory. `status` and an unscoped `report` do not summarize every manual scan. Keep `-d` on reports for the chosen manual folder. See [manual scan semantics](cli.md#foreground-manual-scans).

## Review saved evidence

```sh
rydd --data-dir /path/to/private/state review -d /path/to/project
```

The interactive review shows saved `node_modules` candidates. Select numbered rows and inspect the exact preview before typing `save`. Saving records an unapproved historical selection. Unselected folders stay unchanged; this does not save a keep policy.

| Command or result | What it establishes |
| --- | --- |
| Candidate report | Saved evidence meets the report's review filter. Age does not prove inactivity. |
| `plan --show PLAN_ID` | The exact historical selection that was saved. |
| `plan --check PLAN_ID -d /path/to/project` | Whether saved inventory observations match the selection. |
| `plan --verify PLAN_ID -d /path/to/project` | Selected path metadata matched or failed during individual live checks. Dependency contents remain unchecked. |
| Same-size report | Saved logical sizes match. Contents remain unchecked. |
| Completed hash | A full-file digest was observed at a recorded time. Current equality remains unchecked. |
| Saved keeper/copy choice | Explicit historical roles for review. No cleanup permission. |

Use full IDs from the output. A later scan cannot add targets to an existing plan or hash selection. Size measurements are not estimates of space that cleanup can free; overlapping folders, hardlinks and shared storage need separate accounting.

[The CLI guide](cli.md#saved-hash-observations) describes the exact hashing workflow. Full-file reads require separate confirmation and fixed byte caps. Each run performs one bounded step and exits. Original selections and fresh keeper/copy jobs have independent consent, progress and charges. Review consent for a cleanup plan cannot permit a content read or cleanup.

## Configure a worker separately

Manual scans are sufficient for the first review. To prepare configured roots, initialize a dedicated private instance:

```sh
rydd --data-dir /path/to/private/state init --root /path/to/project
rydd --data-dir /path/to/private/state config check
rydd --data-dir /path/to/private/state status --json
```

`init` records configuration and state; it does not scan and does not overwrite an existing configuration. Configuration validation does not prove that an offline root is available. After an intentional configuration edit, `state init` validates it and synchronizes registered roots while retaining inventory. Stop any writer first.

Updated writers retain up to 128 root records before refusing new paths. Disabled roots count, and selecting an existing root preserves its history. Existing roots remain available in older stores already above that limit. Keep the existing state when capacity is reached; see [root admission and uncertain outcomes](cli.md#retained-inventory-root-admission).

`daemon` stays idle unless started with `--experimental-scan`. Experimental background scanning is explicit on each start and admits at most 32 configured roots. Its resource controls remain under development; start with disposable test folders. Manual scans retain their separate invocation flow.

Run the worker in one terminal:

```sh
rydd --data-dir /path/to/private/state daemon --experimental-scan
```

Use another terminal for controls:

```sh
rydd --data-dir /path/to/private/state status --json
rydd --data-dir /path/to/private/state pause
rydd --data-dir /path/to/private/state resume
rydd --data-dir /path/to/private/state stop
```

Pause persists across restart. An acknowledgment can precede the end of an active cooperative chunk; inspect status while it drains. Stop acknowledges a shutdown request. Wait for the foreground daemon to exit before opening another writer. Configuration changes take effect on the next worker start. The optional `scan.compact_inventory = true` setting changes dependency-tree storage for experimental scanning and keeps fixed daily revisits. It cannot be combined with adaptive revisits. A refused mode change requires restoring the previous configuration and finishing saved work; do not clear the queue. See [compact mode and recovery](cli.md#opt-in-compact-background-inventory).

Use the same short absolute `RYDD_RUNTIME_DIR` for the daemon and controls if overriding the default. Keep state on a local filesystem. See [worker ownership and recovery](worker.md#ownership-and-controls).

Optional `scan.cpu_session_charges = true` adds conservative saved CPU charges to this experimental worker. It defaults to false and has no effect on an ordinary idle daemon. Stop the worker before editing configuration. Activation upgrades the private inventory to schema 15, which binaries supporting at most schema 14 refuse. Turning the setting off retains history and debt while disabling this optional gate. Repeated prefixes can overlap; this is not a unique lifetime, hourly, daily or global CPU quota.

To inspect that history without starting or recovering a worker:

```sh
rydd --data-dir /path/to/private/state status --cpu-charges --json
```

The command opens only existing saved state. It loads no configuration, samples no current CPU or admission clock and evaluates no current permission. Unknown or uncertain tracking blocks new work for that Run while controls remain available. Preserve the exact error and inspect saved history before an explicit restart. See [CPU session scope and recovery](cli.md#optional-conservative-cpu-session-charges).

Service descriptors are a separate workflow. `service status` inspects the selected artifact; `service runtime-status` makes qualified Linux observations of an existing loaded unit. Artifact presence, a start reply and a declared manager PID do not prove readiness. macOS runtime observation is currently unsupported. Installing a macOS descriptor can affect future logins. See [service commands](cli.md#user-service-descriptor-preview) before taking a lifecycle action.

## Resolve common waits and failures

Start with the same data directory, folder and IDs used by the original command. Save private output when needed. Use `--json` for stable codes and fields; human wording is not a scripting contract.

| Symptom | Next step |
| --- | --- |
| Writer is busy | Check the foreground worker or other Rydd command. Request stop when appropriate and wait for process exit. Keep `writer.lock`; the kernel releases the lock on process death. |
| Worker is not running | Saved reports can still work. Start an explicit daemon only if you want one. This result alone does not mean saved state is damaged. |
| Worker remains paused after restart | Inspect status, then use `resume` for the same instance when ready. Restart does not clear pause. |
| No candidates | Inspect report selection counts, age and coverage. Confirm the exact `-d` folder. A lower age filter changes this report only and still requires review. |
| Measurement is partial, stale or unknown | Read its coverage explanation. For a compact manual inventory with pending calculations, use `measure -d /path/to/project`; pending scans must finish first. |
| Root is unavailable or changed | Inspect the recorded diagnostic and the intended folder/volume. Saved inventory is historical. Do not substitute another root to continue an exact saved read. |
| Daily allowance or cadence delays source work | Inspect saved dispatch and metadata accounting. Reservations stay charged after interruption. Wait for the legitimate allowance/due time; restart does not refund it. |
| CPU recovery or backoff delays work | Inspect saved CPU feedback. Writer recovery retains uncertain charges and a cooldown. Status does not recover work. |
| Optional CPU session backoff delays work | Inspect `status --cpu-charges` and the live wait reason. Saved deadlines do not calculate current remaining wait. Restart can add a fresh overlapping prefix and retains prior debt. |
| CPU session accounting is unknown or uncertain | Keep the exact error and saved history. Request stop and wait for exit before resolving the error or explicitly restarting. The current Run will not silently fall back to untracked work. |
| WAL backpressure delays work | Finish long-lived readers when appropriate. The worker retries under its existing gates. A large reusable WAL is not evidence that history must be deleted. |
| State threshold delays work | Inspect cached DB/WAL lengths and availability. Pending work remains saved. Restart is not a remedy for the threshold; freed SQLite pages may not shrink file lengths. |
| Optional API pacing blocks source work | Inspect the rate, work window and blocked reason. Low rates can fail conservative capacity planning; unsupported canonical paths block the current run. Stop before changing configuration. Pending work and charges remain saved. |
| Explicit hash read rate cannot fit a durable block | Inspect `scan.read_bytes_per_second` and the remaining work window. A known impossible rate refuses a new reservation. Existing consent and charges remain unchanged; inspect saved progress before another explicit run. |
| Power evidence is unknown | Read the qualified status. Unknown evidence retains fixed pacing; it does not establish external power. macOS performs no provider probe. |
| Clock rollback is refused | Check the machine clock and saved high-water evidence. Preserve the ledger. Do not reset timestamps or budgets to bypass the refusal. |
| Configuration changed during startup | Reload the intended configuration and start a new worker after the previous writer exits. A worker cannot adopt a mixture of old and new settings. |

Do not delete a lock, database, WAL, consent record or history to clear a refusal. There is no supported automatic repair or backup/export command yet. For corrupt state or uncertain storage, stop new writes, preserve the files and collect the exact private error before choosing a recovery action.

## Inspect an interrupted or uncertain read

For an original selection:

```sh
rydd --data-dir /path/to/private/state hash --show SELECTION_ID
rydd --data-dir /path/to/private/state hashes --json
```

For a fresh keeper/copy job:

```sh
rydd --data-dir /path/to/private/state hash --show-job JOB_ID
rydd --data-dir /path/to/private/state hash --show-job-read READ_CONSENT_ID
```

These views inspect saved records without reading selected file bodies or recovering work. A saved running record does not prove an active process. Recorded consent does not evaluate current permission, remaining quota or current contents.

An explicitly invoked run writer can recover interrupted accounting for its exact scope. That metadata recovery may happen even when another content read is denied. The full reservation remains charged; known usage and unknown interruption charge stay separate. A failed run therefore does not guarantee that saved records stayed unchanged.

Do not automatically repeat a failed run. First inspect its exact selection or job. Another explicit run requires still-valid consent, available fixed caps and current preflight checks. Expired, revoked or exhausted consent cannot be renewed or enlarged for that same record. A new read requires a separately reviewed scope and new consent; never create one merely to bypass a refusal. See [original read consent](cli.md#approve-run-one-hash-step-and-revoke-read-consent) and [fresh-job steps](cli.md#run-one-guarded-fresh-job-step).

## Inspect uncertain publication

Saving can succeed before an output, cancellation or sync error reaches the caller. An error reply is not proof that nothing was saved.

1. Preserve the exact returned ID, generation key and global options.
2. Reopen the saved selection, choice, job or consent with its show command.
3. Compare the exact scope and recorded state before choosing another action.

When a command documents an idempotent retry, reuse the same key and arguments. A different fresh-job key creates a different unapproved job. Do not infer read permission from a retry or an unchanged response. For exclusion and service publication, follow their reported inspection guidance; do not remove unfamiliar artifacts.

## Share a useful issue report

Include `rydd --version`, the command mode, exit code, stable error code, whether the worker had exited and a sanitized excerpt of relevant status. Describe the expected behavior and the observed result. Remove private paths, filenames, IDs, credentials and machine inventories before posting publicly. Keep full evidence privately when it is needed for investigation.

Current limits and acceptance evidence are in [PROGRESS.md](../PROGRESS.md). Cleanup, installed owner-service acceptance, physical power/sleep behavior, representative resource tuning, native soak and release/license decisions remain open.
