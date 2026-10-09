# Worker foundation (P1-04)

## Ownership and controls

Every application state writer holds an exclusive nonblocking `flock` on a private `writer.lock` file for its lifetime. Readers do not acquire it. The lock file is retained across shutdown: unlinking it could let two processes lock different inodes. The kernel releases ownership on process death; stored PIDs are diagnostic only. State initialization and migrations obey the same lock as the daemon. Local macOS/Linux filesystems are the intended state storage; network filesystem semantics are not validated.

The foreground daemon loads configuration at startup and serves version 1 JSON-line requests for `status`, `pause`, `resume` and `stop`. Pause is committed to SQLite before acknowledgment. Pause and resume are idempotent. Stop acknowledges a request, then drains the current cooperative chunk; callers must wait for process exit before reopening a writer. SIGINT/SIGTERM follow the same shutdown path. A noncooperative handler causes an error after five seconds of shutdown grace; process exit releases its lease for recovery.

The socket is mode 0600 in an owned 0700 directory. Server and client verify peer user IDs using Linux `SO_PEERCRED` or macOS `LOCAL_PEERCRED`. A hash of the canonical state path plus the user ID keeps the runtime path short enough for macOS. The default parent is `/tmp`; `RYDD_RUNTIME_DIR` can override it and must agree across clients and daemon. Docker uses a shared `/run/rydd` volume. A regular file or symlink at the endpoint is rejected. Only the lock owner may replace a stale socket; shutdown checks socket identity before unlinking.

Requests are capped at 4096 bytes, responses at 8192 bytes on the client, concurrent server connections at eight, and request lifetime at two seconds. Unknown fields, versions and commands are rejected. Saved status is readable without the daemon; a control connection error does not hide the saved summary. Live and saved fields are separate observations, not one atomic snapshot.

## Queue and recovery

Schema v2 adds a random lease token, diagnostic lease deadline and durable pause setting while preserving v1 inventory, roots and cursors. A job is unique by root, kind and relative path. Enqueue is idempotent and preserves existing progress. Claiming atomically picks an eligible pending job and assigns a new token. Completion or checkpoint writes must match that token. Stale attempts cannot overwrite later attempts, including when a deleted job ID is reused.

After acquiring exclusive ownership at startup, the worker requeues all running jobs with their last committed cursor. It does not wait for wall-clock lease expiration: the previous owner is gone. Conversely, it never steals a live handler's job because a deadline passed during sleep. Disabled roots and unknown handler kinds retain their jobs without being dispatched. Cursors are limited to 64 KiB and saved errors to 2048 bytes.

One event loop owns database writes and one handler runs at a time. Handlers are cooperative, read-only, and without a database connection. They return a bounded cursor/completion or an inventory batch. The event loop commits inventory effects, child jobs, reconciliation markers and cursor together through `CommitScan`. Generic handler errors retain the previous cursor and back off exponentially to one hour. Scanner scope/enumeration faults save an error and retry in one hour without reconciling. Cancellation restarts interrupted enumeration without error backoff.

The next chunk starts no earlier than the configured interval after the preceding start (default five minutes); its cooperative timeout defaults to 30 seconds. A paused worker or a queue with no eligible jobs waits on control/cancellation without repeatedly querying SQLite. Startup can dispatch a due chunk immediately. The interval is not a persisted cross-restart budget.

## Process CPU observations and cooperative backoff (P1-06b3)

The worker observes cumulative native user and system CPU time for its own process before dispatch checks and after saving each completed chunk. The difference covers all process threads in that work window, including the owning loop's database writes and control work. It excludes child processes, startup and time outside completed windows. It does not measure physical disk activity or energy use. macOS and Linux use `getrusage(RUSAGE_SELF)`; no helper process or periodic idle polling is needed.

For a known window, additional wait is `max(0, 100 × CPU time − elapsed window)`, toward the fixed gentle pacing target of 1% of one core. The wait is capped at one hour and the live observation reports whether it was capped. CPU waits share the existing cancellable timer and cannot shorten cadence, job retry or durable dispatch restrictions. Pause/resume preserves the CPU deadline. An overdue timer permits one chunk; the worker earns no catch-up credits. Long waits and generated delayed-clock tests are not physical sleep acceptance: monotonic clocks can stop during system sleep.

Unavailable, decreasing, invalid or overflowing native observations are unknown rather than measured zero. An unknown completed window adds no new CPU wait; existing restrictions remain. Live status distinguishes `not_recorded`, `observed` and `unknown`, with nullable measurements and an unknown-window count. This live observation resets with the worker instance; saved experimental-turn feedback is separate, as described below. Cooperative pacing cannot interrupt a busy chunk or prove the hourly CPU target. Source-thread priority requests are implemented. Whole-process resource quotas, power detection, native sleep and soak gates remain open.

References: [Apple getrusage](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/getrusage.2.html), [Linux getrusage](https://man7.org/linux/man-pages/man2/getrusage.2.html), and [Go monotonic clocks](https://pkg.go.dev/time#hdr-Monotonic_Clocks).

## Saved experimental-turn CPU feedback (P1-06b7)

The explicitly experimental worker saves one latest CPU window for a durably claimed fair source or saved-inventory maintenance turn. Its opaque marker binds the open store, root, consumed dispatch and optional source lease. The marker is committed before source construction, scanner allowances or maintenance. The owning loop settles the window after saving progress, even when progress has cleared the source lease. The last begun dispatch remains recorded after settlement, preventing reuse. A failed or uncertain settlement stops further dispatch and preserves evidence for explicit writer recovery.

A completed known observation uses the same 1% formula and one-hour maximum as live pacing. A completed unknown observation retains existing fixed pacing without inventing CPU usage. An interrupted pending window becomes `recovered_unknown` once during explicit writer recovery, with a one-hour delay from that first successful recovery. Repeated recovery does not renew the delay. Accounting recovery waits for its saved clock high-water independently of future jobs, cadence and source quotas; every dispatch gate is checked again afterward. Recovery spends no new dispatch or scanner allowance.

Saved UTC deadlines and live elapsed deadlines remain separate. The timer waits for the largest remaining duration and rechecks after waking. A wall adjustment or status request cannot shorten a retained elapsed wait. Pause and resume preserve it, and delayed wakes grant no catch-up work. Newly recovered feedback retains elapsed protection in the current process; an older terminal deadline reopened after a forward wall adjustment has only historical UTC evidence.

Schema 12 adds constant-size feedback state. Saved readers of schemas 4–11 report feedback as unavailable without migrating. Saved status never recovers a window, advances its clock or evaluates current permission. `pending` does not prove that a process is running. Earlier usage is unknown; untracked window counts do not establish zero CPU use. Exact terminal retries preserve the first saved result, and unknown counts disclose saturation.

The declared scope is `experimental_inventory_dispatch_windows`, with contract `worker_cpu_feedback_v1`. Pre-marker setup, WAL checks, planning, readiness and claim work, plus startup/revisit pages, lack durable interruption coverage. Normally completed deltas can include earlier setup work in the measured window. Final feedback publication is excluded. Child processes, manual scans and explicit hashing are outside this feedback. Ordinary idle and nonexperimental workers remain untracked. This provides restart-safe pacing for the declared turns; it is not a whole-process, daily or hourly CPU quota, a physical sleep detector or cleanup permission.

## Durable experimental scanner API allowances (P1-06b4)

The source scanner is constructed lazily after the private control listener is ready. The owning loop checks that the full startup-plus-step charge fits before claiming a job or reserving dispatch. It commits a startup receipt and a 65,536-attempt Next receipt before invoking the permitted source APIs. Handler code holds no database connection or transaction. The operation guards cancellation, finite allowance, UTC/lease expiry, wall-clock high-water and independent wall-work and monotonic elapsed deadlines. Both work deadlines use the remaining charged reservation window. Delayed construction cannot restart that window, and construction transfers its earliest deadlines to the subsequent Next call. A forward wall gap refuses the next source call even when the elapsed clock did not advance; it is not a sleep detector. Admitted failed API calls count; denied calls publish no batch or filesystem-unavailable/coverage evidence.

The owning loop settles known counts before saving job progress. Unused charge is retained. Panics and process-loss recovery retain full unknown charges; ordinary opening and saved status never recover outstanding work. This applies even when a later restart is idle. Fixed startup/Next slots and cumulative totals bound reservation storage. The first tracked partial day discloses earlier unknown activity. A later day resets daily counters only after outstanding work is settled or recovered; rollback cannot refill them.

Quota waits preserve status, pause and stop. Current scanner metrics are absent until construction, then include only actually admitted APIs. The saved budget and live observations are separate snapshots. Manual scans and explicit read/inspection contracts are unchanged. These allowances count scanner source APIs, not every syscall, physical I/O, configuration/SQLite/runtime work or a universal operation rate. Existing cadence, daily dispatch, WAL, CPU and job-retry gates still apply. See the [configuration and output contract](cli.md#durable-scanner-api-allowances).


## Saved-only inventory maintenance (P1-08d1)

Completed detailed and compact parent listings queue reconciliation of saved children. Once an enabled root has no inventory jobs, including delayed or future work, an exact-root maintenance step advances at most one worked bounded transaction. It retires obsolete compact generations, then revision-fenced absent/replaced subtrees, then invalidated detailed allocation cache/scratch state. Current observations and revision-matched completed caches stay intact. New scan observations cancel old retirement proofs.

The experimental owning loop discovers maintenance roots in pages of at most 128 raw roots plus one continuation row and serves controls between pages. Each selected root prefers its own due source work, otherwise its eligible saved-only maintenance, as described below. Each mutation turn uses the same durable dispatch/day reservation, CPU backoff, WAL and cadence gates as other background work. It opens no source scanner, spends no source API allowance and touches no owner plans, consent, hashes, actions or restore history. Revisit scheduling waits for maintenance to drain; restart rediscovers unfinished work without idle polling. Manual scans finish their saved maintenance before a later invocation starts another pass.

The batch bounds apply to returned roots and changed payload records, not every SQL row examined. Cooperative database deadlines remain required. SQLite can reuse freed pages without shrinking its file. Physical size admission, disabled-root retention and fair/adaptive scheduling remain separate.

## Fair experimental root turns (P1-07b2)

Experimental scanning accepts 1–32 exact configured roots before opening the writer, changing saved roots or constructing a source scanner. Manual scans, explicit hashing and default single-stream library behavior retain their contracts. A bounded saved snapshot rotates admitted root IDs after the last durably claimed root. It uses indexed per-root due/id lookup and pending-cache checks. Disabled roots and roots with no inventory job release their parked stream; future/delayed jobs preserve their root's unfinished evidence and block its maintenance.

```text
Choose the next ready root in saved rotation
  -> due source work, otherwise eligible saved maintenance
  -> shared cadence, day, CPU and WAL gates
  -> commit one dispatch reservation
  -> claim one turn and save root rotation together
  -> run one bounded source or database step
  -> choose the next ready root
```

A claim needs the latest unused dispatch reservation, less than two seconds old and within the lease. Lost replies or interrupted work retain the charge and rotation; explicit writer recovery recovers the source lease. Source turns also need the separate scanner API allowance. A source quota wait can skip source work while allowing another root's eligible database-only maintenance. It sleeps until saved due/resource times when nothing can run and rechecks readiness after CPU/cadence/WAL waits.

One shared scanner retains at most 32 logical streams with no eviction. Each holds at most 128 supported names, bounded to 4,096 bytes each, and preserves its generation/unread buffer when another root runs. Native generated maximum-stream fixtures observed 32 Linux and 64 Darwin directory descriptors, all closed afterward. The pinned Darwin Go directory implementation duplicates the descriptor for fdopendir ownership; a logical stream is not one physical FD on both platforms. The measured finite-fixture RSS is not the full unattended memory gate. Continual mutation, repeated crashes or an insufficient path allowance can still prevent completion. Adaptive timing, portable crash enumeration, million-entry background acceptance and physical resource/sleep tests remain open.

Schema 11 adds scheduler/cache indexes and one bounded saved rotation value; schema-10 saved readers remain compatible without migration. Capabilities use `experimental_root_turns_v1` with the 32-root/stream and 128-name limits. Full CPU/power, adaptive revisits and portable directory continuation remain false.

## Next integrations

- **P1-05 implemented:** `--experimental-scan` registers metadata inventory, seeds root jobs and commits bounded batches atomically. Schema v3 adds directory watermarks and skip reasons. See [inventory design](inventory.md). Keep experimental activation explicit until budget enforcement is verified.
- **P1-06:** enforce persistent metadata/content/CPU and daily budgets, power/sleep behavior and low priority. Durable dispatch cadence/daily batch reservations and WAL checkpoint backpressure are implemented; see [CLI contract](cli.md). The current cadence is only dispatch pacing; it does not enforce those resource targets.
- **P1-07:** fair/adaptive revisits and robust continuation across large directories.
- **P1-09:** install/uninstall native launchd/systemd user services. The current command does not self-install, detach or survive an unsupervised terminal closing.

Tests use synthetic queue jobs, disposable filesystem trees and private temporary state. Process tests cover SIGKILL with an active lease, preserved cursor, stale endpoint replacement, persistent pause, SIGTERM and scanner restart/completion. Native binary smoke checks exercise the CLI and writer exclusion. No cleanup runs; experimental scanning only runs when explicitly selected.
