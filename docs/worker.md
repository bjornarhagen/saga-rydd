# Worker foundation (P1-04)

## Ownership and controls

Every application state writer holds an exclusive nonblocking `flock` on a private `writer.lock` file for its lifetime. Readers do not acquire it. The lock file is retained across shutdown: unlinking it could let two processes lock different inodes. The kernel releases ownership on process death; stored PIDs are diagnostic only. State initialization and migrations obey the same lock as the daemon. Local macOS/Linux filesystems are the intended state storage; network filesystem semantics are not validated.

The foreground daemon loads configuration at startup and serves version 1 JSON-line requests for `status`, `pause`, `resume` and `stop`. Pause is committed to SQLite before acknowledgment. Pause and resume are idempotent. Stop acknowledges a request, then drains the current cooperative chunk; callers must wait for process exit before reopening a writer. SIGINT/SIGTERM follow the same shutdown path. A noncooperative handler causes an error after five seconds of shutdown grace; process exit releases its lease for recovery.

The socket is mode 0600 in an owned 0700 directory. Server and client verify peer user IDs using Linux `SO_PEERCRED` or macOS `LOCAL_PEERCRED`. A hash of the canonical state path plus the user ID keeps the runtime path short enough for macOS. The default parent is `/tmp`; `RYDD_RUNTIME_DIR` can override it and must agree across clients and daemon. Docker uses a shared `/run/rydd` volume. A regular file or symlink at the endpoint is rejected. Only the lock owner may replace a stale socket; shutdown checks socket identity before unlinking.

Requests are capped at 4096 bytes, responses at 8192 bytes on the client, concurrent server connections at eight, and request lifetime at two seconds. Unknown fields, versions and commands are rejected. Saved status is readable without the daemon; a control connection error does not hide the saved summary. Live and saved fields are separate observations, not one atomic snapshot.

## Queue and recovery

Schema v2 adds a random lease token, diagnostic lease deadline and durable pause setting while preserving v1 inventory, roots and cursors. A job is unique by root, kind and relative path. Enqueue is idempotent and preserves existing progress. Claiming atomically picks an eligible pending job and assigns a new token. Completion or checkpoint writes must match that token. Stale attempts cannot overwrite later attempts, including when a deleted job ID is reused.

After acquiring exclusive ownership at startup, the worker requeues all running jobs with their last committed cursor. It does not wait for wall-clock lease expiration: the previous owner is gone. Conversely, it never steals a live handler's job because a deadline passed during sleep. Disabled roots and unknown handler kinds retain their jobs without being dispatched. Cursors are limited to 64 KiB and saved errors to 2048 bytes.

One event loop owns database writes and one handler runs at a time. Handlers are currently an internal test/integration interface: cooperative, read-only, and without a database connection. They return a bounded cursor or completion. On an error, the last committed cursor is preserved; repeated failures back off exponentially to one hour. Cancellation preserves progress without error backoff. A successful yield resets the consecutive failure count.

The next chunk starts no earlier than the configured interval after the preceding start (default five minutes); its cooperative timeout defaults to 30 seconds. A paused worker or a queue with no eligible jobs waits on control/cancellation without repeatedly querying SQLite. Startup can dispatch a due chunk immediately. The interval is not a persisted cross-restart budget.

## Next integrations

- **P1-05:** register scanner handlers, seed root jobs through the owning writer, and wake scheduling when new work is queued. Add bounded result batches and commit inventory effects and cursor/job changes in the same token-checked transaction. Do not write inventory from a handler goroutine or claim that this cursor-only interface already makes scan effects atomic.
- **P1-06:** enforce persistent metadata/content/CPU and daily budgets, power/sleep behavior, low priority and WAL checkpoint/backpressure. The current cadence is only dispatch pacing; it does not enforce those resource targets.
- **P1-07:** fair/adaptive revisits and robust continuation across large directories.
- **P1-09:** install/uninstall native launchd/systemd user services. The current command does not self-install, detach or survive an unsupervised terminal closing.

Tests use synthetic queue jobs and private temporary state. Process tests cover SIGKILL with an active lease, preserved cursor, stale endpoint replacement, persistent pause and SIGTERM. Native binary smoke checks exercise the CLI and writer exclusion. No filesystem scan or cleanup runs in these tests or in the current production worker.
