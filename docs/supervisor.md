# Run the foreground worker under a supervisor

Rydd can run as a foreground child of a supervisor that you select separately, including when a systemd user manager is unavailable. `daemon` does not detach or install a supervisor. This guide provides the invocation scope; it does not select a manager or provide manager configuration.

1. Select an existing Rydd executable, a private instance on a local filesystem and the account that owns its configuration and state. Follow [first use](getting-started.md#configure-a-worker-separately) if the instance does not yet exist. Validate its configuration separately:

   ```sh
   '/path/to/rydd' '--data-dir' '/path/to/private/state' 'config' 'check'
   ```

2. Give your selected supervisor the executable and arguments as separate fields:

   | Field | Value |
   | --- | --- |
   | Executable | `/path/to/rydd` |
   | Arguments | `["--data-dir", "/path/to/private/state", "daemon"]` |
   | Account | The same account used for configuration, state and control commands |
   | Environment | The same `HOME`; the same `RYDD_RUNTIME_DIR` choice for worker and controls |

   Pass each argument literally through the manager's argument interface. Do not put the entire command in its executable field. The shell equivalent is:

   ```sh
   '/path/to/rydd' '--data-dir' '/path/to/private/state' 'daemon'
   ```

   `--data-dir` must precede the command. It selects `config.toml` and state in that directory instead of the platform defaults. Keep foreground stdout, stderr and the actual process exit result available for diagnosis. Choose the supervisor's restart policy separately: a Rydd stop request does not disable that policy.

3. Choose the worker mode explicitly. Plain `daemon` stays idle: it registers no scanner and does not scan configured roots. It still owns the state writer, can initialize or migrate its database, synchronizes roots and recovers saved jobs and reservations. To select experimental configured-root metadata scanning on that start, append exactly one `--experimental-scan` argument. Keep that choice explicit on later starts; begin with disposable folders and review the [worker limits](worker.md). No cleanup runs.

   Use the same executable, account, data directory and runtime environment for a separate status call:

   ```sh
   '/path/to/rydd' '--data-dir' '/path/to/private/state' 'status' '--json'
   ```

   The daemon uses foreground text output. Its `Worker ready` line follows startup and control-listener setup; it does not prove root availability, scan progress or supervisor health. Status separates saved state from a live control response. A successful saved report or a worker state of `unknown` does not establish readiness. Process launch and manager configuration alone do not establish readiness either.

4. Before an intentional shutdown or configuration change, prevent the supervisor's restart policy from immediately starting another writer. Request graceful shutdown through the same instance:

   ```sh
   '/path/to/rydd' '--data-dir' '/path/to/private/state' 'stop'
   ```

   Wait for the foreground process to exit before editing configuration or opening another writer. A stop acknowledgment only confirms the request. SIGINT and SIGTERM use the same cooperative shutdown path. An active handler that does not stop within five seconds causes an error; this is not a hard deadline for every native call. Preserve the exit result and stderr instead of assuming signal shutdown always returns zero. CLI exit codes are 0 for success, 1 for an operation failure and 2 for invalid usage.

Configuration is captured at startup and rechecked after the writer lock is acquired. A changed capture refuses startup; changes made to a running worker's configuration are not a hot reload. Stop it before an intentional edit, then start it explicitly with the chosen mode. `pause` and `resume` use the same instance scope; pause is saved before acknowledgment and survives restart, while a current cooperative chunk can still be draining.

By default the control-runtime parent is `/tmp`. If you override `RYDD_RUNTIME_DIR`, use the same short absolute value for the worker and its clients; the complete socket path must fit within 100 bytes. Clients verify the same user. A different runtime environment can make the selected worker appear unavailable.

Only one writer can own the instance. Keep `writer.lock`, committed jobs, cursors, pause state and budget history; do not remove the lock or clear the queue to force a restart. The kernel releases writer ownership when the process exits. Startup recovery retains committed progress, and an interrupted directory listing can restart. Saved state and logs contain private paths. Keep them private and outside version control.

See [ownership and recovery](worker.md#ownership-and-controls) and the [CLI contract](cli.md). Supervisor examples do not verify installed-manager, login/logout or native runtime behavior. Network-filesystem semantics, physical durability, full resource targets and representative soak acceptance remain open.
