# ADR 001 — pure-Go SQLite for the state foundation

Date: 2026-09-15. Decision: use `modernc.org/sqlite v1.59.0` in the application. [Native macOS/Linux and Docker CI passed](https://github.com/bjornarhagen/saga-rydd/actions/runs/34974105231) for implementation commit `491d932`; detailed evidence is tracked in `PROGRESS.md`.

## Why

Rydd needs small incremental writes, crash recovery, indexed inventory queries and a CLI that can read while a worker writes. The application should build for macOS/Linux arm64/amd64 from Docker without installing a C cross-toolchain on the developer's computer.

Compare the pure-Go [modernc driver](https://pkg.go.dev/modernc.org/sqlite) with the C-backed [mattn driver](https://github.com/mattn/go-sqlite3). The latter requires CGO and a target C compiler for working builds. Modernc avoids that dependency and meets the preliminary memory target in this workload. Its translated SQLite/runtime dependencies are substantial; this decision does not claim it is the smallest binary or fastest driver.

The comparison lives in an isolated Go module under `experiments/sqlite`. The application does not depend on the unselected mattn driver. Production direct dependencies are modernc SQLite and `pelletier/go-toml/v2` for strict TOML decoding; lockfiles pin their transitive dependencies.

## Experiment

Commands, using the existing Docker engine:

```sh
./scripts/dev sqlite-check
./scripts/dev sqlite-bench -rows 1000000
```

One process per candidate, one database connection, SQLite 3.53.4 in both, Go 1.27.1, Linux arm64 in Docker with 2 CPU / 2 GiB limits. Files live on the disposable container filesystem, not a host source bind mount. One million synthetic metadata rows are streamed in 256-row transactions, with a unique path index plus size and parent indexes. Each generated path is approximately 90 bytes. No real filesystem inventory is read.

Both candidates use WAL, `synchronous=FULL`, a 4 MiB page cache, 1 second busy timeout and automatic checkpointing every 1000 pages. After insertion, run a size-group aggregate and truncate-checkpoint before measuring main-database size. Peak RSS comes from the process's `getrusage`, including Go and SQLite allocations. Compilation is outside the measured process.

| Metric | modernc v1.59.0 | mattn v1.14.52 |
| --- | ---: | ---: |
| Peak RSS | 25,444,352 bytes (24.3 MiB) | 17,428,480 bytes (16.6 MiB) |
| Insert 1,000,000 entries | 24.00 s | 22.98 s |
| Size-group query | 69.62 ms | 26.96 ms |
| Main database after checkpoint | 264,577,024 bytes (252.3 MiB) | 264,577,024 bytes (252.3 MiB) |

These are single-run observations, not a general performance ranking. The larger query difference is immaterial to the intended slow scan workload at this scale. Both fit the provisional 100 MiB memory target. Actual paths, indexes, findings and job history will change database size; the database must have its own growth budget.

This is an unthrottled storage experiment. It does **not** establish the app's hourly CPU, physical disk traffic, battery impact or long-term memory use. Those remain P1/P5 acceptance work. No physical power-loss test was performed.

## Correctness and portability

For both candidates, the experiment checks:

- A reader holds a consistent snapshot while a writer commits; a new read sees the commit.
- Rolled-back writes do not appear.
- A subprocess commits one record, starts an uncommitted 32 MiB transaction, signals readiness and is forcibly killed. Reopening preserves the committed record, discards pending rows, and passes `integrity_check`.

Native macOS/Linux CI runs these checks for both drivers and a smaller 100,000-row comparison. The selected application's four-target `CGO_ENABLED=0` cross-build validates packaging without a C toolchain. A CGO-disabled mattn binary is not a working alternative: its stub driver errors at runtime, so a compile alone is insufficient evidence.

## State defaults and boundaries

- WAL with short transactions, one connection per store and a 4 MiB page cache per connection; memory mapping disabled.
- Connection settings are encoded in the DSN so replacing a connection retains foreign-key enforcement, durability and timeouts.
- `synchronous=FULL` for every write initially. SQLite documents FULL as durable for WAL commits; hardware/filesystem behavior still matters. [SQLite synchronous modes](https://sqlite.org/pragma.html#pragma_synchronous)
- Application ID, schema version and transactional migration ledger reject unrelated/newer databases. Migrations preserve existing state.
- Private application directories/files; URI-escaped database paths; read-only report connections.
- Automatic checkpoints plus passive manual checkpoint support. WAL size is observable with a 32 MiB backpressure signal; the future scheduler must enforce it. No claim of a hard WAL cap yet.
- Roots can be disabled without deleting inventory. Path columns use BLOBs to preserve non-UTF-8 filesystem names. Device/inode identifiers use text to avoid signed-integer overflow.
- Inventory is rebuildable. Future action/restore tables are durable and must never be cleared by inventory rebuild or migrations. No action schema or cleanup APIs are implemented prematurely.

Revisit the driver if native validation fails, the supported OS matrix changes, or measured memory/CPU costs exceed the app's resource targets. Keep the raw method reproducible rather than selecting by headline benchmark speed.
