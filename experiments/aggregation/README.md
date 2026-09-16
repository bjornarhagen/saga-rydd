# Compact generated-tree storage experiment

P2-02b1: executable evidence for the `node_modules` measurement design. This package is **not wired into Rydd** and never reads user files or migrates application state. It uses the existing Go module and SQLite driver; there are no new dependencies.

```sh
./scripts/dev check
./scripts/dev shell -c 'RYDD_AGGREGATE_EXPERIMENT=1 go test ./experiments/aggregation -count=1 -v'
```

Correctness tests run with the normal suite. The 100,000-file size comparison is explicitly opt-in. For native macOS arm64, build the test executable in Docker, then execute it on the host:

```sh
./scripts/dev shell -c 'GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -o dist/aggregation-darwin.test ./experiments/aggregation'
RYDD_AGGREGATE_EXPERIMENT=1 ./dist/aggregation-darwin.test -test.v
```

## What it demonstrates

- At most 128 file observations per commit.
- Logical totals per directory and exact device/inode evidence without filenames, full paths or per-file modification timestamps.
- Directory generations replace interrupted contributions, preserving completed siblings. Batch ordinals reject duplicate commits.
- Aggregate updates can participate in the same transaction as a scanner job lease/cursor update. Rollback tests model failure after the aggregate writes; this is not a process-kill or lease implementation test.
- Hardlinks are merged across directories, with conflicting size observations flagged.
- Obsolete inode rows can be retired in batches of at most 128.

`Measure` is deliberately a full-ledger SQL oracle for checking the experiment. **Do not copy it into the product's report path.** Production must update cached totals in budgeted, resumable steps. The prototype assumes the caller fences generations with a current job lease; it does not independently prevent a stale caller selecting an old generation. Unknown file identities are rejected here and still need explicit unknown/partial accounting in production.

## Comparison method

Generate 100 directories with 1,000 files each. Each synthetic logical size is 1,024 bytes and allocated size 4,096 bytes. Every tenth filename uses an inode shared across directories, giving 100,000 paths and 90,100 distinct identities. The full-record side mirrors production `entries`, its unique path index, parent index and size index. Both use WAL, FULL synchronous commits, a 4 MiB SQLite cache, 128-entry batches and the same SQLite driver.

Checkpoint/truncate the WAL before measuring database file lengths. The full-record side excludes unrelated roots, jobs and directory tables; the compact side contains its directory totals and inode ledger. This compares file-storage representations, not full app databases. Longer/shorter paths, inode values, directory fanout, mutation history and additional production indexes change the result. The compact ledger still has O(file identities) rows; it is not O(directories) storage.

See [ADR 002](../../docs/decisions/002-generated-tree-aggregation.md) for recorded results, limitations and production integration gates.
