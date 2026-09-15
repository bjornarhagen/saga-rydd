# SQLite comparison

This separate Go module keeps the alternative C-backed driver out of Rydd's runtime dependencies.

From the repository root:

```sh
./scripts/dev sqlite-check
./scripts/dev sqlite-bench -rows 1000000
```

The default build selects modernc. `-tags sqlite_cgo` selects mattn. The task wrapper runs both with the same workload. Data is synthetic and temporary, with bounded streaming inserts. It does not inspect any user files.

See [ADR 001](../../docs/decisions/001-sqlite-driver.md) for the method, results, limitations and driver decision. CI runs correctness checks and a 100,000-row comparison on macOS/Linux. These are database experiments, not scanner resource benchmarks.
