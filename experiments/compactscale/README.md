# Production compact inventory scale fixture

P2-02b6a runs the real Rydd CLI on newly generated disposable files. It uses the
root Go module and existing SQLite driver. It accepts a binary and fixture size,
never an existing scan root. Fixtures remain in the OS temporary directory for
inspection; the runner moves its own directories and does not delete originals.

Build with Docker, then run on the native host (Apple Silicon example):

```sh
./scripts/dev build-all
./scripts/dev shell -c 'GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/compactscale-darwin-arm64 ./experiments/compactscale'
./dist/compactscale-darwin-arm64 -binary ./dist/rydd-darwin-arm64 -files 100000 -cycles 2 > dist/compactscale.json
```

Native Linux CI builds and runs the same runner and CLI directly on its host.
For a quick harness check, use `-files 1000 -cycles 1`. A Linux container run is
useful validation but is not native Linux evidence.

## Method and assertions

- Generate 100 packages with a total of 100,000 unique regular-file identities
  under one `node_modules`, plus a three-byte `package.json`. Every dependency
  file has logical length 17 bytes; 100 have one written byte and the remainder
  are sparse. Expected allocation is independently summed from native `stat`
  blocks during creation. This fixture is metadata-heavy, not a realistic
  dependency-content or reclaimable-space estimate.
- Force SIGKILL after committed compact identities while inventory is pending;
  resume the CLI and check saved totals and that all maintenance drains.
- Rescan, move half the packages outside the root, interrupt after saved identity
  removal has begun, then resume and check reduced totals. Restore the packages,
  interrupt with a nonempty allocation scratch map, resume and check totals.
- Repeat whole-tree disappearance/reappearance twice. Verify original fixture
  files still exist after saved-record retirement. Each completed compact pass
  must have exact logical/allocated totals, recorded-complete coverage, zero
  excluded rows, no fallback identity walk and no remaining queue/scratch work.
  Nonempty compact trees must use cached allocated totals. Resume commands must
  report `resume`; drained subsequent scans must report `new_pass`.
- Build a separate ordinary detailed inventory of the same unchanged files as
  the production storage baseline. Its per-file row count is checked; its root
  report is intentionally not asserted complete because the 10,000-row report
  coverage cap still applies.

JSON contains platform, fixture parameters and per-process elapsed time, child
peak RSS from `wait4` resource usage (macOS bytes; Linux KiB converted to bytes),
report subprocess latency, scan/maintenance batch counts, database/WAL sampled
high-water marks, sampled queue high-water marks, pre-kill and final database
counts, page count and reusable pages. Paths are excluded from JSON. Diagnostic
stderr names the generated temporary directory for local inspection.

Sampling every 100 ms opens a short read-only SQLite connection and reads counts
in one statement. It also stats the database and WAL. These reads impose some
measurement overhead and may briefly hold a WAL snapshot. Sampled peaks are lower
bounds, not strict resource maxima. RSS excludes the fixture runner; report
latency is one cold-process invocation per completed pass, not a percentile.
Allocation backlog counts unfinished scopes, and retirement backlog counts work
records, not remaining file rows; final compact identities and scratch counts
provide additional context. Each child has a ten-minute timeout. An intended
interruption that is not observed is a failure, not silently skipped evidence.

This does not validate a million-entry tree, a single huge directory, background
scanning, memory scaling across sizes, physical disk reclamation, power/sleep,
provider hydration or real-project usefulness. See PROGRESS.md for rollout gates.

## Native macOS result (2026-10-02)

CLI implementation: `69422da`; darwin/arm64, 100,000 files, two cycles. Full
[sanitized measurements](results/macos-arm64-100000.json) accompany this table.

| Operation | Elapsed | Database after pass | Reusable pages | Report latency |
| --- | ---: | ---: | ---: | ---: |
| Fresh recovery after scan SIGKILL | 15.12 s | 10.84 MiB | 1,072 | 10 ms |
| Rescan | 16.41 s | 13.11 MiB | 1,611 | 12 ms |
| Shrink: interrupted + resumed | 10.68 s | 13.11 MiB | 2,470 | 9 ms |
| Restore: interrupted + resumed | 15.93 s | 13.11 MiB | 1,628 | 10 ms |
| Whole-tree disappearance, second cycle | 1.88 s | 13.11 MiB | 3,318 | 11 ms |
| Reappearance, second cycle | 16.91 s | 13.11 MiB | 1,653 | 13 ms |
| Fresh detailed baseline | 5.92 s | 31.89 MiB | 0 | Not measured |

Maximum CLI child RSS was 29.36 MiB; sampled WAL reached 5.02 MiB. All completed
compact passes drained inventory, retirement, allocation and identity scratch.
Repeated-cycle database size plateaued at 13.11 MiB over these two cycles, about
59% smaller than the detailed baseline (initial compact state was about 66%
smaller). This is below the isolated prototype's savings, and compact full passes
were slower because they also perform two scoped reductions and scratch cleanup.
The scans are unpaced (`--now`); they do not establish background resource budgets.
