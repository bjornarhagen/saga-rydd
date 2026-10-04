# Production compact inventory scale fixture

P2-02b6a/b6b run the real Rydd CLI on newly generated disposable files. It uses the
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

For the million-identity comparison, use `-files 1000000 -cycles 2`. The same
100 packages then contain 10,000 files each. The CI workflow's manual
`scale_files` choice accepts `100000` or `1000000`; push/PR runs remain at
100,000. A manual million-file job has a 60-minute overall limit; each child
still has the same ten-minute timeout, so slow or stuck phases remain failures.

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

The recorded runs validate only their stated sizes and 100-package fixture
shape. They do not validate a single million-entry directory, background
scanning, physical disk reclamation, power/sleep, provider hydration or
real-project usefulness. See PROGRESS.md for rollout gates.

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

## Native Linux CI result (2026-10-02)

[CI run 37067698111](https://github.com/bjornarhagen/saga-rydd/actions/runs/37067698111)
ran commit `6034ffd` on linux/amd64. The 100,000-file fixture passed all assertions
in 3m26s including setup/build. [Sanitized measurements](results/linux-amd64-100000.json)
record each process and pre-kill snapshot.

Compact database size settled at 11.70 MiB after rescan and stayed there through
both disappearance/reappearance cycles, versus a 31.45 MiB detailed baseline
(about 63% smaller). The final disappearance left 2,958 reusable pages. Every
completed compact pass drained all work and scratch; report latency was 5–6 ms.
Maximum child RSS was 29.63 MiB and sampled WAL peaked at 4.35 MiB across the run.
Full compact passes took 25.1–27.1 s, versus 11.5 s for fresh detailed inventory.
These results show the same storage/maintenance tradeoff as macOS; different
hardware, filesystem identities and metadata mean the platforms' timings and
file sizes are not a controlled head-to-head comparison.

## Million-identity native macOS result (2026-10-04)

P2-02b6b used `-files 1000000 -cycles 2` with the same production CLI
implementation (unchanged since `69422da`). All exact-size, drained-work,
observed-phase SIGKILL/resume and repeated-cycle assertions passed. The
[sanitized measurements](results/macos-arm64-1000000.json) preserve all stages.

| Metric | 100,000 files | 1,000,000 files |
| --- | ---: | ---: |
| Maximum CLI child RSS across all stages | 29.36 MiB | 32.83 MiB |
| Compact database after rescan and both cycles | 13.11 MiB | 126.75 MiB |
| Detailed baseline database | 31.89 MiB | 314.78 MiB |
| Compact saved-state reduction | 59% | 60% |
| Completed compact report latency | 9–13 ms | 8–15 ms |
| Fresh recovery (excluding initial interrupted process) | 15.12 s | 185.35 s |
| Unchanged-tree rescan | 16.41 s | 210.65 s |
| Second disappearance | 1.88 s | 17.72 s |
| Second reappearance | 16.91 s | 190.16 s |
| Fresh detailed baseline | 5.92 s | 113.68 s |
| Sampled peak WAL, compact stages | 5.02 MiB | 7.34 MiB |
| Sampled peak WAL, detailed baseline | 4.13 MiB | 14.53 MiB |

The million-file run's measured child processes totaled 20.43 minutes, excluding
fixture creation, report commands and observer work between children. Its database
grew from 105.06 MiB after fresh recovery to 126.75 MiB after rescan, then stayed at
that size through shrink/restore and both full disappearance/reappearance cycles.
Disappearance freed 32,412 reusable pages. All completed stages drained their
queues and scratch, and all three interruption recoveries preserved exact totals.

At this fixture shape, 10× the identities increased peak child RSS by about 12%
and compact database size by about 9.7×; report latency remained small. Scan and
maintenance times grew more than 10× in some comparisons. These runs occurred on
different days without controlled cache state or background load. The 100-ms
observer also counts larger SQLite tables at the larger size, so timings include
size-dependent measurement overhead. Do not attribute the timing difference to a
specific production bottleneck without separate profiling. No timeout, schema,
resource default or product behavior was changed to make the test pass.

## Million-identity native Linux result (2026-10-04)

[Manual CI run 37211415803](https://github.com/bjornarhagen/saga-rydd/actions/runs/37211415803)
passed for `f44a94c` with `scale_files=1000000`. All four jobs passed: native
macOS checks, native Linux checks, Docker/cross-build checks and the million-file
Linux fixture. [Sanitized measurements](results/linux-amd64-1000000.json) confirm
the requested size, all three SIGKILL recoveries, exact sizes and empty queues and
scratch after completed stages.

| Metric | 100,000 files | 1,000,000 files |
| --- | ---: | ---: |
| Maximum CLI child RSS across all stages | 29.63 MiB | 29.83 MiB |
| Compact database after rescan and both cycles | 11.70 MiB | 113.90 MiB |
| Detailed baseline database | 31.45 MiB | 311.52 MiB |
| Compact saved-state reduction | 63% | 63% |
| Completed compact report latency | 5–6 ms | 5–7 ms |
| Fresh recovery (excluding initial interrupted process) | 25.48 s | 252.78 s |
| Unchanged-tree rescan | 27.11 s | 267.57 s |
| Second disappearance | 2.00 s | 20.11 s |
| Second reappearance | 25.07 s | 246.39 s |
| Fresh detailed baseline | 11.54 s | 121.50 s |
| Sampled peak WAL, compact stages | 4.23 MiB | 6.20 MiB |
| Sampled peak WAL, detailed baseline | 4.35 MiB | 5.50 MiB |

Measured child processes totaled 26.11 minutes. Compact state grew from
93.14 MiB after fresh recovery to 113.90 MiB after rescan, then stayed at that
size through the remaining cycles. Each full disappearance freed 29,121 reusable
pages. Peak child RSS increased by less than 1% from the smaller Linux fixture;
full-pass latency and database size grew roughly with the identity count.

Both platforms passed the million-file lifecycle without a production fix or
longer per-child timeout. This completes the bounded P2-02b6b measurement task.
It does not close the parent compact-mode rollout gate: directory shape, active
filesystem mutation, long-term growth, background budgets and owner feedback
still need their own evidence. The next product step is a controlled read-only
trial on an explicitly selected real folder, keeping project-derived data private.
