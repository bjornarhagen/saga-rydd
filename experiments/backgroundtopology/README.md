# Generated wide and deep background fixtures

This finite harness measures native experimental compact inventory on newly generated roots. It accepts an exact trusted Rydd executable and an exclusive output directory. It never accepts an existing source root or calls a service manager.

Build in Docker, then run the matching native executables. For macOS arm64:

```sh
./scripts/dev build-all
./scripts/dev shell -c 'GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/backgroundtopology-darwin-arm64 ./experiments/backgroundtopology'
mkdir -p .local
./dist/backgroundtopology-darwin-arm64 \
  -binary "$PWD/dist/rydd-darwin-arm64" \
  -output "$PWD/.local/topology-wide" \
  -shape wide -files 4096 -seconds 600
```

Use a new output directory for each invocation. Change both executable targets for another native platform. Cross-compilation does not verify native filesystem or process behavior.

| Shape | Primary root | Separate healthy root |
| --- | --- | --- |
| `wide` | One directory with all primary files | 512 files in 33 directories |
| `deep` | 64 file-bearing levels, including the selected root | 512 files in 33 directories |

The helper accepts 256–1,000,000 primary files in multiples of 64 and a 30–3,600 second worker sampling window. The prepared million-entry profiles select exactly 1,000,000 primary files, one shape per invocation, and a separate one-hour worker sample. Generation, oracle checks and stop grace are outside that sample.

Actual million-entry acceptance has not run. Each exact large trial remains held for specific owner approval. Source review, small generated fixtures, cross-builds and a passing capacity observation do not grant that approval.

| Fixed bound | Limit and scope |
| --- | --- |
| Available output-filesystem capacity | At least 16 GiB (17,179,869,184 bytes) before generation; failure or unknown capacity refuses generation and worker launch |
| Generation | 1,800-second cooperative deadline shared by both generated roots |
| Before oracles | 1,800-second cooperative deadline shared by both roots |
| After oracles and comparisons | 1,800-second cooperative deadline shared by both roots |
| Worker sampling | At most 3,600 seconds, with a separate 15-second graceful stop window |
| Whole helper | 9,600-second cooperative deadline |
| Retained samples | At most 721 scalar samples; variable job evidence is written to private pages |
| Saved job evidence | At most 129 admitted jobs, 64 KiB per cursor, 16 jobs per page and 2 MiB per serialized page |

The capacity check observes one filesystem. It reserves no space and cannot prevent a later allocation failure. Deadlines are cooperative; an entered native operation or direct-child reaping can outlast them. Retain any partial fixture and receipts after a failure.

Most generated files are empty; bounded sentinel bodies support independent content checks. Streaming SQLite oracles preserve actual kinds, ordinals, paths, bodies, device/inode identities and allocation observations. Requested named-database page cache totals at most 4 MiB; mmap is disabled and temporary sorting uses files. This is not a physical memory limit. Oracle disk use and generation costs remain separate from worker measurements.

The declared fast profile uses one-second work and cadence, 100,000 entry inspections per second, 100,000 dispatches per saved UTC day and a 1,099,511,627,776-attempt daily metadata reservation ceiling. Compact inventory is enabled. Adaptive revisits, API-attempt pacing and physical power probes are disabled. Real production SELF CPU feedback, state/WAL, metadata, cadence, fairness and control gates remain active. The configured inventory-state threshold is 1 GiB; it is not a strict physical disk cap. The optional CPU-session charge gate stays at its default-off setting. This profile does not establish production-default behavior.

The harness checks healthy-root progress, pause without saved progress, actual SIGKILL, restart with exact retained state and unchanged source oracles. It requests graceful stop at completion or the sampling limit and reaps its direct child. Full child CPU/RSS cover launch through Wait return, including stop grace. Cancellation does not claim descendant cleanup or interrupt a blocked native operation.

Private command receipts, source roots, oracles and state remain in the output directory. Keep them outside version control. Only reviewed aggregate evidence belongs in [results](results/README.md).

## Result limits

Worker CPU excludes generator, oracle and CLI observer costs, which are recorded separately. Generator/oracle RSS is the helper process lifetime high-water through each phase; observer RSS is the maximum individual observer child, not combined simultaneous memory. Database/WAL peaks are sampled lower bounds. Scanner API charges do not measure physical I/O. Report and control latencies are observations of individual CLI calls, not a guaranteed response time or hourly resource bound.

SHA-256 hashes identify the observed supplied Rydd and helper executable bytes. The helper requests build metadata from that exact supplied Rydd executable with `--version --json`, using a 16 KiB reply limit and a five-second cooperative deadline. Revision and clean/dirty status remain declared observations; absent VCS evidence stays unknown. They do not authenticate source provenance. Published measurements additionally require an independent frozen-source and binary audit.

Partial progress remains partial even when every source directory reached EOF. Cache calculations, reconciliation and scratch retirement must drain before complete cached totals and future daily jobs are accepted. The harness does not resume a previous fixture automatically or reset its quotas.

At 128 records per turn, one million files need at least 7,813 source turns. Even if the first turn starts immediately, the minimum one-second cadence separates those turns by at least 7,812 seconds (about 2.17 hours). A one-hour wide sample therefore remains partial before source EOF.

Allocation contribution and scratch retirement each need at least another 7,813 turns. These three passes total at least 23,439 turns, separated by at least 23,438 one-second cadence intervals (about 6.51 hours). That lower bound excludes directory work, the healthy root, controls, reconciliation, resource backoff and other overhead. Deep-directory grouping can add more turns. These small pilots and any later finite partial hour do not accept complete million-entry inventory, default hourly targets, provider/remount behavior, physical power or a representative soak.
