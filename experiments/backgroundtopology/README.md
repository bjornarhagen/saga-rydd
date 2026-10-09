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

The pilot accepts 256–4,096 primary files in multiples of 64 and a 30–600 second worker sampling window. Most generated files are empty; bounded sentinel bodies support independent content checks. Streaming SQLite oracles preserve actual kinds, ordinals, paths, bodies, device/inode identities and allocation observations. Requested named-database page cache totals at most 4 MiB; mmap is disabled and temporary sorting uses files. Oracle disk use and generation costs remain separate from worker measurements.

The declared fast profile uses one-second work and cadence, 100,000 entry inspections per second, 100,000 dispatches per saved UTC day and a large metadata reservation ceiling. Compact inventory is enabled; adaptive revisits and physical power probes are disabled. Real production SELF CPU feedback, state/WAL, metadata, cadence, fairness and control gates remain active. This profile does not establish production-default behavior.

The harness checks healthy-root progress, pause without saved progress, actual SIGKILL, restart with exact retained state and unchanged source oracles. It requests graceful stop at completion or the sampling limit and reaps its direct child. Full child CPU/RSS cover launch through Wait return, including stop grace. Cancellation does not claim descendant cleanup or interrupt a blocked native operation.

Private command receipts, source roots, oracles and state remain in the output directory. Keep them outside version control. Only reviewed aggregate evidence belongs in [results](results/README.md).

## Result limits

Worker CPU excludes generator, oracle and CLI observer costs, which are recorded separately. Generator/oracle RSS is the helper process lifetime high-water through each phase; observer RSS is the maximum individual observer child. Database/WAL peaks are sampled lower bounds. Scanner API charges do not measure physical I/O.

Binary hashes identify the supplied bytes. Build metadata records declared revision information or unknown values; it does not authenticate source provenance. Published measurements additionally require an independent frozen-source and binary audit.

Partial progress remains partial even when every source directory reached EOF. Cache calculations, reconciliation and scratch retirement must drain before complete cached totals and future daily jobs are accepted. The harness does not resume a previous fixture automatically or reset its quotas.

One million files need at least 7,813 source turns, 7,813 allocation-contribution turns and 7,813 scratch-retirement turns. At the minimum one-second cadence, those 23,439 turns already require about 6.51 hours before other costs. These small pilots and any later finite partial hour do not accept complete million-entry inventory, default hourly targets, provider/remount behavior, physical power or a representative soak.
