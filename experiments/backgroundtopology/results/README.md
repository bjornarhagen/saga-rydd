# Native background topology pilots

Two generated macOS arm64 pilots measured exact source `d9f0bf3a046e89eda616d118bdd7f77fb00a8b84` on 2026-10-09. Both used the [declared fast profile](../README.md), 4,096 primary files and a separate healthy root with 512 files in 33 directories. All 431 frozen source fingerprints and both native executable hashes were verified. An independent review matched sanitized summaries to raw receipts, configuration, terminal state and before/after SQLite body-and-identity oracles.

| Observation | Wide | Deep |
| --- | --- | --- |
| Primary file-bearing directories | 1 | 64 |
| Last sample elapsed time | 329.123 s | 599.646 s |
| Outcome at the 600-second sampling limit | Completed early | Partial |
| Primary file observations | 4,096 | 4,096 |
| Ready caches / total caches | 2 / 2 | 1 / 2 |
| Remaining scratch indicator | 0 | 1 |
| Dispatches | 262 | 464 |
| Worker CPU across two process instances | 2.725496 s | 4.829216 s |
| Maximum individual worker peak RSS | 27,738,112 B | 29,589,504 B |
| Maximum observed control latency | 10.969 ms | 12.235 ms |
| Metadata attempts reserved / observed | 6,684,740 / 12,190 | 12,779,588 / 50,297 |
| Unknown / outstanding reservations | 0 / 0 | 0 / 0 |
| Sampled database / WAL peaks | 638,976 / 4,251,872 B | 1,290,240 / 4,194,192 B |

Both pilots preserved unchanged source bodies and identities, pause without progress, actual SIGKILL, exact-state restart and healthy-root fairness. Final directory errors, skips and running jobs were zero. The wide pilot also verified exact cached logical/allocated totals, retired scratch and exactly two future root jobs at the fixed 24-hour due time.

The deep pilot completed source observations but left five saved reconciliation records and one unready primary cache. It did not verify final cached totals or two future daily jobs. Its original partial result remains archived.

A separately reviewed continuation of that exact generated state completed after 125.683 seconds of sampling, without resetting configuration, jobs, cursors, quotas or due times. Both caches were ready, scratch was empty, cached logical/allocated totals matched the independent oracles, and exactly two future daily jobs remained. It added 110 dispatches and no scanner API charges. Worker CPU was 1.135299 seconds, peak individual RSS was 27,131,904 bytes, and stop latency was 13.781 milliseconds. These costs cover the continuation worker only and remain separate from the initial pilot. Observer/oracle costs and unmeasured helper polling CPU are qualified in the [continuation aggregate](macos-arm64-deep-continuation-4096-20261009.json). The fixed continuation helper and raw receipts remain private; its hash provenance is retained in the aggregate.

The [wide aggregate](macos-arm64-wide-4096-20261009.json) and [initial deep aggregate](macos-arm64-deep-4096-20261009.json) retain exact hashes, separate generator/oracle/observer resources, API receipt crosschecks and measurement qualifications. Private paths, process IDs, job IDs, cursor payloads and source identities are omitted from all public aggregates.

Concurrent development can affect elapsed time. Full worker resource observations include separate graceful-stop time, while samples cover only their stated window. Scanner charges are not physical-I/O measurements, and peak memory is scoped to each individual process. No default tuning, complete million-entry inventory, physical/provider behavior, hourly resource target or representative soak is accepted by these pilots.
