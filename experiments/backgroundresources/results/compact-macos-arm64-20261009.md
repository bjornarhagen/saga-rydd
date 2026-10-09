# Generated compact background check on macOS

The generated check completed a compact `node_modules` size above the synchronous 10,000-identity fallback threshold. The independent oracle matched 10,003 saved file observations across 10,001 distinct identities: 10,003 logical bytes and 40,964,096 cached allocated bytes. All three allocation caches subsequently reached `done`, with no allocation or retirement scratch rows. These are saved size observations, not space reclaimed or current-file verification.

The exact tested compact candidate has 417 non-Markdown source fingerprints, source-map SHA-256 `e004c8ee7354dd194909f0a1cdb6ad57b6390cdeeb7b84db35180885c8410009`, and native macOS/arm64 executable SHA-256 `830141a515e13490836f087819302ffdbfb1c28dfb3b7a498e157e128b5c417e`. Measurements precede the later explicit-read pacing and kernel-I/O harness integration. The independently audited sanitized aggregate SHA-256 is `7d33048dd4b245b1dd829bd09c6d23802c20113b6923be3ad286198a4e01b7c0`; raw generated fixtures and logs remain private.

The fixture used two generated roots, compact mode, one-second work/cadence, 100,000 entry inspections per second and an initial daily cap of 512 work chunks. Adaptive timing, power probes and optional API pacing were disabled. Production SELF CPU feedback and the other resource/control gates remained enabled. Concurrent development could affect timing. This is a declared finite test profile; it does not accept production defaults.

| Observation | Initial 600-second window | Separate drained-state verification |
| --- | --- | --- |
| Worker lifetime | 600.819227834 seconds | 1.128818208 seconds |
| Worker process CPU | 6.516275 seconds | 0.026961 seconds |
| Native peak RSS | 29,868,032 bytes | 22,085,632 bytes |
| Largest control CLI elapsed time | 37.861708 ms | 52.263166 ms |
| Saved report CLI elapsed time | Unavailable | 10.915750 ms |
| New dispatch chunks | 411 | 0 |

The first window completed source traversal and all cached sizes, but one cache still needed scratch retirement. An unchanged-state continuation used exactly 11 further charged turns, reaching 422/512. Its verification helper then rejected the expected new future root-listing job after the worker had stopped and body/configuration checks had passed. Cleanup CPU, RSS and elapsed measurements were not retained and remain unknown. A later verification added no source calls or dispatches. It confirmed preserved prior jobs/cursors/due times and exactly two untouched future root listings, each due at its saved complete listing time plus 24 hours. No combined resource total is available.

Source accounting remained 5,505,058 reserved API attempts, 13,960 observed, 5,491,098 known unused and zero unknown/outstanding. Initial-window sampled database/WAL peaks were 1,257,472 and 4,358,992 bytes; these are lower bounds. Ninety distinct status-visible state observations ranged from 6–51 microseconds, with a sampled total of 2.201 ms; intervening observations may be absent. Observer costs are separate from worker accounting.

Earlier 288-cap windows remain partial observations. Their unchanged-state continuation correctly stopped at its daily cap, and its largest control CLI call took 1.408 seconds. That exhausted generated state was retained without resetting charges. Default tuning, hard control-latency guarantees, physical I/O/power, power-loss durability, representative hourly targets and soak acceptance remain open.
