# Finite background-resource fixture

P5-02a measures one generated experimental worker run with contract `generated_background_resources_v1`. Supply an exact trusted native Rydd executable. The harness creates its own private roots, configuration, state and logs; it accepts no existing scan root and calls no service manager.

Build in Docker, then run the generated fixture natively. This macOS arm64 example uses the project's pinned toolchain:

```sh
./scripts/dev build-all
./scripts/dev shell -c 'GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/backgroundresources-darwin-arm64 ./experiments/backgroundresources'
mkdir -p .local
./dist/backgroundresources-darwin-arm64 -binary ./dist/rydd-darwin-arm64 -profile defaults_no_power -seconds 60 > .local/backgroundresources.json
```

For another supported native target, change both executable suffixes and the build's `GOOS`/`GOARCH`. Cross-compilation alone does not establish native behavior.

| Profile | Scan settings | Purpose |
| --- | --- | --- |
| `defaults_no_power` | Production defaults with `pause_on_battery = false` | Finite default-paced measurements without power probes |
| `short_fixture` | One-second work, five-second cadence, power probes disabled | Several generated turns and control checks in a short run |

`-seconds` accepts 30–3,600, `-roots` accepts 1–32 and `-files-per-root` accepts 1–4,096. The total is at most 32,768 deterministic 512-byte files. Five-second sampling is bounded to 750 records, including transition checks. Each invocation is independent; it cannot resume an earlier fixture.

The harness pauses near the midpoint, waits for active work and accounting to settle, checks a saved report, verifies unchanged paused progress, then resumes. It requests graceful stop and reaps its direct worker child once. Timeout, parent cancellation or output overflow can signal only a live owned process group. An already reaped leader is never signaled by numeric group ID; this is not a guarantee of cleanup for an arbitrary forking executable.

Successful JSON contains aggregate observations without fixture paths, source tokens or worker instances. Private command output and generated scopes remain in the temporary directory named on stderr. Any assertion, command, private-log or output failure returns an error rather than a successful partial measurement. Do not add the retained fixture to version control.

## Measurement limits

Native `ProcessState`/`getrusage` measurements report worker user/system CPU and peak RSS after `Wait` returns. Launch-to-Wait-return elapsed time includes startup and output draining. CLI observer CPU is reported separately. Parent fixture generation, independent body hashing and sampling overhead are excluded from worker CPU/RSS; observer commands can still perturb worker timing.

State/WAL peaks are sampled lower bounds. Scanner counters count declared API attempts, not physical disk operations. Physical-read bytes and system wakeups remain null/unknown. The file-body digest checks unchanged generated contents; those deliberate harness reads are separate from scanner activity.

No hourly extrapolation, representative-machine tuning, physical power acceptance or soak acceptance follows from a finite run. The full plan still requires measured hourly targets and a representative native macOS/Linux soak.
