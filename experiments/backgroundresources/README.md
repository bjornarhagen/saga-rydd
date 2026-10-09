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

The harness pauses near the midpoint, waits for active work and accounting to settle, checks a saved report, verifies unchanged paused progress, then resumes. It requests graceful stop and reaps its direct worker child once. Timeout, parent cancellation or output overflow uses Go's guarded direct-child cancellation. Bounded output-pipe waits do not terminate descendants. The supplied Rydd worker creates no helper children; arbitrary forking executables and descendant cleanup are outside this fixture's scope.

Successful JSON contains aggregate observations without fixture paths, source tokens or worker instances. Private command output and generated scopes remain in the temporary directory named on stderr. Any assertion, command, private-log or output failure returns an error rather than a successful partial measurement. Do not add the retained fixture to version control.

## Measurement limits

Native `ProcessState`/`getrusage` measurements report worker user/system CPU and peak RSS after `Wait` returns. Launch-to-Wait-return elapsed time includes startup and output draining. CLI observer CPU is reported separately. Parent fixture generation, independent body hashing and sampling overhead are excluded from worker CPU/RSS; observer commands can still perturb worker timing.

State/WAL peaks are sampled lower bounds. Scanner counters count declared API attempts, not physical disk operations. Physical-read bytes and system wakeups remain null/unknown. The file-body digest checks unchanged generated contents; those deliberate harness reads are separate from scanner activity.

### Sampled Linux kernel I/O

`worker_kernel_io` adds the `linux_proc_io_samples_v1` contract on Linux. It observes only the harness's direct worker process and accounting for children that process has waited for. The current Rydd worker creates no children. macOS reports unsupported observations without probing a process filesystem.

| Counter | Meaning |
| --- | --- |
| `rchar`, `wchar` | Bytes accounted to read/write calls, including cached reads and pipes |
| `syscr`, `syscw` | Accounted read/write call counts |
| `read_bytes` | Storage-layer read accounting |
| `write_bytes` | Storage-layer write accounting, including page dirtying |
| `cancelled_write_bytes` | Cancelled write accounting; never subtracted from writes |

These counters do not measure device traffic, scanner budgets or system wakeups. Samples are sequential, can cover waited-child work, and are not atomic snapshots. `sampled_delta` covers only the observed prefix between successful samples. A zero counter is an observed value; an unavailable counter is null. Regression invalidates the delta and disables further positive observations. The summary never claims full-lifetime coverage, including when the final post-reap sample is unavailable. See the [Linux proc documentation](https://www.kernel.org/doc/html/latest/filesystems/proc.html) for the accounting semantics.

The harness binds the worker before starting its sole `Wait`, verifies an aligned process namespace, and retains one no-follow proc directory descriptor. It never reopens a numeric PID. Missing kernel support or an unverifiable scope produces a qualified unavailable result. Setup retains at most three descriptors temporarily; sampling retains at most two. Setup or sample collection uses at most 28 filesystem calls, or 29 including a refused/terminal retained-descriptor close. The ordinary final close is separate and occurs once. Fixed attribute limits and four read calls per attribute bound copied data: at most 8,205 bytes during setup and 9,219 during a sample. A cooperative two-second timeout does not interrupt an already blocked kernel call. Observer work and these probes remain outside worker CPU/I/O accounting.

No hourly extrapolation, representative-machine tuning, physical power acceptance or soak acceptance follows from a finite run. The full plan still requires measured hourly targets and a representative native macOS/Linux soak.

## Recorded native observations

The [generated macOS one-hour observation](results/README.md) records exact source and executable digests, worker and observer costs, qualified state/API/latency measurements, and open acceptance gates. It is a finite observation of the stated source, not a default-tuning or representative-soak result.
