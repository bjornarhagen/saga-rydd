# Developing Saga — Rydd

## Tools and environment

Use an existing Docker engine with Compose v2+ and `./scripts/dev`. The wrapper builds a pinned Go development image, runs a disposable container as your UID/GID, and retains compiler/module caches in a Docker volume. There are no continuously running development services.

The Go version is pinned in `Dockerfile` and both `go.mod` files. Update them together, including any CI pin that cannot be read from `go.mod`. `GOTOOLCHAIN=local` prevents implicit toolchain downloads. Production direct dependencies are modernc SQLite, the pelletier TOML parser, and `golang.org/x/sys` for native locks and socket peer credentials. The isolated SQLite experiment also includes the alternative mattn driver. See [ADR 001](docs/decisions/001-sqlite-driver.md).

```sh
./scripts/dev check
./scripts/dev race
./scripts/dev build-all
```

`check` enforces formatting, runs `go vet ./...` and `go test ./...`, and builds the host-container CLI. Tests cover configuration, CLI initialization, migration/identity checks, read-only access, writer exclusion, WAL snapshots, path bytes, queue leases/cursors, persistent pause, control protocol and real process-kill recovery. Scanner/resource-budget/action tests must follow with those implementations. `build-all` uses `CGO_ENABLED=0`, supported by the selected SQLite driver and native lock/peer adapters.

Run `./scripts/dev sqlite-check` for both drivers' isolated correctness checks and `./scripts/dev sqlite-bench -rows 1000000` for a synthetic metadata comparison. The experiment is a nested module, so root `go test ./...` does not include it. Native CI runs it explicitly. Cache subdirectories are separated by UID to prevent ownership conflicts when a Compose volume is reused by different users.

For arbitrary Go commands:

```sh
./scripts/dev shell
# Inside the disposable container:
go version
```

For noninteractive commands, `./scripts/dev shell -c 'go version'` preserves the same UID and cache setup. Prefer this wrapper over calling Compose directly.

New dependencies can be added deliberately inside that shell using `GOFLAGS= go get ...` followed by `GOFLAGS= go mod tidy`. Commit `go.mod` and `go.sum` together. Normal commands use read-only module mode.

## Local candidate packages

```sh
./scripts/dev package review-v1
```

This command freshly builds the four fixed targets with Go 1.27.1, CGO disabled and trimmed paths. It writes `dist/candidates/review-v1/` with one flat archive per target and `SHA256SUMS`. Each archive contains `rydd` and bounded `BUILDINFO.json`. It uses the documented toolchain and existing explicit compiler/module caches, with no download fallback. Update the packaging toolchain pin when changing the repository's Go pins.

| Candidate target | Artifact format |
| --- | --- |
| macOS amd64 | Mach-O, tar.gz |
| macOS arm64 | Mach-O, tar.gz |
| Linux amd64 | ELF without interpreter/shared-library imports, tar.gz |
| Linux arm64 | ELF without interpreter/shared-library imports, tar.gz |

These are build targets. Native runtime acceptance covers only the platforms and architectures actually exercised; minimum OS/kernel/filesystem, signing, licensing, installed service and release gates remain open. This command installs no executable or service and publishes no release. Its result keeps `release_accepted: false`.

Use the [candidate and future release procedure](docs/release.md) to retain exact source/CI provenance, verify checksums and native version agreement, and inspect an uncertain publication.

Use a literal version label of 1–64 ASCII characters: an initial letter or digit, then letters, digits, dots, underscores or hyphens. Invalid labels and existing candidate destinations refuse before replacement. One fixed `dist/candidates/.staging` directory bounds interrupted build leftovers. Inspect a reported uncertain publication or leftover before another attempt; the command does not import old `dist/` binaries, overwrite candidates or automatically recover staging.

The archive's version is the fixed recipe input (`candidate_recipe`). Go omits linker flags from recorded build information under `-trimpath`, so structural inspection cannot establish an executed version label. Native fixtures separately run the matching executable. Its `--json --version` reply preserves the version scalar and adds `build_metadata_v1`, with `linked_value` and `executable_label_verified: true` when valid. Allowlisted VCS fields disclose clean, dirty or unknown provenance; they do not authenticate source. Private paths, dependency lists and raw build flags are omitted.

Ordinary checks skip the genuine four-target producer case. The dedicated native CI package jobs opt in with `RYDD_PACKAGE_BUILD_TEST=1` and explicit absolute `GOCACHE`/`GOMODCACHE`. The CI job first prepares the pinned project modules with an explicit dependency download; nested candidate compilers retain their offline contract. These jobs exercise actual compilation, archive/checksum/publication failures, cancellation and native version agreement. They also run the production command and execute its exact generated native archive. Locally, compiled native tests can use an explicit directory of four generated fixture binaries with `RYDD_PACKAGE_TEST_BINARIES`; this does not require host Go. Candidate timestamps/owners are fixed within archives, but reproducibility of compiled bytes across machines is not established.

## Native execution without installing Go

`./scripts/dev build-all` writes:

```text
dist/rydd-darwin-arm64
dist/rydd-darwin-amd64
dist/rydd-linux-arm64
dist/rydd-linux-amd64
```

Run the binary matching your host for native smoke tests, for example `./dist/rydd-darwin-arm64 --help` on Apple Silicon. Do not use the Linux container's `dist/rydd` on macOS. Use an explicit private directory for native fixture state, such as `./dist/rydd-darwin-arm64 --data-dir "$PWD/.local/native-demo" init --root "$PWD"`. Scanner tests must use selected disposable roots and explicit setup.

GitHub Actions runs checks and race detection on native macOS/Linux runners, plus Docker workflow validation and four-target cross-builds on Linux. Native CI does not replace physical laptop battery/sleep testing or the read-only soak.

Native CI tests Cargo layout compatibility with dependency-free generated debug/release library projects and preinstalled declared exact Cargo/Rust pairs 1.98.1/1.98.1 or 1.99.0/1.99.0. Both versions must match one declared pair before project creation; mixed or unknown pairs fail. The fixture requires `RYDD_TEST_CARGO_TOOL=1` and explicit `RYDD_TEST_CARGO_BIN`/`RYDD_TEST_RUSTC_BIN` paths to distinct direct executables in one Rustup toolchain installation. CI selects the fixed stable installation for its declared OS/architecture without invoking Rustup; see the [Ubuntu](https://github.com/actions/runner-images/blob/main/images/ubuntu/scripts/build/install-rust.sh) and [macOS](https://github.com/actions/runner-images/blob/main/images/macos/scripts/build/install-rust.sh) installation profiles. Missing tools or version drift fail this explicit gate; there is no install or download fallback. Fixture children use isolated Cargo/Rustup/temp paths, omit HOME and inherited configuration/wrappers, refuse ancestor Cargo configuration, build offline and bound output/time/cancellation. Saved reports are checked against independent filesystem metadata with producer projects offline. Normal Docker/development tests skip the producer case and require no Rust toolchain. This proves only the tested layout compatibility, not regeneration or cleanup safety.

The full race task allows fifteen minutes per test package. The accumulated instrumented inventory suite exceeded Go's default ten-minute package timeout on native Linux; the native CI job allows twenty-five minutes for compilation, checks, races and subsequent platform fixtures. These are test harness limits. Application step deadlines, consent windows and individual fixture assertions remain unchanged.

Run `./scripts/worker-smoke ./dist/rydd-darwin-arm64` on Apple Silicon (or the matching binary elsewhere) for a disposable native CLI lifecycle test. It initializes a private fixture, starts the worker, checks pause/resume/status and writer exclusion, stops it, and checks offline status. It leaves logs/state in a unique ignored `.local/worker-smoke.*` directory. Native CI runs this script too; Go tests also forcibly kill a worker process while a queue lease is held, then verify restart recovery and SIGTERM shutdown.

Required future coverage includes APFS/ext4 behavior, symlink/path changes, permission-denied directories, interrupted/disconnected volumes, huge directories, sparse/hardlinked files, crash recovery, policy revocation and resource budgets. Track evidence against phase gates in `PROGRESS.md`.

`./scripts/scanner-smoke ./dist/rydd-darwin-arm64` creates a tiny temporary fixture and verifies experimental inventory completion. It leaves logs/state in the OS temporary directory and stops its worker. Scanner tests cover bounded 301-file streaming, a 20-level tree, kill/restart completion, transaction rollback, stale leases, exclusions, symlinks, FIFO metadata, sparse files, unusual filenames, permissions and root replacement. Linux native CI separately runs a real same-filesystem bind-mount test under `sudo unshare --mount --propagation private`; its only mount is inside a temporary fixture. Ordinary dev tests need no mount privileges or Docker socket. Provider hydration, physical remounts, power/sleep and million-entry resource measurements remain phase-gate work.

## Fixtures and caches

- Place disposable local fixtures in ignored `.local/`; committed synthetic fixtures belong in `testdata/`.
- Never commit real scan databases, filenames from personal inventories, quarantine contents, logs or credentials.
- Development mounts the checkout only. Integration with a host Docker engine is a later, separately configured test; the default container gets no socket or credentials.
- `./scripts/dev cache-clear` removes the Compose project's Go cache and control-socket runtime volumes. Stop development workers first. Application state in the checkout is preserved.
- Containers stop after each command; `daemon` is a foreground command that stays running until stopped. No `docker compose up` is needed. A small runtime volume shares control sockets between development commands; all use the same UID and canonical state path.

## Changes and progress

Keep feature work linked to the stable task IDs in `PROGRESS.md`. Update the checklist and handoff with implementation, validation and remaining work in the same commit. CI passing is necessary but is not proof that a phase's unimplemented acceptance gates are complete.

The repository's working directory can have any name. The public identity is **Saga — Rydd**, the repository/module is `github.com/bjornarhagen/saga-rydd`, and the CLI is `rydd`.

For reproducible production compact/detailed storage measurements and forced scan/maintenance recovery, see [the compact scale fixture](experiments/compactscale/README.md). It generates its own temporary tree and emits sanitized JSON; native Linux CI exercises 100,000 identities and repeated retirement.
