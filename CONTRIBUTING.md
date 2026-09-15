# Developing Saga — Rydd

## Tools and environment

Use an existing Docker engine with Compose v2+ and `./scripts/dev`. The wrapper builds a pinned Go development image, runs a disposable container as your UID/GID, and retains compiler/module caches in a Docker volume. There are no continuously running development services.

The Go version is pinned in `Dockerfile` and both `go.mod` files. Update them together, including any CI pin that cannot be read from `go.mod`. `GOTOOLCHAIN=local` prevents implicit toolchain downloads. Production direct dependencies are modernc SQLite and the pelletier TOML parser; the isolated SQLite experiment also includes the alternative mattn driver. See [ADR 001](docs/decisions/001-sqlite-driver.md).

```sh
./scripts/dev check
./scripts/dev race
./scripts/dev build-all
```

`check` enforces formatting, runs `go vet ./...` and `go test ./...`, and builds the host-container CLI. Tests cover configuration, CLI initialization, migration/identity checks, read-only access, writer contention, WAL snapshots, path bytes and process-crash recovery. Scanner/scheduler/action tests must follow with those implementations. `build-all` uses `CGO_ENABLED=0`, supported by the selected SQLite driver; revisit this deliberately when adding native integrations.

Run `./scripts/dev sqlite-check` for both drivers' isolated correctness checks and `./scripts/dev sqlite-bench -rows 1000000` for a synthetic metadata comparison. The experiment is a nested module, so root `go test ./...` does not include it. Native CI runs it explicitly. Cache subdirectories are separated by UID to prevent ownership conflicts when a Compose volume is reused by different users.

For arbitrary Go commands:

```sh
./scripts/dev shell
# Inside the disposable container:
go version
```

For noninteractive commands, `./scripts/dev shell -c 'go version'` preserves the same UID and cache setup. Prefer this wrapper over calling Compose directly.

New dependencies can be added deliberately inside that shell using `GOFLAGS= go get ...` followed by `GOFLAGS= go mod tidy`. Commit `go.mod` and `go.sum` together. Normal commands use read-only module mode.

## Native execution without installing Go

`./scripts/dev build-all` writes:

```text
dist/rydd-darwin-arm64
dist/rydd-darwin-amd64
dist/rydd-linux-arm64
dist/rydd-linux-amd64
```

Run the binary matching your host for native smoke tests, for example `./dist/rydd-darwin-arm64 --help` on Apple Silicon. Do not use the Linux container's `dist/rydd` on macOS. Use an explicit private directory for native fixture state, such as `./dist/rydd-darwin-arm64 --data-dir "$PWD/.local/native-demo" init --root "$PWD"`. The current CLI does not scan; later scanner/service tests must use selected disposable roots and explicit setup.

GitHub Actions runs checks and race detection on native macOS/Linux runners, plus Docker workflow validation and four-target cross-builds on Linux. Native CI does not replace physical laptop battery/sleep testing or the read-only soak.

Required future coverage includes APFS/ext4 behavior, symlink/path changes, permission-denied directories, interrupted/disconnected volumes, huge directories, sparse/hardlinked files, crash recovery, policy revocation and resource budgets. Track evidence against phase gates in `PROGRESS.md`.

## Fixtures and caches

- Place disposable local fixtures in ignored `.local/`; committed synthetic fixtures belong in `testdata/`.
- Never commit real scan databases, filenames from personal inventories, quarantine contents, logs or credentials.
- Development mounts the checkout only. Integration with a host Docker engine is a later, separately configured test; the default container gets no socket or credentials.
- `./scripts/dev cache-clear` removes the Compose project's Go cache volume. It does not perform any product cleanup or touch application data.
- Containers stop after each command; no `docker compose up` is needed.

## Changes and progress

Keep feature work linked to the stable task IDs in `PROGRESS.md`. Update the checklist and handoff with implementation, validation and remaining work in the same commit. CI passing is necessary but is not proof that a phase's unimplemented acceptance gates are complete.

The repository's working directory can have any name. The public identity is **Saga — Rydd**, the repository/module is `github.com/bjornarhagen/saga-rydd`, and the CLI is `rydd`.
