# Developing Saga — Rydd

## Tools and environment

Use an existing Docker engine with Compose v2+ and `./scripts/dev`. The wrapper builds a pinned Go development image, runs a disposable container as your UID/GID, and retains compiler/module caches in a Docker volume. There are no continuously running development services.

The Go version is pinned in `Dockerfile` and `go.mod`. Update them together, including any CI pin that cannot be read from `go.mod`. `GOTOOLCHAIN=local` prevents implicit toolchain downloads. There are currently no third-party Go dependencies.

```sh
./scripts/dev check
./scripts/dev race
./scripts/dev build-all
```

`check` enforces formatting, runs `go vet ./...` and `go test ./...`, and builds the host-container CLI. The bootstrap has no feature tests yet; add meaningful tests with the scanner, scheduler, database and action executor. `build-all` currently uses `CGO_ENABLED=0`; revisit this deliberately when selecting a SQLite driver or adding native integrations.

For arbitrary Go commands:

```sh
./scripts/dev shell
# Inside the disposable container:
go version
```

New dependencies can be added deliberately inside that shell using `GOFLAGS= go get ...` followed by `GOFLAGS= go mod tidy`. Commit `go.mod` and `go.sum` together. Normal commands use read-only module mode.

## Native execution without installing Go

`./scripts/dev build-all` writes:

```text
dist/rydd-darwin-arm64
dist/rydd-darwin-amd64
dist/rydd-linux-arm64
dist/rydd-linux-amd64
```

Run the binary matching your host for native smoke tests, for example `./dist/rydd-darwin-arm64 --help` on Apple Silicon. Do not use the Linux container's `dist/rydd` on macOS. The bootstrap CLI only prints help/version; later scanner/service tests must use selected disposable roots and explicit setup.

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
