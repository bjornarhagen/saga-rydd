# Local candidates and future releases

Rydd can produce local candidate packages. It has no accepted public release procedure yet. The current recipe always records `release_accepted: false`; a successful build or CI run does not change that decision.

This guide describes candidate review and the remaining release gates. It does not authorize a tag, upload, executable installation, service operation or cleanup. See [development setup](../CONTRIBUTING.md#local-candidate-packages), [the current evidence](../PROGRESS.md) and [the acceptance plan](../PLAN.md).

## Prepare an exact candidate

1. Freeze the source you intend to review. Record the full commit ID and whether the checkout is clean. Do not edit source during the build. If you review an uncommitted candidate, retain an exact source manifest and identify it as different from the committed CI input.
2. Check the CI run for that exact commit. Record the run URL, commit ID, job results and actual native runner OS/architecture. A green run for an earlier commit does not validate later edits. The [current workflow](../.github/workflows/ci.yml) has native checks and native candidate jobs on Linux and macOS, plus Linux compact-scale and Docker/cross-build jobs. It does not publish releases.
3. Run the local checks through the existing Docker development environment:

   ```sh
   ./scripts/dev check
   ./scripts/dev race
   ./scripts/dev build-all
   ```

   Container checks and cross-builds do not replace native execution. Ordinary checks skip the genuine four-target packaging producer test. The dedicated native candidate jobs explicitly run that test and the production packaging command.
4. Choose a new literal candidate label, then build it:

   ```sh
   ./scripts/dev package review-v1
   ```

   Labels contain 1–64 ASCII characters. The first character must be a letter or digit. Later characters may also be `.`, `_` or `-`. A label does not create a Git tag or establish semantic version compatibility.

The [fixed recipe](../internal/packaging/package.go) freshly builds `cmd/rydd` with Go 1.27.1, `CGO_ENABLED=0`, trimmed paths and the selected label. It uses explicit compiler/module caches and has no nested download fallback. Keep the Docker, module, CI and packaging toolchain pins consistent. Do not install host Go to follow this procedure.

The output is `dist/candidates/review-v1/`, containing four archives and a sorted `SHA256SUMS` file:

| Build target | Archive |
| --- | --- |
| macOS amd64 | `rydd-review-v1-darwin-amd64.tar.gz` |
| macOS arm64 | `rydd-review-v1-darwin-arm64.tar.gz` |
| Linux amd64 | `rydd-review-v1-linux-amd64.tar.gz` |
| Linux arm64 | `rydd-review-v1-linux-arm64.tar.gz` |

Each archive contains only `rydd` and `BUILDINFO.json`. Existing `dist` binaries are not imported. Archive timestamps, owners and member order are fixed. This makes archive construction deterministic for the same executable and metadata inputs; reproducibility of compiled bytes across machines remains unproven.

## Verify the candidate

1. Preserve the packaging result and verify every archive checksum:

   ```sh
   (cd dist/candidates/review-v1 && shasum -a 256 -c SHA256SUMS)
   ```

   Compare these checksums with the result's archive records. Checksums detect byte differences; they do not authenticate the source or distributor.
2. Extract the archive matching your native OS and architecture into a new, empty review directory. Do not merge it into an installed executable directory. Inspect `BUILDINFO.json`: the contract must be `candidate_package_v1`, the artifact kind must be `local_candidate`, and the target, toolchain, version and declared VCS context must match the candidate record. All four archives must declare the same revision and source status.
3. From that review directory, inspect the exact extracted executable:

   ```sh
   shasum -a 256 rydd
   ./rydd --version
   ./rydd --json --version
   ```

   The executable hash must match `binary_sha256` in `BUILDINFO.json`. For this example, the human version must be `rydd review-v1 (experimental inventory)`. The JSON reply must report version `review-v1`, contract `build_metadata_v1`, the matching native target and toolchain, CGO disabled, `version_source: "linked_value"` and `executable_label_verified: true`.
4. Record native test results against the exact executable hash. Keep OS, architecture, filesystem and test scope explicit. Run generated fixture procedures from [CONTRIBUTING](../CONTRIBUTING.md#native-execution-without-installing-go); candidate review itself requires no personal-folder scan or installed-service action.

The archive's version is recorded as `candidate_recipe`. Under `-trimpath`, structural Go build information does not retain the linker flags needed to prove the executed label. Native execution supplies that separate check. The [build metadata contract](../internal/buildmetadata/metadata.go) reports declared clean, dirty or unknown VCS observations; it does not authenticate source or detect every concurrent edit.

Four successful build targets do not prove four native runtime targets. Hosted runner labels can change. Retain the actual profiles exercised by the matching CI run; minimum macOS, Linux kernel and filesystem support remain unconfirmed. `BUILDINFO.json` keeps `source_authenticated`, `native_compatibility_verified`, `distribution_signed` and `release_accepted` false even after these review steps.

## Handle an interrupted or failed build

Published candidate destinations are immutable to the packaging command: an existing destination refuses replacement. One fixed `dist/candidates/.staging` directory bounds cooperating interrupted-build leftovers. An existing staging directory also refuses another attempt, including one with a different label.

A failed or canceled reply does not prove that no candidate was saved. Preserve the result and error. Inspect the exact candidate destination and `.staging`, together with `publication_attempted`, `rename_completed` and `sync_completed`. A rename can succeed before a later cancellation or parent-directory sync failure. An uncertain rename can leave input staging or a destination that needs inspection.

The command does not recover staging, import partial archives or overwrite an existing candidate. Do not delete an unexplained leftover or choose another label to bypass the refusal. Establish what was published before deciding how to handle those exact generated artifacts. A missing final sync confirmation remains uncertainty, not release acceptance.

## Technical platform floors

These requirements constrain a future support choice. They do not declare a tested release matrix.

| Candidate targets | Toolchain and CPU requirement | Source-access requirement |
| --- | --- | --- |
| macOS amd64 / arm64 | macOS 13 or newer; AMD64 `v1` / ARMv8.0 | Scanner accepts APFS and HFS filesystem scopes |
| Linux amd64 / arm64 | AMD64 `v1` / ARMv8.0; architecture-specific kernel support also matters | Scanner and live hashing require working `STATX_MNT_ID` observations |

The pinned Go 1.27.1 toolchain requires macOS 13. Its linker defaults to a 13.0.0 deployment target. Separate structural inspection of all four frozen P1-06b10 core artifacts verified that deployment target in both Darwin binaries and found neither an interpreter nor dynamic-link program header in either Linux binary. Those Linux artifacts require no installed dynamic glibc, musl or SQLite library. This was file-header inspection, not execution on minimum platforms. See the [Go platform requirements](https://go.dev/wiki/MinimumRequirements) and [Go 1.27 linker changes](https://go.dev/doc/go1.27#linker).

Linux introduced `STATX_MNT_ID` in upstream 5.8; it is absent from the 5.7 interface. The product requires the capability and refuses missing observations rather than falling back to weaker mount identity. Backports, filesystem support and syscall restrictions can change availability, so a kernel version alone is insufficient. See the [5.8 interface](https://raw.githubusercontent.com/torvalds/linux/v5.8/include/uapi/linux/stat.h), [5.7 interface](https://raw.githubusercontent.com/torvalds/linux/v5.7/include/uapi/linux/stat.h) and [product checks](../internal/inventory/platform_linux.go).

The pinned [CGo-free SQLite driver](https://pkg.go.dev/modernc.org/sqlite@v1.59.0) supports all four build targets. Its `modernc.org/libc` dependency is translated Go code, not an installed host-libc requirement. Optional power and generated measurement observations retain their unsupported/unknown results; they do not establish the ordinary product's support matrix. Service publication and clone tests also depend on actual platform/filesystem capabilities. Choose the release-tested minimums and test each supported OS/architecture separately before accepting P5-01.

## Gates before a public release

| Gate | Required decision or evidence | Current limit |
| --- | --- | --- |
| Source and CI provenance | Reviewed exact source, passing required jobs, recorded artifact hashes and native agreement | Build metadata is an observation, not authentication |
| Supported platforms | Declared OS/kernel/filesystem minimums and native evidence for each supported OS/architecture | Cross-builds and finite hosted-runner fixtures are insufficient |
| License | Explicit license selection and dependency/distribution review | [Pinned root notices and a bounded four-target notice map](dependency-notices.md) are inventoried; project choice, complete retained-code provenance and notice packaging remain open |
| Signing and distribution | Approved signing, trust and publication procedure | Distribution signing is unverified; no release publisher exists |
| Installation and services | Reviewed installation/upgrade/recovery instructions and actual supported installed-manager acceptance | Generated artifact/manager fixtures do not prove owner installation |
| Cleanup scope | Explicit product scope and any required source-action safety, consent and recovery acceptance | Cleanup is unavailable; see [ADR 003](decisions/003-source-namespace-boundary.md) |
| Resource and soak acceptance | Representative native read-only soak, physical sleep/power and required resource evidence | Finite generated runs do not satisfy those gates |

Resolve these gates against the current plan before choosing a release label or approving publication. Preserve an evidence record with the exact source, CI run, archive/executable checksums, native profiles, test results and remaining limitations. Keep private paths, inventories, credentials and fixture logs out of public release notes.

After acceptance, define and review the tag, signing and upload steps as a separate change. Do not edit candidate metadata to manufacture acceptance. The current candidate recipe remains `release_accepted: false` and provides no automatic public release step.
