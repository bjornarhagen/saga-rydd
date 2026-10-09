# ADR 004 — Docker builder/cache metadata feasibility

- Date: 2026-10-09
- Status: feasibility verified; Engine-embedded reporting implemented, published acceptance pending
- Scope: P2-04b0, after finite image/container metadata discovery

## Result

A finite HTTP request can observe the selected Engine's embedded build cache. This supports an independently completable read-only reporting leaf under the existing continuation authorization. It does not identify an independently named Buildx builder or its generation, so it cannot complete P2-04's exact-builder acceptance requirement or claim `builder_pinned`.

The query can cause the trusted daemon to calculate and persist internal size accounting. The existing P2-04a contract forbids mutation requests and saved Rydd-state changes; it does not promise zero internal daemon writes. These effects require disclosure and clear client-versus-daemon resource limits, not another development approval. Implement a separate explicit Engine-cache reporting mode, preserving the current image/container mode and its request allowlist. This document itself adds no request, dependency, executable operation or installed-context trial.

## Examined profiles

| Profile | Observed interface | Limit for Rydd |
| --- | --- | --- |
| Engine-embedded builder | Engine API 1.44 `GET /system/df?type=build-cache` | Bounded client report is feasible; independent builder identity is absent and internal accounting effects must be disclosed. |
| Buildx `docker` driver | BuildKit gRPC through Engine `/grpc` HTTP upgrade | Different transport and protocol; not the existing finite HTTP implementation. |
| Buildx `docker-container` driver | Container exec running `buildctl dial-stdio` | Requires a helper process, even when the container already exists. Excluded from the current profile. |
| Kubernetes, remote or cloud builders | Driver-specific nodes and transports | Outside the selected canonical Unix Engine profile. No fallback or node discovery is proposed. |
| Docker volumes | Volume-driver-dependent discovery/usage | Remains outside this leaf. Filtering build cache must not become unfiltered system disk usage. |

Docker documents the default embedded builder as bound to its daemon and context. A selected Buildx builder can instead be a separate instance. A context name or the word `default` is therefore insufficient to establish which named builder was selected. [Docker builder model](https://docs.docker.com/build/builders/)

Versioned Buildx source confirms that the `docker` driver dials Engine `/grpc`. The container driver's dial path creates and attaches a container exec for its helper. Its bootstrap path can create/start a container and pull an image. This does not mean every inspect command bootstraps: the reviewed disk-usage command loads nodes and calls their driver clients without an explicit bootstrap call. Those delegated clients are still outside Rydd's four-request allowlist. [Buildx v0.13.0 Docker driver](https://github.com/docker/buildx/blob/v0.13.0/driver/docker/driver.go), [container driver](https://github.com/docker/buildx/blob/v0.13.0/driver/docker-container/driver.go), [disk-usage command](https://github.com/docker/buildx/blob/v0.13.0/commands/diskusage.go)

## Engine HTTP candidate and its limits

API 1.44 supports a repeated `type` selector on system disk usage, including `build-cache`. Its response contains cache IDs, type, usage flags, dates and reported sizes, but no builder-instance or worker-generation identity. The request has no documented pagination or result limit. [Moby v25.0.0 API 1.44 schema](https://github.com/moby/moby/blob/v25.0.0/docs/api/v1.44.yaml)

The examined v25.0.0 handler honors the selector at API 1.42 and later. Requesting only build cache skips the separate image/container/volume `SystemDiskUsage` branch and invokes the embedded builder's disk-usage method. That method converts records from its BuildKit controller; it does not select a named Buildx instance. On success the handler normalizes a nil cache list to an explicit empty JSON array. A missing/null `BuildCache` field therefore does not supply the declared successful-empty evidence. This is evidence for the examined version, not every daemon implementation advertising API 1.44. [System handler](https://github.com/moby/moby/blob/v25.0.0/api/server/router/system/system_routes.go), [embedded builder](https://github.com/moby/moby/blob/v25.0.0/builder/builder-next/builder.go)

The pinned daemon's vendored BuildKit enumerates its cache and calculates unknown sizes. The size path can invoke snapshotter usage and content-store metadata operations, then queue and commit the computed size. Dropping size fields from Rydd's output would not prevent that work or those accounting writes. This is not a prune, build or image pull, but it prevents a promise that all daemon records remain unchanged. [Vendored cache manager](https://github.com/moby/moby/blob/v25.0.0/vendor/github.com/moby/buildkit/cache/manager.go), [size accounting](https://github.com/moby/moby/blob/v25.0.0/vendor/github.com/moby/buildkit/cache/refs.go)

Client deadlines and response caps cannot bound the daemon's cache enumeration, snapshotter work, allocation or accounting writes. Closing the connection is not proof that all server work stopped. Configured daemon storage/extensions remain trusted; this review does not establish that arbitrary snapshotters perform only local operations.

The smallest Engine-cache candidate is three fixed requests on one held Unix connection:

1. `GET /v1.44/info` to capture the declared daemon ID/version and Linux profile.
2. `GET /v1.44/system/df?type=build-cache` only.
3. `GET /v1.44/info` to compare the same declared daemon ID/version.

Reuse the existing exact context resolver, frozen canonical Unix endpoint, sanitized child environment and strict HTTP/JSON parser. Retain a whole-operation five-second deadline, 16 KiB headers, 1 MiB bodies and at most 128 cache records. Refuse overflow, unsupported framing, malformed/duplicate fields, redirects, reconnects, changed daemon evidence or partial replies. Add no SDK, gRPC, session, arbitrary query, builder fallback or retry. These are client bounds for the generated implementation, not daemon resource controls.

The report would name its scope `engine_embedded_cache`, retain exact context/endpoint/daemon evidence and individual bounded cache IDs/types/usage flags/dates, and label observations sequential. Exclude descriptions, labels, source/build commands, credentials and host paths. Keep sizes and reclaimable savings null in the first report; an in-use flag or shared flag does not establish exact reclaimable host space. Missing fields must remain unknown or refuse the profile, never become invented zero values.

Daemon ID/version agreement supports continuity of the declared observations. It authenticates neither locality nor a namespace. The API supplies no independent cache incarnation or builder generation, so this candidate cannot set `builder_pinned` as proof of an exact named instance. Approval, execution, current verification and atomic-snapshot flags remain false. A direct BuildKit alternative exposes worker records and disk-usage RPCs but introduces a separate transport/schema/dependency design; its disk-usage response itself is not a worker-generation token. [BuildKit v0.12.4 control API](https://github.com/moby/buildkit/blob/v0.12.4/api/services/control/control.proto), [Buildx v0.13.0 node loading](https://github.com/docker/buildx/blob/v0.13.0/builder/node.go)

## Next implementation and remaining decisions

Proceed with the bounded Engine-embedded report on generated fixtures under the current instruction to complete independently completable read-only plan work. No additional owner approval is needed for this development. The declared context/endpoint/daemon scope is useful even though exact builder-instance pinning remains unsupported. Do not silently broaden the existing command, use a selected Buildx default, or turn this partial reporting leaf into full P2-04 acceptance.

Generated implementation should test the three-request allowlist, selector preservation, one connection, exact context/daemon evidence, empty versus unavailable results, required/null fields, safe projection, overflow, cancellation, late output and zero partial positive results. Generated servers cannot prove how an installed daemon handles accounting, extensions or builder identity. Native installed acceptance requires an explicitly selected owner context and supported version/storage profile; no context is selected here.

If a later report must support named Buildx instances, its exact driver/node/transport and identity contract need another bounded design. Other conforming local read-only profiles can be investigated under the continuation authorization; support must follow evidence, not an assertion that every possible API lacks a solution. Container helpers, remote transports and additional compatibility versions remain outside this profile. Adopting helpers or remote-context access would change the current product constraints and need a new scope decision. An owner choice is needed before selecting an installed/private context for acceptance or choosing actual cache-action/rebuild-cost policy. None of these choices blocks the generated Engine-cache leaf.

The existing P2-04a command remains independently useful. Audit its exact published native CI while the next leaf is developed. Neither this feasibility result nor general continuation approval closes P2-04, automatic cache eligibility, source-namespace cleanup, Linux clone, unattended resource/service, soak or release gates. No new source-operation experiment follows from this record.

Primary schemas and pinned source above were reviewed on 2026-10-09. No installed Docker/Buildx command, private configuration, daemon, builder or cache was accessed.
