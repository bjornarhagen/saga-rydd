# Dependency notices for distribution review

The pinned linked dependencies have the root notices listed below. This inventory supports distribution review; it does not select Rydd's project license or accept a public release.

The inspection used an existing generated binary's Go build information and already cached module files, with downloads disabled. All eleven linked module versions and module sums match `go.mod` and `go.sum` in exact source `d9f0bf3a046e89eda616d118bdd7f77fb00a8b84`. File lengths and SHA-256 digests are preserved in the [machine-readable inventory](dependency-notice-inventory.json). No host Go installation or package publication occurred.

| Component | Pinned version | Observed root license text | Additional root notices |
| --- | --- | --- | --- |
| Go toolchain / standard library | 1.27.1 | BSD 3-clause | `PATENTS` |
| `github.com/dustin/go-humanize` | v1.0.1 | MIT | — |
| `github.com/google/uuid` | v1.6.0 | BSD 3-clause | — |
| `github.com/mattn/go-isatty` | v0.0.24 | MIT | — |
| `github.com/ncruces/go-strftime` | v1.0.0 | MIT | — |
| `github.com/pelletier/go-toml/v2` | v2.4.3 | MIT | — |
| `github.com/remyoudompheng/bigfft` | v0.0.0-20230129092748-24d4a6f8daec | BSD 3-clause | — |
| `golang.org/x/sys` | v0.47.0 | BSD 3-clause | `PATENTS` |
| `modernc.org/libc` | v1.75.7 | BSD 3-clause | `LICENSE-3RD-PARTY.md` |
| `modernc.org/mathutil` | v1.7.1 | BSD 3-clause | — |
| `modernc.org/memory` | v1.12.1 | BSD 3-clause | `LICENSE-GO`, `LICENSE-MMAP-GO`, `LICENSE-LOGO` |
| `modernc.org/sqlite` | v1.59.0 | BSD 3-clause | `LICENSE-SQLITE`, `LICENSE-SQLITE_VEC` |

The extra notices matter. Libc's third-party notice names Go, musl, go-netdb and NixOS/nixpkgs, and points to further per-file terms. Memory carries separate Go and mmap-go notices; its logo reference is a source asset, not evidence that the asset is included in an executable. SQLite carries its upstream public-domain statement and a separate MIT notice for sqlite-vec. A root license label alone does not cover all embedded code or prove which optional code is linked on each target.

The current candidate archives contain only `rydd` and `BUILDINFO.json`; they do not include these license/notice texts. Before distribution acceptance, review linked embedded and standard-library/vendor notices for every target, preserve the applicable original notices in the chosen distribution materials, and verify the resulting package contents. This bounded inventory leaves that audit and notice packaging open. Recheck it when module/toolchain pins or build features change.

Project-license choice, signing and public release remain separate gates in the [release procedure](release.md#gates-before-a-public-release). No compatibility or legal acceptance is inferred from this table.

## Bounded linked-package notice map

The four-target map uses exact source `be8feb97dd9e1b38c397710d856a140c975c39ca`, Go 1.27.1 and the existing cached module sources, with downloads and network access disabled. Source and cache mounts were read-only. Existing product binaries were inspected; no products or packages were rebuilt.

| Target | Packages in the dependency graph | External module versions in existing binary build information |
| --- | ---: | ---: |
| macOS arm64 | 261 | 11 |
| macOS amd64 | 262 | 11 |
| Linux arm64 | 237 | 10 |
| Linux amd64 | 238 | 10 |

The four graphs contain 264 distinct packages, including sixteen Go standard-library vendor packages from `golang.org/x/crypto`, `x/net`, `x/sys` and `x/text`. The toolchain's vendor versions differ from the separately linked `golang.org/x/sys` module pin. All recorded external versions and sums match the pinned manifest. `github.com/ncruces/go-strftime` is in the Darwin graphs and binary module records; it is absent on Linux.

| Evidence | Observed notice sources | Remaining limit |
| --- | --- | --- |
| Go vendor packages | Each of the four vendor module roots has `LICENSE` and `PATENTS`; versions are preserved from `GOROOT/src/vendor/modules.txt` | Package selection does not prove exact code retained by the linker |
| Go standard library | Inline Fiat-crypto, Sun/Cephes math and amd64 Lucent/Vita Nuova notices, in addition to Go's root notices | Review retained code and applicable original attribution before distribution |
| `modernc.org/libc` | Root third-party terms and generated per-file notices; Darwin prefixes include APSL and GCC GPLv3+ with GCC Runtime Library Exception 3.1; Linux prefixes include glibc LGPLv2.1+ notices | Resolve original inputs, full terms and whether copied header constants/types contribute retained code; no product-license conclusion is established |
| `modernc.org/memory` | Selected `mmap_unix.go` refers directly to `LICENSE-MMAP-GO`; other root notices remain preserved | Later source provenance and optional assets remain unverified |
| `modernc.org/sqlite` | Generated SQLite source selected on all targets; root SQLite and sqlite-vec notices preserved | `modernc.org/sqlite/vec` is absent from these graphs; generator flags and source-root assets do not prove optional linked code |
| Optional BoringCrypto | Default stub Go files selected; no selected CGo files or compiled system objects; ancillary header and root notice observed | Presence of the notice/header is not evidence that a BoringCrypto object was compiled |

The audit preserved exact lengths and SHA-256 digests for 33 notice files and one vendor module index, totaling 59,642 bytes. It inspected at most 16 KiB from each of 2,183 metadata-listed files: 2,058 Go files, 118 assembly files and seven ancillary headers. Total prefix bytes were 12,856,039. Of these prefixes, 354 were truncated and 493 contained generated-file markers. No Go `EmbedFiles` were selected. These counts do not exclude later notices, generated constant data or embedded code within ordinary source files.

This is a bounded evidence map. Cached file hashes identify the inspected bytes; they do not authenticate upstream source. Complete per-file provenance, retained-code review, project-license choice, a verified notice bundle and distribution acceptance remain open. Current candidate archives still contain only `rydd` and `BUILDINFO.json`.
