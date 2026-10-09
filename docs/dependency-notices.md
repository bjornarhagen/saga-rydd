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
