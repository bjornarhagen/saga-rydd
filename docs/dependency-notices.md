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

## Complete-file hashes with bounded marker details

The 2026-10-10 follow-up used published source `fa64338f9675654bd2430b41e6e512b8faf00ddd` and the same pinned offline toolchain and modules. Its four-target package comparison preserved the recorded dependency versions and sums. The exact full-file scope contained 351 unchanged truncated sources and 13 changed/new project sources. All 364 complete hashes cover 43,071,507 bytes. The reader checked all 351 historical size/prefix bindings before reading their remaining bytes or the new project sources; all 39 selected project files also matched current full-source pins.

Marker counts use fourteen fixed ASCII terms and count matching lines, with each term counted at most once per line. Regions describe each whole line relative to the initial 16 KiB boundary. Retained details include exact first term positions, a full-line hash and at most 512 context bytes.

| Line region | Matching lines | Details retained | Details omitted |
| --- | ---: | ---: | ---: |
| Within the initial prefix | 2,373 | 1,048 | 1,325 |
| Crosses the prefix boundary | 6 | 2 | 4 |
| After the initial prefix | 32,543 | 3,046 | 29,497 |
| Total | 34,922 | 4,096 | 30,826 |

The fixed detail limits omitted 2,445 lines at the per-file limit and 28,381 at the global limit. No detail was omitted by the reply byte limit. The final reply was 2,351,720 bytes within the unchanged 4 MiB ceiling. Omitted contexts remain unreviewed; a complete file hash does not make the marker detail review complete.

The first preparation stopped in auxiliary compilation before the reader ran. A separately reviewed compiler-cap correction then reached a marker-detail limit and returned no accepted full-file report. Both outcomes remain recorded. The final protocol completed once in 7.56 seconds, preserved every source pin, reaped all direct children and observed the exact generated container absent. Its generated parser checks and independent result review passed. Auxiliary standard-library tool compilation and source fingerprint reads are separate from the declared notice-source byte count.

These are dated source observations for the declared commit. Later source changes require another package/scope comparison. Full hashes and bounded lexical details do not establish upstream authenticity, original-input provenance, linker retention, complete attribution terms, legal compatibility or a distribution-ready notice bundle. Those release gates remain open.

## Final CPU candidate package comparison

The four-target comparison for the 482-file CPU candidate preserved dependency versions, sums and complete import/module/selection closures. The selected union has 2,194 files. Of these, 1,825 have earlier complete-small observations, 325 cache files and thirty project files have dated complete hashes, and fourteen changed/new project files total 251,492 bytes. A separate complete review of those fourteen project files passed with zero matching lines for the fixed fourteen ASCII terms. The package comparison itself refreshed no cache bodies; the separate two-file review below has its own exact scope.

This comparison checks metadata selection and current project fingerprints. It does not refresh historical dependency contents or close the omitted-context, provenance, retained-code, legal-compatibility or notice-packaging gates.

## Selected Darwin FTS matching-line review

A separate finite reader checked both `modernc.org/libc` Darwin FTS files associated with the published CPU source. It read 527,117 source bytes and retained all 3,469 matching lines in a 698,651-byte payload. This selection covers the 2,445 locations previously omitted at the per-file limit. The other 28,381 omitted locations remain outside this review.

Root and independent review read the 46 distinct complete matching-line bodies, totaling 3,147 bytes, and verified every location's mapping to those bodies. All observed bodies begin with `//`. This describes the retained text; surrounding Go syntax was not retained or verified.

| Observed text | Distinct line bodies |
| --- | ---: |
| Generator commands | 2 |
| Apple license boundary markers | 4 |
| Copyright dates or entities | 23 |
| UC Regents rights and entity | 1 |
| Berkeley contributor attribution fragment | 1 |
| Other license or clause fragments | 15 |
| Total | 46 |

The full-file hashes and offsets remain observations from the reviewed reader. Filtered matching lines cannot reconstruct the 318,924 nonmatching source bytes or supply missing neighboring clauses. This closes only the declared matching-line assessment. Complete multiline notices, original-input provenance, applicable licenses, code retained by the linker, legal compatibility and the distribution notice bundle remain open.

## Selected Darwin netdb complete-text assessment

The 2026-10-12 retained-source assessment covers the complete declared extents of two generated Darwin netdb files from `modernc.org/libc v1.75.7`. The [static source-notice catalog](netdb-notice-catalog.json) preserves all 48 distinct exact notice-group bodies, their byte lengths and SHA-256 digests, and each file's occurrence counts. Source names are relative to the module root.

| Source | Source bytes | Complete notice groups | Distinct bodies in this file | Full source SHA-256 |
| --- | ---: | ---: | ---: | --- |
| `netdb/netdb_darwin_amd64.go` | 331,419 | 98 | 40 | `88a06f155a1fa0e55acd6d3ae1d03623687c6df2a66e3f83e0347b3de592dc5d` |
| `netdb/netdb_darwin_arm64.go` | 330,519 | 99 | 43 | `48dc354d92f8d8cf0edbc1f27d39f100bac94c18c6676d53f319263d6e05a60a` |

The two source files total 661,938 bytes. Their 197 notice-group occurrences contain 48 distinct bodies totaling 108,552 unique bytes; shared bodies count once in that total. The scan covers all source bytes and excludes Go string and rune literals. A group joins adjacent lexical comment lines without a blank line and can include preceding inline technical context. These group counts differ from individual license-header counts because one group can contain several notices.

| Observed notice wording | amd64 occurrences | arm64 occurrences |
| --- | ---: | ---: |
| Ordinary Apple APSL 2.0 header | 7 | 8 |
| Apple OSREFERENCE APSL 2.0 header | 138 | 132 |
| Regents four-condition redistribution notice | 10 | 10 |
| WIDE Project three-condition redistribution notice | 2 | 2 |
| Digital Equipment permission grant | 1 | 1 |
| LLVM license-reference fragment | 1 | 1 |

The Apple headers require compliance with the referenced APSL 2.0 license and obtaining and reading it before use. OSREFERENCE headers add restrictions concerning unlawful or unlicensed Apple operating-system copies and circumvention or violation of Apple operating-system license agreements. The full referenced APSL text remains unresolved.

The Regents notices include source retention, reproduction in binary-distribution materials, a Berkeley acknowledgment in advertising mentioning features or use, and prior permission for endorsement using University or contributor names. The WIDE notices include source retention, binary reproduction and prior permission for endorsement using project or contributor names. The Digital Equipment grant requires its copyright and permission notices in all copies and prior written permission for use of its name in distribution advertising or publicity. These notices also preserve their warranty and liability disclaimers.

The LLVM fragment names Apache License v2.0 with LLVM Exceptions and carries `SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception`. Its full referenced license and exception terms remain unresolved. Copyright-only NeXT and Apple wording is also preserved; it adds no independent permission conditions in the observed statements.

This catalog records embedded source wording and its associations with the two retained files. At this assessment, 91 other historical source carriers remained. The separate two-file assessment below covers two of them. Upstream authenticity, compiled-target applicability, full referenced licenses, legal compatibility, project-license choice, notice packaging and release acceptance remain open. The fixed twenty-row root-notice catalog and existing candidate archives keep their separate scope.

## Selected memory and mathutil complete-text assessment

The 2026-10-12 assessment reads the complete retained `memory.go` from `modernc.org/memory v1.12.1` and `mathutil.go` from `modernc.org/mathutil v1.7.1`. The [static source-notice catalog](memory-mathutil-notice-catalog.json) preserves both exact copyright/license-reference headers and the explicit minux acknowledgment, with source digests and byte/line spans.

| Source | Source bytes | Physical comment groups | Comment tokens |
| --- | ---: | ---: | ---: |
| `modernc.org/memory/memory.go` | 21,664 | 55 | 203 |
| `modernc.org/mathutil/mathutil.go` | 30,735 | 118 | 242 |
| Total | 52,399 | 173 | 445 |

Both headers refer to a separate BSD-style `LICENSE` file. Neither source embeds a license grant, redistribution conditions or a disclaimer. The catalog associates those pointers with the exact length/digest records in the earlier root-notice inventory; it does not reread or reproduce the root texts. The mathutil changelog explicitly thanks minux. Nine technical URL occurrences were observed without fetching their contents. TODO and benchmark identity tags are not inferred contribution acknowledgments or license terms.

The full-source comment census excludes string and rune literals. It covers four matching locations omitted by the earlier bounded marker report; those locations are not a count of notices or legal obligations. The dated residual is 89 other source carriers, with 24,789 omitted marker locations across 21,194,231 historical source bytes. This is arithmetic on the earlier inventory, not a fresh source census. Other source files, including `mathutil/tables.go`, remain outside this assessment. Upstream authenticity, compiled applicability, complete attribution, legal compatibility, project-license choice, notice packaging and release acceptance remain open. The fixed twenty-row root-notice assembler and candidate archive contents are unchanged.

## Selected asynchronous-state and plans-export project review

The separate 2026-10-10 project review selected exactly fifty integration changes before the adaptive-revisit readiness correction. Complete selected bodies total 693,041 bytes: thirty test files contain 417,699 bytes and twenty other files contain 275,342 bytes. The held reader checked exact source identities and retained a complete 29,176-byte fixed-term report. Root and independent review reproduced every selected hash and the complete report; no matching line was truncated or omitted.

The fourteen ASCII terms matched one complete 71-byte line after the initial 16 KiB prefix. It is an operational comment in `internal/worker/worker.go` about stale “original source” selection and claim authority. The other thirteen terms had no matches. This classifies that exact comment; it does not establish the absence of other notices or legal obligations.

The later adaptive-readiness correction received a separate incremental review after formatting passed without changes. That invocation read only the changed 15,367-byte fixture and found no matching lines. It inherited the other forty-nine exact source associations and retained observations from the accepted report above. Both reviews reproduced the complete 30,708-byte incremental report. Its current fifty-file association totals 694,668 bytes: 419,326 test bytes and 275,342 other bytes. No matching line was omitted or truncated.

This is an incremental association, not a new read of all fifty bodies. The one inherited operational-comment match and the broader dated qualifications remain. Test inclusion does not establish production linkage. Dependency contexts, provenance, linker retention, project-license choice, a complete notice bundle and distribution acceptance remain open.

## Selected Doctor and historical copy-size project review

The separate 2026-10-10 review selected exactly eighteen feature paths in the combined Doctor and historical copy-size candidate. Complete selected bodies total 205,641 bytes: test sources contain 51,732 bytes and other sources contain 153,909 bytes. Root and independent review reproduced every selected hash and the complete fixed-term report. None of the fourteen ASCII terms matched a line; no matching line was truncated or omitted.

This is a complete read of those eighteen selected bodies only. It does not refresh the earlier fifty-file or dependency-body observations, prove the absence of other notices, or establish which code the linker retains. Original-input provenance, applicable licenses, project-license choice, legal compatibility, the complete distribution notice bundle and release acceptance remain open.

The later sparse-fixture correction received a separate incremental review. That invocation read only the changed 14,859-byte test file and found no matching lines for the same fourteen terms. Both reviews reproduced its report and the seventeen unchanged source associations. The current eighteen-path association totals 205,690 bytes: 51,781 test bytes and 153,909 other bytes. No matching line was omitted. This inherits the other seventeen dated complete observations; it is not a new read of all eighteen bodies or a refresh of dependency content.
