# ADR 002 — Compact generated-tree measurements

- Date: 2026-09-16
- Status: opt-in production storage and scoped reductions implemented; full default rollout pending (P2-02b)
- Scope: initially `node_modules`; read-only metadata and saved reports

## Decision

Represent a generated dependency tree as one visible inventory item. Internally retain directory work records and compact device/inode contributions, rather than ordinary file-path inventory. Preserve logical bytes per file path, but count observed allocated bytes once per known device/inode within a measurement scope. Keep byte-preserving directory paths and identity/freshness evidence at tree/directory boundaries.

Do not rely on `st_nlink == 1` to discard identities: links can change during a scan, and links may cross package or tree boundaries. Do not replace exact observed identity accounting with a Bloom filter or content sample. Clones, snapshots and links outside the measured scope remain separate qualifications; an observed size is never a promise of reclaimed space.

This is O(file identities) compact evidence, not constant storage per tree. It avoids the path strings, path indexes and timestamps stored for each ordinary file. We prefer this explicit tradeoff over silently inflating allocated sizes. Investigate further compression only if measured storage or write costs justify it.

## Evidence

The runnable [storage experiment](../../experiments/aggregation/README.md) uses the existing SQLite driver and 100,000 synthetic file paths across 100 directories, including 90,100 distinct identities. After checkpointing:

| Representation | Database bytes |
| --- | ---: |
| Ordinary file records and indexes | 33,239,040 |
| Compact directory totals and inode ledger | 3,375,104 |

The compact file representation is 10.2% of the baseline size, about 90% smaller **for this fixture**. Logical and allocated totals match the independently queried baseline. This is not a filesystem scan, a whole-app footprint prediction, or evidence of lower CPU, RAM or wall-clock scan time. Extra production indexes, directory paths, retirement backlog and future reduction state are not all represented.

Correctness fixtures exercise reopen/restart, replacing interrupted contributions while preserving siblings, rolled-back batches, duplicate batch rejection, hardlinks across directories, conflicting observations, integer overflow and bounded obsolete-generation retirement. Native macOS verification and Docker results are recorded in PROGRESS.

## Durable continuation model

1. The existing filesystem scanner continues to supply bounded, revalidated metadata batches. Generated-tree handling changes the persistence destination, not path/symlink/mount/protection checks or entry pacing.
2. Each directory contributes a generation with partial logical totals, file counts and inode evidence. Commit aggregate changes and the job cursor/lease fence in one transaction. Never accept an aggregate batch independently of that fence.
3. A directory that restarts enumeration gets a new generation. Immediately exclude its old contribution from new totals; do not subtract and then blindly add replayed values. Completed sibling directories retain their contributions.
4. A tree pass also needs parent-child membership generations. Disappeared/replaced directories must cease contributing, even when old directory records still exist. Root identity changes retain old evidence as stale and block updates, using the existing protections.
5. Retire obsolete generations through durable, bounded cleanup jobs. A directory that shrinks or disappears must still get cleanup work; “delete a few old rows whenever new files arrive” cannot drain that case. This only retires rebuildable inventory evidence, never user files or action/restore records.
6. Interrupted *directory enumeration* still starts over with the current scanner. Do not market this as exact per-entry resume. Wide-directory continuation remains an explicit requirement; the aggregate prototype does not solve enumeration starvation under repeated interruptions.

## Production integration gates

Implement these as one coherent read-only feature before enabling compact storage by default:

- **Boundary recognition:** first outermost `node_modules` in an enabled root, including when the selected root itself has that name. Nested `node_modules` belongs to that existing boundary. A symlink, excluded/unavailable directory or mount boundary must not become a successful measured tree. Name recognition does not authorize deletion.
- **Storage migration:** append a schema migration preserving existing inventories/settings. Link tree, directory and inode records to root identity, pass/generation and current jobs. Add explicit unknown identities, skipped/error counts, conflict state and oldest/newest observations. Test migration rollback and old-reader/new-writer behavior.
- **Bounded totals:** maintain tree logical totals and inode reductions using resumable, budgeted jobs. The experiment's full-ledger `GROUP BY` is an oracle only. A high-fanout inode can require multiple reduction batches; do not claim bounded work because a SQL query returns one row. Expose partial totals while reduction/retirement is pending.
- **Reports:** use saved cached totals without re-traversing the filesystem. Do not add independently deduplicated allocated totals across trees or ordinary inventory: shared inodes cross those boundaries. Merge identity evidence incrementally for the requested scope or expose the allocated total as unknown until the scoped reduction exists. Preserve existing human/JSON partial, stale, unknown, overflow and evidence semantics.
- **Legacy overlap:** once a boundary's compact generation is authoritative for the active mode, ignore its ordinary descendant file rows in reports and candidates immediately. Retire those old rows in bounded jobs. Never sum legacy descendants and the aggregate. Until the new observation is sufficient, show partial/unknown rather than silently treating omitted files as zero.
- **Detailed override:** expose `--detailed` for manual scans and a corresponding saved setting for background scans. Pin storage mode for a pass; reject a mode switch while that pass has unfinished jobs rather than mixing representations. A mode switch after completion starts a new pass, with honest partial results until replaced. Document the state/store selection behavior.
- **End-to-end validation:** fresh/legacy/detailed stores; cancellation and process kill; mutation/shrink/disappearance; unknown and shared inode identities; sparse files; nested boundaries; exclusions and symlink/mount swaps; candidate evidence; offline and concurrent reports. Verify native macOS/Linux and measure state/WAL growth, including cleanup backlog, on a large generated fixture.

## Initial production slice (P2-02b2)

An opt-in manual `--compact` mode now uses production-owned schema-5 tables; it does not import the experiment or its full-ledger oracle. Per-directory logical totals are updated with the scanner lease/cursor transaction. Known inode evidence stays compact, old generations and legacy file rows are excluded immediately, and durable retirement jobs remove at most 128 obsolete records per transaction. The manual scan drains those jobs and preserves them across interruption.

Reports retain the existing 10,000-entry coverage bound and inspect at most 10,000 additional compact inode records. They combine ordinary/compact identities for the measured scope. Above that identity budget, allocated size is unknown and the report is partial; logical totals remain useful. This deliberately postpones the full incremental allocated reduction described above rather than introducing a full-ledger report query.

The detailed override is pass-pinned. Schema-4 readers remain supported by the new CLI, while writers migrate additively. Background compact mode is refused until its maintenance dispatch is implemented. Exact per-entry directory resume remains unsolved. These limitations keep the full P2-02b task and default enablement open.

## Saved membership reconciliation (P2-02b3)

Directory reports now exclude historical entries and descendant compact totals when a successful completed ancestor listing no longer contains them, or an ancestor is recorded as a non-directory. This uses the existing saved membership generations and bounded path-order traversal; no schema change, scan or database retirement occurs during reporting. Excluded rows still consume the report budget and are disclosed in human/JSON output. Selecting a directory absent from its completed ancestor listing returns unknown sizes. Incomplete/failed listings retain qualified stale evidence. Historical rows and compact inode records still require a separate durable retirement design; this report correction does not bound database growth or complete the production membership lifecycle.

## Scoped allocated reductions (P2-02b4)

Schema 6 adds root inventory revisions, scalar scope caches, durable entry/inode cursors, membership scratch and scoped identity scratch. Manual compact scans schedule the selected root plus each outermost `node_modules` scope. Every committed scan batch advances its root revision in the lease-fenced transaction, including faults and detailed passes. This deliberately conservative invalidation avoids stale cross-directory hardlink or ancestor-membership totals. No independent subtree totals are added together.

After the root's inventory queue finishes, each maintenance transaction processes at most 128 inventory entries, compact inode contributions or scratch-cleanup rows. Entries are traversed in byte-path order with the same overlap suppression, membership exclusion and 10,000-entry coverage cap as reports. A disk-backed identity map tracks maximum allocated bytes, conflicts and repeat counts across ordinary and compact evidence. Cursor and total updates commit together; high-fanout identities do not require an unbounded `GROUP BY`. A truncated reduction is never published. Completed scoped totals can exceed the report's 10,000 compact-identity fallback budget, but never override directory coverage or unknown-identity qualifications.

Reports read matching revision/scope caches from their existing snapshot. Other scopes and pending/invalidated caches use the bounded fallback. Cache scratch is retired in bounded resumable batches after publication; a revision change first retires old scratch before rebuilding. Switching to detailed mode drains and removes obsolete caches so background detailed scans cannot be blocked by abandoned maintenance. Schema-4/5 readers remain supported by the new CLI; older binaries reject schema 6.

This does not eliminate O(identity-count) temporary storage: one active scope's identity map can grow in SQLite before being retired, and freed pages may be reused rather than shrinking the file. Native Linux evidence, larger state/WAL/backlog measurements, durable disappeared-subtree retirement and background/default integration remain open. The isolated prototype's storage ratio does not describe this implementation.
