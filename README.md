# Saga — Rydd

A quiet storage cleanup companion for macOS and Linux. Part of Saga.

Rydd will gradually discover developer clutter and duplicate files, explain what can be removed, and help reclaim space through reviewed actions or explicitly enabled automatic policies.

**Status: experimental read-only inventory and explicit hashing.** The worker can scan explicitly selected fixture directories in bounded, resumable batches and save metadata, skip reasons and directory reconciliation markers in SQLite. Use `scan -d PATH` for a foreground scan, or `daemon --experimental-scan` for configured roots; ordinary `daemon` stays idle. Durable dispatch cadence, a daily batch cap, charged scanner API allowances, child-entry pacing and WAL backpressure are enforced; saved largest-file and selected-directory size reports and review-required `node_modules` candidates are available. Explicit full-file hashing requires separate exact consent and one bounded invocation at a time. Process CPU observations, cooperative dispatch backoff, source-thread priority requests and sparse experimental source-power admission are live; exact idle-only service descriptors can be installed and inspected, with explicit start/stop requests and descriptor removal. Full CPU/I/O/power quotas, runtime verification and cleanup remain pending. Nothing runs in the background when you clone, build or initialize this repository.

For humans and AI: readable output by default, versioned JSON with `--json`, and `rydd capabilities --json` for discovery. Status separates live process CPU observations from saved experimental-turn feedback. Interrupted tracked turns require explicit writer recovery and retain a one-hour cooldown; saved views never recover them. Scanner metadata API counters help inspect background work. See the [CLI contract](docs/cli.md). The [finite generated resource harness](experiments/backgroundresources/README.md) measures native worker CPU, peak memory and control/report latency with separate observer overhead; hourly and soak acceptance remain open.

Start with [first use and recovery](docs/getting-started.md) for explicit folder/private-state selection, the available review flow and common waits or interrupted commands.

For distribution work, follow the [local candidate and future release procedure](docs/release.md). Local packages remain review candidates; license, platform and release acceptance gates are open.

**Next milestone: resource controls and user services.** The generated owner read-only walkthrough and human-output corrections passed, including guided review and separately consented fresh hashing with historical comparisons. Exact finding dismissal, persistent exclusion controls, saved Cargo/Go cache reports and finite selected Docker image/container/Engine-cache metadata are verified on generated native fixtures. Genuine Cargo producer compatibility passed both native platforms. Restart-safe CPU feedback and bounded Linux power observations are verified. The current continuation adds sparse source-power scheduling, finite resource measurements and the remaining state/service controls after the read-only MVP. Installed-Docker, cache-action/regeneration and Linux clone acceptance remain open. Cleanup needs an operation that preserves the exact reviewed source object and ancestor scope; the reviewed rename operations do not provide that boundary. See [ADR 003](docs/decisions/003-source-namespace-boundary.md), the [current handoff](PROGRESS.md#current-state) and [trial lessons](docs/mvp-trial-results.md). Numbered phases are not a strict work order, and the full plan remains unfinished.

## Scan a chosen folder

```sh
rydd scan -d /path/to/project          # 10 ms between entry inspections
rydd scan -d /path/to/project -s 25    # slower, 25 ms
rydd scan -d /path/to/project --now    # no deliberate delay
rydd report -d /path/to/project
rydd report -d /path/to/project --candidates --json
```

No initialization is needed for a manual scan. It runs in the foreground and saves a separate inventory per selected folder; your configured background roots remain unchanged. Ctrl+C stops it; rerun the same command to finish pending work before revisiting completed folders. Once all scan jobs and saved maintenance/calculations finish, the next invocation starts a fresh pass. The interrupted directory itself restarts its listing. Filesystem protections and configured exclusions still apply. No file contents are read or deleted. `--now` can produce substantial metadata I/O. `-s` spaces child-entry inspections, not every filesystem operation.

### Try compact dependency inventory

```sh
rydd scan -d /path/to/project --compact
rydd report -d /path/to/project
```

This opt-in mode stores `node_modules` file sizes and identities compactly instead of keeping each filename in ordinary inventory. It still traverses metadata to measure size; contents are not read. The mode is saved per manual inventory, so the same scan command without flags resumes it. Use `--detailed` after pending work finishes to switch back. Existing pending detailed scans must finish before switching modes.

Logical sizes use saved directory totals. After scanning, the manual command calculates cached allocated totals for the selected root and each outermost `node_modules` tree, deduplicating hardlinks across the entire scope. These calculations resume in bounded batches after interruption and are invalidated by the next saved scan batch. Reports can use matching caches even above 10,000 compact identities. Other scopes, or caches still being calculated, fall back to checking at most 10,000 compact inode records; larger or unknown identity sets then show allocated size as unknown. Completed scope calculations also cache full directory coverage and logical size. These reports can cover more than 10,000 entries; other scopes and unfinished calculations retain the synchronous cap. Old observations or exclusions can still make reports partial or stale.

To finish saved calculations after an upgrade or interruption without rescanning:

```sh
rydd measure -d /path/to/project
rydd report -d /path/to/project --candidates --min-age-days 30
```

`measure` requires an existing compact manual inventory. It runs at most 128 batches or five seconds per invocation; repeat until complete. Each batch processes at most 128 records. It writes derived database state only; source folders can be offline. Pending scans must finish first. Reports remain read-only.

Old ordinary file records are hidden from totals immediately per measured directory, then retired in bounded batches. Cache calculations temporarily store a scoped identity map in SQLite and retire it after completion. [Production fixture measurements](experiments/compactscale/README.md) compare compact and detailed state, memory, latency and interruption recovery; they do not establish unattended resource budgets. SQLite may reuse freed pages rather than shrinking its file. Compact background scanning and default enablement remain pending.

After a successful completed parent listing no longer contains a directory, its saved descendants stop contributing to directory sizes. Reports disclose excluded historical rows; incomplete or failed listings retain stale evidence. Once a manual detailed or compact scan queue finishes, resumable maintenance retires obsolete subtree records and caches in bounded batches. The experimental worker also dispatches bounded saved-only maintenance before scheduling that root's next revisit. New scan observations invalidate pending retirement proofs, preserving reappearing directories and current replacement files. This only removes rebuildable database records; user files are untouched. Interrupted maintenance finishes before another pass or mode switch.

Reports select the manual inventory for that exact normalized path when present. Directory-size reports otherwise use the existing configured inventory. Scoped `--candidates` requires a manual scan of that exact folder. Relative paths and `~/` are accepted; use the same global `--data-dir` across commands when overriding it. Manual inventories are separate from default `status` and unscoped reports. See [manual scan semantics](docs/cli.md#foreground-manual-scans).

## View saved results

```sh
rydd report
rydd report --limit 10 --json
rydd report --candidates --json
rydd report --candidates --min-age-days 30 --json
rydd report --directory /absolute/path/to/folder
rydd report --directory /absolute/path/to/folder --json
rydd report --limit 10 --cursor TOKEN
```

The report works while the worker is stopped and reads only saved inventory. It lists the largest observed regular files, sizes, timestamps, parent-pass freshness and saved root diagnostics. Use `--directory` to read a completed scope calculation or measure up to 10,000 saved entries in a selected subtree, with partial/stale/unknown labels and qualified hardlink accounting. `--candidates` selects old recorded `node_modules` and sibling `package.json` timestamps for review. It does not inspect manifest contents or establish inactivity or safe deletion. The default age filter is 90 days; `--min-age-days N` changes it for one report only. Candidate reports now explain selection outcomes (including age, missing evidence and incomplete listings) and whether more saved entries remain. Reports do not rescan paths or delete anything. Use the returned `next_cursor` for another page; keep global `--data-dir` before `report` if using a separate instance. See the [report contract](docs/cli.md#saved-file-reports).

## Review saved Cargo build output

```sh
rydd report --build-output -d /path/to/project
rydd report --build-output -d /path/to/project --min-age-days 30 --json
```

This mode requires an existing manual scan of that exact folder. It recognizes an outermost `target` beside saved `Cargo.toml` and `Cargo.lock`, with a supported `debug` or `release` layout. Every required marker must meet the age filter and have a confirmed saved parent listing. Each page checks up to 1,000 entries and measures up to 20 candidates; use its continuation command when present.

The report reads saved metadata only. It shows qualified logical/allocated sizes and recognition evidence. It does not check artifact contents, project use or whether a build can be reproduced. Custom and unsupported layouts are excluded. There is no cleanup action, saved plan or dismissal for this category. See the [Cargo report contract](docs/cli.md#saved-cargo-build-output).

## Review saved Go build-cache files

```sh
rydd report --go-cache -d /path/to/cache
rydd report --go-cache -d /path/to/cache --min-age-days 30 --json
```

First perform an explicit manual metadata scan of that exact proposed cache root. This report requires a saved regular `README`, all 256 hexadecimal shard directories and completed saved listing evidence. It selects old regular action/data filenames in the supported shard layout. Partial or unsupported layouts produce a qualified empty report; empty file pages can still have continuation.

It reads saved metadata only. Names do not establish that Go produced the files, uses this cache or can regenerate its contents. Per-file sizes and page identity counts are historical observations, not cache totals or space you can free. Fuzz data, module downloads, external cache management and cached executable directories are outside this category. No Go tool runs, source read, saved-record change, approval or cleanup occurs. See the [Go cache report contract](docs/cli.md#saved-go-build-cache-files).

## Read selected Docker metadata

```sh
rydd docker --metadata --context NAME
rydd docker --metadata --context NAME --json
```

Choose an exact existing Docker context explicitly. This command resolves its endpoint once and accepts only the supported canonical Unix socket form. It makes four fixed metadata requests on one connection and checks the daemon ID and version before and after image/container observations. It does not reconnect. Missing Docker, unavailable sockets and unsupported or oversized replies fail without a partial report.

The report shows IDs, tags/names, creation dates and container state. It measures no sizes or savings, saves no Rydd history and provides no cleanup action. Volumes, builders and cache are outside this mode. The supported profile is Linux Docker Engine 25+ with API 1.44; installed-Docker acceptance remains open. A Unix endpoint does not establish physical locality or authenticate the daemon. The installed CLI, its configuration and daemon extensions remain trust boundaries. See the [Docker metadata contract](docs/cli.md#selected-docker-metadata).

## Read Engine-embedded cache metadata

```sh
rydd docker --cache-metadata --context NAME
rydd docker --cache-metadata --context NAME --json
```

This separate mode queries only the selected Engine's embedded build cache. It makes three fixed requests on one connection: daemon metadata, filtered cache metadata, then daemon metadata again. It does not inspect the selected Buildx builder, volumes, images or containers. The report shows bounded cache IDs/types, optional usage flags and dates. Missing observations remain unknown. Sizes and savings are not reported, and no cleanup action is available.

The daemon can calculate and save its internal size accounting during this query, even though Rydd omits sizes. Client limits do not bound daemon work, and cancellation does not prove that server work stopped. The same installed CLI/daemon trust and supported profile apply. Named-builder verification and installed-context acceptance remain open. See the [cache metadata contract](docs/cli.md#engine-embedded-cache-metadata) and [category evidence](docs/category-acceptance.md).

## Dismiss one exact saved finding

```sh
rydd ignore --preview -d /path/to/project --json FINDING_ID > /path/to/private/dismissal.json
rydd ignore --save --from /path/to/private/dismissal.json
rydd ignore --show DISMISSAL_ID
rydd ignore --undo DISMISSAL_ID
rydd report --candidates -d /path/to/project --include-dismissed
```

Review the preview before saving it. Use the finding's full `node-modules-v1:ROOT_ID:ENTRY_ID` reference from a manual candidate report. Put options before that reference. Keep the same global data directory throughout; the preview file contains private paths and belongs outside version control.

Dismissal hides only this exact historical finding from candidate reports and guided review. It preserves its saved identities, timestamps, age filter and qualified size evidence. Changed observations or a different age filter make it appear again, including a later rescan of unchanged files. `--include-dismissed` shows the eligible findings for inspection and names included dismissed references. Filtering keeps the original page and cursor; a fully dismissed page can be empty and still have a next page. Undo restores visibility without deleting the historical decision.

Show, undo and exact save retries work with the source and inventory offline. A new save requires matching saved inventory. No source files are read, moved or deleted. Dismissal creates no permanent exclusion, keep policy, read consent or cleanup permission. Up to 128 distinct immutable dismissal records fit in the private plan store; an undone exact record stays undone on save retries.

## Configure exclusions for later scans

```sh
rydd exclude --list
rydd exclude --add /path/to/project/cache
rydd exclude --remove /path/to/project/cache
```

These commands require existing valid configuration. Editing also requires the configured state and its existing writer lock. An active worker, including a paused worker, causes an edit to fail; stop it explicitly first. Missing state requires a separate `state init`. The editor creates no state and starts no scan or worker.

Use one mode and an absolute path. Paths are normalized without opening them; they need not exist. Add is unchanged when the exact exclusion is present. Remove deletes all exact matches while keeping other exclusions. A path that fully excludes a configured root is refused. These controls support up to 128 roots and exclusions and preserve the other setting values; an actual edit rewrites TOML formatting and comments.

Later invocations use the updated exclusions. Active manual scans and hash steps retain their captured settings. Saved reports, selections, dismissals and hash observations stay unchanged. Keep the same global data directory throughout. Add `--json` for the exact result and configuration digest. If a reply is lost or publication is uncertain, use `exclude --list` to inspect the configuration before retrying.

## View same-size saved files

```sh
rydd report --same-size -d /path/to/project
rydd report --same-size -d /path/to/project --min-size-bytes 1048576 --limit 20 --json
```

This is the first filter for duplicate discovery. It groups saved regular-file metadata by size without opening source contents. Equal sizes do not prove equal contents. The default minimum is 1 MiB. `--limit` bounds raw saved file rows per page, including rows later excluded from the report. Dependency trees named `node_modules`, skipped entries and disabled roots are excluded.

Page-local counts distinguish known device/inode objects, repeated saved aliases and unknown or conflicting identities. A size band can cross pages; boundary labels preserve even a single candidate at an edge. Use the returned cursor with the same minimum and data directory. A rebuilt inventory or changed minimum refuses the cursor. Scans can change membership between pages, so this is not a frozen export. Same-size reports do not provide verified duplicates, keeper choices or savings estimates.

A separate hashing store supports bounded full hashing with private checkpoints and conservative byte reservations. `rydd hashes [--work WORK_ID] [--json]` reads its existing saved observations, consent records and whole-selection budget. This command does not create, recover or resume work. Explicit consent and one-step reads are described below; worker integration remains pending. See [durable hashing limits](docs/inventory.md#explicit-file-observations-and-durable-hashing).

To group matching completed observations from that saved selection:

```sh
rydd hashes --groups
rydd hashes --groups --json
```

Groups contain equal recorded logical sizes and full SHA-256 hashes. They show exact paths, each historical check time and qualified saved identities, including aliases and conflicts. Counts show how much selected work has a full observation and how many completed observations have no match. The command reads saved hash storage only. The observations need not be simultaneous and do not establish current duplicates, a keeper or reclaimable space. `--groups` cannot be combined with `--work`.

To preview possible keeper and copy roles for an explicit matching subset:

```sh
rydd hashes --preview SELECTION_ID --keeper 1 2 3
rydd hashes --preview SELECTION_ID --keeper 1 2 3 --json
```

Use the full selection ID and work IDs from the saved reports. This example previews work 1 as a possible keeper and works 2 and 3 as possible copies for review. Every selected work item must have a complete matching historical size/hash, with no repeated or conflicting saved identity anywhere in the selection. The preview includes only the paths you name; it does not choose or add paths. Flags other than `--json` must precede the copy IDs. Preview, group and single-work modes cannot be combined.

This reads saved records only, so it works with source folders and inventory offline. It preserves observation sequences/times and whole-selection coverage, budget and saved consent. No decision is saved, no approval is available, and reclaimable space remains unknown. Current file verification and executable cleanup plans remain separate.

For a guided review of saved historical matches:

```sh
rydd review --hashes
rydd --data-dir /absolute/path/to/state review --hashes
```

1. Choose a numbered matching group.
2. Choose one numbered possible keeper.
3. Choose the other numbered paths to review as possible copies.
4. Read the exact preview and its whole-selection coverage and charges.

Every choice is explicit, even when there is only one group. `back` keeps the displayed evidence; `refresh` at the group menu reloads saved records. Rydd checks the selected saved evidence again before showing the preview and gives a finite command to reopen it. This text flow reads no source files, inventory or configuration. It saves no decision or consent, and cannot perform cleanup. Use the same data directory as the hashing selection. Directory/age options and JSON output are unavailable in this mode.

## Save and reopen a keeper/copy choice

```sh
rydd review --hashes --save-choice
rydd hash --save-choice SELECTION_ID --keeper 1 3 2 --json
rydd hashes --choice CHOICE_ID
rydd hashes --choice CHOICE_ID --json
```

The guided saving mode shows the exact preview, then asks you to type `save`, `back` or `quit`. Only `save` preserves the choice. The finite save command names the full selection ID and explicit work IDs; copy order is preserved. Saving rechecks the selected saved evidence and refuses changed or ambiguous observations. It opens no source files, inventory or configuration.

Keep the returned choice ID and the same data directory. Reopening works with source folders and inventory offline. Exact retries return the first saved ID, time and evidence. There is room for 128 distinct immutable choices per hashing selection, each at most 256 KiB. Existing choices are never replaced or discarded automatically.

A saved choice records historical roles only. It does not hide candidates, create a keep policy, approve another content read or permit cleanup. Current equality and reclaimable space remain unverified. If a reply fails after publication, the choice may already exist; inspect its full ID or repeat the exact save request. Private state and reports belong in a private data directory, outside version control.

## Check metadata for a saved choice

```sh
rydd hash --check-choice CHOICE_ID
rydd hash --check-choice CHOICE_ID --json
```

Use the full saved choice ID and the same data directory. This checks only its keeper and ordered copies against the frozen inventory and current path metadata. The folder comes from the saved choice; root and target overrides are unavailable. Current exclusions apply. Missing or changed inventory, files, ancestors, mounts or private storage block the check.

Files are checked one at a time without reading their bodies. Configuration and saved SQLite records can be read. A metadata match does not prove current content equality or authorize another content read or cleanup. Results are not saved; approval remains unavailable and reclaimable space unknown. In JSON, inspect `hash.status`: exit 0 can include a completed `blocked` result. `hashes --choice` continues to reopen saved evidence without live checks.

## Review a fresh-read request from a saved choice

```sh
rydd hash --request-choice CHOICE_ID
rydd hash --request-choice CHOICE_ID --json
```

This saved-only review preserves the exact keeper and ordered copies, their full frozen target evidence, and the historical context saved with the choice. It works with source files and inventory offline and does not load configuration. The request ID identifies this exact proposed scope; it is separate from a future job or read approval.

Nothing is saved or read from source files. No old approval is renewed and no hashing charges are changed. This request proves neither current equality nor safe cleanup.

## Save and reopen an independent fresh job

```sh
rydd hash --new-job-key
rydd hash --save-choice-job CHOICE_ID --job-key KEY
rydd hash --show-job JOB_ID --json
```

Generate and keep an explicit key before saving. Reuse that key with the same choice after a failed reply; the exact retry returns the first job, time and evidence. The same key with a different choice is refused. A new key creates a separate unapproved job.

The job preserves exact selected roles and frozen targets, with zero fresh progress and byte charges. Original hashing records and approvals remain unchanged. Archived choice context and original context at first job publication are labelled separately from new work. Saving and reopening work with source files, inventory and configuration offline. Up to 128 immutable jobs fit in one hash store, each at most 2 MiB.

Job creation does not authorize or start a read. Separate fresh consent and one explicit guarded step are described below. Keep the job ID, key and original private data directory. Failed output can leave a saved job; inspect the ID or repeat the exact key before creating another generation.

## Record separate fresh-job read consent

```sh
rydd hash --approve-job JOB_ID --confirm-content-read --max-day-bytes 1048576 --max-total-bytes 1048576
rydd hash --show-job-read APPROVAL_ID --json
rydd hash --run-job APPROVAL_ID
rydd hash --show-job JOB_ID --json
rydd hash --revoke-job APPROVAL_ID
```

Choose both caps explicitly. The example permits at most 1 MiB of fresh reservations per UTC reservation day and across the job's lifetime. Consent binds the exact job, key, request and ordered roles. It expires after 24 hours and fixes each step at at most 1 MiB. Original approvals and charges provide no fresh allowance.

Approval, show and revocation work with sources, inventory and configuration offline. They record or show consent without starting reads or recovering work. Each explicit `--run-job` performs at most one 1 MiB step under one five-second deadline and can recover only that exact job. Running requires the frozen inventory, current configuration and matching live metadata. Each fresh job starts new SHA progress and byte charges; old progress is never reused. Saved job reports compare each fresh copy observation with the selected keeper, showing historical matches, differences, incomplete work or blocked work. These comparisons do not prove current equality or safe cleanup. Exact retries preserve the first limits and expiry; expired or revoked consent cannot renew. Saved views do not evaluate current read permission. Consent never authorizes cleanup.

## Save an exact hashing proposal

```sh
rydd report --same-size -d /path/to/project --json > /path/to/private/same-size.json
rydd hash --select -d /path/to/project --from /path/to/private/same-size.json 12 18
rydd hash --show SELECTION_ID --json
```

Use the saved file IDs from that report and the returned selection ID. The report file contains private paths. This captures one immutable unapproved proposal and shows its complete saved evidence. Changed rows or a different selection are refused. Selection and display open no selected file contents, start no scanner and recover no hashing work.

## Approve and read one hashing step

After reviewing the exact proposal, choose explicit byte limits:

```sh
rydd hash --approve SELECTION_ID --confirm-content-read --max-day-bytes 8388608 --max-total-bytes 33554432
rydd hash --run APPROVAL_ID
rydd hashes
rydd hash --revoke APPROVAL_ID
```

Replace the example IDs with the saved selection and returned approval IDs. Keep the same global `--data-dir` throughout. Approval permits full-file hashing of that exact selection for 24 hours, within both limits. It opens no source files. The limits cover byte reservations, including canceled or interrupted attempts and charges recorded before approval. Retrying approval cannot extend expiry or raise limits.

Each explicit run attempts one step of at most 1 MiB. Rydd rechecks consent, limits, current saved inventory, configuration exclusions and live paths before publishing checked progress. Run again explicitly to continue later; there is no automatic loop or retry. Opening the writer can recover interrupted accounting even if the new read is refused. Saved readers do not perform that recovery. Revocation works offline and blocks later reservations after taking the writer lock; it cannot interrupt a step that already holds the lock.

Completed hashes are historical observations. They do not prove that files are still equal, choose a keeper automatically or authorize cleanup. Hashing currently supports one immutable selection per state directory. Background hashing, current duplicate verification, savings estimates and cleanup remain pending.

## Preview an exact selection

```sh
rydd plan --preview -d /path/to/project --min-age-days 30 node-modules-v1:1:42
```

Replace the example ID with a finding ID from the candidate report for the same inventory. Select 1–20 unique IDs; put flags before IDs. The preview rechecks saved eligibility and shows exact targets, qualified sizes and the checks needed before a future quarantine action. Project activity remains unconfirmed. It is read-only, has no approval ID and cannot execute cleanup. Quarantine would support recovery but would not free disk space; purge requires a separate future approval.

## Save a selection for later review

For a guided text review of an existing manual scan:

```sh
rydd review -d /path/to/project --min-age-days 30
```

1. Read the numbered candidates and their saved size qualifications.
2. Enter unique row numbers, such as `1,3`. Check the selected paths shown again.
3. Type `save` to preserve that exact unapproved selection, or `back` or `quit`.

Each page has at most 20 candidates. `next` replaces it with a later page; `refresh` returns to the first page. Numbers reset, and selections do not carry between pages. Empty pages show rejection counts and a command to read the same saved page, preserving the folder, state location, age filter and cursor. Unselected folders stay unchanged. This does not save a keep decision or hide candidates from later reports. EOF or Ctrl+C before saving ends review without publishing a plan.

The page is frozen before input. If the selected inventory evidence changes before saving, review refuses it and asks you to start again. Incomplete but unchanged evidence can be saved as a qualified historical record. Saving never records cleanup consent, moves files or verifies current source contents. Afterward, start another scan explicitly when needed. `review` prompts in text mode; use the finite report and plan commands for JSON workflows.

```sh
rydd plan --save -d /path/to/project --min-age-days 30 node-modules-v1:1:42
rydd plan --show PLAN_ID
rydd plan --check PLAN_ID -d /path/to/project
rydd plan --verify PLAN_ID -d /path/to/project
rydd plan --inspect PLAN_ID -d /path/to/project
rydd plan --inspect PLAN_ID --tree -d /path/to/project
rydd plan --capture PLAN_ID -d /path/to/project
rydd plan --compare OBSERVATION_ID -d /path/to/project
```

Use an actual finding ID, then replace `PLAN_ID` with the returned saved plan ID. Keep the same global `--data-dir` if you override it. The saved record contains the exact selection and evidence from one inventory snapshot. Later scans cannot add targets to it. Reopening works while the source folder or its inventory is offline.

`--check` compares the selection with its saved inventory and reports changed or incomplete evidence. Use the original manual scan directory, or omit `-d` for configured inventory. The check reads saved observations only: it can match while the source folder is offline. A match does not establish safe cleanup. Scanning again changes inventory revisions and requires a new selection for review.

`--verify` also reads current metadata for the selected root, path ancestors, dependency directory and `package.json`. It blocks changed, missing, excluded or symlinked paths and descendant mount boundaries. Use a canonical scan path: root aliases accepted by scanning are rejected by this stricter check. It does not read dependency contents, inspect the whole subtree or establish that cleanup is safe. No files are moved, and the result cannot authorize a later move.

`--inspect` explicitly reads bounded `package.json` and `package-lock.json` contents after the same path checks. The first supported format is a narrow subset of npm lockfile versions 2 and 3 with public registry dependencies. It compares root dependency declarations and reports exact-byte digests. Unsupported managers, local sources, install scripts and project configuration need separate review. The command does not check installed dependency contents, detect local edits or prove that reinstall will succeed. Inspection describes this request; use capture below to save a separate observation for later comparison. See the [input inspection contract](docs/cli.md#project-input-inspection).

Add `--tree` to also inspect up to 10,000 installed entries through recursive metadata listings. It accepts lock-listed package directories, ordinary files and supported internal `.bin` links; it blocks unknown boundary entries, hardlinked files, special objects, exclusions and mount boundaries. Counts and a metadata digest describe the observed layout, including two matching metadata passes. Ordinary dependency contents remain unopened. This does not establish a complete or unmodified install, and large or unsupported trees need separate review. Input-only inspection stays unchanged.

`--capture` repeats the full input/tree check and saves one immutable observation for the exact plan only when all targets pass. It saves digests and counts, with no dependency file bodies or descendant filename list. `--show` exposes the observation ID and observed time, even while source folders or inventory are offline. Repeating capture returns the same ID and time when the observations still match. Changed observations require a new saved selection; an existing baseline is never replaced.

Use the returned `OBSERVATION_ID` with `--compare` to repeat the same checks and report a match, changed evidence or a block. Comparison writes no records. A match means the checked input bytes and tree metadata agree with that capture; it cannot detect edits made before capture or prove that dependencies can be safely reinstalled. Existing review consent is separate and remains non-executable.

Plans are private local records in a separate database under the global data directory. Saving does not approve cleanup, move files or free space. Review consent can be recorded separately as described below. Cleanup remains unavailable; Rydd does not verify project activity or regeneration safety.

Saving, checking, live verification, inspection, capture and comparison require inventory schema 9. Reports and previews can still read schemas 4–8. Existing configured state can be migrated with `state init` while its worker is stopped; a manual inventory migrates on the next explicit scan, or with `measure` when it is compact and has no pending scan. Writer commands can migrate plan-store schemas 1/2 to schema 3; capture publishes its observation atomically, with existing plans and consent unchanged. Read-only `--show`, `--check`, `--verify`, `--inspect`, `--compare` and `--preview` do not migrate storage.

## Read a saved recovery record

```sh
rydd journal --show INTENT_ID
rydd journal --show INTENT_ID --json
rydd journal --observe INTENT_ID --json
```

This reads a preparation record and its recorded history without inspecting either filesystem location. The journal foundation is available for development fixtures; there is no CLI command to prepare or execute a move. Saved outcomes are caller-supplied records, not proof of current state. Missing results remain unknown and cannot permit retry. `--observe` separately checks metadata at the two recorded locations. It writes no history and keeps the cleanup outcome unknown, even when metadata agrees. Quarantine, restoration and purge remain unavailable. See the [journal contract](docs/cli.md#saved-recovery-journal).

## Record or revoke review consent

After reviewing the exact saved selection with the project owner:

```sh
rydd plan --approve PLAN_ID -d /path/to/project --confirm-project-review --confirm-quarantine
rydd plan --show PLAN_ID
rydd plan --revoke PLAN_ID
```

The two confirmations mean the owner reviewed project activity, local dependency edits and reinstall requirements, and accepts same-filesystem quarantine without permanent deletion. Consent lasts 24 hours. The saved inventory must still match; changed or incomplete evidence requires a new selection. Retrying keeps the same approval and expiry. Revocation works even when the source folder and inventory are offline.

This version records **review consent only**. It cannot execute cleanup. A future cleanup command will require renewed approval after live checks and recovery are implemented. `--show` displays the review status separately from the original saved evidence. Revoked or expired approvals cannot be renewed for the same plan; save a new selection for another review.

Native Linux can observe an already loaded idle service with `service runtime-status --executable /absolute/path/to/rydd`. Manager state labels are historical evidence; running, stopped and application readiness remain unknown. The command starts or loads no unit. macOS currently returns unsupported without a manager probe. See the [runtime observation contract](docs/cli.md#existing-loaded-service-observations).

## Project map

- [PLAN.md](PLAN.md): architecture, safety requirements and acceptance gates.
- [PROGRESS.md](PROGRESS.md): completed work, remaining tasks, validation evidence and the next handoff.
- [Category acceptance](docs/category-acceptance.md): supported read-only profiles, their evidence and the gates still open.
- [CONTRIBUTING.md](CONTRIBUTING.md): Docker development and native validation.
- [AGENTS.md](AGENTS.md): working instructions for coding agents.
- [SQLite decision](docs/decisions/001-sqlite-driver.md): reproducible driver comparison and resource measurements.

## Development with Docker

Host requirements: Git, a POSIX shell, and a running Docker engine with Compose v2+. No host Go installation, package manager, database server or task runner is required. Development containers run only when requested.

```sh
git clone https://github.com/bjornarhagen/saga-rydd.git
cd saga-rydd
./scripts/dev check
./scripts/dev run --help
./scripts/dev build-all
```

The first command builds the development image. Later runs reuse that image and per-user Go caches in a project-specific Docker volume. The container mounts only this checkout, not your home directory or Docker socket. Build output goes to the ignored `dist/` directory. The wrapper uses your host UID/GID to avoid root-owned files on Linux.

| Command | Purpose |
| --- | --- |
| `./scripts/dev check` | Check formatting, run vet and tests, build the CLI |
| `./scripts/dev fmt` | Format Go source |
| `./scripts/dev test` | Run tests |
| `./scripts/dev race` | Run Go race detection on the container platform |
| `./scripts/dev run --help` | Run the CLI inside Docker |
| `./scripts/dev build-all` | Cross-build macOS/Linux arm64/amd64 binaries |
| `./scripts/dev sqlite-check` | Run both SQLite candidates' transaction/crash checks |
| `./scripts/dev sqlite-bench -rows 1000000` | Compare drivers using synthetic metadata |
| `./scripts/dev shell` | Open a development shell |
| `./scripts/dev rebuild` | Refresh/rebuild the development image |
| `./scripts/dev cache-clear` | Explicitly remove this project's development caches |

The installed app will run **natively**, using launchd on macOS or a user service on Linux. Docker is the development environment. Linux containers cannot establish macOS permissions, APFS behavior, battery handling or LaunchAgent correctness; those need native checks. CI runs on both macOS and Linux.

## Try the current foundation

Inside Docker, keep disposable state in this checkout so it survives between commands:

```sh
./scripts/dev run --data-dir /workspace/.local/demo init --root /workspace
./scripts/dev run --data-dir /workspace/.local/demo config check
./scripts/dev run --data-dir /workspace/.local/demo status --json
```

This records the selected root but does not scan it. `--data-dir` is a global option and comes **before** the command. Use a dedicated directory: existing shared directories and symlinked state/config files are rejected. `init` never overwrites an existing config. After editing the config, `state init` validates it and updates registered roots while preserving existing inventory; it also retries a failed initial database setup.

Without `--data-dir`, native macOS uses `~/Library/Application Support/saga-rydd`; Linux follows XDG config/state directories. Explicit roots and exclusions are absolute paths or start with `~/`; exclusions refer to subtrees, not glob patterns. Unknown settings, overlapping roots and invalid budgets are rejected. Roots may be offline at configuration time. Experimental scanning establishes a root/filesystem fingerprint, rejects available root aliases, and retains saved inventory when a root is unavailable or its identity changes. Budget settings are saved; dispatch cadence, a daily batch cap, charged background-scanner API allowances, cooperative work timeouts, child-entry pacing and WAL backpressure are implemented so far. `scan.metadata_attempts_per_day` defaults to 20,000,000 charged attempts, with full conservative reservations and no refunds; interruptions remain charged. This is scanner-only accounting, not physical I/O or a limit for manual scans. `scan.metadata_per_second` currently paces child-entry inspections; traversal and database operations are not yet rate limited. `scan.max_state_bytes` defaults to 1 GiB and defers experimental source work when private DB+WAL length observations reach that threshold or are unavailable. Pending work and owner history are preserved. This is admission pacing, not a hard space ceiling; see the [state threshold contract](docs/cli.md#configured-inventory-state-threshold).

### Run and control the worker

In one terminal:

```sh
./scripts/dev run --data-dir /workspace/.local/demo daemon
```

In another terminal, using the same checkout:

```sh
./scripts/dev run --data-dir /workspace/.local/demo status --json
./scripts/dev run --data-dir /workspace/.local/demo pause
./scripts/dev run --data-dir /workspace/.local/demo resume
./scripts/dev run --data-dir /workspace/.local/demo stop
```

`daemon` runs in the foreground until stopped, interrupted or sent SIGTERM. On a native build, replace `./scripts/dev run` with your binary and use a native data path. Service installation will follow in P1-09. The worker stays idle unless `--experimental-scan` is supplied each time it starts.

Pause is saved before acknowledgment and survives restart. It cancels an active chunk cooperatively; status shows whether that chunk is still draining. `stop` acknowledges a shutdown request; wait for the daemon process to exit before restarting or running `state init`. Reports work while the worker is stopped. A second worker or state writer using the same state directory is rejected. Configuration changes take effect on the next worker start.

Controls use a private Unix socket with same-user peer checks. Docker commands share a small runtime volume so separate development containers can communicate. Native sockets use a private directory under `/tmp`; set the same short, absolute `RYDD_RUNTIME_DIR` for daemon and clients if overriding it. Keep application state on a local filesystem. See [worker design](docs/worker.md) for recovery and remaining scheduler work.

## Preview a user-service descriptor

```sh
rydd service preview --executable /absolute/path/to/rydd
rydd --data-dir /path/to/private/state service preview --executable /absolute/path/to/rydd --json
```

This proposes an idle-only launchd or systemd user descriptor. It binds the exact executable, configuration, state and control-runtime paths. It renders plain `daemon`, with no scanner. It opens no configuration, state or executable contents, writes no descriptor and invokes no service manager. The output declares a supported profile; installed support and future executable identity are unverified.

Installing a descriptor in a login service directory can affect future logins. Exact artifact installation/status is available below; explicit start/stop requests are available below; exact descriptor removal is available below. The worker would update normal root/recovery bookkeeping when started. Scheduling hints supplement resource pacing; they do not prove effective CPU/I/O priority or hourly limits. See the [service preview contract](docs/cli.md#user-service-descriptor-preview).

## Install or inspect an idle service descriptor

```sh
rydd service install --executable /absolute/path/to/rydd
rydd service status --executable /absolute/path/to/rydd
```

Use the same executable and global data directory on every invocation. These commands publish or inspect one exact descriptor. They preserve user configuration, inventory/history and the selected executable. Exact retries are unchanged; foreign/different artifacts and unsafe paths refuse. A failed reply can follow publication: inspect status before retrying.

macOS publication can start the idle worker at a future login. Linux install checks current manager directory visibility but does not enable/start the unit. No scanning command is issued. Artifact presence and manager visibility do not prove runtime readiness. Connecting to Linux’s selected local broker socket can activate the broker; destination activation is suppressed. Native installed-manager/login/logout acceptance and verified runtime state remain open. See [installation/status scope](docs/cli.md#exact-user-service-artifact-installation-and-status).

## Request service start or stop

```sh
rydd service start --executable /absolute/path/to/rydd
rydd service stop --executable /absolute/path/to/rydd
```

Use the exact installed spec and selected data directory. These send one explicit request for the idle service. A queued Linux job or successful macOS client reply is historical acceptance evidence; running/stopped state remains unknown. Linux checks declared loaded settings; macOS addresses the fixed current-user managed label without loaded-origin proof. Normal manager dependencies and preflight bookkeeping can be affected. Stop preserves the descriptor and its future-login effects. Inspect an uncertain reply before deciding to retry. See the [request contract](docs/cli.md#explicit-idle-service-start-and-stop-requests).

## Change the selected Linux login link

```sh
rydd service enable-login --executable /absolute/path/to/rydd
rydd service disable-login --executable /absolute/path/to/rydd
```

On native Linux, these commands create or remove only the fixed `default.target.wants` link to the exact existing descriptor. They preserve other links, the descriptor and user data. A matching manually created link at this selected name is also in scope; its origin is unknown. No start, stop or reload request is sent. Preflight can load the unit and change manager bookkeeping. Effective enablement, running state and next-login behavior remain unverified. Disable this link before removing its required descriptor when wanted. See the [selected-link contract](docs/cli.md#selected-linux-login-dependency-link).

## Remove the managed service descriptor

```sh
rydd service uninstall --executable /absolute/path/to/rydd
```

Use the exact installed scope. This removes only the managed descriptor and preserves the executable, configuration, saved history, quarantine, directories and lock. It sends no stop, disable or reload request. A loaded service can continue running. The Rydd stop adapter requires the descriptor, so request stop before uninstall when wanted; an accepted request does not prove shutdown. See the [removal contract](docs/cli.md#exact-service-descriptor-removal).

## Background scan revisits

The experimental scanner admits at most 32 configured roots and rotates bounded source or saved-maintenance turns across them. It retains each root's unfinished directory stream and one shared entry pacing clock. It schedules one root listing at least 24 hours after its last completed root listing, once that root's unfinished work drains. A healthy root can revisit while another root waits on an error. Saved due times survive restart; controls and resource gates still apply. Root listing completion does not prove full-tree coverage. Manual scans keep their explicit invocation flow. See the [revisit contract](docs/cli.md#periodic-experimental-scan-revisits).

The optional `scan.adaptive_revisits = true` setting tracks historical metadata across each saved pass and reconciliation. Changed or uncertain completed passes use a daily interval. Two consecutive completed passes without an observed metadata change may use seven days. The default remains false. Restart the worker to apply a configuration change; scanning still requires `--experimental-scan`. Cached status does not inspect source folders or establish inactivity or cleanup safety. See [adaptive scheduling](docs/cli.md#opt-in-adaptive-historical-metadata-revisits).

## Try the experimental scanner

Try experimental scanning on a disposable fixture with `./scripts/scanner-smoke ./dist/rydd-darwin-arm64` after `./scripts/dev build-all` (choose the binary for your host). Inside Docker, run `./scripts/dev shell -c './scripts/scanner-smoke ./dist/rydd'` after `./scripts/dev check`. The script creates five synthetic entries, scans them with a short fixture cadence, checks results and stops its worker. No ordinary file contents are opened, hashed or deleted. Keep broad personal-directory scans disabled until P1-06 resource enforcement.

Each batch contains at most 128 child observations. Throttle waits yield partial batches with bounded pending names so slow entry rates can make progress. A restart re-enumerates the interrupted directory with a fresh generation and idempotent upserts. Completed directory passes have reconciliation markers; old observations remain for later stale-entry processing. Counts are historical observations, not a whole-tree percentage or reclaimable space. See [inventory design](docs/inventory.md) for filesystem support and limitations.

## Design commitments

- Slow, resumable work with explicit CPU and I/O budgets.
- Local SQLite state and readable TOML configuration.
- Explainable, freshness-aware findings with realistic savings estimates.
- Scanning only by default; automatic cleanup requires an explicitly enabled policy.
- Quarantine and restoration before permanent file removal where supported.
- Exact duplicates require full verification; matching samples never authorize deletion.

See the plan for the full scope. A public release is gated on resource benchmarks, cleanup recovery tests and a multi-week native soak.
