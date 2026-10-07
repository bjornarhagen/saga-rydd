# ADR 003 — Source namespace boundary before cleanup

- Date: 2026-10-08
- Status: accepted execution constraint; production source moves remain unavailable
- Scope: P4-03a feasibility decision for macOS/Linux quarantine and restoration

## Decision

Do not implement production moves from ordinary user-owned project folders with the current rename primitives. No enforceable boundary has been demonstrated that keeps the approved source object and its ancestor scope bound to the operation. Keep cleanup disabled while the application provides read-only review and separately consented hashing.

A future executor must either use a demonstrated conditional-source operation or operate within a namespace whose writers are excluded by an enforceable authority boundary. Both approaches must preserve the selected-object invariant through the operation. Destination exclusivity, held descriptors, metadata checks, approval records and recovery journals are necessary supporting mechanisms; they do not supply this missing authority.

This is a decision about the examined APIs and the current product scope. It is not proof that every possible platform or filesystem mechanism lacks a solution. New evidence can reopen the decision without weakening the invariant. This ADR adds no executor, probe, permission change, source read or executable consent.

## Actor and scope model

Here, a namespace means directory names and their links to filesystem objects. Rydd currently runs with the invoking user's credentials. Selected roots are ordinary development folders. Editors, package managers, build tools, synchronization clients and another user command can change their names concurrently under the same credentials. They need not follow Rydd's writer lock, and an accidental replacement is sufficient to expose the gap.

The required guarantee is precise: an operation must not move a replacement object that was never selected and approved. A held parent must also remain within the approved namespace; retaining its identity after it is renamed elsewhere is insufficient. The model includes non-cooperating ordinary writers. Closing an editor or confirming that a project is idle does not enforce their exclusion.

Private application storage is a separate boundary. Mode `0700` restricts other accounts; it does not distinguish Rydd from another process with the owner's credentials. Owners can change their file modes on both platforms. Accordingly, making a folder private under the same user does not establish exclusive namespace control. Rydd's [private-storage helpers](../../internal/localfs/private.go) deliberately do not change scan-root permissions. [Linux chmod documentation](https://man7.org/linux/man-pages/man2/chmod.2.html), [Apple chmod documentation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/chmod.2)

Any future boundary must state which credentials, administrators and filesystem services are trusted. Credential isolation would require a separate design; this ADR does not establish authentication against deliberate same-user state rewrites or a hostile privileged administrator. Filesystem or mount changes and unsupported operations must remain explicit refusals or uncertain outcomes.

## Required authority

| Boundary | Required guarantee at the operation | Current evidence | Missing authority |
| --- | --- | --- | --- |
| Destination name | Do not overwrite an existing object; use the exact approved destination parent. | Native exclusive rename refuses tested collisions. | Destination-parent namespace continuity still needs an enforceable boundary. |
| Source name | Move only the reviewed object, or refuse before changing any namespace. | A held source descriptor retains the reviewed object; the rename still resolves its name separately. | An atomic expected-object condition or exclusion of competing source-name writers. |
| Ancestor scope | Keep both held parent chains in the approved root and mount scope throughout the operation. | No-follow held chains and repeated checks observe identities and mounts. | Prevention of ancestor relocation or equivalent operation-time scope enforcement. |

Content and policy checks remain separate requirements. Closing this namespace gap would not prove regeneration safety, current duplicate equality, reclaimable space or power-loss durability.

## Existing native evidence

The [rename experiment](../../experiments/rename/README.md) uses Linux `renameat2(..., RENAME_NOREPLACE)` and macOS `renameatx_np(..., RENAME_EXCL)` with held parent descriptors. Its generated native macOS/Linux evidence already demonstrates the failure:

1. Rydd observes and opens the selected object.
2. A competing writer parks that object and puts a different object at its name.
3. Exclusive rename moves the replacement to the empty destination.
4. The held source descriptor still identifies the original. A later comparison detects the mismatch after the namespace changed.

`TestHeldSourceDoesNotGuardNamedRename` covers files and directories. `TestHeldParentsSurvivePathReplacement` also moves the held parents away and replaces their former paths. The operation continues within those held parents. Repeating either successful probe cannot turn its observed limitation into an execution guarantee.

The platform interfaces match those results. Linux `RENAME_NOREPLACE` protects an existing destination; `RENAME_EXCHANGE` swaps names. Apple's `RENAME_EXCL` protects an existing destination and `RENAME_SWAP` swaps names. The examined rename signatures do not accept an expected source identity. These facts support the decision for the current wrappers, not an exhaustive claim about all APIs. [Linux rename documentation](https://man7.org/linux/man-pages/man2/rename.2.html), [Apple rename documentation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/rename.2)

## Candidates and limits

| Candidate | Useful property | Decision for the current scope |
| --- | --- | --- |
| Another final check, then rename | Detects some changes before the syscall. | Refuse as a solution: a writer can replace the name after that check. |
| Exclusive rename or atomic name exchange | Destination collision protection or atomic swapping. | Neither examined operation conditions the source on the reviewed object's identity. Exchange also changes both names. |
| Held no-follow paths and resolution constraints | Reject unwanted path traversal and retain opened objects. | Retain for validation; they do not freeze source names or a held parent's location. |
| Application lock or user promise of inactivity | Coordinates participants that cooperate. | Not an authority boundary against the actors in this model. |
| Permissions or ownership transfer | Could form part of a boundary with separate writer credentials. | Same-user privacy is insufficient. Changing existing roots or adding privileged custody is a new product and security design, not an authorized implementation shortcut. |
| Copy from a held descriptor, then remove the source name | Can preserve a copy of the opened object. | Removal still needs the source-name guarantee. A copy is neither a safe move nor reclaimed space. |
| Kernel delegation | Some Linux kernels can block conflicting namespace operations temporarily. | Relevant research candidate; no portable boundary that remains effective during the operation is demonstrated below. |

Linux `openat2` resolution flags constrain an open's traversal. Apple's rename documentation also describes `RENAME_NOFOLLOW_ANY` and `RENAME_RESOLVE_BENEATH`. These are useful path constraints, but they do not supply an expected-source identity or prove that the starting directory remains at its approved historical path. That latter conclusion follows from the distinct guarantee required here and the held-parent probe. [Linux openat2 documentation](https://man7.org/linux/man-pages/man2/openat2.2.html), [Apple rename documentation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/rename.2)

Both platforms document `flock` as advisory. It does not enforce cooperation by unrelated writers. Linux's older `F_SETLEASE` is limited to regular files and documents conflicting opens and truncation, not a directory namespace boundary. [Linux flock documentation](https://man7.org/linux/man-pages/man2/flock.2.html), [Apple flock documentation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/flock.2), [Linux lease documentation](https://man7.org/linux/man-pages/man2/F_GETLEASE.2const.html)

Linux `F_SETDELEG` is a different, newer candidate. Its documentation identifies Linux 6.19, file and directory delegations, and kernel blocking of conflicting rename/unlink or directory-entry changes. It also permits forced release after the lease-break timeout and refuses unsupported filesystems. It must not be described as merely advisory or confused with `F_SETLEASE`. [Linux delegation documentation](https://man7.org/linux/man-pages/man2/F_GETDELEG.2const.html)

The reviewed Linux 6.19 code breaks directory/source delegations in the rename path, releases namespace locks while waiting, then retries lookup. The ordinary userspace delegation manager supplies no owner-specific bypass for that rename. **Inference:** acquiring a delegation and then releasing it, waiting for its forced release or polling its state does not establish an atomic reviewed-source move. A proposed protocol must demonstrate how its own operation commits while every required guard remains effective, including ancestor guards and suspension beyond the break timeout. No such protocol or macOS equivalent was demonstrated in this leaf. There is no new native probe or support claim. [Linux 6.19 rename implementation](https://github.com/torvalds/linux/blob/v6.19/fs/namei.c), [Linux 6.19 delegation implementation](https://github.com/torvalds/linux/blob/v6.19/fs/locks.c)

## Journal and approval consequences

The existing [journal contract](../../internal/plans/journal.go) is `preparation_only_v1`. Its identities are supplied evidence, and its generation is a saved inventory generation, not a platform inode-generation token. An intent or result records history; it cannot make a named-source operation conditional.

`journal --observe` reports bounded current metadata at recorded recovery locations. Missing results and ambiguous locations remain unknown. It grants no retry or restoration permission and does not establish which object moved. Post-move mismatch detection, attempted compensation and successful parent sync calls do not establish pre-move safety or power-loss recovery.

Saved review approvals, keeper/copy choices, metadata matches and fresh historical hashes cannot authorize a future executor. Read consent remains content-read authority only. General approval to continue development does not approve source moves, permission changes, replacement objects or automatic cleanup. A future executable contract must be new, exact and separately approved after its boundary is verified.

## Owner decisions available after independent work

These are concrete scope decisions to present together when owner input resumes. No option is selected by this ADR except the current no-go.

| Option | Decision required | Consequence |
| --- | --- | --- |
| Retain the read-only product for ordinary projects | Accept cleanup as deferred while reports and joint testing continue. | Keep current selected-object invariant and cleanup unavailable. This is the current safe endpoint. |
| Investigate a restricted namespace with separate writer authority | Choose eligible objects, trusted credentials, custody setup and supported platforms. | Requires a new bounded architecture review. Objects created inside that boundary may be candidates; adopting an existing project must not reuse the unsafe source handoff. Ordinary project cleanup remains unavailable. |
| Commission a platform-specific conditional-operation investigation | Choose the exact kernel/OS/filesystem support scope and provide native validation access. | Evaluate a credible operation/protocol against the criteria below. Other platforms and ordinary unsupported folders remain read-only. This does not authorize deployment or existing-source operations. |

An acknowledgement that races can happen, or permission to move whatever occupies a path, is not a valid way to satisfy the selected-object invariant. Owner input cannot substitute for the missing enforcement evidence.

## Conditions for reopening the execution gate

A candidate must document its exact API, platform/filesystem support, actor model and continuous authority over the source, destination and ancestors. It must refuse unsupported cases without changing source data, permissions or namespaces. It must demonstrate that competing replacements cannot move an unreviewed object, including during guard acquisition, the syscall, forced release, cancellation, process suspension and recovery.

Only after that argument is credible should a narrowly scoped generated native experiment test the candidate. Require macOS/Linux evidence for every claimed supported platform, independent identities/content checks, ancestor-relocation and collision cases, and explicit uncertain results after process loss. Never use original folders to establish feasibility. Crash tests remain distinct from physical power-loss durability.

Even a passing boundary experiment would leave executable consent, action-time content/policy checks, bounded journal settlement, restore collisions, full disks, quarantine/purge accounting and release gates open. Until those dependencies are verified, the executor and cleanup capability remain unavailable.

Primary API documentation and Linux 6.19 source were checked on 2026-10-08. This leaf adds a decision record only; prior native experiment results are reused with their original scope and limitations.
