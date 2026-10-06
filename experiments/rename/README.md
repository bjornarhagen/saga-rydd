# Native no-overwrite rename experiment

This isolated test package probes native APIs for a future same-filesystem quarantine and restore design. It has no application API, approval handling or executor. All tests create small temporary fixtures. They never scan or move an existing project, and they never fall back to ordinary overwriting rename or cross-filesystem copy.

| Platform | API | Destination protection |
| --- | --- | --- |
| Linux | `unix.Renameat2` with `RENAME_NOREPLACE` | An existing destination produces an error |
| macOS | `unix.RenameatxNp` with `RENAME_EXCL` | An existing destination produces an error |

These wrappers already exist in the pinned `golang.org/x/sys` dependency. No raw syscall, C toolchain or new dependency is needed. Both paths use held parent directory descriptors and single-component relative names. The experiment exercises files and directories, including tab/newline names.

The tests verify identity and fixture contents after a move and a return move. Destination collisions preserve both objects, including an existing directory or dangling link. A restore collision retains the quarantined object and the new original-path object. Parent `fsync` is attempted before and after each rename. Injected sync errors retain their cause and stage: a pre-rename error prevents the operation; a post-rename error reports the error even though the rename returned success. Invalid descriptor and missing-source errors also remain visible. A failed syscall is not general proof that a filesystem outcome is known.

The deterministic source-replacement test exposes a critical limit: after observing and opening the selected object, a competing writer can replace its name. The rename then moves the replacement, while the held descriptor still refers to the original. Post-move identity comparison detects the mismatch after the namespace already changed. Destination exclusivity and held parent descriptors therefore do not enforce an observed source inode. Renaming a held parent also leaves the operation bound to that parent even when its former path is replaced, so ancestor scope requires separate validation. Do not use these probes as an executor.

## Run

```sh
./scripts/dev shell -c 'go test ./experiments/rename -count=1 -v'
./scripts/dev shell -c 'GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -o dist/rename-darwin-arm64.test ./experiments/rename'
./dist/rename-darwin-arm64.test -test.v
```

The first command is container evidence only. Run the matching compiled test binary on its native platform. Native macOS/Linux CI runs the package with verbose sanitized observations. Tests fail if exclusive rename or ordinary directory sync is unavailable; there is no fallback. The macOS probe separately reports whether `F_FULLFSYNC` succeeds for the fixture directory.

## Observed results and limits

Initial local tests passed on native macOS arm64 APFS and on a Docker Linux overlay fixture. Exclusive collisions preserved identities and contents. The source-replacement gap was reproduced for both files and directories. Device/inode/type persisted after rename and return; ctime changed in these runs. APFS directory `fsync` and `F_FULLFSYNC` returned success. Native Linux CI evidence is pending for this change; container execution alone does not establish it.

Successful sync calls are API smoke evidence. They do not establish power-loss ordering or recovery, test every filesystem/kernel/drive, validate network/provider mounts or simulate a full disk. The experiment does not sync an arbitrary dependency tree's contents. An unsupported syscall, filesystem flag or directory durability operation must block future production moves. The named-source identity gap also remains an execution-design blocker; intent records and preflight checks cannot close it by themselves. No cleanup capability is enabled by this experiment.

## Primary API references

- [Linux `rename(2)`](https://man7.org/linux/man-pages/man2/rename.2.html) documents descriptor-relative resolution, `RENAME_NOREPLACE`, filesystem support and error/outcome caveats.
- [Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html) explains the separate parent-directory synchronization needed for directory entries.
- [Apple's `rename(2)` source](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/rename.2) documents `renameatx_np`, relative descriptors, `RENAME_EXCL` and unsupported-filesystem errors.
- [Apple's `fsync(2)` source](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fsync.2) distinguishes `fsync` from the stronger drive-cache request made by `F_FULLFSYNC`.
- [Apple's exclusive-renaming volume capability](https://developer.apple.com/documentation/foundation/urlresourcevalues/volumesupportsexclusiverenaming) makes clear that support depends on the volume.
