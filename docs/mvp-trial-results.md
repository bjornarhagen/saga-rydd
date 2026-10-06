# Lessons from the controlled trial

Only general product lessons are published here. Project-derived measurements, paths and raw reports remain private, as requested by the user.

## Implemented response: explain empty reports

An empty candidate page should explain whether dependency folders failed the age rule, lacked supported project evidence, were skipped, or had incomplete parent observations. It should also distinguish an exhausted saved inventory from a page with more results to inspect.

P2-05a now provides bounded per-page selection diagnostics in human and JSON output, using mutually exclusive reason counts and explicit coverage. It preserves nested dependency suppression, the existing age rule and review-required classification. Owner feedback has now calibrated known used/unused examples in the controlled scope. Do not change thresholds merely to produce findings.

## Other lessons

- Keep partial, stale and unknown sizes prominent. A short scan is not a complete inventory, and observed allocated bytes are not guaranteed reclaimable space.
- Evaluate the tradeoff between dispatch pauses and progress through many small directories before changing resource defaults.
- Optional owner feedback helps assess the age rule. Distinguish active development, projects still run or built, and projects no longer used; not working on a project now does not establish that it is unused. Keep owner statements separate from saved timestamps and leave dependency edits unknown without specific evidence. Feedback does not select or approve cleanup targets.
- Keep trial state and raw reports separate from public documentation. Publishing general lessons does not authorize disclosure of measurements or project details.
- Guided review needs the same empty-page explanation as finite candidate reports. Keep the exact folder, private state, age and current-page cursor in any suggested command; a generic command can open a different inventory or page. P4-01e shares the bounded rejection summary on empty guided pages while preserving frozen nonempty selection evidence.
- Separate exhausted saved report pages, a drained scan queue and completed saved size calculations. A bounded trial can inspect every saved page while filesystem work remains. Foreground interruption retains partial evidence and pending progress; resume the same saved work before assessing completed coverage. Report and navigation commands must not start another pass.
- A completed permitted pass can still have partial broader size coverage because protected/excluded folders were deliberately left unlisted. Explain that distinction rather than equating every partial size with queued work.
- A lower exploratory age cutoff can agree with known unused examples, while a very short cutoff can also include known used projects. These examples calibrate the filter; they do not estimate accuracy, prove dependency contents unmodified or authorize cleanup. Keep the default unchanged pending broader evidence.
- Bounded work must also seek unfinished database records. The completed trial exposed a pending allocation lookup that revisited completed prefixes; explicitly using the existing pending index preserved progress and avoided that work. Synthetic bundled-driver/native seek and restart checks cover the correction.
- A separately approved bounded sample request can produce both usable observations and metadata-change blocks. Preserve the frozen read scope; changed parent evidence must not cause automatic recapture or wider reads. Sample equality still does not establish exact duplicate contents or savings. Keep target data, digests and measurements private.

## Remaining acceptance work

MVP-TRIAL is complete for the controlled metadata scope, saved-report diagnostics and limited owner-labelled comparison. A separate exact bounded sample request was explicitly approved and completed. Full hashing of original files needs a new scoped proposal and approval. These controlled observations do not establish current contents, cleanup suitability, unattended-operation safety, resource targets or release-soak readiness.
