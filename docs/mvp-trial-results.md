# Lessons from the controlled trial

Only general product lessons are published here. Project-derived measurements, paths and raw reports remain private, as requested by the user.

## Implemented response: explain empty reports

An empty candidate page should explain whether dependency folders failed the age rule, lacked supported project evidence, were skipped, or had incomplete parent observations. It should also distinguish an exhausted saved inventory from a page with more results to inspect.

P2-05a now provides bounded per-page selection diagnostics in human and JSON output, using mutually exclusive reason counts and explicit coverage. It preserves nested dependency suppression, the existing age rule and review-required classification. The next step is owner feedback on report usefulness; do not change thresholds merely to produce findings.

## Other lessons

- Keep partial, stale and unknown sizes prominent. A short scan is not a complete inventory, and observed allocated bytes are not guaranteed reclaimable space.
- Evaluate the tradeoff between dispatch pauses and progress through many small directories before changing resource defaults.
- Optional owner feedback helps assess the age rule. Distinguish active development, projects still run or built, and projects no longer used; not working on a project now does not establish that it is unused. Keep owner statements separate from saved timestamps and leave dependency edits unknown without specific evidence. Feedback does not select or approve cleanup targets.
- Keep trial state and raw reports separate from public documentation. Publishing general lessons does not authorize disclosure of measurements or project details.
- Guided review needs the same empty-page explanation as finite candidate reports. Keep the exact folder, private state, age and current-page cursor in any suggested command; a generic command can open a different inventory or page. P4-01e shares the bounded rejection summary on empty guided pages while preserving frozen nonempty selection evidence.
- Separate exhausted saved report pages, a drained scan queue and completed saved size calculations. A bounded trial can inspect every saved page while filesystem work remains. Foreground interruption retains partial evidence and pending progress; resume the same saved work before assessing completed coverage. Report and navigation commands must not start another pass.

## Remaining acceptance work

MVP-TRIAL remains open for owner feedback and a review of candidate diagnostics. A controlled scan does not establish unattended-operation safety, resource targets or release-soak readiness.
