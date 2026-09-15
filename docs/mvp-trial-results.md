# Lessons from the controlled trial

Only general product lessons are published here. Project-derived measurements, paths and raw reports remain private, as requested by the user.

## Implemented response: explain empty reports

An empty candidate page should explain whether dependency folders failed the age rule, lacked supported project evidence, were skipped, or had incomplete parent observations. It should also distinguish an exhausted saved inventory from a page with more results to inspect.

P2-05a now provides bounded per-page selection diagnostics in human and JSON output, using mutually exclusive reason counts and explicit coverage. It preserves nested dependency suppression, the existing age rule and review-required classification. The next step is owner feedback on report usefulness; do not change thresholds merely to produce findings.

## Other lessons

- Keep partial, stale and unknown sizes prominent. A short scan is not a complete inventory, and observed allocated bytes are not guaranteed reclaimable space.
- Evaluate the tradeoff between dispatch pauses and progress through many small directories before changing resource defaults.
- Ask owners whether a project is still used. Modification timestamps alone cannot establish inactivity or whether dependencies contain local edits.
- Keep trial state and raw reports separate from public documentation. Publishing general lessons does not authorize disclosure of measurements or project details.

## Remaining acceptance work

MVP-TRIAL remains open for owner feedback and a review of candidate diagnostics. A controlled scan does not establish unattended-operation safety, resource targets or release-soak readiness.
