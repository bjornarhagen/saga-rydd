# Internal packages

Implemented packages: `config` (strict TOML and platform paths), `localfs`
(private application files), `state` (SQLite migrations/connections), and `cli`.
Scheduler, inventory, detectors, duplicates, actions and platform adapters follow
their tracked milestones. Avoid creating empty abstractions ahead of need.
