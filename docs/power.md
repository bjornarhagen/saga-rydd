# Bounded power observations

P1-06b8a adds an internal observation library. It does not change worker scheduling, expose a command or establish physical power state. Worker integration and hardware acceptance are separate tasks.

`powerinfo.Observe(ctx)` uses contract `power_observation_v1`. Linux uses profile `linux_sysfs_power_v1`; macOS returns an unsupported, unknown observation without opening a power provider. No helper process, write, new dependency or internal goroutine is used.

## Evidence and unknown results

The Linux profile requires exact `Battery`, `System`, explicit `present=1` and `Discharging` attributes from a provider before reporting a positive witness. `system_battery_discharging_observed` is `true` or `null`, never an inferred `false`. Missing or ambiguous attributes, unavailable providers and unsupported resolution retain qualified unknown evidence. Device batteries are excluded. An online AC provider can coexist with a discharging system battery; online metadata or absent batteries do not prove external power.

Provider observations are sequential and need not describe one simultaneous state. A positive witness can coexist with incomplete coverage. `coverage_complete` describes bounded enumeration, independently of the witness. `namespace_authenticated` and `physical_power_verified` remain false. Provider names, models and serial numbers are not returned.

| Field | Meaning |
| --- | --- |
| `started_at`, `finished_at` | Guarded operation-local UTC observations |
| `supply_entries_observed` | Returned provider names, including an overflow sentinel; not a total |
| `providers_processed` | Providers admitted within the finite page |
| `confirmed_system_batteries` | Providers with the required system-battery evidence |
| `discharging_system_batteries` | Providers with the positive discharging witness |
| `ambiguous_providers` | Providers whose required evidence could not establish the witness |
| `attribute_attempts` | Admitted fixed-attribute reads, including failures |
| `returned_attribute_bytes` | Bytes returned by those reads, including overflow sentinels |

## Scope and limits

The observer holds canonical `/sys`, verifies sysfs filesystem magic and resolves the class directory and ordinary class-to-device links beneath that descriptor. Linux `openat2` uses `RESOLVE_BENEATH`, `RESOLVE_NO_MAGICLINKS` and `RESOLVE_NO_XDEV`. Attribute opens also use `RESOLVE_NO_SYMLINKS`. Unsupported resolution has no weaker fallback. Held and named root identity and mount observations are checked before publishing a witness; these checks do not authenticate a namespace against deliberate replacement between checks.

One operation enumerates at most 32 providers plus one overflow sentinel. It probes at most five fixed attributes per provider, with 64 supported bytes plus one overflow sentinel per attribute. The resulting ceilings are 160 attribute attempts, 10,400 returned attribute bytes and four held descriptors. Short-read iterations are bounded. These counts are not scanner metadata allowances, physical I/O measurements or limits on a kernel driver's work.

A cooperative five-second context and independent wall deadline guard admission and the final timestamps. Wall rollback or deadline refusal stays latched for the operation. Cancellation, rollback or deadline expiry returns no usable observation. The observer closes every acquired scope on return.

A sysfs read can enter a driver callback that remains blocked. Context and wall guards cannot interrupt that callback, and this synchronous library creates no replacement goroutine. Future worker integration must retain a single asynchronous observation slot, preserve controls and database-only maintenance, avoid idle polling and disclose stalled or stale evidence. This library does not validate that later integration.

## Validation boundaries

Generated fixtures cover classification, exact tokens, partial evidence, finite limits, cancellation, clock changes, scope replacement and descriptor closure. Native Linux fixtures exercise actual `openat2` resolution in generated directories; macOS fixtures verify the unsupported path performs no probe. These checks do not establish real battery transitions, driver latency, power loss, physical sleep or unattended energy/resource targets.

The supported interpretation is based on the [Linux v6.18 power-supply ABI](https://github.com/torvalds/linux/blob/v6.18/Documentation/ABI/testing/sysfs-class-power), its [attribute implementation](https://github.com/torvalds/linux/blob/v6.18/drivers/power/supply/power_supply_sysfs.c), the [sysfs callback model](https://docs.kernel.org/6.18/filesystems/sysfs.html) and [openat2 resolution flags](https://man7.org/linux/man-pages/man2/openat2.2.html). These references define the narrow profile; they do not substitute for native hardware acceptance.
