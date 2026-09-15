package inventory

import "sync/atomic"

// Metrics reports attempted scanner API calls, entry pacing and wait time,
// including failures and startup scope validation. It is not a syscall, byte,
// CPU or durable daily budget.
// Snapshots are lock-free so status never waits behind filesystem operations.
type Metrics struct {
	EntryInspections    uint64 `json:"entry_inspections"`
	EntryRatePerSecond  int    `json:"entry_rate_per_second"`
	ThrottleWaitNS      uint64 `json:"throttle_wait_ns"`
	Throttled           bool   `json:"throttled"`
	StatCalls           uint64 `json:"stat_calls"`
	DirectoryOpenCalls  uint64 `json:"directory_open_calls"`
	DirectoryReadCalls  uint64 `json:"directory_read_calls"`
	FilesystemStatCalls uint64 `json:"filesystem_stat_calls"`
	MountIdentityCalls  uint64 `json:"mount_identity_calls"`
	PathResolutionCalls uint64 `json:"path_resolution_calls"`
}

type counters struct {
	inspections atomic.Uint64
	waitNS      atomic.Uint64
	throttled   atomic.Bool
	stat        atomic.Uint64
	open        atomic.Uint64
	read        atomic.Uint64
	filesystem  atomic.Uint64
	mount       atomic.Uint64
	resolve     atomic.Uint64
}

// Metrics returns independently sampled monotonic counters for this scanner's
// lifetime. There is no lock spanning all fields and no per-path history.
func (s *Scanner) Metrics() Metrics {
	return Metrics{
		EntryInspections: s.metrics.inspections.Load(), EntryRatePerSecond: s.entryRate, ThrottleWaitNS: s.metrics.waitNS.Load(), Throttled: s.metrics.throttled.Load(),
		StatCalls: s.metrics.stat.Load(), DirectoryOpenCalls: s.metrics.open.Load(),
		DirectoryReadCalls: s.metrics.read.Load(), FilesystemStatCalls: s.metrics.filesystem.Load(),
		MountIdentityCalls: s.metrics.mount.Load(), PathResolutionCalls: s.metrics.resolve.Load(),
	}
}
