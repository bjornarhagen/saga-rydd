package config

// MaxStateBytes is a source-admission threshold for the configured inventory's
// database and WAL logical lengths. It is not a hard physical storage quota.
const (
	MinStateBytes        int64 = 1 << 20
	DefaultMaxStateBytes int64 = 1 << 30
	MaxStateBytes        int64 = 1 << 40
)
