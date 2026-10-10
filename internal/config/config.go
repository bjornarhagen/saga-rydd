// Package config defines the versioned on-disk configuration. Loading it has no
// side effects: it neither creates state nor starts scanning.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/pelletier/go-toml/v2"
)

const Version = 1
const maxConfigBytes = 1 << 20

type Paths struct{ ConfigFile, StateDir string }

func ResolvePaths(dataDir string) (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, err
	}
	return PathsFor(runtime.GOOS, home, dataDir, os.Getenv)
}

// PathsFor is separate from environment access so both platforms are testable.
func PathsFor(goos, home, dataDir string, getenv func(string) string) (Paths, error) {
	if dataDir != "" {
		path, err := normalizePath(dataDir, home)
		if err != nil {
			return Paths{}, err
		}
		return Paths{filepath.Join(path, "config.toml"), path}, nil
	}
	if !filepath.IsAbs(home) {
		return Paths{}, errors.New("home directory must be absolute")
	}
	switch goos {
	case "darwin":
		dir := filepath.Join(home, "Library", "Application Support", "saga-rydd")
		return Paths{filepath.Join(dir, "config.toml"), dir}, nil
	case "linux":
		base := func(key, fallback string) string {
			value := getenv(key)
			// XDG requires ignoring relative environment values.
			if !filepath.IsAbs(value) {
				return filepath.Join(home, fallback)
			}
			return value
		}
		return Paths{filepath.Join(base("XDG_CONFIG_HOME", ".config"), "saga-rydd", "config.toml"), filepath.Join(base("XDG_STATE_HOME", ".local/state"), "saga-rydd")}, nil
	default:
		return Paths{}, fmt.Errorf("unsupported OS %q; Rydd supports macOS and Linux", goos)
	}
}

type Config struct {
	Version  int      `toml:"version"`
	Roots    []string `toml:"roots"`
	Excludes []string `toml:"excludes"`
	Scan     Scan     `toml:"scan"`
}

type Scan struct {
	WorkSeconds             int   `toml:"work_seconds"`
	IntervalSeconds         int   `toml:"interval_seconds"`
	MetadataPerSecond       int   `toml:"metadata_per_second"`
	APIAttemptsPerSecond    int   `toml:"api_attempts_per_second"`
	MetadataAttemptsPerDay  int64 `toml:"metadata_attempts_per_day"`
	ReadBytesPerSecond      int64 `toml:"read_bytes_per_second"`
	ReadBytesPerDay         int64 `toml:"read_bytes_per_day"`
	PauseOnBattery          bool  `toml:"pause_on_battery"`
	MaxScanChunksPerDay     int   `toml:"max_scan_chunks_per_day"`
	MaxStateBytes           int64 `toml:"max_state_bytes"`
	AdaptiveRevisits        bool  `toml:"adaptive_revisits"`
	CompactInventory        bool  `toml:"compact_inventory"`
	CPUSessionCharges       bool  `toml:"cpu_session_charges"`
	CPUChargeSecondsPerHour int   `toml:"cpu_charge_seconds_per_hour"`
	CPUChargeSecondsPerDay  int   `toml:"cpu_charge_seconds_per_day"`
}

func Default() Config {
	return Config{Version: Version, Roots: []string{}, Excludes: []string{}, Scan: Scan{
		WorkSeconds: 30, IntervalSeconds: 300, MetadataPerSecond: 100,
		MetadataAttemptsPerDay: 20_000_000, ReadBytesPerSecond: 5 << 20,
		ReadBytesPerDay: 5 << 30, PauseOnBattery: true, MaxScanChunksPerDay: 288, MaxStateBytes: DefaultMaxStateBytes,
	}}
}

func normalizePath(value, home string) (string, error) {
	if value == "~" {
		value = home
	} else if strings.HasPrefix(value, "~/") {
		value = filepath.Join(home, value[2:])
	}
	if strings.ContainsRune(value, 0) || !filepath.IsAbs(value) {
		return "", fmt.Errorf("path %q must be absolute or start with ~/", value)
	}
	return filepath.Clean(value), nil
}

// Within uses directory boundaries, so /work/project-old is not in /work/project.
func Within(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (c *Config) Validate(home string) error {
	if c.Version != Version {
		return fmt.Errorf("unsupported config version %d (supported: %d)", c.Version, Version)
	}
	if len(c.Roots) == 0 {
		return errors.New("at least one explicit scan root is required")
	}
	for _, paths := range [][]string{c.Roots, c.Excludes} {
		for i, path := range paths {
			normalized, err := normalizePath(path, home)
			if err != nil {
				return err
			}
			paths[i] = normalized
		}
	}
	for i, root := range c.Roots {
		for _, earlier := range c.Roots[:i] {
			if Within(root, earlier) || Within(earlier, root) {
				return fmt.Errorf("overlapping roots %q and %q", root, earlier)
			}
		}
		for _, exclude := range c.Excludes {
			if Within(root, exclude) {
				return fmt.Errorf("root %q is fully excluded by %q", root, exclude)
			}
		}
	}
	s := c.Scan
	if s.CPUChargeSecondsPerHour < 0 || s.CPUChargeSecondsPerHour > 3600 || s.CPUChargeSecondsPerDay < 0 || s.CPUChargeSecondsPerDay > 86400 {
		return errors.New("CPU charge limits must be 0–3600 seconds/hour and 0–86400 seconds/day")
	}
	if !s.CPUSessionCharges && (s.CPUChargeSecondsPerHour != 0 || s.CPUChargeSecondsPerDay != 0) {
		return errors.New("positive CPU charge limits require cpu_session_charges")
	}
	if s.CompactInventory && s.AdaptiveRevisits {
		return errors.New("compact_inventory cannot be combined with adaptive_revisits; compact background inventory uses fixed 24-hour revisits")
	}
	if s.MaxStateBytes < MinStateBytes || s.MaxStateBytes > MaxStateBytes {
		return errors.New("max_state_bytes must be 1048576–1099511627776 (1 MiB–1 TiB)")
	}
	if s.MaxScanChunksPerDay < 1 || s.MaxScanChunksPerDay > 100000 {
		return errors.New("max_scan_chunks_per_day must be 1–100000")
	}
	if s.WorkSeconds < 1 || s.IntervalSeconds < s.WorkSeconds || s.IntervalSeconds > 86400 {
		return errors.New("scan interval must be 1–86400 seconds and at least work_seconds; work_seconds must be positive")
	}
	if s.MetadataPerSecond < 1 || s.MetadataPerSecond > 100000 {
		return errors.New("metadata_per_second must be 1–100000")
	}
	if s.APIAttemptsPerSecond < 0 || s.APIAttemptsPerSecond > 100000 {
		return errors.New("api_attempts_per_second must be 0–100000 (0 disables API pacing)")
	}
	if s.MetadataAttemptsPerDay < 1 || s.MetadataAttemptsPerDay > 1<<50 {
		return errors.New("metadata_attempts_per_day must be 1–1125899906842624")
	}
	if s.ReadBytesPerSecond < 1 || s.ReadBytesPerSecond > 1<<30 || s.ReadBytesPerDay < 1 || s.ReadBytesPerDay > 1<<50 {
		return errors.New("read budgets must be positive and at most 1 GiB/second and 1 PiB/day")
	}
	return nil
}

func Load(path, home string) (Config, error) {
	c := Default()
	if err := localfs.CheckPrivateFile(path); err != nil {
		return c, err
	}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return c, err
	}
	return Decode(data, home)
}

// Decode reads bounded configuration bytes with the same defaults, strict TOML
// fields and path/budget validation as Load. It performs no filesystem access,
// so a caller can validate bytes read from an already held file descriptor.
func Decode(data []byte, home string) (Config, error) {
	c := Default()
	if len(data) > maxConfigBytes {
		return c, errors.New("configuration exceeds 1 MiB")
	}
	if err := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&c); err != nil {
		return c, fmt.Errorf("decode config: %w", err)
	}
	return c, c.Validate(home)
}

// Create refuses to replace an existing configuration. Roots need not currently
// be mounted; availability and physical root identity are checked by the scanner.
func Create(path, home string, c Config) error {
	if err := c.Validate(home); err != nil {
		return err
	}
	data, err := toml.Marshal(c)
	if err != nil {
		return err
	}
	if err := localfs.EnsurePrivateDir(filepath.Dir(path)); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
