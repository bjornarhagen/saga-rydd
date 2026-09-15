// Package config defines the versioned on-disk configuration. Loading it has no
// side effects: it neither creates state nor starts scanning.
package config

import (
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
	WorkSeconds        int   `toml:"work_seconds"`
	IntervalSeconds    int   `toml:"interval_seconds"`
	MetadataPerSecond  int   `toml:"metadata_per_second"`
	ReadBytesPerSecond int64 `toml:"read_bytes_per_second"`
	ReadBytesPerDay    int64 `toml:"read_bytes_per_day"`
	PauseOnBattery     bool  `toml:"pause_on_battery"`
}

func Default() Config {
	return Config{Version: Version, Roots: []string{}, Excludes: []string{}, Scan: Scan{30, 300, 100, 5 << 20, 5 << 30, true}}
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
	if s.WorkSeconds < 1 || s.IntervalSeconds < s.WorkSeconds || s.IntervalSeconds > 86400 {
		return errors.New("scan interval must be 1–86400 seconds and at least work_seconds; work_seconds must be positive")
	}
	if s.MetadataPerSecond < 1 || s.MetadataPerSecond > 100000 {
		return errors.New("metadata_per_second must be 1–100000")
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
	if len(data) > maxConfigBytes {
		return c, errors.New("configuration exceeds 1 MiB")
	}
	if err := toml.NewDecoder(strings.NewReader(string(data))).DisallowUnknownFields().Decode(&c); err != nil {
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
