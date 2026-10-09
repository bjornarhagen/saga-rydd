// Package service renders bounded, idle-only user-service previews. It does not
// read files, inspect managers, publish descriptors or start processes.
package service

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
)

const Contract = "service_descriptor_preview_v1"
const ManagedLabel = "io.github.bjornarhagen.saga-rydd"
const MaxPathBytes = 4096
const MaxDescriptorBytes = 64 << 10

var ErrSpec = errors.New("unsupported service descriptor specification")

// Spec supplies an exact executable name and frozen paths. Build does not prove
// that these names exist, retain their identities or suit an installed manager.
type Spec struct {
	GOOS       string
	Executable string
	Paths      config.Paths
	RuntimeDir string
}

type Descriptor struct {
	Contract              string            `json:"contract"`
	Platform              string            `json:"platform"`
	ManagerProfile        string            `json:"manager_profile"`
	Label                 string            `json:"label"`
	Filename              string            `json:"filename"`
	Content               string            `json:"content"`
	Executable            string            `json:"executable"`
	ConfigFile            string            `json:"config_file"`
	StateDir              string            `json:"state_dir"`
	RuntimeDir            string            `json:"runtime_dir"`
	Argv                  []string          `json:"argv"`
	Env                   map[string]string `json:"env"`
	InstallationPerformed bool              `json:"installation_performed"`
	ActivationPerformed   bool              `json:"activation_performed"`
	ScanningEnabled       bool              `json:"scanning_enabled"`
}

// Build returns deterministic descriptor text and the literal intended child
// arguments/environment. Environment contains additions, not the entire manager
// environment. Plain daemon remains idle; scanner activation is not an option.
func Build(spec Spec) (Descriptor, error) {
	if spec.GOOS != "darwin" && spec.GOOS != "linux" {
		return Descriptor{}, fmt.Errorf("%w: platform must be darwin or linux", ErrSpec)
	}
	for _, field := range []struct{ name, value string }{
		{"executable", spec.Executable}, {"configuration", spec.Paths.ConfigFile},
		{"state", spec.Paths.StateDir}, {"runtime", spec.RuntimeDir},
	} {
		if err := validatePath(field.name, field.value); err != nil {
			return Descriptor{}, err
		}
	}
	if spec.Executable == "/" {
		return Descriptor{}, fmt.Errorf("%w: executable must name a path below the root", ErrSpec)
	}
	// systemd v255 rejects these bytes in the decoded executable name, even
	// when they were correctly quoted in ExecStart. Other arguments and
	// environment values retain literal quoting support.
	if spec.GOOS == "linux" && strings.ContainsAny(spec.Executable, "\"'\\") {
		return Descriptor{}, fmt.Errorf("%w: Linux executable must not contain quotes or backslashes", ErrSpec)
	}
	result := Descriptor{
		Contract: Contract, Platform: spec.GOOS, Label: ManagedLabel,
		Executable: spec.Executable, ConfigFile: spec.Paths.ConfigFile,
		StateDir: spec.Paths.StateDir, RuntimeDir: spec.RuntimeDir,
		Argv: []string{spec.Executable}, Env: map[string]string{"RYDD_RUNTIME_DIR": spec.RuntimeDir},
	}
	if spec.Paths.ConfigFile == path.Join(spec.Paths.StateDir, "config.toml") {
		result.Argv = append(result.Argv, "--data-dir", spec.Paths.StateDir, "daemon")
	} else if spec.GOOS == "linux" && path.Base(spec.Paths.ConfigFile) == "config.toml" && path.Base(path.Dir(spec.Paths.ConfigFile)) == "saga-rydd" && path.Base(spec.Paths.StateDir) == "saga-rydd" {
		result.Env["XDG_CONFIG_HOME"] = path.Dir(path.Dir(spec.Paths.ConfigFile))
		result.Env["XDG_STATE_HOME"] = path.Dir(spec.Paths.StateDir)
		result.Argv = append(result.Argv, "daemon")
	} else {
		return Descriptor{}, fmt.Errorf("%w: use unified data-dir paths or standard split Linux saga-rydd paths", ErrSpec)
	}
	var err error
	if spec.GOOS == "darwin" {
		result.ManagerProfile = "launchd_background_xml_v1"
		result.Filename = ManagedLabel + ".plist"
		result.Content, err = renderLaunchd(result)
	} else {
		result.ManagerProfile = "systemd_user_v255"
		result.Filename = ManagedLabel + ".service"
		result.Content = renderSystemd(result)
	}
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: descriptor encoding failed", ErrSpec)
	}
	if len(result.Content) > MaxDescriptorBytes {
		return Descriptor{}, fmt.Errorf("%w: descriptor exceeds 64 KiB", ErrSpec)
	}
	return result, nil
}

func validatePath(name, value string) error {
	if value == "" || len(value) > MaxPathBytes || !utf8.ValidString(value) || !path.IsAbs(value) || path.Clean(value) != value {
		return fmt.Errorf("%w: %s must be a clean absolute UTF-8 path of at most 4096 bytes", ErrSpec, name)
	}
	if strings.ContainsFunc(value, func(r rune) bool {
		// XML cannot preserve these noncharacters, even in otherwise valid UTF-8.
		return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\ufffe' || r == '\uffff'
	}) {
		return fmt.Errorf("%w: %s contains a control or unsupported XML character", ErrSpec, name)
	}
	return nil
}
