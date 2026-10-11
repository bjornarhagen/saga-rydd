package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/service"
)

func servicePreview(ctx context.Context, args []string, paths config.Paths) (service.Descriptor, error) {
	var empty service.Descriptor
	if len(args) == 0 || args[0] != "preview" {
		return empty, usageError{errors.New("use service preview --executable ABSOLUTE_PATH")}
	}
	executable, scan, err := servicePreviewArguments(args)
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	runtimeDir := os.Getenv("RYDD_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = "/tmp"
	}
	profile := service.ProfileIdle
	if scan {
		profile = service.ProfileExperimentalInventory
	}
	descriptor, err := service.Build(service.Spec{GOOS: runtime.GOOS, Executable: executable, Paths: paths, RuntimeDir: runtimeDir, Profile: profile})
	if errors.Is(err, service.ErrSpec) {
		return empty, usageError{err}
	}
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return descriptor, nil
}

func servicePreviewArguments(args []string) (string, bool, error) {
	if len(args) == 0 || args[0] != "preview" {
		return "", false, usageError{errors.New("use service preview --executable ABSOLUTE_PATH")}
	}
	flags := flag.NewFlagSet("service preview", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	executable := flags.String("executable", "", "exact absolute executable path")
	scan := flags.Bool("experimental-scan", false, "select experimental inventory intent")
	if err := validateServicePreviewArguments(args[1:]); err != nil {
		return "", false, usageError{err}
	}
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *executable == "" {
		return "", false, usageError{errors.New("service preview requires one --executable ABSOLUTE_PATH and no other arguments")}
	}
	return *executable, *scan, nil
}

func validateServicePreviewArguments(args []string) error {
	return validateServiceOptionArguments(args, false)
}

// Validate before flag.Parse or path resolution: only exact boolean literals
// are admitted. String values and tokens after -- are never option selectors.
func validateServiceOptionArguments(args []string, directoryAllowed bool) error {
	executables, directories, modes := 0, 0, 0
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			break
		}
		if arg == "--executable" || arg == "-executable" {
			executables++
			index++
			continue
		}
		if strings.HasPrefix(arg, "--executable=") || strings.HasPrefix(arg, "-executable=") {
			executables++
			continue
		}
		if directoryAllowed && (arg == "--directory" || arg == "-directory") {
			directories++
			index++
			continue
		}
		if directoryAllowed && (strings.HasPrefix(arg, "--directory=") || strings.HasPrefix(arg, "-directory=")) {
			directories++
			continue
		}
		if arg == "--experimental-scan" || arg == "-experimental-scan" {
			modes++
			continue
		}
		for _, prefix := range []string{"--experimental-scan=", "-experimental-scan="} {
			if value, found := strings.CutPrefix(arg, prefix); found {
				modes++
				if value != "true" && value != "false" {
					return errors.New("service --experimental-scan accepts only true or false")
				}
			}
		}
	}
	if executables != 1 || directories > 1 || modes > 1 {
		return errors.New("service requires one --executable, at most one --directory and at most one --experimental-scan across spellings")
	}
	return nil
}

// This pure admission is called before ResolvePaths/UserHomeDir by both modes.
func validateServiceSelection(args []string) error {
	if len(args) > 0 && args[0] == "preview" {
		_, _, err := servicePreviewArguments(args)
		return err
	}
	_, _, _, _, err := serviceLifecycleSelection(args)
	return err
}

func printServicePreview(out io.Writer, descriptor service.Descriptor) error {
	out = &reviewOutput{writer: out}
	if descriptor.ScannerIntent != nil {
		_, err := fmt.Fprintf(out, "SERVICE SCANNER DESCRIPTOR PREVIEW\n\nNothing was installed or started by this command. No scanning was enabled.\nPlatform: %s\nDeclared manager profile: %s\nService label: %s\nDescriptor name: %s\nExecutable: %q\nConfiguration: %q\nState: %q\nRuntime directory: %q\n\nThis proposes experimental metadata inventory at a future worker start.\nConfigured roots would come from the then-current configuration. Their\ncontents and current source permission were not checked. Representative\nresources were not verified. This grants no hashing or cleanup authority.\nExplicit scanner-profile lifecycle commands are supported by the API.\nThis preview performed none. Installed manager and native lifecycle support\nwere not verified.\nPlacing a descriptor in a login service directory can affect future logins.\nConfiguration, state and executable contents were not checked. The selected\nexecutable path is trusted and can later change. Installed manager support\nwas not detected; native service lifecycle acceptance is still required.\n\nPROPOSED DESCRIPTOR\n\n%s", descriptor.Platform, descriptor.ManagerProfile, descriptor.Label, descriptor.Filename, descriptor.Executable, descriptor.ConfigFile, descriptor.StateDir, descriptor.RuntimeDir, descriptor.Content)
		return err
	}
	_, err := fmt.Fprintf(out, "SERVICE DESCRIPTOR PREVIEW\n\nNothing was installed or started by this command. No scanning was enabled.\nPlatform: %s\nDeclared manager profile: %s\nService label: %s\nDescriptor name: %s\nExecutable: %q\nConfiguration: %q\nState: %q\nRuntime directory: %q\n\nThis proposes plain daemon mode. When started, the worker would load\nconfiguration and update root/recovery bookkeeping, with no scanner.\nInstalling this descriptor in a login service directory can affect future\nlogins. Installation and start need separate lifecycle commands.\nConfiguration, state and executable contents were not checked. The selected\nexecutable path is trusted and can later change. Installed manager support\nwas not detected; native service lifecycle acceptance is still required.\n\nPROPOSED DESCRIPTOR\n\n%s", descriptor.Platform, descriptor.ManagerProfile, descriptor.Label, descriptor.Filename, descriptor.Executable, descriptor.ConfigFile, descriptor.StateDir, descriptor.RuntimeDir, descriptor.Content)
	return err
}
