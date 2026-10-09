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
		return empty, usageError{errors.New("use service preview --executable ABSOLUTE_PATH; installation and activation are unavailable")}
	}
	flags := flag.NewFlagSet("service preview", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	executable := flags.String("executable", "", "exact absolute executable path")
	count := 0
	for _, arg := range args[1:] {
		if arg == "--executable" || arg == "-executable" || strings.HasPrefix(arg, "--executable=") || strings.HasPrefix(arg, "-executable=") {
			count++
		}
	}
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || count != 1 || *executable == "" {
		return empty, usageError{errors.New("service preview requires one --executable ABSOLUTE_PATH and no other arguments")}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	runtimeDir := os.Getenv("RYDD_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = "/tmp"
	}
	descriptor, err := service.Build(service.Spec{GOOS: runtime.GOOS, Executable: *executable, Paths: paths, RuntimeDir: runtimeDir})
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

func printServicePreview(out io.Writer, descriptor service.Descriptor) error {
	out = &reviewOutput{writer: out}
	_, err := fmt.Fprintf(out, "SERVICE DESCRIPTOR PREVIEW\n\nNothing was installed or started by this command. No scanning was enabled.\nPlatform: %s\nDeclared manager profile: %s\nService label: %s\nDescriptor name: %s\nExecutable: %q\nConfiguration: %q\nState: %q\nRuntime directory: %q\n\nThis proposes plain daemon mode. When started, the worker would load\nconfiguration and update root/recovery bookkeeping, with no scanner.\nInstalling this descriptor in a login service directory can affect future\nlogins. Installation and start need separate lifecycle commands.\nConfiguration, state and executable contents were not checked. The selected\nexecutable path is trusted and can later change. Installed manager support\nwas not detected; native service lifecycle acceptance is still required.\n\nPROPOSED DESCRIPTOR\n\n%s", descriptor.Platform, descriptor.ManagerProfile, descriptor.Label, descriptor.Filename, descriptor.Executable, descriptor.ConfigFile, descriptor.StateDir, descriptor.RuntimeDir, descriptor.Content)
	return err
}
