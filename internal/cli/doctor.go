package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/doctor"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const doctorUsage = "Usage: rydd --data-dir ABS_EXISTING_PRIVATE_INSTANCE doctor [--json]\nExisting-only configuration, metadata and main-header observations. Live health is not checked.\n"

var errDoctorOutput = errors.New("doctor reply exceeds its fixed output bound")

type doctorArgs struct {
	directory string
	machine   bool
	help      bool
}
type doctorOperations struct {
	observe func(context.Context, doctor.Request, doctor.ConfigValidator) (doctor.Report, error)
}

// Route the original arguments before generic JSON removal/default path/HOME
// resolution. Known global values are skipped; another command is never stolen.
func doctorInvocation(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--version" || a == "--help" || a == "-h" {
			return false
		}
		if a == "--data-dir" || a == "-data-dir" {
			i++
			continue
		}
		if strings.HasPrefix(a, "--data-dir=") || strings.HasPrefix(a, "-data-dir=") || a == "--json" {
			continue
		}
		if a == "--" {
			if i+1 < len(args) {
				return args[i+1] == "doctor"
			}
			return false
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		return a == "doctor"
	}
	return false
}

func doctorJSONRequested(args []string) bool {
	for i := 0; i < min(len(args), 16); i++ {
		if args[i] == "--data-dir" || args[i] == "-data-dir" {
			i++
			continue
		}
		if args[i] == "--json" {
			return true
		}
	}
	return false
}

func parseDoctorArgs(args []string) (doctorArgs, error) {
	a := doctorArgs{machine: doctorJSONRequested(args)}
	if len(args) > 16 {
		return a, doctor.ErrArguments
	}
	bytes, dataCount, jsonCount := 0, 0, 0
	command := false
	for i := 0; i < len(args); i++ {
		bytes += len(args[i])
		if bytes > 64<<10 {
			return a, doctor.ErrArguments
		}
		token := args[i]
		switch {
		case token == "--json":
			jsonCount++
			if jsonCount > 1 {
				return a, doctor.ErrArguments
			}
		case token == "--data-dir" || token == "-data-dir":
			if command || dataCount != 0 || i+1 >= len(args) {
				return a, doctor.ErrArguments
			}
			i++
			bytes += len(args[i])
			a.directory = args[i]
			dataCount++
		case strings.HasPrefix(token, "--data-dir=") || strings.HasPrefix(token, "-data-dir="):
			if command || dataCount != 0 {
				return a, doctor.ErrArguments
			}
			a.directory = strings.SplitN(token, "=", 2)[1]
			dataCount++
		case token == "doctor" && !command:
			command = true
		case (token == "--help" || token == "-h") && command:
			if a.help {
				return a, doctor.ErrArguments
			}
			a.help = true
		default:
			return a, doctor.ErrArguments
		}
	}
	if bytes > 64<<10 || !command {
		return a, doctor.ErrArguments
	}
	if a.help {
		return a, nil
	}
	if dataCount != 1 || doctor.ValidateDirectory(a.directory) != nil {
		return a, doctor.ErrArguments
	}
	return a, nil
}

// Only this known pure decoder/catalog is used by production; neither callback
// is selectable through argv or environment. HOME is needed solely for ~ input.
func doctorConfiguration(body []byte) (doctor.Configuration, error) {
	cfg, err := config.DecodeSyntax(body)
	if err != nil {
		return doctor.Configuration{}, errors.Join(doctor.ErrConfigInvalid, err)
	}
	if len(cfg.Roots) > doctor.MaxConfiguredPaths || len(cfg.Excludes) > doctor.MaxConfiguredPaths {
		return doctor.Configuration{}, doctor.ErrConfigUnsupported
	}
	needHome := false
	for _, paths := range [][]string{cfg.Roots, cfg.Excludes} {
		for _, p := range paths {
			if len(p) > 4096 {
				return doctor.Configuration{}, doctor.ErrConfigUnsupported
			}
			if p == "~" || strings.HasPrefix(p, "~/") {
				needHome = true
			}
		}
	}
	home := ""
	if needHome {
		home, err = os.UserHomeDir()
		if err != nil || !filepath.IsAbs(home) {
			return doctor.Configuration{}, errors.Join(doctor.ErrConfigEnvironment, err)
		}
	}
	pathBytes := 0
	for _, paths := range [][]string{cfg.Roots, cfg.Excludes} {
		for i, p := range paths {
			if p == "~" {
				p = home
			} else if strings.HasPrefix(p, "~/") {
				p = filepath.Join(home, p[2:])
			}
			if !utf8.ValidString(p) || strings.IndexByte(p, 0) >= 0 || !filepath.IsAbs(p) {
				return doctor.Configuration{}, doctor.ErrConfigInvalid
			}
			p = filepath.Clean(p)
			pathBytes += len(p)
			if len(p) > 4096 || pathBytes > doctor.MaxConfiguredPathBytes {
				return doctor.Configuration{}, doctor.ErrConfigUnsupported
			}
			paths[i] = p
		}
	}
	if err = cfg.Validate(home); err != nil {
		return doctor.Configuration{}, errors.Join(doctor.ErrConfigInvalid, err)
	}
	return doctor.Configuration{Version: cfg.Version, Roots: len(cfg.Roots), Exclusions: len(cfg.Excludes)}, nil
}

func doctorError(err error) (string, string, int) {
	switch {
	case errors.Is(err, doctor.ErrArguments):
		return "invalid_arguments", doctor.ErrArguments.Error(), 2
	case errors.Is(err, doctor.ErrPlatform):
		return "doctor_platform_unsupported", doctor.ErrPlatform.Error(), 1
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled", "Doctor observations were canceled; no repair or record write was performed.", 1
	case errors.Is(err, errDoctorOutput):
		return "doctor_output_bounds", "Doctor reply exceeds its fixed output bound; no partial success is established.", 1
	case errors.Is(err, doctor.ErrAttention):
		return "doctor_attention_required", doctor.ErrAttention.Error(), 1
	case err != nil:
		return "doctor_incomplete", doctor.ErrIncomplete.Error(), 1
	default:
		return "", "", 0
	}
}

type doctorReply struct {
	APIVersion int            `json:"api_version"`
	OK         bool           `json:"ok"`
	Command    string         `json:"command"`
	Doctor     *doctor.Report `json:"doctor,omitempty"`
	Error      *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type doctorBuffer struct {
	body []byte
	err  error
}

func (b *doctorBuffer) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if len(p) > doctor.MaxReportBytes-len(b.body) {
		b.err = errDoctorOutput
		return 0, b.err
	}
	b.body = append(b.body, p...)
	return len(p), nil
}

func runDoctor(ctx context.Context, args []string, out, errOut io.Writer) int {
	return runDoctorWith(ctx, args, out, errOut, doctorOperations{observe: doctor.Observe})
}

func runDoctorWith(ctx context.Context, args []string, out, errOut io.Writer, ops doctorOperations) int {
	a, err := parseDoctorArgs(args)
	var report doctor.Report
	if err == nil && a.help {
		if a.machine {
			return machineFailure(out, errOut, "doctor", "invalid_arguments", "Use capabilities --json for command discovery; doctor --help is text only.", 2)
		}
		n, writeErr := io.WriteString(out, doctorUsage)
		if writeErr != nil || n != len(doctorUsage) {
			fmt.Fprintln(errOut, "Doctor help output was incomplete.")
			return 1
		}
		return 0
	}
	if err == nil {
		if ctx == nil || ops.observe == nil {
			err = doctor.ErrIncomplete
		} else if err = ctx.Err(); err == nil {
			catalog := state.MainHeaderReaderProfile()
			report, err = ops.observe(ctx, doctor.Request{DataDirectory: a.directory, Catalog: doctor.HeaderCatalog{ApplicationID: catalog.ApplicationID, MinReaderSchema: catalog.MinReaderSchema, MaxReaderSchema: catalog.MaxReaderSchema}}, doctorConfiguration)
		}
	}
	if ctx != nil {
		err = errors.Join(err, ctx.Err())
	}
	code, message, exit := doctorError(err)
	reply := doctorReply{APIVersion: APIVersion, OK: exit == 0, Command: "doctor"}
	if report.Contract != "" {
		reply.Doctor = &report
	}
	if err != nil {
		reply.Error = &struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{code, message}
	}
	buffer := &doctorBuffer{}
	if a.machine {
		err = json.NewEncoder(buffer).Encode(reply)
	} else if reply.Doctor == nil {
		_, err = fmt.Fprintln(buffer, message)
		if exit == 2 {
			_, err = fmt.Fprint(buffer, doctorUsage)
		}
	} else {
		printDoctor(buffer, report, message)
		err = buffer.err
	}
	if err != nil || buffer.err != nil {
		if a.machine {
			return machineFailure(out, errOut, "doctor", "doctor_output_bounds", "Doctor reply exceeded its fixed output bound; no report was written.", 1)
		}
		fmt.Fprintln(errOut, "Doctor reply exceeded its fixed output bound; no report was written.")
		return 1
	}
	canceledBeforeWrite := ctx != nil && ctx.Err() != nil
	if canceledBeforeWrite && exit == 0 {
		fmt.Fprintln(errOut, "Doctor reply was canceled before output; no repair or record write was performed.")
		return 1
	}
	if !a.machine && exit == 2 {
		n, e := errOut.Write(buffer.body)
		if e != nil || n != len(buffer.body) {
			return 1
		}
		return exit
	}
	n, e := out.Write(buffer.body)
	if e != nil || n != len(buffer.body) {
		fmt.Fprintln(errOut, "Doctor output was incomplete; no second reply was written. No repair or record write was performed.")
		return 1
	}
	if ctx != nil && ctx.Err() != nil && !canceledBeforeWrite {
		fmt.Fprintln(errOut, "Doctor reply completed but was canceled; observations are historical only. No second reply was written.")
		return 1
	}
	return exit
}

func printDoctor(out io.Writer, r doctor.Report, message string) {
	banner := "CHECKS FINISHED"
	if r.Status == "attention_required" {
		banner = "ATTENTION REQUIRED"
	} else if r.Status == "incomplete" {
		banner = "CHECKS INCOMPLETE"
	}
	fmt.Fprintf(out, "%s - LIVE HEALTH NOT CHECKED\n", banner)
	fmt.Fprintf(out, "Selected directory: %q\n", r.DataDirectory)
	printWrapped(out, "Configuration and the main header can be read. Doctor does not open SQLite, acquire a lock, contact a worker/service, sample power, inspect scan roots or repair records. Treat these paths and reports as private local data.", "")
	for _, c := range r.Checks {
		fmt.Fprintf(out, "\n%s: %s\n", c.ID, strings.ReplaceAll(c.Status, "_", " "))
		if c.Metadata != nil && c.Metadata.LogicalBytes != nil {
			fmt.Fprintf(out, "  File size: %d bytes\n", *c.Metadata.LogicalBytes)
		}
		if c.Configuration != nil {
			fmt.Fprintf(out, "  Config version: %d; roots: %d; exclusions: %d\n", c.Configuration.Version, c.Configuration.Roots, c.Configuration.Exclusions)
		}
		if c.Header != nil {
			fmt.Fprintf(out, "  Main header version: %d; page size: %d bytes\n", c.Header.UserVersion, c.Header.PageSize)
		}
		printWrapped(out, c.Qualification, "  ")
	}
	fmt.Fprintln(out, "\nCurrent state schema, ledger integrity, writer activity and lock availability: unknown.")
	if message != "" {
		printWrapped(out, message, "")
	}
	fmt.Fprintln(out, "\nNEXT EXPLICIT STEPS - INFERRED GUIDANCE")
	for _, g := range r.Guidance {
		printWrapped(out, g.Message, "")
		printWrapped(out, g.Effect, "  ")
		for _, condition := range g.Preconditions {
			printWrapped(out, condition, "  ")
		}
		parts := make([]string, len(g.Argv))
		for i, arg := range g.Argv {
			if i == 0 {
				parts[i] = arg
			} else {
				parts[i] = shellQuote(arg)
			}
		}
		fmt.Fprintf(out, "  %s\n", strings.Join(parts, " "))
	}
}
