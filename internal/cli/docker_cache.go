package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/dockerinfo"
)

type DockerCacheMetadataReport struct{ dockerinfo.CacheReport }

// Old image/container parsing and projection remain separate. Flag values and
// tokens after -- are data and cannot choose an alternate discovery scope.
func dispatchDockerMetadata(ctx context.Context, args []string) (any, error) {
	if hasDockerCacheFlag(args) {
		return dockerCacheMetadata(ctx, args)
	}
	return dockerMetadata(ctx, args)
}

func hasDockerCacheFlag(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || !strings.HasPrefix(a, "-") || a == "-" {
			return false
		}
		name, _, assigned := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-"), "=")
		if name == "cache-metadata" {
			return true
		}
		if name == "context" && !assigned {
			i++
		}
	}
	return false
}

type dockerCacheMode struct {
	value bool
	set   bool
}

func (v *dockerCacheMode) String() string   { return strconv.FormatBool(v.value) }
func (v *dockerCacheMode) IsBoolFlag() bool { return true }
func (v *dockerCacheMode) Set(value string) error {
	if v.set {
		return errors.New("--cache-metadata cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

func dockerCacheMetadata(ctx context.Context, args []string) (DockerCacheMetadataReport, error) {
	return dockerCacheMetadataWithDiscover(ctx, args, dockerinfo.DiscoverCache)
}

func dockerCacheMetadataWithDiscover(ctx context.Context, args []string, discover func(context.Context, string) (dockerinfo.CacheReport, error)) (DockerCacheMetadataReport, error) {
	var mode dockerCacheMode
	var selected dockerContextValue
	f := flag.NewFlagSet("docker --cache-metadata", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&mode, "cache-metadata", "read only the selected Engine's embedded cache metadata")
	f.Var(&selected, "context", "one explicitly selected Docker context")
	if err := f.Parse(args); err != nil {
		return DockerCacheMetadataReport{}, usageError{err}
	}
	if !mode.set || !mode.value || !selected.set || !dockerinfo.ValidContextName(selected.value) || f.NArg() != 0 {
		return DockerCacheMetadataReport{}, usageError{errors.New("docker requires exactly one true metadata mode and --context NAME; --cache-metadata selects only the Engine-embedded cache; repeated options and other scopes are unavailable")}
	}
	ctx, cancel := context.WithTimeout(ctx, dockerinfo.OperationLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return DockerCacheMetadataReport{}, err
	}
	if discover == nil {
		return DockerCacheMetadataReport{}, dockerMetadataFailure(dockerinfo.ErrResolver)
	}
	r, err := discover(ctx, selected.value)
	if err != nil {
		return DockerCacheMetadataReport{}, dockerMetadataFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return DockerCacheMetadataReport{}, err
	}
	return DockerCacheMetadataReport{CacheReport: r}, nil
}

func printDockerResult(ctx context.Context, out io.Writer, result any) error {
	switch r := result.(type) {
	case DockerMetadataReport:
		return printDockerMetadataReport(ctx, out, r)
	case DockerCacheMetadataReport:
		return printDockerCacheMetadataReport(ctx, out, r)
	default:
		return errors.New("Docker metadata result type is unavailable")
	}
}

func dockerOptionalBool(value *bool) string {
	if value == nil {
		return "NOT RECORDED"
	}
	if *value {
		return "YES"
	}
	return "NO"
}

func dockerOptionalTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "NOT RECORDED"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func printDockerCacheMetadataReport(ctx context.Context, out io.Writer, r DockerCacheMetadataReport) error {
	guard := &reviewOutput{writer: out}
	noun := "records"
	if len(r.Records) == 1 {
		noun = "record"
	}
	printWrapped(guard, fmt.Sprintf("Engine-embedded cache metadata observed: %s %s.", humanCount(len(r.Records)), noun), "")
	printField(guard, "Selected context", fmt.Sprintf("%q", r.ContextName))
	printField(guard, "Resolved endpoint", fmt.Sprintf("%q", r.Endpoint))
	printField(guard, "Daemon ID", fmt.Sprintf("%q", r.DaemonID))
	printField(guard, "Server version", fmt.Sprintf("%q", r.ServerVersion))
	printField(guard, "Engine API profile", r.APIVersion)
	printField(guard, "Observations started", r.StartedAt.UTC().Format(time.RFC3339Nano))
	printField(guard, "Observations completed", r.CompletedAt.UTC().Format(time.RFC3339Nano))
	printWrapped(guard, "Scope: the selected Engine's embedded cache only. Images, containers and volumes were not checked. No named Buildx builder was discovered or pinned, and other builders or cache managers are outside this scope.", "")
	printWrapped(guard, "These are sequential observations on one held connection. The declared daemon ID and version matched before and after the cache request. This is not an atomic snapshot, current-state verification or proof of content equality.", "")
	for i, record := range r.Records {
		fmt.Fprintf(guard, "\nCache record %d: %q\n", i+1, record.ID)
		printField(guard, "Type reported", fmt.Sprintf("%q", record.Type))
		printField(guard, "In use reported", dockerOptionalBool(record.InUse))
		printField(guard, "Shared reported", dockerOptionalBool(record.Shared))
		printField(guard, "Created reported", dockerOptionalTime(record.CreatedAt))
		printField(guard, "Last used reported", dockerOptionalTime(record.LastUsedAt))
		usage := "NOT RECORDED"
		if record.UsageCount != nil {
			usage = fmt.Sprint(*record.UsageCount)
		}
		printField(guard, "Usage count reported", usage)
		printField(guard, "Size", "NOT REPORTED")
	}
	printWrapped(guard, "Missing flags, dates and usage counts remain NOT RECORDED. Reported usage flags do not prove dispensability. Sizes and reclaimable savings are not reported. No cleanup is approved or available.", "")
	printWrapped(guard, "The installed Docker CLI and selected daemon/extensions are trust boundaries. The cache query can enumerate server records, calculate sizes, run snapshotter/content-store work and change internal daemon accounting even though Rydd omits sizes. Client time, response and record limits do not bound that server work; closing the connection does not prove it stopped.", "")
	printWrapped(guard, "A Unix endpoint does not prove physical locality or authenticate its namespace. Rydd requested no mutation, helper container, pull, gRPC or build and saved no report or history changes.", "")
	if guard.err != nil {
		return fmt.Errorf("Docker cache metadata reply did not finish; no report or Rydd history was saved: %w", guard.err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("Docker cache metadata reply was canceled; no report or Rydd history was saved: %w", err)
	}
	return nil
}
