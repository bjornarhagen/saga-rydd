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

// These qualifications describe this finite image/container leaf. They do not
// extend its scope to volumes, builders or cache discovery.
type DockerMetadataReport struct {
	dockerinfo.Report
	BuilderPinned  bool `json:"builder_pinned"`
	VolumesChecked bool `json:"volumes_checked"`
	CacheChecked   bool `json:"cache_checked"`
}

type dockerMetadataMode struct {
	value bool
	set   bool
}

func (v *dockerMetadataMode) String() string   { return strconv.FormatBool(v.value) }
func (v *dockerMetadataMode) IsBoolFlag() bool { return true }
func (v *dockerMetadataMode) Set(value string) error {
	if v.set {
		return errors.New("--metadata cannot be repeated")
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	v.value, v.set = parsed, true
	return nil
}

type dockerContextValue struct {
	value string
	set   bool
}

func (v *dockerContextValue) String() string { return v.value }
func (v *dockerContextValue) Set(value string) error {
	if v.set {
		return errors.New("--context cannot be repeated")
	}
	v.value, v.set = value, true
	return nil
}

func dockerMetadata(ctx context.Context, args []string) (DockerMetadataReport, error) {
	return dockerMetadataWithDiscover(ctx, args, dockerinfo.Discover)
}

// The injected discovery function is private and never changes global dispatch.
// Parsing and context-name validation finish before discovery can run.
func dockerMetadataWithDiscover(ctx context.Context, args []string, discover func(context.Context, string) (dockerinfo.Report, error)) (DockerMetadataReport, error) {
	var metadata dockerMetadataMode
	var selected dockerContextValue
	f := flag.NewFlagSet("docker", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&metadata, "metadata", "read finite image and container metadata")
	f.Var(&selected, "context", "one explicitly selected Docker context")
	if err := f.Parse(args); err != nil {
		return DockerMetadataReport{}, usageError{err}
	}
	if !metadata.set || !metadata.value || !selected.set || !dockerinfo.ValidContextName(selected.value) || f.NArg() != 0 {
		return DockerMetadataReport{}, usageError{errors.New("docker requires --metadata --context NAME; NAME must use 1–128 ASCII letters or digits, with _, - or . allowed after the first character; repeated options and other scopes are unavailable")}
	}
	ctx, cancel := context.WithTimeout(ctx, dockerinfo.OperationLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return DockerMetadataReport{}, err
	}
	if discover == nil {
		return DockerMetadataReport{}, dockerMetadataFailure(dockerinfo.ErrResolver)
	}
	r, err := discover(ctx, selected.value)
	if err != nil {
		return DockerMetadataReport{}, dockerMetadataFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return DockerMetadataReport{}, err
	}
	return DockerMetadataReport{Report: r}, nil
}

// Keep sentinels and cancellation available to the envelope while excluding
// subprocess stderr, server response text and unrelated configuration details.
type dockerMetadataError struct {
	cause   error
	message string
}

func (e dockerMetadataError) Error() string { return e.message }
func (e dockerMetadataError) Unwrap() error { return e.cause }

func dockerMetadataFailure(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, dockerinfo.ErrContext) {
		return dockerMetadataError{cause: errors.Join(usageError{dockerinfo.ErrContext}, dockerinfo.ErrContext), message: dockerinfo.ErrContext.Error()}
	}
	message := "Docker metadata is unavailable. No report was returned or saved."
	switch {
	case errors.Is(err, dockerinfo.ErrBounds):
		message = "Docker metadata exceeded the finite output/object profile. No report was returned or saved."
	case errors.Is(err, dockerinfo.ErrEndpoint):
		message = "The selected Docker context does not name a supported canonical Unix socket endpoint. No daemon metadata was requested."
	case errors.Is(err, dockerinfo.ErrDaemonChanged):
		message = "Docker daemon identity or version changed during the sequential observations. No report was returned or saved."
	case errors.Is(err, dockerinfo.ErrResolver):
		message = "The selected Docker context could not be resolved. Check the installed Docker CLI and the exact context name. No report was returned or saved."
	case errors.Is(err, dockerinfo.ErrProtocol):
		message = "The selected Docker daemon was unavailable or did not meet the supported metadata protocol. No report was returned or saved."
	}
	return dockerMetadataError{cause: err, message: message}
}

func printDockerMetadataReport(ctx context.Context, out io.Writer, r DockerMetadataReport) error {
	guard := &reviewOutput{writer: out}
	imageNoun, containerNoun := "images", "containers"
	if len(r.Images) == 1 {
		imageNoun = "image"
	}
	if len(r.Containers) == 1 {
		containerNoun = "container"
	}
	printWrapped(guard, fmt.Sprintf("Docker metadata observed: %s %s and %s %s.", humanCount(len(r.Images)), imageNoun, humanCount(len(r.Containers)), containerNoun), "")
	printField(guard, "Selected context", fmt.Sprintf("%q", r.ContextName))
	printField(guard, "Resolved endpoint", fmt.Sprintf("%q", r.Endpoint))
	printField(guard, "Daemon ID", fmt.Sprintf("%q", r.DaemonID))
	printField(guard, "Server version", fmt.Sprintf("%q", r.ServerVersion))
	printField(guard, "Engine API profile", r.APIVersion)
	printField(guard, "Observations started", r.StartedAt.UTC().Format(time.RFC3339Nano))
	printField(guard, "Observations completed", r.CompletedAt.UTC().Format(time.RFC3339Nano))
	printWrapped(guard, "These are sequential observations on one held connection. The daemon ID and version matched before and after the requests. This is not an atomic snapshot or a check of the daemon's current state.", "")
	for i, image := range r.Images {
		fmt.Fprintf(guard, "\nImage %d: %q\n", i+1, image.ID)
		printField(guard, "Tags", quotedDockerNames(image.Tags))
		printField(guard, "Created", image.CreatedAt.UTC().Format(time.RFC3339Nano))
		printField(guard, "Size", "NOT MEASURED")
	}
	for i, container := range r.Containers {
		fmt.Fprintf(guard, "\nContainer %d: %q\n", i+1, container.ID)
		printField(guard, "Names", quotedDockerNames(container.Names))
		printField(guard, "State observed", fmt.Sprintf("%q", container.State))
		printField(guard, "Created", container.CreatedAt.UTC().Format(time.RFC3339Nano))
		printField(guard, "Size", "NOT MEASURED")
	}
	printWrapped(guard, "Image and container metadata only. Volumes and cache were not checked. No builder was selected or pinned. Sizes and savings are not measured. No cleanup is approved or available.", "")
	printWrapped(guard, "A Unix socket endpoint does not prove physical locality or authenticate its namespace. The installed Docker CLI, its configuration and the selected daemon/extensions are trust boundaries. No registry request, helper container, pull, mutation or Rydd history change was requested.", "")
	if guard.err != nil {
		return fmt.Errorf("Docker metadata reply did not finish; no report or Rydd history was saved: %w", guard.err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("Docker metadata reply was canceled; no report or Rydd history was saved: %w", err)
	}
	return nil
}

func quotedDockerNames(names []string) string {
	if len(names) == 0 {
		return "NONE RECORDED"
	}
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
}
