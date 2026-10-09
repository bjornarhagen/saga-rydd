package dockerinfo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const contextTemplate = `{"context_name":{{json .Name}},"endpoint":{{json .Endpoints.docker.Host}}}`

func resolverArgs(name string) []string {
	return []string{"--context", "default", "context", "inspect", "--format", contextTemplate, "--", name}
}

// Preserve only process-location/user variables needed by the installed CLI.
// Inherited context, daemon/API overrides, telemetry destinations, proxies and
// credential variables are absent. The default CLI configuration is trusted.
func resolverEnv(environ []string) []string {
	allowed := map[string]bool{"PATH": true, "HOME": true, "USER": true, "LOGNAME": true, "TMPDIR": true}
	values := map[string]string{}
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if ok && allowed[key] {
			values[key] = value
		}
	}
	result := []string{}
	for _, key := range []string{"PATH", "HOME", "USER", "LOGNAME", "TMPDIR"} {
		if value, ok := values[key]; ok {
			result = append(result, key+"="+value)
		}
	}
	return append(result, "OTEL_SDK_DISABLED=true")
}

func resolveContext(ctx context.Context, name string) (ResolvedContext, error) {
	if !ValidContextName(name) {
		return ResolvedContext{}, ErrContext
	}
	binary, err := exec.LookPath("docker")
	if err != nil {
		return ResolvedContext{}, errors.Join(ErrResolver, err)
	}
	return resolveCommand(ctx, name, binary, resolverEnv(os.Environ()))
}

type cappedOutput struct {
	buffer bytes.Buffer
	limit  int
	cancel context.CancelCauseFunc
}

func (w *cappedOutput) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.buffer.Len() {
		n := w.limit - w.buffer.Len()
		_, _ = w.buffer.Write(p[:n])
		w.cancel(ErrBounds)
		return n, ErrBounds
	}
	return w.buffer.Write(p)
}

func resolveCommand(parent context.Context, name, binary string, env []string) (ResolvedContext, error) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	if err := ctx.Err(); err != nil {
		return ResolvedContext{}, err
	}
	cmd := exec.CommandContext(ctx, binary, resolverArgs(name)...)
	cmd.Env = append([]string(nil), env...)
	cmd.Stdin = nil
	// CommandContext uses Process.Kill, which fences cancellation against reap.
	// Numeric group IDs can be reused before the cancellation watcher joins.
	// Descendants are outside this direct-child cancellation scope.
	cmd.WaitDelay = 250 * time.Millisecond
	stdout := &cappedOutput{limit: MaxResolverStdoutBytes, cancel: cancel}
	stderr := &cappedOutput{limit: MaxResolverStderrBytes, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		return ResolvedContext{}, errors.Join(ErrResolver, err, ctx.Err())
	}
	err := cmd.Wait()
	if cause := context.Cause(ctx); cause != nil {
		return ResolvedContext{}, errors.Join(ErrResolver, cause)
	}
	if err != nil {
		return ResolvedContext{}, fmt.Errorf("%w: context inspect failed: %w", ErrResolver, err)
	}
	// Stderr is deliberately not copied into errors or the user report: it can
	// contain unrelated installed configuration details.
	b := bytes.TrimSpace(stdout.buffer.Bytes())
	if err := strictJSON(b); err != nil {
		return ResolvedContext{}, errors.Join(ErrResolver, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil || len(fields) != 2 || fields["context_name"] == nil || fields["endpoint"] == nil {
		return ResolvedContext{}, ErrResolver
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var result ResolvedContext
	if err := d.Decode(&result); err != nil || result.ContextName != name {
		return ResolvedContext{}, ErrResolver
	}
	if _, err := socketPath(result.Endpoint); err != nil {
		return ResolvedContext{}, err
	}
	if err := ctx.Err(); err != nil {
		return ResolvedContext{}, err
	}
	return result, nil
}
