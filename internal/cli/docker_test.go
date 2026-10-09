package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/dockerinfo"
)

const dockerCLIInfoBody = `{"ID":"generated-cli-daemon","OSType":"linux","ServerVersion":"25.0.0","Labels":["discard-private-setting"]}`
const dockerCLISocketName = "engine-123456789.sock"

type dockerCLIEngine struct {
	endpoint    string
	connections atomic.Int64
	mu          sync.Mutex
	requests    []string
	methods     []string
}

func newDockerCLIEngine(t *testing.T, change func(int, http.ResponseWriter, *http.Request) bool) *dockerCLIEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "rydd-docker-cli-")
	if err != nil {
		t.Fatal(err)
	}
	if len(filepath.Join(dir, dockerCLISocketName)) > 90 {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		dir, err = os.MkdirTemp("/tmp", "rydd-docker-cli-")
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, dockerCLISocketName)
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	e := &dockerCLIEngine{endpoint: "unix://" + path}
	server := &http.Server{ReadHeaderTimeout: time.Second, ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			e.connections.Add(1)
		}
	}}
	server.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
		e.mu.Lock()
		index := len(e.requests)
		e.requests = append(e.requests, request.URL.RequestURI())
		e.methods = append(e.methods, request.Method)
		e.mu.Unlock()
		out.Header().Set("Content-Type", "application/json")
		out.Header().Set("API-Version", dockerinfo.APIVersion)
		out.Header().Set("OSType", "linux")
		out.Header().Set("Server", "Docker/25.0.0 (linux)")
		if change != nil && change(index, out, request) {
			return
		}
		switch request.URL.Path {
		case "/v1.44/info":
			_, _ = fmt.Fprint(out, dockerCLIInfoBody)
		case "/v1.44/images/json":
			_, _ = fmt.Fprintf(out, `[{"Id":"sha256:%s","RepoTags":["fixture:Ω\"quoted"],"Created":1700000000,"Size":123456789,"Labels":{"private":"discard-image-label"}}]`, strings.Repeat("a", 64))
		case "/v1.44/containers/json":
			_, _ = fmt.Fprintf(out, `[{"Id":"%s","Names":["/generated Ω\"quoted"],"Created":1700000001,"State":"running","Command":"discard-command","Mounts":[{"Source":"discard-mount"}]},{"Id":"%s","Names":[],"Created":1700000002,"State":"exited","SizeRw":123456789}]`, strings.Repeat("b", 64), strings.Repeat("c", 64))
		default:
			http.NotFound(out, request)
		}
	})
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		<-done
		_ = os.RemoveAll(dir)
	})
	return e
}

func (e *dockerCLIEngine) resolver(_ context.Context, name string) (dockerinfo.ResolvedContext, error) {
	return dockerinfo.ResolvedContext{ContextName: name, Endpoint: e.endpoint}, nil
}

func (e *dockerCLIEngine) observed() ([]string, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.requests...), append([]string(nil), e.methods...)
}

func dockerCLIReportFromEngine(ctx context.Context, args []string, e *dockerCLIEngine) (DockerMetadataReport, error) {
	return dockerMetadataWithDiscover(ctx, args, func(ctx context.Context, name string) (dockerinfo.Report, error) {
		return dockerinfo.DiscoverWithResolver(ctx, name, e.resolver)
	})
}

func dockerCLIZeroReport(t *testing.T, r DockerMetadataReport, err, want error) {
	t.Helper()
	if err == nil || want != nil && !errors.Is(err, want) || !reflect.DeepEqual(r, DockerMetadataReport{}) {
		t.Fatal("refusal returned positive partial metadata or lost error identity", r, err, want)
	}
}

func assertDockerCLIProjectionSizes(t *testing.T, raw []byte, lists ...string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range lists {
		var rows []map[string]json.RawMessage
		if err := json.Unmarshal(fields[name], &rows); err != nil {
			t.Fatal("invalid projected list", name, err)
		}
		for _, row := range rows {
			if size, ok := row["size_bytes"]; !ok || !bytes.Equal(size, []byte("null")) {
				t.Fatal("projected size must remain explicit null", name, string(raw))
			}
			for _, omitted := range []string{"Size", "SizeRw", "SizeRootFs"} {
				if _, ok := row[omitted]; ok {
					t.Fatal("unprojected daemon size field exposed", name, omitted, string(raw))
				}
			}
		}
	}
}

func TestDockerMetadataCLIArgumentsRefuseBeforeDiscovery(t *testing.T) {
	calls := 0
	discover := func(context.Context, string) (dockerinfo.Report, error) {
		calls++
		return dockerinfo.Report{}, errors.New("unexpected discovery")
	}
	for _, args := range [][]string{
		{}, {"--metadata"}, {"--context", "generated"}, {"--metadata=false", "--context", "generated"},
		{"--metadata=bad", "--context", "generated"}, {"--metadata", "--metadata", "--context", "generated"},
		{"--metadata", "--context", "generated", "--context", "generated"}, {"--metadata", "--context", ""},
		{"--metadata", "--context", "__complete"}, {"--metadata", "--context", "--help"},
		{"--metadata", "--context", "bad/name"}, {"--metadata", "--context", "bad name"},
		{"--metadata", "--context", "bad\x1bname"}, {"--metadata", "--context", "bad\xffname"},
		{"--metadata", "--context", strings.Repeat("a", 129)}, {"--metadata", "--context", "generated", "extra"},
		{"--metadata", "--context", "generated", "--", "--metadata"},
		{"--metadata", "--context", "generated", "--host", "unix:///tmp/generated.sock"},
		{"--metadata", "--context", "generated", "--builder", "generated"},
		{"--metadata", "--context", "generated", "--volumes=false"},
		{"--metadata", "--context", "generated", "--cache=false"},
		{"--metadata", "--context", "generated", "--limit", "1"},
		{"--metadata", "--context", "generated", "--max-body-bytes", "1"},
		{"--metadata", "--context", "generated", "-d", "/generated"},
	} {
		r, err := dockerMetadataWithDiscover(context.Background(), args, discover)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Fatal("malformed input did not return a usage error", args, err)
		}
		dockerCLIZeroReport(t, r, err, nil)
	}
	if calls != 0 {
		t.Fatal("refused input reached discovery", calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := dockerMetadataWithDiscover(ctx, []string{"--metadata", "--context", "generated"}, discover)
	dockerCLIZeroReport(t, r, err, context.Canceled)
	if calls != 0 {
		t.Fatal("already canceled input reached discovery")
	}
}

func TestDockerMetadataCLIGeneratedEngineProjectionAndHumanScope(t *testing.T) {
	e := newDockerCLIEngine(t, nil)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unchanged-generated-history"), []byte("generated saved history"), 0600); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, dir)
	r, err := dockerCLIReportFromEngine(context.Background(), []string{"--metadata=true", "--context", "Generated_1.test-context"}, e)
	if err != nil {
		t.Fatal(err)
	}
	paths, methods := e.observed()
	wantPaths := []string{"/v1.44/info", "/v1.44/images/json?all=true", "/v1.44/containers/json?all=true&size=false&limit=129", "/v1.44/info"}
	if r.ContextName != "Generated_1.test-context" || r.Endpoint != e.endpoint || r.DaemonID != "generated-cli-daemon" || len(r.Images) != 1 || len(r.Containers) != 2 || r.Images[0].SizeBytes != nil || r.Containers[0].SizeBytes != nil || r.Containers[1].SizeBytes != nil || e.connections.Load() != 1 || !reflect.DeepEqual(paths, wantPaths) {
		t.Fatal("CLI widened metadata scope or lost exact selected observations", r, paths, e.connections.Load())
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatal("mutation method", method)
		}
	}
	var out, stderr bytes.Buffer
	if code := emit(&out, &stderr, map[string]any{"api_version": APIVersion, "ok": true, "command": "docker", "report": r}, 0); code != 0 || stderr.Len() != 0 {
		t.Fatal("JSON metadata reply failed", code, stderr.String())
	}
	var envelope struct {
		Version int                  `json:"api_version"`
		OK      bool                 `json:"ok"`
		Command string               `json:"command"`
		Report  DockerMetadataReport `json:"report"`
	}
	decoder := json.NewDecoder(&out)
	if err := decoder.Decode(&envelope); err != nil || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "docker" {
		t.Fatal("wrong metadata envelope", envelope, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatal("second JSON reply", err)
	}
	encoded, err := json.Marshal(envelope.Report)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{`"sequential_observations":true`, `"atomic_snapshot":false`, `"physical_locality_verified":false`, `"namespace_authenticated":false`, `"sizes_measured":false`, `"current_state_verified":false`, `"cleanup_approved":false`, `"executable":false`, `"savings_bytes":null`, `"persisted":false`, `"builder_pinned":false`, `"volumes_checked":false`, `"cache_checked":false`, `"size_bytes":null`} {
		if !strings.Contains(string(encoded), text) {
			t.Fatal("machine reply lost a metadata qualification", text, string(encoded))
		}
	}
	assertDockerCLIProjectionSizes(t, encoded, "images", "containers")
	if !strings.Contains(envelope.Report.Endpoint, "123456789") {
		t.Fatal("generated endpoint lost legitimate numeric canary", envelope.Report.Endpoint)
	}
	for _, discarded := range []string{"discard-private-setting", "discard-image-label", "discard-command", "discard-mount"} {
		if strings.Contains(string(encoded), discarded) {
			t.Fatal("unrelated daemon metadata leaked", discarded, string(encoded))
		}
	}
	var human bytes.Buffer
	if err := printDockerMetadataReport(context.Background(), &human, r); err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(human.String()), " ")
	for _, text := range []string{"1 image and 2 containers", fmt.Sprintf("%q", r.ContextName), fmt.Sprintf("%q", r.Endpoint), fmt.Sprintf("%q", r.DaemonID), r.StartedAt.Format(time.RFC3339Nano), r.CompletedAt.Format(time.RFC3339Nano), fmt.Sprintf("%q", r.Images[0].ID), fmt.Sprintf("%q", r.Images[0].Tags[0]), fmt.Sprintf("%q", r.Containers[0].Names[0]), "NONE RECORDED", "NOT MEASURED", "sequential observations", "not an atomic snapshot", "Volumes and cache were not checked", "No builder was selected or pinned", "does not prove physical locality", "No cleanup is approved", "Rydd history change was requested"} {
		if !strings.Contains(flat, strings.Join(strings.Fields(text), " ")) {
			t.Fatal("human metadata reply lost scope or exact evidence", text, human.String())
		}
	}
	if !utf8.ValidString(human.String()) || strings.ContainsRune(human.String(), '\x1b') || !reflect.DeepEqual(before, hashCLIBytes(t, dir)) {
		t.Fatal("human reply exposed controls or changed generated history")
	}
}

func TestDockerMetadataCLIUnavailableAndPartialRepliesAreNotReports(t *testing.T) {
	for _, sentinel := range []error{dockerinfo.ErrResolver, dockerinfo.ErrEndpoint, dockerinfo.ErrBounds, dockerinfo.ErrProtocol, dockerinfo.ErrDaemonChanged, dockerinfo.ErrContext, context.Canceled, context.DeadlineExceeded} {
		r, err := dockerMetadataWithDiscover(context.Background(), []string{"--metadata", "--context", "generated"}, func(ctx context.Context, name string) (dockerinfo.Report, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > dockerinfo.OperationLimit || name != "generated" {
				t.Fatal("discovery escaped the finite selected scope", deadline, name)
			}
			return dockerinfo.Report{ContextName: name, DaemonID: "partial-positive", Images: []dockerinfo.Image{{ID: "partial-positive"}}}, errors.Join(sentinel, errors.New("untrusted stderr\x1b[31m private configuration"))
		})
		dockerCLIZeroReport(t, r, err, sentinel)
		if strings.Contains(err.Error(), "untrusted") || strings.Contains(err.Error(), "private") || strings.ContainsRune(err.Error(), '\x1b') {
			t.Fatal("discovery error exposed untrusted details", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := dockerMetadataWithDiscover(ctx, []string{"--metadata", "--context", "generated"}, func(context.Context, string) (dockerinfo.Report, error) {
		cancel()
		return dockerinfo.Report{ContextName: "generated", Images: []dockerinfo.Image{{ID: "partial-positive"}}}, nil
	})
	dockerCLIZeroReport(t, r, err, context.Canceled)
}

func TestDockerMetadataCLIGeneratedEngineFailureAndCancellation(t *testing.T) {
	t.Run("daemon changed after image and container observations", func(t *testing.T) {
		e := newDockerCLIEngine(t, func(index int, out http.ResponseWriter, _ *http.Request) bool {
			if index != 3 {
				return false
			}
			_, _ = fmt.Fprint(out, strings.Replace(dockerCLIInfoBody, "generated-cli-daemon", "changed-generated-daemon", 1))
			return true
		})
		r, err := dockerCLIReportFromEngine(context.Background(), []string{"--metadata", "--context", "generated"}, e)
		dockerCLIZeroReport(t, r, err, dockerinfo.ErrDaemonChanged)
		paths, _ := e.observed()
		if len(paths) != 4 || e.connections.Load() != 1 {
			t.Fatal("failed operation retried or used another connection", paths, e.connections.Load())
		}
	})
	t.Run("canceled final observation", func(t *testing.T) {
		entered, closed := make(chan struct{}), make(chan struct{})
		e := newDockerCLIEngine(t, func(index int, _ http.ResponseWriter, request *http.Request) bool {
			if index != 3 {
				return false
			}
			close(entered)
			<-request.Context().Done()
			close(closed)
			return true
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		var report DockerMetadataReport
		var err error
		go func() {
			report, err = dockerCLIReportFromEngine(ctx, []string{"--metadata", "--context", "generated"}, e)
			close(done)
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("generated final observation not reached")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("cancellation did not return")
		}
		dockerCLIZeroReport(t, report, err, context.Canceled)
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("held generated connection did not close")
		}
	})
}

func TestDockerMetadataCLIHumanOutputFailuresAndLateCancellation(t *testing.T) {
	e := newDockerCLIEngine(t, nil)
	r, err := dockerCLIReportFromEngine(context.Background(), []string{"--metadata", "--context", "generated"}, e)
	if err != nil {
		t.Fatal(err)
	}
	for _, short := range []bool{false, true} {
		if err := printDockerMetadataReport(context.Background(), hashFailWriter{short: short}, r); err == nil || !strings.Contains(err.Error(), "reply did not finish") {
			t.Fatal("failed human reply appeared successful", short, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := &hashChoiceCancelWriter{cancel: cancel}
	err = printDockerMetadataReport(ctx, out, r)
	cancel()
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "reply was canceled") || !strings.Contains(err.Error(), "no report or Rydd history was saved") {
		t.Fatal("late cancellation lost reply qualification", err)
	}
	paths, _ := e.observed()
	if len(paths) != 4 || e.connections.Load() != 1 {
		t.Fatal("rendering performed new daemon discovery", paths, e.connections.Load())
	}
}

func TestDockerMetadataCLIRunStrictArgumentsValuesAndNoInitialization(t *testing.T) {
	base := filepath.Join(t.TempDir(), "missing generated state")
	run := func(ctx context.Context, args []string) (int, string, string) {
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &stderr)
		return code, out.String(), stderr.String()
	}
	for _, args := range [][]string{
		{"docker"}, {"docker", "--metadata"}, {"docker", "--context", "generated"},
		{"docker", "--metadata=false", "--context", "generated"},
		{"docker", "--metadata", "--metadata", "--context", "generated"},
		{"docker", "--metadata", "--context", "generated", "--context", "generated"},
		{"docker", "--metadata", "--context", "__complete"},
		{"docker", "--metadata", "--context", "generated", "--volumes=false"},
		{"docker", "--metadata", "--context", "generated", "--host", "unix:///tmp/generated.sock"},
		{"docker", "--metadata", "--context", "generated", "extra"},
		{"docker", "--metadata", "--context", "generated", "--", "--context", "other"},
	} {
		code, raw, stderr := run(context.Background(), append([]string{"--json"}, args...))
		buildOutputCLIError(t, code, raw, stderr, "docker", "invalid_arguments", 2)
		code, raw, stderr = run(context.Background(), args)
		if code != 2 || raw != "" || stderr == "" || strings.Contains(stderr, "Use rydd init") {
			t.Fatal("human malformed Docker command used discovery or initialization guidance", args, code, raw, stderr)
		}
	}
	for _, option := range []string{"--context", "-context"} {
		for _, value := range []string{"--json", "-json"} {
			args := []string{"docker", "--metadata", option, value}
			code, raw, stderr := run(context.Background(), args)
			if code != 2 || raw != "" || stderr == "" {
				t.Fatal("context data selected machine output", option, value, code, raw, stderr)
			}
			code, raw, stderr = run(context.Background(), append([]string{"--json"}, args...))
			buildOutputCLIError(t, code, raw, stderr, "docker", "invalid_arguments", 2)
		}
	}
	for _, machine := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		args := []string{"docker", "--metadata", "--context", "generated"}
		if machine {
			args = append([]string{"--json"}, args...)
		}
		code, raw, stderr := run(ctx, args)
		if machine {
			buildOutputCLIError(t, code, raw, stderr, "docker", "canceled", 1)
		} else if code != 1 || raw != "" || !strings.Contains(stderr, "context canceled") || strings.Contains(stderr, "Use rydd init") {
			t.Fatal("canceled Docker command reached discovery or initialization", code, raw, stderr)
		}
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("malformed or canceled Docker commands initialized storage", err)
	}
}

func TestDockerMetadataCLIErrorEnvelopePriorityAndCapabilities(t *testing.T) {
	for _, test := range []struct {
		cause error
		code  string
		exit  int
	}{
		{dockerinfo.ErrContext, "invalid_arguments", 2},
		{errors.Join(dockerinfo.ErrResolver, os.ErrNotExist), "docker_context_unavailable", 1},
		{dockerinfo.ErrEndpoint, "docker_endpoint_unsupported", 1},
		{errors.Join(dockerinfo.ErrResolver, dockerinfo.ErrBounds), "docker_metadata_bounds", 1},
		{dockerinfo.ErrProtocol, "docker_protocol_unsupported", 1},
		{dockerinfo.ErrDaemonChanged, "docker_daemon_changed", 1},
		{errors.Join(dockerinfo.ErrResolver, dockerinfo.ErrBounds, context.Canceled), "canceled", 1},
		{errors.Join(dockerinfo.ErrProtocol, context.DeadlineExceeded), "canceled", 1},
	} {
		var out, stderr bytes.Buffer
		err := dockerMetadataFailure(errors.Join(test.cause, errors.New("private untrusted\x1b stderr")))
		code := operationFailure(&out, &stderr, "docker", err)
		buildOutputCLIError(t, code, out.String(), stderr.String(), "docker", test.code, test.exit)
		if strings.Contains(out.String(), "untrusted") || strings.Contains(out.String(), "private") || strings.Contains(out.String(), "stderr") {
			t.Fatal("error envelope leaked external details", out.String())
		}
	}
	// Exercise joined-cause priority directly, rather than only the helper's
	// safe cancellation projection.
	var out, stderr bytes.Buffer
	code := operationFailure(&out, &stderr, "docker", errors.Join(dockerinfo.ErrResolver, dockerinfo.ErrBounds, context.Canceled))
	buildOutputCLIError(t, code, out.String(), stderr.String(), "docker", "canceled", 1)
	base := filepath.Join(t.TempDir(), "never initialized")
	for _, args := range [][]string{{"--help"}, {"capabilities"}, {"--json", "capabilities"}} {
		out.Reset()
		stderr.Reset()
		code := Run(context.Background(), append([]string{"--data-dir", base}, args...), &out, &stderr)
		if code != 0 || stderr.Len() != 0 {
			t.Fatal("Docker discovery changed global help/capabilities", args, code, out.String(), stderr.String())
		}
		if args[0] == "--help" && !strings.Contains(out.String(), "docker --metadata --context NAME [--json]") {
			t.Fatal("help lacks explicit finite Docker scope", out.String())
		}
		if args[0] == "capabilities" && !strings.Contains(out.String(), ", docker,") {
			t.Fatal("human capabilities omitted Docker", out.String())
		}
		if args[0] == "--json" {
			for _, text := range []string{`"docker_image_container_metadata":true`, `"builder_metadata":false`, `"docker_volume_metadata":false`, `"docker_cache_metadata":true`, `"cleanup":false`, `"name":"docker"`, "--metadata --context NAME", "docker_context_unavailable", "docker_endpoint_unsupported", "docker_metadata_bounds", "docker_protocol_unsupported", "docker_daemon_changed"} {
				if !strings.Contains(out.String(), text) {
					t.Fatal("capabilities lost finite Docker support or qualifications", text, out.String())
				}
			}
		}
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("help/capabilities initialized storage", err)
	}
}

func TestDockerMetadataCLIMissingExecutableIsHarmless(t *testing.T) {
	// An empty generated executable directory makes real host Docker discovery
	// impossible while exercising production dispatch's unavailable path.
	t.Setenv("PATH", t.TempDir())
	base := filepath.Join(t.TempDir(), "missing generated state")
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", base, "docker", "--metadata", "--context", "generated"}
		if machine {
			args = append([]string{"--json"}, args...)
		}
		var out, stderr bytes.Buffer
		code := Run(context.Background(), args, &out, &stderr)
		if machine {
			buildOutputCLIError(t, code, out.String(), stderr.String(), "docker", "docker_context_unavailable", 1)
		} else if code != 1 || out.Len() != 0 || !strings.Contains(stderr.String(), "context could not be resolved") || strings.Contains(stderr.String(), "Use rydd init") {
			t.Fatal("missing Docker was an empty success or initialization request", code, out.String(), stderr.String())
		}
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("missing Docker initialized Rydd storage", err)
	}
}
