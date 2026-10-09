package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/dockerinfo"
)

const dockerCLICacheBody = `{"BuildCache":[{"ID":"generated-cache-one","Type":"regular","InUse":true,"Shared":false,"CreatedAt":"2024-01-02T03:04:05Z","LastUsedAt":null,"UsageCount":0,"Size":123456789,"Description":"discard-private-build-description"},{"ID":"generated-cache-two","Type":"source.local"},{"ID":"generated-cache-three","Type":"regular","InUse":null,"Shared":null,"CreatedAt":null,"LastUsedAt":null,"UsageCount":null}],"Images":null,"Containers":null,"Volumes":null}`

func newDockerCacheCLIEngine(t *testing.T, body string, change func(int, http.ResponseWriter, *http.Request) bool) *dockerCLIEngine {
	t.Helper()
	return newDockerCLIEngine(t, func(index int, out http.ResponseWriter, request *http.Request) bool {
		if change != nil && change(index, out, request) {
			return true
		}
		if request.URL.Path == "/v1.44/system/df" {
			_, _ = fmt.Fprint(out, body)
			return true
		}
		return false
	})
}

func dockerCacheCLIFromEngine(ctx context.Context, args []string, e *dockerCLIEngine) (DockerCacheMetadataReport, error) {
	return dockerCacheMetadataWithDiscover(ctx, args, func(ctx context.Context, name string) (dockerinfo.CacheReport, error) {
		return dockerinfo.DiscoverCacheWithResolver(ctx, name, e.resolver)
	})
}

func dockerCacheCLIZero(t *testing.T, r DockerCacheMetadataReport, err, want error) {
	t.Helper()
	if err == nil || want != nil && !errors.Is(err, want) || !reflect.DeepEqual(r, DockerCacheMetadataReport{}) {
		t.Fatal("cache refusal returned positive metadata or lost error identity", r, err, want)
	}
}

func dockerCacheCLIEnvelope(t *testing.T, code int, raw, stderr string) DockerCacheMetadataReport {
	t.Helper()
	var envelope struct {
		Version int                       `json:"api_version"`
		OK      bool                      `json:"ok"`
		Command string                    `json:"command"`
		Report  DockerCacheMetadataReport `json:"report"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(&envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "docker" || envelope.Report.Scope != "engine_embedded_cache" {
		t.Fatal("cache reply lost standard scope/envelope", code, raw, stderr, err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		t.Fatal("second reply", raw, err)
	}
	for _, text := range []string{`"scope":"engine_embedded_cache"`, `"cache_checked":true`, `"images_checked":false`, `"containers_checked":false`, `"volumes_checked":false`, `"builder_pinned":false`, `"daemon_accounting_may_change":true`, `"sequential_observations":true`, `"atomic_snapshot":false`, `"current_state_verified":false`, `"physical_locality_verified":false`, `"namespace_authenticated":false`, `"sizes_measured":false`, `"savings_bytes":null`, `"cleanup_approved":false`, `"executable":false`, `"persisted":false`} {
		if !strings.Contains(raw, text) {
			t.Fatal("cache report lost qualification", text, raw)
		}
	}
	return envelope.Report
}

func TestDockerCacheCLIArgumentsRefuseBeforeDiscovery(t *testing.T) {
	calls := 0
	discover := func(context.Context, string) (dockerinfo.CacheReport, error) {
		calls++
		return dockerinfo.CacheReport{}, errors.New("unexpected discovery")
	}
	for _, args := range [][]string{
		{}, {"--cache-metadata"}, {"--context", "generated"}, {"--cache-metadata=false", "--context", "generated"},
		{"--cache-metadata=bad", "--context", "generated"}, {"--cache-metadata", "--cache-metadata", "--context", "generated"},
		{"--cache-metadata", "--context", "generated", "--context", "generated"}, {"--cache-metadata", "--context", ""},
		{"--cache-metadata", "--context", "__complete"}, {"--cache-metadata", "--context", "bad/name"}, {"--cache-metadata", "--context", strings.Repeat("a", 129)},
		{"--cache-metadata", "--metadata=false", "--context", "generated"}, {"--metadata", "--cache-metadata", "--context", "generated"},
		{"--cache-metadata", "--context", "generated", "--volumes=false"}, {"--cache-metadata", "--context", "generated", "--host", "unix:///tmp/generated.sock"},
		{"--cache-metadata", "--context", "generated", "--builder", "generated"}, {"--cache-metadata", "--context", "generated", "--limit", "1"},
		{"--cache-metadata", "--context", "generated", "--max-body-bytes", "1"}, {"--cache-metadata", "--context", "generated", "extra"},
		{"--cache-metadata", "--context", "generated", "--", "--metadata"},
	} {
		r, err := dockerCacheMetadataWithDiscover(context.Background(), args, discover)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Fatal("invalid cache scope is not usage error", args, err)
		}
		dockerCacheCLIZero(t, r, err, nil)
	}
	if calls != 0 {
		t.Fatal("invalid cache scope reached discovery", calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := dockerCacheMetadataWithDiscover(ctx, []string{"--cache-metadata", "--context", "generated"}, discover)
	dockerCacheCLIZero(t, r, err, context.Canceled)
	if calls != 0 {
		t.Fatal("early cancellation reached discovery")
	}
}

func TestDockerCacheCLIGeneratedProjectionUnknownFieldsAndOldScope(t *testing.T) {
	e := newDockerCacheCLIEngine(t, dockerCLICacheBody, nil)
	r, err := dockerCacheCLIFromEngine(context.Background(), []string{"--cache-metadata", "--context", "generated"}, e)
	if err != nil {
		t.Fatal(err)
	}
	paths, methods := e.observed()
	want := []string{"/v1.44/info", "/v1.44/system/df?type=build-cache", "/v1.44/info"}
	if !reflect.DeepEqual(paths, want) || e.connections.Load() != 1 || len(r.Records) != 3 || r.Endpoint != e.endpoint || r.ContextName != "generated" {
		t.Fatal("cache query widened scope", r, paths, e.connections.Load())
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatal("mutation request", method)
		}
	}
	for _, record := range r.Records {
		if record.SizeBytes != nil {
			t.Fatal("size projected as measured/reclaimable", record)
		}
	}
	if r.Records[0].InUse == nil || !*r.Records[0].InUse || r.Records[0].Shared == nil || *r.Records[0].Shared || r.Records[0].UsageCount == nil || *r.Records[0].UsageCount != 0 {
		t.Fatal("recorded false/zero values became unknown", r.Records[0])
	}
	for _, record := range r.Records[1:] {
		if record.InUse != nil || record.Shared != nil || record.CreatedAt != nil || record.LastUsedAt != nil || record.UsageCount != nil {
			t.Fatal("missing/null fields became false/zero", record)
		}
	}
	var out, stderr bytes.Buffer
	code := emit(&out, &stderr, map[string]any{"api_version": APIVersion, "ok": true, "command": "docker", "report": r}, 0)
	dockerCacheCLIEnvelope(t, code, out.String(), stderr.String())
	if strings.Contains(out.String(), "discard-private") || strings.Contains(out.String(), "123456789") {
		t.Fatal("discarded daemon fields leaked", out.String())
	}
	var human bytes.Buffer
	if err := printDockerResult(context.Background(), &human, r); err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(human.String()), " ")
	for _, text := range []string{"3 records", fmt.Sprintf("%q", r.Endpoint), "Engine's embedded cache only", "Images, containers and volumes were not checked", "No named Buildx builder", "NOT RECORDED", "YES", "NO", "NOT REPORTED", "not an atomic snapshot", "proof of content equality", "change internal daemon accounting", "snapshotter/content-store work", "do not bound that server work", "closing the connection does not prove it stopped", "does not prove physical locality", "No cleanup is approved"} {
		if !strings.Contains(flat, strings.Join(strings.Fields(text), " ")) {
			t.Fatal("human cache output lost evidence/scope/uncertainty", text, human.String())
		}
	}
	if strings.Contains(human.String(), "0001-01-01") || strings.Contains(human.String(), "NOT MEASURED") {
		t.Fatal("unknown times or server size work misrepresented", human.String())
	}
	oldEngine := newDockerCLIEngine(t, nil)
	old, err := dockerMetadataWithDiscover(context.Background(), []string{"--metadata", "--context", "generated"}, func(ctx context.Context, name string) (dockerinfo.Report, error) {
		return dockerinfo.DiscoverWithResolver(ctx, name, oldEngine.resolver)
	})
	if err != nil {
		t.Fatal(err)
	}
	oldPaths, _ := oldEngine.observed()
	if len(oldPaths) != 4 || old.CacheChecked || len(old.Images) != 1 || len(old.Containers) != 2 {
		t.Fatal("old mode widened to cache", old, oldPaths)
	}
}

func TestDockerCacheCLIEmptyRequiredNullBoundsAndNoPartialReply(t *testing.T) {
	for _, test := range []struct {
		body  string
		want  error
		empty bool
	}{
		{body: `{"BuildCache":[]}`, empty: true},
		{body: `{}`, want: dockerinfo.ErrProtocol},
		{body: `{"BuildCache":null}`, want: dockerinfo.ErrProtocol},
		{body: `{"BuildCache":[],"BuildCache":[]}`, want: dockerinfo.ErrProtocol},
	} {
		e := newDockerCacheCLIEngine(t, test.body, nil)
		r, err := dockerCacheCLIFromEngine(context.Background(), []string{"--cache-metadata", "--context", "generated"}, e)
		if test.empty {
			if err != nil || r.Records == nil || len(r.Records) != 0 {
				t.Fatal("explicit empty records refused or invented unknown", r, err)
			}
		} else {
			dockerCacheCLIZero(t, r, err, test.want)
		}
	}
	rows := make([]map[string]any, 129)
	for i := range rows {
		rows[i] = map[string]any{"ID": fmt.Sprintf("generated-%d", i), "Type": "regular"}
	}
	body, _ := json.Marshal(map[string]any{"BuildCache": rows})
	e := newDockerCacheCLIEngine(t, string(body), nil)
	r, err := dockerCacheCLIFromEngine(context.Background(), []string{"--cache-metadata", "--context", "generated"}, e)
	dockerCacheCLIZero(t, r, err, dockerinfo.ErrBounds)
	e = newDockerCacheCLIEngine(t, dockerCLICacheBody, func(index int, out http.ResponseWriter, _ *http.Request) bool {
		if index != 2 {
			return false
		}
		_, _ = fmt.Fprint(out, strings.Replace(dockerCLIInfoBody, "generated-cli-daemon", "changed-daemon", 1))
		return true
	})
	r, err = dockerCacheCLIFromEngine(context.Background(), []string{"--cache-metadata", "--context", "generated"}, e)
	dockerCacheCLIZero(t, r, err, dockerinfo.ErrDaemonChanged)
	for _, cause := range []error{dockerinfo.ErrResolver, dockerinfo.ErrEndpoint, dockerinfo.ErrProtocol, dockerinfo.ErrBounds, context.Canceled} {
		r, err := dockerCacheMetadataWithDiscover(context.Background(), []string{"--cache-metadata", "--context", "generated"}, func(context.Context, string) (dockerinfo.CacheReport, error) {
			return dockerinfo.CacheReport{Records: []dockerinfo.CacheRecord{{ID: "positive-partial"}}}, errors.Join(cause, errors.New("private untrusted daemon stderr"))
		})
		dockerCacheCLIZero(t, r, err, cause)
		if strings.Contains(err.Error(), "private") {
			t.Fatal("daemon details leaked", err)
		}
	}
}

func TestDockerCacheCLICancellationClosesHeldConnectionWithoutPartialReply(t *testing.T) {
	entered, closed := make(chan struct{}), make(chan struct{})
	e := newDockerCacheCLIEngine(t, dockerCLICacheBody, func(index int, _ http.ResponseWriter, request *http.Request) bool {
		if index != 2 {
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
	var report DockerCacheMetadataReport
	var err error
	go func() {
		report, err = dockerCacheCLIFromEngine(ctx, []string{"--cache-metadata", "--context", "generated"}, e)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("final cache observation was not reached")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cache cancellation remained blocked")
	}
	dockerCacheCLIZero(t, report, err, context.Canceled)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("held generated cache connection remained open")
	}
}

func TestDockerCacheCLIRunStrictModesJSONValuesMissingExecutableAndCapabilities(t *testing.T) {
	// Installed Docker cannot be selected in any path below.
	t.Setenv("PATH", t.TempDir())
	base := filepath.Join(t.TempDir(), "missing Rydd state")
	run := func(ctx context.Context, args ...string) (int, string, string) {
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &stderr)
		return code, out.String(), stderr.String()
	}
	for _, args := range [][]string{
		{"--cache-metadata=false", "--context", "generated"}, {"--cache-metadata", "--cache-metadata", "--context", "generated"},
		{"--cache-metadata", "--metadata", "--context", "generated"}, {"--metadata=false", "--cache-metadata", "--context", "generated"},
		{"--cache-metadata", "--context", "generated", "--context", "generated"}, {"--cache-metadata", "--context", "generated", "--cache=false"},
		{"--cache-metadata", "--context", "generated", "--", "--metadata"},
	} {
		args = append([]string{"docker"}, args...)
		code, raw, stderr := run(context.Background(), append([]string{"--json"}, args...)...)
		buildOutputCLIError(t, code, raw, stderr, "docker", "invalid_arguments", 2)
		code, raw, stderr = run(context.Background(), args...)
		if code != 2 || raw != "" || stderr == "" || strings.Contains(stderr, "Use rydd init") {
			t.Fatal("human mode selected discovery", args, code, raw, stderr)
		}
	}
	for _, option := range []string{"--context", "-context"} {
		for _, value := range []string{"--json", "-json", "--cache-metadata"} {
			args := []string{"docker", "--cache-metadata", option, value}
			code, raw, stderr := run(context.Background(), args...)
			if code != 2 || raw != "" || stderr == "" {
				t.Fatal("context value selected output/scope", option, value, code, raw, stderr)
			}
			code, raw, stderr = run(context.Background(), append([]string{"--json"}, args...)...)
			buildOutputCLIError(t, code, raw, stderr, "docker", "invalid_arguments", 2)
		}
	}
	code, raw, stderr := run(context.Background(), "--json", "docker", "--cache-metadata", "--context", "generated")
	buildOutputCLIError(t, code, raw, stderr, "docker", "docker_context_unavailable", 1)
	code, raw, stderr = run(context.Background(), "docker", "--cache-metadata", "--context", "generated")
	if code != 1 || raw != "" || strings.Contains(stderr, "Use rydd init") {
		t.Fatal("missing Docker initialized or succeeded", code, raw, stderr)
	}
	code, raw, stderr = run(context.Background(), "capabilities", "--json")
	if code != 0 || stderr != "" {
		t.Fatal(code, raw, stderr)
	}
	for _, text := range []string{`"docker_engine_cache_metadata":true`, `"builder_metadata":false`, "--cache-metadata --context NAME", "type=build-cache only", "change daemon accounting"} {
		if !strings.Contains(raw, text) {
			t.Fatal("capability scope missing", text, raw)
		}
	}
	code, raw, stderr = run(context.Background(), "--help")
	if code != 0 || stderr != "" || !strings.Contains(raw, "docker --cache-metadata --context NAME") {
		t.Fatal("missing cache help", code, raw, stderr)
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("cache metadata initialized storage", err)
	}
}

func TestDockerCacheCLIGeneratedProductionDispatchOutputAndCancellation(t *testing.T) {
	e := newDockerCacheCLIEngine(t, dockerCLICacheBody, nil)
	helperDir := t.TempDir()
	resolved, _ := json.Marshal(dockerinfo.ResolvedContext{ContextName: "generated", Endpoint: e.endpoint})
	// This generated executable performs no Docker/configuration discovery. Its
	// only output is the generated server's exact endpoint for built-in inspect.
	helper := "#!/bin/sh\nprintf '%s\\n' " + shellQuote(string(resolved)) + "\n"
	if err := os.WriteFile(filepath.Join(helperDir, "docker"), []byte(helper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", helperDir)
	t.Setenv("HOME", helperDir)
	base := filepath.Join(t.TempDir(), "untouched state")
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", base, "docker", "--cache-metadata", "--context", "generated"}
		if machine {
			args = append(args, "--json")
		}
		var out, stderr bytes.Buffer
		code := Run(context.Background(), args, &out, &stderr)
		if machine {
			dockerCacheCLIEnvelope(t, code, out.String(), stderr.String())
		} else if code != 0 || stderr.Len() != 0 || !strings.Contains(out.String(), "3 records") {
			t.Fatal("production human cache dispatch failed", code, out.String(), stderr.String())
		}
		for _, short := range []bool{false, true} {
			stderr.Reset()
			code = Run(context.Background(), args, hashFailWriter{short: short}, &stderr)
			if code != 1 || stderr.Len() == 0 {
				t.Fatal("short/failed cache reply reported success", machine, short, code, stderr.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		canceledOut := &hashChoiceCancelWriter{cancel: cancel}
		stderr.Reset()
		code = Run(ctx, args, canceledOut, &stderr)
		cancel()
		if code != 1 || !strings.Contains(stderr.String(), "reply was canceled") || !strings.Contains(stderr.String(), "no report or Rydd history was saved") {
			t.Fatal("cache late cancellation reported success", machine, code, canceledOut.String(), stderr.String())
		}
		if machine {
			dockerCacheCLIEnvelope(t, 0, canceledOut.String(), "")
		}
	}
	paths, methods := e.observed()
	if len(paths) != 24 || e.connections.Load() != 8 {
		t.Fatal("render/output paths requested unexpected discovery/reconnection", paths, e.connections.Load())
	}
	for i, path := range paths {
		want := []string{"/v1.44/info", "/v1.44/system/df?type=build-cache", "/v1.44/info"}[i%3]
		if path != want || methods[i] != http.MethodGet {
			t.Fatal("cache scope/request changed", paths, methods)
		}
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("cache production dispatch saved Rydd state", err)
	}
}
