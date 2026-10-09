package dockerinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

var expectedPaths = []string{"/v1.44/info", "/v1.44/images/json?all=true", "/v1.44/containers/json?all=true&size=false&limit=129", "/v1.44/info"}

const daemonBody = `{"ID":"generated-daemon-full-id","OSType":"linux","ServerVersion":"25.0.0"}`

var imageBody = `[{"Id":"sha256:` + strings.Repeat("a", 64) + `","RepoTags":["fixture:generated"],"Created":1700000000,"Size":999,"Labels":{"private":"discard"}}]`
var containerBody = `[{"Id":"` + strings.Repeat("b", 64) + `","Names":["/generated"],"Created":1700000001,"State":"exited","SizeRw":999,"Mounts":[{"Source":"private"}],"Command":"discard"}]`

type generatedEngine struct {
	endpoint    string
	connections atomic.Int64
	mu          sync.Mutex
	requests    []string
	methods     []string
}

func engineFixture(t *testing.T, change func(int, http.ResponseWriter, *http.Request) bool) *generatedEngine {
	t.Helper()
	// Keep generated Unix socket paths below the native sockaddr_un limit.
	dir, err := os.MkdirTemp("", "rydd-docker-")
	if err != nil {
		t.Fatal(err)
	}
	// macOS TMPDIR is long; use a short generated path if needed.
	if len(filepath.Join(dir, "engine.sock")) > 90 {
		_ = os.RemoveAll(dir)
		dir, err = os.MkdirTemp("/tmp", "rydd-docker-")
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "engine.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	e := &generatedEngine{endpoint: "unix://" + path}
	server := &http.Server{ReadHeaderTimeout: time.Second, ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			e.connections.Add(1)
		}
	}}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		i := len(e.requests)
		e.requests = append(e.requests, r.URL.RequestURI())
		e.methods = append(e.methods, r.Method)
		e.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("API-Version", "1.44")
		w.Header().Set("OSType", "linux")
		w.Header().Set("Server", "Docker/25.0.0 (linux)")
		if change != nil && change(i, w, r) {
			return
		}
		switch r.URL.Path {
		case "/v1.44/info":
			_, _ = fmt.Fprint(w, daemonBody)
		case "/v1.44/images/json":
			_, _ = fmt.Fprint(w, imageBody)
		case "/v1.44/containers/json":
			_, _ = fmt.Fprint(w, containerBody)
		default:
			http.NotFound(w, r)
		}
	})
	done := make(chan struct{})
	go func() { _ = server.Serve(ln); close(done) }()
	t.Cleanup(func() { _ = server.Close(); <-done; _ = os.RemoveAll(dir) })
	return e
}

func (e *generatedEngine) resolver(_ context.Context, name string) (ResolvedContext, error) {
	return ResolvedContext{ContextName: name, Endpoint: e.endpoint}, nil
}

func (e *generatedEngine) observed() ([]string, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.requests...), append([]string(nil), e.methods...)
}

func noReport(t *testing.T, r Report, err error, want error) {
	t.Helper()
	if err == nil || want != nil && !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	if !reflect.DeepEqual(r, Report{}) {
		t.Fatalf("partial positive report: %#v", r)
	}
}

func TestDockerDiscoverOneHeldConnectionAndSafeProjection(t *testing.T) {
	e := engineFixture(t, nil)
	resolves, dials := 0, 0
	r, err := discover(context.Background(), "generated", func(ctx context.Context, name string) (ResolvedContext, error) {
		resolves++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > OperationLimit {
			t.Error("resolver lacks whole-operation deadline")
		}
		return e.resolver(ctx, name)
	}, func(ctx context.Context, network, path string) (net.Conn, error) {
		dials++
		return (&net.Dialer{}).DialContext(ctx, network, path)
	})
	if err != nil {
		t.Fatal(err)
	}
	requests, methods := e.observed()
	if resolves != 1 || dials != 1 || e.connections.Load() != 1 || !reflect.DeepEqual(requests, expectedPaths) {
		t.Fatalf("scope resolves=%d dials=%d conns=%d requests=%v", resolves, dials, e.connections.Load(), requests)
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatal(method)
		}
	}
	if len(r.Images) != 1 || len(r.Containers) != 1 || r.Images[0].ID != "sha256:"+strings.Repeat("a", 64) || r.Containers[0].ID != strings.Repeat("b", 64) || r.DaemonID != "generated-daemon-full-id" || r.APIVersion != APIVersion || r.Images[0].SizeBytes != nil || r.Containers[0].SizeBytes != nil || r.SavingsBytes != nil {
		t.Fatalf("report: %#v", r)
	}
	if !r.SequentialObservations || r.AtomicSnapshot || r.PhysicalLocalityVerified || r.NamespaceAuthenticated || r.SizesMeasured || r.CurrentStateVerified || r.CleanupApproved || r.Executable || r.Persisted {
		t.Fatalf("authority flags: %#v", r)
	}
	assertProjection := func(view Report) {
		t.Helper()
		b, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"discard", `"Labels":`, `"Mounts":`, `"Command":`, `"Size":`, `"SizeRw":`} {
			if strings.Contains(string(b), secret) {
				t.Fatalf("discarded metadata exposed: %s", b)
			}
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["savings_bytes"]) != "null" {
			t.Fatalf("savings became measured in JSON: %s", b)
		}
		for kind, keys := range map[string][]string{
			"images":     {"id", "tags", "created_at", "size_bytes"},
			"containers": {"id", "names", "created_at", "state", "size_bytes"},
		} {
			var rows []map[string]json.RawMessage
			if err := json.Unmarshal(fields[kind], &rows); err != nil || len(rows) != 1 {
				t.Fatalf("invalid %s projection: %s (%v)", kind, b, err)
			}
			if len(rows[0]) != len(keys) || string(rows[0]["size_bytes"]) != "null" {
				t.Fatalf("extra fields or measured size in %s: %s", kind, b)
			}
			for _, key := range keys {
				if _, ok := rows[0][key]; !ok {
					t.Fatalf("missing projected %s field %q: %s", kind, key, b)
				}
			}
		}
	}
	assertProjection(r)
	// A legitimate endpoint or observation time can contain the discarded
	// source size's digits. Only the projected fields determine size exposure.
	numericOverlap := r
	numericOverlap.Endpoint = "unix:///tmp/rydd-docker-3152999449/engine.sock"
	numericOverlap.StartedAt = time.Date(2026, 10, 9, 9, 9, 9, 999999999, time.UTC)
	numericOverlap.CompletedAt = numericOverlap.StartedAt.Add(time.Second)
	assertProjection(numericOverlap)
}

func TestDockerDiscoverRejectsRemoteOrNoncanonicalBeforeDial(t *testing.T) {
	for _, endpoint := range []string{"tcp://127.0.0.1:2375", "ssh://host", "http://host", "unix://host/tmp/docker.sock", "unix:///tmp/../docker.sock", "unix:////tmp/docker.sock", "unix:///tmp/docker.sock?x=1", "unix:///tmp/docker.sock?", "unix:///tmp/docker.sock#x", "unix:///tmp/%64ocker.sock", "unix:///tmp/%2Fdocker.sock", "unix:///tmp/docker.sock\n", "unix:///tmp/a b.sock", "unix:///tmp/%FF", "unix:///tmp/%E2%80%AE", "unix:///", "unix:///tmp/\xff"} {
		t.Run(fmt.Sprintf("%q", endpoint), func(t *testing.T) {
			dials := 0
			r, err := discover(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
				return ResolvedContext{ContextName: "generated", Endpoint: endpoint}, nil
			}, func(context.Context, string, string) (net.Conn, error) {
				dials++
				return nil, errors.New("must not dial")
			})
			noReport(t, r, err, ErrEndpoint)
			if dials != 0 {
				t.Fatal("dialed refused endpoint")
			}
		})
	}
	for endpoint, want := range map[string]string{"unix:///tmp/a%20b.sock": "/tmp/a b.sock", "unix:///tmp/%C3%B8.sock": "/tmp/ø.sock"} {
		path, err := socketPath(endpoint)
		if err != nil || path != want {
			t.Fatalf("canonical URI %q path=%q err=%v", endpoint, path, err)
		}
	}
	for _, name := range []string{"", "__complete", "--help", "bad/name", "bad name", "bad\nname", strings.Repeat("a", 129)} {
		calls := 0
		r, err := DiscoverWithResolver(context.Background(), name, func(context.Context, string) (ResolvedContext, error) { calls++; return ResolvedContext{}, nil })
		noReport(t, r, err, ErrContext)
		if calls != 0 {
			t.Fatal("invalid name reached resolver")
		}
	}
}

func TestDockerDiscoverFreezesContextAndChecksDaemonIdentity(t *testing.T) {
	t.Run("frozen", func(t *testing.T) {
		var endpoint atomic.Value
		e := engineFixture(t, func(i int, _ http.ResponseWriter, _ *http.Request) bool {
			if i == 0 {
				endpoint.Store("unix:///nonexistent-generated-new-context.sock")
			}
			return false
		})
		endpoint.Store(e.endpoint)
		calls := 0
		r, err := DiscoverWithResolver(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
			calls++
			return ResolvedContext{ContextName: "generated", Endpoint: endpoint.Load().(string)}, nil
		})
		if err != nil || calls != 1 || r.Endpoint != e.endpoint {
			t.Fatalf("r=%#v calls=%d err=%v", r, calls, err)
		}
	})
	t.Run("changed", func(t *testing.T) {
		e := engineFixture(t, func(i int, w http.ResponseWriter, _ *http.Request) bool {
			if i == 3 {
				_, _ = fmt.Fprint(w, strings.Replace(daemonBody, "generated-daemon-full-id", "different-full-daemon-id", 1))
				return true
			}
			return false
		})
		r, err := DiscoverWithResolver(context.Background(), "generated", e.resolver)
		noReport(t, r, err, ErrDaemonChanged)
	})
	t.Run("resolver-name", func(t *testing.T) {
		r, err := DiscoverWithResolver(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
			return ResolvedContext{ContextName: "another", Endpoint: "unix:///tmp/generated.sock"}, nil
		})
		noReport(t, r, err, ErrResolver)
	})
}

func TestDockerDiscoverRefusesProtocolAndMalformedJSON(t *testing.T) {
	cases := []struct {
		name, body string
		headers    map[string]string
		code       int
		want       error
	}{
		{name: "redirect", code: 302, want: ErrProtocol},
		{name: "encoding", headers: map[string]string{"Content-Encoding": "gzip"}, want: ErrProtocol},
		{name: "connection-close", headers: map[string]string{"Connection": "close"}, want: ErrProtocol},
		{name: "old-api", headers: map[string]string{"API-Version": "1.43"}, want: ErrProtocol},
		{name: "windows", headers: map[string]string{"OSType": "windows"}, want: ErrProtocol},
		{name: "wrong-server", headers: map[string]string{"Server": "alternate"}, want: ErrProtocol},
		{name: "wrong-content", headers: map[string]string{"Content-Type": "text/plain"}, want: ErrProtocol},
		{name: "duplicate-key", body: `{"ID":"a","ID":"b","OSType":"linux","ServerVersion":"25.0.0"}`, want: ErrProtocol},
		{name: "ignored-duplicate", body: `{"ID":"a","OSType":"linux","ServerVersion":"25.0.0","ignored":{"x":1,"x":2}}`, want: ErrProtocol},
		{name: "invalid-utf8", body: "{\"ID\":\"\xff\"}", want: ErrProtocol},
		{name: "lone-surrogate", body: `{"ID":"\ud800"}`, want: ErrProtocol},
		{name: "dangerous-text", body: `{"ID":"a","OSType":"linux","ServerVersion":"25.0.0","ignored":"\u001b[31m"}`, want: ErrProtocol},
		{name: "bidi-text", body: `{"ID":"a\u202eb","OSType":"linux","ServerVersion":"25.0.0"}`, want: ErrProtocol},
		{name: "empty-id", body: `{"ID":"","OSType":"linux","ServerVersion":"25.0.0"}`, want: ErrProtocol},
		{name: "depth", body: strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34), want: ErrBounds},
		{name: "long-string", body: `{"ignored":"` + strings.Repeat("x", 4097) + `"}`, want: ErrBounds},
		{name: "trailing-json", body: daemonBody + " {}", want: ErrProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := engineFixture(t, func(i int, w http.ResponseWriter, _ *http.Request) bool {
				if i != 0 {
					return false
				}
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				if tc.code != 0 {
					w.WriteHeader(tc.code)
				}
				body := tc.body
				if body == "" {
					body = daemonBody
				}
				_, _ = fmt.Fprint(w, body)
				return true
			})
			r, err := DiscoverWithResolver(context.Background(), "generated", e.resolver)
			noReport(t, r, err, tc.want)
			requests, _ := e.observed()
			if len(requests) != 1 || e.connections.Load() != 1 {
				t.Fatalf("retried/followed: %v connections=%d", requests, e.connections.Load())
			}
		})
	}
}

func TestDockerDiscoverObjectAndBodyBounds(t *testing.T) {
	for _, kind := range []string{"images", "containers"} {
		t.Run(kind, func(t *testing.T) {
			rows := make([]any, 129)
			for i := range rows {
				if kind == "images" {
					rows[i] = map[string]any{"Id": fmt.Sprintf("sha256:%064x", i), "RepoTags": []string{}, "Created": int64(1700000000)}
				} else {
					rows[i] = map[string]any{"Id": fmt.Sprintf("%064x", i), "Names": []string{}, "Created": int64(1700000000), "State": "exited"}
				}
			}
			body, _ := json.Marshal(rows)
			e := engineFixture(t, func(_ int, w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/"+kind+"/json") {
					_, _ = w.Write(body)
					return true
				}
				return false
			})
			r, err := DiscoverWithResolver(context.Background(), "generated", e.resolver)
			noReport(t, r, err, ErrBounds)
		})
	}
	for _, kind := range []string{"body", "header", "tokens", "names"} {
		t.Run(kind, func(t *testing.T) {
			e := engineFixture(t, func(i int, w http.ResponseWriter, _ *http.Request) bool {
				if i != 0 {
					return false
				}
				switch kind {
				case "body":
					w.Header().Set("Content-Length", fmt.Sprint(MaxBodyBytes+1))
					_, _ = fmt.Fprint(w, "x")
				case "header":
					w.Header().Set("X-Generated", strings.Repeat("x", MaxHeaderBytes+1))
					_, _ = fmt.Fprint(w, daemonBody)
				case "tokens":
					_, _ = fmt.Fprint(w, "["+strings.Repeat("0,", 32768)+"0]")
				case "names":
					_, _ = fmt.Fprint(w, `{"ignored":"`+strings.Repeat("x", 4097)+`"}`)
				}
				return true
			})
			r, err := DiscoverWithResolver(context.Background(), "generated", e.resolver)
			noReport(t, r, err, ErrBounds)
		})
	}
}

func TestDockerDiscoverCancellationClosesConnectionAndNoPartialResult(t *testing.T) {
	for _, stage := range []int{0, 2, 3} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			entered, closed := make(chan struct{}), make(chan struct{})
			e := engineFixture(t, func(i int, _ http.ResponseWriter, r *http.Request) bool {
				if i != stage {
					return false
				}
				close(entered)
				<-r.Context().Done()
				close(closed)
				return true
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var report Report
			var err error
			go func() { report, err = DiscoverWithResolver(ctx, "generated", e.resolver); close(done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("request not reached")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("operation remained blocked")
			}
			noReport(t, report, err, context.Canceled)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("held connection not closed")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	r, err := DiscoverWithResolver(ctx, "generated", func(context.Context, string) (ResolvedContext, error) { calls++; return ResolvedContext{}, nil })
	noReport(t, r, err, context.Canceled)
	if calls != 0 {
		t.Fatal("early cancel resolved")
	}
}

func TestDockerDiscoverNoReconnectAfterConnectionLoss(t *testing.T) {
	e := engineFixture(t, func(i int, w http.ResponseWriter, _ *http.Request) bool {
		if i != 1 {
			return false
		}
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return true
		}
		_ = c.Close()
		return true
	})
	dials := 0
	r, err := discover(context.Background(), "generated", e.resolver, func(ctx context.Context, network, path string) (net.Conn, error) {
		dials++
		return (&net.Dialer{}).DialContext(ctx, network, path)
	})
	noReport(t, r, err, ErrProtocol)
	requests, _ := e.observed()
	if dials != 1 || e.connections.Load() != 1 || len(requests) != 2 {
		t.Fatalf("reconnected: dials=%d requests=%v", dials, requests)
	}
}

func TestDockerDiscoverGenuineEmptyVersusUnavailableSocket(t *testing.T) {
	t.Run("genuine empty", func(t *testing.T) {
		e := engineFixture(t, func(i int, w http.ResponseWriter, _ *http.Request) bool {
			if i == 1 || i == 2 {
				_, _ = fmt.Fprint(w, "[]")
				return true
			}
			return false
		})
		r, err := DiscoverWithResolver(context.Background(), "generated", e.resolver)
		if err != nil {
			t.Fatal(err)
		}
		requests, _ := e.observed()
		if !reflect.DeepEqual(requests, expectedPaths) || e.connections.Load() != 1 || r.DaemonID != "generated-daemon-full-id" || r.Images == nil || r.Containers == nil || len(r.Images) != 0 || len(r.Containers) != 0 {
			t.Fatalf("empty result scope=%v connections=%d report=%#v", requests, e.connections.Load(), r)
		}
		if !r.SequentialObservations || r.AtomicSnapshot || r.PhysicalLocalityVerified || r.NamespaceAuthenticated || r.SizesMeasured || r.CurrentStateVerified || r.CleanupApproved || r.Executable || r.Persisted || r.SavingsBytes != nil {
			t.Fatalf("empty result authority: %#v", r)
		}
	})
	dir, err := os.MkdirTemp("/tmp", "rydd-docker-unavailable-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	stopped := filepath.Join(dir, "stopped.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: stopped, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stopped); err != nil {
		t.Fatalf("stopped fixture name missing: %v", err)
	}
	for _, path := range []string{stopped, filepath.Join(dir, "missing.sock")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			resolves, dials := 0, 0
			r, err := discover(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
				resolves++
				return ResolvedContext{ContextName: "generated", Endpoint: "unix://" + path}, nil
			}, func(ctx context.Context, network, path string) (net.Conn, error) {
				dials++
				return (&net.Dialer{}).DialContext(ctx, network, path)
			})
			noReport(t, r, err, ErrProtocol)
			if resolves != 1 || dials != 1 {
				t.Fatalf("unavailable socket retried resolves=%d dials=%d", resolves, dials)
			}
		})
	}
}
