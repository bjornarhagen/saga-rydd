package dockerinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

var expectedCachePaths = []string{"/v1.44/info", "/v1.44/system/df?type=build-cache", "/v1.44/info"}

const cacheBody = `{"BuildCache":[{"ID":"ndlpt0hhvkqcdfkputsk4cq9c","Type":"regular","InUse":false,"Shared":true,"CreatedAt":"2021-06-28T13:31:01.474619385Z","LastUsedAt":"2021-07-07T22:02:32.738075951Z","UsageCount":26,"Size":987654321,"Description":"private generated build command","Parents":["discarded-parent"],"Labels":{"private":"discarded-label"}},{"ID":"generated:opaque_ID-2","Type":"exec.cachemount","InUse":null,"Shared":null,"CreatedAt":null,"LastUsedAt":null,"UsageCount":null}],"Images":[{"private":"discarded-image"}],"Containers":[{"private":"discarded-container"}],"Volumes":[{"Mountpoint":"/private/generated-volume"}]}`

func cacheEngineFixture(t *testing.T, body string, change func(int, http.ResponseWriter, *http.Request) bool) *generatedEngine {
	t.Helper()
	return engineFixture(t, func(i int, w http.ResponseWriter, r *http.Request) bool {
		if change != nil && change(i, w, r) {
			return true
		}
		if r.URL.Path == "/v1.44/system/df" {
			_, _ = fmt.Fprint(w, body)
			return true
		}
		return false
	})
}

func noCacheReport(t *testing.T, r CacheReport, err, want error) {
	t.Helper()
	if err == nil || want != nil && !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	if !reflect.DeepEqual(r, CacheReport{}) {
		t.Fatalf("partial cache report: %#v", r)
	}
}

func cacheReportLimits(t *testing.T, r CacheReport) {
	t.Helper()
	if r.Scope != CacheScope || !r.CacheChecked || !r.SequentialObservations || !r.DaemonAccountingMayChange || r.BuilderPinned || r.ImagesChecked || r.ContainersChecked || r.VolumesChecked || r.AtomicSnapshot || r.PhysicalLocalityVerified || r.NamespaceAuthenticated || r.SizesMeasured || r.CurrentStateVerified || r.CleanupApproved || r.Executable || r.Persisted || r.SavingsBytes != nil {
		t.Fatalf("unqualified cache report: %#v", r)
	}
	for _, record := range r.Records {
		if record.SizeBytes != nil {
			t.Fatal("daemon size escaped projection", record)
		}
	}
}

func TestDockerCacheDiscoverOneHeldConnectionSelectorAndSafeProjection(t *testing.T) {
	e := cacheEngineFixture(t, cacheBody, nil)
	resolves, dials := 0, 0
	r, err := discoverCache(context.Background(), "generated", func(ctx context.Context, name string) (ResolvedContext, error) {
		resolves++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > OperationLimit {
			t.Error("resolver lacks shared operation deadline")
		}
		return e.resolver(ctx, name)
	}, func(ctx context.Context, network, path string) (net.Conn, error) {
		dials++
		if network != "unix" || path != strings.TrimPrefix(e.endpoint, "unix://") {
			t.Errorf("unfrozen dial %q %q", network, path)
		}
		return (&net.Dialer{}).DialContext(ctx, network, path)
	})
	if err != nil {
		t.Fatal(err)
	}
	requests, methods := e.observed()
	if resolves != 1 || dials != 1 || e.connections.Load() != 1 || !reflect.DeepEqual(requests, expectedCachePaths) {
		t.Fatalf("scope resolves=%d dials=%d connections=%d requests=%v", resolves, dials, e.connections.Load(), requests)
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatal(method)
		}
	}
	if r.ContextName != "generated" || r.Endpoint != e.endpoint || r.DaemonID != "generated-daemon-full-id" || r.ServerVersion != "25.0.0" || r.APIVersion != APIVersion || r.StartedAt.IsZero() || r.CompletedAt.Before(r.StartedAt) || len(r.Records) != 2 {
		t.Fatal(r)
	}
	first := r.Records[0]
	wantCreated := time.Date(2021, 6, 28, 13, 31, 1, 474619385, time.UTC)
	wantUsed := time.Date(2021, 7, 7, 22, 2, 32, 738075951, time.UTC)
	if first.ID != "ndlpt0hhvkqcdfkputsk4cq9c" || first.Type != "regular" || first.InUse == nil || *first.InUse || first.Shared == nil || !*first.Shared || first.UsageCount == nil || *first.UsageCount != 26 || first.CreatedAt == nil || !first.CreatedAt.Equal(wantCreated) || first.LastUsedAt == nil || !first.LastUsedAt.Equal(wantUsed) {
		t.Fatal(first)
	}
	second := r.Records[1]
	if second.ID != "generated:opaque_ID-2" || second.Type != "exec.cachemount" || second.InUse != nil || second.Shared != nil || second.CreatedAt != nil || second.LastUsedAt != nil || second.UsageCount != nil {
		t.Fatal("unknown observations became known", second)
	}
	cacheReportLimits(t, r)
	assertProjection := func(view CacheReport) {
		t.Helper()
		b, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"private generated build command", "discarded-", "/private/generated-volume", `"Description":`, `"Parents":`, `"Labels":`, `"Mountpoint":`, `"Size":`} {
			if strings.Contains(string(b), secret) {
				t.Fatalf("discarded field exposed: %s", b)
			}
		}
		var projection map[string]json.RawMessage
		if err := json.Unmarshal(b, &projection); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"images", "containers", "volumes"} {
			if _, ok := projection[name]; ok {
				t.Fatal("unobserved scope exposed", name)
			}
		}
		if string(projection["savings_bytes"]) != "null" {
			t.Fatalf("savings became measured in JSON: %s", b)
		}
		var records []map[string]json.RawMessage
		if err := json.Unmarshal(projection["records"], &records); err != nil || len(records) != 2 {
			t.Fatalf("invalid record projection: %s (%v)", b, err)
		}
		keys := []string{"id", "type", "in_use", "shared", "created_at", "last_used_at", "usage_count", "size_bytes"}
		for _, record := range records {
			if len(record) != len(keys) || string(record["size_bytes"]) != "null" {
				t.Fatalf("extra fields or reported size in cache record: %s", b)
			}
			for _, key := range keys {
				if _, ok := record[key]; !ok {
					t.Fatalf("missing projected record field %q: %s", key, b)
				}
			}
		}
	}
	assertProjection(r)
	// Source size digits may legitimately occur in another projected field.
	// They must not be confused with reported cache sizes.
	numericOverlap := r
	numericOverlap.Endpoint = "unix:///tmp/rydd-cache-987654321/engine.sock"
	numericOverlap.StartedAt = time.Date(2026, 10, 9, 9, 9, 9, 987654321, time.UTC)
	numericOverlap.CompletedAt = numericOverlap.StartedAt.Add(time.Second)
	assertProjection(numericOverlap)
}

func TestDockerCacheDiscoverEmptyRequiresExplicitArrayAndCompleteIdentityChecks(t *testing.T) {
	for _, body := range []string{`{"BuildCache":[]}`, `{"BuildCache":[],"Images":null,"Containers":null,"Volumes":null}`} {
		e := cacheEngineFixture(t, body, nil)
		r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
		if err != nil || r.Records == nil || len(r.Records) != 0 {
			t.Fatal(r, err)
		}
		cacheReportLimits(t, r)
		paths, _ := e.observed()
		if !reflect.DeepEqual(paths, expectedCachePaths) || e.connections.Load() != 1 {
			t.Fatal(paths)
		}
	}
	for _, body := range []string{`{}`, `{"BuildCache":null}`, `{"buildcache":[]}`, `{"BuildCache":{}}`, `{"BuildCache":"[]"}`, `null`, `[]`, `{"BuildCache":[null]}`} {
		t.Run(body, func(t *testing.T) {
			e := cacheEngineFixture(t, body, nil)
			r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
			noCacheReport(t, r, err, ErrProtocol)
			paths, _ := e.observed()
			if len(paths) != 2 || e.connections.Load() != 1 {
				t.Fatal("continued/retried unavailable cache", paths)
			}
		})
	}
}

func TestDockerCacheDiscoverOptionalUnknownAndKnownZeroStayDistinct(t *testing.T) {
	body := `{"BuildCache":[{"ID":"unknown","Type":"source.local"},{"ID":"zero","Type":"frontend","InUse":false,"Shared":false,"CreatedAt":"0001-01-01T00:00:00Z","LastUsedAt":null,"UsageCount":0},{"ID":"offset","Type":"internal","CreatedAt":"2020-01-02T03:04:05.000000001+02:30","LastUsedAt":"2020-01-02T03:04:05.123456789+02:30"}]} `
	e := cacheEngineFixture(t, body, nil)
	r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
	if err != nil || len(r.Records) != 3 {
		t.Fatal(r, err)
	}
	a, b, c := r.Records[0], r.Records[1], r.Records[2]
	if a.InUse != nil || a.Shared != nil || a.CreatedAt != nil || a.LastUsedAt != nil || a.UsageCount != nil {
		t.Fatal(a)
	}
	if b.InUse == nil || *b.InUse || b.Shared == nil || *b.Shared || b.UsageCount == nil || *b.UsageCount != 0 || b.CreatedAt != nil || b.LastUsedAt != nil {
		t.Fatal(b)
	}
	if c.CreatedAt == nil || !c.CreatedAt.Equal(time.Date(2020, 1, 2, 0, 34, 5, 1, time.UTC)) || c.LastUsedAt == nil || !c.LastUsedAt.Equal(time.Date(2020, 1, 2, 0, 34, 5, 123456789, time.UTC)) {
		t.Fatal(c)
	}
	cacheReportLimits(t, r)
	for _, kind := range []string{"internal", "frontend", "source.local", "source.git.checkout", "exec.cachemount", "regular"} {
		e := cacheEngineFixture(t, `{"BuildCache":[{"ID":"type-`+kind+`","Type":"`+kind+`"}]}`, nil)
		if r, e := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver); e != nil || len(r.Records) != 1 || r.Records[0].Type != kind {
			t.Fatal(kind, r, e)
		}
	}
}

func TestDockerCacheDiscoverRejectsAmbiguousRecordFieldsAndDates(t *testing.T) {
	for name, fields := range map[string]string{
		"missing_id": `"Type":"regular"`, "null_id": `"ID":null,"Type":"regular"`, "wrong_id_case": `"id":"a","Type":"regular"`,
		"empty_id": `"ID":"","Type":"regular"`, "space_id": `"ID":"two words","Type":"regular"`, "path_id": `"ID":"/private/path","Type":"regular"`,
		"unicode_id": `"ID":"雪","Type":"regular"`, "missing_type": `"ID":"a"`, "null_type": `"ID":"a","Type":null`,
		"unknown_type": `"ID":"a","Type":"custom"`, "wrong_type_case": `"ID":"a","type":"regular"`,
		"bool_string": `"ID":"a","Type":"regular","InUse":"false"`, "bool_number": `"ID":"a","Type":"regular","Shared":0`,
		"negative_usage": `"ID":"a","Type":"regular","UsageCount":-1`, "fraction_usage": `"ID":"a","Type":"regular","UsageCount":1.5`,
		"overflow_usage": `"ID":"a","Type":"regular","UsageCount":9223372036854775808`,
		"date_number":    `"ID":"a","Type":"regular","CreatedAt":0`, "empty_date": `"ID":"a","Type":"regular","CreatedAt":""`,
		"bad_date":          `"ID":"a","Type":"regular","LastUsedAt":"2021-02-30T00:00:00Z"`,
		"timezone":          `"ID":"a","Type":"regular","CreatedAt":"2021-01-01T00:00:00+24:00"`,
		"minute_zone":       `"ID":"a","Type":"regular","CreatedAt":"2021-01-01T00:00:00+01:60"`,
		"fraction_comma":    `"ID":"a","Type":"regular","CreatedAt":"2021-01-01T00:00:00,5Z"`,
		"fraction_overflow": `"ID":"a","Type":"regular","CreatedAt":"2021-01-01T00:00:00.1234567890Z"`,
		"duplicate_id_key":  `"ID":"a","ID":"b","Type":"regular"`, "ignored_duplicate": `"ID":"a","Type":"regular","Labels":{"x":1,"x":2}`,
		"ignored_unsafe": `"ID":"a","Type":"regular","Description":"\u001b[31m"`,
	} {
		t.Run(name, func(t *testing.T) {
			e := cacheEngineFixture(t, `{"BuildCache":[{`+fields+`}]}`, nil)
			r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
			noCacheReport(t, r, err, ErrProtocol)
		})
	}
	for _, body := range []string{`{"BuildCache":[],"BuildCache":[]}`, `{"BuildCache":[{"ID":"a","Type":"regular"},{"ID":"a","Type":"regular"}]}`, `{"BuildCache":[{"ID":"a","Type":"regular"},{}]}`, `{"BuildCache":[]} {}`, "{\"BuildCache\":[{\"ID\":\"\xff\",\"Type\":\"regular\"}]}"} {
		e := cacheEngineFixture(t, body, nil)
		r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
		noCacheReport(t, r, err, ErrProtocol)
	}
}

func TestDockerCacheDiscoverBoundsApplyBeforePublishingAnyRecord(t *testing.T) {
	for _, count := range []int{128, 129} {
		rows := make([]map[string]any, count)
		for i := range rows {
			rows[i] = map[string]any{"ID": fmt.Sprintf("opaque-%d", i), "Type": "regular"}
		}
		body, err := json.Marshal(map[string]any{"BuildCache": rows})
		if err != nil {
			t.Fatal(err)
		}
		e := cacheEngineFixture(t, string(body), nil)
		r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
		if count > MaxObjects {
			noCacheReport(t, r, err, ErrBounds)
		} else if err != nil || len(r.Records) != 128 {
			t.Fatal(r, err)
		} else {
			cacheReportLimits(t, r)
		}
	}
	for _, kind := range []string{"id", "header", "body", "string", "tokens", "depth"} {
		t.Run(kind, func(t *testing.T) {
			e := cacheEngineFixture(t, cacheBody, func(i int, w http.ResponseWriter, _ *http.Request) bool {
				if i != 1 {
					return false
				}
				switch kind {
				case "id":
					_, _ = fmt.Fprint(w, `{"BuildCache":[{"ID":"`+strings.Repeat("a", 129)+`","Type":"regular"}]}`)
				case "header":
					w.Header().Set("X-Generated", strings.Repeat("x", MaxHeaderBytes+1))
					_, _ = fmt.Fprint(w, cacheBody)
				case "body":
					w.Header().Set("Content-Length", fmt.Sprint(MaxBodyBytes+1))
					_, _ = fmt.Fprint(w, "x")
				case "string":
					_, _ = fmt.Fprint(w, `{"BuildCache":[],"Description":"`+strings.Repeat("x", 4097)+`"}`)
				case "tokens":
					_, _ = fmt.Fprint(w, `{"BuildCache":[],"ignored":[`+strings.Repeat("0,", 32768)+`0]}`)
				case "depth":
					_, _ = fmt.Fprint(w, `{"BuildCache":[],"ignored":`+strings.Repeat("[", 34)+"0"+strings.Repeat("]", 34)+`}`)
				}
				return true
			})
			r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
			noCacheReport(t, r, err, ErrBounds)
		})
	}
}

func TestDockerCacheDiscoverContextEndpointAndDaemonRefusals(t *testing.T) {
	for _, name := range []string{"", "--help", "__complete", "with space", strings.Repeat("a", 129)} {
		calls := 0
		r, err := DiscoverCacheWithResolver(context.Background(), name, func(context.Context, string) (ResolvedContext, error) { calls++; return ResolvedContext{}, nil })
		noCacheReport(t, r, err, ErrContext)
		if calls != 0 {
			t.Fatal("invalid name reached resolver")
		}
	}
	for _, endpoint := range []string{"tcp://127.0.0.1:2375", "ssh://host", "unix://host/tmp/cache.sock", "unix:///tmp/../cache.sock", "unix:///tmp/cache.sock?type=build-cache", "unix:///tmp/%63ache.sock"} {
		dials := 0
		r, err := discoverCache(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
			return ResolvedContext{ContextName: "generated", Endpoint: endpoint}, nil
		}, func(context.Context, string, string) (net.Conn, error) { dials++; return nil, errors.New("unexpected") })
		noCacheReport(t, r, err, ErrEndpoint)
		if dials != 0 {
			t.Fatal("unsupported endpoint dialed")
		}
	}
	t.Run("resolved_name_changed", func(t *testing.T) {
		dials := 0
		r, err := discoverCache(context.Background(), "generated", func(context.Context, string) (ResolvedContext, error) {
			return ResolvedContext{ContextName: "other", Endpoint: "unix:///tmp/unopened.sock"}, nil
		}, func(context.Context, string, string) (net.Conn, error) { dials++; return nil, errors.New("unexpected") })
		noCacheReport(t, r, err, ErrResolver)
		if dials != 0 {
			t.Fatal("changed context dialed")
		}
	})
	for _, kind := range []string{"identity", "version", "header", "redirect", "close", "encoding", "wrong_api"} {
		t.Run(kind, func(t *testing.T) {
			e := cacheEngineFixture(t, cacheBody, func(i int, w http.ResponseWriter, _ *http.Request) bool {
				if kind == "identity" && i == 2 {
					_, _ = fmt.Fprint(w, `{"ID":"changed","OSType":"linux","ServerVersion":"25.0.0"}`)
					return true
				}
				if kind == "version" && i == 2 {
					w.Header().Set("Server", "Docker/25.0.1 (linux)")
					_, _ = fmt.Fprint(w, `{"ID":"generated-daemon-full-id","OSType":"linux","ServerVersion":"25.0.1"}`)
					return true
				}
				if i != 1 {
					return false
				}
				switch kind {
				case "header":
					w.Header().Set("API-Version", "1.45")
				case "redirect":
					w.Header().Set("Location", "http://unvisited.invalid/")
					w.WriteHeader(302)
				case "close":
					w.Header().Set("Connection", "close")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "wrong_api":
					w.Header().Set("API-Version", "1.43")
				default:
					return false
				}
				_, _ = fmt.Fprint(w, cacheBody)
				return true
			})
			r, err := DiscoverCacheWithResolver(context.Background(), "generated", e.resolver)
			want := ErrProtocol
			if kind == "identity" || kind == "version" || kind == "header" {
				want = ErrDaemonChanged
			}
			noCacheReport(t, r, err, want)
			paths, _ := e.observed()
			if e.connections.Load() != 1 || len(paths) > 3 {
				t.Fatal("redirect/reconnect", paths)
			}
		})
	}
}

func TestDockerCacheDiscoverCancellationAndNoReconnect(t *testing.T) {
	for _, stage := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			entered, closed := make(chan struct{}), make(chan struct{})
			e := cacheEngineFixture(t, cacheBody, func(i int, _ http.ResponseWriter, r *http.Request) bool {
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
			var r CacheReport
			var err error
			go func() { r, err = DiscoverCacheWithResolver(ctx, "generated", e.resolver); close(done) }()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("request not reached")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("operation remained blocked")
			}
			noCacheReport(t, r, err, context.Canceled)
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("connection remained open")
			}
			paths, _ := e.observed()
			if len(paths) != stage+1 || e.connections.Load() != 1 {
				t.Fatal(paths)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	r, err := DiscoverCacheWithResolver(ctx, "generated", func(context.Context, string) (ResolvedContext, error) { calls++; return ResolvedContext{}, nil })
	noCacheReport(t, r, err, context.Canceled)
	if calls != 0 {
		t.Fatal("early cancellation resolved")
	}
	e := cacheEngineFixture(t, cacheBody, func(i int, w http.ResponseWriter, _ *http.Request) bool {
		if i != 1 {
			return false
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return true
		}
		_ = conn.Close()
		return true
	})
	dials := 0
	r, err = discoverCache(context.Background(), "generated", e.resolver, func(ctx context.Context, network, path string) (net.Conn, error) {
		dials++
		return (&net.Dialer{}).DialContext(ctx, network, path)
	})
	noCacheReport(t, r, err, ErrProtocol)
	paths, _ := e.observed()
	if dials != 1 || e.connections.Load() != 1 || len(paths) != 2 {
		t.Fatal("connection retried", dials, paths)
	}
}
