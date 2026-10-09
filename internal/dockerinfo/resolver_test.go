package dockerinfo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func scriptFixture(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "generated-docker")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDockerResolverFixedArgumentsAndSanitizedEnvironment(t *testing.T) {
	wantArgs := []string{"--context", "default", "context", "inspect", "--format", `{"context_name":{{json .Name}},"endpoint":{{json .Endpoints.docker.Host}}}`, "--", "generated"}
	if !reflect.DeepEqual(resolverArgs("generated"), wantArgs) {
		t.Fatal(resolverArgs("generated"))
	}
	env := resolverEnv([]string{"PATH=/generated/bin", "HOME=/generated/home", "USER=fixture", "LOGNAME=fixture", "TMPDIR=/generated/tmp", "DOCKER_HOST=tcp://remote", "DOCKER_CONTEXT=remote", "DOCKER_API_VERSION=1.1", "DOCKER_CONFIG=/private", "OTEL_SDK_DISABLED=false", "OTEL_EXPORTER_OTLP_ENDPOINT=http://remote", "DOCKER_CLI_OTEL_EXPORTER_OTLP_ENDPOINT=http://remote", "HTTP_PROXY=http://remote", "AWS_SECRET_ACCESS_KEY=private", "SSH_AUTH_SOCK=/private", "TOKEN=private"})
	wantEnv := []string{"PATH=/generated/bin", "HOME=/generated/home", "USER=fixture", "LOGNAME=fixture", "TMPDIR=/generated/tmp", "OTEL_SDK_DISABLED=true"}
	if !reflect.DeepEqual(env, wantEnv) {
		t.Fatalf("env: %v", env)
	}
	// Verify argv at the real child boundary, not only the argument builder.
	argsFile := filepath.Join(t.TempDir(), "argv")
	path := scriptFixture(t, `printf '%s\n' "$@" > "$GENERATED_ARGS"`+"\n"+`printf '%s' '{"context_name":"generated","endpoint":"unix:///tmp/generated-engine.sock"}'`+"\n")
	r, err := resolveCommand(context.Background(), "generated", path, append(env, "GENERATED_ARGS="+argsFile))
	if err != nil || r.ContextName != "generated" {
		t.Fatalf("r=%#v err=%v", r, err)
	}
	b, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(strings.Split(strings.TrimSuffix(string(b), "\n"), "\n"), wantArgs) {
		t.Fatalf("child argv %q", b)
	}
}

func TestDockerResolverStrictOutputAndUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       error
	}{
		{"missing-name", `{"endpoint":"unix:///tmp/generated.sock"}`, ErrResolver},
		{"wrong-name", `{"context_name":"other","endpoint":"unix:///tmp/generated.sock"}`, ErrResolver},
		{"extra-field", `{"context_name":"generated","endpoint":"unix:///tmp/generated.sock","private":"discard"}`, ErrResolver},
		{"duplicate", `{"context_name":"generated","context_name":"generated","endpoint":"unix:///tmp/generated.sock"}`, ErrProtocol},
		{"remote", `{"context_name":"generated","endpoint":"tcp://remote:2375"}`, ErrEndpoint},
		{"array", `[{"context_name":"generated","endpoint":"unix:///tmp/generated.sock"}]`, ErrResolver},
		{"trailing", `{"context_name":"generated","endpoint":"unix:///tmp/generated.sock"} {}`, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// JSON is generated input, passed as an argument-free literal fixture.
			jsonFile := filepath.Join(t.TempDir(), "response.json")
			if err := os.WriteFile(jsonFile, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			path := scriptFixture(t, `/bin/cat "$GENERATED_RESPONSE"`+"\n")
			r, err := resolveCommand(context.Background(), "generated", path, []string{"GENERATED_RESPONSE=" + jsonFile})
			if !errors.Is(err, tc.want) || r != (ResolvedContext{}) {
				t.Fatalf("r=%#v err=%v want=%v", r, err, tc.want)
			}
		})
	}
	r, err := resolveCommand(context.Background(), "generated", filepath.Join(t.TempDir(), "missing"), nil)
	if !errors.Is(err, ErrResolver) || r != (ResolvedContext{}) {
		t.Fatalf("missing r=%#v err=%v", r, err)
	}
	path := scriptFixture(t, "printf 'unrelated private details' >&2\nexit 3\n")
	r, err = resolveCommand(context.Background(), "generated", path, nil)
	if !errors.Is(err, ErrResolver) || strings.Contains(err.Error(), "private details") || r != (ResolvedContext{}) {
		t.Fatalf("failed r=%#v err=%v", r, err)
	}
}

func TestDockerResolverOverflowCancellationKillsAndReapsChild(t *testing.T) {
	for _, kind := range []string{"stdout", "stderr", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "pid")
			body := `printf '%s' "$$" > "$GENERATED_PID"` + "\n"
			if kind == "cancel" {
				body += "/bin/sleep 30\n"
			} else {
				// A real endless child exercises overflow cancellation and reaping.
				body += "while :; do printf '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'"
				if kind == "stderr" {
					body += " >&2"
				}
				body += "; done\n"
			}
			path := scriptFixture(t, body)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan struct{})
			var result ResolvedContext
			var err error
			go func() {
				result, err = resolveCommand(ctx, "generated", path, []string{"GENERATED_PID=" + pidFile})
				close(done)
			}()
			var pid int
			until := time.Now().Add(time.Second)
			for time.Now().Before(until) {
				b, readErr := os.ReadFile(pidFile)
				if readErr == nil {
					pid, _ = strconv.Atoi(string(b))
					if pid > 0 {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			if pid == 0 {
				cancel()
				<-done
				t.Fatal("generated child did not start")
			}
			if kind == "cancel" {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("resolver did not kill/reap")
			}
			want := ErrBounds
			if kind == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || result != (ResolvedContext{}) {
				t.Fatalf("r=%#v err=%v want=%v", result, err, want)
			}
			if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("direct child was not reaped: pid=%d err=%v", pid, err)
			}
		})
	}
}

func TestDockerJSONStrictEscapesAndBoundedProjection(t *testing.T) {
	for _, b := range []string{`{"safe":"\ud83d\ude00"}`, `{"safe":"quote\" and slash\\"}`, `[]`, `{}`} {
		if err := strictJSON([]byte(b)); err != nil {
			t.Fatalf("valid JSON %q: %v", b, err)
		}
	}
	for _, b := range []string{`{"x":"\udc00"}`, `{"x":"\ud800\u0041"}`, `{"x":"\ud800"}`, `{"x":1,"\u0078":2}`, `{"x":true} false`, " ", "{", `{"x":"\u0000"}`} {
		if err := strictJSON([]byte(b)); err == nil {
			t.Fatalf("accepted malformed JSON %q", b)
		}
	}
	if _, err := boundedNames(make([]string, 33)); !errors.Is(err, ErrBounds) {
		t.Fatal(err)
	}
	if _, err := boundedNames([]string{"name\n"}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	b, err := json.Marshal(ResolvedContext{ContextName: "generated", Endpoint: "unix:///tmp/generated.sock"})
	if err != nil || strictJSON(b) != nil {
		t.Fatalf("canonical fixture %q %v", b, err)
	}
}
