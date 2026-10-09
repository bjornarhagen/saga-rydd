package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func metadataReply(t *testing.T, record buildmetadata.Record) []byte {
	t.Helper()
	b, e := json.Marshal(map[string]any{"api_version": 1, "ok": true, "command": "version", "version": record.Version, "build_metadata": record})
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func unknownMetadata() buildmetadata.Record {
	return buildmetadata.Record{Contract: buildmetadata.Contract, Version: "dev", VersionSource: "linked_value", SourceStatus: "unknown", GoVersion: "go1.27.1", GOOS: "darwin", GOARCH: "arm64"}
}
func TestTopologyDeclaredMetadataUnknownAndCurrent(t *testing.T) {
	unknown := unknownMetadata()
	r, e := parseDeclaredMetadata(metadataReply(t, unknown))
	if e != nil || r.Revision != nil || r.SourceStatus != "unknown" {
		t.Fatal(r, e)
	}
	current := buildmetadata.Current()
	r, e = parseDeclaredMetadata(metadataReply(t, current))
	if e != nil || r.Contract != buildmetadata.Contract || r.Version != current.Version {
		t.Fatal(r, e)
	}
	value := strings.Repeat("a", 40)
	known := unknown
	known.Revision = &value
	known.SourceStatus = "dirty"
	r, e = parseDeclaredMetadata(metadataReply(t, known))
	if e != nil || r.Revision == nil || *r.Revision != value {
		t.Fatal(r, e)
	}
}
func TestTopologyDeclaredMetadataStrictBoundedReply(t *testing.T) {
	good := metadataReply(t, unknownMetadata())
	for _, body := range [][]byte{nil, []byte("{"), append(append([]byte(nil), good...), []byte(" {}")...), []byte(strings.Repeat("x", versionReplyLimit+1)), []byte(strings.Replace(string(good), `"api_version":1`, `"api_version":1,"api_version":1`, 1)), []byte(strings.Replace(string(good), `"command":"version"`, `"command":"version","other":true`, 1))} {
		if _, e := parseDeclaredMetadata(body); e == nil {
			t.Fatal("invalid reply accepted")
		}
	}
	for _, change := range []func(*buildmetadata.Record){func(r *buildmetadata.Record) { r.Contract = "other" }, func(r *buildmetadata.Record) { v := strings.Repeat("a", 40); r.Revision = &v }, func(r *buildmetadata.Record) { r.SourceStatus = "clean" }, func(r *buildmetadata.Record) { r.Version = string([]byte{'b', 'a', 'd', 27}) }, func(r *buildmetadata.Record) { r.GOOS = strings.Repeat("a", 33) }} {
		r := unknownMetadata()
		change(&r)
		if _, e := parseDeclaredMetadata(metadataReply(t, r)); e == nil {
			t.Fatal("invalid declaration accepted")
		}
	}
}

func TestTopologyDeclaredMetadataBoundedChildAndReceipts(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "oversized"}[oversized], func(t *testing.T) {
			base := t.TempDir()
			record := unknownMetadata()
			body := string(metadataReply(t, record))
			if oversized {
				body = strings.Repeat("x", versionReplyLimit+1)
			}
			binary := filepath.Join(base, "generated-version")
			// Fixed generated child, shell builtins only; no source/config/provider use.
			script := "#!/bin/sh\nif [ \"$#\" -ne 2 ] || [ \"$1\" != --version ] || [ \"$2\" != --json ]; then exit 9; fi\nprintf '%s\\n' '" + body + "'\n"
			if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			r := &runner{o: options{Binary: binary}, base: base, env: os.Environ(), observers: usage{Available: true}}
			got, err := r.declaredMetadata(context.Background())
			if oversized {
				if !errors.Is(err, errChildOutput) || got.Contract != "" {
					t.Fatal(got, err)
				}
			} else if err != nil || got.Revision != nil || got.SourceStatus != "unknown" {
				t.Fatal(got, err)
			}
			if r.seq != 1 || !r.observers.Available || r.observers.ElapsedNS <= 0 || r.observers.RSS <= 0 {
				t.Fatal("missing separate child cost", r.observers)
			}
			receipt, readErr := os.ReadFile(filepath.Join(base, "command-001.stdout"))
			if readErr != nil || len(receipt) > versionReplyLimit {
				t.Fatal(readErr, len(receipt))
			}
			if _, readErr = os.ReadFile(filepath.Join(base, "command-001.stderr")); readErr != nil {
				t.Fatal(readErr)
			}
		})
	}
}
