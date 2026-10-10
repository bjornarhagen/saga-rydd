package storeexport

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func validTestManifest(t *testing.T) manifest {
	t.Helper()
	m := manifest{Version: 1, Contract: Contract, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), SourcePathBytes: []byte("/generated/plans/plans.sqlite3"), DataFile: "plans.sqlite3", DataBytes: 512, DataSHA256: strings.Repeat("0", 64), Profile: profile{ApplicationID: 0x5259504c, SchemaVersion: 1, PageSize: 512}}
	var err error
	m.ExportID, err = manifestID(m)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestManifestCanonicalIdentityAndAuthorityRemainDistinct(t *testing.T) {
	m := validTestManifest(t)
	body, err := encodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseManifest(body)
	if err != nil || parsed.ExportID != m.ExportID {
		t.Fatal(err)
	}
	for _, kind := range []string{"bytes", "hash", "authority", "source", "page", "identity", "unknown", "duplicate", "trailing", "format"} {
		t.Run(kind, func(t *testing.T) {
			changed := append([]byte(nil), body...)
			switch kind {
			case "bytes":
				changed = bytes.Replace(changed, []byte(`"data_bytes":512`), []byte(`"data_bytes":1024`), 1)
			case "hash":
				changed = bytes.Replace(changed, []byte(strings.Repeat("0", 64)), []byte(strings.Repeat("1", 64)), 1)
			case "authority":
				changed = bytes.Replace(changed, []byte(`"cleanup_authority":false`), []byte(`"cleanup_authority":true`), 1)
			case "source":
				bad := m
				bad.SourcePathBytes = []byte("/generated/other.sqlite3")
				bad.ExportID, _ = manifestID(bad)
				if _, e := encodeManifest(bad); e == nil {
					t.Fatal("nonfixed historical source admitted")
				}
				return
			case "page":
				changed = bytes.Replace(changed, []byte(`"page_size":512`), []byte(`"page_size":513`), 1)
			case "identity":
				changed = bytes.Replace(changed, []byte("plans-export-v1-"), []byte("plans-export-v2-"), 1)
			case "unknown":
				changed = append(append([]byte(nil), body[:len(body)-2]...), []byte(",\"unknown\":false}\n")...)
			case "duplicate":
				changed = append(append([]byte(nil), body[:len(body)-2]...), []byte(",\"version\":1}\n")...)
			case "trailing":
				changed = append(changed, []byte("{}")...)
			case "format":
				changed = append([]byte(" "), changed...)
			}
			if _, e := parseManifest(changed); e == nil {
				t.Fatal("changed manifest admitted")
			}
		})
	}
}

func TestManifestParserBoundsBeforeTypedDecode(t *testing.T) {
	for _, body := range [][]byte{bytes.Repeat([]byte(" "), maxManifestBytes+1), []byte(strings.Repeat("[", maxManifestDepth+1) + "0" + strings.Repeat("]", maxManifestDepth+1)), []byte("[" + strings.Repeat("0,", maxManifestTokens) + "0]")} {
		if tokenBounds(body) == nil {
			t.Fatal("parser cap did not refuse")
		}
	}
	m := validTestManifest(t)
	m.SourceBytesEqual = new(bool)
	m.ExportID, _ = manifestID(m)
	if _, err := encodeManifest(m); err == nil {
		t.Fatal("source byte equality assertion admitted")
	}
}
