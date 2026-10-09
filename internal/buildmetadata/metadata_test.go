package buildmetadata

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"
)

func TestBuildMetadataProjectsOnlyFiniteDeclaredFields(t *testing.T) {
	rev := strings.Repeat("a", 40)
	info := &debug.BuildInfo{GoVersion: "go1.27.1", Path: "/private/generated/canary", Settings: []debug.BuildSetting{
		{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "arm64"}, {Key: "CGO_ENABLED", Value: "0"},
		{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: rev}, {Key: "vcs.modified", Value: "false"},
		{Key: "vcs.time", Value: "private-canary"}, {Key: "-ldflags", Value: "private-canary"},
	}, Deps: []*debug.Module{{Path: "private-canary"}}}
	r := FromBuildInfo(info, "v0.1.0-candidate")
	if r.Contract != Contract || r.Version != "v0.1.0-candidate" || r.SourceStatus != "clean" || r.Revision == nil || *r.Revision != rev || r.CGOEnabled == nil || *r.CGOEnabled || r.GOARCH != "arm64" {
		t.Fatal(r)
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "private") || strings.Contains(string(b), "ldflags") || strings.Contains(string(b), "vcs.time") {
		t.Fatal(string(b))
	}
	info.Settings[5].Value = "true"
	if FromBuildInfo(info, "dev").SourceStatus != "dirty" {
		t.Fatal("dirty declaration lost")
	}
	*r.Revision = "changed"
	if *FromBuildInfo(info, "dev").Revision != rev {
		t.Fatal("returned metadata aliases input")
	}
	info.Settings = append(info.Settings, debug.BuildSetting{Key: "vcs.modified", Value: "false"})
	if duplicate := FromBuildInfo(info, "dev"); duplicate.SourceStatus != "unknown" || duplicate.Revision != nil {
		t.Fatal(duplicate)
	}
}

func TestBuildMetadataUnknownAndVersionBounds(t *testing.T) {
	for _, v := range []string{"", ".", "..", "a/b", "a b", "-x", "a\x00b", "ø", strings.Repeat("x", 65)} {
		if ValidVersion(v) {
			t.Fatalf("accepted %q", v)
		}
	}
	for _, v := range []string{"dev", "v1.2.3-rc.1", "candidate_1", strings.Repeat("x", 64)} {
		if !ValidVersion(v) {
			t.Fatalf("refused %q", v)
		}
	}
	for _, info := range []*debug.BuildInfo{nil, {GoVersion: "private /path\n", Settings: []debug.BuildSetting{{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: strings.Repeat("g", 40)}, {Key: "vcs.modified", Value: "false"}, {Key: "GOOS", Value: "bad/path"}}}, {Settings: make([]debug.BuildSetting, 65)}} {
		r := FromBuildInfo(info, "../bad")
		if r.SourceStatus != "unknown" || r.Revision != nil || r.Version != "dev" || r.GOOS != "unknown" {
			t.Fatal(r)
		}
	}
}
