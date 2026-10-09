// Package buildmetadata projects finite, non-sensitive build information.
package buildmetadata

import (
	"runtime"
	"runtime/debug"
	"strings"
)

const Contract = "build_metadata_v1"

// Version is the sole candidate label set by the fixed packaging linker recipe.
// Unstamped development builds retain the existing dev version.
var Version = "dev"

type Record struct {
	Contract                string  `json:"contract"`
	Version                 string  `json:"version"`
	VersionSource           string  `json:"version_source"`
	ExecutableLabelVerified bool    `json:"executable_label_verified"`
	Revision                *string `json:"revision"`
	SourceStatus            string  `json:"source_status"`
	GoVersion               string  `json:"go_version"`
	GOOS                    string  `json:"goos"`
	GOARCH                  string  `json:"goarch"`
	CGOEnabled              *bool   `json:"cgo_enabled"`
}

// ValidVersion permits a finite literal label, never a pathname or linker input.
func ValidVersion(value string) bool {
	if len(value) == 0 || len(value) > 64 || value == "." || value == ".." || !alphaNumeric(value[0]) {
		return false
	}
	for i := range value {
		if !alphaNumeric(value[i]) && value[i] != '.' && value[i] != '-' && value[i] != '_' {
			return false
		}
	}
	return true
}

func alphaNumeric(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func word(value string) string {
	if len(value) > 32 || !ValidVersion(value) {
		return "unknown"
	}
	return value
}

func revision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for i := range value {
		if !(value[i] >= '0' && value[i] <= '9' || value[i] >= 'a' && value[i] <= 'f') {
			return false
		}
	}
	return true
}

// FromBuildInfo deliberately omits dependencies, paths, raw flags and VCS time.
// VCS fields are declared build observations, not source authentication.
func FromBuildInfo(info *debug.BuildInfo, version string) Record {
	if !ValidVersion(version) {
		version = "dev"
	}
	r := Record{Contract: Contract, Version: version, VersionSource: "candidate_recipe", SourceStatus: "unknown", GoVersion: "unknown", GOOS: "unknown", GOARCH: "unknown"}
	if info == nil || len(info.Settings) > 64 {
		return r
	}
	r.GoVersion = word(info.GoVersion)
	settings := make(map[string]string, 7)
	duplicate := false
	for _, s := range info.Settings {
		switch s.Key {
		case "GOOS", "GOARCH", "CGO_ENABLED", "vcs", "vcs.revision", "vcs.modified":
			if _, present := settings[s.Key]; present {
				duplicate = true
			}
			settings[s.Key] = s.Value
		}
	}
	if duplicate {
		return r
	}
	r.GOOS, r.GOARCH = word(settings["GOOS"]), word(settings["GOARCH"])
	if value := settings["CGO_ENABLED"]; value == "0" || value == "1" {
		v := value == "1"
		r.CGOEnabled = &v
	}
	if settings["vcs"] == "git" && revision(settings["vcs.revision"]) {
		if value := settings["vcs.modified"]; value == "true" || value == "false" {
			v := strings.Clone(settings["vcs.revision"])
			r.Revision = &v
			r.SourceStatus = "clean"
			if value == "true" {
				r.SourceStatus = "dirty"
			}
		}
	}
	return r
}

func Current() Record {
	info, _ := debug.ReadBuildInfo()
	r := FromBuildInfo(info, Version)
	if ValidVersion(Version) {
		r.VersionSource = "linked_value"
		r.ExecutableLabelVerified = true
	} else {
		r.VersionSource = "unknown"
	}
	r.GOOS, r.GOARCH, r.GoVersion = word(runtime.GOOS), word(runtime.GOARCH), word(runtime.Version())
	return r
}
