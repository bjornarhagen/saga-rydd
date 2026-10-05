// Package regeneration inspects a deliberately narrow npm input format. It
// performs no installation, dependency resolution or filesystem operation.
package regeneration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	ManifestLimit  = 256 << 10
	LockfileLimit  = 2 << 20
	JSONDepthLimit = 64
	JSONValueLimit = 50000
)

type Summary struct {
	LockfileVersion int `json:"lockfile_version"`
	LockedPackages  int `json:"locked_packages"`
}

// Error deliberately contains no source text, dependency name or URL.
type Error struct{ Code, Message string }

func (e Error) Error() string { return e.Message }

func invalid() error {
	return Error{"input_invalid", "The npm inputs contain invalid or ambiguous data."}
}
func unsupported() error {
	return Error{"input_unsupported", "The npm inputs use a feature or dependency source outside the supported inspection contract."}
}
func limited() error {
	return Error{"input_limit", "The npm inputs exceed the bounded inspection limits."}
}

var (
	versionPattern    = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
	namePattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	scopedNamePattern = regexp.MustCompile(`^[a-z0-9._-]+$`)
	specPattern       = regexp.MustCompile(`^[0-9A-Za-z.*+^~<>=| _-]+$`)
	tarballPattern    = regexp.MustCompile(`^[0-9A-Za-z._+-]+\.tgz$`)
)

var dependencyFields = []string{"dependencies", "devDependencies", "optionalDependencies", "peerDependencies"}
var lifecycleFields = []string{"preinstall", "install", "postinstall", "prepublish", "preprepare", "prepare", "postprepare", "dependencies"}

// Analyze compares root dependency declarations and checks locked entries for
// a restricted registry-only npm v2/v3 shape. A success does not validate npm's
// dependency graph or semver resolution, installed contents, download
// availability, external configuration, platform compatibility or regeneration.
func Analyze(manifest, lock []byte) (Summary, error) {
	if len(manifest) > ManifestLimit || len(lock) > LockfileLimit {
		return Summary{}, limited()
	}
	values := 0
	m, err := decodeObject(manifest, &values)
	if err != nil {
		return Summary{}, err
	}
	l, err := decodeObject(lock, &values)
	if err != nil {
		return Summary{}, err
	}
	version, ok := l["lockfileVersion"].(json.Number)
	if !ok {
		return Summary{}, invalid()
	}
	v := 0
	switch version.String() {
	case "2":
		v = 2
	case "3":
		v = 3
	default:
		return Summary{}, unsupported()
	}
	packages, ok := l["packages"].(map[string]any)
	if !ok {
		return Summary{}, invalid()
	}
	root, ok := packages[""].(map[string]any)
	if !ok {
		return Summary{}, invalid()
	}
	for _, field := range []string{"name", "version"} {
		if value, present := l[field]; present {
			text, ok := value.(string)
			if !ok {
				return Summary{}, invalid()
			}
			if rootValue, present := root[field]; present && rootValue != text {
				return Summary{}, invalid()
			}
		}
	}
	if err := validateProject(m); err != nil {
		return Summary{}, err
	}
	if err := validateProject(root); err != nil {
		return Summary{}, err
	}
	for _, field := range dependencyFields {
		a, err := dependencies(m, field)
		if err != nil {
			return Summary{}, err
		}
		b, err := dependencies(root, field)
		if err != nil {
			return Summary{}, err
		}
		if !reflect.DeepEqual(a, b) {
			return Summary{}, invalid()
		}
		for name := range a {
			if _, exists := packages["node_modules/"+name]; !exists && !optionalPeer(m, field, name) {
				return Summary{}, invalid()
			}
		}
	}
	if !reflect.DeepEqual(m["peerDependenciesMeta"], root["peerDependenciesMeta"]) {
		return Summary{}, invalid()
	}
	for _, field := range []string{"name", "version"} {
		if value, present := m[field]; present {
			text, ok := value.(string)
			if !ok || root[field] != text {
				return Summary{}, invalid()
			}
		}
	}
	for path, value := range packages {
		if path == "" {
			continue
		}
		if !packagePath(path) {
			return Summary{}, unsupported()
		}
		entry, ok := value.(map[string]any)
		if !ok {
			return Summary{}, invalid()
		}
		if err := validateProject(entry); err != nil {
			return Summary{}, err
		}
		if name, present := entry["name"]; present {
			if name != packagePathName(path) {
				return Summary{}, unsupported()
			}
		}
		version, ok := entry["version"].(string)
		if !ok {
			return Summary{}, invalid()
		}
		if !concreteVersion(version) {
			return Summary{}, unsupported()
		}
		resolved, ok := entry["resolved"].(string)
		if !ok {
			return Summary{}, invalid()
		}
		if !registryTarball(resolved) {
			return Summary{}, unsupported()
		}
		integrity, ok := entry["integrity"].(string)
		if !ok {
			return Summary{}, invalid()
		}
		if !sha512Integrity(integrity) {
			return Summary{}, unsupported()
		}
	}
	return Summary{LockfileVersion: v, LockedPackages: len(packages) - 1}, nil
}

func decodeObject(input []byte, values *int) (map[string]any, error) {
	if !utf8.Valid(input) {
		return nil, invalid()
	}
	d := json.NewDecoder(bytes.NewReader(input))
	d.UseNumber()
	value, err := decodeValue(d, values, 1)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid()
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, invalid()
	}
	return object, nil
}

func decodeValue(d *json.Decoder, values *int, depth int) (any, error) {
	*values = *values + 1
	if depth > JSONDepthLimit || *values > JSONValueLimit {
		return nil, limited()
	}
	token, err := d.Token()
	if err != nil {
		return nil, invalid()
	}
	delim, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	switch delim {
	case '{':
		object := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, invalid()
			}
			name, ok := key.(string)
			if !ok {
				return nil, invalid()
			}
			if _, duplicate := object[name]; duplicate {
				return nil, invalid()
			}
			value, err := decodeValue(d, values, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, invalid()
		}
		return object, nil
	case '[':
		var array []any
		for d.More() {
			value, err := decodeValue(d, values, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, invalid()
		}
		return array, nil
	default:
		return nil, invalid()
	}
}

func validateProject(object map[string]any) error {
	for _, field := range []string{"name", "version"} {
		if value, present := object[field]; present {
			if _, ok := value.(string); !ok {
				return invalid()
			}
		}
	}
	for _, field := range []string{"dev", "optional", "peer", "devOptional"} {
		if value, present := object[field]; present {
			if _, ok := value.(bool); !ok {
				return invalid()
			}
		}
	}
	for _, field := range []string{"link", "inBundle", "hasInstallScript"} {
		if value, present := object[field]; present {
			flag, ok := value.(bool)
			if !ok {
				return invalid()
			}
			if flag {
				return unsupported()
			}
		}
	}
	for _, field := range []string{"workspaces", "overrides", "resolutions", "bundleDependencies", "bundledDependencies"} {
		if _, present := object[field]; present {
			return unsupported()
		}
	}
	if value, present := object["packageManager"]; present {
		manager, ok := value.(string)
		if !ok {
			return invalid()
		}
		if !strings.HasPrefix(manager, "npm@") || !concreteVersion(strings.TrimPrefix(manager, "npm@")) {
			return unsupported()
		}
	}
	if value, present := object["gypfile"]; present {
		flag, ok := value.(bool)
		if !ok {
			return invalid()
		}
		if flag {
			return unsupported()
		}
	}
	if value, present := object["scripts"]; present {
		scripts, ok := value.(map[string]any)
		if !ok {
			return invalid()
		}
		for _, value := range scripts {
			if _, ok := value.(string); !ok {
				return invalid()
			}
		}
		for _, field := range lifecycleFields {
			if _, present := scripts[field]; present {
				return unsupported()
			}
		}
	}
	for _, field := range dependencyFields {
		if _, err := dependencies(object, field); err != nil {
			return err
		}
	}
	if value, present := object["peerDependenciesMeta"]; present {
		meta, ok := value.(map[string]any)
		if !ok {
			return invalid()
		}
		peers, err := dependencies(object, "peerDependencies")
		if err != nil {
			return err
		}
		for name, value := range meta {
			if _, present := peers[name]; !present {
				return invalid()
			}
			flags, ok := value.(map[string]any)
			if !ok {
				return invalid()
			}
			for key, value := range flags {
				if key != "optional" {
					return unsupported()
				}
				if _, ok := value.(bool); !ok {
					return invalid()
				}
			}
		}
	}
	return nil
}

func dependencies(object map[string]any, field string) (map[string]string, error) {
	result := map[string]string{}
	value, present := object[field]
	if !present {
		return result, nil
	}
	deps, ok := value.(map[string]any)
	if !ok {
		return nil, invalid()
	}
	for name, value := range deps {
		spec, ok := value.(string)
		if !ok {
			return nil, invalid()
		}
		if !packageName(name) || len(spec) > 256 || strings.TrimSpace(spec) == "" || !specPattern.MatchString(spec) {
			return nil, unsupported()
		}
		result[name] = spec
	}
	return result, nil
}

func optionalPeer(object map[string]any, field, name string) bool {
	if field != "peerDependencies" {
		return false
	}
	meta, _ := object["peerDependenciesMeta"].(map[string]any)
	flags, _ := meta[name].(map[string]any)
	return flags["optional"] == true
}

func packageName(name string) bool {
	if len(name) < 1 || len(name) > 214 {
		return false
	}
	parts := strings.Split(name, "/")
	if len(parts) == 1 {
		return namePattern.MatchString(name)
	}
	return len(parts) == 2 && strings.HasPrefix(parts[0], "@") && namePattern.MatchString(parts[0][1:]) && parts[1] != "." && parts[1] != ".." && scopedNamePattern.MatchString(parts[1])
}

func packagePath(path string) bool {
	parts := strings.Split(path, "/")
	for len(parts) != 0 {
		if len(parts) < 2 || parts[0] != "node_modules" {
			return false
		}
		count := 2
		name := parts[1]
		if strings.HasPrefix(name, "@") {
			if len(parts) < 3 {
				return false
			}
			name += "/" + parts[2]
			count = 3
		}
		if !packageName(name) {
			return false
		}
		parts = parts[count:]
	}
	return true
}

func packagePathName(path string) string {
	parts := strings.Split(path, "/")
	last := parts[len(parts)-1]
	if len(parts) >= 2 && strings.HasPrefix(parts[len(parts)-2], "@") {
		return parts[len(parts)-2] + "/" + last
	}
	return last
}

func concreteVersion(version string) bool {
	if len(version) > 256 || !versionPattern.MatchString(version) {
		return false
	}
	plain := strings.SplitN(version, "+", 2)[0]
	parts := strings.SplitN(plain, "-", 2)
	if len(parts) == 2 {
		for _, identifier := range strings.Split(parts[1], ".") {
			numeric := strings.Trim(identifier, "0123456789") == ""
			if numeric && len(identifier) > 1 && identifier[0] == '0' {
				return false
			}
		}
	}
	return true
}

func registryTarball(text string) bool {
	u, err := url.Parse(text)
	if err != nil || u.Scheme != "https" || u.Host != "registry.npmjs.org" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.String() != text {
		return false
	}
	// Do not permit encoded separators or path traversal. Only the registry's
	// normal unscoped/scoped tarball layout is supported.
	if u.RawPath != "" || strings.Contains(u.Path, "\\") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) == 4 && strings.HasPrefix(parts[0], "@") {
		parts = []string{parts[0] + "/" + parts[1], parts[2], parts[3]}
	}
	return len(parts) == 3 && packageName(parts[0]) && parts[1] == "-" && len(parts[2]) > 4 && tarballPattern.MatchString(parts[2])
}

func sha512Integrity(text string) bool {
	if !strings.HasPrefix(text, "sha512-") {
		return false
	}
	encoded := strings.TrimPrefix(text, "sha512-")
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	return err == nil && len(decoded) == 64 && base64.StdEncoding.EncodeToString(decoded) == encoded
}
