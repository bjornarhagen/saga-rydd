package regeneration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T) (map[string]any, map[string]any) {
	t.Helper()
	manifest := map[string]any{"name": "project", "version": "1.0.0", "dependencies": map[string]any{"foo": "^1.0.0"}}
	root := map[string]any{"name": "project", "version": "1.0.0", "dependencies": map[string]any{"foo": "^1.0.0"}}
	lock := map[string]any{"lockfileVersion": 3, "packages": map[string]any{"": root, "node_modules/foo": locked("foo", "1.2.3")}}
	return manifest, lock
}

func locked(name, version string) map[string]any {
	base := name
	if i := strings.LastIndex(name, "/"); i >= 0 {
		base = name[i+1:]
	}
	return map[string]any{"version": version, "resolved": "https://registry.npmjs.org/" + name + "/-/" + base + "-" + version + ".tgz", "integrity": "sha512-" + base64.StdEncoding.EncodeToString(make([]byte, 64))}
}

func encode(t *testing.T, object map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func analyzeFixture(t *testing.T, change func(map[string]any, map[string]any)) (Summary, error) {
	t.Helper()
	m, l := fixture(t)
	if change != nil {
		change(m, l)
	}
	return Analyze(encode(t, m), encode(t, l))
}

func TestAnalyzeSupportedInputs(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			summary, err := analyzeFixture(t, func(m, l map[string]any) {
				l["lockfileVersion"] = version
				m["packageManager"] = "npm@11.0.0"
				m["scripts"] = map[string]any{"test": "test command", "build": "build command"}
				m["description"] = "Unknown non-input fields remain allowed."
				p := l["packages"].(map[string]any)
				p["node_modules/@scope/.pkg"] = locked("@scope/.pkg", "2.0.0-beta.1+build.02")
				p["node_modules/foo/node_modules/bar"] = locked("bar", "3.0.0")
			})
			if err != nil || summary.LockfileVersion != version || summary.LockedPackages != 3 {
				t.Fatalf("summary=%+v err=%v", summary, err)
			}
		})
	}
}

func TestAnalyzeLayoutExactSortedPaths(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			manifest, lock := fixture(t)
			lock["lockfileVersion"] = version
			packages := lock["packages"].(map[string]any)
			packages["node_modules/zebra"] = locked("zebra", "2.0.0")
			packages["node_modules/@scope/.pkg"] = locked("@scope/.pkg", "1.0.0")
			packages["node_modules/foo/node_modules/bar"] = locked("bar", "3.0.0")
			packages["node_modules/foo/node_modules/@nested/leaf"] = locked("@nested/leaf", "4.0.0")
			manifestBytes, lockBytes := encode(t, manifest), encode(t, lock)
			want := []string{
				"node_modules/@scope/.pkg",
				"node_modules/foo",
				"node_modules/foo/node_modules/@nested/leaf",
				"node_modules/foo/node_modules/bar",
				"node_modules/zebra",
			}
			for range 8 {
				summary, layout, err := AnalyzeLayout(manifestBytes, lockBytes)
				if err != nil || summary.LockfileVersion != version || summary.LockedPackages != len(want) || !reflect.DeepEqual(layout.PackagePaths, want) {
					t.Fatalf("summary=%+v layout=%+v err=%v", summary, layout, err)
				}
				ordinary, err := Analyze(manifestBytes, lockBytes)
				if err != nil || ordinary != summary {
					t.Fatalf("ordinary=%+v summary=%+v err=%v", ordinary, summary, err)
				}
			}
		})
	}
}

func TestAnalyzeLayoutWithoutLockedPackages(t *testing.T) {
	manifest, lock := fixture(t)
	delete(manifest, "dependencies")
	packages := lock["packages"].(map[string]any)
	delete(packages[""].(map[string]any), "dependencies")
	delete(packages, "node_modules/foo")
	summary, layout, err := AnalyzeLayout(encode(t, manifest), encode(t, lock))
	if err != nil || summary.LockedPackages != 0 || layout.PackagePaths == nil || len(layout.PackagePaths) != 0 {
		t.Fatalf("summary=%+v layout=%+v err=%v", summary, layout, err)
	}
}

func TestAnalyzeLayoutNoPartialResultOnFailure(t *testing.T) {
	manifest, lock := fixture(t)
	manifestBytes, lockBytes := encode(t, manifest), encode(t, lock)
	lock["packages"].(map[string]any)["node_modules/foo/node_modules/../custom"] = locked("custom", "1.0.0")
	cases := []struct {
		name, code string
		manifest   []byte
		lock       []byte
	}{
		{"invalid", "input_invalid", []byte(`{"name":"project","name":"ambiguous"}`), lockBytes},
		{"unsupported", "input_unsupported", manifestBytes, encode(t, lock)},
		{"limited", "input_limit", bytes.Repeat([]byte(" "), ManifestLimit+1), lockBytes},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			summary, layout, err := AnalyzeLayout(test.manifest, test.lock)
			assertCode(t, err, test.code)
			if summary != (Summary{}) || layout.PackagePaths != nil {
				t.Fatalf("partial result: summary=%+v layout=%+v", summary, layout)
			}
			_, ordinaryErr := Analyze(test.manifest, test.lock)
			assertCode(t, ordinaryErr, test.code)
		})
	}
}

func TestAnalyzeDependencyDeclarations(t *testing.T) {
	for _, field := range dependencyFields {
		t.Run(field, func(t *testing.T) {
			_, err := analyzeFixture(t, func(m, l map[string]any) {
				root := l["packages"].(map[string]any)[""].(map[string]any)
				m[field] = map[string]any{"bar": "2.x"}
				root[field] = map[string]any{"bar": "2.x"}
				l["packages"].(map[string]any)["node_modules/bar"] = locked("bar", "2.1.0")
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = analyzeFixture(t, func(m, l map[string]any) { m[field] = map[string]any{"bar": "2.x"} })
			assertCode(t, err, "input_invalid")
		})
	}
	t.Run("optional peer", func(t *testing.T) {
		_, err := analyzeFixture(t, func(m, l map[string]any) {
			root := l["packages"].(map[string]any)[""].(map[string]any)
			for _, object := range []map[string]any{m, root} {
				object["peerDependencies"] = map[string]any{"peer": "^1.0.0"}
				object["peerDependenciesMeta"] = map[string]any{"peer": map[string]any{"optional": true}}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	// Declared ranges are compared literally; this is not a semver solver or
	// npm ci readiness result, even when an invalid resolution is hand-written.
	t.Run("does not solve semver", func(t *testing.T) {
		_, err := analyzeFixture(t, func(m, l map[string]any) { l["packages"].(map[string]any)["node_modules/foo"] = locked("foo", "9.0.0") })
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestAnalyzeRefusesUnsupportedInputs(t *testing.T) {
	cases := map[string]func(map[string]any, map[string]any){
		"old lock":       func(m, l map[string]any) { l["lockfileVersion"] = 1 },
		"future lock":    func(m, l map[string]any) { l["lockfileVersion"] = 4 },
		"workspaces":     func(m, l map[string]any) { m["workspaces"] = []any{} },
		"overrides":      func(m, l map[string]any) { m["overrides"] = map[string]any{} },
		"resolutions":    func(m, l map[string]any) { m["resolutions"] = map[string]any{} },
		"bundled":        func(m, l map[string]any) { m["bundleDependencies"] = []any{"foo"} },
		"manager":        func(m, l map[string]any) { m["packageManager"] = "pnpm@9.0.0" },
		"gyp":            func(m, l map[string]any) { m["gypfile"] = true },
		"workspace path": func(m, l map[string]any) { l["packages"].(map[string]any)["packages/local"] = locked("local", "1.0.0") },
		"dot path": func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/../foo"] = locked("foo", "1.0.0")
		},
		"locked alias": func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)["name"] = "bar"
		},
	}
	for _, field := range lifecycleFields {
		cases["script "+field] = func(m, l map[string]any) { m["scripts"] = map[string]any{field: "a private script"} }
	}
	for _, spec := range []string{"file:../local", "link:../local", "workspace:*", "npm:other@1.0.0", "git+https://example.test/repo.git", "owner/repo", "https://example.test/archive.tgz"} {
		cases["source "+spec] = func(m, l map[string]any) { m["dependencies"].(map[string]any)["foo"] = spec }
	}
	for _, field := range []string{"link", "inBundle", "hasInstallScript"} {
		cases[field] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)[field] = true
		}
		cases["root "+field] = func(m, l map[string]any) {
			l["packages"].(map[string]any)[""].(map[string]any)[field] = true
		}
	}
	for _, version := range []string{"latest", "^1.0.0", "1.0", "01.2.3", "1.2.3-01", "file:../private"} {
		cases["version "+version] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)["version"] = version
		}
	}
	for _, resolved := range []string{"http://registry.npmjs.org/foo/-/foo.tgz", "https://private.test/foo/-/foo.tgz", "https://user:secret@registry.npmjs.org/foo/-/foo.tgz", "https://registry.npmjs.org:443/foo/-/foo.tgz", "https://registry.npmjs.org/foo/-/foo.tgz?private=secret", "https://registry.npmjs.org/foo/-/foo.tgz?", "https://registry.npmjs.org/foo/-/foo.tgz#secret", "https://registry.npmjs.org/foo/-/foo%20bar.tgz", "https://registry.npmjs.org/foo%2fbar/-/foo.tgz", "https://registry.npmjs.org/foo/bar.tgz", "file:../private"} {
		cases["resolved "+resolved] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)["resolved"] = resolved
		}
	}
	for _, integrity := range []string{"sha1-secret", "sha512-", "sha512-" + base64.StdEncoding.EncodeToString(make([]byte, 63)), "sha512-" + base64.StdEncoding.EncodeToString(make([]byte, 64)) + "\n", "sha512-" + base64.StdEncoding.EncodeToString(make([]byte, 64)) + " sha256-private"} {
		cases["integrity "+integrity] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)["integrity"] = integrity
		}
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) { _, err := analyzeFixture(t, change); assertCode(t, err, "input_unsupported") })
	}
}

func TestAnalyzeRefusesInvalidShapes(t *testing.T) {
	cases := map[string]func(map[string]any, map[string]any){
		"lock version string":       func(m, l map[string]any) { l["lockfileVersion"] = "3" },
		"lock name array":           func(m, l map[string]any) { l["name"] = []any{} },
		"lock name differs":         func(m, l map[string]any) { l["name"] = "changed" },
		"missing packages":          func(m, l map[string]any) { delete(l, "packages") },
		"null root":                 func(m, l map[string]any) { l["packages"].(map[string]any)[""] = nil },
		"missing direct dependency": func(m, l map[string]any) { delete(l["packages"].(map[string]any), "node_modules/foo") },
		"name differs":              func(m, l map[string]any) { m["name"] = "changed" },
		"version differs":           func(m, l map[string]any) { m["version"] = "2.0.0" },
		"dependency array":          func(m, l map[string]any) { m["dependencies"] = []any{} },
		"null dependency map":       func(m, l map[string]any) { m["dependencies"] = nil },
		"dependency number":         func(m, l map[string]any) { m["dependencies"].(map[string]any)["foo"] = 42 },
		"script number":             func(m, l map[string]any) { m["scripts"] = map[string]any{"test": 42} },
		"manager number":            func(m, l map[string]any) { m["packageManager"] = 42 },
		"gyp number":                func(m, l map[string]any) { m["gypfile"] = 42 },
		"peer metadata array":       func(m, l map[string]any) { m["peerDependenciesMeta"] = []any{} },
		"peer metadata not declared": func(m, l map[string]any) {
			m["peerDependenciesMeta"] = map[string]any{"unknown": map[string]any{"optional": true}}
		},
		"package array": func(m, l map[string]any) { l["packages"].(map[string]any)["node_modules/foo"] = []any{} },
	}
	for _, field := range []string{"version", "resolved", "integrity"} {
		cases["package "+field] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)[field] = 42
		}
	}
	for _, field := range []string{"link", "inBundle", "hasInstallScript"} {
		cases["flag "+field] = func(m, l map[string]any) {
			l["packages"].(map[string]any)["node_modules/foo"].(map[string]any)[field] = "false"
		}
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) { _, err := analyzeFixture(t, change); assertCode(t, err, "input_invalid") })
	}
}

func TestAnalyzeStrictJSON(t *testing.T) {
	_, lock := fixture(t)
	cases := map[string][]byte{
		"array": []byte(`[]`), "null": []byte(`null`), "trailing": []byte(`{} {}`),
		"syntax": []byte(`{"private":}`), "duplicate": []byte(`{"private":true,"private":false}`),
		"nested duplicate":  []byte(`{"ignored":{"a":1,"a":2}}`),
		"escaped duplicate": []byte(`{"a":1,"\u0061":2}`),
		"invalid utf8":      append([]byte(`{"ignored":"`), append([]byte{0xff}, []byte(`"}`)...)...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) { _, err := Analyze(data, encode(t, lock)); assertCode(t, err, "input_invalid") })
	}
	m, _ := fixture(t)
	for _, data := range [][]byte{[]byte(`{"lockfileVersion":3,"packages":{"":{},"":{}}}`), []byte(`{"lockfileVersion":3,"packages":{}}`)} {
		_, err := Analyze(encode(t, m), data)
		assertCode(t, err, "input_invalid")
	}
}

func TestAnalyzeBounds(t *testing.T) {
	m, l := fixture(t)
	manifest, lock := encode(t, m), encode(t, l)
	_, err := Analyze(bytes.Repeat([]byte(" "), ManifestLimit+1), lock)
	assertCode(t, err, "input_limit")
	_, err = Analyze(manifest, bytes.Repeat([]byte(" "), LockfileLimit+1))
	assertCode(t, err, "input_limit")
	deep := []byte(`{"ignored":` + strings.Repeat("[", JSONDepthLimit) + "0" + strings.Repeat("]", JSONDepthLimit) + "}")
	_, err = Analyze(deep, lock)
	assertCode(t, err, "input_limit")
	wide := []byte(`{"ignored":[` + strings.Repeat("0,", JSONValueLimit) + "0]}")
	_, err = Analyze(wide, lock)
	assertCode(t, err, "input_limit")
	// Byte caps include whitespace and both documents share the value budget.
	_, err = Analyze(append(manifest, bytes.Repeat([]byte(" "), ManifestLimit-len(manifest))...), append(lock, bytes.Repeat([]byte(" "), LockfileLimit-len(lock))...))
	if err != nil {
		t.Fatal(err)
	}
	nested := append(append([]byte{}, manifest[:len(manifest)-1]...), []byte(`,"ignored":`+strings.Repeat("[", JSONDepthLimit-2)+"0"+strings.Repeat("]", JSONDepthLimit-2)+"}")...)
	_, err = Analyze(nested, lock)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []map[string]any{m, l} {
		object["ignored"] = make([]any, JSONValueLimit/2)
	}
	_, err = Analyze(encode(t, m), encode(t, l))
	assertCode(t, err, "input_limit")
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var typed Error
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("error=%v want=%s", err, code)
	}
	for _, private := range []string{"private", "secret", "example.test", "registry.npmjs.org", "../", "node_modules"} {
		if strings.Contains(err.Error(), private) {
			t.Fatalf("error exposes source material: %q", err)
		}
	}
}
