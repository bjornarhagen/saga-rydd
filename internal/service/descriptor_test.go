package service

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
)

func unifiedSpec(platform string) Spec {
	spec := Spec{
		GOOS: platform, Executable: "/unavailable tools/<bin & Ω>/rydd$TEST%\"'\\end",
		Paths:      config.Paths{ConfigFile: "/unavailable state/$STATE%\"'\\name/config.toml", StateDir: "/unavailable state/$STATE%\"'\\name"},
		RuntimeDir: "/unavailable runtime/$RUNTIME%\"'\\name",
	}
	if platform == "linux" {
		spec.Executable = "/unavailable tools/<bin & Ω>/rydd$TEST%end"
	}
	return spec
}

type plistNode struct {
	XMLName  xml.Name
	Version  string      `xml:"version,attr"`
	Text     string      `xml:",chardata"`
	Children []plistNode `xml:",any"`
}

func plistDictionary(t *testing.T, node plistNode) map[string]plistNode {
	t.Helper()
	if node.XMLName.Local != "dict" || len(node.Children)%2 != 0 {
		t.Fatal("invalid plist dictionary", node)
	}
	values := make(map[string]plistNode, len(node.Children)/2)
	for index := 0; index < len(node.Children); index += 2 {
		key := node.Children[index]
		if key.XMLName.Local != "key" || key.Text == "" {
			t.Fatal("invalid plist key", key)
		}
		if _, exists := values[key.Text]; exists {
			t.Fatal("duplicate plist key", key.Text)
		}
		values[key.Text] = node.Children[index+1]
	}
	return values
}

func TestServiceLaunchdLiteralIdleDescriptor(t *testing.T) {
	spec := unifiedSpec("darwin")
	descriptor, err := Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := []string{spec.Executable, "--data-dir", spec.Paths.StateDir, "daemon"}
	wantEnv := map[string]string{"RYDD_RUNTIME_DIR": spec.RuntimeDir}
	assertDescriptorScope(t, descriptor, spec, wantArgs, wantEnv)
	if descriptor.ManagerProfile != "launchd_background_xml_v1" || descriptor.Filename != "io.github.bjornarhagen.saga-rydd.plist" {
		t.Fatal("unexpected launchd profile", descriptor)
	}
	var plist plistNode
	if err := xml.Unmarshal([]byte(descriptor.Content), &plist); err != nil {
		t.Fatal("rendered descriptor is not XML", err)
	}
	if plist.XMLName.Local != "plist" || plist.Version != "1.0" || len(plist.Children) != 1 {
		t.Fatal("invalid plist envelope", plist)
	}
	values := plistDictionary(t, plist.Children[0])
	if len(values) != 8 || values["Label"].Text != descriptor.Label || values["ProcessType"].Text != "Background" || values["LowPriorityIO"].XMLName.Local != "true" || values["ExitTimeOut"].XMLName.Local != "integer" || values["ExitTimeOut"].Text != "10" || values["ThrottleInterval"].Text != "30" {
		t.Fatal("launchd background/lifecycle contract differs", values)
	}
	arguments := values["ProgramArguments"]
	if arguments.XMLName.Local != "array" {
		t.Fatal("program arguments are not a literal array", arguments)
	}
	var decodedArgs []string
	for _, argument := range arguments.Children {
		if argument.XMLName.Local != "string" {
			t.Fatal("non-string launchd argument", argument)
		}
		decodedArgs = append(decodedArgs, argument.Text)
	}
	if !reflect.DeepEqual(decodedArgs, wantArgs) {
		t.Fatal("XML changed literal argument bytes", decodedArgs, wantArgs)
	}
	decodedEnv := make(map[string]string)
	for key, value := range plistDictionary(t, values["EnvironmentVariables"]) {
		if value.XMLName.Local != "string" {
			t.Fatal("non-string launchd environment", value)
		}
		decodedEnv[key] = value.Text
	}
	if !reflect.DeepEqual(decodedEnv, wantEnv) {
		t.Fatal("XML changed the frozen runtime", decodedEnv)
	}
	keepAlive := plistDictionary(t, values["KeepAlive"])
	if len(keepAlive) != 1 || keepAlive["SuccessfulExit"].XMLName.Local != "false" {
		t.Fatal("normal exit would be unconditionally restarted", keepAlive)
	}
	for _, forbidden := range []string{"WatchPaths", "StartInterval", "StartCalendarInterval", "EnableGlobbing", "Program", "UserName"} {
		if _, exists := values[forbidden]; exists {
			t.Fatal("descriptor gained activation, expansion or privileged scope", forbidden)
		}
	}
}

// This independent decoder checks the rendered C-quoted subset, followed by
// literal %% specifier resolution. It never uses the production encoder.
func systemdQuotedItems(t *testing.T, value string) []string {
	t.Helper()
	var items []string
	for value != "" {
		if value[0] != '"' {
			t.Fatal("systemd item is not wholly quoted", value)
		}
		end := 1
		for end < len(value) {
			if value[end] == '\\' {
				end += 2
				continue
			}
			if value[end] == '"' {
				break
			}
			end++
		}
		if end >= len(value) {
			t.Fatal("unterminated systemd item", value)
		}
		decoded, err := strconv.Unquote(value[:end+1])
		if err != nil {
			t.Fatal("invalid C-quoted systemd item", err)
		}
		var literal strings.Builder
		for index := 0; index < len(decoded); index++ {
			if decoded[index] == '%' {
				if index+1 >= len(decoded) || decoded[index+1] != '%' {
					t.Fatal("descriptor retained a live specifier", decoded)
				}
				index++
			}
			literal.WriteByte(decoded[index])
		}
		items = append(items, literal.String())
		value = value[end+1:]
		if value != "" {
			if value[0] != ' ' {
				t.Fatal("items lack a separator", value)
			}
			value = strings.TrimLeft(value, " ")
		}
	}
	return items
}

func TestServiceSystemdLiteralIdleDescriptors(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "unified", true: "standard split"}[split], func(t *testing.T) {
			spec := unifiedSpec("linux")
			wantArgs := []string{spec.Executable, "--data-dir", spec.Paths.StateDir, "daemon"}
			wantEnv := map[string]string{"RYDD_RUNTIME_DIR": spec.RuntimeDir}
			if split {
				spec.Paths = config.Paths{ConfigFile: "/unavailable config/$CFG%\"'\\name/saga-rydd/config.toml", StateDir: "/unavailable state/$STATE%\"'\\name/saga-rydd"}
				wantArgs = []string{spec.Executable, "daemon"}
				wantEnv["XDG_CONFIG_HOME"] = "/unavailable config/$CFG%\"'\\name"
				wantEnv["XDG_STATE_HOME"] = "/unavailable state/$STATE%\"'\\name"
			}
			descriptor, err := Build(spec)
			if err != nil {
				t.Fatal(err)
			}
			assertDescriptorScope(t, descriptor, spec, wantArgs, wantEnv)
			if descriptor.ManagerProfile != "systemd_user_v255" || descriptor.Filename != "io.github.bjornarhagen.saga-rydd.service" {
				t.Fatal("unexpected systemd profile", descriptor)
			}
			directives := map[string][]string{}
			section := ""
			for _, line := range strings.Split(descriptor.Content, "\n") {
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
					section = line[1 : len(line)-1]
					continue
				}
				key, value, found := strings.Cut(line, "=")
				if !found || section == "" {
					t.Fatal("invalid service directive", line)
				}
				directives[section+"."+key] = append(directives[section+"."+key], value)
			}
			for key, wanted := range map[string]string{"Service.Type": "exec", "Service.Restart": "on-failure", "Service.RestartSec": "30s", "Service.TimeoutStopSec": "10s", "Service.Nice": "10", "Service.IOSchedulingClass": "idle", "Install.WantedBy": "default.target"} {
				if !reflect.DeepEqual(directives[key], []string{wanted}) {
					t.Fatal("service lifecycle/priority contract differs", key, directives[key])
				}
			}
			starts := directives["Service.ExecStart"]
			if len(starts) != 1 {
				t.Fatal("service does not have exactly one command", starts)
			}
			decodedArgs := systemdQuotedItems(t, starts[0])
			if len(decodedArgs) == 0 || !strings.HasPrefix(decodedArgs[0], ":/") {
				t.Fatal("environment expansion is not disabled", decodedArgs)
			}
			decodedArgs[0] = decodedArgs[0][1:]
			if !reflect.DeepEqual(decodedArgs, wantArgs) {
				t.Fatal("systemd changed literal argument bytes", decodedArgs, wantArgs)
			}
			decodedEnv := map[string]string{}
			for _, line := range directives["Service.Environment"] {
				items := systemdQuotedItems(t, line)
				if len(items) != 1 {
					t.Fatal("unexpected environment item count", items)
				}
				key, value, found := strings.Cut(items[0], "=")
				if !found {
					t.Fatal("invalid environment assignment", items)
				}
				if _, exists := decodedEnv[key]; exists {
					t.Fatal("duplicate environment assignment", key)
				}
				decodedEnv[key] = value
			}
			if !reflect.DeepEqual(decodedEnv, wantEnv) {
				t.Fatal("systemd changed the frozen environment", decodedEnv, wantEnv)
			}
			for _, forbidden := range []string{"Service.ExecStartPre", "Service.ExecStartPost", "Service.ExecStop", "Service.EnvironmentFile", "Service.User", "Service.CPUQuota", "Service.RuntimeDirectory"} {
				if _, exists := directives[forbidden]; exists {
					t.Fatal("descriptor gained another operation or asserted quota", forbidden)
				}
			}
		})
	}
}

func assertDescriptorScope(t *testing.T, descriptor Descriptor, spec Spec, args []string, env map[string]string) {
	t.Helper()
	if descriptor.Contract != "service_descriptor_preview_v1" || descriptor.Platform != spec.GOOS || descriptor.Label != "io.github.bjornarhagen.saga-rydd" || descriptor.Executable != spec.Executable || descriptor.ConfigFile != spec.Paths.ConfigFile || descriptor.StateDir != spec.Paths.StateDir || descriptor.RuntimeDir != spec.RuntimeDir || !reflect.DeepEqual(descriptor.Argv, args) || !reflect.DeepEqual(descriptor.Env, env) || descriptor.InstallationPerformed || descriptor.ActivationPerformed || descriptor.ScanningEnabled || descriptor.Content == "" || len(descriptor.Content) > 64<<10 {
		t.Fatal("preview scope or authority differs", descriptor)
	}
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"installation_performed", "activation_performed", "scanning_enabled"} {
		if value, exists := fields[key]; !exists || value != false {
			t.Fatal("JSON gained activation or lost an explicit false field", string(raw))
		}
	}
}

func TestServiceDescriptorStrictBoundsAndLayouts(t *testing.T) {
	for _, field := range []string{"executable", "configuration", "state", "runtime"} {
		for name, invalid := range map[string]string{
			"empty": "", "relative": "relative", "home expansion": "~/state", "unclean": "/a/../b", "trailing separator": "/a/", "double separator": "/a//b",
			"invalid UTF8": "/bad\xff", "NUL": "/bad\x00", "newline": "/bad\n", "escape": "/bad\x1b", "DEL": "/bad\x7f", "C1": "/bad\u0085", "format control": "/bad\u202e", "XML noncharacter": "/bad\uffff", "too long": "/" + strings.Repeat("a", 4096),
		} {
			t.Run(field+"/"+name, func(t *testing.T) {
				spec := unifiedSpec("linux")
				switch field {
				case "executable":
					spec.Executable = invalid
				case "configuration":
					spec.Paths.ConfigFile = invalid
				case "state":
					spec.Paths.StateDir = invalid
				case "runtime":
					spec.RuntimeDir = invalid
				}
				assertServiceSpecRefused(t, spec)
			})
		}
	}
	for _, test := range []struct {
		name string
		spec Spec
	}{
		{name: "unknown OS", spec: Spec{GOOS: "windows"}},
		{name: "executable root", spec: Spec{GOOS: "darwin", Executable: "/", Paths: config.Paths{ConfigFile: "/state/config.toml", StateDir: "/state"}, RuntimeDir: "/runtime"}},
		{name: "unsupported Darwin split", spec: Spec{GOOS: "darwin", Executable: "/bin/rydd", Paths: config.Paths{ConfigFile: "/config/saga-rydd/config.toml", StateDir: "/state/saga-rydd"}, RuntimeDir: "/runtime"}},
		{name: "wrong split config name", spec: Spec{GOOS: "linux", Executable: "/bin/rydd", Paths: config.Paths{ConfigFile: "/config/saga-rydd/other.toml", StateDir: "/state/saga-rydd"}, RuntimeDir: "/runtime"}},
		{name: "wrong split config parent", spec: Spec{GOOS: "linux", Executable: "/bin/rydd", Paths: config.Paths{ConfigFile: "/config/other/config.toml", StateDir: "/state/saga-rydd"}, RuntimeDir: "/runtime"}},
		{name: "wrong split state name", spec: Spec{GOOS: "linux", Executable: "/bin/rydd", Paths: config.Paths{ConfigFile: "/config/saga-rydd/config.toml", StateDir: "/state/other"}, RuntimeDir: "/runtime"}},
	} {
		t.Run(test.name, func(t *testing.T) { assertServiceSpecRefused(t, test.spec) })
	}
	// Exercise the byte boundary and worst accepted XML escaping without files.
	spec := Spec{GOOS: "darwin", Executable: "/" + strings.Repeat("&", 4095), Paths: config.Paths{ConfigFile: "/" + strings.Repeat("&", 4083) + "/config.toml", StateDir: "/" + strings.Repeat("&", 4083)}, RuntimeDir: "/" + strings.Repeat("&", 4095)}
	if descriptor, err := Build(spec); err != nil || len(descriptor.Content) > 64<<10 {
		t.Fatal("bounded maximum path descriptor refused or oversized", len(descriptor.Content), err)
	}
}

func TestServiceLinuxExecutableParserRestrictions(t *testing.T) {
	for name, executable := range map[string]string{
		"double quote": "/tools/rydd\"name",
		"single quote": "/tools/rydd'name",
		"backslash":    "/tools/rydd\\name",
	} {
		t.Run(name, func(t *testing.T) {
			spec := unifiedSpec("linux")
			spec.Executable = executable
			assertServiceSpecRefused(t, spec)
			// launchd uses literal XML strings and has no systemd executable
			// parser restriction. Its decoded executable must remain exact.
			spec.GOOS = "darwin"
			descriptor, err := Build(spec)
			if err != nil {
				t.Fatal("Darwin executable was restricted by Linux parsing", err)
			}
			var plist plistNode
			if err := xml.Unmarshal([]byte(descriptor.Content), &plist); err != nil {
				t.Fatal(err)
			}
			arguments := plistDictionary(t, plist.Children[0])["ProgramArguments"]
			if len(arguments.Children) == 0 || arguments.Children[0].Text != executable {
				t.Fatal("Darwin executable did not retain literal bytes", arguments)
			}
		})
	}
}

func assertServiceSpecRefused(t *testing.T, spec Spec) {
	t.Helper()
	descriptor, err := Build(spec)
	if !errors.Is(err, ErrSpec) || !reflect.DeepEqual(descriptor, Descriptor{}) {
		t.Fatal("invalid specification returned a usable descriptor", descriptor, err)
	}
}

func TestServiceDescriptorDeterministicAndEnvironmentIndependent(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/must-not-use-config")
	t.Setenv("XDG_STATE_HOME", "/must-not-use-state")
	t.Setenv("RYDD_RUNTIME_DIR", "/must-not-use-runtime")
	for _, platform := range []string{"darwin", "linux"} {
		spec := unifiedSpec(platform)
		first, err := Build(spec)
		if err != nil {
			t.Fatal(err)
		}
		for range 10 {
			repeated, err := Build(spec)
			if err != nil || !reflect.DeepEqual(repeated, first) {
				t.Fatal("same specification changed descriptor", err)
			}
		}
		first.Argv[0] = "changed"
		first.Env["RYDD_RUNTIME_DIR"] = "changed"
		fresh, err := Build(spec)
		if err != nil || fresh.Argv[0] != spec.Executable || fresh.Env["RYDD_RUNTIME_DIR"] != spec.RuntimeDir {
			t.Fatal("returned mutable data changed later builds", fresh, err)
		}
	}
}
