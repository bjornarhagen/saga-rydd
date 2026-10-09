package scripts

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const raceModule = "github.com/bjornarhagen/saga-rydd"

func raceGroupFixture(t *testing.T, listing []byte, listStatus string) (func(...string) (string, error), func() [][]string) {
	t.Helper()
	script := filepath.Join(raceRepoRoot(t), "scripts", "race-group")
	dir := t.TempDir()
	list := filepath.Join(dir, "packages")
	log := filepath.Join(dir, "calls")
	if err := os.WriteFile(list, listing, 0600); err != nil {
		t.Fatal(err)
	}
	fake := `#!/bin/sh
printf '%s\000' "$@" >> "$RYDD_RACE_CALLS"
printf '\n' >> "$RYDD_RACE_CALLS"
case "$1" in
    list) cat "$RYDD_RACE_PACKAGES"; exit "$RYDD_RACE_LIST_STATUS" ;;
    test) exit 0 ;;
    *) exit 97 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		if err := os.Remove(log); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "/bin/bash", append([]string{script}, args...)...)
		cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "RYDD_RACE_PACKAGES="+list, "RYDD_RACE_CALLS="+log, "RYDD_RACE_LIST_STATUS="+listStatus)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	calls := func() [][]string {
		b, err := os.ReadFile(log)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		var result [][]string
		for _, line := range bytes.Split(bytes.TrimSuffix(b, []byte{'\n'}), []byte{'\n'}) {
			result = append(result, strings.Split(string(bytes.TrimSuffix(line, []byte{0})), "\x00"))
		}
		return result
	}
	return run, calls
}

func raceRepoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{cwd, filepath.Dir(cwd)} {
		if _, err := os.Stat(filepath.Join(root, "scripts", "race-group")); err == nil {
			return root
		}
	}
	t.Fatal("run generated runner tests from the checkout or scripts directory")
	return ""
}

func raceFixturePackages() []string {
	return []string{raceModule + "/cmd/rydd", raceModule + "/internal/cli", raceModule + "/internal/inventory", raceModule + "/internal/state", raceModule + "/internal/worker", raceModule + "/scripts", raceModule + "/new_future_package", raceModule + "/internal/cli/newsubpackage"}
}

func TestRaceGroupsPartitionEveryDiscoveredPackage(t *testing.T) {
	packages := raceFixturePackages()
	run, calls := raceGroupFixture(t, []byte(strings.Join(packages, "\n")+"\n"), "0")
	want := map[string][]string{
		"cli":        {raceModule + "/internal/cli"},
		"inventory":  {raceModule + "/internal/inventory"},
		"state":      {raceModule + "/internal/state"},
		"worker":     {raceModule + "/internal/worker"},
		"supporting": {packages[0], packages[5], packages[6], packages[7]},
	}
	seen := map[string]int{}
	for _, group := range []string{"cli", "inventory", "state", "worker", "supporting"} {
		out, err := run(group)
		if err != nil {
			t.Fatal(group, out, err)
		}
		got := calls()
		argv := append([]string{"test", "-p", "1", "-race", "-timeout=15m"}, want[group]...)
		if len(got) != 2 || !reflect.DeepEqual(got[0], []string{"list", "./..."}) || !reflect.DeepEqual(got[1], argv) {
			t.Fatal("group widened or omitted its exact package argv", group, got, argv)
		}
		for _, path := range got[1][5:] {
			seen[path]++
		}
	}
	if len(seen) != len(packages) {
		t.Fatal("partition omitted or invented package paths", seen)
	}
	for _, path := range packages {
		if seen[path] != 1 {
			t.Fatal("package is not covered exactly once", path, seen[path])
		}
	}
	out, err := run("census")
	if err != nil || !reflect.DeepEqual(calls(), [][]string{{"list", "./..."}}) {
		t.Fatal("census ran tests or refused valid scope", out, err, calls())
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != len(packages)+1 {
		t.Fatal("census omitted package evidence", out)
	}
	for i, path := range packages {
		parts := strings.Split(lines[i+1], "\t")
		if len(parts) != 2 || parts[1] != path || !containsRacePath(want[parts[0]], path) {
			t.Fatal("census disagrees with executed group partition", lines[i+1])
		}
	}
}

func containsRacePath(paths []string, path string) bool {
	for _, p := range paths {
		if p == path {
			return true
		}
	}
	return false
}

func TestRaceGroupsUsageRefusesBeforeDiscovery(t *testing.T) {
	run, calls := raceGroupFixture(t, []byte(strings.Join(raceFixturePackages(), "\n")), "0")
	for _, args := range [][]string{nil, {""}, {"all"}, {"--json"}, {"cli", "worker"}, {"cli;echo"}, {"cli\n"}} {
		out, err := run(args...)
		var exit *exec.ExitError
		if err == nil || !strings.Contains(out, "race") || !errors.As(err, &exit) || exit.ExitCode() != 2 || len(calls()) != 0 {
			t.Fatal("invalid group reached a tool", args, out, err, calls())
		}
	}
}

func TestRaceGroupsDiscoveryBoundsAndIdentity(t *testing.T) {
	base := strings.Join(raceFixturePackages(), "\n") + "\n"
	tooMany := base
	for i := 0; i < 57; i++ {
		tooMany += raceModule + "/future" + strings.Repeat("x", i) + "\n"
	}
	cases := map[string][]byte{
		"empty":         nil,
		"missing_heavy": []byte(strings.ReplaceAll(base, raceModule+"/internal/state\n", "")),
		"duplicate":     []byte(base + raceModule + "/internal/worker\n"),
		"foreign":       []byte(base + "example.test/foreign\n"),
		"prefix_alias":  []byte(base + raceModule + "-other/package\n"),
		"space":         []byte(base + raceModule + "/bad path\n"),
		"control":       []byte(base + raceModule + "/bad\rpath\n"),
		"nul":           []byte(base + raceModule + "/bad\x00path\n"),
		"unicode":       []byte(base + raceModule + "/雪\n"),
		"dot_segment":   []byte(base + raceModule + "/../path\n"),
		"empty_segment": []byte(base + raceModule + "//path\n"),
		"long_path":     []byte(base + raceModule + "/" + strings.Repeat("x", 513-len(raceModule)-1) + "\n"),
		"too_many":      []byte(tooMany),
		"large_output":  []byte(strings.Repeat("x", 2<<20)),
	}
	for name, listing := range cases {
		t.Run(name, func(t *testing.T) {
			run, calls := raceGroupFixture(t, listing, "0")
			out, err := run("supporting")
			if err == nil || len(calls()) != 1 || !reflect.DeepEqual(calls()[0], []string{"list", "./..."}) {
				t.Fatal("invalid discovery reached tests", out, err, calls())
			}
		})
	}
	t.Run("list_failed", func(t *testing.T) {
		run, calls := raceGroupFixture(t, []byte(base), "4")
		out, err := run("cli")
		if err == nil || len(calls()) != 1 {
			t.Fatal("failed discovery reached tests", out, err, calls())
		}
	})
	t.Run("exact_bounds", func(t *testing.T) {
		listing := base + raceModule + "/" + strings.Repeat("x", 512-len(raceModule)-1) + "\n"
		for i := 0; i < 55; i++ {
			listing += raceModule + "/boundary" + strings.Repeat("x", i) + "\n"
		}
		run, calls := raceGroupFixture(t, []byte(listing), "0")
		out, err := run("supporting")
		if err != nil || len(calls()) != 2 || len(calls()[1][5:]) != 60 {
			t.Fatal("exact 64-path/512-byte census refused", out, err, calls())
		}
	})
}

func TestRaceGroupsWorkflowCoversBothNativePlatforms(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(raceRepoRoot(t), ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(b)
	start := strings.Index(workflow, "  native-race:\n")
	end := strings.Index(workflow, "  candidate-package:\n")
	if start < 0 || end < start {
		t.Fatal("dedicated native race jobs are absent")
	}
	race := workflow[start:end]
	if !strings.Contains(race, "os: [ubuntu-latest, macos-latest]") || !strings.Contains(race, "group: [cli, inventory, state, worker, supporting]") || !strings.Contains(race, "timeout-minutes: 25") || !strings.Contains(race, "./scripts/tasks race-group '${{ matrix.group }}'") {
		t.Fatal("native group/platform/deadline coverage changed", race)
	}
	functional := workflow[:start]
	if strings.Contains(functional, "run: ./scripts/tasks race\n") || !strings.Contains(functional, "run: ./scripts/tasks race-census") {
		t.Fatal("functional job duplicated races or omitted census")
	}
	for _, gate := range []string{"./scripts/tasks check", "Generated duplicate acceptance", "Disposable existing-tool Go cache", "Disposable existing-tool Cargo", "Native no-overwrite rename", "Isolated Linux bind-mount", "./scripts/tasks sqlite-check", "./scripts/tasks sqlite-bench", "Native CLI smoke", "Finite generated background-resource"} {
		if !strings.Contains(functional, gate) {
			t.Fatal("functional native acceptance gate omitted", gate)
		}
	}
}
