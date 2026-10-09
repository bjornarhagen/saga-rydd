package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type excludeCLIFixture struct {
	base, home string
	paths      config.Paths
	initial    config.Config
}

func newExcludeCLIFixture(t *testing.T, withState bool, name string) excludeCLIFixture {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := excludeCLIFixture{base: filepath.Join(temp, name), home: temp}
	f.paths, err = config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.initial = config.Default()
	f.initial.Roots = []string{filepath.Join(temp, "offline-one"), filepath.Join(temp, "offline-two")}
	f.initial.Excludes = []string{filepath.Join(f.initial.Roots[0], "private"), filepath.Join(f.initial.Roots[0], "private-other"), filepath.Join(f.initial.Roots[0], "private"), filepath.Join(f.initial.Roots[1], "old-cache")}
	f.initial.Scan = config.Scan{WorkSeconds: 3, IntervalSeconds: 7, MetadataPerSecond: 123, MetadataAttemptsPerDay: 20_000_000, ReadBytesPerSecond: 2048, ReadBytesPerDay: 8192, PauseOnBattery: false, MaxScanChunksPerDay: 17}
	if err := config.Create(f.paths.ConfigFile, f.home, f.initial); err != nil {
		t.Fatal(err)
	}
	if withState {
		writer, err := state.OpenWriter(context.Background(), f.paths.StateDir)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncRoots(context.Background(), f.initial.Roots); err != nil {
			writer.Close()
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f excludeCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func excludeCLIReport(t *testing.T, f excludeCLIFixture, code int, raw, stderr string) ExcludeReport {
	t.Helper()
	var envelope struct {
		Version    int           `json:"api_version"`
		OK         bool          `json:"ok"`
		Command    string        `json:"command"`
		Exclusions ExcludeReport `json:"exclusions"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || envelope.Version != APIVersion || !envelope.OK || envelope.Command != "exclude" {
		t.Fatal("exclude did not emit one successful versioned result", code, raw, stderr, err)
	}
	r := envelope.Exclusions
	if !bytes.Equal(r.ConfigFileBytes, []byte(f.paths.ConfigFile)) || !bytes.Equal(r.StateDirBytes, []byte(f.paths.StateDir)) || r.EffectiveOn != "later_invocations" || r.ActiveInvocationsReloaded || r.SavedHistoryChanged || r.SourceFilesRead || r.SourceFilesChanged || r.CurrentStateVerified || r.ApprovalAvailable || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("exclude widened scope, lost authoritative paths or claimed authority", raw)
	}
	if len(r.Exclusions) != len(r.ExclusionPathBytes) {
		t.Fatal("exclusion paths differ", raw)
	}
	for i, p := range r.Exclusions {
		if !bytes.Equal(r.ExclusionPathBytes[i], []byte(p)) {
			t.Fatal("exclusion path bytes differ", raw)
		}
	}
	for _, field := range []string{"active_invocations_reloaded", "saved_history_changed", "source_files_read", "source_files_changed", "current_state_verified", "approval_available", "executable"} {
		if !strings.Contains(raw, `"`+field+`":false`) {
			t.Fatal("missing false qualification", field, raw)
		}
	}
	if !strings.Contains(raw, `"estimated_reclaimable_bytes":null`) {
		t.Fatal("missing unknown savings", raw)
	}
	body, err := os.ReadFile(f.paths.ConfigFile)
	if err != nil || r.ConfigSHA256 != fmt.Sprintf("%x", sha256.Sum256(body)) {
		t.Fatal("config digest does not describe reported saved bytes", raw, err)
	}
	return r
}

func excludeCLIError(t *testing.T, code int, raw, stderr, want string, exit int) string {
	t.Helper()
	var envelope struct {
		Version int    `json:"api_version"`
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.Version != APIVersion || envelope.OK || envelope.Command != "exclude" || envelope.Error.Code != want || strings.Contains(raw, `"exclusions":`) {
		t.Fatal("exclude refusal emitted partial output or wrong error", code, raw, stderr, want, err)
	}
	return envelope.Error.Message
}

func TestExcludeCLIExactOfflineEditsPreserveSettingsAndNoops(t *testing.T) {
	f := newExcludeCLIFixture(t, true, "private state")
	before := hashCLIBytes(t, f.base)
	code, raw, stderr := f.run(context.Background(), "exclude", "--list", "--json")
	listed := excludeCLIReport(t, f, code, raw, stderr)
	if listed.Action != "list" || listed.Changed || listed.Publication != "not_requested" || !reflect.DeepEqual(listed.Exclusions, f.initial.Excludes) {
		t.Fatal(listed)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("list changed state/configuration")
	}
	for _, args := range [][]string{{"exclude", "--add", f.initial.Excludes[0], "--json"}, {"exclude", "--remove", filepath.Join(f.initial.Roots[0], "absent"), "--json"}} {
		var stampBefore, stampAfter unix.Stat_t
		if err := unix.Lstat(f.paths.ConfigFile, &stampBefore); err != nil {
			t.Fatal(err)
		}
		code, raw, stderr = f.run(context.Background(), args...)
		r := excludeCLIReport(t, f, code, raw, stderr)
		if r.Changed || r.Publication != "not_needed" || !reflect.DeepEqual(r.Exclusions, f.initial.Excludes) {
			t.Fatal("exact no-op rewrote exclusions", r)
		}
		if err := unix.Lstat(f.paths.ConfigFile, &stampAfter); err != nil || !sameHashConfigStamp(stampBefore, stampAfter) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
			t.Fatal("no-op changed config identity/stamp/bytes or state", err)
		}
	}
	path := f.initial.Roots[0] + "/temporary/../cache"
	code, raw, stderr = f.run(context.Background(), "exclude", "--add", path, "--json")
	added := excludeCLIReport(t, f, code, raw, stderr)
	want := append(append([]string(nil), f.initial.Excludes...), filepath.Clean(path))
	if added.Action != "add" || !added.Changed || added.Publication != "saved" || !bytes.Equal(added.PathBytes, []byte(filepath.Clean(path))) || !reflect.DeepEqual(added.Exclusions, want) {
		t.Fatal("add did not preserve exact cleaned order", added)
	}
	code, raw, stderr = f.run(context.Background(), "exclude", "--remove", f.initial.Excludes[0], "--json")
	removed := excludeCLIReport(t, f, code, raw, stderr)
	want = []string{f.initial.Excludes[1], f.initial.Excludes[3], filepath.Clean(path)}
	if removed.Action != "remove" || !removed.Changed || removed.Publication != "saved" || !reflect.DeepEqual(removed.Exclusions, want) {
		t.Fatal("remove did not remove all exact duplicates or damaged a prefix-neighbor", removed)
	}
	cfg, err := config.Load(f.paths.ConfigFile, f.home)
	expected := f.initial
	expected.Excludes = want
	if err != nil || !reflect.DeepEqual(cfg, expected) {
		t.Fatal("edit changed roots/budgets/other settings", cfg, expected, err)
	}
	after := hashCLIBytes(t, f.base)
	delete(before, f.paths.ConfigFile)
	delete(after, f.paths.ConfigFile)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("config edits changed inventory/history/storage bytes")
	}
	for _, root := range f.initial.Roots {
		if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("offline root was inspected or initialized", root, err)
		}
	}
}

func TestExcludeCLIArgumentsMissingConfigurationAndJSONValues(t *testing.T) {
	base := filepath.Join(t.TempDir(), "never-initialized")
	f := excludeCLIFixture{base: base}
	for _, args := range [][]string{
		{"exclude"}, {"exclude", "--list=false"}, {"exclude", "--list", "--list"}, {"exclude", "--list", "-list"},
		{"exclude", "--list", "--add", "/absolute"}, {"exclude", "--add", "/absolute", "--remove", "/absolute"},
		{"exclude", "--add", "/absolute", "-add", "/absolute"}, {"exclude", "--remove", "/absolute", "--remove", "/absolute"},
		{"exclude", "--add", "relative"}, {"exclude", "--remove", "~/relative"}, {"exclude", "--add", ""},
		{"exclude", "--add", "/bad\x00path"}, {"exclude", "--remove", "/bad\xffpath"}, {"exclude", "--add", "/" + strings.Repeat("a", 4096)},
		{"exclude", "--list", "extra"}, {"exclude", "--list", "-d", "/override"}, {"exclude", "--add", "/one", "/two"}, {"exclude", "--unknown"},
	} {
		code, raw, stderr := f.run(context.Background(), append(args, "--json")...)
		excludeCLIError(t, code, raw, stderr, "invalid_arguments", 2)
	}
	for _, args := range [][]string{{"exclude", "--list"}, {"exclude", "--add", "/absolute"}, {"exclude", "--remove", "/absolute"}, {"--json", "exclude", "--list"}, {"exclude", "--list", "--json"}} {
		code, raw, stderr := f.run(context.Background(), append(args, "--json")...)
		excludeCLIError(t, code, raw, stderr, "not_found", 1)
	}
	// A value looking like --json remains a path argument; list remains a bool.
	for _, flag := range []string{"--add", "-add", "--remove", "-remove"} {
		code, raw, stderr := f.run(context.Background(), "exclude", flag, "--json")
		if code != 2 || raw != "" || !strings.Contains(stderr, "absolute UTF-8") {
			t.Fatal("JSON-looking value selected machine mode", code, raw, stderr)
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid/missing command initialized storage", err)
	}
}

func TestExcludeCLIConfigOnlyListMissingAndBusyLocks(t *testing.T) {
	f := newExcludeCLIFixture(t, false, "configuration only")
	before := hashCLIBytes(t, f.base)
	code, raw, stderr := f.run(context.Background(), "exclude", "--list", "--json")
	excludeCLIReport(t, f, code, raw, stderr)
	for _, action := range []string{"--add", "--remove"} {
		code, raw, stderr = f.run(context.Background(), "exclude", action, filepath.Join(f.initial.Roots[0], "cache"), "--json")
		excludeCLIError(t, code, raw, stderr, "not_found", 1)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("config-only list/refused edits initialized state/lock")
	}
	f = newExcludeCLIFixture(t, true, "locked state")
	lock, err := localfs.AcquireLock(f.paths.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	before = hashCLIBytes(t, f.base)
	code, raw, stderr = f.run(context.Background(), "exclude", "--add", filepath.Join(f.initial.Roots[0], "cache"), "--json")
	excludeCLIError(t, code, raw, stderr, "writer_busy", 1)
	code, raw, stderr = f.run(context.Background(), "exclude", "--list", "--json")
	excludeCLIReport(t, f, code, raw, stderr)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("busy refusal or read-only list changed state/config")
	}
}

func TestExcludeCLIInvalidConfigurationBoundsAndWholeRootRefuse(t *testing.T) {
	f := newExcludeCLIFixture(t, true, "private state")
	before := hashCLIBytes(t, f.base)
	for _, path := range []string{f.initial.Roots[0], filepath.Dir(f.initial.Roots[0])} {
		code, raw, stderr := f.run(context.Background(), "exclude", "--add", path, "--json")
		excludeCLIError(t, code, raw, stderr, "invalid_arguments", 2)
		code, raw, stderr = f.run(context.Background(), "exclude", "--add", path)
		if code != 2 || raw != "" || stderr == "" {
			t.Fatal("human root exclusion classification differs", code, raw, stderr)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("whole-root refusal changed configuration/state")
	}
	var exclusions []string
	for i := 0; i < 129; i++ {
		exclusions = append(exclusions, fmt.Sprintf("'/one/cache-%d'", i))
	}
	tooMany := "roots=['/one']\nexcludes=[" + strings.Join(exclusions, ",") + "]\n"
	for _, body := range []string{"roots=['/one']\nunknown_setting=true\n", strings.Repeat("#", (1<<20)+1), "roots=['/one']\nexcludes=['/']\n", tooMany} {
		if err := os.WriteFile(f.paths.ConfigFile, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"exclude", "--list", "--json"}, {"exclude", "--add", "/one/cache", "--json"}} {
			code, raw, stderr := f.run(context.Background(), args...)
			want := "command_failed"
			if body == tooMany {
				want = "exclusion_bounds"
			}
			excludeCLIError(t, code, raw, stderr, want, 1)
		}
		after, err := os.ReadFile(f.paths.ConfigFile)
		if err != nil || string(after) != body {
			t.Fatal("invalid config was repaired or rewritten", err)
		}
	}
}

func TestExcludeCLISpecialConfigurationRefusesBeforePublication(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "hardlink", "shared_mode"} {
		t.Run(kind, func(t *testing.T) {
			f := newExcludeCLIFixture(t, true, "private state")
			body, err := os.ReadFile(filepath.Join(f.base, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink", "fifo":
				if err := os.Rename(f.paths.ConfigFile, f.paths.ConfigFile+".original"); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(f.paths.ConfigFile+".original", f.paths.ConfigFile); err != nil {
						t.Fatal(err)
					}
				} else if err := unix.Mkfifo(f.paths.ConfigFile, 0600); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(f.paths.ConfigFile, f.paths.ConfigFile+".alias"); err != nil {
					t.Fatal(err)
				}
			case "shared_mode":
				if err := os.Chmod(f.paths.ConfigFile, 0644); err != nil {
					t.Fatal(err)
				}
			}
			var before, after unix.Stat_t
			if err := unix.Lstat(f.paths.ConfigFile, &before); err != nil {
				t.Fatal(err)
			}
			namesBefore, err := os.ReadDir(f.base)
			if err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"exclude", "--list", "--json"}, {"exclude", "--add", filepath.Join(f.initial.Roots[0], "cache"), "--json"}} {
				code, raw, stderr := f.run(context.Background(), args...)
				excludeCLIError(t, code, raw, stderr, "command_failed", 1)
			}
			if err := unix.Lstat(f.paths.ConfigFile, &after); err != nil || !sameHashConfigStamp(before, after) {
				t.Fatal("private config refusal changed the named object", err)
			}
			namesAfter, err := os.ReadDir(f.base)
			if err != nil || len(namesAfter) != len(namesBefore) {
				t.Fatal("private config refusal created storage/temp files", err)
			}
			for i, entry := range namesBefore {
				if entry.Name() != namesAfter[i].Name() {
					t.Fatal("private config refusal changed names")
				}
			}
			stateBody, err := os.ReadFile(filepath.Join(f.base, state.Filename))
			if err != nil || !bytes.Equal(body, stateBody) {
				t.Fatal("private config refusal changed inventory", err)
			}
		})
	}
}

func TestExcludeCLIReplyFailuresCancellationAndRecoveryGuidance(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			f := newExcludeCLIFixture(t, true, "private state")
			path := filepath.Join(f.initial.Roots[0], "new-cache")
			args := []string{"--data-dir", f.base, "exclude", "--add", path}
			if jsonMode {
				args = append(args, "--json")
			}
			var stderr bytes.Buffer
			code := Run(context.Background(), args, hashFailWriter{short: short}, &stderr)
			cfg, err := config.Load(f.paths.ConfigFile, f.home)
			if err != nil || code != 1 || cfg.Excludes[len(cfg.Excludes)-1] != path || !strings.Contains(stderr.String(), fmt.Sprintf("%q", path)) || !strings.Contains(stderr.String(), commandPrefix(f.paths)+" exclude --list") {
				t.Fatal("reply failure hid committed exact edit", code, stderr.String(), err)
			}
		}
		f := newExcludeCLIFixture(t, true, "cancel reply state")
		path := filepath.Join(f.initial.Roots[0], "new-cache")
		args := []string{"--data-dir", f.base, "exclude", "--add", path}
		if jsonMode {
			args = append(args, "--json")
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		code := Run(ctx, args, out, &stderr)
		cancel()
		cfg, err := config.Load(f.paths.ConfigFile, f.home)
		if err != nil || code != 1 || cfg.Excludes[len(cfg.Excludes)-1] != path || !strings.Contains(stderr.String(), "canceled") || !strings.Contains(stderr.String(), commandPrefix(f.paths)+" exclude --list") {
			t.Fatal("late cancellation hid saved config edit", code, out.String(), stderr.String(), err)
		}
		if jsonMode {
			decoder := json.NewDecoder(strings.NewReader(out.String()))
			var result, extra map[string]any
			if err := decoder.Decode(&result); err != nil || result["ok"] != true {
				t.Fatal("canceled reply did not keep completed envelope", out.String(), err)
			}
			if err := decoder.Decode(&extra); err != io.EOF {
				t.Fatal("canceled reply emitted a second envelope", out.String(), err)
			}
		}
		before := hashCLIBytes(t, f.base)
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		code, raw, diagnostic := f.run(ctx, "exclude", "--remove", path, "--json")
		excludeCLIError(t, code, raw, diagnostic, "canceled", 1)
		if !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
			t.Fatal("early cancellation changed saved config/state")
		}
	}
}

func TestExcludeCLIPublicationErrorRetainsCandidateWithoutSavedClaim(t *testing.T) {
	f := newExcludeCLIFixture(t, true, "private state")
	path := filepath.Join(f.initial.Roots[0], "new-cache")
	r := ExcludeReport{ExclusionResult: config.ExclusionResult{Action: "add", Path: path, PathBytes: []byte(path), ConfigSHA256: strings.Repeat("a", 64), Publication: "uncertain", Changed: true}, EffectiveOn: "later_invocations"}
	for _, cause := range []error{config.ErrExclusionPublication, errors.Join(config.ErrExclusionPublication, context.Canceled)} {
		err := excludeResultError(r, f.paths, "add", path, cause)
		if !errors.Is(err, cause) || !strings.Contains(err.Error(), r.ConfigSHA256) || !strings.Contains(err.Error(), fmt.Sprintf("%q", path)) || !strings.Contains(err.Error(), commandPrefix(f.paths)+" exclude --list") || strings.Contains(err.Error(), "was saved") || strings.Contains(err.Error(), "nothing was saved") {
			t.Fatal("uncertain edit lost candidate evidence or overstated outcome", err)
		}
		var out, stderr bytes.Buffer
		code := operationFailure(&out, &stderr, "exclude", err)
		message := excludeCLIError(t, code, out.String(), stderr.String(), "config_outcome_unknown", 1)
		if message != err.Error() {
			t.Fatal("failure envelope hid inspect guidance", out.String())
		}
	}
}

func TestExcludeCLIQuotedPathsCapabilitiesAndHelp(t *testing.T) {
	f := newExcludeCLIFixture(t, true, "state \"雪\x1b\t")
	path := filepath.Join(f.initial.Roots[0], "literal* quote\"雪\x1b\t")
	code, raw, stderr := f.run(context.Background(), "exclude", "--add", path, "--json")
	r := excludeCLIReport(t, f, code, raw, stderr)
	if !bytes.Equal(r.PathBytes, []byte(path)) || r.Exclusions[len(r.Exclusions)-1] != path {
		t.Fatal("literal/control/Unicode path changed", raw)
	}
	code, human, stderr := f.run(context.Background(), "exclude", "--list")
	prose := strings.Join(strings.Fields(human), " ")
	if code != 0 || stderr != "" || !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || !strings.Contains(human, fmt.Sprintf("Configuration: %q", f.paths.ConfigFile)) || !strings.Contains(human, fmt.Sprintf("%q", path)) || !strings.Contains(human, commandPrefix(f.paths)+" exclude --list") || !strings.Contains(prose, "Active manual scans and hash steps retain their captured settings.") || !strings.Contains(prose, "Saved reports, plans, dismissals and hash history remain unchanged and historical.") {
		t.Fatal("human output lost exact paths or future/saved-history qualifications", code, human, stderr)
	}
	code, raw, stderr = f.run(context.Background(), "capabilities", "--json")
	var caps struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &caps); err != nil || code != 0 || stderr != "" || !caps.Features["persistent_path_exclusions"] || caps.Features["configuration_reload"] || caps.Features["cleanup"] || caps.Features["service_installation"] || !strings.Contains(raw, "--add ABSOLUTE_PATH") || !strings.Contains(raw, "config_outcome_unknown") {
		t.Fatal("capabilities overclaimed or omitted finite exclusions", code, raw, stderr, err)
	}
	code, human, stderr = f.run(context.Background(), "--help")
	if code != 0 || stderr != "" || !strings.Contains(human, "exclude --list / --add ABSOLUTE_PATH / --remove ABSOLUTE_PATH") {
		t.Fatal("help omitted exclusions", code, human, stderr)
	}
}

func TestExcludeCLIInvalidByteDataDirectoryLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("invalid-byte filesystem fixture is Linux-only; Unicode/control output checks run separately on macOS")
	}
	f := newExcludeCLIFixture(t, true, "state-\xff")
	path := filepath.Join(f.initial.Roots[0], "cache")
	code, raw, stderr := f.run(context.Background(), "exclude", "--add", path, "--json")
	excludeCLIReport(t, f, code, raw, stderr)
	code, human, stderr := f.run(context.Background(), "exclude", "--list")
	if code != 0 || stderr != "" || !utf8.ValidString(human) || !strings.Contains(human, fmt.Sprintf("Configuration: %q", f.paths.ConfigFile)) || !strings.Contains(human, commandPrefix(f.paths)+" exclude --list") {
		t.Fatal("raw global data-dir lost authoritative bytes or safe human quoting", code, human, stderr)
	}
}
