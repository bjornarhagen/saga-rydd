package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type buildOutputCLIFixture struct {
	base, root, inventory string
}

func (f buildOutputCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func addBuildOutputCLIProject(t *testing.T, root, name, profile string, old, partial bool) {
	t.Helper()
	project := filepath.Join(root, name)
	p := filepath.Join(project, "target", profile)
	dirs := []string{project, filepath.Join(project, "target"), p, filepath.Join(p, "deps"), filepath.Join(p, ".fingerprint")}
	for _, path := range dirs {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := []string{filepath.Join(project, "Cargo.toml"), filepath.Join(project, "Cargo.lock"), filepath.Join(p, ".cargo-lock"), filepath.Join(p, "deps", "artifact")}
	for _, path := range files {
		// Deliberately invalid Cargo inputs: recognition must use metadata only.
		if err := os.WriteFile(path, []byte("generated fixture; not validated Cargo syntax"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if partial {
		if err := os.Mkdir(filepath.Join(p, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if old {
		stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
		for _, path := range append(files, dirs...) {
			if err := os.Chtimes(path, stamp, stamp); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func newBuildOutputCLIFixture(t *testing.T, projects int, rootName string, partial bool) buildOutputCLIFixture {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := buildOutputCLIFixture{base: filepath.Join(temp, "private state"), root: filepath.Join(temp, rootName)}
	for i := 0; i < projects; i++ {
		profile := "debug"
		if i%2 != 0 {
			profile = "release"
		}
		addBuildOutputCLIProject(t, f.root, fmt.Sprintf("old-%02d", i), profile, true, partial)
	}
	addBuildOutputCLIProject(t, f.root, "recent", "debug", false, false)
	addBuildOutputCLIProject(t, f.root, "unsupported", "debug", true, false)
	if err := os.Remove(filepath.Join(f.root, "unsupported", "Cargo.lock")); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--detailed", "--now", "--json")
	if code != 0 || stderr != "" {
		t.Fatal("generated metadata scan failed", code, raw, stderr)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.inventory = manualState(paths, f.root)
	// Reporting must not load this malformed configuration or invoke Cargo.
	if err := os.WriteFile(paths.ConfigFile, []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func buildOutputCLIReport(t *testing.T, code int, raw, stderr string) state.BuildOutputReport {
	t.Helper()
	var envelope struct {
		Version int              `json:"api_version"`
		OK      bool             `json:"ok"`
		Command string           `json:"command"`
		Report  state.FileReport `json:"report"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "report" || envelope.Report.BuildOutput == nil {
		t.Fatal("Cargo report did not return one success envelope", code, raw, stderr, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatal("Cargo report emitted another envelope", raw, err)
	}
	r := *envelope.Report.BuildOutput
	if r.Contract != state.BuildOutputContract || r.Source != "saved_inventory" || r.ContentVerified || r.CurrentStateVerified || r.ApprovalAvailable || r.Executable || r.EstimatedReclaimableBytes != nil || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || r.EntryLimit != state.BuildOutputEntryLimit || r.EntriesExamined > r.EntryLimit || len(r.Findings) > state.BuildOutputFindingLimit || envelope.Report.CurrentStateVerified || envelope.Report.Directory != nil || envelope.Report.Candidates != nil || envelope.Report.SameSize != nil {
		t.Fatal("Cargo report gained authority or mixed modes", raw)
	}
	for _, want := range []string{`"content_verified":false`, `"current_state_verified":false`, `"approval_available":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`, `"selected_file_body_requested_bytes":0`, `"selected_file_body_read_bytes":0`} {
		if !strings.Contains(raw, want) {
			t.Fatal("Cargo report omitted an explicit qualification", want, raw)
		}
	}
	count := 0
	for _, d := range r.Diagnostics {
		count += d.Count
	}
	if count != r.EntriesExamined || buildOutputCLIDiagnostic(r, "selected") != len(r.Findings) {
		t.Fatal("page diagnostics do not account for the actual findings", r)
	}
	for _, f := range r.Findings {
		if f.Rule != state.BuildOutputRule || f.Classification != "review_required" || f.Recognition != "saved_layout_metadata_only" || len(f.Markers) != 7 || len(f.Actions) != 0 || f.ContentVerified || f.CurrentStateVerified || f.ApprovalAvailable || f.Executable || f.EstimatedReclaimableBytes != nil || f.Measurement.CurrentStateVerified {
			t.Fatal("Cargo finding gained authority or lost frozen layout evidence", f)
		}
	}
	return r
}

func buildOutputCLIDiagnostic(r state.BuildOutputReport, code string) int {
	for _, d := range r.Diagnostics {
		if d.Code == code {
			return d.Count
		}
	}
	return 0
}

func buildOutputCLIError(t *testing.T, code int, raw, stderr, command, errorCode string, exit int) {
	t.Helper()
	var envelope struct {
		Version int    `json:"api_version"`
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.OK || envelope.Version != APIVersion || envelope.Command != command || envelope.Error.Code != errorCode || strings.Contains(raw, `"report":`) || strings.Contains(raw, `"plan":`) || strings.Contains(raw, `"dismissal_request":`) {
		t.Fatal("refusal emitted partial evidence or the wrong error", code, raw, stderr, err)
	}
}

func TestCargoBuildOutputCLIRecognizedSavedLayoutOfflineAndReadOnly(t *testing.T) {
	f := newBuildOutputCLIFixture(t, 1, "Cargo Ω\t'\x1b fixture", true)
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root, "--json")
	r := buildOutputCLIReport(t, code, raw, stderr)
	if len(r.Findings) != 1 || r.Findings[0].Path != filepath.Join(f.root, "old-00", "target") || r.Findings[0].Profile != "debug" || r.Findings[0].Measurement.Status != "partial" || r.Findings[0].Measurement.SkippedEntries == 0 || buildOutputCLIDiagnostic(r, "age_not_met") != 1 || buildOutputCLIDiagnostic(r, "manifest_missing_or_unsupported") != 1 {
		t.Fatal("saved recognition or independent size qualifications changed", raw)
	}
	code, human, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"1 Cargo build-output candidate on this page", fmt.Sprintf("%q", string(r.Findings[0].PathBytes)), "Incomplete measurement", "Known layout", "target/debug", "Too recent / future-dated", "at least 90 days old", "not estimates of space you can free", "No source bodies, configuration or Cargo commands", "Full saved marker and size evidence (JSON)", commandPrefix(config.Paths{StateDir: f.base}) + " report --build-output -d " + shellQuote(f.root) + " --min-age-days 90 --json"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, strings.Join(strings.Fields(want), " ")) {
			t.Fatal("human report lost material scope or qualification", want, code, human, stderr)
		}
	}
	if !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("report changed generated source/state bytes or exposed raw controls")
	}
	for _, args := range [][]string{
		{"plan", "--preview", "-d", f.root, r.Findings[0].ID, "--json"},
		{"plan", "--save", "-d", f.root, r.Findings[0].ID, "--json"},
		{"ignore", "--preview", "-d", f.root, r.Findings[0].ID, "--json"},
	} {
		code, raw, stderr := f.run(context.Background(), args...)
		buildOutputCLIError(t, code, raw, stderr, args[0], "invalid_arguments", 2)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("Cargo references initialized or changed node_modules plan/dismissal storage")
	}
	offline := f.root + "-offline"
	if err := os.Rename(f.root, offline); err != nil {
		t.Fatal(err)
	}
	offlineBefore := hashCLIBytes(t, f.base, offline)
	code, raw, stderr = f.run(context.Background(), "report", "--build-output", "-d", f.root, "--json")
	got := buildOutputCLIReport(t, code, raw, stderr)
	if len(got.Findings) != 1 || !bytes.Equal(got.Findings[0].PathBytes, r.Findings[0].PathBytes) || !reflect.DeepEqual(got.Findings[0].Markers, r.Findings[0].Markers) || !reflect.DeepEqual(offlineBefore, hashCLIBytes(t, f.base, offline)) {
		t.Fatal("offline report refreshed source evidence or changed bytes", raw)
	}
	if _, err := os.Lstat(f.root); !os.IsNotExist(err) {
		t.Fatal("saved report created the offline source root", err)
	}
}

func TestCargoBuildOutputCLIPageContinuationAgeAndPriorJSONModes(t *testing.T) {
	f := newBuildOutputCLIFixture(t, 21, "projects", false)
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "--json", "report", "--build-output", "--directory", f.root, "--min-age-days", "30")
	first := buildOutputCLIReport(t, code, raw, stderr)
	if len(first.Findings) != 20 || first.NextCursor == "" || first.PageCoverage != "more_saved_entries" || first.MinimumAgeDays != 30 {
		t.Fatal("bounded first page was not truthful", raw)
	}
	code, raw, stderr = f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "30", "--cursor", first.NextCursor, "--json")
	second := buildOutputCLIReport(t, code, raw, stderr)
	if len(second.Findings) != 1 || second.NextCursor != "" || second.PageCoverage != "saved_entries_exhausted" {
		t.Fatal("continuation lost or duplicated findings", raw)
	}
	seen := map[string]bool{}
	for _, finding := range append(first.Findings, second.Findings...) {
		if seen[finding.ID] {
			t.Fatal("continuation repeated a finding", finding.ID)
		}
		seen[finding.ID] = true
	}
	code, human, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "30", "--cursor", first.NextCursor)
	if code != 0 || stderr != "" || !strings.Contains(human, "--cursor "+shellQuote(first.NextCursor)+" --json") || !strings.Contains(human, "End of saved entries. This does not prove the scan is complete.") {
		t.Fatal("human current-page evidence command lost cursor or scan qualification", code, human, stderr)
	}
	code, raw, stderr = f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "36500", "--json")
	empty := buildOutputCLIReport(t, code, raw, stderr)
	if len(empty.Findings) != 0 || buildOutputCLIDiagnostic(empty, "age_not_met") != 22 {
		t.Fatal("age filter did not use every required saved marker", raw)
	}
	for _, args := range [][]string{
		{"report", "-d", f.root, "--json"},
		{"report", "--candidates", "-d", f.root, "--json"},
		{"report", "--same-size", "--min-size-bytes", "1", "-d", f.root, "--json"},
	} {
		code, raw, stderr := f.run(context.Background(), args...)
		if code != 0 || stderr != "" || !strings.Contains(raw, `"ok":true`) || strings.Contains(raw, `"build_output":`) {
			t.Fatal("ordinary JSON report contract changed", code, raw, stderr)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("pagination or compatibility reports changed generated source/state bytes")
	}
}

func TestCargoBuildOutputCLIStrictArgumentsAndNoConfiguredFallback(t *testing.T) {
	base := filepath.Join(t.TempDir(), "missing state")
	root := filepath.Join(t.TempDir(), "not created")
	f := buildOutputCLIFixture{base: base, root: root}
	for _, args := range [][]string{
		{"--build-output"}, {"--build-output=false", "-d", root}, {"--build-output=bad", "-d", root},
		{"--build-output", "--build-output", "-d", root}, {"--build-output", "-d", root, "-d", root},
		{"--build-output", "-d", root, "--directory", root}, {"--build-output", "-d", ""},
		{"--build-output", "-d", root, "--candidates=false"}, {"--candidates", "--build-output", "-d", root},
		{"--build-output", "-d", root, "--same-size=false"}, {"--same-size", "--build-output", "-d", root},
		{"--build-output", "-d", root, "--include-dismissed=false"}, {"--build-output", "-d", root, "--limit", "20"},
		{"--build-output", "-d", root, "--min-size-bytes", "1"}, {"--build-output", "-d", root, "extra"},
		{"--build-output", "-d", root, "--", "--cursor", "cargo1:90:1"},
		{"--build-output", "-d", root, "--min-age-days", "0"}, {"--build-output", "-d", root, "--min-age-days", "36501"},
		{"--build-output", "-d", root, "--min-age-days", "30", "--min-age-days", "30"},
		{"--build-output", "-d", root, "--cursor", "cargo1:30:1"}, {"--build-output", "-d", root, "--cursor", "cargo1:90:01"},
		{"--build-output", "-d", root, "--cursor", "cargo1:90:0"}, {"--build-output", "-d", root, "--cursor", "cargo1:90:9223372036854775808"},
		{"--build-output", "-d", root, "--cursor", "cargo1:90:1", "--cursor", "cargo1:90:2"},
	} {
		args = append([]string{"report"}, args...)
		code, raw, stderr := f.run(context.Background(), append([]string{"--json"}, args...)...)
		buildOutputCLIError(t, code, raw, stderr, "report", "invalid_arguments", 2)
		code, _, stderr = f.run(context.Background(), args...)
		if code != 2 || stderr == "" {
			t.Fatal("human malformed input did not refuse before storage", args, code, stderr)
		}
	}
	for _, valueFlag := range []string{"--cursor", "-cursor", "--min-age-days", "-min-age-days", "-d", "--directory"} {
		args := []string{"--json", "report", "--build-output"}
		if valueFlag != "-d" && valueFlag != "--directory" {
			args = append(args, "-d", root)
		}
		args = append(args, valueFlag, "--json")
		code, raw, stderr := f.run(context.Background(), args...)
		want, exit := "invalid_arguments", 2
		if valueFlag == "-d" || valueFlag == "--directory" {
			// --json is the literal directory value, not an output option.
			want, exit = "not_found", 1
		}
		buildOutputCLIError(t, code, raw, stderr, "report", want, exit)
	}
	for _, valueFlag := range []string{"--min-age-days", "-min-age-days", "--cursor", "-cursor"} {
		for _, value := range []string{"--json", "-json"} {
			code, raw, stderr := f.run(context.Background(), "report", "--build-output", "-d", root, valueFlag, value)
			if code != 2 || raw != "" || stderr == "" {
				t.Fatal("invalid flag-like data selected JSON output", valueFlag, value, code, raw, stderr)
			}
		}
	}
	if _, err := os.Lstat(base); !os.IsNotExist(err) {
		t.Fatal("refused arguments initialized state", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr := f.run(ctx, "report", "--build-output", "-d", root, "--json")
	buildOutputCLIError(t, code, raw, stderr, "report", "canceled", 1)
	// A valid configured/global inventory must never substitute for the exact
	// missing manual inventory, even when it contains the requested root.
	writer, err := state.OpenWriter(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.SyncRoots(context.Background(), []string{root}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, base)
	code, raw, stderr = f.run(context.Background(), "report", "--build-output", "-d", root, "--json")
	buildOutputCLIError(t, code, raw, stderr, "report", "not_found", 1)
	code, _, stderr = f.run(context.Background(), "report", "--build-output", "-d", root)
	if code != 1 || !strings.Contains(stderr, "Scan it first") || strings.Contains(stderr, "Use rydd init") || !reflect.DeepEqual(before, hashCLIBytes(t, base)) {
		t.Fatal("missing manual scan used fallback or initialization guidance", code, stderr)
	}
	if _, err := os.Lstat(filepath.Join(base, "manual")); !os.IsNotExist(err) {
		t.Fatal("missing report initialized a manual store", err)
	}
}

func TestCargoBuildOutputCLIEmptyBoundedPageContinues(t *testing.T) {
	f := newBuildOutputCLIFixture(t, 1, "projects", false)
	// Generated saved rows place the one eligible target beyond the examined
	// entry limit. No filesystem traversal is needed by either report page.
	db, err := sql.Open("sqlite", filepath.Join(f.inventory, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= state.BuildOutputEntryLimit; i++ {
		if _, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,'file',0,0,1,1,'fixture','fixture',1,1)`, []byte(fmt.Sprintf("saved-only-%04d", i)), []byte(".")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(`UPDATE entries SET id=(SELECT max(id)+1 FROM entries) WHERE path=?`, []byte(filepath.Join("old-00", "target"))); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "30", "--json")
	first := buildOutputCLIReport(t, code, raw, stderr)
	if len(first.Findings) != 0 || first.EntriesExamined != state.BuildOutputEntryLimit || first.NextCursor == "" || first.PageCoverage != "more_saved_entries" {
		t.Fatal("bounded empty page hid continuation", raw)
	}
	code, human, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "30")
	flat := strings.Join(strings.Fields(human), " ")
	command := commandPrefix(config.Paths{StateDir: f.base}) + " report --build-output -d " + shellQuote(f.root) + " --min-age-days 30 --cursor " + shellQuote(first.NextCursor)
	if code != 0 || stderr != "" || !strings.Contains(flat, "0 Cargo build-output candidates on this page") || !strings.Contains(flat, "This page is empty, but more saved entries remain.") || !strings.Contains(flat, strings.Join(strings.Fields(command), " ")) || strings.Contains(human, "End of saved entries") {
		t.Fatal("empty-page human output implied complete inventory or lost scope", code, human, stderr)
	}
	code, raw, stderr = f.run(context.Background(), "report", "--build-output", "-d", f.root, "--min-age-days", "30", "--cursor", first.NextCursor, "--json")
	second := buildOutputCLIReport(t, code, raw, stderr)
	if len(second.Findings) != 1 || second.NextCursor != "" || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("empty-page continuation lost finding or changed bytes", raw)
	}
}

func TestCargoBuildOutputCLIOutputFailuresCancellationAndCapabilities(t *testing.T) {
	f := newBuildOutputCLIFixture(t, 1, "projects", false)
	before := hashCLIBytes(t, f.base, f.root)
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "report", "--build-output", "-d", f.root}
		if machine {
			args = append(args, "--json")
		}
		for _, short := range []bool{false, true} {
			var stderr bytes.Buffer
			if code := Run(context.Background(), args, hashFailWriter{short: short}, &stderr); code != 1 || stderr.Len() == 0 {
				t.Fatal("failed output reported success", machine, short, code, stderr.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		code := Run(ctx, args, out, &stderr)
		cancel()
		if code != 1 || !strings.Contains(stderr.String(), "reply was canceled") {
			t.Fatal("late canceled output reported success", machine, code, out.String(), stderr.String())
		}
		if machine {
			buildOutputCLIReport(t, 0, out.String(), "")
		}
	}
	code, raw, stderr := f.run(context.Background(), "capabilities", "--json")
	if code != 0 || stderr != "" || !strings.Contains(raw, `"cargo_build_output_reports":true`) || !strings.Contains(raw, `"cleanup":false`) || !strings.Contains(raw, "--build-output -d ROOT") {
		t.Fatal("capabilities lost supported scope", code, raw, stderr)
	}
	code, human, stderr := f.run(context.Background(), "--help")
	if code != 0 || stderr != "" || !strings.Contains(human, "report --build-output -d ROOT") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("help/output tests changed generated source or saved state", code, human, stderr)
	}
}

func TestCargoBuildOutputCLIInvalidBytePathsLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Darwin filesystems reject invalid-UTF-8 filenames; Unicode/control rendering is tested independently")
	}
	f := newBuildOutputCLIFixture(t, 1, "Cargo-\xff", false)
	code, raw, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root, "--json")
	r := buildOutputCLIReport(t, code, raw, stderr)
	if len(r.Findings) != 1 || !bytes.Equal(r.Findings[0].PathBytes, []byte(filepath.Join(f.root, "old-00", "target"))) {
		t.Fatal("machine report changed authoritative raw path bytes", raw)
	}
	code, human, stderr := f.run(context.Background(), "report", "--build-output", "-d", f.root)
	if code != 0 || stderr != "" || !utf8.ValidString(human) || !strings.Contains(human, fmt.Sprintf("%q", string(r.Findings[0].PathBytes))) {
		t.Fatal("human report did not quote the exact raw path", code, human, stderr)
	}
}
