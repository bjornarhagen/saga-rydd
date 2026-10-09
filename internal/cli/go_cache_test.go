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

type goCacheCLIFixture struct{ base, root, inventory string }

func (f goCacheCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func newGoCacheCLIFixture(t *testing.T, oldFiles int, rootName string) goCacheCLIFixture {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := goCacheCLIFixture{base: filepath.Join(temp, "private state"), root: filepath.Join(temp, rootName)}
	for i := 0; i < 256; i++ {
		if err := os.MkdirAll(filepath.Join(f.root, fmt.Sprintf("%02x", i)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.root, "README"), []byte("generated layout only; deliberately not validated cache contents"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := 0; i < oldFiles; i++ {
		suffix := "a"
		if i%2 != 0 {
			suffix = "d"
		}
		path := filepath.Join(f.root, "00", fmt.Sprintf("%064x-%s", i, suffix))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("generated invalid cache body %d", i)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{fmt.Sprintf("%064x-a", 5000), strings.Repeat("A", 64) + "-a", "11" + strings.Repeat("0", 62) + "-d", fmt.Sprintf("%064x-q", 5001)} {
		if err := os.WriteFile(filepath.Join(f.root, "00", name), []byte("generated recent or unsupported object"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(f.root, "00", fmt.Sprintf("%064x-d", 6000)), 0700); err != nil {
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
	if err := os.WriteFile(paths.ConfigFile, []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func goCacheCLIReport(t *testing.T, code int, raw, stderr string) state.GoCacheReport {
	t.Helper()
	var envelope struct {
		Version int              `json:"api_version"`
		OK      bool             `json:"ok"`
		Command string           `json:"command"`
		Report  state.FileReport `json:"report"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(&envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != APIVersion || envelope.Command != "report" || envelope.Report.GoCache == nil {
		t.Fatal("missing one Go cache success reply", code, raw, stderr, err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		t.Fatal("second reply", raw, err)
	}
	r := *envelope.Report.GoCache
	if r.Contract != state.GoCacheContract || r.Rule != state.GoCacheRule || r.Source != "saved_inventory" || r.EntryLimit != state.GoCacheEntryLimit || r.EntriesExamined > r.EntryLimit || len(r.Files) > state.GoCacheFindingLimit || r.ContentVerified || r.CurrentStateVerified || r.RegenerationVerified || r.ApprovalAvailable || r.AutomationEligible || r.Executable || r.EstimatedReclaimableBytes != nil || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || envelope.Report.BuildOutput != nil || envelope.Report.SameSize != nil || envelope.Report.Directory != nil || envelope.Report.Candidates != nil {
		t.Fatal("cache report widened scope or authority", raw)
	}
	count := 0
	for _, diagnostic := range r.Diagnostics {
		count += diagnostic.Count
	}
	if count != r.EntriesExamined || goCacheCLIDiagnostic(r, "selected") != len(r.Files) {
		t.Fatal("diagnostics do not describe actual page", raw)
	}
	for _, file := range r.Files {
		if file.Classification != "review_required" || file.ContentVerified || file.CurrentStateVerified || file.RegenerationVerified || file.ApprovalAvailable || file.AutomationEligible || file.Executable || file.EstimatedReclaimableBytes != nil {
			t.Fatal("cache file gained authority", file)
		}
	}
	for _, text := range []string{`"content_verified":false`, `"current_state_verified":false`, `"regeneration_verified":false`, `"approval_available":false`, `"automation_eligible":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`, `"selected_file_body_requested_bytes":0`, `"selected_file_body_read_bytes":0`} {
		if !strings.Contains(raw, text) {
			t.Fatal("explicit qualification omitted", text, raw)
		}
	}
	return r
}

func goCacheCLIDiagnostic(r state.GoCacheReport, code string) int {
	for _, diagnostic := range r.Diagnostics {
		if diagnostic.Code == code {
			return diagnostic.Count
		}
	}
	return 0
}

func TestGoCacheCLIRecognizedSavedLayoutOfflineAndQualifiedFiles(t *testing.T) {
	f := newGoCacheCLIFixture(t, 3, "Go Ω\t'\x1b fixture")
	// Generated saved uncertainty must remain visible without a source refresh.
	db, err := sql.Open("sqlite", filepath.Join(f.inventory, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE entries SET inode='',observed_at_ns=0 WHERE path=?`, []byte(filepath.Join("00", fmt.Sprintf("%064x-a", 0)))); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--json")
	r := goCacheCLIReport(t, code, raw, stderr)
	if r.Layout.Status != "layout_metadata_recognized" || r.Layout.ConfirmedShards != 256 || len(r.Layout.Markers) != 258 || len(r.Files) != 3 || r.UnknownIdentities != 1 || goCacheCLIDiagnostic(r, "age_not_met") != 1 || goCacheCLIDiagnostic(r, "executable_directory") != 1 {
		t.Fatal("saved layout recognition lost qualifications", raw)
	}
	for _, file := range r.Files {
		if file.LogicalBytes == nil || file.AllocatedBytes == nil || !bytes.HasPrefix(file.PathBytes, []byte(f.root+string(os.PathSeparator))) {
			t.Fatal("file metadata lost exact scope", file)
		}
	}
	code, human, stderr := f.run(context.Background(), "report", "--go-cache", "-d", f.root)
	flat := strings.Join(strings.Fields(human), " ")
	for _, text := range []string{"3 Go build-cache files on this page", fmt.Sprintf("%q", f.root), "256 of 256", "Unknown identities", "unknown", "NOT RECORDED", "Logical size observed", "Allocated size observed", "Too recent / future-dated", "Executable directories unsupported", "not whole-cache totals", "Go performs its own cache trimming", "No source paths, configuration or Go commands", "Full saved layout and file evidence (JSON)", commandPrefix(config.Paths{StateDir: f.base}) + " report --go-cache -d " + shellQuote(f.root) + " --min-age-days 90 --json"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, strings.Join(strings.Fields(text), " ")) {
			t.Fatal("human report lost exact scope or qualifications", text, code, human, stderr)
		}
	}
	if !utf8.ValidString(human) || strings.ContainsRune(human, '\x1b') || strings.Contains(human, "0001-01-01") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("cache report changed bytes, emitted controls or labeled an unknown observation as a date")
	}
	for _, args := range [][]string{{"plan", "--preview", "-d", f.root, r.Files[0].ID, "--json"}, {"ignore", "--preview", "-d", f.root, r.Files[0].ID, "--json"}} {
		code, raw, stderr := f.run(context.Background(), args...)
		buildOutputCLIError(t, code, raw, stderr, args[0], "invalid_arguments", 2)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("cache IDs changed node_modules plan/dismissal history")
	}
	offline := f.root + "-offline"
	if err := os.Rename(f.root, offline); err != nil {
		t.Fatal(err)
	}
	before = hashCLIBytes(t, f.base, offline)
	code, raw, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--json")
	got := goCacheCLIReport(t, code, raw, stderr)
	if !reflect.DeepEqual(got.Layout, r.Layout) || !reflect.DeepEqual(got.Files, r.Files) || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, offline)) {
		t.Fatal("offline report refreshed saved evidence or bytes", raw)
	}
}

func TestGoCacheCLIPaginationAgeAndEarlierReportJSON(t *testing.T) {
	f := newGoCacheCLIFixture(t, 21, "generated cache")
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "--json", "report", "--go-cache", "--directory", f.root, "--min-age-days", "30")
	first := goCacheCLIReport(t, code, raw, stderr)
	if len(first.Files) != 20 || first.NextCursor == "" || first.PageCoverage != "more_saved_entries" {
		t.Fatal("first page lost bound/continuation", raw)
	}
	code, raw, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--min-age-days", "30", "--cursor", first.NextCursor, "--json")
	second := goCacheCLIReport(t, code, raw, stderr)
	if len(second.Files) != 1 || second.NextCursor != "" || second.PageCoverage != "saved_entries_exhausted" {
		t.Fatal("second page lost final file", raw)
	}
	seen := map[string]bool{}
	for _, file := range append(first.Files, second.Files...) {
		if seen[file.ID] {
			t.Fatal("repeated file", file.ID)
		}
		seen[file.ID] = true
	}
	code, human, stderr := f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--min-age-days", "30", "--cursor", first.NextCursor)
	if code != 0 || stderr != "" || !strings.Contains(human, "--cursor "+shellQuote(first.NextCursor)+" --json") || !strings.Contains(human, "End of saved entries.") {
		t.Fatal("same-page evidence command lost age/cursor", code, human, stderr)
	}
	code, raw, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--min-age-days", "36500", "--json")
	aged := goCacheCLIReport(t, code, raw, stderr)
	if len(aged.Files) != 0 || goCacheCLIDiagnostic(aged, "age_not_met") != 22 {
		t.Fatal("age filter did not apply to selected file dates", raw)
	}
	for _, args := range [][]string{{"report", "-d", f.root, "--json"}, {"report", "--candidates", "-d", f.root, "--json"}, {"report", "--same-size", "-d", f.root, "--min-size-bytes", "1", "--json"}, {"report", "--build-output", "-d", f.root, "--json"}} {
		code, raw, stderr := f.run(context.Background(), args...)
		if code != 0 || stderr != "" || !strings.Contains(raw, `"ok":true`) || strings.Contains(raw, `"go_cache":`) {
			t.Fatal("earlier report JSON changed", code, raw, stderr)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("pagination/compatibility reports changed bytes")
	}
}

func TestGoCacheCLIStrictArgumentsJSONValuesAndMissingManualInventory(t *testing.T) {
	f := goCacheCLIFixture{base: filepath.Join(t.TempDir(), "missing state"), root: filepath.Join(t.TempDir(), "missing root")}
	for _, args := range [][]string{
		{"--go-cache"}, {"--go-cache=false", "-d", f.root}, {"--go-cache=bad", "-d", f.root}, {"--go-cache", "--go-cache", "-d", f.root},
		{"--go-cache", "-d", f.root, "--directory", f.root}, {"--go-cache", "-d", ""}, {"--go-cache", "-d", f.root, "--build-output=false"}, {"--build-output", "--go-cache", "-d", f.root},
		{"--go-cache", "-d", f.root, "--candidates=false"}, {"--go-cache", "-d", f.root, "--same-size=false"}, {"--go-cache", "-d", f.root, "--include-dismissed=false"},
		{"--go-cache", "-d", f.root, "--limit", "20"}, {"--go-cache", "-d", f.root, "--min-size-bytes", "1"}, {"--go-cache", "-d", f.root, "extra"},
		{"--go-cache", "-d", f.root, "--directory-size=false"}, {"--go-cache", "-d", "/" + strings.Repeat("x", 4096)}, {"--go-cache", "-d", "/bad\x00root"},
		{"--go-cache", "-d", f.root, "--", "--cursor", "gocache1:90:1"}, {"--go-cache", "-d", f.root, "--min-age-days", "0"}, {"--go-cache", "-d", f.root, "--min-age-days", "36501"},
		{"--go-cache", "-d", f.root, "--min-age-days", "30", "--min-age-days", "30"}, {"--go-cache", "-d", f.root, "--cursor", "gocache1:30:1"},
		{"--go-cache", "-d", f.root, "--cursor", "gocache1:90:01"}, {"--go-cache", "-d", f.root, "--cursor", "gocache1:90:0"},
		{"--go-cache", "-d", f.root, "--cursor", "gocache1:90:9223372036854775808"}, {"--go-cache", "-d", f.root, "--cursor", "gocache1:90:1", "--cursor", "gocache1:90:2"},
	} {
		args = append([]string{"report"}, args...)
		code, raw, stderr := f.run(context.Background(), append([]string{"--json"}, args...)...)
		buildOutputCLIError(t, code, raw, stderr, "report", "invalid_arguments", 2)
		code, raw, stderr = f.run(context.Background(), args...)
		if code != 2 || raw != "" || stderr == "" {
			t.Fatal("human invalid input did not refuse", args, code, raw, stderr)
		}
	}
	for _, option := range []string{"--cursor", "-cursor", "--min-age-days", "-min-age-days"} {
		for _, value := range []string{"--json", "-json"} {
			args := []string{"report", "--go-cache", "-d", f.root, option, value}
			code, raw, stderr := f.run(context.Background(), args...)
			if code != 2 || raw != "" || stderr == "" {
				t.Fatal("flag-like value selected JSON mode", option, value, code, raw, stderr)
			}
			code, raw, stderr = f.run(context.Background(), append([]string{"--json"}, args...)...)
			buildOutputCLIError(t, code, raw, stderr, "report", "invalid_arguments", 2)
		}
	}
	for _, option := range []string{"-d", "--directory", "-directory"} {
		code, raw, stderr := f.run(context.Background(), "--json", "report", "--go-cache", option, "--json")
		buildOutputCLIError(t, code, raw, stderr, "report", "not_found", 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, stderr := f.run(ctx, "report", "--go-cache", "-d", f.root, "--json")
	buildOutputCLIError(t, code, raw, stderr, "report", "canceled", 1)
	if _, err := os.Lstat(f.base); !os.IsNotExist(err) {
		t.Fatal("refused input initialized state", err)
	}
	writer, err := state.OpenWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.SyncRoots(context.Background(), []string{f.root}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base)
	code, raw, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--json")
	buildOutputCLIError(t, code, raw, stderr, "report", "not_found", 1)
	code, _, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root)
	if code != 1 || !strings.Contains(stderr, "Scan it first") || strings.Contains(stderr, "Use rydd init") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base)) {
		t.Fatal("missing exact manual reader used fallback/init", code, stderr)
	}
	if _, err := os.Lstat(filepath.Join(f.base, "manual")); !os.IsNotExist(err) {
		t.Fatal("missing report created manual storage", err)
	}
}

func TestGoCacheCLIUnsupportedLayoutAndOutputCancellation(t *testing.T) {
	f := newGoCacheCLIFixture(t, 1, "generated cache")
	before := hashCLIBytes(t, f.base, f.root)
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", f.base, "report", "--go-cache", "-d", f.root}
		if machine {
			args = append(args, "--json")
		}
		for _, short := range []bool{false, true} {
			var stderr bytes.Buffer
			if code := Run(context.Background(), args, hashFailWriter{short: short}, &stderr); code != 1 || stderr.Len() == 0 {
				t.Fatal("output failure reported success", machine, short, code, stderr.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		out := &hashChoiceCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		code := Run(ctx, args, out, &stderr)
		cancel()
		if code != 1 || !strings.Contains(stderr.String(), "Go build-cache report reply was canceled") {
			t.Fatal("late cancellation reported success", machine, code, out.String(), stderr.String())
		}
		if machine {
			goCacheCLIReport(t, 0, out.String(), "")
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("reply failures changed records/source")
	}
	if err := os.Remove(filepath.Join(f.root, "README")); err != nil {
		t.Fatal(err)
	}
	// The report-only fixture deliberately has malformed configuration. The
	// explicit scan loads configuration; restore its supported missing-config
	// path before recording the changed generated layout.
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(paths.ConfigFile); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--detailed", "--now", "--json")
	if code != 0 || stderr != "" {
		t.Fatal(code, raw, stderr)
	}
	before = hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(context.Background(), "report", "--go-cache", "-d", f.root, "--json")
	r := goCacheCLIReport(t, code, raw, stderr)
	if r.Layout.Status != "layout_unsupported" || len(r.Files) != 0 || r.NextCursor != "" || r.EntriesExamined != 0 {
		t.Fatal("unsupported layout became unqualified empty cache", raw)
	}
	code, human, stderr := f.run(context.Background(), "report", "--go-cache", "-d", f.root)
	if code != 0 || stderr != "" || !strings.Contains(human, "empty result does not prove") || strings.Contains(human, "End of saved entries.") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("unsupported human report overstated coverage", code, human, stderr)
	}
}

func TestGoCacheCLIHelpCapabilitiesAndInvalidBytePaths(t *testing.T) {
	f := newGoCacheCLIFixture(t, 1, "generated cache")
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr := f.run(context.Background(), "capabilities", "--json")
	if code != 0 || stderr != "" || !strings.Contains(raw, `"go_build_cache_reports":true`) || !strings.Contains(raw, `"cleanup":false`) || !strings.Contains(raw, "--go-cache -d ROOT") {
		t.Fatal("capabilities lost Go cache scope", code, raw, stderr)
	}
	code, human, stderr := f.run(context.Background(), "--help")
	if code != 0 || stderr != "" || !strings.Contains(human, "report --go-cache -d ROOT") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("help changed bytes or omitted scope", code, human, stderr)
	}
	if runtime.GOOS != "linux" {
		return
	}
	rawFixture := newGoCacheCLIFixture(t, 1, "cache-\xff")
	code, raw, stderr = rawFixture.run(context.Background(), "report", "--go-cache", "-d", rawFixture.root, "--json")
	r := goCacheCLIReport(t, code, raw, stderr)
	if len(r.Files) != 1 || !bytes.Equal(r.RootPathBytes, []byte(rawFixture.root)) {
		t.Fatal("raw root bytes changed", raw)
	}
	code, human, stderr = rawFixture.run(context.Background(), "report", "--go-cache", "-d", rawFixture.root)
	if code != 0 || stderr != "" || !utf8.ValidString(human) || !strings.Contains(human, fmt.Sprintf("%q", string(r.Files[0].PathBytes))) {
		t.Fatal("invalid bytes escaped raw path authority", code, human, stderr)
	}
}
