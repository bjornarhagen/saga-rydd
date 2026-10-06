package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type sameSizeCLIFile struct {
	state.ReportFile
	Device     string `json:"device"`
	Inode      string `json:"inode"`
	ChangedNS  int64  `json:"ctime_ns"`
	Generation int64  `json:"generation"`
}

type sameSizeCLIBand struct {
	LogicalBytes    int64             `json:"logical_bytes"`
	Files           []sameSizeCLIFile `json:"files"`
	ContinuesBefore bool              `json:"continues_before"`
	ContinuesAfter  bool              `json:"continues_after"`
	KnownObjects    int               `json:"known_objects"`
}

type sameSizeCLIReport struct {
	Source                    string            `json:"source"`
	MinimumBytes              int64             `json:"minimum_bytes"`
	Limit                     int               `json:"limit"`
	EntriesExamined           int               `json:"entries_examined"`
	PageCoverage              string            `json:"page_coverage"`
	NextCursor                string            `json:"next_cursor"`
	ContentVerified           bool              `json:"content_verified"`
	CurrentStateVerified      bool              `json:"current_state_verified"`
	EstimatedReclaimableBytes *int64            `json:"estimated_reclaimable_bytes"`
	Bands                     []sameSizeCLIBand `json:"bands"`
}

type sameSizeCLIEnvelope struct {
	Version int  `json:"api_version"`
	OK      bool `json:"ok"`
	Report  struct {
		SameSize *sameSizeCLIReport `json:"same_size"`
		Files    []state.ReportFile `json:"files"`
	} `json:"report"`
}

type sameSizeCLIFixture struct {
	t                 *testing.T
	base, root, state string
}

func newSameSizeCLIFixture(t *testing.T) sameSizeCLIFixture {
	t.Helper()
	f := sameSizeCLIFixture{t: t, base: filepath.Join(t.TempDir(), "state with 'quote"), root: filepath.Join(t.TempDir(), "offline with 'quote")}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.state = manualState(paths, f.root)
	seedSameSizeCLIInventory(t, f.state, f.root)
	return f
}

func seedSameSizeCLIInventory(t *testing.T, directory, root string) {
	t.Helper()
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	if err := w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
	if err != nil || job == nil {
		t.Fatal(job, err)
	}
	var entries []state.Entry
	for i, file := range []struct {
		name string
		size int64
	}{
		{"large-a", 2 << 20}, {"large-b", 2 << 20}, {"large-c", 2 << 20},
		{"medium-a", 1 << 20}, {"medium-b", 1 << 20}, {"unique", 3 << 20},
		{"empty-a", 0}, {"empty-b", 0},
	} {
		entries = append(entries, state.Entry{Path: []byte(file.name), Kind: "file", Size: file.size, Allocated: file.size, Device: "7", Inode: fmt.Sprint(i + 1), MtimeNS: 1, CtimeNS: 2})
	}
	err = w.CommitScan(ctx, *job, state.ScanBatch{Identity: "fixture-volume", Generation: 1, Complete: true, Directory: state.Entry{Path: []byte("."), Kind: "directory", Device: "fixture-device", Inode: "fixture-root", MtimeNS: 1, CtimeNS: 2}, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func (f sameSizeCLIFixture) run(args ...string) (int, string, string) {
	f.t.Helper()
	var out, errOut bytes.Buffer
	code := Run(context.Background(), append([]string{"--data-dir", f.base}, args...), &out, &errOut)
	return code, out.String(), errOut.String()
}

func (f sameSizeCLIFixture) report(args ...string) (sameSizeCLIReport, string) {
	f.t.Helper()
	command := append([]string{"report", "--same-size", "-d", f.root}, args...)
	command = append(command, "--json")
	code, raw, stderr := f.run(command...)
	var envelope sameSizeCLIEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || stderr != "" || !envelope.OK || envelope.Version != 1 || envelope.Report.SameSize == nil {
		f.t.Fatal(code, raw, stderr, err)
	}
	return *envelope.Report.SameSize, raw
}

func TestSameSizeCLIHumanJSONOfflineAndReadOnly(t *testing.T) {
	f := newSameSizeCLIFixture(t)
	before, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	report, raw := f.report()
	if report.Source != "saved_inventory" || report.MinimumBytes != 1<<20 || report.Limit != 20 || report.ContentVerified || report.CurrentStateVerified || report.EstimatedReclaimableBytes != nil || report.NextCursor != "" || len(report.Bands) != 2 {
		t.Fatal(raw)
	}
	var qualifications struct {
		Report struct {
			SameSize map[string]any `json:"same_size"`
		} `json:"report"`
	}
	if err := json.Unmarshal([]byte(raw), &qualifications); err != nil {
		t.Fatal(err)
	}
	estimate, estimatePresent := qualifications.Report.SameSize["estimated_reclaimable_bytes"]
	if qualifications.Report.SameSize["content_verified"] != false || qualifications.Report.SameSize["current_state_verified"] != false || !estimatePresent || estimate != nil {
		t.Fatal("JSON omits safety qualifications", raw)
	}
	files := map[string]bool{}
	for _, band := range report.Bands {
		if band.ContinuesBefore || band.ContinuesAfter || band.KnownObjects != len(band.Files) || (band.LogicalBytes != 1<<20 && band.LogicalBytes != 2<<20) {
			t.Fatal(band)
		}
		for _, file := range band.Files {
			if files[file.Path] || file.Size != band.LogicalBytes || file.Device != "7" || file.Inode == "" || file.ChangedNS != 2 || file.Generation != 1 || file.ParentPass != "observed_in_completed_parent_pass" || !bytes.Equal(file.PathBytes, []byte(file.Path)) {
				t.Fatal("incorrect saved file evidence", file)
			}
			files[file.Path] = true
		}
	}
	if len(files) != 5 || files[filepath.Join(f.root, "unique")] || files[filepath.Join(f.root, "empty-a")] {
		t.Fatal("unique or below-threshold files became candidates", files)
	}
	code, human, stderr := f.run("report", "--same-size", "-d", f.root)
	if code != 0 || stderr != "" || !strings.Contains(human, "CONTENT NOT CHECKED") || !strings.Contains(strings.ToLower(human), "same-size") {
		t.Fatal(code, human, stderr)
	}
	if _, err := os.Stat(f.root); !os.IsNotExist(err) {
		t.Fatal("offline report touched or required the source", err)
	}
	after, err := os.ReadFile(filepath.Join(f.state, state.Filename))
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("same-size report changed inventory", err)
	}
	if _, err := os.Stat(filepath.Join(f.base, "plans")); !os.IsNotExist(err) {
		t.Fatal("same-size report initialized plan storage", err)
	}
}

func TestSameSizeCLIBoundedPagesAndQuotedContinuation(t *testing.T) {
	f := newSameSizeCLIFixture(t)
	first, _ := f.report("--limit", "2", "--min-size-bytes", "1")
	// The unique 3 MiB row consumes one bounded raw entry but produces no
	// complete singleton band. The remaining boundary singleton is retained.
	if len(first.Bands) != 1 || len(first.Bands[0].Files) != 1 || first.Bands[0].LogicalBytes != 2<<20 || first.Bands[0].ContinuesBefore || !first.Bands[0].ContinuesAfter || first.NextCursor == "" {
		t.Fatal(first)
	}
	code, human, stderr := f.run("report", "--same-size", "-d", f.root, "--limit", "2", "--min-size-bytes", "1")
	if code != 0 || stderr != "" {
		t.Fatal(code, human, stderr)
	}
	for _, want := range []string{"--same-size", "--data-dir " + shellQuote(f.base), "-d " + shellQuote(f.root), "--min-size-bytes 1", "--limit 2", "--cursor " + shellQuote(first.NextCursor)} {
		if !strings.Contains(human, want) {
			t.Fatal("continuation loses scope, filter, bound or safe quoting", want, human)
		}
	}
	seen := map[string]bool{}
	page := first
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("pagination did not finish")
		}
		count := 0
		for _, band := range page.Bands {
			for _, file := range band.Files {
				if seen[file.Path] {
					t.Fatal("candidate repeated across pages", file.Path)
				}
				seen[file.Path] = true
				count++
			}
		}
		if count > 2 || page.EntriesExamined > 2 || page.ContentVerified || page.CurrentStateVerified {
			t.Fatal("page exceeded its bounds or verified contents", page)
		}
		if page.NextCursor == "" {
			break
		}
		page, _ = f.report("--limit", "2", "--min-size-bytes", "1", "--cursor", page.NextCursor)
		if pages == 0 && (len(page.Bands) == 0 || !page.Bands[0].ContinuesBefore || len(page.Bands[0].Files) != 2) {
			t.Fatal("boundary singleton lost its continuation evidence", page)
		}
	}
	if len(seen) != 5 {
		t.Fatal("paginated result lost saved candidates", seen)
	}
	filtered, _ := f.report("--min-size-bytes", "2097152", "--limit", "200")
	if len(filtered.Bands) != 1 || filtered.Bands[0].LogicalBytes != 2<<20 || len(filtered.Bands[0].Files) != 3 {
		t.Fatal("minimum-size filtering failed", filtered)
	}
}

func TestSameSizeCLIArgumentsCursorAndCapabilities(t *testing.T) {
	f := newSameSizeCLIFixture(t)
	for _, args := range [][]string{
		{"report", "--same-size", "--candidates", "--json"},
		{"report", "--same-size=false", "--candidates", "--json"},
		{"report", "--same-size", "--candidates=false", "--json"},
		{"report", "--min-size-bytes", "1", "--json"},
		{"report", "--same-size=false", "--min-size-bytes", "1", "--json"},
		{"report", "--same-size", "--min-age-days", "30", "--json"},
		{"report", "--same-size", "--min-size-bytes", "-1", "--json"},
		{"report", "--same-size", "--min-size-bytes", "0", "--json"},
		{"report", "--same-size", "--min-size-bytes", "abc", "--json"},
		{"report", "--same-size", "--min-size-bytes", "9223372036854775808", "--json"},
		{"report", "--same-size", "--limit", "0", "--json"},
		{"report", "--same-size", "--limit", "201", "--json"},
		{"report", "--same-size", "--limit", "abc", "--json"},
		{"report", "--same-size", "-d", f.root, "--directory", f.root, "--json"},
		{"report", "--same-size", "-d", "", "--json"},
		{"report", "--same-size", "extra", "--json"},
		{"report", "--same-size", "-d", f.root, "--cursor", "bad", "--json"},
		{"report", "--same-size", "-d", f.root, "--cursor", strings.Repeat("x", 4097), "--json"},
	} {
		code, output, stderr := f.run(args...)
		if code != 2 || stderr != "" || !strings.Contains(output, `"invalid_arguments"`) {
			t.Fatal(args, code, output, stderr)
		}
	}
	first, _ := f.report("--limit", "1")
	code, output, stderr := f.run("report", "--same-size", "-d", f.root, "--min-size-bytes", "2097152", "--cursor", first.NextCursor, "--json")
	if code != 2 || stderr != "" || !strings.Contains(output, `"invalid_arguments"`) {
		t.Fatal("cursor accepted a changed minimum-size filter", code, output, stderr)
	}
	code, raw, stderr := f.run("capabilities", "--json")
	var capabilities struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || stderr != "" || !capabilities.Features["same_size_candidates"] || capabilities.Features["duplicates"] || capabilities.Features["cleanup"] {
		t.Fatal(code, raw, stderr, err)
	}
}

func TestSameSizeCLIManualScopeCannotFallBackOrInitialize(t *testing.T) {
	f := newSameSizeCLIFixture(t)
	seedSameSizeCLIInventory(t, f.base, filepath.Join(t.TempDir(), "configured-offline"))
	code, raw, stderr := f.run("report", "--limit", "1", "--json")
	var ordinary sameSizeCLIEnvelope
	if err := json.Unmarshal([]byte(raw), &ordinary); err != nil || code != 0 || stderr != "" || ordinary.Report.SameSize != nil || len(ordinary.Report.Files) != 1 || ordinary.Report.Files[0].Size != 3<<20 {
		t.Fatal("ordinary largest-file pagination changed", code, raw, stderr, err)
	}
	code, raw, stderr = f.run("report", "--same-size", "--json")
	var configured sameSizeCLIEnvelope
	if err := json.Unmarshal([]byte(raw), &configured); err != nil || code != 0 || stderr != "" || configured.Report.SameSize == nil || len(configured.Report.SameSize.Bands) != 2 {
		t.Fatal("unscoped report did not use configured inventory", code, raw, stderr, err)
	}
	missingRoot := filepath.Join(t.TempDir(), "not-manually-scanned")
	code, output, stderr := f.run("report", "--same-size", "-d", missingRoot, "--json")
	if code == 0 || stderr != "" || strings.Contains(output, `"ok":true`) {
		t.Fatal("same-size scope fell back to configured inventory", code, output, stderr)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manualState(paths, missingRoot)); !os.IsNotExist(err) {
		t.Fatal("missing scoped report initialized manual state", err)
	}
	base := filepath.Join(t.TempDir(), "new-state")
	var out, errOut bytes.Buffer
	code = Run(context.Background(), []string{"--data-dir", base, "report", "--same-size", "--json"}, &out, &errOut)
	if code == 0 || errOut.Len() != 0 {
		t.Fatal(code, out.String(), errOut.String())
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatal("same-size report initialized missing state", err)
	}
}
