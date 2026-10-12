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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type sameSizeCargoCLIFixture struct{ base, root, inventory string }

// The only source trees created here are generated fixtures. The manifests are
// deliberately invalid and the large files have distinct final bytes.
func newSameSizeCargoCLIFixture(t *testing.T) sameSizeCargoCLIFixture {
	t.Helper()
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := sameSizeCargoCLIFixture{base: filepath.Join(temp, "private state"), root: filepath.Join(temp, "Cargo Ω with 'quote")}
	for _, profile := range []string{"debug", "release"} {
		project := profile + "-project"
		addBuildOutputCLIProject(t, f.root, project, profile, false, false)
		for index, name := range []string{"manual-unique-a.bin", "manual-unique-b.bin"} {
			writeSameSizeCargoCLIFile(t, filepath.Join(f.root, project, "target", profile, "deps", name), 3<<20, byte('a'+index))
		}
	}
	near := filepath.Join(f.root, "near-miss", "target", "debug", "deps")
	for index, name := range []string{"keep-a.bin", "keep-b.bin"} {
		writeSameSizeCargoCLIFile(t, filepath.Join(near, name), 2<<20, byte('c'+index))
	}
	for index, name := range []string{"ordinary-a.bin", "ordinary-b.bin"} {
		writeSameSizeCargoCLIFile(t, filepath.Join(f.root, name), 1<<20, byte('e'+index))
	}
	// A future saved modification date must not become an age admission rule.
	if err := filepath.WalkDir(f.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		stamp := time.Date(2035, 1, 2, 3, 4, 5, 0, time.UTC)
		return os.Chtimes(path, stamp, stamp)
	}); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--detailed", "--now", "--json")
	if code != 0 || stderr != "" {
		t.Fatal("generated scan failed", code, raw, stderr)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.inventory = manualState(paths, f.root)
	if err := os.WriteFile(paths.ConfigFile, []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+"-offline"); err != nil {
		t.Fatal(err)
	}
	return f
}

func writeSameSizeCargoCLIFile(t *testing.T, path string, size int64, tail byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(size-1, io.SeekStart); err != nil {
		f.Close()
		t.Fatal(err)
	}
	n, writeErr := f.Write([]byte{tail})
	closeErr := f.Close()
	if n != 1 || writeErr != nil || closeErr != nil {
		t.Fatal(n, writeErr, closeErr)
	}
}

func (f sameSizeCargoCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	var out, diagnostic bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &diagnostic)
	return code, out.String(), diagnostic.String()
}

func sameSizeCargoCLIReply(t *testing.T, code int, raw, stderr string, version int, selected bool) reportResult {
	t.Helper()
	var reply struct {
		Version int          `json:"api_version"`
		OK      bool         `json:"ok"`
		Command string       `json:"command"`
		Report  reportResult `json:"report"`
	}
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(&reply); err != nil || code != 0 || stderr != "" || !reply.OK || reply.Version != version || reply.Command != "report" || reply.Report.SameSize == nil {
		t.Fatal("same-size reply contract", code, raw, stderr, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatal("trailing same-size reply", raw, err)
	}
	r := reply.Report.SameSize
	if r.Source != "saved_inventory" || r.ContentVerified || r.CurrentStateVerified || r.EstimatedReclaimableBytes != nil || (r.CargoTargetExclusion != nil) != selected {
		t.Fatal("saved filter gained authority", raw)
	}
	if selected && (r.CargoTargetExclusion.Policy != state.SameSizeCargoPolicy || r.CargoTargetExclusion.Recognition != "saved_layout_metadata_only") {
		t.Fatal("filter receipt scope changed", raw)
	}
	return reply.Report
}

func (f sameSizeCargoCLIFixture) page(t *testing.T, selected bool, options ...string) (reportResult, string) {
	t.Helper()
	args := []string{"report", "--same-size", "-d", f.root}
	version := 1
	if selected {
		args = append(args, "--exclude-cargo-target")
		version = 2
	}
	args = append(args, options...)
	args = append(args, "--json")
	code, raw, stderr := f.run(context.Background(), args...)
	return sameSizeCargoCLIReply(t, code, raw, stderr, version, selected), raw
}

func sameSizeCargoCLIInventoryHash(t *testing.T, directory string) [32]byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(directory, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(body)
}

func TestSameSizeCargoCLIArgumentsBeforeStorage(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"numeric_true", []string{"--same-size", "--exclude-cargo-target=1"}},
		{"short_true", []string{"--same-size", "--exclude-cargo-target=t"}},
		{"uppercase", []string{"--same-size", "--exclude-cargo-target=TRUE"}},
		{"numeric_false", []string{"--same-size", "--exclude-cargo-target=0"}},
		{"empty", []string{"--same-size", "--exclude-cargo-target="}},
		{"duplicate", []string{"--same-size", "--exclude-cargo-target", "--exclude-cargo-target"}},
		{"duplicate_false", []string{"--same-size", "--exclude-cargo-target=false", "--exclude-cargo-target=false"}},
		{"mixed_values", []string{"--same-size", "--exclude-cargo-target=true", "--exclude-cargo-target=false"}},
		{"separate_value", []string{"--same-size", "--exclude-cargo-target", "true"}},
		{"after_separator", []string{"--same-size", "--", "--exclude-cargo-target"}},
		{"missing_mode", []string{"--exclude-cargo-target"}},
		{"false_missing_mode", []string{"--exclude-cargo-target=false"}},
		{"same_size_false", []string{"--same-size=false", "--exclude-cargo-target=false"}},
		{"candidates_false", []string{"--same-size", "--candidates=false", "--exclude-cargo-target"}},
		{"page_summary_false", []string{"--same-size", "--page-size-summary=false", "--exclude-cargo-target"}},
		{"dismissed_false", []string{"--same-size", "--include-dismissed=false", "--exclude-cargo-target"}},
		{"age", []string{"--same-size", "--min-age-days", "90", "--exclude-cargo-target"}},
		{"cargo_report", []string{"--build-output", "-d", "/generated-unused-root", "--exclude-cargo-target=false"}},
		{"go_report", []string{"--go-cache", "-d", "/generated-unused-root", "--exclude-cargo-target"}},
	}
	for _, tc := range cases {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/machine_%t", tc.name, machine), func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "absent-state")
				args := []string{"--data-dir", base}
				if machine {
					args = append(args, "--json")
				}
				args = append(args, "report")
				args = append(args, tc.args...)
				var out, diagnostic bytes.Buffer
				code := Run(context.Background(), args, &out, &diagnostic)
				if machine {
					buildOutputCLIError(t, code, out.String(), diagnostic.String(), "report", "invalid_arguments", 2)
				} else if code != 2 || out.Len() != 0 || diagnostic.Len() == 0 {
					t.Fatal("invalid selector reached evidence", code, out.String(), diagnostic.String())
				}
				if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("invalid selector initialized storage", err)
				}
			})
		}
	}
}

func TestSameSizeCargoCLIOfflineScopesAndNoAge(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	before := sameSizeCargoCLIInventoryHash(t, f.inventory)
	manual, raw := f.page(t, true)
	c := manual.SameSize.CargoTargetExclusion
	if c.ExcludedRows != 4 || c.RetainedUnrecognizedRows != 2 || c.RetainedUnknownRows != 0 || len(manual.SameSize.Bands) != 2 {
		t.Fatal("fresh/future saved layouts or near misses changed", raw)
	}
	for _, band := range manual.SameSize.Bands {
		for _, file := range band.Files {
			if strings.Contains(file.Path, "manual-unique-") {
				t.Fatal("supported target row stayed visible", file)
			}
		}
	}
	// Copy the closed generated inventory to the configured store. Its root is
	// still offline and the deliberately malformed configuration is not loaded.
	database, err := os.ReadFile(filepath.Join(f.inventory, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.base, state.Filename), database, 0600); err != nil {
		t.Fatal(err)
	}
	configuredBefore := sameSizeCargoCLIInventoryHash(t, f.base)
	code, configuredJSON, stderr := f.run(context.Background(), "report", "--same-size", "--exclude-cargo-target=true", "--json")
	configured := sameSizeCargoCLIReply(t, code, configuredJSON, stderr, 2, true)
	manual.GeneratedAt, configured.GeneratedAt = time.Time{}, time.Time{}
	manual.SameSize.GeneratedAt, configured.SameSize.GeneratedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(manual, configured) {
		t.Fatal("configured and manual saved scopes diverged", raw, configuredJSON)
	}
	if _, err := os.Lstat(f.root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("report reopened offline sources", err)
	}
	if before != sameSizeCargoCLIInventoryHash(t, f.inventory) || configuredBefore != sameSizeCargoCLIInventoryHash(t, f.base) {
		t.Fatal("report changed saved inventory")
	}
	for _, name := range []string{"hashes", "plans"} {
		if _, err := os.Lstat(filepath.Join(f.base, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("report created action/content storage", name, err)
		}
	}
}

func TestSameSizeCargoCLIFalseParityAndVersions(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	plain, _ := f.page(t, false, "--limit", "1")
	disabled, raw := f.page(t, false, "--limit", "1", "--exclude-cargo-target=false")
	plain.GeneratedAt, disabled.GeneratedAt = time.Time{}, time.Time{}
	plain.SameSize.GeneratedAt, disabled.SameSize.GeneratedAt = time.Time{}, time.Time{}
	left, _ := json.Marshal(plain)
	right, _ := json.Marshal(disabled)
	if !bytes.Equal(left, right) || strings.Contains(raw, "cargo_target_exclusion") {
		t.Fatal("false changed the legacy V1 report", string(left), string(right), raw)
	}
	code, human, stderr := f.run(context.Background(), "report", "--same-size", "-d", f.root, "--limit", "1")
	code2, disabledHuman, stderr2 := f.run(context.Background(), "report", "--same-size", "-d", f.root, "--limit", "1", "--exclude-cargo-target=false")
	if code != 0 || code2 != 0 || stderr != "" || stderr2 != "" || human != disabledHuman {
		t.Fatal("false changed human bytes", human, disabledHuman, stderr, stderr2)
	}
	for _, flag := range []string{"--exclude-cargo-target", "--exclude-cargo-target=true"} {
		t.Run(flag, func(t *testing.T) {
			code, raw, stderr := f.run(context.Background(), "report", "--same-size=true", "-d", f.root, flag, "--json")
			sameSizeCargoCLIReply(t, code, raw, stderr, 2, true)
		})
	}
}

func TestSameSizeCargoCLIEmptyPagesAndPolicyCursors(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	plain, _ := f.page(t, false, "--limit", "1")
	filtered, _ := f.page(t, true, "--limit", "1")
	if filtered.SameSize.NextCursor == "" || len(filtered.SameSize.Bands) != 0 || filtered.SameSize.EntriesExamined != 1 || filtered.SameSize.CargoTargetExclusion.ExcludedRows != 1 {
		t.Fatal("hidden raw row refilled its page", filtered)
	}
	for _, tc := range []struct {
		name     string
		selected bool
		cursor   string
		minimum  string
	}{
		{"V1_into_filtered", true, plain.SameSize.NextCursor, "1048576"}, {"V2_into_default", false, filtered.SameSize.NextCursor, "1048576"}, {"V2_changed_minimum", true, filtered.SameSize.NextCursor, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"report", "--same-size", "-d", f.root, "--limit", "1", "--cursor", tc.cursor, "--min-size-bytes", tc.minimum, "--json"}
			if tc.selected {
				args = append(args, "--exclude-cargo-target")
			}
			code, raw, stderr := f.run(context.Background(), args...)
			buildOutputCLIError(t, code, raw, stderr, "report", "invalid_arguments", 2)
		})
	}
	seen := map[int64]bool{}
	visible := 0
	page := filtered
	for count := 0; ; count++ {
		if count > 9 {
			t.Fatal("filtered pagination did not finish")
		}
		if page.SameSize.EntriesExamined > 1 {
			t.Fatal("raw bound grew", page)
		}
		for _, band := range page.SameSize.Bands {
			for _, file := range band.Files {
				if seen[file.ID] {
					t.Fatal("visible ID repeated", file.ID)
				}
				seen[file.ID] = true
				visible++
			}
		}
		if page.SameSize.NextCursor == "" {
			break
		}
		page, _ = f.page(t, true, "--limit", "1", "--cursor", page.SameSize.NextCursor)
	}
	if visible != 4 {
		t.Fatal("continuation lost retained near-miss or ordinary rows", visible)
	}
	code, human, stderr := f.run(context.Background(), "report", "--same-size", "--exclude-cargo-target", "-d", f.root, "--limit", "1")
	if code != 0 || stderr != "" {
		t.Fatal(code, human, stderr)
	}
	for _, want := range []string{"CARGO FILTER - SAVED LAYOUT ONLY", "Hidden Cargo target rows", "Retained unrecognized rows", "Retained unknown rows", "Retained ancestor-limit rows", "no age cutoff", "unique or manually edited files", "--exclude-cargo-target", "--data-dir " + shellQuote(f.base), "-d " + shellQuote(f.root), "--cursor " + shellQuote(filtered.SameSize.NextCursor)} {
		if !strings.Contains(human, want) {
			t.Fatal("human warning or continuation missing", want, human)
		}
	}
}

func TestSameSizeCargoCLIMissingScopeCannotFallback(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	database, err := os.ReadFile(filepath.Join(f.inventory, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.base, state.Filename), database, 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(filepath.Dir(f.root), "never-scanned")
	code, raw, stderr := f.run(context.Background(), "report", "--same-size", "--exclude-cargo-target", "-d", missing, "--json")
	if code == 0 || stderr != "" || strings.Contains(raw, `"ok":true`) {
		t.Fatal("missing manual scope fell back", code, raw, stderr)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(manualState(paths, missing)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing manual inventory initialized", err)
	}
	base := filepath.Join(t.TempDir(), "missing-instance")
	var out, diagnostic bytes.Buffer
	code = Run(context.Background(), []string{"--data-dir", base, "report", "--same-size", "--exclude-cargo-target", "--json"}, &out, &diagnostic)
	if code == 0 || diagnostic.Len() != 0 {
		t.Fatal(code, out.String(), diagnostic.String())
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("filtered report initialized state", err)
	}
}

type sameSizeCargoCLIReader struct {
	store                             *state.Store
	result                            state.SameSizeReport
	readErr, closeErr                 error
	cancelAfterRead, cancelAfterClose context.CancelFunc
	reads, closes                     int
	options                           state.SameSizeOptions
}

func (r *sameSizeCargoCLIReader) SameSizeCandidatesWithOptions(ctx context.Context, limit int, cursor string, minimum int64, options state.SameSizeOptions) (state.SameSizeReport, error) {
	r.reads++
	r.options = options
	result, err := r.result, r.readErr
	if r.store != nil {
		result, err = r.store.SameSizeCandidatesWithOptions(ctx, limit, cursor, minimum, options)
	}
	if r.cancelAfterRead != nil {
		r.cancelAfterRead()
	}
	return result, err
}
func (r *sameSizeCargoCLIReader) Close() error {
	r.closes++
	var err error
	if r.store != nil {
		err = r.store.Close()
	}
	if r.cancelAfterClose != nil {
		r.cancelAfterClose()
	}
	return errors.Join(err, r.closeErr)
}

func TestSameSizeCargoCLIReadCloseAndCancellation(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	readCause, closeCause := errors.New("generated filter read failure"), errors.New("generated filter close failure")
	for _, name := range []string{"success", "read_and_close", "close", "cancel_after_read", "cancel_after_close", "invalid_cursor", "missing_summary", "wrong_policy", "wrong_recognition"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &sameSizeCargoCLIReader{}
			cursor := ""
			if name == "success" || name == "close" || strings.HasPrefix(name, "cancel_") || name == "invalid_cursor" {
				var err error
				reader.store, err = state.OpenReader(ctx, f.inventory)
				if err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "read_and_close":
				reader.readErr, reader.closeErr = readCause, closeCause
			case "close":
				reader.closeErr = closeCause
			case "cancel_after_read":
				reader.cancelAfterRead = cancel
			case "cancel_after_close":
				reader.cancelAfterClose = cancel
			case "invalid_cursor":
				cursor = "not-a-filter-cursor"
			case "wrong_policy", "wrong_recognition":
				reader.result.CargoTargetExclusion = &state.SameSizeCargoTargetExclusion{Policy: state.SameSizeCargoPolicy, Recognition: "saved_layout_metadata_only"}
				if name == "wrong_policy" {
					reader.result.CargoTargetExclusion.Policy = "other"
				} else {
					reader.result.CargoTargetExclusion.Recognition = "current"
				}
			}
			result, err := readCargoSameSizePage(ctx, reader, 20, cursor, 1<<20, "generated report command")
			if reader.reads != 1 || reader.closes != 1 || !reader.options.ExcludeCargoDefaultTarget {
				t.Fatal("filtered reader changed ownership or selection", reader)
			}
			if name == "success" {
				if err != nil || result.SameSize == nil || result.SameSize.CargoTargetExclusion == nil {
					t.Fatal(result, err)
				}
				return
			}
			if err == nil || !reflect.DeepEqual(result, reportResult{}) {
				t.Fatal("failure published report evidence", name, result, err)
			}
			if name == "read_and_close" && (!errors.Is(err, readCause) || !errors.Is(err, closeCause)) || name == "close" && !errors.Is(err, closeCause) || strings.HasPrefix(name, "cancel_") && !errors.Is(err, context.Canceled) {
				t.Fatal("reader lost observed cause", name, err)
			}
			if name == "invalid_cursor" {
				var usage usageError
				if !errors.As(err, &usage) || !errors.Is(err, state.ErrReportCursor) {
					t.Fatal("cursor refusal lost usage contract", err)
				}
			}
		})
	}
}

func TestSameSizeCargoCLIOutputFailuresAndCapabilities(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	before := sameSizeCargoCLIInventoryHash(t, f.inventory)
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("writer_machine_%t_short_%t", machine, short), func(t *testing.T) {
				args := []string{"--data-dir", f.base, "report", "--same-size", "--exclude-cargo-target", "-d", f.root}
				if machine {
					args = append(args, "--json")
				}
				var diagnostic bytes.Buffer
				if code := Run(context.Background(), args, hashFailWriter{short: short}, &diagnostic); code != 1 || diagnostic.Len() == 0 {
					t.Fatal("broken output acknowledged filter", code, diagnostic.String())
				}
			})
		}
		t.Run(fmt.Sprintf("late_cancel_machine_%t", machine), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := &hashChoiceCancelWriter{cancel: cancel}
			args := []string{"--data-dir", f.base, "report", "--same-size", "--exclude-cargo-target", "-d", f.root}
			if machine {
				args = append(args, "--json")
			}
			var diagnostic bytes.Buffer
			code := Run(ctx, args, out, &diagnostic)
			if code != 1 || !strings.Contains(diagnostic.String(), context.Canceled.Error()) || !strings.Contains(diagnostic.String(), "Filtered same-size") {
				t.Fatal("late cancellation acknowledged filter", code, out.String(), diagnostic.String())
			}
			if machine {
				var envelope map[string]any
				d := json.NewDecoder(bytes.NewReader(out.Bytes()))
				if err := d.Decode(&envelope); err != nil || envelope["ok"] != true || envelope["api_version"] != float64(2) {
					t.Fatal("late veto lost the written V2 observation", out.String(), err)
				}
				if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
					t.Fatal("late veto emitted another JSON reply", err)
				}
			}
		})
	}
	for _, option := range []string{"", "--exclude-cargo-target=false"} {
		t.Run("legacy_human"+option, func(t *testing.T) {
			args := []string{"--data-dir", f.base, "report", "--same-size", "-d", f.root}
			if option != "" {
				args = append(args, option)
			}
			var diagnostic bytes.Buffer
			if code := Run(context.Background(), args, hashFailWriter{}, &diagnostic); code != 0 || diagnostic.Len() != 0 {
				t.Fatal("selected guard changed legacy human path", code, diagnostic.String())
			}
		})
	}
	code, raw, stderr := f.run(context.Background(), "capabilities", "--json")
	var caps struct {
		Features map[string]bool `json:"features"`
		Contract struct {
			Policy   string `json:"policy"`
			Selected int    `json:"selected_success_api_version"`
			Default  int    `json:"default_success_api_version"`
			Errors   int    `json:"errors_api_version"`
			Age      bool   `json:"age_filter"`
			Hash     bool   `json:"hash_selection_supported"`
			Source   bool   `json:"source_content_read"`
			Cleanup  bool   `json:"cleanup_authority"`
		} `json:"same_size_cargo_filter_contract"`
	}
	if err := json.Unmarshal([]byte(raw), &caps); err != nil || code != 0 || stderr != "" || !caps.Features["same_size_cargo_filter"] || caps.Contract.Policy != state.SameSizeCargoPolicy || caps.Contract.Selected != 2 || caps.Contract.Default != 1 || caps.Contract.Errors != 1 || caps.Contract.Age || caps.Contract.Hash || caps.Contract.Source || caps.Contract.Cleanup {
		t.Fatal("capabilities altered filter authority", code, raw, stderr, err)
	}
	if before != sameSizeCargoCLIInventoryHash(t, f.inventory) {
		t.Fatal("output fixtures changed inventory")
	}
}

func TestSameSizeCargoCLIHashSelectionRefusesV2(t *testing.T) {
	f := newSameSizeCargoCLIFixture(t)
	report, raw := f.page(t, true)
	if len(report.SameSize.Bands) == 0 || len(report.SameSize.Bands[0].Files) == 0 {
		t.Fatal("no retained file to select")
	}
	path := filepath.Join(filepath.Dir(f.base), "filtered-report.json")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	id := strconv.FormatInt(report.SameSize.Bands[0].Files[0].ID, 10)
	code, reply, stderr := f.run(context.Background(), "hash", "--select", "-d", f.root, "--from", path, "--json", id)
	buildOutputCLIError(t, code, reply, stderr, "hash", "invalid_arguments", 2)
	if _, err := os.Lstat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("V2 refusal initialized hash storage", err)
	}
}
