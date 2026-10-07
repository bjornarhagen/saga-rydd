package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type hashProposalCLIFixture struct {
	t                          *testing.T
	base, root, source, report string
	page                       state.SameSizeReport
	input                      []byte
}

func newHashProposalCLIFixture(t *testing.T) *hashProposalCLIFixture {
	return newHashProposalCLIFixtureWithFilename(t, "quote\"雪")
}

func newHashProposalCLIFixtureWithFilename(t *testing.T, filename string) *hashProposalCLIFixture {
	t.Helper()
	canonical := func() string {
		path, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	f := &hashProposalCLIFixture{t: t, base: filepath.Join(canonical(), "private state"), root: canonical(), report: filepath.Join(canonical(), "saved page.json")}
	parent := filepath.Join(f.root, "nested\nfolder")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filename, "peer"} {
		if err := os.WriteFile(filepath.Join(parent, name), bytes.Repeat([]byte("generated"), 17), 0600); err != nil {
			if runtime.GOOS == "darwin" && !utf8.ValidString(name) && (errors.Is(err, unix.EILSEQ) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EPERM)) {
				t.Skip("macOS filesystem rejects this invalid UTF-8 fixture filename")
			}
			t.Fatal(err)
		}
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.source = manualState(paths, f.root)
	code, out, stderr := f.run(context.Background(), "scan", "-d", f.root, "--now", "--detailed", "--json")
	if code != 0 || stderr != "" {
		t.Fatal(code, out, stderr)
	}
	code, out, stderr = f.run(context.Background(), "report", "--same-size", "-d", f.root, "--min-size-bytes", "1", "--json")
	var envelope hashReportEnvelope
	if err := json.Unmarshal([]byte(out), &envelope); err != nil || code != 0 || stderr != "" || envelope.Report.SameSize == nil {
		t.Fatal(code, out, stderr, err)
	}
	f.page = *envelope.Report.SameSize
	if len(f.page.Bands) != 1 || len(f.page.Bands[0].Files) != 2 {
		t.Fatal(out)
	}
	f.input = []byte(out)
	if err := os.WriteFile(f.report, f.input, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ConfigFile, []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *hashProposalCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	f.t.Helper()
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func (f *hashProposalCLIFixture) selectArgs() []string {
	return []string{"hash", "--select", "-d", f.root, "--from", f.report, strconv.FormatInt(f.page.Bands[0].Files[1].ID, 10), strconv.FormatInt(f.page.Bands[0].Files[0].ID, 10), "--json"}
}

func hashCLIProposal(t *testing.T, code int, raw, stderr string) inventory.HashProposal {
	t.Helper()
	var r struct {
		Version int                    `json:"api_version"`
		OK      bool                   `json:"ok"`
		Command string                 `json:"command"`
		Hash    inventory.HashProposal `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &r); err != nil || code != 0 || stderr != "" || r.Version != APIVersion || !r.OK || r.Command != "hash" {
		t.Fatal(code, raw, stderr, err)
	}
	p := r.Hash
	if p.Source != "saved_hash_selection" || p.Contract != inventory.FileHashContract || !validHashSelectionID(p.StoreID) || !validHashSelectionID(p.SelectionID) || p.ProvenanceVerified || p.ContentVerified || p.CurrentStateVerified || p.DuplicatesVerified || p.Executable || p.EstimatedReclaimableBytes != nil {
		t.Fatal("stronger proposal claim", raw)
	}
	for _, forbidden := range []string{`"checkpoint"`, `"state"`, `"durable_offset"`, `"sha256"`} {
		if strings.Contains(raw, forbidden) {
			t.Fatal("proposal exposed continuation state", raw)
		}
	}
	return p
}

func hashProposalFailure(t *testing.T, code int, raw, stderr, want string, exit int) {
	t.Helper()
	var envelope struct {
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || code != exit || stderr != "" || envelope.OK || envelope.Command != "hash" || envelope.Error.Code != want {
		t.Fatal(code, raw, stderr, want, err)
	}
	if strings.Contains(raw, `"hash":`) {
		t.Fatal("failure exposed partially trusted evidence", raw)
	}
}

func TestHashCLISelectShowExactOfflineAndNoContentReads(t *testing.T) {
	ctx := context.Background()
	f := newHashProposalCLIFixture(t)
	before := hashCLIBytes(t, f.root, f.source)
	// The selected source is already unavailable during selection. Only the
	// explicitly named report and existing saved inventory may be opened.
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(ctx, f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	if p.SourceLocator == nil || p.SourceLocator.Kind != "manual_inventory_v1" || !bytes.Equal(p.SourceLocator.RootPathBytes, []byte(f.root)) || p.SourceLocator.InventoryKey != fmt.Sprintf("%x", sha256.Sum256([]byte(f.root))) || p.InventoryID != f.page.InventoryID || len(p.Targets) != 2 {
		t.Fatal(raw)
	}
	for i, target := range p.Targets {
		expected := f.page.Bands[0].Files[1-i]
		// Display Path can lose invalid UTF-8 in JSON; all exact saved fields
		// including authoritative raw bytes must still match the reviewed row.
		expected.Path = target.File.Path
		if !reflect.DeepEqual(expected, target.File) || !bytes.Equal(target.Root.PathBytes, []byte(f.root)) || len(target.Ancestors) != 2 || string(target.Ancestors[0].Path) != "." || string(target.Ancestors[1].Path) != "nested\nfolder" {
			t.Fatal(target, expected)
		}
	}
	if err := os.Rename(f.root+".offline", f.root); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("selection changed source or inventory bytes")
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	snapshot := hashCLIReport(t, code, raw, stderr)
	if snapshot.SelectionID != p.SelectionID || snapshot.Budget != nil || len(snapshot.Work) != 2 {
		t.Fatal(raw)
	}
	for _, work := range snapshot.Work {
		if work.Status != "pending" || work.Sequence != 0 || work.DurableOffset != 0 || work.LatestAttempt != nil || work.SHA256 != "" {
			t.Fatal("selection started reads", raw)
		}
	}
	// Both display and matching retry preserve the same immutable proposal.
	code, raw, stderr = f.run(ctx, f.selectArgs()...)
	if retry := hashCLIProposal(t, code, raw, stderr); !reflect.DeepEqual(p, retry) {
		t.Fatal("matching retry changed proposal", raw)
	}
	before = hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(ctx, "hash", "--show", p.SelectionID, "--json")
	if shown := hashCLIProposal(t, code, raw, stderr); !reflect.DeepEqual(p, shown) {
		t.Fatal("saved proposal changed", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("show changed saved/source bytes")
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.report); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr = f.run(ctx, "hash", "--show", p.SelectionID, "--json")
	if shown := hashCLIProposal(t, code, raw, stderr); !reflect.DeepEqual(p, shown) {
		t.Fatal("show required sources", raw)
	}
	code, human, stderr := f.run(ctx, "hash", "--show", p.SelectionID)
	flat := strings.Join(strings.Fields(human), " ")
	if code != 0 || stderr != "" || !strings.Contains(flat, "UNAPPROVED HASH SELECTION") || !strings.Contains(flat, "No selected-source bytes were read") || !strings.Contains(flat, "not estimates of space you can free") || !strings.Contains(human, "Selection: "+p.SelectionID) || !strings.Contains(human, fmt.Sprintf("%q", string(p.Targets[0].File.PathBytes))) || !strings.Contains(human, `"nested\nfolder"`) || !strings.Contains(human, "Saved file ID") {
		t.Fatal(code, human, stderr)
	}
	if _, err := os.Stat(f.source); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("show initialized unavailable inventory", err)
	}
}

func TestHashCLIInvalidArgumentsBeforeStorage(t *testing.T) {
	fullID := strings.Repeat("a", 64)
	cases := [][]string{
		{}, {"--select"}, {"--show", "short"}, {"--show", strings.ToUpper(fullID)}, {"--show", fullID, "1"}, {"--show", fullID, "--show", fullID}, {"--show", fullID, "-d", "/missing"}, {"--show", fullID, "--from", "/missing"}, {"--show", fullID, "--select"}, {"--select=false", "-d", "/missing", "--from", "/missing", "1"},
		{"--select", "--select", "-d", "/missing", "--from", "/missing", "1"}, {"--select", "-d", "/missing", "--directory", "/missing", "--from", "/missing", "1"}, {"--select", "-d", "/missing", "--from", "/missing", "--from", "/missing", "1"},
		{"--select", "-d", "/missing", "--from", "/missing", "1", "1"}, {"--select", "-d", "/missing", "--from", "/missing", "01"}, {"--select", "-d", "/missing", "--from", "/missing", "+1"}, {"--select", "-d", "/missing", "--from", "/missing", "0"}, {"--select", "-d", "/missing", "--from", "/missing", "9223372036854775808"},
		{"--approve", fullID}, {"--run", fullID}, {"--revoke", fullID}, {"--select", "-d", "/missing", "--from", "/missing", "1", "--show", fullID},
		{"--show", "--json"}, {"-show", "--json"}, {"--select", "-d", "/missing", "--from", "--json", "01"},
	}
	tooMany := []string{"--select", "-d", "/missing", "--from", "/missing"}
	for i := 1; i <= 21; i++ {
		tooMany = append(tooMany, strconv.Itoa(i))
	}
	cases = append(cases, tooMany)
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "absent")
			var out, stderr bytes.Buffer
			code := Run(context.Background(), append([]string{"--json", "--data-dir", base, "hash"}, args...), &out, &stderr)
			hashProposalFailure(t, code, out.String(), stderr.String(), "invalid_arguments", 2)
			if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid arguments initialized storage", err)
			}
		})
	}
}

func TestHashCLIReportStrictSchemaAndBoundedInput(t *testing.T) {
	f := newHashProposalCLIFixture(t)
	mutate := func(change func(map[string]any)) []byte {
		var value map[string]any
		if err := json.Unmarshal(f.input, &value); err != nil {
			t.Fatal(err)
		}
		change(value)
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	cases := map[string][]byte{
		"duplicate_top":    bytes.Replace(f.input, []byte(`"api_version":1`), []byte(`"api_version":1,"api_version":1`), 1),
		"duplicate_nested": bytes.Replace(f.input, []byte(`"content_verified":false`), []byte(`"content_verified":false,"content_verified":false`), 1),
		"trailing":         append(bytes.Clone(f.input), []byte(` {}`)...),
		"oversized":        bytes.Repeat([]byte(" "), hashReportByteLimit+1),
		"version":          mutate(func(v map[string]any) { v["api_version"] = 2 }),
		"failed":           mutate(func(v map[string]any) { v["ok"] = false }),
		"command":          mutate(func(v map[string]any) { v["command"] = "scan" }),
		"unknown":          mutate(func(v map[string]any) { v["future"] = true }),
		"case_alias":       mutate(func(v map[string]any) { v["OK"] = v["ok"]; delete(v, "ok") }),
		"missing":          mutate(func(v map[string]any) { delete(v, "api_version") }),
		"null":             mutate(func(v map[string]any) { v["ok"] = nil }),
		"wrong_report":     mutate(func(v map[string]any) { delete(v["report"].(map[string]any), "same_size") }),
		"null_same_size":   mutate(func(v map[string]any) { v["report"].(map[string]any)["same_size"] = nil }),
		"other_mode_null":  mutate(func(v map[string]any) { v["report"].(map[string]any)["directory"] = nil }),
	}
	for _, key := range []string{"inventory_id", "current_state_verified", "estimated_reclaimable_bytes", "bands"} {
		key := key
		cases["missing_"+key] = mutate(func(v map[string]any) { delete(v["report"].(map[string]any)["same_size"].(map[string]any), key) })
	}
	for _, key := range []string{"content_verified", "current_state_verified"} {
		key := key
		cases["claim_"+key] = mutate(func(v map[string]any) { v["report"].(map[string]any)["same_size"].(map[string]any)[key] = true })
	}
	fileChange := func(change func(map[string]any)) []byte {
		return mutate(func(v map[string]any) {
			change(v["report"].(map[string]any)["same_size"].(map[string]any)["bands"].([]any)[0].(map[string]any)["files"].([]any)[0].(map[string]any))
		})
	}
	cases["missing_file_stamp"] = fileChange(func(v map[string]any) { delete(v, "ctime_ns") })
	cases["null_raw_path"] = fileChange(func(v map[string]any) { v["path_bytes"] = nil })
	cases["wrong_size_band"] = fileChange(func(v map[string]any) { v["logical_bytes"] = 999 })
	cases["unknown_file_field"] = fileChange(func(v map[string]any) { v["hash"] = "unsupported" })
	cases["duplicate_file_id"] = mutate(func(v map[string]any) {
		rows := v["report"].(map[string]any)["same_size"].(map[string]any)["bands"].([]any)[0].(map[string]any)["files"].([]any)
		rows[1].(map[string]any)["id"] = rows[0].(map[string]any)["id"]
	})
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(f.report, body, 0600); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
			hashProposalFailure(t, code, raw, stderr, "invalid_arguments", 2)
			if _, err := os.Stat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("bad report initialized hash storage", err)
			}
		})
	}
	for _, name := range []string{"directory", "symlink", "fifo"} {
		t.Run(name, func(t *testing.T) {
			input := filepath.Join(t.TempDir(), "input")
			switch name {
			case "directory":
				if err := os.Mkdir(input, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(f.report, input); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(input, 0600); err != nil {
					t.Fatal(err)
				}
			}
			args := f.selectArgs()
			args[5] = input
			code, raw, stderr := f.run(context.Background(), args...)
			if code == 0 || stderr != "" {
				t.Fatal(code, raw, stderr)
			}
			if _, err := os.Stat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("nonregular report initialized storage", err)
			}
		})
	}
}

func TestHashCLIExactEvidenceAndWrongRootBeforeHashStorage(t *testing.T) {
	for _, mode := range []string{"changed_report_stamp", "missing_id", "wrong_root", "mismatched_root_binding", "changed_inventory"} {
		t.Run(mode, func(t *testing.T) {
			f := newHashProposalCLIFixture(t)
			args := f.selectArgs()
			switch mode {
			case "changed_report_stamp":
				var envelope hashReportEnvelope
				if err := json.Unmarshal(f.input, &envelope); err != nil {
					t.Fatal(err)
				}
				envelope.Report.SameSize.Bands[0].Files[0].ChangedNS++
				body, err := json.Marshal(envelope)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.report, body, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing_id":
				args[6] = "9223372036854775807"
			case "wrong_root":
				args[3] = filepath.Join(f.root, "different")
			case "mismatched_root_binding":
				args[3] = filepath.Join(f.root, "different")
				paths, err := config.ResolvePaths(f.base)
				if err != nil {
					t.Fatal(err)
				}
				other := manualState(paths, args[3])
				if err := os.Mkdir(other, 0700); err != nil {
					t.Fatal(err)
				}
				body, err := os.ReadFile(filepath.Join(f.source, state.Filename))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(other, state.Filename), body, 0600); err != nil {
					t.Fatal(err)
				}
			case "changed_inventory":
				path := string(f.page.Bands[0].Files[0].PathBytes)
				if err := os.WriteFile(path, []byte("new size"), 0600); err != nil {
					t.Fatal(err)
				}
				// Scan reads optional configuration; the proposal itself must
				// succeed with malformed configuration in every other fixture.
				if err := os.Remove(filepath.Join(f.base, "config.toml")); err != nil {
					t.Fatal(err)
				}
				code, raw, stderr := f.run(context.Background(), "scan", "-d", f.root, "--now", "--detailed", "--json")
				if code != 0 || stderr != "" {
					t.Fatal(code, raw, stderr)
				}
			}
			code, raw, stderr := f.run(context.Background(), args...)
			if code == 0 || stderr != "" || !strings.Contains(raw, `"ok":false`) {
				t.Fatal(code, raw, stderr)
			}
			if _, err := os.Stat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unreviewed evidence initialized hash storage", err)
			}
		})
	}
}

func TestHashCLIMissingCanceledAndPublicationOutputFailure(t *testing.T) {
	base := filepath.Join(t.TempDir(), "absent")
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hash", "--show", strings.Repeat("a", 64), "--json"}, &out, &stderr)
	hashProposalFailure(t, code, out.String(), stderr.String(), "not_found", 1)
	if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing show initialized storage", err)
	}
	f := newHashProposalCLIFixture(t)
	// An option value that happens to be --json remains a value. Losing it in
	// the global filter would change this into an argument-validation error.
	missingArgs := f.selectArgs()
	missingArgs[5] = "--json"
	code, raw, diagnostic := f.run(context.Background(), missingArgs...)
	hashProposalFailure(t, code, raw, diagnostic, "not_found", 1)
	missingArgs = f.selectArgs()
	missingArgs[5] = filepath.Join(t.TempDir(), "missing report")
	missingArgs = missingArgs[:len(missingArgs)-1]
	code, raw, diagnostic = f.run(context.Background(), missingArgs...)
	if code != 1 || raw != "" || !strings.Contains(diagnostic, "named same-size report is unavailable") || strings.Contains(diagnostic, "rydd init") {
		t.Fatal(code, raw, diagnostic)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, raw, diagnostic = f.run(ctx, f.selectArgs()...)
	hashProposalFailure(t, code, raw, diagnostic, "canceled", 1)
	if _, err := os.Stat(filepath.Join(f.base, "hashes")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pre-canceled selection initialized storage", err)
	}
	// Publication precedes output. A broken output must not erase the saved
	// proposal or cause an implicit retry; hashes exposes its ID for reopening.
	stderr.Reset()
	code = Run(context.Background(), append([]string{"--data-dir", f.base}, f.selectArgs()...), hashFailWriter{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "write JSON") {
		t.Fatal(code, stderr.String())
	}
	code, raw, diagnostic = f.run(context.Background(), "hashes", "--json")
	saved := hashCLIReport(t, code, raw, diagnostic)
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show", saved.SelectionID, "--json")
	p := hashCLIProposal(t, code, raw, diagnostic)
	for _, short := range []bool{false, true} {
		stderr.Reset()
		code = Run(context.Background(), []string{"--data-dir", f.base, "hash", "--show", p.SelectionID}, hashFailWriter{short: short}, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), p.SelectionID) || !strings.Contains(stderr.String(), "hash --show") {
			t.Fatal(code, stderr.String())
		}
	}
	code, raw, diagnostic = f.run(context.Background(), "hash", "--show", strings.Repeat("f", 64), "--json")
	hashProposalFailure(t, code, raw, diagnostic, "not_found", 1)
}

func TestHashCLISelectAndShowPreserveReservedAttemptNoRecovery(t *testing.T) {
	f := newHashProposalCLIFixture(t)
	code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A coherent unsettled reservation is seeded without source reads. It
	// represents the crash boundary after a committed grant, before any read.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, query := range []struct {
		text string
		args []any
	}{
		{"UPDATE hash_work SET status='running' WHERE id=1", nil},
		{"INSERT INTO hash_attempt VALUES(1,?,0,0,64,?,'reserved',NULL,NULL,NULL)", []any{strings.Repeat("a", 64), now.Format("2006-01-02")}},
		{"INSERT INTO hash_budget VALUES(1,?,?,64,0,0,0,64,0,0,0)", []any{now.Format("2006-01-02"), now.UnixNano()}},
	} {
		if _, err := tx.Exec(query.text, query.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(context.Background(), f.selectArgs()...)
	if retry := hashCLIProposal(t, code, raw, stderr); !reflect.DeepEqual(p, retry) {
		t.Fatal("retry changed proposal", raw)
	}
	code, raw, stderr = f.run(context.Background(), "hash", "--show", p.SelectionID, "--json")
	_ = hashCLIProposal(t, code, raw, stderr)
	code, raw, stderr = f.run(context.Background(), "hashes", "--json")
	saved := hashCLIReport(t, code, raw, stderr)
	if saved.Work[0].Status != "running" || saved.Work[0].Sequence != 0 || saved.Work[0].LatestAttempt == nil || saved.Work[0].LatestAttempt.Status != "reserved" || saved.Work[0].LatestAttempt.RequestedBytes != nil || saved.Budget.TotalReservedBytes != 64 || saved.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("proposal commands recovered reserved work", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("proposal commands changed reserved storage/source bytes")
	}
	// A different exact subset conflicts; it must not recover or replace work.
	args := f.selectArgs()
	args = append(args[:7], "--json")
	code, raw, stderr = f.run(context.Background(), args...)
	hashProposalFailure(t, code, raw, stderr, "already_exists", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("conflicting proposal recovered or replaced work")
	}
}

func TestHashCLIShowCorruptionAndCapabilities(t *testing.T) {
	f := newHashProposalCLIFixture(t)
	code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("UPDATE hash_work SET checkpoint=X'7b7d' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr = f.run(context.Background(), "hash", "--show", p.SelectionID, "--json")
	hashProposalFailure(t, code, raw, stderr, "hash_invalid", 1)
	code, raw, stderr = f.run(context.Background(), "capabilities", "--json")
	if code != 0 || stderr != "" || !strings.Contains(raw, `"saved_hash_proposals":true`) || !strings.Contains(raw, `"full_hashing":false`) || !strings.Contains(raw, `"name":"hash"`) {
		t.Fatal(code, raw, stderr)
	}
}

func TestHashCLIAuthoritativeInvalidBytePaths(t *testing.T) {
	f := newHashProposalCLIFixtureWithFilename(t, "raw\xff")
	var envelope hashReportEnvelope
	if err := json.Unmarshal(f.input, &envelope); err != nil {
		t.Fatal(err)
	}
	for i := range envelope.Report.SameSize.Bands[0].Files {
		envelope.Report.SameSize.Bands[0].Files[i].Path = "lossy display does not bind selected file"
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.report, body, 0600); err != nil {
		t.Fatal(err)
	}
	code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	found := false
	for _, target := range p.Targets {
		if bytes.Contains(target.File.PathBytes, []byte{0xff}) {
			found = true
		}
		if target.File.Path == "lossy display does not bind selected file" {
			t.Fatal("display replaced authoritative raw path", raw)
		}
	}
	if !found {
		t.Fatal("invalid raw bytes did not round trip", raw)
	}
	code, human, stderr := f.run(context.Background(), "hash", "--show", p.SelectionID)
	if code != 0 || stderr != "" || !strings.Contains(human, `raw\xff`) {
		t.Fatal(code, human, stderr)
	}
}

func TestHashCLIWriterBusyAndShowWhileWriterHeld(t *testing.T) {
	f := newHashProposalCLIFixture(t)
	code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
	p := hashCLIProposal(t, code, raw, stderr)
	writer, err := inventory.OpenHashSelectionWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	before := hashCLIBytes(t, f.base, f.root)
	code, raw, stderr = f.run(context.Background(), "hash", "--show", p.SelectionID, "--json")
	if shown := hashCLIProposal(t, code, raw, stderr); !reflect.DeepEqual(p, shown) {
		t.Fatal("reader required writer lock", raw)
	}
	code, raw, stderr = f.run(context.Background(), f.selectArgs()...)
	hashProposalFailure(t, code, raw, stderr, "writer_busy", 1)
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("busy selection/read changed evidence")
	}
}
