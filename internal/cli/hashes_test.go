package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type hashCLIFixture struct {
	t                     *testing.T
	base, sourceDir, root string
	scanner               *inventory.Scanner
	source                *state.Store
	store                 *inventory.HashStore
	contents              []byte
	paths                 []string
}

// All selected rows come from an ordinary production metadata scan. The hash
// API, not a fabricated checkpoint or digest import, creates the saved work.
func newHashCLIFixture(t *testing.T) *hashCLIFixture {
	t.Helper()
	ctx := context.Background()
	canonicalTemp := func() string {
		path, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	f := &hashCLIFixture{t: t, base: filepath.Join(canonicalTemp(), "state with 'quote"), sourceDir: filepath.Join(canonicalTemp(), "inventory"), root: canonicalTemp(), contents: bytes.Repeat([]byte("generated fixture bytes"), 7)}
	for _, name := range []string{"line\nquote\"雪", "peer"} {
		path := filepath.Join(f.root, name)
		if err := os.WriteFile(path, f.contents, 0600); err != nil {
			t.Fatal(err)
		}
		f.paths = append(f.paths, path)
	}
	var err error
	f.scanner, err = inventory.New([]string{f.root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.scanner.Close)
	f.source, err = state.OpenWriter(ctx, f.sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.source.Close() })
	if err = f.source.SyncRoots(ctx, []string{f.root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = f.source.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = f.source.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for i := 0; i < 12; i++ {
		job, e := f.source.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if job == nil {
			finished = true
			break
		}
		batch, e := f.scanner.Next(ctx, *job)
		if e != nil || batch.Fault != "" {
			t.Fatal(batch, e)
		}
		if e = f.source.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("bounded production hash CLI fixture did not finish metadata scan")
	}
	report, err := f.source.SameSizeCandidates(ctx, 20, "", 1)
	if err != nil || len(report.Bands) != 1 || len(report.Bands[0].Files) != 2 {
		t.Fatal(report, err)
	}
	f.store, err = inventory.OpenHashWriter(ctx, f.base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	if _, err = f.store.CreateSelection(ctx, f.source, report.InventoryID, report.Bands[0].Files); err != nil {
		t.Fatal(err)
	}
	// A malformed config proves this saved-only command does not load it.
	if err = os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("not valid [ toml"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *hashCLIFixture) run(ctx context.Context, args ...string) (int, string, string) {
	f.t.Helper()
	var out, stderr bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, args...), &out, &stderr)
	return code, out.String(), stderr.String()
}

func hashCLIReport(t *testing.T, code int, raw, stderr string) HashReport {
	t.Helper()
	var r struct {
		Version int        `json:"api_version"`
		OK      bool       `json:"ok"`
		Command string     `json:"command"`
		Hashes  HashReport `json:"hashes"`
	}
	if err := json.Unmarshal([]byte(raw), &r); err != nil || code != 0 || stderr != "" || !r.OK || r.Version != 1 || r.Command != "hashes" {
		t.Fatal(code, raw, stderr, err)
	}
	if r.Hashes.Source != "saved_hash_observations" || r.Hashes.Contract != inventory.FileHashContract || r.Hashes.BudgetScope != "whole_saved_selection" || r.Hashes.ProvenanceVerified || r.Hashes.ContentVerified || r.Hashes.CurrentStateVerified || r.Hashes.DuplicatesVerified || r.Hashes.Executable || r.Hashes.EstimatedReclaimableBytes != nil {
		t.Fatal("stronger hash report claim", raw)
	}
	return r.Hashes
}

func hashCLIBytes(t *testing.T, roots ...string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || strings.HasSuffix(path, "-shm") || strings.HasSuffix(path, "writer.lock") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// SQLite can create/remove empty WAL reader sidecars. Compare all
			// source/config/database bytes and WALs containing saved frames.
			if len(body) == 0 && strings.HasSuffix(path, "-wal") {
				return nil
			}
			out[path] = sha256.Sum256(body)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestHashesCLIPartialCompleteOfflineAndReadOnly(t *testing.T) {
	ctx := context.Background()
	f := newHashCLIFixture(t)
	partial, err := f.store.RunNext(ctx, f.source, f.scanner, 64, 4096)
	if err != nil || partial.DurableOffset != 64 {
		t.Fatal(partial, err)
	}
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	code, raw, stderr := f.run(ctx, "hashes", "--json")
	r := hashCLIReport(t, code, raw, stderr)
	if len(r.Work) != 2 || r.Work[0].DurableOffset != 64 || r.Work[0].SHA256 != "" || r.Work[1].LatestAttempt != nil || r.Budget == nil || r.Budget.TotalReservedBytes != 64 {
		t.Fatal(raw)
	}
	code, raw, stderr = f.run(ctx, "--json", "hashes", "--work", "1")
	selected := hashCLIReport(t, code, raw, stderr)
	if selected.SelectedWorkID != "1" || len(selected.Work) != 1 || !reflect.DeepEqual(selected.Work[0], r.Work[0]) || !reflect.DeepEqual(selected.Budget, r.Budget) {
		t.Fatal("filtered report changed whole-selection budget", raw)
	}
	code, human, stderr := f.run(ctx, "hashes", "--work", "1")
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"SAVED HASH OBSERVATIONS - CURRENT FILES NOT CHECKED", "Saved prefix 64 B", "whole saved selection", "does not prove current contents", "No source files or saved records were changed", fmt.Sprintf("%q", string(r.Work[0].PathBytes))} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal(want, code, human, stderr)
		}
	}
	if strings.ContainsAny(human, "\x1b") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("saved report changed database or source bytes")
	}
	code, human, stderr = f.run(ctx, "hashes", "--work=2")
	if code != 0 || stderr != "" || !strings.Contains(human, fmt.Sprintf("%q", string(r.Work[1].PathBytes))) || strings.Contains(human, "line\nquote") {
		t.Fatal("production path controls were not quoted exactly", code, human, stderr)
	}
	for i := 0; i < 5; i++ {
		if _, err = f.store.RunNext(ctx, f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
			t.Fatal(err)
		}
	}
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	complete := hashCLIReport(t, code, raw, stderr)
	want := fmt.Sprintf("%x", sha256.Sum256(f.contents))
	for _, w := range complete.Work {
		if w.Status != "complete" || w.SHA256 != want || w.DurableOffset != int64(len(f.contents)) {
			t.Fatal(w)
		}
	}
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.source.Close(); err != nil {
		t.Fatal(err)
	}
	f.scanner.Close()
	if err = os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(f.sourceDir, f.sourceDir+".offline"); err != nil {
		t.Fatal(err)
	}
	before = hashCLIBytes(t, f.base, f.sourceDir+".offline", f.root+".offline")
	code, raw, stderr = f.run(ctx, "hashes", "--json")
	offline := hashCLIReport(t, code, raw, stderr)
	if !reflect.DeepEqual(offline, complete) {
		t.Fatal("offline report depended on sources", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir+".offline", f.root+".offline")) {
		t.Fatal("offline read changed database or source bytes")
	}
	code, human, stderr = f.run(ctx, "hashes")
	if code != 0 || stderr != "" || !strings.Contains(human, "Historical SHA-256") || !strings.Contains(human, want) || strings.Contains(human, "Verified") {
		t.Fatal(code, human, stderr)
	}
	if _, err = os.Stat(f.sourceDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("hashes initialized offline inventory", err)
	}
}

func TestHashesCLIExistingEmptyStore(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base = filepath.Join(base, "state")
	writer, err := inventory.OpenHashWriter(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", base, "hashes", "--json"}, &out, &stderr)
	r := hashCLIReport(t, code, out.String(), stderr.String())
	if r.SelectionID != "" || r.InventoryID != "" || len(r.Work) != 0 || r.Budget != nil {
		t.Fatal("empty store invented work or budget", out.String())
	}
	out.Reset()
	code = Run(context.Background(), []string{"--data-dir", base, "hashes"}, &out, &stderr)
	flat := strings.Join(strings.Fields(out.String()), " ")
	if code != 0 || stderr.Len() != 0 || !strings.Contains(flat, "No selection is saved") || !strings.Contains(flat, "does not create or start hashing work") {
		t.Fatal(code, out.String(), stderr.String())
	}
}

func TestHashesCLIUnknownAndReservedUsageNoRecovery(t *testing.T) {
	for _, recoverUnknown := range []bool{false, true} {
		t.Run(fmt.Sprint(recoverUnknown), func(t *testing.T) {
			ctx := context.Background()
			f := newHashCLIFixture(t)
			db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err = db.Exec("CREATE TRIGGER fail_hash_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err = f.store.RunNext(ctx, f.source, f.scanner, 64, 4096); !errors.Is(err, inventory.ErrHashRecoveryRequired) {
				t.Fatal(err)
			}
			if _, err = db.Exec("DROP TRIGGER fail_hash_fixture"); err != nil {
				t.Fatal(err)
			}
			if recoverUnknown {
				if err = f.store.Close(); err != nil {
					t.Fatal(err)
				}
				f.store, err = inventory.OpenHashWriter(ctx, f.base)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
			code, raw, stderr := f.run(ctx, "hashes", "--work", "1", "--json")
			r := hashCLIReport(t, code, raw, stderr)
			if len(r.Work) != 1 || r.Work[0].DurableOffset != 0 || r.Work[0].SHA256 != "" || r.Budget.TotalReservedBytes != 64 || r.Budget.TotalReadBytes != 0 || r.Work[0].LatestAttempt == nil {
				t.Fatal(raw)
			}
			a := r.Work[0].LatestAttempt
			wantStatus, wantWork, wantUnknown := "reserved", "running", int64(0)
			if recoverUnknown {
				wantStatus, wantWork, wantUnknown = "interrupted_unknown", "pending", 64
			}
			if a.Status != wantStatus || r.Work[0].Status != wantWork || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil || r.Budget.TotalUnknownReservedBytes != wantUnknown {
				t.Fatal("invented known zero or recovered during show", raw)
			}
			if !strings.Contains(raw, `"observed_requested_bytes":null`) || !strings.Contains(raw, `"observed_read_bytes":null`) {
				t.Fatal("unknown usage omitted or zero", raw)
			}
			code, human, stderr := f.run(ctx, "hashes", "--work", "1")
			flat := strings.Join(strings.Fields(human), " ")
			if code != 0 || stderr != "" || !strings.Contains(flat, "Observed requested Unknown") || !strings.Contains(flat, "Observed read Unknown") || !strings.Contains(flat, "does not prove a process is active") {
				t.Fatal(code, human, stderr)
			}
			if !strings.Contains(flat, "Total interrupted charge "+humanBytes(wantUnknown)) || !strings.Contains(flat, "Unsettled reservations remain charged but are excluded from the interrupted counters") {
				t.Fatal("unsettled charge was described as recovered interrupted usage", human)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
				t.Fatal("hashes changed unresolved work or source bytes")
			}
		})
	}
}

func TestHashesCLIKnownZeroUsageRemainsZero(t *testing.T) {
	f := newHashCLIFixture(t)
	if err := os.Rename(f.root, f.root+".unavailable"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096); err == nil {
		t.Fatal("unavailable source was read")
	}
	code, raw, stderr := f.run(context.Background(), "hashes", "--work", "1", "--json")
	r := hashCLIReport(t, code, raw, stderr)
	a := r.Work[0].LatestAttempt
	if a == nil || a.Status != "settled" || a.RequestedBytes == nil || *a.RequestedBytes != 0 || a.ReadBytes == nil || *a.ReadBytes != 0 || a.ElapsedNS == nil || r.Work[0].Status != "invalidated" {
		t.Fatal(raw)
	}
	code, human, stderr := f.run(context.Background(), "hashes", "--work", "1")
	flat := strings.Join(strings.Fields(human), " ")
	if code != 0 || stderr != "" || !strings.Contains(flat, "Observed requested 0 B") || !strings.Contains(flat, "Observed read 0 B") {
		t.Fatal(code, human, stderr)
	}
}

func TestHashesCLIArgumentsMissingCancellationAndCapabilities(t *testing.T) {
	base := filepath.Join(t.TempDir(), "absent")
	for _, args := range [][]string{
		{"--work"}, {"--work", ""}, {"--work", "0"}, {"--work", "21"}, {"--work", "01"}, {"--work", "+1"}, {"--work", "invalid"},
		{"--work", "1", "--work", "1"}, {"--work=1", "-work=1"}, {"--work", "--json"}, {"-work", "--json"},
		{"--directory", "/fixture"}, {"-d", "/fixture"}, {"--create"}, {"--run"}, {"--recover"}, {"--limit", "1"}, {"--cursor", "x"}, {"--", "--work", "1"}, {"extra"},
	} {
		var out, stderr bytes.Buffer
		command := append([]string{"--data-dir", base, "--json", "hashes"}, args...)
		if code := Run(context.Background(), command, &out, &stderr); code != 2 || stderr.Len() != 0 || !strings.Contains(out.String(), `"invalid_arguments"`) {
			t.Fatal(args, code, out.String(), stderr.String())
		}
		if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("bad arguments touched/created storage", err)
		}
	}
	var out, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", base, "hashes", "--json"}, &out, &stderr); code != 1 || stderr.Len() != 0 || !strings.Contains(out.String(), `"not_found"`) {
		t.Fatal(code, out.String(), stderr.String())
	}
	out.Reset()
	stderr.Reset()
	if code := Run(context.Background(), []string{"--data-dir", base, "hashes"}, &out, &stderr); code != 1 || strings.Contains(stderr.String(), "rydd init") || !strings.Contains(stderr.String(), "existing observations") {
		t.Fatal(code, out.String(), stderr.String())
	}
	if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing report initialized storage", err)
	}
	f := newHashCLIFixture(t)
	if code, raw, stderr := f.run(context.Background(), "hashes", "--work", "20", "--json"); code != 1 || stderr != "" || !strings.Contains(raw, `"not_found"`) {
		t.Fatal(code, raw, stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code, raw, stderr := f.run(ctx, "hashes", "--json"); code != 1 || stderr != "" || !strings.Contains(raw, `"canceled"`) {
		t.Fatal(code, raw, stderr)
	}
	code, raw, capStderr := f.run(context.Background(), "capabilities", "--json")
	var capabilities struct{ Features map[string]bool }
	if err := json.Unmarshal([]byte(raw), &capabilities); err != nil || code != 0 || capStderr != "" || !capabilities.Features["saved_hash_reports"] || !capabilities.Features["full_hashing"] || capabilities.Features["duplicates"] || capabilities.Features["cleanup"] || !strings.Contains(raw, "--work WORK_ID") || !strings.Contains(raw, "hash_invalid") {
		t.Fatal(code, raw, capStderr, err)
	}
}

func TestHashesCLICorruptOrIncompatibleStoreReturnsNoPartialReport(t *testing.T) {
	for _, kind := range []string{"checkpoint", "oversized", "version", "missing_meta"} {
		t.Run(kind, func(t *testing.T) {
			f := newHashCLIFixture(t)
			db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			switch kind {
			case "checkpoint":
				_, err = db.Exec("UPDATE hash_work SET checkpoint=? WHERE id=1", []byte("{}"))
			case "oversized":
				_, err = db.Exec("PRAGMA ignore_check_constraints=1; UPDATE hash_work SET checkpoint=? WHERE id=1", bytes.Repeat([]byte{'x'}, 8193))
			case "version":
				_, err = db.Exec("PRAGMA user_version=5")
			case "missing_meta":
				_, err = db.Exec("DELETE FROM hash_meta")
			}
			if err != nil {
				t.Fatal(err)
			}
			before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
			code, raw, stderr := f.run(context.Background(), "hashes", "--json")
			if code != 1 || stderr != "" || !strings.Contains(raw, `"hash_invalid"`) || strings.Contains(raw, `"hashes":`) || strings.Contains(raw, `"path_bytes"`) || strings.Contains(raw, `"sha256"`) {
				t.Fatal("corrupt report disclosed partially trusted records", code, raw, stderr)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
				t.Fatal("reader migrated or changed damaged records")
			}
		})
	}
}

type hashFailWriter struct{ short bool }

func (w hashFailWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) / 2, nil
	}
	return 0, errors.New("fixture output failed")
}

func TestHashesCLIOutputFailures(t *testing.T) {
	f := newHashCLIFixture(t)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			var stderr bytes.Buffer
			args := []string{"--data-dir", f.base, "hashes"}
			if machine {
				args = append(args, "--json")
			}
			if code := Run(context.Background(), args, hashFailWriter{short: short}, &stderr); code != 1 || stderr.Len() == 0 {
				t.Fatal("write failure returned success", machine, short, code, stderr.String())
			}
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("output failure changed sources or saved records")
	}
}

func TestHashesHumanQualifiedStatesAndExactPaths(t *testing.T) {
	zero := int64(0)
	path := []byte("/fixture/" + strings.Repeat("long", 30) + "\n\x1b\xff")
	r := HashReport{HashSnapshot: inventory.HashSnapshot{StoreID: strings.Repeat("a", 64), SelectionID: strings.Repeat("b", 64), Work: []inventory.SavedHashWork{{ID: "1", PathBytes: path, Status: "running", LatestAttempt: &inventory.HashAttempt{Status: "reserved"}}, {ID: "2", PathBytes: []byte("/fixture/zero"), Status: "invalidated", LatestAttempt: &inventory.HashAttempt{Status: "settled", RequestedBytes: &zero, ReadBytes: &zero, ElapsedNS: &zero}}}}, BudgetScope: "whole_saved_selection"}
	before, _ := json.Marshal(r)
	var out bytes.Buffer
	if err := printHashes(&out, r); err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(out.String()), " ")
	for _, want := range []string{fmt.Sprintf("%q", string(path)), "Running in saved records; active process unknown", "Observed read Unknown", "Observed read 0 B", "No checked prefix recorded", "No reservation is recorded"} {
		if !strings.Contains(flat, want) {
			t.Fatal(want, out.String())
		}
	}
	if strings.ContainsAny(out.String(), "\x1b\xff") {
		t.Fatal("raw terminal control or invalid bytes printed")
	}
	after, _ := json.Marshal(r)
	if !bytes.Equal(before, after) {
		t.Fatal("human rendering changed saved evidence")
	}
	if err := printHashes(io.Discard, r); err != nil {
		t.Fatal(err)
	}
}
