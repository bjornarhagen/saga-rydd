package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestManualScanIsolationAndReports(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	root := t.TempDir()
	other := t.TempDir()
	for _, p := range []string{root, other} {
		if err := os.WriteFile(filepath.Join(p, "file"), []byte("hello"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) map[string]any {
		t.Helper()
		var out, errout bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &errout)
		if code != 0 {
			t.Fatal(code, out.String(), errout.String())
		}
		var v map[string]any
		if err := json.Unmarshal(out.Bytes(), &v); err != nil {
			t.Fatal(err, out.String())
		}
		return v
	}
	run("init", "--root", other, "--exclude", filepath.Join(root, "excluded"), "--json")
	if err := os.Mkdir(filepath.Join(root, "excluded"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "excluded", "hidden"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(base, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	run("scan", "-d", root, "--now", "--json")
	run("scan", "--directory", other, "--sleep", "1", "--json")
	r := run("report", "-d", root, "--json")["report"].(map[string]any)["directory"].(map[string]any)
	if r["logical_file_bytes"] != float64(5) || r["status"] != "partial" {
		t.Fatal(r)
	}
	run("report", "-d", root, "--candidates", "--json")
	after, _ := os.ReadFile(filepath.Join(base, "config.toml"))
	if !bytes.Equal(before, after) {
		t.Fatal("background config changed")
	}
	s, err := state.OpenReader(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := s.Summary(ctx)
	s.Close()
	if err != nil || summary.Entries != 0 {
		t.Fatal(summary, err)
	}
	if err := os.WriteFile(filepath.Join(root, "new"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	run("scan", "-d", root, "--now", "--json")
	r = run("report", "-d", root, "--json")["report"].(map[string]any)["directory"].(map[string]any)
	if r["logical_file_bytes"] != float64(8) {
		t.Fatal(r)
	}
	r = run("report", "-d", other, "--json")["report"].(map[string]any)["directory"].(map[string]any)
	if r["logical_file_bytes"] != float64(5) {
		t.Fatal("cross-root contamination", r)
	}
}

func TestManualScanInterruptAndWriterLock(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	paths, err := config.ResolvePaths(base)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, name := range []string{"one", "two", "three"} {
		if err = os.WriteFile(filepath.Join(root, name), []byte("a"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = scan(ctx, []string{"-d", root, "-s", "1000"}, paths, &bytes.Buffer{}); err == nil {
		t.Fatal("expected cancellation")
	}
	r, err := scan(context.Background(), []string{"-d", root, "--now"}, paths, &bytes.Buffer{})
	if err != nil || r.Inventory.Entries != 4 || r.Outcome != "queue_drained" {
		t.Fatal(r, err)
	}
	w, err := state.OpenWriter(context.Background(), manualState(paths, root))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = scan(context.Background(), []string{"-d", root, "--now"}, paths, &bytes.Buffer{}); err == nil {
		t.Fatal("second writer accepted")
	}
}

func TestManualScanInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"scan"}, {"scan", "-d", "/fixture", "-s", "-1"}, {"scan", "-d", "/fixture", "--now", "-s", "10"}, {"scan", "-d", "/fixture", "--sleep", "60001"}, {"report", "-d", "/fixture", "--directory", "/fixture"}} {
		var out, errout bytes.Buffer
		code := Run(context.Background(), append(append([]string{"--data-dir", filepath.Join(t.TempDir(), "state")}, args...), "--json"), &out, &errout)
		if code != 2 || !bytes.Contains(out.Bytes(), []byte("invalid_arguments")) {
			t.Fatal(code, out.String(), errout.String())
		}
	}
}

// Construct the durable state left between directory batches, without relying
// on filesystem speed or sleeps to interrupt at a particular directory.
func TestManualScanFinishesSavedPassBeforeRevisiting(t *testing.T) {
	for _, pending := range []string{"pending", "running", "delayed"} {
		t.Run(pending, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			for _, name := range []string{"a", "b"} {
				if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			// Let the CLI establish private parent directories and the first pass.
			first, err := scan(ctx, []string{"-d", root, "--now"}, paths, &bytes.Buffer{})
			if err != nil || first.Mode != "new_pass" {
				t.Fatal(first, err)
			}
			w, err := state.OpenWriter(ctx, manualState(paths, root))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err = w.SeedInventory(ctx); err != nil {
				t.Fatal(err)
			}
			scanner, err := inventory.New([]string{root}, nil, []string{paths.StateDir})
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			// Complete the root and one child, leaving only the other child.
			for completed := 0; completed < 2; {
				j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
				if err != nil || j == nil {
					t.Fatal(j, err)
				}
				b, err := scanner.Next(ctx, *j)
				if err != nil {
					t.Fatal(b, err)
				}
				if err = w.CommitScan(ctx, *j, b); err != nil {
					t.Fatal(err)
				}
				if b.Complete {
					completed++
				}
			}
			if pending != "pending" {
				j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
				if err != nil || j == nil {
					t.Fatal(j, err)
				}
				if pending == "delayed" {
					if err = w.FinishJob(ctx, *j, false, j.Cursor, time.Now().Add(time.Hour), "retry"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			r, err := scan(ctx, []string{"-d", root, "--now"}, paths, &out)
			if err != nil || r.Mode != "resume" || !bytes.Contains(out.Bytes(), []byte("Resuming saved work")) {
				t.Fatal(r, err, out.String())
			}
			if pending == "delayed" {
				if r.Batches != 0 || r.Outcome != "pending_retry" || r.Inventory.PendingJobs != 1 {
					t.Fatal(r)
				}
			} else if r.Batches != 1 || r.Outcome != "queue_drained" || r.Inventory.PendingJobs != 0 {
				t.Fatal("completed directories were revisited", r)
			}
		})
	}
}
