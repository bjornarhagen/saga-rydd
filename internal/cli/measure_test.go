package cli

import (
	"bytes"
	"context"
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

func TestMeasureSavedOnlyResumeAndReports(t *testing.T) {
	ctx := context.Background()
	paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately nonexistent: saved calculations must work with the root offline.
	root := filepath.Join(t.TempDir(), "offline")
	store := manualState(paths, root)
	w, err := state.OpenWriter(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	on := true
	if _, err = w.ConfigureCompact(ctx, &on); err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	for batch := 0; batch < 3; batch++ {
		j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil || j == nil {
			t.Fatal(j, err)
		}
		entries := []state.Entry{}
		for i := 0; i < 100; i++ {
			entries = append(entries, state.Entry{Path: []byte(fmt.Sprintf("f%03d", batch*100+i)), Kind: "file", Size: 1, Allocated: 4096, Device: "d", Inode: "shared"})
		}
		if err = w.CommitScan(ctx, *j, state.ScanBatch{Identity: "fixture", Generation: 1, Complete: batch == 2, Directory: state.Entry{Path: []byte("."), Kind: "directory"}, Entries: entries}); err != nil {
			t.Fatal(err)
		}
	}
	for {
		worked, err := w.RetireSubtrees(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	w.Close()
	r, err := measure(ctx, []string{"-d", root, "--batches", "1"}, paths)
	if err != nil || r.Complete || r.Batches != 1 || r.Outcome != "batch_limit" {
		t.Fatal(r, err)
	}
	var human bytes.Buffer
	printMeasureReport(&human, r)
	if !strings.Contains(human.String(), "Progress is saved") || !strings.Contains(human.String(), shellQuote(paths.StateDir)) {
		t.Fatal(human.String())
	}
	for i := 0; !r.Complete && i < 20; i++ {
		r, err = measure(ctx, []string{"-d", root, "--batches", "1"}, paths)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !r.Complete {
		t.Fatal(r)
	}
	report, err := report(ctx, []string{"-d", root}, paths)
	if err != nil || report.Directory.CoverageSource != "cached_reduction" || *report.Directory.LogicalBytes != 300 || *report.Directory.AllocatedBytes != 4096 || report.Directory.EntriesExamined != 301 {
		t.Fatal(report, err)
	}
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("offline root changed", err)
	}
	var out, errout bytes.Buffer
	code := Run(ctx, []string{"--data-dir", paths.StateDir, "measure", "-d", root, "--json"}, &out, &errout)
	var envelope struct {
		OK      bool          `json:"ok"`
		Measure MeasureReport `json:"measure"`
	}
	if err = json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 0 || !envelope.OK || !envelope.Measure.Complete || envelope.Measure.Batches != 0 {
		t.Fatal(code, out.String(), err)
	}
	w, err = state.OpenWriter(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err = measure(ctx, []string{"-d", root}, paths); err == nil || !strings.Contains(err.Error(), "pending manual scan") {
		t.Fatal(err)
	}
}

func TestMeasureValidation(t *testing.T) {
	paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{nil, {"-d", ""}, {"-d", "/fixture", "--directory", "/fixture"}, {"-d", "/fixture", "--batches", "0"}, {"-d", "/fixture", "--batches", "1001"}, {"-d", "/fixture", "extra"}} {
		if _, err := measure(context.Background(), args, paths); err == nil {
			t.Fatal(args)
		}
	}
	if _, err := measure(context.Background(), []string{"-d", "/fixture"}, paths); err == nil {
		t.Fatal("missing inventory accepted")
	}
	if _, err := os.Stat(paths.StateDir); !os.IsNotExist(err) {
		t.Fatal("created missing state", err)
	}
}
