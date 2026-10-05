package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestPlanPreviewHumanJSONReadOnly(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "state")
	paths, err := config.ResolvePaths(base)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "offline")
	dir := manualState(paths, root)
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	old := time.Now().Add(-60 * 24 * time.Hour).UnixNano()
	if err = w.CommitScan(ctx, *j, state.ScanBatch{Identity: "fixture", Generation: 1, Complete: true, Directory: state.Entry{Path: []byte("."), Kind: "directory"}, Entries: []state.Entry{
		{Path: []byte("node_modules"), Kind: "directory", Device: "d", Inode: "target", MtimeNS: old},
		{Path: []byte("package.json"), Kind: "file", Device: "d", Inode: "manifest", MtimeNS: old},
	}}); err != nil {
		t.Fatal(err)
	}
	findings, err := w.NodeModulesFindings(ctx, "", 30)
	if err != nil || len(findings.Findings) != 1 {
		t.Fatal(findings, err)
	}
	id := findings.Findings[0].ID
	w.Close()
	db := filepath.Join(dir, state.Filename)
	before, err := os.ReadFile(db)
	if err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (int, string) {
		t.Helper()
		var out, stderr bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", base}, args...), &out, &stderr)
		if stderr.Len() != 0 {
			t.Fatal(stderr.String())
		}
		return code, out.String()
	}
	code, raw := run("plan", "--preview", "-d", root, "--min-age-days", "30", id, "--json")
	var envelope struct {
		OK   bool        `json:"ok"`
		Plan PlanPreview `json:"plan"`
	}
	if err = json.Unmarshal([]byte(raw), &envelope); err != nil || code != 0 || !envelope.OK {
		t.Fatal(code, raw, err)
	}
	r := envelope.Plan
	if r.Mode != "preview" || r.Executable || r.ApprovalAvailable || r.QuarantineReclaimsSpace || r.EstimatedReclaimableBytes != nil || r.Activity != "unconfirmed" || len(r.Evidence.Findings) != 1 || r.Evidence.Findings[0].ID != id || r.Evidence.CurrentStateVerified || len(r.Evidence.Findings[0].Actions) != 0 || r.Evidence.Findings[0].Measurement.Status != "partial" {
		t.Fatal(r)
	}
	code, human := run("plan", "--preview", "-d", root, "--min-age-days", "30", id)
	for _, want := range []string{"PREVIEW ONLY", id, "partial", "unconfirmed", "frees no disk space", "restore without overwriting"} {
		if code != 0 || !strings.Contains(strings.Join(strings.Fields(human), " "), want) {
			t.Fatal(want, code, human)
		}
	}
	for _, args := range [][]string{
		{"plan", id, "--json"}, {"plan", "--preview", "--json"}, {"plan", "--preview=false", id, "--json"},
		{"plan", "--preview", "-d", root, id, "--json"},
		{"plan", "--preview", "-d", root, "--min-age-days", "0", id, "--json"},
		{"plan", "--preview", "-d", root, "--directory", root, id, "--json"},
		{"plan", "--preview", "-d", root, "--min-age-days", "30", id, id, "--json"},
		{"plan", "--preview", "-d", root, "--min-age-days", "30", id, "node-modules-v1:1:9999999", "--json"},
	} {
		code, raw := run(args...)
		if code != 2 || !strings.Contains(raw, "invalid_arguments") {
			t.Fatal(args, code, raw)
		}
	}
	after, err := os.ReadFile(db)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("preview changed inventory", err)
	}
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("offline source changed", err)
	}
}
