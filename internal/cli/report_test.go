package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestReportHumanAndJSONOffline(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	run := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := Run(ctx, append([]string{"--data-dir", dir}, args...), &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, _, err := run("init", "--root", "/offline-fixture"); code != 0 {
		t.Fatal(err)
	}
	s, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	err = s.CommitScan(ctx, *j, state.ScanBatch{Identity: "fixture", Generation: 1, Complete: true, Directory: state.Entry{Path: []byte("."), Kind: "directory"}, Entries: []state.Entry{{Path: []byte("large\nfile"), Kind: "file", Size: 2048, Allocated: 4096}, {Path: []byte("small"), Kind: "file", Size: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	code, human, errOut := run("report", "--limit", "1")
	if code != 0 || errOut != "" || !strings.Contains(human, "2.0 KiB") || !strings.Contains(human, `large\nfile`) {
		t.Fatal(code, human, errOut)
	}
	code, machine, errOut := run("report", "--limit", "1", "--json")
	var envelope struct {
		Version int              `json:"api_version"`
		OK      bool             `json:"ok"`
		Report  state.FileReport `json:"report"`
	}
	if err = json.Unmarshal([]byte(machine), &envelope); err != nil {
		t.Fatal(err)
	}
	if code != 0 || errOut != "" || envelope.Version != 1 || !envelope.OK || len(envelope.Report.Files) != 1 || envelope.Report.Files[0].Size != 2048 || envelope.Report.NextCursor == "" {
		t.Fatal(machine, errOut)
	}
	code, machine, errOut = run("report", "--cursor", envelope.Report.NextCursor, "--json")
	if err = json.Unmarshal([]byte(machine), &envelope); err != nil {
		t.Fatal(err)
	}
	if code != 0 || len(envelope.Report.Files) != 1 || envelope.Report.Files[0].Size != 1 {
		t.Fatal(machine, errOut)
	}
	code, machine, errOut = run("report", "--directory", "/offline-fixture", "--json")
	if err = json.Unmarshal([]byte(machine), &envelope); err != nil {
		t.Fatal(err)
	}
	d := envelope.Report.Directory
	if code != 0 || errOut != "" || d == nil || d.Status != "recorded_complete" || d.LogicalBytes == nil || *d.LogicalBytes != 2049 {
		t.Fatal(machine, errOut)
	}
	code, human, errOut = run("report", "--directory", "/offline-fixture")
	if code != 0 || errOut != "" || !strings.Contains(human, "saved directory size") || !strings.Contains(human, "2.0 KiB") {
		t.Fatal(code, human, errOut)
	}
	for _, args := range [][]string{{"report", "--limit", "0", "--json"}, {"report", "--cursor", "bad", "--json"}, {"report", "--bad", "--json"}, {"report", "--directory", "relative", "--json"}, {"report", "--directory", "/offline-fixture", "--limit", "1", "--json"}} {
		code, out, stderr := run(args...)
		if code != 2 || stderr != "" || !strings.Contains(out, `"invalid_arguments"`) {
			t.Fatal(code, out, stderr)
		}
	}
}
