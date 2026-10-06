package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestHumanMeasurementsPreserveQualifications(t *testing.T) {
	zero := int64(0)
	for _, tc := range []struct {
		status string
		label  string
	}{
		{"recorded_complete", "Complete in saved scan"},
		{"partial", "Incomplete measurement"},
		{"stale", "Saved data may be outdated"},
		{"unknown", "Unknown"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			// Control characters, invalid UTF-8 and long Unicode names must stay
			// quoted and intact, regardless of the prose wrapping policy.
			path := "/fixture/" + strings.Repeat("資料e\u0301", 30) + "\n\x1b[31m\xff/node_modules"
			measurement := state.DirectoryReport{Status: tc.status, PathBytes: []byte(path)}
			if tc.status != "unknown" {
				measurement.LogicalBytes = &zero
			} else {
				measurement.UnknownReason = "No saved directory observation."
			}
			measurement.Truncated = tc.status == "partial"
			finding := state.Finding{ID: "node-modules-v1:1:42", PathBytes: []byte(path), Measurement: measurement}
			r := state.FindingReport{MinimumAgeDays: 90, Findings: []state.Finding{finding}, EntriesExamined: 10}
			before, _ := json.Marshal(r)
			var out bytes.Buffer
			printFindingReport(&out, r, "rydd report --candidates")
			human := out.String()
			flat := strings.Join(strings.Fields(human), " ")
			for _, want := range []string{tc.label, fmt.Sprintf("%q", path), finding.ID, "Allocated on disk Unknown", "Current contents have not been checked", "activity is unconfirmed", "Cleanup is not yet supported", "not estimates of space you can free"} {
				if !strings.Contains(flat, want) {
					t.Fatalf("missing %q: %s", want, human)
				}
			}
			if strings.ContainsAny(human, "\x1b\xff") || strings.Index(human, finding.ID) > strings.Index(human, "PAGE SUMMARY") {
				t.Fatal(human)
			}
			if tc.status == "partial" && !strings.Contains(flat, "sizes cover only the measured portion") {
				t.Fatal(human)
			}
			if tc.status == "unknown" && (!strings.Contains(flat, "File size Unknown") || !strings.Contains(flat, measurement.UnknownReason)) {
				t.Fatal(human)
			}
			if tc.status != "unknown" && !strings.Contains(flat, "File size 0 B") {
				t.Fatal(human)
			}
			after, _ := json.Marshal(r)
			if !bytes.Equal(before, after) {
				t.Fatal("human rendering changed report evidence")
			}
		})
	}
}

func TestHumanNarrowLayout(t *testing.T) {
	for _, width := range []int{32, 40, 78} {
		var out bytes.Buffer
		writeField(&out, "Measurement", "Complete in saved scan", width)
		writeWrapped(&out, "Saved 資料 and cafe\u0301 records may be incomplete. Current contents have not been checked.", "  - ", "    ", width)
		for _, line := range strings.Split(out.String(), "\n") {
			if proseWidth(line) > width {
				t.Fatalf("width %d: %q", width, line)
			}
		}
		if width < 60 && !strings.Contains(out.String(), "Measurement:\n    ") {
			t.Fatal(out.String())
		}
		if !strings.Contains(strings.Join(strings.Fields(out.String()), " "), "Complete in saved scan") {
			t.Fatal(out.String())
		}
	}
}

func TestHumanRedirectedBannerAndEmptyPage(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	for _, noColor := range []string{"", "1"} {
		t.Setenv("NO_COLOR", noColor)
		var out bytes.Buffer
		printFindingReport(&out, state.FindingReport{MinimumAgeDays: 30, NextCursor: "nm2:30:1000"}, "rydd report --candidates")
		flat := strings.Join(strings.Fields(out.String()), " ")
		for _, want := range []string{"NO CANDIDATES ON THIS PAGE", "This page is empty, but more saved entries remain.", "--cursor 'nm2:30:1000'", "at least 30 days old"} {
			if !strings.Contains(flat, want) {
				t.Fatal(want, out.String())
			}
		}
		if strings.Contains(out.String(), "\x1b") || strings.Contains(out.String(), "End of saved entries") {
			t.Fatal(out.String())
		}
	}
}

func TestHumanScanContinuationUsesParentState(t *testing.T) {
	base := "/fixture/state with 'quotes' $dollars `literal`"
	root := "/fixture/project with 'quotes' $dollars `literal`"
	for _, tc := range []struct{ outcome, headline string }{
		{"queue_drained", "Scan pass finished"},
		{"pending_retry", "Scan paused"},
		{"wal_backpressure", "Scan paused"},
	} {
		var out bytes.Buffer
		printScanReport(&out, ScanReport{
			Directory: root, StateDir: manualState(config.Paths{StateDir: base}, root), Outcome: tc.outcome, SleepMS: 25,
			Inventory: state.Summary{SkippedEntries: 2, DirectoryErrors: 1},
		})
		flat := strings.Join(strings.Fields(out.String()), " ")
		for _, want := range []string{tc.headline, "Directory errors 1", "Skipped entries 2", "do not prove complete coverage or current contents"} {
			if !strings.Contains(flat, want) {
				t.Fatal(want, out.String())
			}
		}
		commands := 0
		for _, line := range strings.Split(out.String(), "\n") {
			if !strings.HasPrefix(line, "  rydd ") {
				continue
			}
			commands++
			// A harmless shell function records the suggested command's exact
			// arguments. No scanner, filesystem traversal or state write runs.
			raw, err := exec.Command("sh", "-c", "rydd() { printf '%s\\000' \"$@\"; }; "+line).Output()
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(string(raw), "\x00")
			if len(args) < 6 || args[0] != "--data-dir" || args[1] != base || args[3] != "-d" || args[4] != root {
				t.Fatal(args)
			}
			if args[2] == "scan" && (args[5] != "-s" || args[6] != "25") {
				t.Fatal(args)
			}
		}
		wantCommands := 3
		if tc.outcome == "queue_drained" {
			wantCommands = 2
		}
		if commands != wantCommands || strings.Contains(out.String(), tc.outcome) || !strings.Contains(out.String(), " review -d ") {
			t.Fatal(out.String())
		}
	}
}

func TestHumanMeasureResults(t *testing.T) {
	for _, outcome := range []string{"complete", "batch_limit", "time_limit", "wal_backpressure"} {
		var out bytes.Buffer
		printMeasureReport(&out, MeasureReport{Directory: "/fixture/root", StateDir: filepath.Join("/fixture/state", "manual", "id"), Complete: outcome == "complete", Outcome: outcome})
		flat := strings.Join(strings.Fields(out.String()), " ")
		if !strings.Contains(flat, "Current contents have not been checked") || !strings.Contains(out.String(), "rydd --data-dir '/fixture/state' report -d '/fixture/root'") {
			t.Fatal(out.String())
		}
		if outcome != "complete" && (!strings.Contains(out.String(), "More size calculations remain. Progress is saved.") || !strings.Contains(out.String(), "measure -d '/fixture/root'") || strings.Contains(out.String(), outcome)) {
			t.Fatal(out.String())
		}
		if outcome == "complete" && (!strings.HasPrefix(out.String(), "Saved size calculations are complete.") || strings.Contains(out.String(), "Continue the calculation")) {
			t.Fatal(out.String())
		}
	}
}

func TestHumanCommandControlBytes(t *testing.T) {
	path := "/fixture/資料\x1b[31m\xff\n"
	quoted := shellQuote(path)
	if strings.ContainsAny(quoted, "\x1b\n\xff") {
		t.Fatal("terminal control bytes in suggested command", quoted)
	}
	for _, shell := range []string{"bash", "zsh"} {
		t.Run(shell, func(t *testing.T) {
			binary, err := exec.LookPath(shell)
			if err != nil {
				t.Skip("shell not installed")
			}
			raw, err := exec.Command(binary, "-c", "printf '%s' "+quoted).Output()
			if err != nil || string(raw) != path {
				t.Fatalf("path bytes changed: %q, %v", raw, err)
			}
		})
	}
}
