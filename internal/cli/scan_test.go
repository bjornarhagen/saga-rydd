package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

func TestScanRefreshStrictArgumentsAndOperandRouting(t *testing.T) {
	for _, tc := range []struct {
		args, want []string
		selected   bool
	}{
		{[]string{"-d", "--refresh", "--now"}, []string{"-d", "--refresh", "--now"}, false},
		{[]string{"--directory", "--refresh", "--refresh=true"}, []string{"--directory", "--refresh"}, true},
		{[]string{"-d=--refresh", "--refresh=false"}, []string{"-d=--refresh"}, false},
		{[]string{"-s", "--refresh", "-d", "root"}, []string{"-s", "--refresh", "-d", "root"}, false},
		{[]string{"--sleep", "--refresh", "--refresh"}, []string{"--sleep", "--refresh"}, true},
		{[]string{"--", "--refresh"}, []string{"--", "--refresh"}, false},
	} {
		got, selected, err := scanRefreshArgs(tc.args)
		if err != nil || selected != tc.selected || !reflect.DeepEqual(got, tc.want) {
			t.Fatal(tc.args, got, selected, err)
		}
	}
	for _, args := range [][]string{
		{"--refresh", "--refresh=false"}, {"--refresh=false", "--refresh=true"},
		{"--refresh=TRUE"}, {"--refresh=1"}, {"--refresh="}, {"--refresh", "false"},
		{"unexpected", "--refresh"}, {"--refresh", "--", "--refresh"},
		{"--refresh", "-d", "root", "-s", "--refresh"},
		{"--refresh", "-d", "root", "--s", "--json"},
		{"--refresh", "-d", "root", "-sleep", "--json"},
		{"--refresh", "-d", "root", "--now", "-s", "1"},
		{"--refresh", "-d", "root", "--directory", "root"},
	} {
		base := filepath.Join(t.TempDir(), "not-created")
		var out, errOut bytes.Buffer
		argv := append([]string{"--data-dir", base, "scan"}, args...)
		argv = append(argv, "--json")
		if code := Run(context.Background(), argv, &out, &errOut); code != 2 {
			t.Fatal(args, code, out.String(), errOut.String())
		}
		if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid refresh created state", args, err)
		}
	}
}

func TestScanRefreshCapabilitiesAndKnownMachineFailures(t *testing.T) {
	advertised := capabilities()
	features := advertised["features"].(map[string]bool)
	if !features["manual_scan_refresh"] {
		t.Fatal("refresh is not discoverable")
	}
	codes := advertised["error_codes"].([]string)
	for _, code := range []string{"manual_refresh_failed", "manual_refresh_unknown", "manual_refresh_scope", "manual_refresh_invalid"} {
		count := 0
		for _, declared := range codes {
			if declared == code {
				count++
			}
		}
		if count != 1 {
			t.Fatal("refresh error code must be listed once", code, count)
		}
	}
	receipt := &state.ManualRefreshReceipt{Contract: "inventory_manual_refresh_v1", InventoryIncarnationID: strings.Repeat("a", 64), RootID: 1, RequestGeneration: 1, Phase: "unknown", ActivatedRootJobID: 1, TerminalQualification: "activation_lineage_unknown"}
	for _, tc := range []struct {
		err  error
		code string
	}{{state.ErrManualRefreshUnknown, "manual_refresh_unknown"}, {state.ErrManualRefreshScope, "manual_refresh_scope"}, {state.ErrManualRefreshInvalid, "manual_refresh_invalid"}, {context.Canceled, "canceled"}, {io.ErrClosedPipe, "manual_refresh_failed"}} {
		var out, errOut bytes.Buffer
		if scanRefreshMachineFailure(&out, &errOut, ScanReport{}, receipt, tc.err) != 1 {
			t.Fatal(tc.code)
		}
		var reply struct {
			OK      bool                        `json:"ok"`
			Receipt *state.ManualRefreshReceipt `json:"scan_refresh"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.OK || reply.Receipt == nil || *reply.Receipt != *receipt || reply.Error.Code != tc.code {
			t.Fatal("known identity or classification was lost", out.String(), err)
		}
	}
}

func TestScanRefreshFalseKeepsLegacyReplies(t *testing.T) {
	for _, tail := range [][]string{{"--s", "--json"}, {"-sleep", "--json"}, {"-s", "-1", "--json"}, {"--", "--json"}} {
		base := filepath.Join(t.TempDir(), "not-created")
		var before, beforeErr, after, afterErr bytes.Buffer
		prefix := []string{"--data-dir", base, "scan", "-d", "/generated-not-opened"}
		x := append(append([]string{}, prefix...), tail...)
		y := append(append(append([]string{}, prefix...), "--refresh=false"), tail...)
		code := Run(context.Background(), x, &before, &beforeErr)
		falseCode := Run(context.Background(), y, &after, &afterErr)
		if code != falseCode || !bytes.Equal(before.Bytes(), after.Bytes()) || !bytes.Equal(beforeErr.Bytes(), afterErr.Bytes()) {
			t.Fatal("false changed legacy malformed routing", tail, code, falseCode, before.String(), after.String(), beforeErr.String(), afterErr.String())
		}
	}
	ctx := context.Background()
	root := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	// Warm the same exact store so initialization is not compared with a revisit.
	if _, err = scan(ctx, []string{"-d", root, "--now"}, paths, io.Discard); err != nil {
		t.Fatal(err)
	}
	var legacy, explicitFalse bytes.Buffer
	r, err := scan(ctx, []string{"-d", root, "--now"}, paths, &legacy)
	if err != nil {
		t.Fatal(err)
	}
	printScanReport(&legacy, r)
	s, receipt, err := scanWithRefresh(ctx, []string{"-d", root, "--now", "--refresh=false"}, paths, &explicitFalse)
	if err != nil || receipt != nil {
		t.Fatal(s, receipt, err)
	}
	printScanReport(&explicitFalse, s)
	if !reflect.DeepEqual(r, s) || !bytes.Equal(legacy.Bytes(), explicitFalse.Bytes()) {
		t.Fatal("false changed the complete legacy human result", r, s, legacy.String(), explicitFalse.String())
	}
	var plainJSON, falseJSON, errOut bytes.Buffer
	a := []string{"--data-dir", paths.StateDir, "scan", "-d", root, "--now", "--json"}
	if Run(ctx, a, &plainJSON, &errOut) != 0 {
		t.Fatal(errOut.String())
	}
	b := append(append([]string{}, a...), "--refresh=false")
	if Run(ctx, b, &falseJSON, &errOut) != 0 {
		t.Fatal(errOut.String())
	}
	if !bytes.Equal(plainJSON.Bytes(), falseJSON.Bytes()) || bytes.Contains(falseJSON.Bytes(), []byte("scan_refresh")) {
		t.Fatal("false changed the complete legacy JSON result", plainJSON.String(), falseJSON.String())
	}
}

func TestScanRefreshGeneratedDetailedAndCompactCompletion(t *testing.T) {
	for _, mode := range []string{"detailed", "compact"} {
		t.Run(mode, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "node_modules")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "one"), []byte("hello"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "two"), []byte("1234567"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(filepath.Join(root, "one"), filepath.Join(root, "alias")); err != nil {
				t.Fatal(err)
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"-d", root, "--now", "--" + mode, "--refresh"}
			r, receipt, err := scanWithRefresh(context.Background(), args, paths, io.Discard)
			if err != nil || receipt == nil || receipt.Phase != "completed" || !receipt.RootListingEOFWitness || receipt.ObservedRootGeneration <= 0 || receipt.RequestGeneration != 1 || r.Mode != "new_pass" || r.Outcome != "queue_drained" || r.Batches != 2 || r.Compact != (mode == "compact") {
				t.Fatal(r, receipt, err)
			}
			if err = scanRefreshReceiptBound(receipt); err != nil {
				t.Fatal(err)
			}
			rep, err := report(context.Background(), []string{"-d", root}, paths)
			if err != nil || rep.Directory == nil || rep.Directory.LogicalBytes == nil || *rep.Directory.LogicalBytes != 17 || rep.Directory.RepeatedInodes != 1 {
				t.Fatal("independent saved size/hardlink oracle", rep, err)
			}
			_, next, err := scanWithRefresh(context.Background(), args, paths, io.Discard)
			if err != nil || next.RequestGeneration != 2 || next.RootID != receipt.RootID || next.InventoryIncarnationID != receipt.InventoryIncarnationID || next.Phase != "completed" {
				t.Fatal("one new explicit request must advance once", next, err)
			}
		})
	}
}

func TestScanRefreshKnownReceiptSurvivesProgressCloseAndReplyFailures(t *testing.T) {
	for _, failure := range []string{"progress_short", "progress_error", "after_activation", "after_first_batch", "store_close"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			for _, name := range []string{"one", "two"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte("a"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("generated selected boundary refusal")
			hooks := scanRefreshHooks{}
			var progress io.Writer = io.Discard
			switch failure {
			case "progress_short":
				progress = serviceShortWriter{}
			case "progress_error":
				progress = serviceFailWriter{}
			case "after_activation":
				hooks.afterActivation = func(*state.Store, *state.ManualRefreshActivation, *state.ManualRefreshReceipt) error { return injected }
			case "after_first_batch":
				hooks.afterCommit = func(*state.ManualRefreshReceipt) error { return injected }
			case "store_close":
				hooks.closeStore = func(w *state.Store) error { return errors.Join(w.Close(), injected) }
			}
			_, receipt, err := scanSelectedRefresh(context.Background(), []string{"-d", root, "--now"}, paths, progress, hooks)
			if err == nil || receipt == nil || receipt.RequestGeneration != 1 || receipt.RootID <= 0 || receipt.InventoryIncarnationID == "" {
				t.Fatal("known request was lost", receipt, err)
			}
			if failure == "after_first_batch" && (receipt.ObservedRootGeneration <= 0 || receipt.RootListingEOFWitness) {
				t.Fatal("partial witness fixture did not remain partial", receipt)
			}
			if failure == "store_close" && (receipt.Phase != "completed" || !receipt.RootListingEOFWitness) {
				t.Fatal("close failure erased saved completion evidence", receipt)
			}
			var out, errOut bytes.Buffer
			if scanRefreshMachineFailure(&out, &errOut, ScanReport{}, receipt, err) != 1 || !bytes.Contains(out.Bytes(), []byte("\"scan_refresh\"")) || !bytes.Contains(out.Bytes(), []byte(receipt.InventoryIncarnationID)) {
				t.Fatal(out.String(), errOut.String())
			}
		})
	}
	for _, failure := range []string{"short_reply", "late_cancel"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			base := filepath.Join(t.TempDir(), "state")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var output io.Writer = serviceShortWriter{}
			if failure == "late_cancel" {
				output = serviceCancelWriter{cancel: cancel}
			}
			var errOut bytes.Buffer
			if code := Run(ctx, []string{"--data-dir", base, "scan", "-d", root, "--refresh", "--now", "--json"}, output, &errOut); code != 1 || !strings.Contains(errOut.String(), "Manual refresh request 1") {
				t.Fatal(code, errOut.String())
			}
			w, err := state.OpenWriter(context.Background(), manualState(config.Paths{StateDir: base}, root))
			if err != nil {
				t.Fatal(err)
			}
			_, saved, err := w.PreflightManualRefresh(context.Background(), []byte(root), nil)
			closeErr := w.Close()
			if err != nil || closeErr != nil || saved == nil || saved.Phase != "completed" || saved.RequestGeneration != 1 || !saved.RootListingEOFWitness {
				t.Fatal("reply failure undid historical evidence", saved, err, closeErr)
			}
		})
	}
}

func TestScanRefreshDrainAndFutureRetry(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(fmt.Sprint(future), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			for _, name := range []string{"a", "b"} {
				if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, name, "old"), []byte("old"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = scan(ctx, []string{"-d", root, "--now"}, paths, io.Discard); err != nil {
				t.Fatal(err)
			}
			w, err := state.OpenWriter(ctx, manualState(paths, root))
			if err != nil {
				t.Fatal(err)
			}
			if err = w.SeedInventory(ctx); err != nil {
				w.Close()
				t.Fatal(err)
			}
			scanner, err := inventory.New([]string{root}, nil, []string{paths.StateDir})
			if err != nil {
				w.Close()
				t.Fatal(err)
			}
			var completedChild []byte
			for completed := 0; completed < 2; {
				j, e := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
				if e != nil || j == nil {
					t.Fatal(j, e)
				}
				b, e := scanner.Next(ctx, *j)
				if e != nil {
					t.Fatal(e)
				}
				if e = w.CommitScan(ctx, *j, b); e != nil {
					t.Fatal(e)
				}
				if b.Complete {
					completed++
					if string(j.Path) != "." {
						completedChild = bytes.Clone(j.Path)
					}
				}
			}
			scanner.Close()
			if len(completedChild) == 0 {
				t.Fatal("no completed sibling")
			}
			if err = os.WriteFile(filepath.Join(root, string(completedChild), "new"), []byte("new"), 0600); err != nil {
				t.Fatal(err)
			}
			var pending *state.Job
			due := time.Now().Add(time.Hour)
			if future {
				pending, err = w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
				if err != nil || pending == nil {
					t.Fatal(pending, err)
				}
				if err = w.FinishJob(ctx, *pending, false, pending.Cursor, due, "generated retry"); err != nil {
					t.Fatal(err)
				}
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			r, receipt, err := scanWithRefresh(ctx, []string{"-d", root, "--refresh", "--now"}, paths, io.Discard)
			if err != nil || receipt == nil {
				t.Fatal(r, receipt, err)
			}
			if future {
				if receipt.Phase != "pending" || r.Mode != "resume" || r.Batches != 0 || r.Outcome != "pending_retry" {
					t.Fatal("future work was reset or polled", r, receipt)
				}
				w, err = state.OpenWriter(ctx, r.StateDir)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				actualDue, e := w.NextJobDue(ctx, []string{state.ScanKind})
				if e != nil || !actualDue.Equal(due) {
					t.Fatal(actualDue, due, e)
				}
				j, e := w.ClaimJob(ctx, []string{state.ScanKind}, due.Add(time.Second), time.Minute)
				if e != nil || j == nil || j.ID != pending.ID || !bytes.Equal(j.Cursor, pending.Cursor) || j.Attempts != pending.Attempts+1 {
					t.Fatal("saved retry evidence changed", j, pending, e)
				}
			} else {
				if receipt.Phase != "completed" || r.Mode != "new_pass" || r.Outcome != "queue_drained" {
					t.Fatal(r, receipt)
				}
				rep, e := report(ctx, []string{"-d", root}, paths)
				if e != nil || rep.Directory == nil || rep.Directory.LogicalBytes == nil || *rep.Directory.LogicalBytes != 9 {
					t.Fatal("completed sibling was not revisited after drain", rep, e)
				}
			}
		})
	}
}

func TestScanRefreshSourceFaultAndCancellationKeepWork(t *testing.T) {
	for _, action := range []string{"source_fault", "cancel_after_partial_commit"} {
		t.Run(action, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "generated-root")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"one", "two"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte("a"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := scanRefreshHooks{}
			moved := root + "-moved"
			if action == "source_fault" {
				hooks.afterActivation = func(*state.Store, *state.ManualRefreshActivation, *state.ManualRefreshReceipt) error {
					return os.Rename(root, moved)
				}
			} else {
				hooks.afterCommit = func(*state.ManualRefreshReceipt) error { cancel(); return nil }
			}
			r, receipt, err := scanSelectedRefresh(ctx, []string{"-d", root, "--now"}, paths, io.Discard, hooks)
			if receipt == nil || receipt.Phase != "active" || receipt.RootListingEOFWitness {
				t.Fatal(r, receipt, err)
			}
			if action == "source_fault" {
				if err != nil || r.Outcome != "pending_retry" || r.Inventory.DirectoryErrors != 1 {
					t.Fatal(r, err)
				}
				if err = os.Rename(moved, root); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.Canceled) || receipt.ObservedRootGeneration <= 0 {
				t.Fatal(receipt, err)
			}
			before, err := state.OpenReader(context.Background(), r.StateDir)
			if err != nil {
				t.Fatal(err)
			}
			oldSummary, err := before.Summary(context.Background())
			closeErr := before.Close()
			if err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			resumed, unknown, err := scanWithRefresh(context.Background(), []string{"-d", root, "--refresh", "--now"}, paths, io.Discard)
			if !errors.Is(err, state.ErrManualRefreshUnknown) || unknown == nil || unknown.Phase != "unknown" || unknown.RequestGeneration != receipt.RequestGeneration || unknown.ObservedRootGeneration != receipt.ObservedRootGeneration || unknown.RootListingEOFWitness || resumed.Batches != 0 {
				t.Fatal("reopen manufactured selected evidence", resumed, unknown, err)
			}
			after, err := state.OpenReader(context.Background(), r.StateDir)
			if err != nil {
				t.Fatal(err)
			}
			newSummary, err := after.Summary(context.Background())
			closeErr = after.Close()
			if err != nil || closeErr != nil || oldSummary.PendingJobs != newSummary.PendingJobs || oldSummary.RunningJobs != newSummary.RunningJobs || oldSummary.Entries != newSummary.Entries {
				t.Fatal("unknown discovery changed saved work", oldSummary, newSummary, err, closeErr)
			}
		})
	}
}

// The output observer is bounded and cannot hold the child on a blocked channel.
// Seeing the banner proves SeedInventory returned; the first entry can already
// have been inspected. Two or more direct files prevent immediate root EOF.
type refreshOldOutput struct {
	mu    sync.Mutex
	data  bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (w *refreshOldOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) > 32768-w.data.Len() {
		return 0, io.ErrShortBuffer
	}
	n, err := w.data.Write(p)
	if w.ready != nil && bytes.Contains(w.data.Bytes(), []byte("Starting a new pass.\n")) {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}

func (w *refreshOldOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.String()
}

func runRefreshOldCLI(t *testing.T, binary string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	var output refreshOldOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		t.Fatal("older fixture command did not finish", err, output.String())
	}
	return output.String()
}

func stopRefreshOldCLIAtNewPass(t *testing.T, binary string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	output := &refreshOldOutput{ready: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var signalErr error
	var early bool
	var waited error
	select {
	case <-output.ready:
		signalErr = cmd.Process.Signal(os.Interrupt)
		waited = <-wait
	case waited = <-wait:
		early = true
	case <-ctx.Done():
		waited = <-wait
		early = true
	}
	if early || signalErr != nil {
		t.Fatal("older fixture did not retain the post-seed cancellation boundary", signalErr, waited, output.String())
	}
	var failed *exec.ExitError
	if !errors.As(waited, &failed) || failed.ExitCode() != 1 {
		t.Fatal("older scan did not report cancellation after reaping", waited, output.String())
	}
}

func TestScanRefreshOldCLICannotCreateSelectedWitness(t *testing.T) {
	binary := os.Getenv("RYDD_REFRESH_OLD_CLI")
	if binary == "" {
		t.Skip("explicit fixed schema-14 native executable fixture")
	}
	info, err := os.Lstat(binary)
	if err != nil || !filepath.IsAbs(binary) || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Size() <= 0 || info.Size() > 64<<20 {
		t.Fatal("old fixture must be a direct absolute bounded executable", err)
	}
	var version struct {
		OK    bool `json:"ok"`
		Build struct {
			Revision     string `json:"revision"`
			SourceStatus string `json:"source_status"`
			GOOS         string `json:"goos"`
			GOARCH       string `json:"goarch"`
		} `json:"build_metadata"`
	}
	if err = json.Unmarshal([]byte(runRefreshOldCLI(t, binary, "--json", "--version")), &version); err != nil || !version.OK || version.Build.Revision != "733deb60d31d9eb5d11cbc663505c4c166c210ab" || version.Build.SourceStatus != "clean" || version.Build.GOOS != runtime.GOOS || version.Build.GOARCH != runtime.GOARCH {
		t.Fatal("old fixture provenance/platform differs", version, err)
	}
	for _, name := range []string{"missing_first_generation", "first_generation_without_selected_eof"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			count := 2
			if name == "first_generation_without_selected_eof" {
				count = state.MaxBatchEntries + 2
			}
			for i := 0; i < count; i++ {
				if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), []byte("a"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			var original *state.ManualRefreshReceipt
			boundary := errors.New("generated fixture ends the activating invocation")
			hooks := scanRefreshHooks{afterActivation: func(w *state.Store, a *state.ManualRefreshActivation, receipt *state.ManualRefreshReceipt) error {
				copyReceipt := *receipt
				original = &copyReceipt
				if name == "first_generation_without_selected_eof" {
					j, e := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
					if e != nil || j == nil {
						t.Fatal(j, e)
					}
					scanner, e := inventory.New([]string{root}, nil, []string{paths.StateDir})
					if e != nil {
						t.Fatal(e)
					}
					defer scanner.Close()
					batch, e := scanner.Next(ctx, *j)
					if e != nil || batch.Complete {
						t.Fatal("fixture requires a genuine partial first batch", batch, e)
					}
					original, e = w.CommitManualRefreshScan(ctx, a, *j, batch)
					if e != nil {
						t.Fatal(e)
					}
				}
				return boundary
			}}
			_, known, err := scanSelectedRefresh(ctx, []string{"-d", root, "--now"}, paths, io.Discard, hooks)
			if !errors.Is(err, boundary) || original == nil || known == nil || original.Phase != "active" || original.RootListingEOFWitness || (original.ObservedRootGeneration > 0) != (name == "first_generation_without_selected_eof") {
				t.Fatal(original, known, err)
			}
			// Only baseline configuration fields are sent to the genuine older CLI.
			configBytes := []byte("version = " + strconv.Itoa(config.Version) + "\nroots = [" + strconv.Quote(root) + "]\n")
			if err = os.WriteFile(paths.ConfigFile, configBytes, 0600); err != nil {
				t.Fatal(err)
			}
			text := runRefreshOldCLI(t, binary, "--data-dir", paths.StateDir, "scan", "-d", root, "--now")
			if !strings.Contains(text, "Scan pass finished. Results are saved.") {
				t.Fatal("older writer did not drain its ordinary work", text)
			}
			w, err := state.OpenWriter(ctx, manualState(paths, root))
			if err != nil {
				t.Fatal(err)
			}
			_, retained, err := w.PreflightManualRefresh(ctx, []byte(root), nil)
			summary, summaryErr := w.Summary(ctx)
			if err != nil || summaryErr != nil || retained == nil || *retained != *original || summary.PendingJobs != 0 || summary.RunningJobs != 0 {
				w.Close()
				t.Fatal("older writer changed the selected slot or failed to drain", retained, original, summary, err, summaryErr)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			stopRefreshOldCLIAtNewPass(t, binary, "--data-dir", paths.StateDir, "scan", "-d", root, "-s", "60000")
			w, err = state.OpenWriter(ctx, manualState(paths, root))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if _, err = w.RecoverJobs(ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
			j, err := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
			if err != nil || j == nil || j.ID != original.ActivatedRootJobID || string(j.Path) != "." {
				t.Fatal("genuine old replacement did not survive with the recycled root ID", j, original, err)
			}
			// This is an API-permitted positive-generation supplement, not a
			// claim that the older executable's crypto/rand actually collided.
			scanner, err := inventory.New([]string{root}, nil, []string{paths.StateDir})
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			batch, err := scanner.Next(ctx, *j)
			if err != nil || batch.Generation <= 0 {
				t.Fatal(batch, err)
			}
			cursor := bytes.Clone(batch.Cursor)
			if original.ObservedRootGeneration > 0 {
				batch.Generation = original.ObservedRootGeneration
			}
			if err = w.CommitScan(ctx, *j, batch); err != nil || !bytes.Equal(batch.Cursor, cursor) {
				t.Fatal("ordinary generated batch/cursor refused", err)
			}
			before, err := w.Summary(ctx)
			if err != nil {
				t.Fatal(err)
			}
			due, err := w.NextJobDue(ctx, []string{state.ScanKind})
			if err != nil {
				t.Fatal(err)
			}
			session, prior, err := w.PreflightManualRefresh(ctx, []byte(root), nil)
			if err != nil || prior == nil || *prior != *original {
				t.Fatal(prior, original, err)
			}
			unknown, err := w.RequestManualRefresh(ctx, session)
			if !errors.Is(err, state.ErrManualRefreshUnknown) || unknown == nil || unknown.Phase != "unknown" || unknown.RootListingEOFWitness || unknown.ObservedRootGeneration != original.ObservedRootGeneration || unknown.RequestGeneration != original.RequestGeneration || unknown.InventoryIncarnationID != original.InventoryIncarnationID || unknown.RootID != original.RootID || unknown.ActivatedRootJobID != original.ActivatedRootJobID {
				t.Fatal("recycled ID/generation manufactured selected evidence", unknown, original, err)
			}
			_, activation, err := w.TryActivateManualRefresh(ctx, session, time.Now())
			if !errors.Is(err, state.ErrManualRefreshUnknown) || activation != nil {
				t.Fatal("unknown request seeded another pass", activation, err)
			}
			after, err := w.Summary(ctx)
			nextDue, dueErr := w.NextJobDue(ctx, []string{state.ScanKind})
			if err != nil || dueErr != nil || before.PendingJobs != after.PendingJobs || before.RunningJobs != after.RunningJobs || before.Entries != after.Entries || !nextDue.Equal(due) {
				t.Fatal("unknown discovery changed remaining ordinary work", before, after, due, nextDue, err, dueErr)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			resumed, selectedUnknown, err := scanWithRefresh(ctx, []string{"-d", root, "--refresh", "--now"}, paths, io.Discard)
			if !errors.Is(err, state.ErrManualRefreshUnknown) || selectedUnknown == nil || selectedUnknown.Phase != "unknown" || selectedUnknown.RequestGeneration != original.RequestGeneration || selectedUnknown.RootListingEOFWitness || resumed.Batches != 0 {
				t.Fatal("selected CLI reopened unknown work as a new pass", resumed, selectedUnknown, err)
			}
		})
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

func TestManualCompactScanAndDetailedOverride(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	nm := filepath.Join(root, "node_modules")
	if err := os.MkdirAll(filepath.Join(nm, "nested", "node_modules"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(root, "package.json"), filepath.Join(nm, "a"), filepath.Join(nm, "nested", "node_modules", "b")} {
		if err := os.WriteFile(p, []byte("hello"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(nm, "a"), filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}
	paths, err := config.ResolvePaths(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := scan(ctx, []string{"-d", root, "--compact", "--now"}, paths, &bytes.Buffer{})
	if err != nil || !r.Compact || r.Outcome != "queue_drained" || r.AllocationBatches == 0 {
		t.Fatal(r, err)
	}
	rep, err := report(ctx, []string{"-d", root}, paths)
	if err != nil || rep.Directory == nil || *rep.Directory.LogicalBytes != 20 || rep.Directory.CompactedFiles != 2 || rep.Directory.RepeatedInodes != 1 || rep.Directory.AllocatedSizeSource != "cached_reduction" {
		t.Fatal(rep, err)
	}
	var compactAllocated int64 = *rep.Directory.AllocatedBytes
	// Saved mode survives an invocation without flags.
	r, err = scan(ctx, []string{"-d", root, "--now"}, paths, &bytes.Buffer{})
	if err != nil || !r.Compact {
		t.Fatal(r, err)
	}
	r, err = scan(ctx, []string{"-d", root, "--detailed", "--now"}, paths, &bytes.Buffer{})
	if err != nil || r.Compact {
		t.Fatal(r, err)
	}
	rep, err = report(ctx, []string{"-d", root}, paths)
	if err != nil || *rep.Directory.LogicalBytes != 20 || *rep.Directory.AllocatedBytes != compactAllocated || rep.Directory.CompactedFiles != 0 {
		t.Fatal(rep, err)
	}
}
