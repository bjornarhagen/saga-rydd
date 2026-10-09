package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Run the actual worker in a separate process so SIGKILL exercises kernel lock
// release, an orphaned socket, and recovery of a committed running lease.
func TestWorkerProcess(t *testing.T) {
	dir := os.Getenv("RYDD_TEST_WORKER_DIR")
	if dir == "" {
		return
	}
	c := config.Default()
	c.Roots = []string{"/synthetic"}
	options := Options{Ready: func(s Snapshot) { _ = json.NewEncoder(os.Stdout).Encode(s) }}
	// Recovery fixtures retain their existing bounded cadence. Native CPU
	// measurement and feedback are checked by the dedicated CPU fixtures.
	if os.Getenv("RYDD_TEST_WORKER_CPU_NATIVE") != "1" {
		options.cpuObserve = func() (time.Duration, error) { return 0, nil }
	}
	if root := os.Getenv("RYDD_TEST_SCAN_ROOT"); root != "" {
		c.Roots = []string{root}
		options.ExperimentalScan = true
		options.Interval = time.Second // Persisted cooldown survives the kill.
		if os.Getenv("RYDD_TEST_SCAN_FAST") == "1" {
			options.Interval = 5 * time.Millisecond
		} else {
			// Hold after the first committed batch regardless of disk speed.
			// The restarted fixture uses the default daily cap.
			c.Scan.MaxScanChunksPerDay = 1
		}
	}
	if os.Getenv("RYDD_TEST_WORKER_CLAIM") == "1" {
		options.Handlers = map[string]Handler{"fixture": func(ctx context.Context, j state.Job) (Result, error) {
			fmt.Println("claimed:" + string(j.Cursor))
			<-ctx.Done()
			return Result{}, ctx.Err()
		}}
	}
	if os.Getenv("RYDD_TEST_METADATA_SLOW") == "1" {
		c.Scan.MetadataPerSecond = 1
		c.Scan.MaxScanChunksPerDay = 288
	}
	if cap := os.Getenv("RYDD_TEST_METADATA_CAP"); cap != "" {
		value, err := strconv.ParseInt(cap, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		c.Scan.MetadataAttemptsPerDay = value
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx, dir, c, options); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataWorkerSIGKILLRetainsUnknownCharge(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(t.TempDir(), "state")
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RYDD_TEST_SCAN_ROOT", root)
	t.Setenv("RYDD_TEST_METADATA_SLOW", "1")
	t.Setenv("RYDD_TEST_WORKER_CPU_NATIVE", "1")
	child, ready := spawnWorker(t, dir, false)
	if ready.InventoryMetrics != nil || ready.Metadata == nil || ready.Metadata.Status != "untracked" {
		t.Fatal("eager source work before readiness", ready)
	}
	var admitted Snapshot
	waitUntil(t, func() bool {
		admitted = control(t, dir, "status")
		return admitted.ActiveJob != 0 && admitted.CPUFeedback != nil && admitted.CPUFeedback.Status == "pending" && admitted.InventoryMetrics != nil && admitted.InventoryMetrics.Throttled && admitted.Metadata.TotalCharges != nil && admitted.Metadata.TotalCharges.OutstandingReserved >= inventory.MaxAPIAttemptAllowance
	})
	reserved := admitted.Metadata.TotalCharges.Reserved
	if reserved <= inventory.MaxAPIAttemptAllowance {
		t.Fatal("missing construction charge", admitted)
	}
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-child.done:
		if err == nil {
			t.Fatal("SIGKILL succeeded normally")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("kill timed out")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Saved-only status retains outstanding receipts until a writer restarts.
	before, err := r.MetadataBudget(context.Background(), time.Now(), 20_000_000)
	if err != nil || before.TotalCharges.OutstandingReserved != reserved || before.TotalCharges.UnknownReserved != 0 {
		t.Fatal(before, err)
	}
	beforeCPU, err := r.CPUFeedback(context.Background())
	if err != nil || beforeCPU.Status != "pending" || beforeCPU.Window == nil || beforeCPU.Window.Token != admitted.CPUFeedback.Window.Token || beforeCPU.Window.CPUTimeNS != nil {
		t.Fatal("saved reader settled an interrupted CPU window", beforeCPU, err)
	}
	t.Setenv("RYDD_TEST_METADATA_CAP", strconv.FormatInt(reserved, 10))
	child, restarted := spawnWorker(t, dir, false)
	if restarted.RecoveredJobs != 1 || restarted.Metadata == nil || restarted.Metadata.TotalCharges.OutstandingReserved != 0 || restarted.Metadata.TotalCharges.UnknownReserved != reserved || restarted.InventoryMetrics != nil {
		t.Fatal("restart failed to recover charges before source work", restarted)
	}
	if restarted.CPUFeedback == nil || restarted.CPUFeedback.Status != "recovered_unknown" || restarted.CPUFeedback.RecoveredUnknownWindows != 1 || restarted.CPUFeedback.Window.Token != beforeCPU.Window.Token || restarted.CPUFeedback.Window.CPUTimeNS != nil || restarted.CPUFeedback.Window.ElapsedNS != nil || restarted.CPUFeedback.NextAllowedAt == nil || restarted.CPUFeedback.Window.SettledAt == nil || restarted.CPUFeedback.NextAllowedAt.Sub(*restarted.CPUFeedback.Window.SettledAt) != state.CPUUnknownRecoveryDelay || restarted.CPU == nil || restarted.CPU.Status != "not_recorded" || restarted.CPU.WindowCPUNS != nil {
		t.Fatal("restart invented a cross-process CPU delta or shortened unknown recovery", restarted)
	}
	waitUntil(t, func() bool {
		waiting := control(t, dir, "status")
		return waiting.WaitReason == "daily_metadata_limit" || waiting.WaitReason == "cpu_recovery_backoff"
	})
	control(t, dir, "pause")
	control(t, dir, "resume")
	snapshot := control(t, dir, "status")
	if snapshot.ActiveJob != 0 || snapshot.InventoryMetrics != nil || snapshot.Metadata.TotalCharges.Reserved != reserved || snapshot.Dispatch.Used != 1 {
		t.Fatal("restart refunded usage or burned another dispatch", snapshot)
	}
	if snapshot.CPUFeedback.Window.Token != beforeCPU.Window.Token || snapshot.CPUFeedback.RecoveredUnknownWindows != 1 || !snapshot.CPUFeedback.NextAllowedAt.Equal(*restarted.CPUFeedback.NextAllowedAt) {
		t.Fatal("controls renewed or cleared saved CPU recovery", snapshot.CPUFeedback)
	}
	control(t, dir, "stop")
	waitExit(t, child.done)
	summary, err := r.Summary(context.Background())
	if err != nil || summary.Entries != 0 || summary.CompleteDirectories != 0 || summary.DirectoryErrors != 0 {
		t.Fatal("killed tentative source batch escaped", summary, err)
	}
}

func TestInventoryWorkerKillAndComplete(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(t.TempDir(), "state")
	for i := 0; i < 301; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	childPath := root
	for i := 0; i < 20; i++ {
		childPath = filepath.Join(childPath, "directory")
		if err := os.Mkdir(childPath, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(childPath, "file"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RYDD_TEST_SCAN_ROOT", root)
	child, _ := spawnWorker(t, dir, false)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitUntil(t, func() bool {
		summary, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		feedback, err := r.CPUFeedback(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Source progress commits before CPU accounting. Kill only after this
		// known-zero terminal record, leaving interruption coverage to the
		// deliberately pending-window fixture above.
		return summary.Entries == 129 && summary.RunningJobs == 0 && feedback.Status == "observed" && feedback.Window != nil && feedback.Window.CPUTimeNS != nil && *feedback.Window.CPUTimeNS == 0
	})
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-child.done:
		if err == nil {
			t.Fatal("kill exited successfully")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("kill timed out")
	}
	t.Setenv("RYDD_TEST_SCAN_FAST", "1")
	child, _ = spawnWorker(t, dir, false)
	// This verifies recovery correctness, not throughput. Race-instrumented
	// SQLite on a shared CI runner can need more than five seconds for the
	// 21 directory passes. Keep a bounded deadline and report saved progress.
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case err := <-child.done:
			t.Fatalf("restarted worker exited before completion: %v", err)
		default:
		}
		summary, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		due, dueErr := r.NextJobDue(context.Background(), []string{state.ScanKind})
		if dueErr != nil {
			t.Fatal(dueErr)
		}
		if summary.Entries == 342 && summary.CompleteDirectories == 21 && summary.PendingJobs == 1 && summary.RunningJobs == 0 && summary.DirectoryErrors == 0 && due.After(time.Now().Add(23*time.Hour)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("inventory recovery did not finish: %+v", summary)
		}
		time.Sleep(20 * time.Millisecond)
	}
	control(t, dir, "stop")
	waitExit(t, child.done)
}

type childWorker struct {
	cmd   *exec.Cmd
	lines chan string
	done  chan error
}

func spawnWorker(t *testing.T, dir string, claim bool) (*childWorker, Snapshot) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcess$")
	cmd.Env = append(os.Environ(), "RYDD_TEST_WORKER_DIR="+dir)
	if claim {
		cmd.Env = append(cmd.Env, "RYDD_TEST_WORKER_CLAIM=1")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	child := &childWorker{cmd: cmd, lines: make(chan string, 8), done: make(chan error, 1)}
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			child.lines <- s.Text()
		}
		close(child.lines)
		child.done <- cmd.Wait()
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	var snapshot Snapshot
	if err := json.Unmarshal([]byte(child.line(t)), &snapshot); err != nil {
		t.Fatal("worker readiness", err)
	}
	return child, snapshot
}

func (c *childWorker) line(t *testing.T) string {
	t.Helper()
	select {
	case line, ok := <-c.lines:
		if !ok {
			t.Fatal("child worker exited before expected output")
		}
		return line
	case <-time.After(8 * time.Second):
		t.Fatal("child worker output timed out")
	}
	return ""
}

func TestProcessKillRecoveryAndSIGTERM(t *testing.T) {
	dir, _ := fixture(t)
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueJob(ctx, 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	j, err := w.ClaimJob(ctx, []string{"fixture"}, time.Now(), time.Minute)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if err := w.FinishJob(ctx, *j, false, []byte("saved-page"), time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	w.Close()
	child, initial := spawnWorker(t, dir, true)
	if line := child.line(t); line != "claimed:saved-page" {
		t.Fatal(line)
	}
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-child.done:
		if err == nil {
			t.Fatal("killed worker exited successfully")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("killed worker did not exit")
	}
	child, recovered := spawnWorker(t, dir, false)
	if recovered.RecoveredJobs != 1 || recovered.Instance == initial.Instance {
		t.Fatal(recovered)
	}
	control(t, dir, "pause")
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitExit(t, child.done)
	child, restarted := spawnWorker(t, dir, false)
	if !restarted.Paused || restarted.RecoveredJobs != 0 {
		t.Fatal(restarted)
	}
	control(t, dir, "stop")
	waitExit(t, child.done)
	w, err = state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	j, err = w.ClaimJob(ctx, []string{"fixture"}, time.Now(), time.Minute)
	if err != nil || j == nil || string(j.Cursor) != "saved-page" || j.Attempts != 2 {
		t.Fatal("checkpoint or retry state lost after crash", j, err)
	}
}
