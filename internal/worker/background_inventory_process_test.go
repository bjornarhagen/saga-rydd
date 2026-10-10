package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type compactProcessCursor struct {
	JobID  int64  `json:"job_id"`
	Cursor []byte `json:"cursor"`
}

func TestCompactBackgroundProcessHelper(t *testing.T) {
	dir := os.Getenv("RYDD_COMPACT_FIXTURE_STATE")
	if dir == "" {
		return
	}
	root := os.Getenv("RYDD_COMPACT_FIXTURE_ROOT")
	restart := os.Getenv("RYDD_COMPACT_FIXTURE_RESTART") == "1"
	c := config.Default()
	c.Roots = []string{root}
	c.Scan.CompactInventory = true
	c.Scan.PauseOnBattery = false
	c.Scan.MetadataPerSecond = 100000
	o := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, powerCoordinator: fixturePowerCoordinator(nil, nil), cpuObserve: func() (time.Duration, error) { return 0, nil }, priorityRequest: func(context.Context) ThreadPriorityObservation { return newThreadPriorityObservation("fixture") }, Ready: func(s Snapshot) {
		if e := json.NewEncoder(os.Stdout).Encode(s); e != nil {
			t.Fatal(e)
		}
	}}
	if !restart {
		c.Scan.MaxScanChunksPerDay = 1
	}
	var printed atomic.Bool
	o.scannerNext = func(ctx context.Context, s *inventory.Scanner, j state.Job, p inventory.APIPermit) (state.ScanBatch, error) {
		first := printed.CompareAndSwap(false, true)
		if first && restart {
			if e := json.NewEncoder(os.Stdout).Encode(compactProcessCursor{j.ID, j.Cursor}); e != nil {
				return state.ScanBatch{}, e
			}
		}
		b, e := s.NextPermitted(ctx, j, p)
		if first && !restart && e == nil {
			if x := json.NewEncoder(os.Stdout).Encode(compactProcessCursor{j.ID, b.Cursor}); x != nil {
				return state.ScanBatch{}, x
			}
		}
		return b, e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx, dir, c, o); err != nil {
		t.Fatal(err)
	}
}

func spawnCompactWorker(t *testing.T, dir, root string, restart bool) (*childWorker, Snapshot) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestCompactBackgroundProcessHelper$")
	cmd.Env = append(os.Environ(), "RYDD_COMPACT_FIXTURE_STATE="+dir, "RYDD_COMPACT_FIXTURE_ROOT="+root)
	if restart {
		cmd.Env = append(cmd.Env, "RYDD_COMPACT_FIXTURE_RESTART=1")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	child := &childWorker{cmd: cmd, lines: make(chan string, 8), done: make(chan error, 1)}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 8192)
		for scanner.Scan() {
			child.lines <- scanner.Text()
		}
		close(child.lines)
		child.done <- cmd.Wait()
		close(child.done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = cmd.Process.Kill()
		select {
		case <-child.done:
		case <-time.After(5 * time.Second):
			t.Error("compact child did not exit")
		}
	})
	var ready Snapshot
	if err = json.Unmarshal([]byte(child.line(t)), &ready); err != nil {
		t.Fatal("compact child readiness", err)
	}
	return child, ready
}

func TestWorkerCompactSIGKILLRetainsPartialModeAndCursor(t *testing.T) {
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(temp, "node_modules")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if err = os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), []byte("generated source"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	logical, allocated, _, beforeBody := compactWorkerOracle(t, root)
	dir := filepath.Join(temp, "state")
	child, ready := spawnCompactWorker(t, dir, root, false)
	if ready.InventoryMode != nil || ready.InventoryMetrics != nil {
		t.Fatal("listener readiness ran staged mode/source setup", ready)
	}
	var saved compactProcessCursor
	if err = json.Unmarshal([]byte(child.line(t)), &saved); err != nil || saved.JobID == 0 || len(saved.Cursor) == 0 {
		t.Fatal(saved, err)
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var settled Snapshot
	waitUntil(t, func() bool {
		settled = control(t, dir, "status")
		if settled.WaitReason != "daily_chunk_limit" {
			return false
		}
		m, e := r.MeasureDirectory(context.Background(), root)
		if e != nil {
			return false
		}
		f, e := r.CPUFeedback(context.Background())
		return e == nil && m.CompactedFiles == state.MaxBatchEntries && m.Status == "partial" && f.Status == "observed" && f.Window != nil && f.Window.JobID == saved.JobID && f.Window.CPUTimeNS != nil && *f.Window.CPUTimeNS == 0
	})
	// The exact committed partial batch and settled CPU window, rather than
	// listener readiness, establish completed mode setup and source admission.
	if settled.InventoryMode == nil || !settled.InventoryMode.Compact || settled.InventoryMetrics == nil || settled.ActiveJob != 0 || settled.CPUFeedback == nil || settled.CPUFeedback.Status != "observed" || settled.CPUFeedback.Window == nil || settled.CPUFeedback.Window.JobID != saved.JobID || settled.CPUFeedback.Window.CPUTimeNS == nil || *settled.CPUFeedback.Window.CPUTimeNS != 0 {
		t.Fatal("committed compact partial batch lacks initialized settled status", settled)
	}
	dueBefore, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
	if err != nil {
		t.Fatal(err)
	}
	if err = child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-child.done:
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal("child was not killed", err)
		}
		status, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatal("wrong process termination", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("kill/reap timed out")
	}
	afterKill, err := r.MeasureDirectory(context.Background(), root)
	if err != nil || afterKill.CompactedFiles != state.MaxBatchEntries || afterKill.Status != "partial" {
		t.Fatal(afterKill, err)
	}
	dueAfter, err := r.NextJobDue(context.Background(), []string{state.ScanKind})
	if err != nil || !dueAfter.Equal(dueBefore) {
		t.Fatal("process loss changed saved due", dueBefore, dueAfter, err)
	}
	child, restarted := spawnCompactWorker(t, dir, root, true)
	if restarted.InventoryMode != nil || restarted.InventoryMetrics != nil || restarted.CPUFeedback == nil || restarted.CPUFeedback.RecoveredUnknownWindows != 0 {
		t.Fatal("listener readiness ran setup or known terminal process loss invented unknown CPU", restarted)
	}
	var resumed compactProcessCursor
	if err = json.Unmarshal([]byte(child.line(t)), &resumed); err != nil || resumed.JobID != saved.JobID || !bytes.Equal(resumed.Cursor, saved.Cursor) {
		t.Fatal("restart did not retain exact saved job and cursor", saved, resumed, err)
	}
	initialized := control(t, dir, "status")
	if initialized.InventoryMode == nil || !initialized.InventoryMode.Compact || initialized.InventoryMetrics == nil || initialized.CPUFeedback == nil || initialized.CPUFeedback.RecoveredUnknownWindows != 0 {
		t.Fatal("exact resumed cursor lacks initialized compact source status", initialized)
	}
	// Keep a finite restart bound inside the child's unchanged 30-second lease.
	// The generic five-second lifecycle wait is too short for this completion
	// fixture on a concurrent native race runner. Observe cached drain status,
	// then perform one independent exact report instead of polling fallback
	// measurements while the saved reduction is still running.
	deadline := time.Now().Add(20 * time.Second)
	for {
		live := control(t, dir, "status")
		if live.WaitReason == "inventory_revisit" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("compact restart did not drain within its fixture bound", live)
		}
		time.Sleep(25 * time.Millisecond)
	}
	completed, err := r.MeasureDirectory(context.Background(), root)
	if err != nil || completed.Status != "recorded_complete" || completed.CompactedFiles != 400 || completed.LogicalBytes == nil || *completed.LogicalBytes != logical || completed.AllocatedBytes == nil || *completed.AllocatedBytes != allocated || completed.AllocatedSizeSource != "cached_reduction" {
		t.Fatal("restart reduction differs from independent native oracle", completed, err)
	}
	_, _, _, afterBody := compactWorkerOracle(t, root)
	if beforeBody != afterBody {
		t.Fatal("process/restart changed source bodies")
	}
	control(t, dir, "stop")
	waitExit(t, child.done)
}
