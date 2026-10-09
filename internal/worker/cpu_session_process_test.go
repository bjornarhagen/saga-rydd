package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type cpuChildOutput struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *cpuChildOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := len(p)
	if remaining := 2048 - b.data.Len(); remaining > 0 {
		b.data.Write(p[:min(remaining, count)])
	}
	return count, nil
}
func (b *cpuChildOutput) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.data.String() }

// This helper uses actual native SELF in an explicit generated process. Pure
// source capacity refusal and pause_on_battery=false prevent source/power calls.
func TestCPUSessionWorkerProcess(t *testing.T) {
	dir := os.Getenv("RYDD_CPU_SESSION_PROCESS_DIR")
	if dir == "" {
		t.Skip("generated process helper")
	}
	cfg := config.Default()
	cfg.Roots = []string{filepath.Join(dir, "generated-source")}
	cfg.Scan.CPUSessionCharges, cfg.Scan.PauseOnBattery = true, false
	cfg.Scan.APIAttemptsPerSecond = 1
	hooks := &cpuSessionHooks{begin: func(ctx context.Context, w *state.Store, start state.CPUSessionStart) (state.CPUSessionMarker, state.CPUChargeState, error) {
		marker, saved, err := w.BeginCPUSession(ctx, start)
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "begun"), []byte(strconv.FormatInt(saved.Generation, 10)), 0600)
		}
		return marker, saved, err
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Run(ctx, dir, cfg, Options{ExperimentalScan: true, WorkDuration: time.Second, cpuSessionHooks: hooks}); err != nil {
		t.Fatal(err)
	}
}

func TestCPUSessionWorkerSIGKILLRecoveryAndFreshNativePrefix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.MkdirAll(filepath.Join(dir, "generated-source"), 0700); err != nil {
		t.Fatal(err)
	}
	// The generated runtime root is inherited literally; no HOME reassignment.
	runtimeDir, err := os.MkdirTemp("/tmp", "cpu-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(runtimeDir) })
	t.Setenv("RYDD_RUNTIME_DIR", runtimeDir)
	launch := func(expected int64) (*exec.Cmd, <-chan error, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCPUSessionWorkerProcess$", "-test.count=1")
		cmd.Env = append(os.Environ(), "RYDD_CPU_SESSION_PROCESS_DIR="+dir)
		output := &cpuChildOutput{}
		cmd.Stderr, cmd.Stdout = output, output
		cmd.WaitDelay = time.Second
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait(); close(done) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("generated CPU worker child was not reaped")
			}
		})
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(filepath.Join(dir, "begun"))
			if err == nil && string(data) == strconv.FormatInt(expected, 10) {
				return cmd, done, cancel
			}
			select {
			case err := <-done:
				t.Fatal("child exited before fresh tracking", err, output.String())
			default:
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("child did not publish fresh generation", expected, output.String())
		return nil, nil, cancel
	}
	first, firstDone, firstCancel := launch(1)
	before := sessionSaved(t, dir)
	if before.Status != "active" || before.Generation != 1 || before.Session.Start.SelfCPUNS == nil {
		t.Fatal(before)
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-firstDone
	firstCancel()
	exit, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatal("SIGKILL was not a process loss", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("wrong process-loss evidence", err)
	}
	_, secondDone, secondCancel := launch(2)
	after := sessionSaved(t, dir)
	if after.Generation != 2 || after.Status != "active" || after.RecoveredSessions != 1 || after.UnknownTailSessions != 1 || after.Session.Start.SelfCPUNS == nil || after.Session.Start.Instance == before.Session.Start.Instance || after.ChargedCPUNS != before.ChargedCPUNS+*after.Session.Start.SelfCPUNS {
		t.Fatal("recovery/fresh prefix lost accounting", before, after)
	}
	if after.NextAllowedAt == nil || after.ClockHighWater == nil || after.NextAllowedAt.Before(after.ClockHighWater.Add(state.CPUChargesUnknownDelay-time.Second)) {
		t.Fatal("lost inherited recovery debt", after)
	}
	control(t, dir, "stop")
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restarted worker did not finish")
	}
	secondCancel()
	finished := sessionSaved(t, dir)
	if finished.Status != "finished" || finished.UnknownTailSessions != 2 {
		t.Fatal(fmt.Sprint(finished))
	}
}
