package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestCPUPeriodWorkerKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_CPU_PERIOD_WORKER_CHILD")
	if dir == "" {
		t.Skip("owned generated subprocess fixture only")
	}
	cfg := config.Default()
	cfg.Roots = []string{filepath.Join(dir, "generated-root")}
	cfg.Scan.CPUSessionCharges = true
	cfg.Scan.CPUChargeSecondsPerHour = 1
	cfg.Scan.PauseOnBattery = false
	cfg.Scan.APIAttemptsPerSecond = 1
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hook := &cpuPeriodHooks{begin: func(ctx context.Context, w *state.Store, q state.CPULimitedSessionStart) (state.CPULimitedSessionMarker, state.CPUChargeState, state.CPUChargeAdmissionState, error) {
		marker, cpu, a, err := w.BeginCPUSessionLimited(ctx, q)
		if err == nil {
			fmt.Println("cpu-period-worker-ready:1")
			<-ctx.Done()
		}
		return marker, cpu, a, err
	}}
	err := Run(ctx, dir, cfg, Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuPeriodHooks: hook, cpuObserve: func() (time.Duration, error) { return 0, nil }, cpuSessionObserve: func() (time.Duration, error) { return 0, nil }})
	t.Fatal("child escaped its declared committed boundary", err)
}
func TestCPUPeriodWorkerActualProcessLossUsesLimitedRecovery(t *testing.T) {
	dir, cfg := sessionWorkerFixture(t)
	cfg.Scan.CPUChargeSecondsPerHour = 0
	// The child uses this exact generated root; no installed configuration/home roots.
	if err := os.Mkdir(filepath.Join(dir, "generated-root"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg.Roots = []string{filepath.Join(dir, "generated-root")}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCPUPeriodWorkerKillChild$")
	cmd.Env = append(os.Environ(), "RYDD_CPU_PERIOD_WORKER_CHILD="+dir)
	cmd.WaitDelay = time.Second
	var stderr cpuChildOutput
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = out.Close() }()
	ready := make(chan string, 1)
	go func() {
		scan := bufio.NewScanner(out)
		scan.Buffer(make([]byte, 256), 256)
		if scan.Scan() {
			ready <- scan.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case text := <-ready:
		if text != "cpu-period-worker-ready:1" {
			t.Fatal("child readiness", text, stderr.String())
		}
	case <-ctx.Done():
		t.Fatal("child readiness deadline", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatal("no signaled child exit", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("not actual SIGKILL", exit.ProcessState)
	}
	before := periodSaved(t, dir)
	cpu := sessionSaved(t, dir)
	if before.CPUGeneration != 1 || cpu.Status != "active" || cpu.RecoveredSessions != 0 {
		t.Fatal(before, cpu)
	}
	// Fresh Run uses actual16 even with zero new caps; bare methods cannot recover it.
	options := Options{ExperimentalScan: true, Interval: time.Millisecond, WorkDuration: time.Second, cpuObserve: func() (time.Duration, error) { return 0, nil }, cpuSessionObserve: func() (time.Duration, error) { return 0, nil }, cpuSessionHooks: &cpuSessionHooks{recover: func(context.Context, *state.Store, time.Time) (state.CPUChargeState, error) {
		t.Error("schema15 recovery used for16")
		return state.CPUChargeState{}, state.ErrCPUChargeAdmissionPolicyRequired
	}}}
	_, done := start(t, dir, cfg, options)
	waitUntil(t, func() bool { return sessionSaved(t, dir).Generation == 2 })
	after := periodSaved(t, dir)
	cpu = sessionSaved(t, dir)
	if cpu.RecoveredSessions != 1 || after.Limits != (state.CPUChargeLimits{}) || !after.Hour.BlockingUnknown || !after.Day.BlockingUnknown || after.CPUGeneration != 2 {
		t.Fatal("recovery lost conservative evidence", after, cpu)
	}
	if s := control(t, dir, "status"); s.InventoryMode != nil || s.ActiveJob != 0 {
		t.Fatal("recovered debt bypassed setup", s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	cpu = sessionSaved(t, dir)
	if cpu.RecoveredSessions != 1 || cpu.Status != "finished" || cpu.Generation != 2 {
		t.Fatal("recovery repeated or finish unrecorded", cpu)
	}
}
