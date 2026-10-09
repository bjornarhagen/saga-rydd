package state

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type cpuFixtureOutput struct {
	mu        sync.Mutex
	data      []byte
	truncated bool
}

func (output *cpuFixtureOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	n := min(len(data), 8192-len(output.data))
	output.data = append(output.data, data[:n]...)
	output.truncated = output.truncated || n < len(data)
	return len(data), nil
}

func (output *cpuFixtureOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	text := string(output.data)
	if output.truncated {
		text += " [truncated]"
	}
	return text
}

func TestCPUFeedbackKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_CPU_FEEDBACK_KILL_DIR")
	if dir == "" {
		t.Skip("disposable child fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(ctx, []string{"/fixture/cpu-kill"}); err != nil {
		t.Fatal(err)
	}
	if err = s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Unix(0, 1)); err != nil {
		t.Fatal(err)
	}
	scope, err := s.ResolveFairInventoryRoots(ctx, []string{"/fixture/cpu-kill"})
	if err != nil {
		t.Fatal(err)
	}
	reservedAt := time.Now()
	if _, err = s.ReserveScanChunk(ctx, reservedAt, time.Second, 100); err != nil {
		t.Fatal(err)
	}
	turn, err := s.ClaimFairInventoryTurn(ctx, scope, time.Now(), time.Minute, true, reservedAt)
	if err != nil || turn == nil {
		t.Fatal(turn, err)
	}
	marker, err := s.BeginCPUWindow(ctx, turn, time.Now(), CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: reservedAt})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RYDD_CPU_FEEDBACK_KILL_PHASE") == "progress_committed" {
		if err = s.FinishJob(ctx, *turn.Job, true, nil, time.Now(), ""); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("pending:" + marker.Token())
	<-ctx.Done()
	t.Fatal("parent did not kill this finite child")
}

func TestCPUFeedbackActualSIGKILLPreservesPendingAndOneRecovery(t *testing.T) {
	for _, phase := range []string{"pending", "progress_committed"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := privateDir(t)
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCPUFeedbackKillChild$")
			command.Env = append(os.Environ(), "RYDD_CPU_FEEDBACK_KILL_DIR="+dir, "RYDD_CPU_FEEDBACK_KILL_PHASE="+phase)
			var stderr cpuFixtureOutput
			command.Stderr = &stderr
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
			line := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				if scanner.Scan() {
					line <- scanner.Text()
				} else {
					line <- ""
				}
			}()
			var marker string
			select {
			case marker = <-line:
			case <-ctx.Done():
				t.Fatal("child admission deadline", stderr.String())
			}
			if !strings.HasPrefix(marker, "pending:") || !metadataDigest(strings.TrimPrefix(marker, "pending:"), 64) {
				t.Fatal("child failed before durable pending marker", marker, stderr.String())
			}
			if err = command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = command.Wait(); err == nil {
				t.Fatal("child was not killed")
			}
			if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("not an actual SIGKILL", command.ProcessState)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			state, err := r.CPUFeedback(ctx)
			if err != nil || state.Status != "pending" || state.Window.Token != strings.TrimPrefix(marker, "pending:") || state.RecoveredUnknownWindows != 0 {
				t.Fatal("reader recovered or lost killed window", state, err)
			}
			r.Close()
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			recoveredAt := time.Now().UTC()
			state, err = w.RecoverCPUWindow(ctx, recoveredAt)
			if err != nil || state.Status != "recovered_unknown" || state.RecoveredUnknownWindows != 1 || state.Window.CPUTimeNS != nil || !state.NextAllowedAt.Equal(recoveredAt.Add(time.Hour)) {
				t.Fatal(state, err)
			}
			before := cpuFeedbackBytes(t, w)
			if _, err = w.RecoverCPUWindow(ctx, recoveredAt.Add(time.Minute)); err != nil || string(before) != string(cpuFeedbackBytes(t, w)) {
				t.Fatal("recovery extended the first cooldown", err)
			}
			jobs, err := w.RecoverJobs(ctx, time.Now())
			want := int64(1)
			if phase == "progress_committed" {
				want = 0
			}
			if err != nil || jobs != want {
				t.Fatal("CPU accounting replayed completed source progress", jobs, want, err)
			}
			budget, err := w.DispatchBudget(ctx, time.Now(), 100)
			if err != nil || budget.Used != 1 {
				t.Fatal("kill refunded dispatch", budget, err)
			}
		})
	}
}
