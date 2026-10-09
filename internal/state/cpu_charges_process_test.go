package state

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

// The child uses scripted SELF values to exercise durable accounting loss. It
// does not measure native worker CPU or establish any resource-rate acceptance.
func TestCPUChargesKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_CPU_CHARGES_CHILD_DIR")
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
	at := time.Now().UTC()
	if _, err = s.ActivateCPUCharges(ctx, at); err != nil {
		t.Fatal(err)
	}
	marker, _, err := s.BeginCPUSession(ctx, chargeStart(0, at, 12345, "a"))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RYDD_CPU_CHARGES_CHILD_PHASE") == "sample" {
		if _, err = s.SampleCPUSession(ctx, marker, chargeSample(1, time.Now().UTC(), 12355, int64(time.Millisecond))); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("cpu-charge-ready:1")
	<-ctx.Done()
	t.Fatal("parent did not kill this bounded child")
}

func TestCPUChargesActualSIGKILLAndOnceOnlyRecovery(t *testing.T) {
	for _, phase := range []string{"begin", "sample"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := privateDir(t)
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCPUChargesKillChild$")
			cmd.Env = append(os.Environ(), "RYDD_CPU_CHARGES_CHILD_DIR="+dir, "RYDD_CPU_CHARGES_CHILD_PHASE="+phase)
			cmd.WaitDelay = time.Second
			var stderr cpuFixtureOutput
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			ready := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				scanner.Buffer(make([]byte, 256), 256)
				if scanner.Scan() {
					ready <- scanner.Text()
				} else {
					ready <- ""
				}
			}()
			select {
			case line := <-ready:
				if strings.TrimSpace(line) != "cpu-charge-ready:1" {
					t.Fatal("child refused admission", line, stderr.String())
				}
			case <-ctx.Done():
				t.Fatal("durable readiness deadline", stderr.String())
			}
			if other, err := OpenWriter(ctx, dir); !errors.Is(err, localfs.ErrLocked) {
				if other != nil {
					other.Close()
				}
				t.Fatal("live child did not hold exclusive writer", err)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			var killed *exec.ExitError
			if !errors.As(err, &killed) {
				t.Fatal("child did not exit by signal", err)
			}
			status, ok := killed.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("not exact SIGKILL", killed.ProcessState)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			v, err := r.CPUCharges(ctx)
			r.Close()
			want := int64(12345)
			ordinal := int64(0)
			if phase == "sample" {
				want = 12355
				ordinal = 1
			}
			if err != nil || v.Status != "active" || v.ChargedCPUNS != want || v.Session.LastOrdinal != ordinal || v.RecoveredSessions != 0 || v.UnknownTailSessions != 0 {
				t.Fatal("saved reader fabricated recovery or lost charge", v, err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			at := time.Now().UTC()
			v, err = w.RecoverCPUSession(ctx, at)
			if err != nil || v.Status != "recovered_unknown" || v.ChargedCPUNS != want || v.RecoveredSessions != 1 || v.UnknownTailSessions != 1 || !v.NextAllowedAt.Equal(at.Add(time.Hour)) {
				t.Fatal(v, err)
			}
			before := chargeBytes(t, w)
			if _, err = w.RecoverCPUSession(ctx, at.Add(time.Minute)); err != nil || string(before) != string(chargeBytes(t, w)) {
				t.Fatal("recovery renewed/refunded", err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if string(before) != string(chargeBytes(t, w)) {
				t.Fatal("reopen changed historical charge")
			}
			_, v, err = w.BeginCPUSession(ctx, chargeStart(1, at.Add(time.Millisecond), 0, "b"))
			if err != nil || v.ChargedCPUNS != want || v.Generation != 2 || v.RecoveredSessions != 1 || !v.NextAllowedAt.Equal(at.Add(time.Hour)) {
				t.Fatal("new tracking discarded recovery debt", v, err)
			}
		})
	}
}

// The supplied executable must be built from the recorded pre-extension source.
// Ordinary checks need no old executable; an explicit native gate supplies it.
func TestCPUChargesOldCLIRefusesActivatedStore(t *testing.T) {
	binary := os.Getenv("RYDD_CPU_CHARGES_OLD_CLI")
	if binary == "" {
		t.Skip("explicit pre-extension native executable fixture")
	}
	info, err := os.Lstat(binary)
	if err != nil || !filepath.IsAbs(binary) || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		t.Fatal("old executable fixture must be a direct absolute executable", err)
	}
	s, dir := chargeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := filepath.Join(dir, "generated-root")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = []string{root}
	if err = config.Create(filepath.Join(dir, "config.toml"), "", cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.BeginCPUSession(ctx, chargeStart(0, chargeNow(), 100, "a")); err != nil {
		t.Fatal(err)
	}
	before := chargeBytes(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "--data-dir", dir, "state", "init")
	cmd.WaitDelay = time.Second
	var stdout, stderr cpuFixtureOutput
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	var failed *exec.ExitError
	if !errors.As(err, &failed) || failed.ExitCode() != 1 || !strings.Contains(stderr.String(), "unsupported state schema 15") || strings.Contains(stdout.String(), "State ready") {
		t.Fatal("old binary did not refuse optional schema", err, stdout.String(), stderr.String())
	}
	reader, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !bytes.Equal(before, chargeBytes(t, reader)) {
		t.Fatal("old binary mutated/recovered charge history")
	}
	summary, err := reader.Summary(ctx)
	if err != nil || summary.Schema != 15 || summary.EnabledRoots != 0 || summary.PendingJobs != 0 {
		t.Fatal("old binary mutated ordinary inventory", summary, err)
	}
}
