package plans

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func TestPlanAdmissionProcessHelper(t *testing.T) {
	base, mode := os.Getenv("RYDD_ADMISSION_BASE"), os.Getenv("RYDD_ADMISSION_MODE")
	if base == "" || mode == "" {
		return
	}
	ordinal, err := strconv.Atoi(os.Getenv("RYDD_ADMISSION_ORDINAL"))
	if err != nil {
		t.Fatal(err)
	}
	boundary := func() {
		fmt.Println("boundary")
		var b [1]byte
		if _, err := io.ReadFull(os.Stdin, b[:]); err != nil {
			t.Fatal(err)
		}
	}
	hooks := admissionHooks{}
	switch mode {
	case "before", "release":
		hooks.beforeCommit = boundary
	case "after":
		hooks.afterCommit = boundary
	case "try":
	default:
		t.Fatal("unknown helper mode")
	}
	result, err := saveRecord(context.Background(), base, admissionRecord(ordinal), hooks)
	switch {
	case errors.Is(err, localfs.ErrLocked):
		fmt.Println("locked")
	case errors.Is(err, ErrPlanCapacity):
		fmt.Println("capacity")
	case err == nil && result.ID != "":
		fmt.Println("saved")
	default:
		t.Fatal("unexpected helper result", result.ID, err)
	}
}

type admissionChild struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan string
	done   chan error
	cancel context.CancelFunc
}

func startAdmissionChild(t *testing.T, base, mode string, ordinal int) *admissionChild {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPlanAdmissionProcessHelper$", "-test.count=1")
	cmd.WaitDelay = time.Second
	cmd.Env = append(os.Environ(), "RYDD_ADMISSION_BASE="+base, "RYDD_ADMISSION_MODE="+mode, "RYDD_ADMISSION_ORDINAL="+strconv.Itoa(ordinal))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
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
	c := &admissionChild{cmd: cmd, stdin: stdin, lines: make(chan string, 16), done: make(chan error, 1), cancel: cancel}
	// Drain the finite helper output before Wait; no pipe readers race with reap.
	go func() {
		scanner := bufio.NewScanner(io.LimitReader(stdout, 4096))
		scanner.Buffer(make([]byte, 128), 1024)
		for scanner.Scan() {
			c.lines <- scanner.Text()
		}
		close(c.lines)
		waitErr := cmd.Wait()
		cancel()
		if scanner.Err() != nil && waitErr == nil {
			waitErr = scanner.Err()
		}
		c.done <- waitErr
		close(c.done)
	}()
	t.Cleanup(func() {
		cancel() // Default os.Process.Kill guards direct-child ownership after reap.
		_ = stdin.Close()
		select {
		case <-c.done:
		case <-time.After(3 * time.Second):
			t.Error("owned helper was not reaped")
		}
	})
	return c
}

func admissionChildLine(t *testing.T, c *admissionChild, want string) {
	t.Helper()
	select {
	case got, ok := <-c.lines:
		if !ok || got != want {
			t.Fatalf("helper response %q, want %q", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper response deadline")
	}
}

func admissionChildWait(t *testing.T, c *admissionChild, killed bool) {
	t.Helper()
	select {
	case err := <-c.done:
		if !killed && err != nil {
			t.Fatal("helper exit", err)
		}
		if killed {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal("helper did not exit from a signal", err)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("helper did not exit from exact SIGKILL", err)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("helper reap deadline")
	}
}

func TestPlanAdmissionSIGKILLPublication(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "state")
			admissionPopulate(t, base, PlanLimit-1)
			child := startAdmissionChild(t, base, mode, 999)
			admissionChildLine(t, child, "boundary")
			if err := child.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			admissionChildWait(t, child, true)
			count, version := admissionCountVersion(t, base)
			if mode == "before" {
				if count != PlanLimit-1 || version != 1 {
					t.Fatal("process loss leaked admission or migration", count, version)
				}
			} else if count != PlanLimit || version != 6 {
				t.Fatal("post-commit process loss erased admission", count, version)
			}
			retry, err := SaveRecord(context.Background(), base, admissionRecord(999))
			if err != nil || retry.ID == "" {
				t.Fatal("frozen retry could not recover lost reply", err)
			}
			if count, version = admissionCountVersion(t, base); count != PlanLimit || version != 6 {
				t.Fatal("retry created another root", count, version)
			}
		})
	}
}

func TestPlanAdmissionConcurrentLastSlot(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	admissionPopulate(t, base, PlanLimit-1)
	first := startAdmissionChild(t, base, "release", 999)
	admissionChildLine(t, first, "boundary")
	second := startAdmissionChild(t, base, "try", 1000)
	admissionChildLine(t, second, "locked")
	admissionChildWait(t, second, false)
	if _, err := first.stdin.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	admissionChildLine(t, first, "saved")
	admissionChildWait(t, first, false)
	secondRetry := startAdmissionChild(t, base, "try", 1000)
	admissionChildLine(t, secondRetry, "capacity")
	admissionChildWait(t, secondRetry, false)
	if count, version := admissionCountVersion(t, base); count != PlanLimit || version != 6 {
		t.Fatal("concurrent writers oversubscribed final slot", count, version)
	}
}
