package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
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
	if os.Getenv("RYDD_TEST_WORKER_CLAIM") == "1" {
		options.Handlers = map[string]Handler{"fixture": func(ctx context.Context, j state.Job) (Result, error) {
			fmt.Println("claimed:" + string(j.Cursor))
			<-ctx.Done()
			return Result{}, ctx.Err()
		}}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := Run(ctx, dir, c, options); err != nil {
		t.Fatal(err)
	}
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
