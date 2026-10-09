package state

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestFairInventoryKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_TEST_FAIR_TURN_CHILD")
	if dir == "" {
		t.Skip("disposable child only")
	}
	ctx := context.Background()
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	paths := []string{"/fixture/a", "/fixture/b"}
	if err = s.SyncRoots(ctx, paths); err != nil {
		t.Fatal(err)
	}
	scope, err := s.ResolveFairInventoryRoots(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []int64{1, 2} {
		if err = s.EnqueueJob(ctx, root, ScanKind, []byte("."), time.Unix(0, 1)); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	if _, err = s.ReserveScanChunk(ctx, now, 10*time.Millisecond, 100); err != nil {
		t.Fatal(err)
	}
	turn, err := s.ClaimFairInventoryTurn(ctx, scope, time.Now(), time.Minute, true, now)
	if err != nil || turn == nil || turn.RootID != 1 || turn.Job == nil {
		t.Fatal(turn, err)
	}
	fmt.Fprintln(os.Stdout, "FAIR_TURN_COMMITTED")
	select {}
}

func TestFairInventoryActualKillRetainsRotationAndCharge(t *testing.T) {
	dir := privateDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFairInventoryKillChild$")
	command.Env = append(os.Environ(), "RYDD_TEST_FAIR_TURN_CHILD="+dir)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	lines := make(chan string, 1)
	go func() {
		reader := bufio.NewScanner(stdout)
		for reader.Scan() {
			if reader.Text() == "FAIR_TURN_COMMITTED" {
				lines <- reader.Text()
				return
			}
		}
		lines <- ""
	}()
	select {
	case line := <-lines:
		if line == "" {
			t.Fatal("child did not commit fair claim")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = command.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if recovered, err := s.RecoverJobs(context.Background(), time.Now()); err != nil || recovered != 1 {
		t.Fatal(recovered, err)
	}
	scope, err := s.ResolveFairInventoryRoots(context.Background(), []string{"/fixture/a", "/fixture/b"})
	if err != nil {
		t.Fatal(err)
	}
	peek, err := s.NextFairInventoryTurn(context.Background(), scope, time.Now(), true)
	if err != nil || peek.Turn == nil || peek.Turn.RootID != 2 {
		t.Fatal("SIGKILL reset rotation", peek, err)
	}
	budget, err := s.DispatchBudget(context.Background(), time.Now(), 100)
	if err != nil || budget.Used != 1 {
		t.Fatal("SIGKILL refunded dispatch", budget, err)
	}
	if wait := time.Until(budget.NextAllowed); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
	}
	now := time.Now()
	if _, err = s.ReserveScanChunk(context.Background(), now, 10*time.Millisecond, 100); err != nil {
		t.Fatal(err)
	}
	turn, err := s.ClaimFairInventoryTurn(context.Background(), scope, time.Now(), time.Minute, true, now)
	if err != nil || turn.RootID != 2 {
		t.Fatal(turn, err)
	}
	budget, err = s.DispatchBudget(context.Background(), time.Now(), 100)
	if err != nil || budget.Used != 2 {
		t.Fatal("fresh turn did not charge", budget, err)
	}
}
