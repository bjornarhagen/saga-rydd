package inventory

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// This helper is inert in ordinary test runs. The parent supplies only the
// generated fixture, exact approval and one controlled crash point.
func TestHashStoreReadBudgetProcessHelper(t *testing.T) {
	base := os.Getenv("RYDD_DAILY_HASH_FIXTURE")
	if base == "" {
		t.Skip("parent-controlled generated process fixture")
	}
	ctx := context.Background()
	source, e := state.OpenReader(ctx, os.Getenv("RYDD_DAILY_HASH_INVENTORY"))
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	scanner, e := New([]string{os.Getenv("RYDD_DAILY_HASH_ROOT")}, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer scanner.Close()
	store, e := OpenExistingHashWriter(ctx, base)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	store.now = hashConsentProcessClock
	stop := func() {
		if _, e := fmt.Fprintln(os.Stdout, "READY"); e != nil {
			t.Fatal(e)
		}
		var one [1]byte
		if _, e := os.Stdin.Read(one[:]); e != nil {
			t.Fatal(e)
		}
		t.Fatal("crash seam unexpectedly resumed")
	}
	l := dailyHashLimits(64)
	h := hashStoreHooks{pacing: newHashReadPacer(l.RequestedBytesPerSecond, store.now, time.Now, nil), execution: &l, storeBudgetHooks: &hashStoreReadBudgetHooks{}}
	switch os.Getenv("RYDD_DAILY_HASH_STAGE") {
	case "before_migration":
		h.storeBudgetHooks.beforeCommit = stop
	case "after_migration":
		h.storeBudgetHooks.afterCommit = stop
	case "before_reservation":
		h.beforeReserveCommit = stop
	case "after_reservation":
		h.afterReserve = stop
	default:
		t.Fatal("unknown generated crash stage")
	}
	_, e = store.runConsented(ctx, os.Getenv("RYDD_DAILY_HASH_APPROVAL"), source, scanner, h)
	t.Fatal("crash seam was not reached", e)
}

type dailyCrashOutput struct{ buf bytes.Buffer }

func (b *dailyCrashOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 4096 - b.buf.Len()
	if remaining > 0 {
		_, _ = b.buf.Write(p[:min(remaining, n)])
	}
	return n, nil
}
func TestHashStoreReadBudgetProcessLossMigrationAndReservation(t *testing.T) {
	for _, stage := range []string{"before_migration", "after_migration", "before_reservation", "after_reservation"} {
		t.Run(stage, func(t *testing.T) {
			f := newHashConsentFixture(t)
			c := f.approve(t, 4096, 8192)
			if e := f.store.Close(); e != nil {
				t.Fatal(e)
			}
			runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(runCtx, os.Args[0], "-test.run=^TestHashStoreReadBudgetProcessHelper$")
			cmd.WaitDelay = time.Second
			cmd.Env = append(os.Environ(), "RYDD_DAILY_HASH_FIXTURE="+f.base, "RYDD_DAILY_HASH_INVENTORY="+f.stateDir, "RYDD_DAILY_HASH_ROOT="+f.root, "RYDD_DAILY_HASH_APPROVAL="+c.ID, "RYDD_DAILY_HASH_STAGE="+stage)
			stdout, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			input, e := cmd.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			defer input.Close()
			var stderr dailyCrashOutput
			cmd.Stderr = &stderr
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			waited := false
			defer func() {
				cancel()
				if !waited {
					_ = cmd.Wait()
				}
			}()
			ready := make(chan bool, 1)
			go func() {
				reader := bufio.NewScanner(io.LimitReader(stdout, 1024))
				reader.Buffer(make([]byte, 128), 1024)
				ready <- reader.Scan() && reader.Text() == "READY"
			}()
			select {
			case ok := <-ready:
				if !ok {
					cancel()
					_ = cmd.Wait()
					waited = true
					t.Fatal("generated helper did not reach selected seam", stderr.buf.String())
				}
			case <-runCtx.Done():
				cancel()
				_ = cmd.Wait()
				waited = true
				t.Fatal("generated helper deadline before seam", stderr.buf.String())
			}
			if e = cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			e = cmd.Wait()
			waited = true
			status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if e == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("controlled helper did not die from SIGKILL", e)
			}
			reader, e := OpenHashReader(context.Background(), f.base)
			if e != nil {
				t.Fatal(e)
			}
			b, e := reader.StoreReadBudget(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			expectedVersion := 7
			if stage == "before_migration" {
				expectedVersion = 2
				if b != nil {
					t.Fatal("uncommitted migration survived SIGKILL", b)
				}
			} else if b == nil {
				t.Fatal("committed tracking missing")
			}
			if hashChoiceSchemaVersion(t, reader.db) != expectedVersion {
				t.Fatal("partial additive migration")
			}
			before, e := reader.Snapshot(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			reader.Close()
			wantCharge := int64(0)
			if stage == "after_reservation" {
				wantCharge = 64
				if b.TotalReservedBytes != 64 || b.TotalOutstandingReservedBytes != 64 || b.TotalUnknownReservedBytes != 0 || before.Work[0].Status != "running" {
					t.Fatal("committed claim/charge not jointly visible", b, before.Work[0])
				}
			} else if b != nil && b.TotalReservedBytes != 0 {
				t.Fatal("pre-reservation crash invented charge", b)
			}
			// Park the generated source so recovery cannot reopen or hash it.
			if e = os.Rename(f.root, f.root+"-offline"); e != nil {
				t.Fatal(e)
			}
			defer os.Rename(f.root+"-offline", f.root)
			recovered, e := OpenExistingHashWriter(context.Background(), f.base)
			if e != nil {
				t.Fatal(e)
			}
			defer recovered.Close()
			recovered.now = func() time.Time { t.Fatal("saved recovery sampled clock"); return time.Time{} }
			if expectedVersion == 7 {
				b = requireStoreReadBudget(t, recovered)
				if b.TotalReservedBytes != wantCharge || b.TotalOutstandingReservedBytes != 0 || b.TotalUnknownReservedBytes != wantCharge || b.TotalReadBytes != 0 {
					t.Fatal("recovery refunded/double charged or invented known usage", b)
				}
			}
			saved, e := recovered.Snapshot(context.Background())
			if e != nil || saved.Work[0].DurableOffset != 0 || saved.Work[0].Status != "pending" {
				t.Fatal(saved, e)
			}
			if stage == "after_reservation" {
				if saved.Work[0].Sequence != 1 || saved.Work[0].LatestAttempt.Status != "interrupted_unknown" || saved.Work[0].LatestAttempt.ReadBytes != nil {
					t.Fatal("unknown crash became known usage", saved.Work[0])
				}
			} else if saved.Work[0].Sequence != 0 || saved.Work[0].LatestAttempt != nil {
				t.Fatal("preclaim crash advanced head", saved.Work[0])
			}
			if expectedVersion == 7 {
				if _, e = recovered.RunNext(context.Background(), nil, nil, 64, 128); !errors.Is(e, ErrHashStoreReadBudgetRequired) {
					t.Fatal("recovery reopened legacy dispatch bypass", e)
				}
			}
		})
	}
}
