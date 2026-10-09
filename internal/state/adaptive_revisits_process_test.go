package state

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAdaptiveRevisitProcessChild(t *testing.T) {
	dir := os.Getenv("RYDD_ADAPTIVE_REVISIT_CHILD_DIR")
	if dir == "" {
		t.Skip("generated subprocess fixture only")
	}
	phase := os.Getenv("RYDD_ADAPTIVE_REVISIT_CHILD_PHASE")
	if phase != "before" && phase != "after" {
		t.Fatal("invalid generated child phase")
	}
	ctx := context.Background()
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	roots, err := s.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := s.ConfigureAdaptiveRevisits(ctx, roots, true, strings.Repeat("d", 64), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	hooks := inventoryRevisitHooks{}
	if phase == "before" {
		hooks.beforeCommit = func() { fmt.Println("ready"); select {} }
	}
	result, err := s.finalizeAdaptiveRevisit(ctx, scope, 1, time.Now().UTC(), hooks)
	if err != nil || !result.Scheduled {
		t.Fatal(result, err)
	}
	fmt.Println("ready")
	select {}
}

func TestAdaptiveRevisitActualSIGKILLFinalizationAtomicity(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			s, dir := queueStore(t)
			ctx := context.Background()
			now := time.Now().UTC()
			roots, err := s.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := s.ConfigureAdaptiveRevisits(ctx, roots, true, strings.Repeat("d", 64), now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.FinalizeAdaptiveRevisit(ctx, scope, 1, now); err != nil {
				t.Fatal(err)
			}
			j := adaptiveClaimJob(t, s, now)
			adaptiveCommit(t, s, scope, j, []Entry{adaptiveEntry("file", "file", 5)}, 1, true, now)
			adaptiveDrain(t, s, 1)
			var oldEpoch, oldFinalized, oldJob, oldDue int64
			if err = s.db.QueryRow("SELECT epoch,finalized_epoch,root_job_id,scheduled_due_ns FROM adaptive_inventory_revisits WHERE root_id=1").Scan(&oldEpoch, &oldFinalized, &oldJob, &oldDue); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			childCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(childCtx, executable, "-test.run=^TestAdaptiveRevisitProcessChild$")
			cmd.Env = append(os.Environ(), "RYDD_ADAPTIVE_REVISIT_CHILD_DIR="+dir, "RYDD_ADAPTIVE_REVISIT_CHILD_PHASE="+phase)
			cmd.WaitDelay = time.Second
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			joined := false
			defer func() {
				if !joined {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || scanner.Text() != "ready" {
				t.Fatal("generated child did not reach exact publication seam")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			joined = true
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatal("generated child did not return terminal signal evidence", err)
			}
			waitStatus, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
				t.Fatal("generated child did not terminate with SIGKILL", err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			jobs := countRows(t, w, "SELECT count(*) FROM jobs WHERE root_id=1 AND kind='inventory'")
			want := 0
			if phase == "after" {
				want = 1
			}
			if jobs != want {
				t.Fatal("partial atomic publication survived", jobs, want)
			}
			var epoch, finalized, job, due, finished, interval int64
			if err = w.db.QueryRow("SELECT epoch,finalized_epoch,root_job_id,scheduled_due_ns,finished_ns,interval_ns FROM adaptive_inventory_revisits WHERE root_id=1").Scan(&epoch, &finalized, &job, &due, &finished, &interval); err != nil {
				t.Fatal(err)
			}
			if phase == "before" {
				if epoch != oldEpoch || finalized != oldFinalized || job != oldJob || due != oldDue {
					t.Fatal("uncommitted tracker advance survived", epoch, finalized, job, due)
				}
			} else {
				var queuedID, queuedDue int64
				if err = w.db.QueryRow("SELECT id,due_at_ns FROM jobs WHERE root_id=1 AND kind='inventory'").Scan(&queuedID, &queuedDue); err != nil {
					t.Fatal(err)
				}
				if epoch != oldEpoch+1 || finalized != oldEpoch || job != queuedID || due != queuedDue || finished <= 0 || interval != int64(InventoryRevisitInterval) || due != finished+interval {
					t.Fatal("job and tracker did not advance atomically", epoch, finalized, job, due, finished, interval)
				}
			}
			roots, err = w.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
			if err != nil {
				t.Fatal(err)
			}
			scope, err = w.ConfigureAdaptiveRevisits(ctx, roots, true, strings.Repeat("d", 64), time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			result, err := w.FinalizeAdaptiveRevisit(ctx, scope, 1, time.Now().UTC())
			if err != nil || result.Scheduled != (phase == "before") || result.UnchangedStreak != 0 || !result.Unknown {
				t.Fatal("restart learned twice or lost uncertainty", result, err)
			}
			if countRows(t, w, "SELECT count(*) FROM jobs WHERE root_id=1 AND kind='inventory'") != 1 {
				t.Fatal("retry duplicated next listing")
			}
		})
	}
}
