package plans

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDismissalCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_DISMISSAL_CRASH_BASE")
	if base == "" {
		return
	}
	point := os.Getenv("RYDD_DISMISSAL_CRASH_POINT")
	stop := func() {
		fmt.Println("DISMISSAL_READY")
		select {}
	}
	hooks := dismissalHooks{}
	switch point {
	case "before":
		hooks.beforeCommit = stop
	case "after":
		hooks.afterCommit = stop
	default:
		t.Fatal("unknown crash fixture point")
	}
	_, err := saveDismissal(context.Background(), base, dismissalRequest(t), hooks)
	t.Fatal("crash helper passed stopping point", err)
}

func TestDismissalActualProcessLossAtomicMigrationAndLostReply(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			base := filepath.Join(t.TempDir(), "state")
			plan, err := Save(ctx, base, selection())
			if err != nil {
				t.Fatal(err)
			}
			admissionLegacyFixture(t, base, 3)
			plan, err = Load(ctx, base, plan.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeVersion := dismissalVersion(t, base)
			command := exec.Command(os.Args[0], "-test.run=^TestDismissalCrashHelper$", "-test.timeout=45s")
			command.Env = append(os.Environ(), "RYDD_DISMISSAL_CRASH_BASE="+base, "RYDD_DISMISSAL_CRASH_POINT="+point)
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = command.Process.Kill() })
			ready := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				var before strings.Builder
				for scanner.Scan() {
					line := scanner.Text()
					if line == "DISMISSAL_READY" {
						ready <- ""
						return
					}
					before.WriteString(line + "\n")
				}
				rest, _ := io.ReadAll(stdout)
				before.Write(rest)
				ready <- fmt.Sprintf("helper exited before marker: %s; scan error: %v", before.String(), scanner.Err())
			}()
			select {
			case failure := <-ready:
				if failure != "" {
					_ = command.Wait()
					t.Fatal(failure, stderr.String())
				}
			case <-time.After(30 * time.Second):
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal("helper marker timeout", stderr.String())
			}
			if err = command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal("child did not die from SIGKILL", err, stderr.String())
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("wrong child exit", err, stderr.String())
			}
			if again, e := Load(ctx, base, plan.ID); e != nil || !reflect.DeepEqual(again.Record, plan.Record) {
				t.Fatal("process loss changed original plan", again, e)
			}
			request := dismissalRequest(t)
			saved, err := FindDismissal(ctx, base, request.ID)
			if point == "before" {
				if !errors.Is(err, os.ErrNotExist) || dismissalVersion(t, base) != beforeVersion {
					t.Fatal("uncommitted migration survived", saved, err)
				}
			} else {
				if err != nil || dismissalVersion(t, base) != 5 || !ValidDismissalID(saved.ID) {
					t.Fatal("committed lost reply missing", saved, err)
				}
			}
			retry, err := SaveDismissal(ctx, base, request)
			if err != nil {
				t.Fatal(err)
			}
			if point == "after" && !reflect.DeepEqual(retry, saved) {
				t.Fatal("lost reply retry changed first record", retry, saved)
			}
		})
	}
}
