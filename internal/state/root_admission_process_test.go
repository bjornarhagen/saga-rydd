package state

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestRootAdmissionKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_ROOT_ADMISSION_CHILD_DIR")
	if dir == "" {
		t.Skip("generated child fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	paths := []string{"/generated/new"}
	_, id, err := rootAdmissionRequest(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	ready := func() {
		fmt.Println("root-admission-ready:" + id)
		<-ctx.Done()
		t.Fatal("parent did not kill bounded generated child")
	}
	hooks := rootAdmissionHooks{}
	if os.Getenv("RYDD_ROOT_ADMISSION_CHILD_PHASE") == "before" {
		hooks.beforeCommit = ready
	} else {
		hooks.afterCommit = ready
	}
	if err = s.syncRootsAdmitted(ctx, paths, hooks); err != nil {
		t.Fatal(err)
	}
}

func TestRootAdmissionActualSIGKILLAtomicMembership(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			s, dir := rootAdmissionStore(t)
			if err := s.SyncRoots(context.Background(), []string{"/generated/old"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE roots SET volume_id='preserved',last_scan_ns=123,last_error='historical'"); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRootAdmissionKillChild$")
			command.Env = append(os.Environ(), "RYDD_ROOT_ADMISSION_CHILD_DIR="+dir, "RYDD_ROOT_ADMISSION_CHILD_PHASE="+phase)
			command.WaitDelay = time.Second
			var stderr cpuFixtureOutput
			command.Stderr = &stderr
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					_ = command.Process.Kill()
					_ = command.Wait()
				}
			}()
			ready := make(chan string, 1)
			go func() {
				reader := bufio.NewScanner(stdout)
				reader.Buffer(make([]byte, 256), 256)
				if reader.Scan() {
					ready <- reader.Text()
				} else {
					ready <- ""
				}
			}()
			_, expectedID, _ := rootAdmissionRequest(ctx, []string{"/generated/new"})
			select {
			case got := <-ready:
				if got != "root-admission-ready:"+expectedID {
					t.Fatal("child readiness mismatch", got, stderr.String())
				}
			case <-ctx.Done():
				t.Fatal("bounded child readiness failed", stderr.String())
			}
			if err = command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			waited = true
			var killed *exec.ExitError
			if !errors.As(err, &killed) {
				t.Fatal("child not killed", err)
			}
			status, ok := killed.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("not actual SIGKILL", killed.ProcessState)
			}
			r, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			wantCount := 1
			wantEnabled := "/generated/old"
			if phase == "after" {
				wantCount = 2
				wantEnabled = "/generated/new"
			}
			var enabled []byte
			if err = r.db.QueryRow("SELECT path FROM roots WHERE enabled=1").Scan(&enabled); err != nil || string(enabled) != wantEnabled || countRows(t, r, "SELECT count(*) FROM roots") != wantCount {
				t.Fatal("partial membership after process loss", string(enabled), err)
			}
			var volume, failure string
			var scanned int64
			if err = r.db.QueryRow("SELECT volume_id,last_scan_ns,last_error FROM roots WHERE id=1").Scan(&volume, &scanned, &failure); err != nil || volume != "preserved" || scanned != 123 || failure != "historical" {
				t.Fatal("lost historical root evidence", volume, scanned, failure, err)
			}
			if err = r.SyncRoots(ctx, []string{"/generated/new"}); err != nil || countRows(t, r, "SELECT count(*) FROM roots") != 2 {
				t.Fatal("exact intentional retry failed", err)
			}
		})
	}
}
