package config

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func TestExclusionCrashHelper(t *testing.T) {
	stage := os.Getenv("RYDD_EXCLUSION_CRASH_STAGE")
	if stage == "" {
		t.Skip("subprocess fixture only")
	}
	paths := Paths{ConfigFile: os.Getenv("RYDD_EXCLUSION_CRASH_CONFIG"), StateDir: os.Getenv("RYDD_EXCLUSION_CRASH_STATE")}
	stop := func() {
		fmt.Fprintln(os.Stdout, "publication-stage-ready")
		for {
			time.Sleep(time.Hour)
		}
	}
	hooks := exclusionHooks{}
	if stage == "before" {
		hooks.beforePublish = stop
	} else if stage == "after" {
		hooks.afterPublish = stop
	} else {
		t.Fatal("unknown generated crash stage")
	}
	_, err := editExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new", hooks)
	t.Fatal("crash seam was not reached", err)
}

func TestExclusionSIGKILLBeforeAndAfterPublication(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			paths, cfg := exclusionFixture(t, true)
			before := readExclusionFixture(t, paths.ConfigFile)
			lockPath := filepath.Join(paths.StateDir, "writer.lock")
			lockInfo, err := os.Lstat(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExclusionCrashHelper$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "RYDD_EXCLUSION_CRASH_STAGE="+stage, "RYDD_EXCLUSION_CRASH_CONFIG="+paths.ConfigFile, "RYDD_EXCLUSION_CRASH_STATE="+paths.StateDir)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill() }()
			reached := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "publication-stage-ready" {
						reached <- true
						return
					}
				}
				reached <- false
			}()
			select {
			case ready := <-reached:
				if !ready {
					t.Fatal("generated child never reached publication seam", stderr.String())
				}
			case <-ctx.Done():
				t.Fatal("generated child timed out", ctx.Err(), stderr.String())
			}
			if lock, err := localfs.AcquireExistingLock(context.Background(), paths.StateDir); !errors.Is(err, localfs.ErrLocked) || lock != nil {
				t.Fatal("child did not hold shared writer lock", lock, err)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			waitErr := cmd.Wait()
			status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("child was not actually killed by SIGKILL", waitErr, status, stderr.String())
			}
			list, err := ListExclusions(context.Background(), paths, "/generated-home")
			if stage == "after" {
				cfg.Excludes = append(cfg.Excludes, "/generated-root/new")
			}
			if err != nil || !reflect.DeepEqual(list.Exclusions, cfg.Excludes) {
				t.Fatal("process loss produced partial TOML or wrong complete list", list, cfg, err)
			}
			if stage == "before" && !bytes.Equal(before, readExclusionFixture(t, paths.ConfigFile)) {
				t.Fatal("prepublication process loss changed original bytes")
			}
			assertExclusionSettings(t, paths, cfg)
			retry, err := EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new")
			if err != nil || (stage == "after" && (retry.Changed || retry.Publication != "not_needed")) || (stage == "before" && (!retry.Changed || retry.Publication != "saved")) {
				t.Fatal("explicit retry did not resolve exact published setting", retry, err)
			}
			currentLock, err := os.Lstat(lockPath)
			if err != nil || !os.SameFile(lockInfo, currentLock) {
				t.Fatal("process recovery replaced stable lock", err)
			}
		})
	}
}
