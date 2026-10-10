//go:build darwin || linux

package storeexport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type processWitness struct {
	Phase       string `json:"phase"`
	Candidate   string `json:"candidate"`
	Destination string `json:"destination"`
}

func TestPlansExportOwnedProcessBoundaryChild(t *testing.T) {
	mode := os.Getenv("RYDD_STORE_EXPORT_CHILD")
	if mode == "" {
		t.Skip("child helper is executed only by the exact generated process fixture")
	}
	base, parent, destination := os.Getenv("RYDD_STORE_EXPORT_BASE"), os.Getenv("RYDD_STORE_EXPORT_PARENT"), os.Getenv("RYDD_STORE_EXPORT_DESTINATION")
	if cleanPath(base) != nil || cleanPath(parent) != nil || cleanPath(destination) != nil || filepath.Dir(destination) != parent {
		t.Fatal("invalid generated child scope")
	}
	witness := processWitness{Phase: mode, Destination: destination}
	switch mode {
	case "primitive-stage-before-publication":
		// This witnesses the reviewed exclusive-stage ownership primitive.
		// It does not claim an interruption inside ExportPlanStore or an
		// entered SQLite backup call; that distinct acceptance remains open.
		g := newGuard(context.Background())
		held, err := openDir(parent)
		if err != nil {
			t.Fatal(err)
		}
		defer held.close()
		stage, err := createStage(held, g)
		if err != nil {
			t.Fatal(err)
		}
		defer stage.close()
		if _, err = writeFile(stage, "plans.sqlite3", []byte("generated staging primitive"), g); err != nil {
			t.Fatal(err)
		}
		witness.Candidate = stage.path
	case "completed-export-after-publication":
		result, err := ExportPlanStore(context.Background(), base, destination)
		if err != nil || !result.Published || !result.Verified || !result.SyncCompleted || !result.EngineClosureKnown {
			t.Fatal(result, err)
		}
	default:
		t.Fatal("unknown child mode")
	}
	if err := json.NewEncoder(os.Stdout).Encode(witness); err != nil {
		t.Fatal(err)
	}
	// A timer keeps the child alive without a deadlock detector refusal. The
	// parent kills and waits for this exact direct PID before the timer ends.
	<-time.After(6 * time.Second)
	t.Fatal("parent did not kill the owned witness child")
}

type boundedProcessOutput struct {
	mu       sync.Mutex
	body     []byte
	limit    int
	cancel   context.CancelFunc
	ready    chan struct{}
	notified bool
}

func (w *boundedProcessOutput) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(body) > w.limit-len(w.body) {
		w.cancel()
		return 0, io.ErrShortWrite
	}
	w.body = append(w.body, body...)
	if w.ready != nil && !w.notified && bytes.IndexByte(w.body, '\n') >= 0 {
		w.notified = true
		w.ready <- struct{}{}
	}
	return len(body), nil
}
func (w *boundedProcessOutput) snapshot() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body...)
}

func TestOwnedSIGKILLRetainsStageOrCompletedPublication(t *testing.T) {
	for _, phase := range []string{"primitive-stage-before-publication", "completed-export-after-publication"} {
		t.Run(phase, func(t *testing.T) {
			f := fixtureStore(t, 6, 4096)
			parent := privateTemp(t)
			destination := filepath.Join(parent, "published")
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPlansExportOwnedProcessBoundaryChild$", "-test.timeout=7s")
			cmd.WaitDelay = time.Second
			// Replace only this fixture's four explicit variables. Existing HOME
			// is preserved; source and output paths are absolute generated paths.
			env := make([]string, 0, len(os.Environ())+4)
			for _, item := range os.Environ() {
				if !strings.HasPrefix(item, "RYDD_STORE_EXPORT_CHILD=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_BASE=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_PARENT=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_DESTINATION=") {
					env = append(env, item)
				}
			}
			cmd.Env = append(env, "RYDD_STORE_EXPORT_CHILD="+phase, "RYDD_STORE_EXPORT_BASE="+f.base, "RYDD_STORE_EXPORT_PARENT="+parent, "RYDD_STORE_EXPORT_DESTINATION="+destination)
			ready := make(chan struct{}, 1)
			stdout := &boundedProcessOutput{limit: 64 << 10, cancel: cancel, ready: ready}
			stderr := &boundedProcessOutput{limit: 64 << 10, cancel: cancel}
			cmd.Stdout = stdout
			cmd.Stderr = stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			reaped := false
			defer func() {
				if !reaped {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					reaped = true
				}
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatalf("witness not reached: %v stdout=%q stderr=%q", ctx.Err(), stdout.snapshot(), stderr.snapshot())
			}
			body := stdout.snapshot()
			if len(body) > 4096 {
				t.Fatal("witness metadata exceeded cap")
			}
			var witness processWitness
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			if err = decoder.Decode(&witness); err != nil {
				t.Fatal(err)
			}
			if err = decoder.Decode(new(any)); err != io.EOF {
				t.Fatal("extra witness output", err)
			}
			if witness.Phase != phase || witness.Destination != destination {
				t.Fatal("wrong generated witness", witness)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			waitErr := cmd.Wait()
			reaped = true
			var exit *exec.ExitError
			if !errors.As(waitErr, &exit) {
				t.Fatal("child was not signaled", waitErr)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("expected exact owned SIGKILL", waitErr)
			}
			if len(stderr.snapshot()) != 0 {
				t.Fatalf("unexpected child stderr %q", stderr.snapshot())
			}
			if phase == "primitive-stage-before-publication" {
				if witness.Candidate == "" || filepath.Dir(witness.Candidate) != parent {
					t.Fatal("invalid staged child", witness)
				}
				if _, err = os.Stat(filepath.Join(witness.Candidate, "plans.sqlite3")); err != nil {
					t.Fatal("owned stage lost after death", err)
				}
				if _, err = os.Lstat(destination); !os.IsNotExist(err) {
					t.Fatal("primitive stage published", err)
				}
			} else {
				if witness.Candidate != "" {
					t.Fatal("completed export claimed stage")
				}
				verified, err := VerifyPlanStoreExport(context.Background(), destination, privateTemp(t))
				if err != nil || !verified.Verified {
					t.Fatal("completed publication invalid after death", verified, err)
				}
			}
			t.Log(fmt.Sprintf("exact direct child reaped after SIGKILL; witness scope=%s; generated framework temporary subtrees can be cleaned after this test", phase))
		})
	}
}
