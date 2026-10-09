package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestBackgroundResourceInputsBeforeFixture(t *testing.T) {
	base := options{binary: "explicit", profile: "defaults_no_power", seconds: 30, roots: 1, files: 1}
	if !base.valid() {
		t.Fatal("valid minimum refused")
	}
	for _, change := range []func(*options){func(o *options) { o.binary = "" }, func(o *options) { o.profile = "unknown" }, func(o *options) { o.seconds = 29 }, func(o *options) { o.seconds = 3601 }, func(o *options) { o.roots = 33 }, func(o *options) { o.roots = 0 }, func(o *options) { o.files = 4097 }, func(o *options) { o.files = 0 }, func(o *options) { o.roots = 32; o.files = 4096 }} {
		o := base
		change(&o)
		if o.valid() {
			t.Fatal("unbounded input admitted", o)
		}
		if _, err := run(context.Background(), o); !errors.Is(err, errInput) {
			t.Fatal("invalid input opened fixture", err)
		}
	}
}

func TestBackgroundResourceBodiesMutationAndSymlink(t *testing.T) {
	ctx := context.Background()
	roots, err := createFixture(ctx, t.TempDir(), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	before, err := fixtureDigest(ctx, roots, 3)
	if err != nil || len(before) != 64 {
		t.Fatal(before, err)
	}
	if again, err := fixtureDigest(ctx, roots, 3); err != nil || again != before {
		t.Fatal("unchanged bodies differed", again, err)
	}
	path := filepath.Join(roots[1], "file-000002")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	original[511] = 'z'
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixtureDigest(ctx, roots, 3); err == nil {
		t.Fatal("one changed byte was not detected")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(roots[0], "file-000000"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := fixtureDigest(ctx, roots, 3); err == nil {
		t.Fatal("symlink followed as a fixture original")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := fixtureDigest(canceled, roots, 3); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled read proceeded", err)
	}
}

func TestBackgroundResourceChildHelper(t *testing.T) {
	mode := os.Getenv("RYDD_BACKGROUND_CHILD_MODE")
	if mode == "" {
		t.Skip("disposable process only")
	}
	switch mode {
	case "success":
		fmt.Print("{\"ok\":true}")
	case "fail":
		fmt.Fprint(os.Stderr, "/private/generated/diagnostic")
		os.Exit(7)
	case "flood":
		for i := 0; i < 10000; i++ {
			fmt.Print(strings.Repeat("x", 4096))
		}
	case "wait":
		if err := os.WriteFile(os.Getenv("RYDD_BACKGROUND_READY_PATH"), []byte("ready"), 0600); err != nil {
			os.Exit(9)
		}
		time.Sleep(5 * time.Second)
	case "signal-controller":
		ctx, cancel := measurementContext(5 * time.Second)
		c, err := startChild(ctx, os.Args[0], []string{"-test.run=^TestBackgroundResourceChildHelper$"}, append(os.Environ(), "RYDD_BACKGROUND_CHILD_MODE=wait"), os.TempDir(), 128)
		if err != nil {
			os.Exit(10)
		}
		<-ctx.Done()
		got := c.wait(ctx)
		status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
		cancel()
		if got.err == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			os.Exit(11)
		}
		fmt.Print("{\"owned_child_reaped\":true}")
	case "group-leader":
		c := exec.Command(os.Args[0], "-test.run=^TestBackgroundResourceChildHelper$")
		c.Env = append(os.Environ(), "RYDD_BACKGROUND_CHILD_MODE=heartbeat")
		c.Stdout, c.Stderr = os.Stdout, os.Stderr
		if err := c.Run(); err != nil {
			os.Exit(12)
		}
	case "heartbeat":
		group, err := syscall.Getpgid(0)
		if err != nil {
			os.Exit(13)
		}
		marker, _ := json.Marshal(map[string]int{"pid": os.Getpid(), "parent_pid": os.Getppid(), "group": group})
		if err := os.WriteFile(os.Getenv("RYDD_BACKGROUND_READY_PATH"), marker, 0600); err != nil {
			os.Exit(14)
		}
		// This descendant is finite even if an assertion fails. Its generated
		// heartbeat gives evidence of actual work before group cancellation.
		for n := 0; n < 100; n++ {
			f, err := os.OpenFile(os.Getenv("RYDD_BACKGROUND_HEARTBEAT_PATH"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				os.Exit(15)
			}
			_, writeErr := f.Write([]byte("x"))
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				os.Exit(16)
			}
			time.Sleep(20 * time.Millisecond)
		}
	default:
		os.Exit(8)
	}
	os.Exit(0)
}

func helper(t *testing.T, mode string, ctx context.Context, limit int, extraEnv ...string) *child {
	t.Helper()
	env := append(os.Environ(), "RYDD_BACKGROUND_CHILD_MODE="+mode)
	env = append(env, extraEnv...)
	c, err := startChild(ctx, os.Args[0], []string{"-test.run=^TestBackgroundResourceChildHelper$"}, env, t.TempDir(), limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.terminate)
	return c
}

func TestBackgroundResourceRealChildRusageAndOutputFailures(t *testing.T) {
	for _, mode := range []string{"success", "fail", "flood"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c := helper(t, mode, ctx, 128)
			got := c.wait(ctx)
			if got.usage.PeakRSSBytes <= 0 || got.usage.LaunchToWaitReturnNS <= 0 || got.usage.UserCPUNS < 0 || got.usage.SystemCPUNS < 0 {
				t.Fatal("native child usage missing", got.usage)
			}
			if len(got.stdout)+len(got.stderr) > 128 {
				t.Fatal("retained output exceeded cap")
			}
			switch mode {
			case "success":
				if got.err != nil || !bytes.Contains(got.stdout, []byte("true")) || !c.cmd.ProcessState.Success() {
					t.Fatal(got)
				}
			case "fail":
				if !errors.Is(got.err, errChild) || strings.Contains(got.err.Error(), "/private/") || !bytes.Contains(got.stderr, []byte("/private/")) {
					t.Fatal("private diagnostics exposed/lost", got)
				}
			case "flood":
				if !errors.Is(got.err, errOutput) {
					t.Fatal("overflow did not fail", got.err)
				}
			}
			// Cleanup after reap only cancels the completed context; it must not
			// issue another numeric process-group signal or call Wait again.
			c.terminate()
			if second := c.wait(ctx); second.err != got.err {
				t.Fatal("reaped result changed")
			}
		})
	}
}

func TestBackgroundResourceCanceledChildIsReaped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := filepath.Join(t.TempDir(), "ready")
	c := helper(t, "wait", ctx, 128, "RYDD_BACKGROUND_READY_PATH="+ready)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if marker, err := os.ReadFile(ready); err == nil && string(marker) == "ready" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("owned disposable child did not reach its running marker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	got := c.wait(ctx)
	status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if got.err == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("canceled owned child was not killed/reaped", got.err, status)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("wait returned before reaping")
	}
}

func TestBackgroundResourcePrivateLogFailureIsReported(t *testing.T) {
	r := runner{base: filepath.Join(t.TempDir(), "missing")}
	if err := r.writePrivate("worker", childResult{stderr: []byte("private")}); !errors.Is(err, errChild) {
		t.Fatal("private-log write failure ignored", err)
	}
}

func TestBackgroundResourceSignalContextReapsItsOwnedChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := filepath.Join(t.TempDir(), "ready")
	c := helper(t, "signal-controller", ctx, 256, "RYDD_BACKGROUND_READY_PATH="+ready)
	waitFixtureMarker(t, c, func() bool {
		body, err := os.ReadFile(ready)
		return err == nil && string(body) == "ready"
	})
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	got := c.wait(ctx)
	if got.err != nil || !c.cmd.ProcessState.Success() || !bytes.Contains(got.stdout, []byte("\"owned_child_reaped\":true")) {
		t.Fatal("signal-aware parent did not cancel and reap its running child", got.err, string(got.stdout))
	}
}

func TestBackgroundResourceCancellationStopsLiveOwnedGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	ready, heartbeat := filepath.Join(dir, "ready"), filepath.Join(dir, "heartbeat")
	c := helper(t, "group-leader", ctx, 256, "RYDD_BACKGROUND_READY_PATH="+ready, "RYDD_BACKGROUND_HEARTBEAT_PATH="+heartbeat)
	waitFixtureMarker(t, c, func() bool {
		body, err := os.ReadFile(ready)
		var marker struct {
			PID    int `json:"pid"`
			Parent int `json:"parent_pid"`
			Group  int `json:"group"`
		}
		if err != nil || json.Unmarshal(body, &marker) != nil || marker.PID == c.cmd.Process.Pid || marker.Parent != c.cmd.Process.Pid || marker.Group != c.cmd.Process.Pid {
			return false
		}
		work, err := os.ReadFile(heartbeat)
		return err == nil && len(work) > 0
	})
	cancel()
	if got := c.wait(ctx); got.err == nil || c.cmd.ProcessState.Success() {
		t.Fatal("canceled live group leader was not reaped", got.err)
	}
	before, err := os.ReadFile(heartbeat)
	if err != nil || len(before) == 0 {
		t.Fatal("generated descendant work missing", err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeat)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("same-group generated descendant continued after cancellation", err)
	}
}

func waitFixtureMarker(t *testing.T, c *child, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		select {
		case <-c.done:
			t.Fatal("disposable child exited before its running marker")
		default:
		}
		if !time.Now().Before(deadline) {
			t.Fatal("disposable child did not reach its running marker")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func stablePausedView() statusView {
	s := statusView{}
	s.State.Entries = 2
	s.Worker.Live = &worker.Snapshot{Paused: true, Dispatch: &state.DispatchBudget{Day: "2026-10-09", Used: 1}, InventoryMetrics: &inventory.Metrics{StatCalls: 5, EntryInspections: 2}}
	s.Metadata.TotalCharges = &state.MetadataCharges{Reserved: 65536}
	s.Dispatch = state.DispatchBudget{Day: "2026-10-09", Used: 1}
	return s
}

func TestBackgroundResourcePausedChecksChargesAndAPIs(t *testing.T) {
	a, b := stablePausedView(), stablePausedView()
	if !pausedStable(a, b) {
		t.Fatal("unchanged paused state refused")
	}
	b.Dispatch.Day = "2026-10-10"
	b.Dispatch.Used = 0
	if !pausedStable(a, b) {
		t.Fatal("saved-only UTC view reset confused with a new dispatch")
	}
	for _, change := range []func(*statusView){func(s *statusView) { s.State.Entries++ }, func(s *statusView) { s.Worker.Live.ActiveJob = 1 }, func(s *statusView) { s.Worker.Live.InventoryMetrics.StatCalls++ }, func(s *statusView) { s.Worker.Live.Dispatch.Used++ }, func(s *statusView) { s.Metadata.TotalCharges.Reserved++ }, func(s *statusView) { s.Worker.Live.Paused = false }} {
		changed := stablePausedView()
		change(&changed)
		if pausedStable(a, changed) {
			t.Fatal("paused mutation admitted", changed)
		}
	}
	for _, change := range []func(*statusView){func(s *statusView) { s.CPUFeedback.Status = "pending" }, func(s *statusView) { s.Metadata.TotalCharges.OutstandingReserved = 65536 }, func(s *statusView) { s.Worker.Live.InventoryMetrics = nil }} {
		changed := stablePausedView()
		change(&changed)
		if pausedStable(a, changed) {
			t.Fatal("unsettled or unobserved paused evidence admitted")
		}
	}
	for _, change := range []func(*statusView){func(s *statusView) { s.State.PendingJobs++ }, func(s *statusView) { s.State.RunningJobs++ }, func(s *statusView) { s.State.CompleteDirectories++ }, func(s *statusView) { s.State.DirectoryErrors++ }, func(s *statusView) { s.State.SkippedEntries++ }, func(s *statusView) { s.State.EnabledRoots++ }, func(s *statusView) { s.Worker.Live.RecoveredJobs++ }} {
		changed := stablePausedView()
		change(&changed)
		if pausedStable(a, changed) {
			t.Fatal("paused queue/coverage/error mutation admitted")
		}
	}
	if safeReason("/private/path") != "unrecognized" || safeReason("cpu_backoff") != "cpu_backoff" {
		t.Fatal("unsafe reason projection")
	}
}

func TestBackgroundResourceJSONOmitsFixtureAndPrivateEvidence(t *testing.T) {
	private := "/private/generated/fixture-and-token"
	r := runner{base: private, stateDir: private + "/state", roots: []string{private + "/source"}, worker: &child{started: time.Now().Add(-time.Second)}}
	s := stablePausedView()
	s.Worker.Live.Instance, s.Worker.Live.WaitReason = private, private
	s.CPUFeedback.Window = &state.CPUWindowRecord{Token: private, Instance: private, JobToken: private}
	if err := r.sample(s); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(r.result)
	if err != nil || bytes.Contains(encoded, []byte(private)) || bytes.Contains(encoded, []byte("job_token")) || bytes.Contains(encoded, []byte("instance")) {
		t.Fatal("public measurement exposed private evidence", err)
	}
	var view map[string]any
	if err := json.Unmarshal(encoded, &view); err != nil || view["physical_read_bytes"] != nil || view["system_wakeups"] != nil || view["hourly_target_accepted"] != false || view["physical_power_accepted"] != false {
		t.Fatal("unmeasured physical/hourly claims fabricated", err, view)
	}
}
