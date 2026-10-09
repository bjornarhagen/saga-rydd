package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
			// Cleanup after reap only cancels the completed context. The saved
			// result must remain available without calling Wait again.
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

func TestBackgroundResourceCompletedCancellationRetainsDirectChildOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	completed := helper(t, "success", ctx, 128)
	first := completed.wait(ctx)
	if first.err != nil || !completed.cmd.ProcessState.Success() {
		t.Fatal("completed direct child was not reaped", first.err)
	}
	reaped := completed.cmd.ProcessState
	ready := filepath.Join(t.TempDir(), "ready")
	running := helper(t, "wait", ctx, 128, "RYDD_BACKGROUND_READY_PATH="+ready)
	waitFixtureMarker(t, running, func() bool {
		body, err := os.ReadFile(ready)
		return err == nil && string(body) == "ready"
	})
	// Process.Kill checks the completed Process object's state. Do not send a
	// numeric PID or group signal after reap, even to test that it is absent.
	if err := completed.cmd.Cancel(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatal("completed child cancellation bypassed Process state", err)
	}
	completed.terminate()
	if second := completed.wait(ctx); second.err != first.err || second.usage != first.usage || !bytes.Equal(second.stdout, first.stdout) || !bytes.Equal(second.stderr, first.stderr) || completed.cmd.ProcessState != reaped {
		t.Fatal("completed ownership/result changed after cancellation")
	}
	select {
	case <-running.done:
		t.Fatal("cancelling the completed child ended another owned child")
	default:
	}
	running.terminate()
	if got := running.wait(ctx); got.err == nil || running.cmd.ProcessState == nil {
		t.Fatal("running direct child was not canceled and reaped", got.err)
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
