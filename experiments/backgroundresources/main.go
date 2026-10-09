// backgroundresources measures one finite, generated native worker fixture.
// It accepts an executable, never an existing scan root or service manager.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

const (
	cliOutputLimit    = 256 << 10
	workerOutputLimit = 64 << 10
	sampleInterval    = 5 * time.Second
	maxSamples        = 750
)

var (
	errInput    = errors.New("invalid bounded background fixture parameters")
	errChild    = errors.New("fixture command failed; inspect retained private logs")
	errOutput   = errors.New("fixture command output exceeded its retained limit")
	errProtocol = errors.New("fixture command returned unsupported JSON")
)

type options struct {
	binary, profile       string
	seconds, roots, files int
}

func (o options) valid() bool {
	return o.binary != "" && o.seconds >= 30 && o.seconds <= 3600 && o.roots >= 1 && o.roots <= 32 && o.files >= 1 && o.files <= 4096 && o.roots*o.files <= 32768 && (o.profile == "defaults_no_power" || o.profile == "short_fixture")
}

type usage struct {
	UserCPUNS            int64 `json:"user_cpu_ns"`
	SystemCPUNS          int64 `json:"system_cpu_ns"`
	PeakRSSBytes         int64 `json:"peak_rss_bytes"`
	LaunchToWaitReturnNS int64 `json:"launch_to_wait_return_elapsed_ns"`
}

type latency struct {
	Count   int   `json:"count"`
	TotalNS int64 `json:"total_ns"`
	MaxNS   int64 `json:"max_ns"`
}

type cpuEvidence struct {
	Status           string `json:"status"`
	CPUTimeNS        *int64 `json:"cpu_time_ns"`
	ElapsedNS        *int64 `json:"elapsed_ns"`
	BackoffNS        int64  `json:"backoff_ns"`
	CompletedUnknown int64  `json:"completed_unknown_windows"`
	RecoveredUnknown int64  `json:"recovered_unknown_windows"`
}

type sample struct {
	ElapsedNS           int64                `json:"elapsed_ns"`
	Entries             int64                `json:"entries"`
	PendingJobs         int64                `json:"pending_jobs"`
	RunningJobs         int64                `json:"running_jobs"`
	CompleteDirectories int64                `json:"complete_directories"`
	Paused              bool                 `json:"paused"`
	ActiveJob           int64                `json:"active_job"`
	WaitReason          string               `json:"wait_reason"`
	Metrics             *inventory.Metrics   `json:"scanner_api_observation"`
	Dispatch            state.DispatchBudget `json:"dispatch_budget"`
	Metadata            state.MetadataBudget `json:"scanner_metadata_budget"`
	CPU                 cpuEvidence          `json:"saved_cpu_feedback"`
}

type result struct {
	Contract                      string              `json:"contract"`
	Platform                      string              `json:"platform"`
	Profile                       string              `json:"profile"`
	ConfiguredScan                map[string]any      `json:"configured_scan"`
	ProductionDefaultsExceptPower bool                `json:"production_defaults_except_power"`
	PowerProbesDisabled           bool                `json:"power_probes_disabled"`
	RequestedSeconds              int                 `json:"requested_seconds"`
	Roots                         int                 `json:"generated_roots"`
	Files                         int                 `json:"generated_files"`
	SampleIntervalNS              int64               `json:"sampling_interval_ns"`
	WorkerUsage                   usage               `json:"worker_full_lifetime_usage"`
	CLIProcesses                  int                 `json:"observer_cli_processes"`
	CLIUserCPUNS                  int64               `json:"observer_cli_user_cpu_ns"`
	CLISystemCPUNS                int64               `json:"observer_cli_system_cpu_ns"`
	Latencies                     map[string]*latency `json:"command_latencies"`
	PeakDatabaseBytes             int64               `json:"sampled_peak_database_bytes"`
	PeakWALBytes                  int64               `json:"sampled_peak_wal_bytes"`
	PhysicalReadBytes             *int64              `json:"physical_read_bytes"`
	SystemWakeups                 *int64              `json:"system_wakeups"`
	PhysicalReadMeasurement       string              `json:"physical_read_measurement"`
	SystemWakeupMeasurement       string              `json:"system_wakeup_measurement"`
	ObserverOverhead              string              `json:"observer_overhead"`
	WorkerUsageScope              string              `json:"worker_usage_scope"`
	CleanupScope                  string              `json:"cleanup_scope"`
	Samples                       []sample            `json:"samples"`
	Final                         state.Summary       `json:"final_saved_summary"`
	BodiesSHA256                  string              `json:"generated_bodies_sha256"`
	BodiesUnchanged               bool                `json:"generated_bodies_unchanged"`
	PauseVerified                 bool                `json:"paused_progress_unchanged"`
	ResumeVerified                bool                `json:"resume_acknowledged"`
	GracefulExit                  bool                `json:"worker_graceful_exit_reaped"`
	HourlyTargetAccepted          bool                `json:"hourly_target_accepted"`
	PhysicalPowerAccepted         bool                `json:"physical_power_accepted"`
	SampledPeaksAreLowerBounds    bool                `json:"sampled_peaks_are_lower_bounds"`
}

type statusView struct {
	APIVersion int           `json:"api_version"`
	OK         bool          `json:"ok"`
	Command    string        `json:"command"`
	State      state.Summary `json:"state"`
	Worker     struct {
		State string           `json:"state"`
		Live  *worker.Snapshot `json:"live"`
	} `json:"worker"`
	Dispatch    state.DispatchBudget   `json:"dispatch_budget"`
	Metadata    state.MetadataBudget   `json:"scanner_metadata_budget"`
	CPUFeedback state.CPUFeedbackState `json:"cpu_feedback"`
}

type cappedOutput struct {
	mu             sync.Mutex
	stdout, stderr bytes.Buffer
	limit          int
	overflow       bool
	cancel         context.CancelFunc
}

type outputWriter struct {
	output *cappedOutput
	stderr bool
}

func (w outputWriter) Write(p []byte) (int, error) {
	b := w.output
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	remaining := b.limit - b.stdout.Len() - b.stderr.Len()
	if len(p) > remaining {
		p = p[:remaining]
		b.overflow = true
		b.cancel()
	}
	if w.stderr {
		_, _ = b.stderr.Write(p)
	} else {
		_, _ = b.stdout.Write(p)
	}
	return n, nil
}

func (b *cappedOutput) snapshot() ([]byte, []byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.stdout.Bytes()...), append([]byte(nil), b.stderr.Bytes()...), b.overflow
}

type childResult struct {
	stdout, stderr []byte
	usage          usage
	err            error
}
type child struct {
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	done    chan struct{}
	started time.Time
	result  childResult // immutable after done is closed
}

func startChild(parent context.Context, binary string, args, env []string, directory string, limit int) (*child, error) {
	if parent == nil || parent.Err() != nil {
		return nil, errChild
	}
	ctx, cancel := context.WithCancel(parent)
	c := &child{cancel: cancel, done: make(chan struct{})}
	c.cmd = exec.CommandContext(ctx, binary, args...)
	c.cmd.Env, c.cmd.Dir = env, directory
	// Keep CommandContext's Process.Kill cancellation. Cmd.Wait can reap its
	// child before joining the cancellation watcher, so a raw numeric group
	// signal here would bypass Process's protections against PID reuse.
	c.cmd.WaitDelay = time.Second
	out := &cappedOutput{limit: limit, cancel: cancel}
	c.cmd.Stdout, c.cmd.Stderr = outputWriter{out, false}, outputWriter{out, true}
	c.started = time.Now()
	if err := c.cmd.Start(); err != nil {
		cancel()
		return nil, errChild
	}
	go func() {
		err := c.cmd.Wait()              // exactly one reap
		elapsed := time.Since(c.started) // includes launch and output-pipe drain
		cancel()                         // exec's cancellation watcher has joined
		stdout, stderr, overflow := out.snapshot()
		u, usageErr := childUsage(c.cmd.ProcessState, elapsed)
		switch {
		case overflow:
			err = errOutput
		case err != nil:
			err = errChild
		case usageErr != nil:
			err = usageErr
		}
		c.result = childResult{stdout, stderr, u, err}
		close(c.done)
	}()
	return c, nil
}

func childUsage(ps *os.ProcessState, elapsed time.Duration) (usage, error) {
	if ps == nil || elapsed <= 0 {
		return usage{}, errChild
	}
	r, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || r.Maxrss <= 0 {
		return usage{}, errChild
	}
	u := usage{UserCPUNS: ps.UserTime().Nanoseconds(), SystemCPUNS: ps.SystemTime().Nanoseconds(), PeakRSSBytes: r.Maxrss, LaunchToWaitReturnNS: elapsed.Nanoseconds()}
	if runtime.GOOS == "linux" {
		u.PeakRSSBytes *= 1024
	}
	if u.UserCPUNS < 0 || u.SystemCPUNS < 0 || u.PeakRSSBytes <= 0 {
		return usage{}, errChild
	}
	return u, nil
}

func (c *child) wait(ctx context.Context) childResult {
	select {
	case <-c.done:
	case <-ctx.Done():
		c.cancel()
		<-c.done
	}
	return c.result
}

func (c *child) terminate() { c.cancel(); <-c.done }

type runner struct {
	base, stateDir, binary string
	roots                  []string
	env                    []string
	worker                 *child
	result                 result
}

func main() {
	o := options{}
	flag.StringVar(&o.binary, "binary", "", "explicit native Rydd executable")
	flag.StringVar(&o.profile, "profile", "defaults_no_power", "defaults_no_power or short_fixture")
	flag.IntVar(&o.seconds, "seconds", 60, "finite worker duration, 30–3600 seconds")
	flag.IntVar(&o.roots, "roots", 2, "generated roots, 1–32")
	flag.IntVar(&o.files, "files-per-root", 256, "generated files per root, 1–4096; at most 32768 total")
	flag.Parse()
	if flag.NArg() != 0 || !o.valid() {
		fmt.Fprintln(os.Stderr, errInput)
		os.Exit(2)
	}
	ctx, cancel := measurementContext(time.Duration(o.seconds+90) * time.Second)
	defer cancel()
	measured, err := run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(measured); err != nil {
		fmt.Fprintln(os.Stderr, "measurement output failed")
		os.Exit(1)
	}
}

func measurementContext(limit time.Duration) (context.Context, context.CancelFunc) {
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(signalCtx, limit)
	return ctx, func() { cancel(); stop() }
}

func run(ctx context.Context, o options) (measured result, runErr error) {
	if !o.valid() {
		return result{}, errInput
	}
	binary, err := filepath.Abs(o.binary)
	if err != nil {
		return result{}, errInput
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		return result{}, errInput
	}
	base, err := os.MkdirTemp("", "rydd-background-resources-")
	if err != nil {
		return result{}, errChild
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return result{}, errChild
	}
	fmt.Fprintln(os.Stderr, "Generated fixture and private logs retained at", base)
	r := &runner{base: base, stateDir: filepath.Join(base, "state"), binary: binary}
	for _, name := range []string{"tmp", "runtime"} {
		if err := os.Mkdir(filepath.Join(base, name), 0700); err != nil {
			return result{}, errChild
		}
	}
	r.env = []string{"TMPDIR=" + filepath.Join(base, "tmp"), "XDG_RUNTIME_DIR=" + filepath.Join(base, "runtime"), "PATH=" + os.Getenv("PATH")}
	// The CLI requires the caller's home string even with explicit --data-dir.
	// Preserve that entry unchanged; all actual fixture paths remain explicit.
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "HOME=") {
			r.env = append(r.env, entry)
			break
		}
	}
	r.roots, err = createFixture(ctx, base, o.roots, o.files)
	if err != nil {
		return result{}, err
	}
	before, err := fixtureDigest(ctx, r.roots, o.files)
	if err != nil {
		return result{}, err
	}
	cfg := config.Default()
	cfg.Roots = r.roots
	cfg.Scan.PauseOnBattery = false
	if o.profile == "short_fixture" {
		cfg.Scan.WorkSeconds, cfg.Scan.IntervalSeconds = 1, 5
	}
	if err := config.Create(filepath.Join(r.stateDir, "config.toml"), base, cfg); err != nil {
		return result{}, errChild
	}
	r.result = result{Contract: "generated_background_resources_v1", Platform: runtime.GOOS + "/" + runtime.GOARCH, Profile: o.profile, ConfiguredScan: scanSettings(cfg.Scan), ProductionDefaultsExceptPower: o.profile == "defaults_no_power", PowerProbesDisabled: true, RequestedSeconds: o.seconds, Roots: o.roots, Files: o.roots * o.files, SampleIntervalNS: int64(sampleInterval), Latencies: map[string]*latency{}, PhysicalReadMeasurement: "unknown", SystemWakeupMeasurement: "unknown", ObserverOverhead: "parent fixture generation, hashing and status sampling excluded from worker CPU/RSS; CLI process CPU reported separately; probes can perturb worker timing", SampledPeaksAreLowerBounds: true}
	r.result.WorkerUsageScope = "full child lifetime CPU/RSS includes startup, admitted work, controls and idle; elapsed includes launch and output drain; this is not the saved dispatch-window CPU scope"
	r.result.CleanupScope = "direct child killed if needed and reaped; descendants are outside the cleanup scope"
	if _, err := r.command(ctx, "state-init", "state", "init"); err != nil {
		return result{}, err
	}
	workerCtx, workerCancel := context.WithTimeout(ctx, time.Duration(o.seconds+45)*time.Second)
	defer workerCancel()
	r.worker, err = startChild(workerCtx, binary, []string{"--data-dir", r.stateDir, "daemon", "--experimental-scan"}, r.env, base, workerOutputLimit)
	if err != nil {
		return result{}, err
	}
	defer func() {
		r.worker.terminate()
		if err := r.writePrivate("worker", r.worker.result); err != nil && runErr == nil {
			measured, runErr = result{}, err
		}
	}()
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = r.untilStatus(readyCtx, func(s statusView) bool {
		return s.Worker.Live != nil && s.Worker.State == "running" && s.Worker.Live.PID == r.worker.cmd.Process.Pid
	})
	cancel()
	if err != nil {
		return result{}, err
	}
	deadline := r.worker.started.Add(time.Duration(o.seconds) * time.Second)
	pauseAt := r.worker.started.Add(time.Duration(o.seconds) * time.Second / 2)
	nextSample := time.Now()
	paused := false
	for time.Now().Before(deadline) {
		if !paused && !time.Now().Before(pauseAt) {
			if err := r.pauseCheck(ctx); err != nil {
				return result{}, err
			}
			paused = true
		}
		if !time.Now().Before(nextSample) {
			s, err := r.status(ctx)
			if err != nil {
				return result{}, err
			}
			if err := r.sample(s); err != nil {
				return result{}, err
			}
			nextSample = time.Now().Add(sampleInterval)
		}
		wake := minTime(nextSample, deadline)
		if !paused {
			wake = minTime(wake, pauseAt)
		}
		if err := r.waitUntil(ctx, wake); err != nil {
			return result{}, err
		}
	}
	if !paused {
		return result{}, errors.New("paused phase was not exercised")
	}
	stopping, err := r.control(ctx, "stop")
	if err != nil || !stopping.Stopping {
		return result{}, errors.New("worker stop was not acknowledged")
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	stopped := r.worker.wait(stopCtx)
	cancel()
	if stopped.err != nil || !r.worker.cmd.ProcessState.Success() {
		return result{}, errors.New("worker did not exit gracefully")
	}
	r.result.WorkerUsage, r.result.GracefulExit = stopped.usage, true
	final, err := r.status(ctx)
	if err != nil || final.Worker.State != "not-running" || final.Worker.Live != nil || final.State.RunningJobs != 0 || final.Metadata.TotalCharges == nil || final.Metadata.TotalCharges.OutstandingReserved != 0 || final.CPUFeedback.Status == "pending" {
		return result{}, errors.New("stopped worker has unresolved progress/accounting")
	}
	if err := r.sample(final); err != nil {
		return result{}, err
	}
	r.result.Final = final.State
	if final.State.Entries < 1 || final.Metadata.TotalCharges.Observed < 1 || !r.sourceObserved() {
		return result{}, errors.New("generated source work was not observed")
	}
	after, err := fixtureDigest(ctx, r.roots, o.files)
	if err != nil || before != after {
		return result{}, errors.New("generated source bodies changed or are unavailable")
	}
	r.result.BodiesSHA256, r.result.BodiesUnchanged = after, true
	return r.result, nil
}

func createFixture(ctx context.Context, base string, roots, files int) ([]string, error) {
	paths := make([]string, roots)
	for i := range paths {
		paths[i] = filepath.Join(base, fmt.Sprintf("root-%02d", i))
		if err := os.Mkdir(paths[i], 0700); err != nil {
			return nil, errChild
		}
		for n := 0; n < files; n++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			body := fixtureBody(i, n)
			if err := os.WriteFile(filepath.Join(paths[i], fmt.Sprintf("file-%06d", n)), body, 0600); err != nil {
				return nil, errChild
			}
		}
	}
	return paths, nil
}

func fixtureBody(root, file int) []byte {
	prefix := fmt.Sprintf("rydd-background-fixture-v1 root=%02d file=%06d\n", root, file)
	return []byte(prefix + strings.Repeat("x", 512-len(prefix)))
}

func fixtureDigest(ctx context.Context, roots []string, files int) (string, error) {
	h := sha256.New()
	for i, root := range roots {
		for n := 0; n < files; n++ {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			path := filepath.Join(root, fmt.Sprintf("file-%06d", n))
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() != 512 {
				return "", errChild
			}
			fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
			if err != nil {
				return "", errChild
			}
			f := os.NewFile(uintptr(fd), path)
			opened, statErr := f.Stat()
			data, readErr := io.ReadAll(io.LimitReader(f, 513))
			closeErr := f.Close()
			if statErr != nil || readErr != nil || closeErr != nil || !os.SameFile(info, opened) || !bytes.Equal(data, fixtureBody(i, n)) {
				return "", errChild
			}
			_, _ = h.Write(data)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (r *runner) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := startChild(commandCtx, r.binary, append([]string{"--data-dir", r.stateDir}, append(args, "--json")...), r.env, r.base, cliOutputLimit)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	got := c.wait(commandCtx)
	if err := r.writePrivate(name, got); err != nil {
		return nil, err
	}
	r.result.CLIProcesses++
	r.result.CLIUserCPUNS += got.usage.UserCPUNS
	r.result.CLISystemCPUNS += got.usage.SystemCPUNS
	measurement := r.result.Latencies[name]
	if measurement == nil {
		measurement = &latency{}
		r.result.Latencies[name] = measurement
	}
	measurement.Count++
	measurement.TotalNS += got.usage.LaunchToWaitReturnNS
	measurement.MaxNS = max(measurement.MaxNS, got.usage.LaunchToWaitReturnNS)
	if got.err != nil {
		return nil, got.err
	}
	var envelope struct {
		APIVersion int    `json:"api_version"`
		OK         bool   `json:"ok"`
		Command    string `json:"command"`
	}
	expected := args[0]
	if expected == "state" && len(args) > 1 {
		expected += " " + args[1]
	}
	if json.Unmarshal(got.stdout, &envelope) != nil || envelope.APIVersion != 1 || !envelope.OK || envelope.Command != expected {
		return nil, errProtocol
	}
	return got.stdout, nil
}

func (r *runner) writePrivate(name string, got childResult) error {
	for _, item := range []struct {
		suffix string
		data   []byte
	}{{"stdout", got.stdout}, {"stderr", got.stderr}} {
		if err := os.WriteFile(filepath.Join(r.base, name+"."+item.suffix), item.data, 0600); err != nil {
			return errChild
		}
	}
	return nil
}

func (r *runner) status(ctx context.Context) (statusView, error) {
	var s statusView
	data, err := r.command(ctx, "status", "status")
	if err != nil {
		return s, err
	}
	if json.Unmarshal(data, &s) != nil || s.Command != "status" || s.State.Schema < 12 || s.Metadata.Contract != state.MetadataBudgetContract || !s.Metadata.Available || s.CPUFeedback.Contract != state.CPUFeedbackContract || !s.CPUFeedback.Available {
		return s, errProtocol
	}
	return s, nil
}

func (r *runner) control(ctx context.Context, action string) (worker.Snapshot, error) {
	var reply struct {
		Command      string          `json:"command"`
		Acknowledged bool            `json:"acknowledged"`
		Worker       worker.Snapshot `json:"worker"`
	}
	data, err := r.command(ctx, action, action)
	if err != nil {
		return reply.Worker, err
	}
	if json.Unmarshal(data, &reply) != nil || reply.Command != action || !reply.Acknowledged || reply.Worker.PID != r.worker.cmd.Process.Pid {
		return reply.Worker, errProtocol
	}
	return reply.Worker, nil
}

func (r *runner) untilStatus(ctx context.Context, ready func(statusView) bool) (statusView, error) {
	for {
		s, err := r.status(ctx)
		if err != nil {
			return s, err
		}
		if ready(s) {
			return s, nil
		}
		if err := r.waitUntil(ctx, time.Now().Add(250*time.Millisecond)); err != nil {
			return s, err
		}
	}
}

func (r *runner) waitUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(max(time.Until(at), 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.New("generated fixture deadline reached")
	case <-r.worker.done:
		return errChild
	case <-timer.C:
		return nil
	}
}

func (r *runner) pauseCheck(ctx context.Context) error {
	snapshot, err := r.control(ctx, "pause")
	if err != nil || !snapshot.Paused {
		return errors.New("worker pause was not acknowledged")
	}
	settleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	before, err := r.untilStatus(settleCtx, func(s statusView) bool {
		return s.Worker.Live != nil && s.Worker.Live.Paused && s.Worker.Live.ActiveJob == 0 && s.CPUFeedback.Status != "pending" && s.Metadata.TotalCharges != nil && s.Metadata.TotalCharges.OutstandingReserved == 0
	})
	cancel()
	if err != nil {
		return err
	}
	if err := r.sample(before); err != nil {
		return err
	}
	reportData, err := r.command(ctx, "report", "report", "--limit", "10")
	if err != nil {
		return err
	}
	var report struct {
		Report struct {
			Source  string `json:"source"`
			Current bool   `json:"current_state_verified"`
			Files   []struct {
				Size int64 `json:"logical_bytes"`
			} `json:"files"`
		} `json:"report"`
	}
	if json.Unmarshal(reportData, &report) != nil || report.Report.Source != "saved_inventory" || report.Report.Current || len(report.Report.Files) < 1 || len(report.Report.Files) > 10 {
		return errProtocol
	}
	for _, file := range report.Report.Files {
		if file.Size != 512 {
			return errors.New("saved report differs from independently generated file sizes")
		}
	}
	if err := r.waitUntil(ctx, time.Now().Add(2*time.Second)); err != nil {
		return err
	}
	after, err := r.status(ctx)
	if err != nil {
		return err
	}
	if !pausedStable(before, after) {
		return errors.New("paused worker changed source progress or charged dispatch")
	}
	if err := r.sample(after); err != nil {
		return err
	}
	r.result.PauseVerified = true
	resumed, err := r.control(ctx, "resume")
	if err != nil || resumed.Paused {
		return errors.New("worker resume was not acknowledged")
	}
	r.result.ResumeVerified = true
	return nil
}

func pausedStable(a, b statusView) bool {
	if a.Worker.Live == nil || b.Worker.Live == nil || !a.Worker.Live.Paused || !b.Worker.Live.Paused || a.Worker.Live.ActiveJob != 0 || b.Worker.Live.ActiveJob != 0 || a.State.Entries != b.State.Entries || a.Metadata.TotalCharges == nil || b.Metadata.TotalCharges == nil || a.Metadata.TotalCharges.Reserved != b.Metadata.TotalCharges.Reserved || a.Metadata.TotalCharges.OutstandingReserved != 0 || b.Metadata.TotalCharges.OutstandingReserved != 0 || a.CPUFeedback.Status == "pending" || b.CPUFeedback.Status == "pending" {
		return false
	}
	if a.State.PendingJobs != b.State.PendingJobs || a.State.RunningJobs != b.State.RunningJobs || a.State.CompleteDirectories != b.State.CompleteDirectories || a.State.DirectoryErrors != b.State.DirectoryErrors || a.State.SkippedEntries != b.State.SkippedEntries || a.State.EnabledRoots != b.State.EnabledRoots || a.Worker.Live.RecoveredJobs != b.Worker.Live.RecoveredJobs {
		return false
	}
	// Saved-only CLI views can roll to another UTC day without a reservation.
	// Compare the worker's retained charged-dispatch snapshot while paused.
	if a.Worker.Live.Dispatch == nil || b.Worker.Live.Dispatch == nil || a.Worker.Live.Dispatch.Day != b.Worker.Live.Dispatch.Day || a.Worker.Live.Dispatch.Used != b.Worker.Live.Dispatch.Used {
		return false
	}
	if a.Worker.Live.InventoryMetrics == nil || b.Worker.Live.InventoryMetrics == nil {
		return false
	}
	return *a.Worker.Live.InventoryMetrics == *b.Worker.Live.InventoryMetrics
}

func (r *runner) sample(s statusView) error {
	if len(r.result.Samples) >= maxSamples {
		return errors.New("generated fixture sample limit reached")
	}
	m := sample{ElapsedNS: time.Since(r.worker.started).Nanoseconds(), Entries: s.State.Entries, PendingJobs: s.State.PendingJobs, RunningJobs: s.State.RunningJobs, CompleteDirectories: s.State.CompleteDirectories, Dispatch: s.Dispatch, Metadata: s.Metadata, CPU: cpuEvidence{Status: s.CPUFeedback.Status, CompletedUnknown: s.CPUFeedback.CompletedUnknownWindows, RecoveredUnknown: s.CPUFeedback.RecoveredUnknownWindows}}
	if window := s.CPUFeedback.Window; window != nil {
		m.CPU.CPUTimeNS, m.CPU.ElapsedNS, m.CPU.BackoffNS = window.CPUTimeNS, window.ElapsedNS, window.BackoffNS
	}
	if live := s.Worker.Live; live != nil {
		m.Paused, m.ActiveJob, m.WaitReason, m.Metrics = live.Paused, live.ActiveJob, safeReason(live.WaitReason), live.InventoryMetrics
	}
	r.result.Samples = append(r.result.Samples, m)
	r.result.PeakDatabaseBytes = max(r.result.PeakDatabaseBytes, s.State.DatabaseBytes)
	r.result.PeakWALBytes = max(r.result.PeakWALBytes, s.State.WALBytes)
	return nil
}

func safeReason(reason string) string {
	if len(reason) > 64 {
		return "unrecognized"
	}
	for _, c := range reason {
		if !(c >= 'a' && c <= 'z' || c == '_') {
			return "unrecognized"
		}
	}
	return reason
}
func (r *runner) sourceObserved() bool {
	for _, s := range r.result.Samples {
		if s.Metrics != nil && s.Metrics.EntryInspections > 0 && s.Metrics.StatCalls > 0 {
			return true
		}
	}
	return false
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func scanSettings(s config.Scan) map[string]any {
	return map[string]any{"work_seconds": s.WorkSeconds, "interval_seconds": s.IntervalSeconds, "metadata_per_second": s.MetadataPerSecond, "metadata_attempts_per_day": s.MetadataAttemptsPerDay, "read_bytes_per_second": s.ReadBytesPerSecond, "read_bytes_per_day": s.ReadBytesPerDay, "pause_on_battery": s.PauseOnBattery, "max_scan_chunks_per_day": s.MaxScanChunksPerDay, "max_state_bytes": s.MaxStateBytes}
}
