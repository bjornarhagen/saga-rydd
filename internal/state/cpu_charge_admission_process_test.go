package state

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCPUChargeAdmissionKillChild(t *testing.T) {
	dir := os.Getenv("RYDD_CPU_ADMISSION_CHILD_DIR")
	if dir == "" {
		t.Skip("disposable child fixture only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := OpenWriter(ctx, dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	at := chargeNow()
	if _, e = s.ActivateCPUCharges(ctx, at); e != nil {
		t.Fatal(e)
	}
	phase := os.Getenv("RYDD_CPU_ADMISSION_CHILD_PHASE")
	held := func() {
		fmt.Println("cpu-admission-ready:1")
		<-ctx.Done()
		t.Fatal("parent did not kill bounded child")
	}
	hooks := cpuAdmissionHooks{}
	if strings.HasSuffix(phase, "before") {
		hooks.beforeCommit = held
	} else {
		hooks.afterCommit = held
	}
	if strings.HasPrefix(phase, "activate") {
		_, e = s.activateCPUChargeAdmission(ctx, at, CPUChargeLimits{HourNS: 100}, hooks)
	} else {
		if _, e = s.ActivateCPUChargeAdmission(ctx, at, CPUChargeLimits{HourNS: 100}); e != nil {
			t.Fatal(e)
		}
		q := admissionStart(0, 0, at, 321, "a", CPUChargeLimits{HourNS: 100})
		if strings.HasPrefix(phase, "begin") {
			_, _, _, e = s.beginCPUSessionLimited(ctx, q, hooks)
		} else {
			_, c, a, err := s.BeginCPUSessionLimited(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			_, _, e = s.openCPUTrackingGap(ctx, CPUTrackingGapRequest{a.PolicyRevision, c.Generation, strings.Repeat("b", 64), at.Add(time.Second)}, hooks)
		}
	}
	t.Fatal("fixture did not remain at declared publication boundary", e)
}
func TestCPUChargeAdmissionActualSIGKILLAtomicityAndRecovery(t *testing.T) {
	for _, phase := range []string{"activate_before", "activate_after", "begin_before", "begin_after", "gap_before", "gap_after"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := privateDir(t)
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCPUChargeAdmissionKillChild$")
			cmd.Env = append(os.Environ(), "RYDD_CPU_ADMISSION_CHILD_DIR="+dir, "RYDD_CPU_ADMISSION_CHILD_PHASE="+phase)
			cmd.WaitDelay = time.Second
			var stderr cpuFixtureOutput
			cmd.Stderr = &stderr
			stdout, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = stdout.Close() }()
			ready := make(chan string, 1)
			go func() {
				scan := bufio.NewScanner(stdout)
				scan.Buffer(make([]byte, 256), 256)
				if scan.Scan() {
					ready <- scan.Text()
				} else {
					ready <- ""
				}
			}()
			select {
			case line := <-ready:
				if line != "cpu-admission-ready:1" {
					t.Fatal("child not ready", line, stderr.String())
				}
			case <-ctx.Done():
				t.Fatal("bounded readiness failed", stderr.String())
			}
			if e = cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			e = cmd.Wait()
			var failed *exec.ExitError
			if !errors.As(e, &failed) {
				t.Fatal("no signaled exit", e)
			}
			status, ok := failed.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("not actual SIGKILL", failed.ProcessState)
			}
			r, e := OpenReader(ctx, dir)
			if e != nil {
				t.Fatal(e)
			}
			c, e := r.CPUCharges(ctx)
			if e != nil {
				t.Fatal(e)
			}
			a, e := r.CPUChargeAdmission(ctx)
			if e != nil {
				t.Fatal(e)
			}
			expectedAvailable := phase != "activate_before"
			if a.Available != expectedAvailable {
				t.Fatal("partial migration published", a)
			}
			switch phase {
			case "activate_before", "activate_after", "begin_before":
				if c.Generation != 0 || c.ChargedCPUNS != 0 || c.RecoveredSessions != 0 {
					t.Fatal(c)
				}
			case "begin_after", "gap_before":
				if c.Generation != 1 || c.ChargedCPUNS != 321 || c.Status != "active" || c.RecoveredSessions != 0 || a.TrackingGapOpen {
					t.Fatal(c, a)
				}
			case "gap_after":
				if c.Generation != 1 || c.ChargedCPUNS != 321 || c.Status != "recovered_unknown" || c.RecoveredSessions != 1 || !a.TrackingGapOpen || a.PolicyRevision != 2 {
					t.Fatal(c, a)
				}
			}
			beforeCPU := chargeBytes(t, r)
			var beforeAdmission []byte
			if expectedAvailable {
				beforeAdmission = admissionBytes(t, r)
				admissionAssertTotals(t, c, a)
			}
			r.Close()
			w, e := OpenWriter(ctx, dir)
			if e != nil {
				t.Fatal(e)
			}
			defer w.Close()
			if !bytes.Equal(beforeCPU, chargeBytes(t, w)) || expectedAvailable && !bytes.Equal(beforeAdmission, admissionBytes(t, w)) {
				t.Fatal("ordinary reopen recovered or mutated state")
			}
			if !expectedAvailable {
				return
			}
			c, a, e = w.RecoverCPUSessionLimited(ctx, chargeNow().Add(2*time.Second))
			if e != nil {
				t.Fatal(c, a, e)
			}
			recovered := c.RecoveredSessions
			if phase == "begin_after" || phase == "gap_before" {
				if recovered != 1 || !a.Hour.BlockingUnknown || !a.Day.BlockingUnknown || c.ChargedCPUNS != 321 {
					t.Fatal(c, a)
				}
			}
			frozen := admissionBytes(t, w)
			c, a, e = w.RecoverCPUSessionLimited(ctx, chargeNow().Add(10*time.Hour))
			if e != nil || c.RecoveredSessions != recovered || !bytes.Equal(frozen, admissionBytes(t, w)) {
				t.Fatal("recovery repeated closure/renewed period", c, a, e)
			}
		})
	}
}

// The native gate supplies a pinned genuine schema-15 production executable.
// A positive schema-15 control distinguishes it from an older schema-14 CLI;
// neither the current test helper nor a synthetic version label substitutes.
func TestCPUChargeAdmissionOldSchema15CLIRefusesActivatedStore(t *testing.T) {
	binary := os.Getenv("RYDD_CPU_ADMISSION_OLD_CLI")
	if binary == "" {
		t.Skip("explicit genuine schema-15 native executable fixture")
	}
	info, e := os.Lstat(binary)
	if e != nil || !filepath.IsAbs(binary) || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		t.Fatal("old executable fixture must be a direct absolute executable", e)
	}
	s, dir := chargeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := filepath.Join(dir, "generated-root")
	if e = os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	data := cpuOldCLIConfig(t, root)
	f, e := os.OpenFile(filepath.Join(dir, "config.toml"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if n, err := f.Write(data); err != nil || n != len(data) {
		_ = f.Close()
		t.Fatal("incomplete baseline configuration write", err)
	}
	if e = f.Sync(); e != nil {
		_ = f.Close()
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	baselineCPU := chargeBytes(t, s)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	invoke := func() (string, string, error) {
		cmd := exec.CommandContext(ctx, binary, "--data-dir", dir, "state", "init")
		cmd.WaitDelay = time.Second
		var stdout, stderr cpuFixtureOutput
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		return stdout.String(), stderr.String(), err
	}
	stdout, stderr, e := invoke()
	if e != nil || !strings.Contains(stdout, "State ready") {
		t.Fatal("selected executable did not genuinely accept schema15", e, stdout, stderr)
	}
	s, e = OpenWriter(ctx, dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(baselineCPU, chargeBytes(t, s)) {
		t.Fatal("schema15 positive control changed CPU accounting")
	}
	var version int
	if e = s.db.QueryRow("PRAGMA user_version").Scan(&version); e != nil || version != 15 {
		t.Fatal("positive control did not preserve schema15", version, e)
	}
	if _, e = s.db.Exec("UPDATE roots SET volume_id='generated-volume',last_scan_ns=123,last_error='retained diagnostic' WHERE id=1"); e != nil {
		t.Fatal(e)
	}
	if e = s.EnqueueJob(ctx, 1, "generated-pending", []byte("pending"), chargeNow()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ActivateCPUChargeAdmission(ctx, chargeNow(), CPUChargeLimits{HourNS: 100}); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.BeginCPUSessionLimited(ctx, admissionStart(0, 0, chargeNow(), 100, "a", CPUChargeLimits{HourNS: 100})); e != nil {
		t.Fatal(e)
	}
	beforeCPU, beforePeriod := chargeBytes(t, s), admissionBytes(t, s)
	// Read only fixed generated membership/queue records, independent of CPU JSON.
	inventory := func(st *Store) string {
		t.Helper()
		var id, enabled, scan, job, attempts, due int64
		var path, parent []byte
		var volume, diagnostic, status string
		if err := st.db.QueryRow("SELECT id,path,enabled,volume_id,last_scan_ns,last_error FROM roots WHERE id=1").Scan(&id, &path, &enabled, &volume, &scan, &diagnostic); err != nil {
			t.Fatal(err)
		}
		if err := st.db.QueryRow("SELECT id,path,status,attempts,due_at_ns FROM jobs WHERE root_id=1 AND kind='generated-pending'").Scan(&job, &parent, &status, &attempts, &due); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%d:%x:%d:%s:%d:%s:%d:%x:%s:%d:%d", id, path, enabled, volume, scan, diagnostic, job, parent, status, attempts, due)
	}
	beforeInventory := inventory(s)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	stdout, stderr, e = invoke()
	var failed *exec.ExitError
	if !errors.As(e, &failed) || failed.ExitCode() != 1 || !strings.Contains(stderr, "unsupported state schema 16") || strings.Contains(stdout, "State ready") {
		t.Fatal("schema15 binary did not refuse schema16", e, stdout, stderr)
	}
	r, e := OpenReader(ctx, dir)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if !bytes.Equal(beforeCPU, chargeBytes(t, r)) || !bytes.Equal(beforePeriod, admissionBytes(t, r)) || beforeInventory != inventory(r) {
		t.Fatal("older CLI changed ledgers or retained inventory")
	}
	summary, e := r.Summary(ctx)
	if e != nil || summary.Schema != 16 || summary.EnabledRoots != 1 || summary.PendingJobs != 1 {
		t.Fatal("older CLI changed ordinary inventory shape", summary, e)
	}
}
