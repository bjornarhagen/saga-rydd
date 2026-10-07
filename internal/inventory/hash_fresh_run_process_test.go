package inventory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHashFreshRunCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_FRESH_RUN_CRASH_BASE")
	if base == "" {
		return
	}
	reader, err := OpenHashReader(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	request, err := reader.PrepareKeeperChoiceFreshRequest(context.Background(), os.Getenv("RYDD_FRESH_RUN_CRASH_CHOICE"))
	if err != nil {
		t.Fatal(err)
	}
	job, err := reader.FreshJob(context.Background(), os.Getenv("RYDD_FRESH_RUN_CRASH_JOB"))
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenHashFreshRunWriter(context.Background(), request, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.now = hashChoiceClock
	scanner, err := New([]string{string(request.SourceLocator().RootPathBytes)}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	stop := func() {
		if _, e := fmt.Fprintln(os.Stdout, "READY"); e != nil {
			t.Fatal(e)
		}
		var b [1]byte
		if _, e := os.Stdin.Read(b[:]); e != nil {
			t.Fatal(e)
		}
		t.Fatal("fresh run crash helper resumed")
	}
	hooks := hashFreshRunHooks{}
	switch os.Getenv("RYDD_FRESH_RUN_CRASH_STAGE") {
	case "before_reserve":
		hooks.beforeReserveCommit = stop
	case "after_reserve":
		hooks.afterReserve = stop
	case "before_settle":
		hooks.beforeSettleCommit = stop
	case "after_settle":
		hooks.afterSettleCommit = stop
	default:
		t.Fatal("unknown fresh run process boundary")
	}
	r, err := w.runFreshConsented(context.Background(), os.Getenv("RYDD_FRESH_RUN_CRASH_APPROVAL"), scanner, hooks)
	t.Fatal("fresh run crash seam was not reached", r, err)
}

func killFreshRunAt(t *testing.T, f *freshRunFixture, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashFreshRunCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_FRESH_RUN_CRASH_BASE="+f.fresh.m.f.base, "RYDD_FRESH_RUN_CRASH_CHOICE="+f.fresh.m.saved.ID, "RYDD_FRESH_RUN_CRASH_JOB="+f.fresh.job.ID, "RYDD_FRESH_RUN_CRASH_APPROVAL="+f.consent.ID, "RYDD_FRESH_RUN_CRASH_STAGE="+stage)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	type childReady struct {
		ready  bool
		output string
	}
	ready := make(chan childReady, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		if s.Scan() && s.Text() == "READY" {
			ready <- childReady{ready: true}
			return
		}
		lines := []string{s.Text()}
		for s.Scan() {
			lines = append(lines, s.Text())
		}
		if err := s.Err(); err != nil {
			lines = append(lines, err.Error())
		}
		ready <- childReady{output: strings.Join(lines, "\n")}
	}()
	select {
	case result := <-ready:
		if !result.ready {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("fresh run child failed before boundary", result.output, stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		result := <-ready
		t.Fatal("fresh run child timed out", result.output, stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("fresh run child survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("fresh run child did not die by SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashFreshRunProcessDeathChargesExactlyOnceAndRecoversOnlyExactJob(t *testing.T) {
	for _, stage := range []string{"before_reserve", "after_reserve", "before_settle", "after_settle"} {
		t.Run(stage, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			other := saveFreshJobFixture(t, f.fresh.m.f.store, f.fresh.request, hashChoiceJobKey(2))
			if err := f.fresh.m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			killFreshRunAt(t, f, stage)
			reader, err := OpenHashReader(context.Background(), f.fresh.m.f.base)
			if err != nil {
				t.Fatal(err)
			}
			beforeRecovery := requireFreshRunSaved(t, f, reader)
			prior := beforeRecovery.Progress[0]
			if stage == "after_reserve" || stage == "before_settle" {
				if prior.Status != "running" || prior.Sequence != 0 || prior.DurableOffset != 0 || prior.LatestAttempt == nil || prior.LatestAttempt.Status != "reserved" || prior.LatestAttempt.RequestedBytes != nil || prior.LatestAttempt.ReadBytes != nil || prior.LatestAttempt.ElapsedNS != nil || beforeRecovery.FreshReservedBytes != 65 || beforeRecovery.FreshReadBytes != 0 || beforeRecovery.FreshBudget.TotalUnknownReservedBytes != 0 {
					t.Fatal("saved-only crash reader invented usage or recovered reservation", beforeRecovery)
				}
			}
			otherAfter, err := reader.FreshJob(context.Background(), other.ID)
			if err != nil || !reflect.DeepEqual(otherAfter, other) {
				t.Fatal("crash/reader initialized unrelated job", otherAfter, err)
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			w := f.open(t)
			job := requireFreshRunSaved(t, f, w)
			work := job.Progress[0]
			switch stage {
			case "before_reserve":
				if job.FreshReservedBytes != 0 || job.FreshReadBytes != 0 || work.Sequence != 0 || work.Status != "pending" || work.LatestAttempt != nil {
					t.Fatal("uncommitted reservation survived process death", job)
				}
			case "after_reserve", "before_settle":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 0 || job.FreshBudget.TotalUnknownReservedBytes != 65 || work.Sequence != 1 || work.DurableOffset != 0 || work.SHA256 != "" || work.LatestAttempt.Status != "interrupted_unknown" || work.LatestAttempt.ReadBytes != nil {
					t.Fatal("exact crash recovery lost charge/unknown or imported tentative SHA", job)
				}
			case "after_settle":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 65 || job.FreshBudget.TotalUnknownReservedBytes != 0 || work.Sequence != 1 || work.Status != "complete" || work.DurableOffset != 65 || work.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(65))) {
					t.Fatal("lost committed reply discarded exact observation/accounting", job)
				}
			}
			r, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
			if err != nil {
				t.Fatal(r, err)
			}
			wantOrdinal := 2
			if stage == "before_reserve" {
				wantOrdinal = 1
			}
			if r.Ordinal != wantOrdinal || r.Status != "hash_observed" || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 {
				t.Fatal("explicit post-crash invocation repeated settled/reserved work or lost fair ordering", r)
			}
			requireFreshRunSaved(t, f, w)
			if got, e := w.FreshJob(context.Background(), other.ID); e != nil || !reflect.DeepEqual(got, other) {
				t.Fatal("exact recovery/run touched unrelated job", got, e)
			}
		})
	}
}
