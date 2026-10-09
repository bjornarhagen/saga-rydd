package inventory

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

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func hashConsentProcessClock() time.Time {
	return time.Date(2026, 1, 2, 23, 59, 30, 0, time.UTC)
}

type hashConsentFixture struct {
	store    *HashStore
	source   *state.Store
	scanner  *Scanner
	base     string
	stateDir string
	root     string
	proposal HashProposal
}

// The manual locator and all selected evidence come from a real disposable
// Scanner.Next/CommitScan pass. The selected file requires more than one fixed
// consented invocation, so neither a crash nor a canceled slice can hide behind
// a completed tiny-file checkpoint.
func newHashConsentFixture(t *testing.T) *hashConsentFixture {
	t.Helper()
	ctx := context.Background()
	data := fullHashContents(int(FileHashStepByteLimit) + 65)
	scanner, targets := sampleFixture(t, data, bytes.Clone(data))
	root := string(targets[0].Root.PathBytes)
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(parent, "consent-data")
	stateDir := filepath.Join(base, "manual", hashManualInventoryKey([]byte(root)))
	writer, err := state.OpenWriter(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if err = writer.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = writer.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = writer.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for i := 0; i < 16; i++ {
		job, e := writer.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if job == nil {
			finished = true
			break
		}
		batch, e := scanner.Next(ctx, *job)
		if e != nil || batch.Fault != "" {
			t.Fatal(batch, e)
		}
		if e = writer.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("manual consent fixture did not drain its bounded inventory")
	}
	finishSelectionFixtureMaintenance(t, ctx, writer, scanner)
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := state.OpenReader(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	report, err := source.SameSizeCandidates(ctx, 20, "", 1)
	if err != nil || len(report.Bands) != 1 || len(report.Bands[0].Files) != 2 {
		t.Fatal(report, err)
	}
	store, err := OpenHashWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	store.now = hashConsentProcessClock
	t.Cleanup(func() { _ = store.Close() })
	proposal, err := store.CreateManualSelection(ctx, source, report.InventoryID, report.Bands[0].Files[:1], []byte(root))
	if err != nil {
		t.Fatal(err)
	}
	return &hashConsentFixture{store: store, source: source, scanner: scanner, base: base, stateDir: stateDir, root: root, proposal: proposal}
}

func (f *hashConsentFixture) approvalRequest(day, total int64) HashReadApprovalRequest {
	return HashReadApprovalRequest{StoreID: f.proposal.StoreID, SelectionID: f.proposal.SelectionID, InventoryID: f.proposal.InventoryID, SourceLocator: cloneHashReadLocator(*f.proposal.SourceLocator), DailyReservedByteLimit: day, LifetimeReservedByteLimit: total, ConfirmFullFileRead: true}
}

func (f *hashConsentFixture) approve(t *testing.T, day, total int64) HashReadConsent {
	t.Helper()
	c, err := f.store.ApproveRead(context.Background(), f.approvalRequest(day, total))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *hashConsentFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenHashWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	s.now = hashConsentProcessClock
	f.store = s
	t.Cleanup(func() { _ = s.Close() })
}

// The subprocess stops only at an exact commit/read seam. Parent SIGKILL is
// the intended continuation; no timer chooses whether publication occurred.
func TestHashReadConsentCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_HASH_CONSENT_CRASH_BASE")
	if base == "" {
		return
	}
	ctx := context.Background()
	store, err := OpenHashWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.now = hashConsentProcessClock
	stop := func() {
		if _, e := fmt.Fprintln(os.Stdout, "READY"); e != nil {
			t.Fatal(e)
		}
		var b [1]byte
		if _, e := os.Stdin.Read(b[:]); e != nil {
			t.Fatal(e)
		}
		t.Fatal("consent crash helper unexpectedly resumed")
	}
	stage := os.Getenv("RYDD_HASH_CONSENT_CRASH_STAGE")
	id := os.Getenv("RYDD_HASH_CONSENT_CRASH_ID")
	switch stage {
	case "before_approval_commit", "after_approval_commit":
		snapshot, e := store.Snapshot(ctx)
		if e != nil {
			t.Fatal(e)
		}
		p, e := store.Proposal(ctx, snapshot.SelectionID)
		if e != nil || p.SourceLocator == nil {
			t.Fatal(p, e)
		}
		req := HashReadApprovalRequest{StoreID: p.StoreID, SelectionID: p.SelectionID, InventoryID: p.InventoryID, SourceLocator: *p.SourceLocator, DailyReservedByteLimit: FileHashStepByteLimit, LifetimeReservedByteLimit: 2 * FileHashStepByteLimit, ConfirmFullFileRead: true}
		hooks := hashReadHooks{}
		if stage == "before_approval_commit" {
			hooks.beforeApprovalCommit = stop
		} else {
			hooks.afterApprovalCommit = stop
		}
		c, e := store.approveRead(ctx, req, hooks)
		t.Fatal("approval crash hook was not reached", c, e)
	case "before_revocation_commit", "after_revocation_commit":
		hooks := hashReadHooks{}
		if stage == "before_revocation_commit" {
			hooks.beforeRevocationCommit = stop
		} else {
			hooks.afterRevocationCommit = stop
		}
		c, e := store.revokeRead(ctx, id, hooks)
		t.Fatal("revocation crash hook was not reached", c, e)
	case "after_reserve", "after_read":
		root := os.Getenv("RYDD_HASH_CONSENT_CRASH_ROOT")
		source, e := state.OpenReader(ctx, filepath.Join(base, "manual", hashManualInventoryKey([]byte(root))))
		if e != nil {
			t.Fatal(e)
		}
		defer source.Close()
		scanner, e := New([]string{root}, nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		defer scanner.Close()
		hooks := hashStoreHooks{}
		if stage == "after_reserve" {
			hooks.afterReserve = stop
		} else {
			hooks.file.afterRead = func(int) { stop() }
		}
		r, e := store.runConsented(ctx, id, source, scanner, hooks)
		t.Fatal("consent reservation crash hook was not reached", r, e)
	default:
		t.Fatal("unknown consent crash stage")
	}
}

func killHashReadConsentAt(t *testing.T, f *hashConsentFixture, id, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashReadConsentCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_HASH_CONSENT_CRASH_BASE="+f.base, "RYDD_HASH_CONSENT_CRASH_ROOT="+f.root, "RYDD_HASH_CONSENT_CRASH_ID="+id, "RYDD_HASH_CONSENT_CRASH_STAGE="+stage)
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
	ready := make(chan bool, 1)
	go func() { scan := bufio.NewScanner(stdout); ready <- scan.Scan() && scan.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("consent helper did not reach exact seam", stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("consent helper timed out", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("consent helper survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("consent helper did not die from SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashReadConsentProcessPublicationSurvivesLostOutput(t *testing.T) {
	for _, stage := range []string{"before_approval_commit", "after_approval_commit", "before_revocation_commit", "after_revocation_commit"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			f := newHashConsentFixture(t)
			id := ""
			if stage == "before_revocation_commit" || stage == "after_revocation_commit" {
				id = f.approve(t, FileHashStepByteLimit, 2*FileHashStepByteLimit).ID
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.source.Close(); err != nil {
				t.Fatal(err)
			}
			// Both lifecycle publications use only the hash DB. Offline source
			// and inventory cannot be a hidden prerequisite or read path.
			for _, path := range []string{f.root, f.stateDir} {
				if err := os.Rename(path, path+"-offline"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Rename(path+"-offline", path); err != nil {
						t.Error(err)
					}
				})
			}
			killHashReadConsentAt(t, f, id, stage)
			reader, err := OpenHashReader(ctx, f.base)
			if err != nil {
				t.Fatal(err)
			}
			reader.now = func() time.Time { return hashConsentProcessClock().Add(48 * time.Hour) }
			snapshot, err := reader.Snapshot(ctx)
			if err != nil || snapshot.Budget != nil || len(snapshot.Work) != 1 || snapshot.Work[0].Sequence != 0 || snapshot.Work[0].DurableOffset != 0 || snapshot.Work[0].LatestAttempt != nil {
				t.Fatal("offline lifecycle read changed work or reservation accounting", snapshot, err)
			}
			if stage == "before_approval_commit" {
				if snapshot.ReadConsent != nil {
					t.Fatal("uncommitted approval survived process loss", snapshot)
				}
			} else {
				c := snapshot.ReadConsent
				if c == nil || c.CurrentReadPermissionEvaluated || c.ExpiredObserved || c.ClockHighWater != hashConsentProcessClock() || c.ID == "" || (id != "" && c.ID != id) {
					t.Fatal("lost output changed approval identity or evaluated reader permission", c)
				}
				wantRevoked := stage == "after_revocation_commit"
				if (c.Revocation != nil) != wantRevoked || (c.Status == "revoked") != wantRevoked {
					t.Fatal("revocation publication was not atomic", c)
				}
				shown, e := reader.Approval(ctx, c.ID)
				if e != nil || !reflect.DeepEqual(shown, *c) {
					t.Fatal("saved approval cannot recover a lost response", shown, e)
				}
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			repeated, err := f.store.Snapshot(ctx)
			if err != nil || !reflect.DeepEqual(snapshot, repeated) {
				t.Fatal("writer restart changed lifecycle evidence or settled nonexistent work", repeated, err)
			}
		})
	}
}

func TestHashReadConsentProcessUnknownChargeSurvivesMidnight(t *testing.T) {
	for _, stage := range []string{"after_reserve", "after_read"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			f := newHashConsentFixture(t)
			approval := f.approve(t, FileHashStepByteLimit, FileHashStepByteLimit)
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			killHashReadConsentAt(t, f, approval.ID, stage)
			f.reopen(t)
			before := hashStoreSnapshot(t, f.store)
			if before.ReadConsent == nil || before.ReadConsent.ID != approval.ID || before.Budget == nil || before.Budget.TotalReservedBytes != FileHashStepByteLimit || before.Budget.TotalUnknownReservedBytes != FileHashStepByteLimit || before.Budget.TotalReadBytes != 0 || before.Work[0].DurableOffset != 0 || before.Work[0].Sequence != 1 || before.Work[0].LatestAttempt == nil || before.Work[0].LatestAttempt.Status != "interrupted_unknown" || before.Work[0].LatestAttempt.ReadBytes != nil {
				t.Fatal("crash did not keep conservative lifetime charge and old prefix", before)
			}
			f.store.now = func() time.Time { return hashConsentProcessClock().Add(time.Minute) }
			opened := false
			r, err := f.store.runConsented(ctx, approval.ID, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
			if !errors.Is(err, ErrHashDeferred) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" || opened {
				t.Fatal("midnight renewed exhausted lifetime permission", r, err, opened)
			}
			after := hashStoreSnapshot(t, f.store)
			if after.Budget.TotalReservedBytes != before.Budget.TotalReservedBytes || after.Budget.TotalUnknownReservedBytes != before.Budget.TotalUnknownReservedBytes || after.Work[0].Sequence != before.Work[0].Sequence || after.Work[0].DurableOffset != 0 {
				t.Fatal("refused next-day invocation refunded or recovered an attempt twice", after)
			}
			f.reopen(t)
			repeated := hashStoreSnapshot(t, f.store)
			if !reflect.DeepEqual(after, repeated) {
				t.Fatal("repeated restart changed charge or recorded consent", repeated)
			}
		})
	}
}
