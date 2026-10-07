package inventory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
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

func processHashStoreClock() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }

// The source is a disposable production inventory, not a fabricated imported
// prefix. Only the parent test creates the exact selected work.
func processHashStoreFixture(t *testing.T) (*Scanner, *state.Store, *HashStore, string, string) {
	t.Helper()
	ctx := context.Background()
	data := fullHashContents(513)
	scanner, targets := sampleFixture(t, data, bytes.Clone(data))
	root := string(targets[0].Root.PathBytes)
	base := filepath.Join(t.TempDir(), "state")
	writer, err := state.OpenWriter(ctx, base)
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
	for i := 0; i < 12; i++ {
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
		t.Fatal("production crash fixture did not drain bounded inventory")
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	source, err := state.OpenReader(ctx, base)
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
	store.now = processHashStoreClock
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.CreateSelection(ctx, source, report.InventoryID, []state.SameSizeFile{report.Bands[0].Files[0]}); err != nil {
		t.Fatal(err)
	}
	return scanner, source, store, base, root
}

func TestHashStoreCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_HASH_CRASH_BASE")
	if base == "" {
		return
	}
	ctx := context.Background()
	source, err := state.OpenReader(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	scanner, err := New([]string{os.Getenv("RYDD_HASH_CRASH_ROOT")}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	store, err := OpenHashWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	store.now = processHashStoreClock
	defer store.Close()
	stop := func() {
		if _, e := fmt.Fprintln(os.Stdout, "READY"); e != nil {
			t.Fatal(e)
		}
		// Parent keeps this pipe open and sends no bytes. SIGKILL is the only
		// expected continuation, so no timed sleep determines the crash point.
		var b [1]byte
		if _, e := os.Stdin.Read(b[:]); e != nil {
			t.Fatal(e)
		}
		t.Fatal("crash helper unexpectedly resumed")
	}
	hooks := hashStoreHooks{}
	switch os.Getenv("RYDD_HASH_CRASH_STAGE") {
	case "before_reserve_commit":
		hooks.beforeReserveCommit = stop
	case "after_reserve":
		hooks.afterReserve = stop
	case "after_read":
		hooks.file.afterRead = func(int) { stop() }
	case "before_settle_commit":
		hooks.beforeSettleCommit = stop
	case "after_settle_commit":
		hooks.afterSettleCommit = stop
	default:
		t.Fatal("unknown crash stage")
	}
	result, err := store.runNext(ctx, source, scanner, 64, 128, hooks)
	t.Fatal("crash hook was not reached", result, err)
}

func killHashStoreAt(t *testing.T, base, root, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashStoreCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_HASH_CRASH_BASE="+base, "RYDD_HASH_CRASH_ROOT="+root, "RYDD_HASH_CRASH_STAGE="+stage)
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
			t.Fatal("crash helper did not reach exact seam", stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("crash helper timed out", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("helper survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("helper did not die from SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashStoreProcessCrashAtomicReservationAndCheckpoint(t *testing.T) {
	for _, stage := range []string{"before_reserve_commit", "after_reserve", "after_read", "before_settle_commit", "after_settle_commit"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			scanner, source, store, base, root := processHashStoreFixture(t)
			first, err := store.RunNext(ctx, source, scanner, 64, 128)
			if err != nil || first.DurableOffset != 64 || first.Usage.ReadBytes != 64 {
				t.Fatal("fixture did not prime real checked prefix", first, err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			killHashStoreAt(t, base, root, stage)
			// Recovery must work from saved records alone. There is no source
			// pathname to reopen, and no Scanner is supplied to OpenHashWriter.
			if err = os.Rename(root, root+"-offline"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := os.Rename(root+"-offline", root); e != nil {
					t.Error(e)
				}
			})
			recovered, err := OpenHashWriter(ctx, base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			snapshot, err := recovered.Snapshot(ctx)
			if err != nil || len(snapshot.Work) != 1 || snapshot.Budget == nil {
				t.Fatal(snapshot, err)
			}
			work, budget := snapshot.Work[0], snapshot.Budget
			wantOffset, wantSequence, wantReserved, wantRead, wantUnknown := int64(64), int64(2), int64(128), int64(64), int64(64)
			if stage == "before_reserve_commit" {
				wantSequence, wantReserved, wantUnknown = 1, 64, 0
			}
			if stage == "after_settle_commit" {
				wantOffset, wantSequence, wantRead, wantUnknown = 128, 2, 128, 0
			}
			if work.Status != "pending" || work.DurableOffset != wantOffset || work.Sequence != wantSequence || work.SHA256 != "" || budget.ReservedBytes != wantReserved || budget.TotalReservedBytes != wantReserved || budget.RequestedBytes != wantRead || budget.TotalRequestedBytes != wantRead || budget.ReadBytes != wantRead || budget.TotalReadBytes != wantRead || budget.UnknownReservedBytes != wantUnknown || budget.TotalUnknownReservedBytes != wantUnknown {
				t.Fatal("crash published partial settlement or changed conservative charge", snapshot)
			}
			if work.LatestAttempt == nil {
				t.Fatal("latest attempt disappeared", work)
			}
			attempt := work.LatestAttempt
			if wantUnknown != 0 {
				if attempt.Status != "interrupted_unknown" || attempt.ReservedBytes != 64 || attempt.RequestedBytes != nil || attempt.ReadBytes != nil || attempt.ElapsedNS != nil {
					t.Fatal("unknown usage became known zero or reservation was lost", attempt)
				}
			} else if attempt.Status != "settled" || attempt.RequestedBytes == nil || *attempt.RequestedBytes != 64 || attempt.ReadBytes == nil || *attempt.ReadBytes != 64 || attempt.ElapsedNS == nil {
				t.Fatal("known atomic settlement disappeared", attempt)
			}
			if err = recovered.Close(); err != nil {
				t.Fatal(err)
			}
			again, err := OpenHashWriter(ctx, base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = again.Close() })
			repeated, err := again.Snapshot(ctx)
			if err != nil || !reflect.DeepEqual(snapshot, repeated) {
				t.Fatal("repeated recovery settled/charged an attempt twice", repeated, err)
			}
		})
	}
}

func TestHashStoreCanceledBeforeSettlementKeepsPreviousCheckpoint(t *testing.T) {
	scanner, source, store, _, _ := processHashStoreFixture(t)
	first, err := store.RunNext(context.Background(), source, scanner, 64, 128)
	if err != nil || first.DurableOffset != 64 {
		t.Fatal(first, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := store.runNext(ctx, source, scanner, 64, 128, hashStoreHooks{beforeSettleCommit: cancel})
	if !errors.Is(err, context.Canceled) || result.DurableOffset != 64 || result.Progress.Offset != 64 || result.Progress.SHA256 != "" || result.ReservedBytes != 64 || result.Usage.RequestedBytes != 64 || result.Usage.ReadBytes != 64 {
		t.Fatal("cancellation at settlement committed tentative prefix or lost known usage", result, err)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil || len(snapshot.Work) != 1 || snapshot.Work[0].DurableOffset != 64 || snapshot.Work[0].Sequence != 2 || snapshot.Work[0].Status != "pending" || snapshot.Budget == nil || snapshot.Budget.TotalReservedBytes != 128 || snapshot.Budget.TotalReadBytes != 128 || snapshot.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("cancellation did not settle known usage with old checkpoint", snapshot, err)
	}
}

func TestHashStoreCanceledAfterCommitKeepsHistoryWithoutLiveDigest(t *testing.T) {
	scanner, source, store, _, _ := processHashStoreFixture(t)
	first, err := store.RunNext(context.Background(), source, scanner, 512, 1024)
	if err != nil || first.DurableOffset != 512 {
		t.Fatal(first, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := store.runNext(ctx, source, scanner, 64, 1024, hashStoreHooks{afterSettleCommit: cancel})
	if !errors.Is(err, context.Canceled) || result.Status != "canceled" || result.Progress.SHA256 != "" || result.DurableOffset != 513 || result.Progress.Offset != 513 || result.ReservedBytes != 1 || result.Usage.ReadBytes != 1 || result.Usage.RequestedBytes != 1 {
		t.Fatal("canceled response exposed a live digest or hid committed accounting", result, err)
	}
	requireFullHashClaims(t, result.Progress)
	snapshot, err := store.Snapshot(context.Background())
	if err != nil || len(snapshot.Work) != 1 || snapshot.Work[0].Status != "complete" || snapshot.Work[0].DurableOffset != 513 || snapshot.Work[0].SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(513))) || snapshot.Budget == nil || snapshot.Budget.TotalReservedBytes != 513 || snapshot.Budget.TotalReadBytes != 513 {
		t.Fatal("cancellation undid completed history or lost known usage", snapshot, err)
	}
}
