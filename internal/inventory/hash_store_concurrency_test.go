package inventory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

type hashConcurrentResult struct {
	result HashRunResult
	err    error
}

func startHeldHashRead(t *testing.T, f *hashStoreTestFixture) (chan struct{}, <-chan hashConcurrentResult) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan hashConcurrentResult, 1)
	go func() {
		r, err := f.store.runNext(context.Background(), f.source, f.scanner, 64, 4096, hashStoreHooks{file: fileHashHooks{afterRead: func(int) {
			close(entered)
			<-release
		}}})
		done <- hashConcurrentResult{r, err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("fixture did not reach its held source read")
	}
	return release, done
}

func finishHeldHashRead(t *testing.T, done <-chan hashConcurrentResult) hashConcurrentResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("held hash read did not settle after release")
		return hashConcurrentResult{}
	}
}

func TestHashStoreCanceledGateWaiterDoesNotReserve(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	release, done := startHeldHashRead(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := f.store.RunNext(ctx, f.source, f.scanner, 64, 4096)
	close(release)
	first := finishHeldHashRead(t, done)
	if !errors.Is(err, context.DeadlineExceeded) || result.ReservedBytes != 0 || result.Usage.RequestedBytes != 0 || result.Progress.SHA256 != "" {
		t.Fatal("canceled gate waiter issued a reservation or source request", result, err)
	}
	if first.err != nil || first.result.DurableOffset != 64 {
		t.Fatal(first)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	if snapshot.Budget.TotalReservedBytes != 64 || snapshot.Budget.TotalReadBytes != 64 || snapshot.Work[0].Sequence != 1 {
		t.Fatal("gate waiter changed the owning attempt", snapshot)
	}
}

func TestHashStoreCloseCancelsReadAndKeepsWriterLockUntilSettlement(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	release, done := startHeldHashRead(t, f)
	closed := make(chan error, 1)
	go func() { closed <- f.store.Close() }()
	select {
	case <-f.store.life.Done():
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("close did not cancel its owned request")
	}
	other, err := OpenHashWriter(context.Background(), f.base)
	if other != nil {
		_ = other.Close()
	}
	close(release)
	first := finishHeldHashRead(t, done)
	if !errors.Is(err, localfs.ErrLocked) || !errors.Is(first.err, context.Canceled) || first.result.Progress.SHA256 != "" || first.result.DurableOffset != 0 || first.result.Usage.ReadBytes != 64 {
		t.Fatal("close released ownership before settlement or lost usage", first, err)
	}
	select {
	case err = <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not finish after settlement")
	}
	f.reopen(t)
	snapshot := hashStoreSnapshot(t, f.store)
	if snapshot.Work[0].DurableOffset != 0 || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.Status != "settled" || snapshot.Budget.TotalReservedBytes != 64 || snapshot.Budget.TotalReadBytes != 64 || snapshot.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("close did not preserve the old checkpoint and known accounting", snapshot)
	}
}
