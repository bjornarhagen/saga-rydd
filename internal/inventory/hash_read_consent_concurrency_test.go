package inventory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func holdHashConsentedRead(t *testing.T, f *hashConsentFixture, id string) (chan struct{}, <-chan hashConcurrentResult) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan hashConcurrentResult, 1)
	var once sync.Once
	go func() {
		r, err := f.store.runConsented(context.Background(), id, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterRead: func(int) {
			once.Do(func() {
				close(entered)
				<-release
			})
		}}})
		done <- hashConcurrentResult{r, err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("consented fixture did not reach its held source read")
	}
	return release, done
}

func TestHashReadConsentCanceledRevokeWaiterAndSerializedRevocation(t *testing.T) {
	f := newHashConsentFixture(t)
	approval := f.approve(t, 3*FileHashStepByteLimit, 3*FileHashStepByteLimit)
	release, done := holdHashConsentedRead(t, f, approval.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	revoked, err := f.store.RevokeRead(ctx, approval.ID)
	close(release)
	first := finishHeldHashRead(t, done)
	if !errors.Is(err, context.DeadlineExceeded) || revoked.ID != "" || first.err != nil || first.result.ReservedBytes != FileHashStepByteLimit || first.result.DurableOffset != FileHashStepByteLimit || first.result.Usage.ReadBytes != FileHashStepByteLimit {
		t.Fatal("canceled revocation waiter changed or interrupted the owning read", revoked, err, first)
	}
	before := hashStoreSnapshot(t, f.store)
	if before.ReadConsent == nil || before.ReadConsent.Revocation != nil || before.ReadConsent.Status != "recorded" || before.Budget.TotalReservedBytes != FileHashStepByteLimit || before.Work[0].Sequence != 1 {
		t.Fatal("canceled waiter published revocation or changed accounting", before)
	}
	revoked, err = f.store.RevokeRead(context.Background(), approval.ID)
	if err != nil || revoked.ID != approval.ID || revoked.Status != "revoked" || revoked.Revocation == nil {
		t.Fatal("revocation did not publish after the owning slice released its gate", revoked, err)
	}
	opened := false
	r, err := f.store.runConsented(context.Background(), approval.ID, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
	if !errors.Is(err, ErrHashReadRevoked) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" || opened {
		t.Fatal("successful revoke permitted another suffix request", r, err, opened)
	}
	after := hashStoreSnapshot(t, f.store)
	if after.Work[0].Sequence != before.Work[0].Sequence || after.Work[0].DurableOffset != before.Work[0].DurableOffset || after.Budget.TotalReservedBytes != before.Budget.TotalReservedBytes || after.Budget.TotalReadBytes != before.Budget.TotalReadBytes {
		t.Fatal("revoked invocation changed the checkpoint or reservation charge", after)
	}
}

func TestHashReadConsentRemainingLifetimeCancelsSliceAndSettlesUsage(t *testing.T) {
	f := newHashConsentFixture(t)
	approval := f.approve(t, 3*FileHashStepByteLimit, 3*FileHashStepByteLimit)
	first, err := f.store.RunConsented(context.Background(), approval.ID, f.source, f.scanner)
	if err != nil || first.DurableOffset != FileHashStepByteLimit || first.Usage.ReadBytes != FileHashStepByteLimit {
		t.Fatal("fixture did not prime a checked prefix", first, err)
	}
	// This fake wall clock advances with real elapsed time. The consent expires
	// well before the ordinary five-second slice bound. A frozen fake clock or
	// an absolute OS deadline based on this historical timestamp cannot satisfy
	// the test by accident.
	started := time.Now()
	nearExpiry := approval.Approval.ExpiresAt.Add(-3 * time.Second)
	f.store.now = func() time.Time { return nearExpiry.Add(time.Since(started)) }
	release, done := holdHashConsentedRead(t, f, approval.ID)
	timer := time.NewTimer(3200 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	close(release)
	last := finishHeldHashRead(t, done)
	if !errors.Is(last.err, ErrHashReadExpired) || last.result.Progress.SHA256 != "" || last.result.DurableOffset != FileHashStepByteLimit || last.result.ReservedBytes != 65 || last.result.Usage.RequestedBytes != 65 || last.result.Usage.ReadBytes != 65 {
		t.Fatal("remaining lifetime was ignored or tentative completion/usage was published incorrectly", last)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	if snapshot.Work[0].Status != "pending" || snapshot.Work[0].DurableOffset != FileHashStepByteLimit || snapshot.Work[0].SHA256 != "" || snapshot.Work[0].Sequence != 2 || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.Status != "settled" || snapshot.Work[0].LatestAttempt.ReadBytes == nil || *snapshot.Work[0].LatestAttempt.ReadBytes != 65 || snapshot.Budget.TotalReservedBytes != FileHashStepByteLimit+65 || snapshot.Budget.TotalReadBytes != FileHashStepByteLimit+65 || snapshot.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("expired held read lost known usage, refunded its charge or committed tentative SHA state", snapshot)
	}
	opened := false
	r, err := f.store.runConsented(context.Background(), approval.ID, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
	if !errors.Is(err, ErrHashReadExpired) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || opened {
		t.Fatal("expired consent issued another reservation/read", r, err, opened)
	}
	expired := hashStoreSnapshot(t, f.store)
	if expired.ReadConsent == nil || !expired.ReadConsent.ExpiredObserved || expired.ReadConsent.Status != "expired_observed" || expired.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("writer did not persist terminal expiry as saved, unevaluated evidence", expired.ReadConsent)
	}
	// An earlier clock after restart cannot reactivate terminal expiry.
	f.reopen(t)
	opened = false
	r, err = f.store.runConsented(context.Background(), approval.ID, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
	if !errors.Is(err, ErrHashReadExpired) || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || opened {
		t.Fatal("clock rollback after restart reactivated expired consent", r, err, opened)
	}
}

func TestHashReadConsentCloseCancelsSliceBeforeReleasingWriterLock(t *testing.T) {
	f := newHashConsentFixture(t)
	approval := f.approve(t, 3*FileHashStepByteLimit, 3*FileHashStepByteLimit)
	release, done := holdHashConsentedRead(t, f, approval.ID)
	closed := make(chan error, 1)
	go func() { closed <- f.store.Close() }()
	select {
	case <-f.store.life.Done():
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("close did not cancel the owned consented request")
	}
	other, err := OpenHashWriter(context.Background(), f.base)
	if other != nil {
		_ = other.Close()
	}
	close(release)
	first := finishHeldHashRead(t, done)
	// life cancellation is synchronous; its AfterFunc propagation to the
	// request context is asynchronous. The first buffer was already read at
	// this seam, and any further reads must stay within the charged grant.
	usage := first.result.Usage
	if !errors.Is(err, localfs.ErrLocked) || !errors.Is(first.err, context.Canceled) || first.result.Progress.SHA256 != "" || first.result.DurableOffset != 0 || usage.ReadBytes < 32*1024 || usage.RequestedBytes < usage.ReadBytes || usage.RequestedBytes > FileHashStepByteLimit || first.result.ReservedBytes != FileHashStepByteLimit {
		t.Fatal("close released writer ownership before accounting or lost canceled read usage", first, err)
	}
	select {
	case err = <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not finish after consented settlement")
	}
	f.reopen(t)
	snapshot := hashStoreSnapshot(t, f.store)
	if snapshot.ReadConsent == nil || snapshot.ReadConsent.ID != approval.ID || snapshot.ReadConsent.Revocation != nil || snapshot.Work[0].DurableOffset != 0 || snapshot.Work[0].Sequence != 1 || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.Status != "settled" || snapshot.Work[0].LatestAttempt.RequestedBytes == nil || *snapshot.Work[0].LatestAttempt.RequestedBytes != usage.RequestedBytes || snapshot.Work[0].LatestAttempt.ReadBytes == nil || *snapshot.Work[0].LatestAttempt.ReadBytes != usage.ReadBytes || snapshot.Budget.TotalReservedBytes != FileHashStepByteLimit || snapshot.Budget.TotalRequestedBytes != usage.RequestedBytes || snapshot.Budget.TotalReadBytes != usage.ReadBytes || snapshot.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("reopen lost the canceled slice's known accounting or changed consent", snapshot)
	}
}

func TestHashReadConsentPostReserveRollbackUsesFreshHighWater(t *testing.T) {
	f := newHashConsentFixture(t)
	approval := f.approve(t, 3*FileHashStepByteLimit, 3*FileHashStepByteLimit)
	now := hashConsentProcessClock()
	f.store.now = func() time.Time { return now }
	opened := false
	r, err := f.store.runConsented(context.Background(), approval.ID, f.source, f.scanner, hashStoreHooks{
		beforeReserveCommit: func() { now = hashConsentProcessClock().Add(time.Hour) },
		afterReserve:        func() { now = hashConsentProcessClock().Add(30 * time.Minute) },
		file:                fileHashHooks{afterOpen: func() { opened = true }},
	})
	if !errors.Is(err, ErrHashReadClockRollback) || r.ReservedBytes != FileHashStepByteLimit || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.DurableOffset != 0 || r.Progress.SHA256 != "" || opened {
		t.Fatal("clock earlier than the just-committed high-water opened source bytes", r, err, opened)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	if snapshot.ReadConsent == nil || !snapshot.ReadConsent.ClockHighWater.Equal(hashConsentProcessClock().Add(time.Hour)) || snapshot.Budget.TotalReservedBytes != FileHashStepByteLimit || snapshot.Budget.TotalReadBytes != 0 || snapshot.Work[0].Sequence != 1 || snapshot.Work[0].DurableOffset != 0 || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.ReadBytes == nil || *snapshot.Work[0].LatestAttempt.ReadBytes != 0 {
		t.Fatal("clock refusal lost its permanent charge or checked high-water", snapshot)
	}
}
