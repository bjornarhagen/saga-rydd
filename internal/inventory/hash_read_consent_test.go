package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func hashReadFixture(t *testing.T, contents ...[]byte) (*hashStoreTestFixture, HashReadApprovalRequest) {
	t.Helper()
	f := hashStoreFixture(t, contents...)
	s := hashProposalFixtureWriter(t)
	p, err := s.CreateManualSelection(context.Background(), f.source, f.inventoryID, f.expected, []byte(f.root))
	if err != nil {
		t.Fatal(err)
	}
	clock := f.store.now
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	f.base = s.base
	f.store, err = OpenHashWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.store.now = clock
	req := HashReadApprovalRequest{StoreID: p.StoreID, SelectionID: p.SelectionID, InventoryID: p.InventoryID, SourceLocator: cloneHashReadLocator(*p.SourceLocator), DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192, ConfirmFullFileRead: true}
	return f, req
}

func hashReadApprove(t *testing.T, f *hashStoreTestFixture, req HashReadApprovalRequest) HashReadConsent {
	t.Helper()
	c, err := f.store.ApproveRead(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	requireHashReadConsentClaims(t, c)
	return c
}
func requireHashReadConsentClaims(t *testing.T, c HashReadConsent) {
	t.Helper()
	if c.CurrentReadPermissionEvaluated || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.Executable || c.EstimatedReclaimableBytes != nil {
		t.Fatal("saved consent claimed current permission or cleanup authority", c)
	}
	if c.Status != "recorded" && c.Status != "revoked" && c.Status != "expired_observed" {
		t.Fatal("invalid saved consent status", c)
	}
}

func TestHashReadApprovalExactImmutableAndSavedOnly(t *testing.T) {
	f, req := hashReadFixture(t, []byte("fixture"))
	ctx := context.Background()
	before := hashStoreSnapshot(t, f.store)
	c := hashReadApprove(t, f, req)
	if c.ID == "" || c.Status != "recorded" || c.Approval.CreatedAt != f.store.now() || c.Approval.ExpiresAt != f.store.now().Add(24*time.Hour) || c.Approval.StepByteLimit != FileHashStepByteLimit || c.Approval.Contract != HashReadApprovalContract || c.Approval.InitialTotalReservedBytes != 0 {
		t.Fatal("approval bounds/binding incorrect", c)
	}
	after := hashStoreSnapshot(t, f.store)
	if after.ReadConsent == nil || !reflect.DeepEqual(*after.ReadConsent, c) || !reflect.DeepEqual(before.Work, after.Work) || after.Budget != nil {
		t.Fatal("approval changed work or lost discoverable ID", after)
	}
	p, err := f.store.Proposal(ctx, req.SelectionID)
	if err != nil || p.ReadConsent == nil || !reflect.DeepEqual(*p.ReadConsent, c) {
		t.Fatal("proposal cannot discover consent", p, err)
	}
	f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(time.Hour) }
	again, err := f.store.ApproveRead(ctx, req)
	if err != nil || again.ID != c.ID || !reflect.DeepEqual(again.Approval, c.Approval) || !again.ClockHighWater.Equal(c.Approval.CreatedAt.Add(time.Hour)) {
		t.Fatal("retry renewed or changed immutable approval", again, err)
	}
	for _, limits := range [][2]int64{{8192, 16384}, {4096, 4096}, {2048, 8192}} {
		changed := req
		changed.DailyReservedByteLimit, changed.LifetimeReservedByteLimit = limits[0], limits[1]
		if _, err = f.store.ApproveRead(ctx, changed); !errors.Is(err, ErrHashReadApprovalConflict) {
			t.Fatal("immutable caps changed", err)
		}
	}
	// Saved readers do not derive active permission or latch wall time.
	f.store.now = func() time.Time { return c.Approval.ExpiresAt.Add(time.Hour) }
	shown, err := f.store.Approval(ctx, c.ID)
	if err != nil || shown.Status != "recorded" || shown.ExpiredObserved || !shown.ClockHighWater.Equal(again.ClockHighWater) {
		t.Fatal("saved show evaluated wall time", shown, err)
	}
	shown.Approval.SourceLocator.RootPathBytes[0] = 'x'
	shown, err = f.store.Approval(ctx, c.ID)
	if err != nil || !bytes.Equal(shown.Approval.SourceLocator.RootPathBytes, []byte(f.root)) {
		t.Fatal("saved consent aliases caller bytes", shown, err)
	}
	if _, err = f.store.Approval(ctx, c.ID[:12]); !errors.Is(err, ErrHashReadApprovalMissing) {
		t.Fatal("partial approval ID accepted", err)
	}
	if _, err = f.store.Approval(ctx, strings.Repeat("0", 64)); !errors.Is(err, ErrHashReadApprovalMissing) {
		t.Fatal("different approval accepted", err)
	}
}

func TestHashReadApprovalInputAndLegacyRefusals(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(129))
	for _, tc := range []struct {
		name   string
		change func(*HashReadApprovalRequest)
		want   error
	}{
		{"unconfirmed", func(r *HashReadApprovalRequest) { r.ConfirmFullFileRead = false }, ErrHashReadConfirmation},
		{"zero_day", func(r *HashReadApprovalRequest) { r.DailyReservedByteLimit = 0 }, ErrHashReadLimits},
		{"zero_total", func(r *HashReadApprovalRequest) { r.LifetimeReservedByteLimit = 0 }, ErrHashReadLimits},
		{"large_day", func(r *HashReadApprovalRequest) { r.DailyReservedByteLimit = 1<<50 + 1 }, ErrHashReadLimits},
		{"large_total", func(r *HashReadApprovalRequest) { r.LifetimeReservedByteLimit = math.MaxInt64 }, ErrHashReadLimits},
		{"store", func(r *HashReadApprovalRequest) { r.StoreID = strings.Repeat("0", 64) }, ErrHashReadBinding},
		{"selection", func(r *HashReadApprovalRequest) { r.SelectionID = strings.Repeat("0", 64) }, ErrHashReadBinding},
		{"inventory", func(r *HashReadApprovalRequest) { r.InventoryID = strings.Repeat("0", 64) }, ErrHashReadBinding},
		{"locator_kind", func(r *HashReadApprovalRequest) { r.SourceLocator.Kind = "configured_inventory_v1" }, ErrHashReadBinding},
		{"locator_key", func(r *HashReadApprovalRequest) { r.SourceLocator.InventoryKey = strings.Repeat("0", 64) }, ErrHashReadBinding},
		{"locator_root", func(r *HashReadApprovalRequest) {
			r.SourceLocator.RootPathBytes = []byte(f.root + "-other")
			r.SourceLocator.InventoryKey = hashManualInventoryKey(r.SourceLocator.RootPathBytes)
		}, ErrHashReadBinding},
		{"oversized_root", func(r *HashReadApprovalRequest) {
			r.SourceLocator.RootPathBytes = []byte("/" + strings.Repeat("x", 4096))
		}, ErrHashReadBinding},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := req
			r.SourceLocator = cloneHashReadLocator(req.SourceLocator)
			tc.change(&r)
			if _, err := f.store.ApproveRead(context.Background(), r); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			if hashStoreSnapshot(t, f.store).ReadConsent != nil {
				t.Fatal("refused approval published")
			}
		})
	}
	opened := false
	r, err := f.store.runConsented(context.Background(), strings.Repeat("0", 64), f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
	if !errors.Is(err, ErrHashReadApprovalMissing) || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || hashStoreSnapshot(t, f.store).Budget != nil {
		t.Fatal("unapproved selection read or charged", r, err)
	}
	legacy := hashStoreFixture(t, []byte("legacy fixture"))
	old := hashStoreSnapshot(t, legacy.store)
	legacyReq := req
	legacyReq.StoreID, legacyReq.SelectionID, legacyReq.InventoryID = old.StoreID, old.SelectionID, old.InventoryID
	legacyReq.SourceLocator = HashSourceLocator{Kind: "manual_inventory_v1", RootPathBytes: []byte(legacy.root), InventoryKey: hashManualInventoryKey([]byte(legacy.root))}
	if _, err = legacy.store.ApproveRead(context.Background(), legacyReq); !errors.Is(err, ErrHashReadBinding) {
		t.Fatal("legacy nil-locator selection approved", err)
	}
}

func TestHashReadConsentedDayLifetimeChargesAndFairReopen(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(193), []byte("small"))
	// A fixture-only prior step is still part of the lifetime/day charge.
	hashStoreRun(t, f, 64, 4096)
	tooSmall := req
	tooSmall.LifetimeReservedByteLimit = 63
	if _, err := f.store.ApproveRead(context.Background(), tooSmall); !errors.Is(err, ErrHashReadLimits) {
		t.Fatal("prior lifetime charge was omitted from approval", err)
	}
	req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = 128, 133
	c := hashReadApprove(t, f, req)
	if c.Approval.InitialTotalReservedBytes != 64 {
		t.Fatal("prior allowance was reset", c)
	}
	r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
	if err != nil || r.ApprovalID != c.ID || r.WorkID != "2" || r.ReservedBytes != 5 || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("small"))) {
		t.Fatal("consented round robin or tail changed", r, err)
	}
	f.reopen(t)
	if r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashDeferred) || r.Code != "durable_quantum" || r.ReservedBytes != 0 {
		t.Fatal("sub-block remaining day charge read", r, err)
	}
	f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(13 * time.Hour) }
	r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
	if err != nil || r.WorkID != "1" || r.ReservedBytes != 64 || r.DurableOffset != 128 || r.Budget.TotalReservedBytes != 133 || r.Budget.ReservedBytes != 64 {
		t.Fatal("midnight removed prior lifetime consumption", r, err)
	}
	f.reopen(t)
	if r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashDeferred) || r.Code != "lifetime_byte_limit" || r.ReservedBytes != 0 {
		t.Fatal("lifetime exhausted charge was bypassed", r, err)
	}
	if _, err = f.store.ApproveRead(context.Background(), req); err != nil {
		t.Fatal("exact recorded retry failed", err)
	}
}

func TestHashReadConsentedSelectionCannotBypassLimitsViaRunNext(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(129))
	req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = 64, 64
	c := hashReadApprove(t, f, req)
	before := hashStoreSnapshot(t, f.store)
	opened := false
	r, err := f.store.runNext(context.Background(), f.source, f.scanner, FileHashStepByteLimit, 1<<50, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
	if !errors.Is(err, ErrHashReadBinding) || opened || r.ReservedBytes != 0 || r.Code != "read_consent_required" || !reflect.DeepEqual(before, hashStoreSnapshot(t, f.store)) {
		t.Fatal("unguarded fixture API bypassed recorded owner limits", r, err)
	}
	r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
	if err != nil || r.ReservedBytes != 64 {
		t.Fatal("guarded dispatch failed", r, err)
	}
	before = hashStoreSnapshot(t, f.store)
	r, err = f.store.RunNext(context.Background(), f.source, f.scanner, FileHashStepByteLimit, 1<<50)
	if !errors.Is(err, ErrHashReadBinding) || r.ReservedBytes != 0 || !reflect.DeepEqual(before, hashStoreSnapshot(t, f.store)) {
		t.Fatal("exhausted consent was bypassed", r, err)
	}
}

func TestHashReadConsentedFixedStepQuantumAndTail(t *testing.T) {
	t.Run("fixed_step", func(t *testing.T) {
		data := fullHashContents(int(FileHashStepByteLimit) + 65)
		f, req := hashReadFixture(t, data)
		req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = 4<<20, 8<<20
		c := hashReadApprove(t, f, req)
		r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
		if err != nil || r.ReservedBytes != FileHashStepByteLimit || r.DurableOffset != FileHashStepByteLimit || r.Progress.SHA256 != "" {
			t.Fatal("step ceiling changed", r, err)
		}
		f.reopen(t)
		r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
		if err != nil || r.ReservedBytes != 65 || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || r.Budget.TotalReservedBytes != int64(len(data)) {
			t.Fatal("restored tail restarted or returned wrong digest", r, err)
		}
	})
	for _, cap := range []int64{1, 63, 64, 65} {
		t.Run(fmt.Sprint(cap), func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129), []byte("x"))
			req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = cap, 256
			c := hashReadApprove(t, f, req)
			r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
			if cap < 64 {
				if err != nil || r.WorkID != "2" || r.ReservedBytes != 1 {
					t.Fatal("unaffordable head blocked eligible tail", r, err)
				}
			} else if err != nil || r.WorkID != "1" || r.ReservedBytes != 64 {
				t.Fatal("64-byte quantum changed", r, err)
			}
			f.reopen(t)
		})
	}
}

func TestHashReadConsentedExpiryAndClockAreDurable(t *testing.T) {
	for _, method := range []string{"dispatch", "approve_retry"} {
		t.Run(method, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129))
			c := hashReadApprove(t, f, req)
			f.store.now = func() time.Time { return c.Approval.ExpiresAt }
			if method == "dispatch" {
				r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner)
				if !errors.Is(err, ErrHashReadExpired) || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 {
					t.Fatal(r, err)
				}
			} else {
				if _, err := f.store.ApproveRead(context.Background(), req); !errors.Is(err, ErrHashReadExpired) {
					t.Fatal(err)
				}
			}
			f.reopen(t)
			f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(time.Hour) }
			shown, err := f.store.Approval(context.Background(), c.ID)
			if err != nil || shown.Status != "expired_observed" || !shown.ExpiredObserved || shown.ClockHighWater.Before(c.Approval.ExpiresAt) {
				t.Fatal("observed expiry did not survive rollback", shown, err)
			}
			if r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashReadExpired) || r.ReservedBytes != 0 {
				t.Fatal("expired consent reactivated", r, err)
			}
			if hashStoreSnapshot(t, f.store).Budget != nil {
				t.Fatal("expiry refusal charged bytes")
			}
		})
	}
	t.Run("clock_highwater_without_reads", func(t *testing.T) {
		f, req := hashReadFixture(t, fullHashContents(129))
		req.DailyReservedByteLimit = 1
		c := hashReadApprove(t, f, req)
		f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(2 * time.Hour) }
		if _, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashDeferred) {
			t.Fatal(err)
		}
		f.reopen(t)
		f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(time.Hour) }
		if r, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashReadClockRollback) || r.ReservedBytes != 0 {
			t.Fatal("unissued work lost clock highwater", r, err)
		}
		if hashStoreSnapshot(t, f.store).Budget != nil {
			t.Fatal("clock observation invented reservation")
		}
	})
}

func TestHashReadConsentedFreshReservationGate(t *testing.T) {
	for _, jump := range []string{"expired", "backward", "midnight"} {
		t.Run(jump, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129))
			c := hashReadApprove(t, f, req)
			opened := false
			r, err := f.store.runConsented(context.Background(), c.ID, f.source, f.scanner, hashStoreHooks{beforeReserveCommit: func() {
				switch jump {
				case "expired":
					f.store.now = func() time.Time { return c.Approval.ExpiresAt }
				case "backward":
					f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(-time.Second) }
				case "midnight":
					f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(13 * time.Hour) }
				}
			}, file: fileHashHooks{afterOpen: func() { opened = true }}})
			if jump == "midnight" {
				if err != nil || !opened || r.Budget.Day != c.Approval.CreatedAt.Add(13*time.Hour).Format(time.DateOnly) {
					t.Fatal("reservation used stale pre-midnight day", r, err)
				}
				return
			}
			want := ErrHashReadExpired
			if jump == "backward" {
				want = ErrHashReadClockRollback
			}
			if !errors.Is(err, want) || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 {
				t.Fatal("fresh transaction gate did not precede reads/charge", r, err)
			}
			snap := hashStoreSnapshot(t, f.store)
			if snap.Budget != nil || snap.Work[0].Status != "pending" || snap.Work[0].Sequence != 0 || snap.Work[0].LatestAttempt != nil {
				t.Fatal("gate refusal rotated or reserved work", snap)
			}
		})
	}
}

func TestHashReadConsentedExpiryDuringReadRetainsChargeAndOldPrefix(t *testing.T) {
	for _, phase := range []string{"clock_after_read", "clock_before_settle"} {
		t.Run(phase, func(t *testing.T) {
			f, req := hashReadFixture(t, []byte("abc"))
			c := hashReadApprove(t, f, req)
			hooks := hashStoreHooks{}
			if phase == "clock_after_read" {
				hooks.file.afterRead = func(n int) {
					if n != 3 {
						t.Fatal("expiry hook did not follow the complete fixture read", n)
					}
					f.store.now = func() time.Time { return c.Approval.ExpiresAt }
				}
			} else {
				hooks.beforeSettleCommit = func() { f.store.now = func() time.Time { return c.Approval.ExpiresAt } }
			}
			r, err := f.store.runConsented(context.Background(), c.ID, f.source, f.scanner, hooks)
			if !errors.Is(err, ErrHashReadExpired) || r.Progress.SHA256 != "" || r.DurableOffset != 0 || r.Usage.RequestedBytes != 3 || r.Usage.ReadBytes != 3 || r.ReservedBytes != 3 {
				t.Fatal("expired completion published a digest or refunded read", r, err)
			}
			snap := hashStoreSnapshot(t, f.store)
			if snap.Work[0].Status != "pending" || snap.Work[0].DurableOffset != 0 || snap.Work[0].SHA256 != "" || snap.Budget.TotalReservedBytes != 3 || snap.Budget.TotalReadBytes != 3 || snap.ReadConsent.Status != "expired_observed" {
				t.Fatal("expiry settlement lost usage/prefix or lifecycle", snap)
			}
			f.reopen(t)
			f.store.now = func() time.Time { return c.Approval.CreatedAt }
			if _, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashReadExpired) {
				t.Fatal("observed expiry reactivated after reopen", err)
			}
		})
	}
}

func TestHashReadConsentedCancellationAndUnknownRecoveryLifetime(t *testing.T) {
	for _, mode := range []string{"cancel", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129))
			req.DailyReservedByteLimit, req.LifetimeReservedByteLimit = 64, 64
			c := hashReadApprove(t, f, req)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashStoreHooks{}
			if mode == "cancel" {
				hooks.file.afterRead = func(int) { cancel() }
			} else {
				if _, err := f.store.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
					t.Fatal(err)
				}
			}
			r, err := f.store.runConsented(ctx, c.ID, f.source, f.scanner, hooks)
			want := context.Canceled
			if mode == "unknown" {
				want = ErrHashRecoveryRequired
				if _, e := f.store.db.Exec("DROP TRIGGER fail_checkpoint"); e != nil {
					t.Fatal(e)
				}
			}
			if !errors.Is(err, want) || r.ReservedBytes != 64 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
				t.Fatal(r, err)
			}
			f.reopen(t)
			snap := hashStoreSnapshot(t, f.store)
			if snap.Budget.TotalReservedBytes != 64 || snap.Work[0].DurableOffset != 0 {
				t.Fatal("cancellation/crash charge refunded", snap)
			}
			if mode == "unknown" && (snap.Budget.TotalUnknownReservedBytes != 64 || snap.Work[0].LatestAttempt.ReadBytes != nil) {
				t.Fatal("unknown consumption fabricated", snap)
			}
			f.store.now = func() time.Time { return c.Approval.CreatedAt.Add(13 * time.Hour) }
			if r, err = f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); !errors.Is(err, ErrHashDeferred) || r.Code != "lifetime_byte_limit" || r.ReservedBytes != 0 {
				t.Fatal("lifetime cap lost after day/reopen", r, err)
			}
		})
	}
}

func TestHashReadRevokeOfflineImmutableAndSelectionWriter(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(129))
	c := hashReadApprove(t, f, req)
	before := hashStoreSnapshot(t, f.store)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenHashSelectionWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.now = func() time.Time { return c.Approval.CreatedAt.Add(-time.Hour) }
	if _, err = s.RunConsented(context.Background(), c.ID, f.source, f.scanner); err == nil {
		t.Fatal("selection-only handle dispatched consented source reads")
	}
	f.scanner.Close()
	if err = f.source.Close(); err != nil {
		t.Fatal(err)
	}
	offline := filepath.Join(filepath.Dir(f.root), filepath.Base(f.root)+"-offline")
	if err = os.Rename(f.root, offline); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(offline, f.root)
	revoked, err := s.RevokeRead(context.Background(), c.ID)
	if err != nil || revoked.Status != "revoked" || revoked.Revocation == nil || revoked.Revocation.ID == "" || revoked.Revocation.ApprovalID != c.ID || revoked.Revocation.RecordedAt.Before(c.Approval.CreatedAt) {
		t.Fatal("offline rollback-time revoke failed", revoked, err)
	}
	requireHashReadConsentClaims(t, revoked)
	again, err := s.RevokeRead(context.Background(), c.ID)
	if err != nil || !reflect.DeepEqual(again, revoked) {
		t.Fatal("revocation retry replaced record", again, err)
	}
	if _, err = s.ApproveRead(context.Background(), req); !errors.Is(err, ErrHashReadRevoked) {
		t.Fatal("revoked consent renewed", err)
	}
	after := hashStoreSnapshot(t, s)
	if !reflect.DeepEqual(before.Work, after.Work) || after.Budget != nil {
		t.Fatal("offline revoke touched hashing work", after)
	}
	for _, table := range []string{"hash_read_approval", "hash_read_revocation"} {
		if _, err = s.db.Exec("DELETE FROM " + table); err == nil {
			t.Fatal("immutable consent deletion allowed", table)
		}
		if _, err = s.db.Exec("UPDATE " + table + " SET payload=payload"); err == nil {
			t.Fatal("immutable consent update allowed", table)
		}
	}
	if _, err = s.db.Exec("UPDATE hash_read_observation SET max_now_ns=max_now_ns-1"); err == nil {
		t.Fatal("clock highwater reversed")
	}
}

func TestHashReadSchemaMigrationReaderCompatibility(t *testing.T) {
	for _, mode := range []string{"reader", "selection_writer", "normal_writer", "corrupt_old"} {
		t.Run(mode, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			before := hashStoreSnapshot(t, f.store)
			if _, err := f.store.db.Exec("DROP TABLE hash_read_observation; DROP TABLE hash_read_revocation; DROP TABLE hash_read_approval; PRAGMA user_version=1"); err != nil {
				t.Fatal(err)
			}
			if mode == "corrupt_old" {
				if _, err := f.store.db.Exec("UPDATE hash_work SET checkpoint=x'00'"); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			var reopened *HashStore
			var err error
			switch mode {
			case "reader":
				reopened, err = OpenHashReader(context.Background(), f.base)
			case "selection_writer":
				reopened, err = OpenHashSelectionWriter(context.Background(), f.base)
			default:
				reopened, err = OpenHashWriter(context.Background(), f.base)
			}
			if mode == "corrupt_old" {
				if !errors.Is(err, ErrHashStoreCorrupt) {
					t.Fatal("corrupt old store migrated", err)
				}
				db, e := sql.Open("sqlite", filepath.Join(f.base, "hashes", hashStoreFilename))
				if e != nil {
					t.Fatal(e)
				}
				defer db.Close()
				var version int
				if e = db.QueryRow("PRAGMA user_version").Scan(&version); e != nil || version != 1 {
					t.Fatal("failed validation published migration", version, e)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			var version int
			if err = reopened.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			want := 2
			if mode == "reader" {
				want = 1
			}
			if version != want || !reflect.DeepEqual(before, hashStoreSnapshot(t, reopened)) {
				t.Fatal("migration/reader changed selected work", version, hashStoreSnapshot(t, reopened))
			}
			if _, err = reopened.Approval(context.Background(), strings.Repeat("0", 64)); !errors.Is(err, ErrHashReadApprovalMissing) {
				t.Fatal(err)
			}
		})
	}
}

func TestHashReadSchemaMigrationSelectionWriterPreservesReservedAttempt(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	if _, err := f.store.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	if r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 128); !errors.Is(err, ErrHashRecoveryRequired) || r.ReservedBytes != 64 {
		t.Fatal("fixture did not leave reserved work", r, err)
	}
	before := hashStoreSnapshot(t, f.store)
	if _, err := f.store.db.Exec("DROP TRIGGER fail_checkpoint; DROP TABLE hash_read_observation; DROP TABLE hash_read_revocation; DROP TABLE hash_read_approval; PRAGMA user_version=1"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenHashSelectionWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version int
	if err = s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 2 {
		t.Fatal(version, err)
	}
	after := hashStoreSnapshot(t, s)
	if !reflect.DeepEqual(before, after) || after.Work[0].Status != "running" || after.Work[0].LatestAttempt.Status != "reserved" || after.Budget.TotalReservedBytes != 64 || after.Work[0].Sequence != 0 {
		t.Fatal("metadata migration recovered or refunded existing attempt", after)
	}
}

func TestHashReadSchemaOldReaderSeesConsentAfterWriterMigration(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(129))
	if _, err := f.store.db.Exec("DROP TABLE hash_read_observation; DROP TABLE hash_read_revocation; DROP TABLE hash_read_approval; PRAGMA user_version=1"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.schemaVersion != 1 || hashStoreSnapshot(t, reader).ReadConsent != nil {
		t.Fatal("fixture did not open a legacy reader")
	}
	writer, err := OpenHashSelectionWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writer.now = f.store.now
	consent, err := writer.ApproveRead(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// The same reader object keeps its original cache but must use the version
	// visible in each fresh read transaction for all saved reporting APIs.
	if reader.schemaVersion != 1 {
		t.Fatal("writer unexpectedly changed reader object")
	}
	snap := hashStoreSnapshot(t, reader)
	if snap.ReadConsent == nil || !reflect.DeepEqual(*snap.ReadConsent, consent) {
		t.Fatal("old reader hid newly recorded consent", snap)
	}
	proposal, err := reader.Proposal(context.Background(), req.SelectionID)
	if err != nil || proposal.ReadConsent == nil || !reflect.DeepEqual(*proposal.ReadConsent, consent) {
		t.Fatal("old proposal reader labeled approved selection unapproved", proposal, err)
	}
	shown, err := reader.Approval(context.Background(), consent.ID)
	if err != nil || !reflect.DeepEqual(shown, consent) {
		t.Fatal("old reader could not show new approval", shown, err)
	}
}

type hashReadCancelQuery struct {
	hashQuery
	match    string
	deadline bool
	cancel   context.CancelFunc
	fired    bool
}

func (q *hashReadCancelQuery) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if !q.fired && strings.Contains(query, q.match) {
		q.fired = true
		if q.deadline {
			// An already expired child query deadline gives deterministic SQL
			// cancellation without a timing-dependent sleep or corrupt record.
			queryCtx, cancel := context.WithDeadline(ctx, time.Unix(1, 0))
			defer cancel()
			return q.hashQuery.QueryRowContext(queryCtx, query, args...)
		}
		q.cancel()
	}
	return q.hashQuery.QueryRowContext(ctx, query, args...)
}

func TestHashReadConsentQueriesPreserveCancellation(t *testing.T) {
	f, req := hashReadFixture(t, fullHashContents(129))
	hashReadApprove(t, f, req)
	before := hashStoreSnapshot(t, f.store)
	for _, site := range []struct{ name, match string }{
		{"schema", "PRAGMA user_version"},
		{"count", "SELECT (SELECT count(*) FROM (SELECT id FROM hash_read_approval"},
		{"approval", "FROM hash_read_approval WHERE id=1"},
		{"observation", "FROM hash_read_observation WHERE id=1"},
		{"revocation", "FROM hash_read_revocation WHERE id=1"},
	} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_deadline_%t", site.name, deadline), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tx, err := f.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				query := &hashReadCancelQuery{hashQuery: tx, match: site.match, deadline: deadline, cancel: cancel}
				_, _, err = f.store.readHashSnapshot(ctx, query)
				_ = tx.Rollback()
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				if !query.fired || !errors.Is(err, want) || errors.Is(err, ErrHashStoreCorrupt) {
					t.Fatal("query cancellation was labeled invalid saved state", query.fired, err)
				}
				if !reflect.DeepEqual(before, hashStoreSnapshot(t, f.store)) {
					t.Fatal("canceled saved read changed records")
				}
			})
		}
	}
}

func TestHashReadImmutableRecordUncertainAcknowledgment(t *testing.T) {
	for _, kind := range []string{"approval", "revocation"} {
		t.Run(kind, func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129))
			id := ""
			if kind == "revocation" {
				id = hashReadApprove(t, f, req).ID
			}
			hooks := hashReadHooks{commit: func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return errors.New("fixture: durable commit acknowledgment was lost")
			}}
			var result HashReadConsent
			var err error
			if kind == "approval" {
				result, err = f.store.approveRead(context.Background(), req, hooks)
			} else {
				result, err = f.store.revokeRead(context.Background(), id, hooks)
			}
			if !errors.Is(err, ErrHashRecoveryRequired) || result.ID != "" || !f.store.poisoned {
				t.Fatal("uncertain record reported success or continued writes", result, err)
			}
			snap := hashStoreSnapshot(t, f.store)
			if snap.ReadConsent == nil || snap.ReadConsent.ID == "" || snap.Budget != nil || snap.Work[0].Sequence != 0 {
				t.Fatal("lost output cannot discover durable record", snap)
			}
			id = snap.ReadConsent.ID
			if kind == "revocation" && (snap.ReadConsent.Status != "revoked" || snap.ReadConsent.Revocation == nil) {
				t.Fatal("lost revoke acknowledgment discarded durable revocation", snap)
			}
			if _, err = f.store.ApproveRead(context.Background(), req); !errors.Is(err, ErrHashRecoveryRequired) {
				t.Fatal("uncertain dispatcher remained writable", err)
			}
			f.reopen(t)
			shown, err := f.store.Approval(context.Background(), id)
			if err != nil || !reflect.DeepEqual(shown, *snap.ReadConsent) {
				t.Fatal("reopen lost acknowledged uncertainty record", shown, err)
			}
			if kind == "approval" {
				again, err := f.store.ApproveRead(context.Background(), req)
				if err != nil || again.ID != id || !reflect.DeepEqual(again.Approval, shown.Approval) {
					t.Fatal("retry renewed uncertain approval", again, err)
				}
			} else {
				again, err := f.store.RevokeRead(context.Background(), id)
				if err != nil || !reflect.DeepEqual(again, shown) {
					t.Fatal("retry replaced uncertain revocation", again, err)
				}
			}
		})
	}
}

func TestHashReadCorruptConsentRefusesBeforeReservation(t *testing.T) {
	for _, mutation := range []string{
		"DROP TRIGGER hash_read_observation_no_delete; DELETE FROM hash_read_observation",
		"DROP TRIGGER hash_read_approval_no_update; UPDATE hash_read_approval SET payload=x'00'",
		"DROP TRIGGER hash_read_approval_no_update; PRAGMA ignore_check_constraints=ON; UPDATE hash_read_approval SET payload=zeroblob(8193)",
		"DROP TRIGGER hash_read_observation_monotonic; UPDATE hash_read_observation SET expired=1",
		"DROP TRIGGER hash_read_observation_monotonic; PRAGMA ignore_check_constraints=ON; UPDATE hash_read_observation SET max_now_ns='bad'",
	} {
		t.Run(fmt.Sprintf("%x", sha256.Sum256([]byte(mutation)))[:12], func(t *testing.T) {
			f, req := hashReadFixture(t, fullHashContents(129))
			c := hashReadApprove(t, f, req)
			if _, err := f.store.db.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			opened := false
			r, err := f.store.runConsented(context.Background(), c.ID, f.source, f.scanner, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
			if !errors.Is(err, ErrHashStoreCorrupt) || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 {
				t.Fatal("malformed consent read/charged", r, err)
			}
			var count int
			if err = f.store.db.QueryRow("SELECT count(*) FROM hash_budget").Scan(&count); err != nil || count != 0 {
				t.Fatal("malformed lifecycle created ledger", count, err)
			}
		})
	}
}
