package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

type sampleStoreFixtureState struct {
	store                             *SampleStore
	source                            *state.Store
	scanner                           *Scanner
	base, stateDir, root, inventoryID string
	expected                          []state.SameSizeFile
	batch                             SampleSnapshot
}

func sampleStoreFixture(t *testing.T, contents ...[]byte) *sampleStoreFixtureState {
	t.Helper()
	scanner, targets := sampleFixture(t, contents...)
	return sampleStoreFixtureFromFiles(t, scanner, targets)
}

func sampleStoreFixtureFromFiles(t *testing.T, scanner *Scanner, targets []SavedFileTarget) *sampleStoreFixtureState {
	t.Helper()
	ctx := context.Background()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stateBase, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &sampleStoreFixtureState{scanner: scanner, base: filepath.Join(base, "sample-data"), stateDir: filepath.Join(stateBase, "source-state"), root: string(targets[0].Root.PathBytes)}
	f.source, err = state.OpenWriter(ctx, f.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.source.Close() })
	if err = f.source.SyncRoots(ctx, []string{f.root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = f.source.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = f.source.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for count := 0; count < 512; count++ {
		job, e := f.source.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
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
		if e = f.source.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("bounded generated inventory did not finish")
	}
	finishSelectionFixtureMaintenance(t, ctx, f.source, scanner)
	db, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.QueryRow("SELECT token FROM inventory_identity WHERE singleton=1").Scan(&f.inventoryID); err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		rel, e := filepath.Rel(f.root, string(target.File.PathBytes))
		if e != nil {
			t.Fatal(e)
		}
		file := target.File
		var observed, modified int64
		err = db.QueryRow("SELECT id,size,allocated,observed_at_ns,mtime_ns,ctime_ns,device,inode,generation FROM entries WHERE root_id=1 AND path=?", []byte(rel)).Scan(&file.ID, &file.Size, &file.Allocated, &observed, &modified, &file.ChangedNS, &file.Device, &file.Inode, &file.Generation)
		if err != nil {
			t.Fatal(err)
		}
		file.ObservedAt = time.Unix(0, observed).UTC()
		file.ModifiedAt = time.Unix(0, modified).UTC()
		file.RootID = 1
		file.ParentPass = "observed_in_completed_parent_pass"
		f.expected = append(f.expected, file)
	}
	f.store, err = OpenSampleWriter(ctx, f.base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	f.store.now = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	f.batch, err = f.store.CreateBatch(ctx, f.source, f.inventoryID, f.expected, SampleReservationPolicy{SampleReservationContract, 1 << 20, 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *sampleStoreFixtureState) reopen(t *testing.T) {
	t.Helper()
	clock := f.store.now
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenSampleWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.store = s
	f.store.now = clock
}

func sampleSaved(t *testing.T, s *SampleStore) SampleSnapshot {
	t.Helper()
	r, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Source != "saved_sample_accounting" || r.Contract != SampleReservationContract || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("saved accounting claimed authority", r)
	}
	return r
}

func sampleReserved(t *testing.T, f *sampleStoreFixtureState) *SampleAttempt {
	t.Helper()
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	a, _, _, err := f.store.reserveSample(context.Background(), f.batch.BatchID, sampleStoreHooks{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func requireSampleRunUsage(t *testing.T, r SampleRunResult, requested, read int64, durable bool) {
	t.Helper()
	if !r.UsageKnown || r.UsageDurable != durable || r.Usage.RequestedBytes != requested || r.Usage.ReadBytes != read || r.Usage.Elapsed < 0 || r.ReservedBytes < requested {
		t.Fatal("incorrect actual/durable usage", r)
	}
}

func TestSampleStoreExactImmutableBatch(t *testing.T) {
	f := sampleStoreFixture(t, []byte("first"), []byte("second"))
	again, err := f.store.CreateBatch(context.Background(), f.source, f.inventoryID, f.expected, f.batch.Policy)
	if err != nil || !reflect.DeepEqual(again, f.batch) {
		t.Fatal("matching retry changed batch", again, err)
	}
	for _, kind := range []string{"policy", "order"} {
		t.Run(kind, func(t *testing.T) {
			expected := append([]state.SameSizeFile(nil), f.expected...)
			policy := f.batch.Policy
			if kind == "policy" {
				policy.LifetimeReservedByteLimit++
			} else {
				expected[0], expected[1] = expected[1], expected[0]
			}
			if _, err := f.store.CreateBatch(context.Background(), f.source, f.inventoryID, expected, policy); !errors.Is(err, ErrSampleBatchConflict) {
				t.Fatal("immutable batch replaced", err)
			}
		})
	}
	f.batch.Targets[0].File.PathBytes[0] = 'x'
	f.batch.Targets[0].Root.PathBytes[0] = 'x'
	f.batch.Targets[0].Ancestors[0].Path[0] = 'x'
	current := sampleSaved(t, f.store)
	if current.Targets[0].File.PathBytes[0] != '/' || current.Targets[0].Root.PathBytes[0] != '/' || string(current.Targets[0].Ancestors[0].Path) != "." {
		t.Fatal("returned evidence mutated store", current)
	}
	current.Targets[0].File.PathBytes[0] = 'y'
	if sampleSaved(t, f.store).Targets[0].File.PathBytes[0] != '/' {
		t.Fatal("snapshot did not deep-copy byte paths")
	}
	if _, err = f.store.db.Exec("UPDATE sample_batch SET payload=payload"); err == nil {
		t.Fatal("immutable payload trigger absent")
	}
}

func TestSampleStoreRawBytesHardlinksAndRangeGeometry(t *testing.T) {
	f := sampleStoreFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
	target := cloneHashTarget(f.batch.Targets[0])
	root := string(target.Root.PathBytes)
	// This purely encoded identity exercises invalid UTF8 on every platform,
	// without requiring a filesystem to create an unsupported byte name.
	target.File.PathBytes = []byte(filepath.Join(root, "project", string([]byte{'r', 0xff, 'w'})))
	target.File.Path = "lossy display"
	digest, err := sampleTargetDigest(target)
	if err != nil {
		t.Fatal(err)
	}
	target.File.Path = "different display"
	if again, e := sampleTargetDigest(target); e != nil || digest != again {
		t.Fatal("display string changed raw-byte identity", e)
	}
	for _, size := range []int64{1, 32768, 32769, 65536, 98304, 262144, math.MaxInt64} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			target.File.Size = size
			var grant int64
			for _, w := range fileSampleRanges(size) {
				if w.Offset < 0 || w.Length <= 0 || w.Offset > size-w.Length {
					t.Fatal(w, size)
				}
				grant += w.Length
			}
			if grant != min(size, int64(98304)) {
				t.Fatal("merged grant differs", grant, size)
			}
			if _, err := sampleTargetDigest(target); err != nil {
				t.Fatal("sample inherited full-hash size cap", err)
			}
		})
	}
	path := string(f.expected[0].PathBytes)
	alias := filepath.Join(filepath.Dir(path), "hardlink")
	if err = os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	targets := captureSampleTargets(t, f.scanner, f.root, []string{path, alias})
	g := sampleStoreFixtureFromFiles(t, f.scanner, targets)
	result, err := g.store.RunSamples(context.Background(), g.batch.BatchID, g.source, g.scanner)
	if err != nil || result.ReservedBytes != 196608 || len(result.Report.Targets) != 2 {
		t.Fatal(result, err)
	}
	for _, item := range result.Report.Targets {
		if item.LiveLinkCount != 2 || !item.RepeatedSavedIdentity || item.SHA256 == "" {
			t.Fatal("source hardlinks rejected or not qualified", item)
		}
	}
	requireSampleRunUsage(t, result, 196608, 196608, true)
}

func TestSampleStoreWholeGrantLimits(t *testing.T) {
	for _, kind := range []string{"exact", "daily_short", "lifetime_short"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, []byte("abc"), []byte("def"))
			base, _ := filepath.EvalSymlinks(t.TempDir())
			s, err := OpenSampleWriter(context.Background(), filepath.Join(base, "limits"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.now = f.store.now
			policy := SampleReservationPolicy{SampleReservationContract, 6, 6}
			if kind == "daily_short" {
				policy.DailyReservedByteLimit = 5
			}
			if kind == "lifetime_short" {
				policy.LifetimeReservedByteLimit = 5
			}
			batch, err := s.CreateBatch(context.Background(), f.source, f.inventoryID, f.expected, policy)
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			result, err := s.runSamples(context.Background(), batch.BatchID, f.source, f.scanner, sampleStoreHooks{sample: fileSampleHooks{afterRead: func(int) { reads++ }}})
			if kind == "exact" {
				if err != nil || reads != 2 || result.ReservedBytes != 6 {
					t.Fatal(result, reads, err)
				}
				requireSampleRunUsage(t, result, 6, 6, true)
			} else {
				if !errors.Is(err, ErrSampleBudget) || reads != 0 || result.UsageKnown || result.ReservedBytes != 0 || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
					t.Fatal("partial grant or source access", result, reads, err)
				}
				if saved := sampleSaved(t, s); saved.Attempt != nil || saved.Budget.TotalReservedBytes != 0 {
					t.Fatal("refusal charged an attempt", saved)
				}
			}
		})
	}
}

func TestSampleStoreAtomicReservationNoReadBeforeAck(t *testing.T) {
	for _, kind := range []string{"canceled", "rollback_error", "commit_ack_error", "acknowledged_corrupt"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, []byte("abc"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			hooks := sampleStoreHooks{sample: fileSampleHooks{afterRead: func(int) { reads++ }}}
			if kind == "canceled" {
				hooks.beforeReserveCommit = cancel
			} else {
				hooks.reserveCommit = func(tx *sql.Tx) error {
					if kind == "commit_ack_error" || kind == "acknowledged_corrupt" {
						if err := tx.Commit(); err != nil {
							return err
						}
					} else {
						if err := tx.Rollback(); err != nil {
							return err
						}
					}
					if kind == "acknowledged_corrupt" {
						_, err := f.store.db.Exec("UPDATE sample_budget SET total_requested_bytes=1")
						return err
					}
					return errors.New("injected reserve acknowledgement loss")
				}
			}
			result, err := f.store.runSamples(ctx, f.batch.BatchID, f.source, f.scanner, hooks)
			if err == nil || reads != 0 || result.UsageKnown || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
				t.Fatal("read without acknowledged reservation", result, reads, err)
			}
			if kind == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrSampleRecoveryRequired) {
				t.Fatal(err)
			}
			if kind == "acknowledged_corrupt" {
				if err = f.store.Close(); err != nil {
					t.Fatal(err)
				}
				if s, e := OpenSampleWriter(context.Background(), f.base); !errors.Is(e, ErrSampleStoreCorrupt) {
					if s != nil {
						_ = s.Close()
					}
					t.Fatal("corrupt acknowledged charge was used or repaired", e)
				}
				return
			}
			f.reopen(t)
			saved := sampleSaved(t, f.store)
			if kind == "commit_ack_error" {
				if saved.Attempt == nil || saved.Attempt.Status != "reserved" || saved.Budget.TotalOutstandingReservedBytes != 3 {
					t.Fatal(saved)
				}
				recovered, e := f.store.Recover(context.Background())
				if e != nil || recovered.Budget.TotalUnknownReservedBytes != 3 || recovered.Attempt.RequestedBytes != nil {
					t.Fatal(recovered, e)
				}
			} else if saved.Attempt != nil || saved.Budget.TotalReservedBytes != 0 {
				t.Fatal("uncommitted charge survived", saved)
			}
		})
	}
}

func TestSampleStoreKnownUsageOnSampleFailures(t *testing.T) {
	for _, kind := range []string{"after_first_read", "after_prior_target", "short_read", "identity_swap", "scanner_close", "last_return"} {
		t.Run(kind, func(t *testing.T) {
			data := bytes.Repeat([]byte{'x'}, 256*1024)
			contents := [][]byte{data}
			if kind == "after_prior_target" {
				contents = append(contents, data)
			}
			f := sampleStoreFixture(t, contents...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			h := fileSampleHooks{afterRead: func(int) {
				reads++
				if kind == "after_first_read" || kind == "after_prior_target" && reads == 4 {
					cancel()
				}
				if kind == "scanner_close" {
					f.scanner.Close()
				}
			}}
			if kind == "short_read" {
				h.afterOpen = func() {
					if err := os.Truncate(string(f.expected[0].PathBytes), 17); err != nil {
						t.Fatal(err)
					}
				}
			}
			if kind == "identity_swap" {
				h.beforeFinalCheck = func() {
					path := string(f.expected[0].PathBytes)
					if err := os.Rename(path, path+".old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if kind == "last_return" {
				h.beforeReturn = cancel
			}
			result, err := f.store.runSamples(ctx, f.batch.BatchID, f.source, f.scanner, sampleStoreHooks{sample: h})
			requested, read := int64(98304), int64(98304)
			switch kind {
			case "after_first_read", "scanner_close":
				requested = 32768
				read = 32768
			case "after_prior_target":
				requested = 131072
				read = 131072
			case "short_read":
				requested = 32768
				read = 17
			}
			requireSampleRunUsage(t, result, requested, read, true)
			if kind == "short_read" || kind == "identity_swap" {
				if err != nil || result.Report.Status != "blocked" || result.Report.Targets[0].SHA256 != "" {
					t.Fatal("blocked path published digest", result, err)
				}
			} else if err == nil || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
				t.Fatal("call error published evidence", result, err)
			}
			saved := sampleSaved(t, f.store)
			if saved.Budget.TotalReservedBytes != int64(len(contents))*98304 || saved.Budget.TotalRequestedBytes != requested || saved.Budget.TotalReadBytes != read || saved.Budget.TotalOutstandingReservedBytes != 0 {
				t.Fatal("error refunded or lost usage", saved)
			}
		})
	}
}

func TestSampleStoreSettlementAckLossAndLateVeto(t *testing.T) {
	for _, kind := range []string{"rollback_error", "commit_ack_error", "before_commit_cancel", "after_commit_cancel", "close_after_read", "settled_usage_replaced"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := sampleStoreHooks{}
			closed := make(chan error, 1)
			completedReads := 0
			switch kind {
			case "rollback_error", "commit_ack_error":
				h.settleCommit = func(tx *sql.Tx) error {
					if kind == "commit_ack_error" {
						if err := tx.Commit(); err != nil {
							return err
						}
					} else {
						if err := tx.Rollback(); err != nil {
							return err
						}
					}
					return errors.New("injected settlement acknowledgement loss")
				}
			case "before_commit_cancel":
				h.beforeSettleCommit = cancel
			case "after_commit_cancel":
				h.afterSettleCommit = cancel
			case "settled_usage_replaced":
				h.afterSettleCommit = func() {
					if _, err := f.store.db.Exec("DROP TRIGGER sample_attempt_transition; UPDATE sample_attempt SET requested_bytes=0,read_bytes=0,elapsed_ns=0; UPDATE sample_budget SET requested_bytes=0,read_bytes=0,total_requested_bytes=0,total_read_bytes=0"); err != nil {
						t.Fatal(err)
					}
				}
			case "close_after_read":
				h.sample.afterRead = func(int) {
					completedReads++
					if completedReads != 1 {
						return
					}
					go func() { closed <- f.store.Close() }()
					select {
					case <-f.store.life.Done():
					case <-time.After(2 * time.Second):
						t.Fatal("Close did not signal lifetime cancellation")
					}
				}
			}
			result, err := f.store.runSamples(ctx, f.batch.BatchID, f.source, f.scanner, h)
			if err == nil || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
				t.Fatal("failed or canceled publication returned evidence", result, err)
			}
			requested := int64(98304)
			if kind == "close_after_read" {
				if ctx.Err() != nil || !errors.Is(err, context.Canceled) || completedReads < 1 || completedReads > 3 {
					t.Fatal("Close changed caller context or lost actual read count", ctx.Err(), completedReads, err)
				}
				requested = int64(completedReads) * 32768
				select {
				case e := <-closed:
					if e != nil {
						t.Fatal(e)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("Close did not finish after settlement")
				}
			}
			durable := kind != "rollback_error" && kind != "commit_ack_error" && kind != "settled_usage_replaced"
			requireSampleRunUsage(t, result, requested, requested, durable)
			if !durable && !errors.Is(err, ErrSampleRecoveryRequired) {
				t.Fatal(err)
			}
			f.reopen(t)
			saved, err := f.store.Recover(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "rollback_error" {
				if saved.Attempt.Status != "interrupted_unknown" || saved.Budget.TotalUnknownReservedBytes != 98304 || saved.Attempt.RequestedBytes != nil {
					t.Fatal(saved)
				}
			} else {
				want := requested
				if kind == "settled_usage_replaced" {
					want = 0
				}
				if saved.Attempt.Status != "settled" || *saved.Attempt.RequestedBytes != want || saved.Budget.TotalUnknownReservedBytes != 0 {
					t.Fatal("known committed usage became unknown", saved)
				}
			}
		})
	}
}

func TestSampleStoreOneShotAndRecoveryIdempotence(t *testing.T) {
	for _, kind := range []string{"settled", "unknown", "recover_rollback", "recover_ack_loss"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, []byte("abc"))
			if kind == "settled" {
				if _, err := f.store.RunSamples(context.Background(), f.batch.BatchID, f.source, f.scanner); err != nil {
					t.Fatal(err)
				}
			} else {
				sampleReserved(t, f)
				if _, err := f.store.RunSamples(context.Background(), f.batch.BatchID, f.source, f.scanner); !errors.Is(err, ErrSampleRecoveryRequired) {
					t.Fatal("outstanding reservation replayed", err)
				}
				h := sampleStoreHooks{}
				if strings.HasPrefix(kind, "recover_") {
					h.recoverCommit = func(tx *sql.Tx) error {
						if kind == "recover_ack_loss" {
							if err := tx.Commit(); err != nil {
								return err
							}
						} else {
							if err := tx.Rollback(); err != nil {
								return err
							}
						}
						return errors.New("injected recovery acknowledgement loss")
					}
				}
				_, err := f.store.recoverSamples(context.Background(), h)
				if strings.HasPrefix(kind, "recover_") {
					if !errors.Is(err, ErrSampleRecoveryRequired) {
						t.Fatal(err)
					}
					f.reopen(t)
				} else if err != nil {
					t.Fatal(err)
				}
			}
			f.store.now = func() time.Time { t.Fatal("metadata recovery sampled wall clock"); return time.Time{} }
			first, err := f.store.Recover(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			second, err := f.store.Recover(context.Background())
			if err != nil || !reflect.DeepEqual(first, second) {
				t.Fatal("recovery changed charge twice", second, err)
			}
			reads := 0
			result, err := f.store.runSamples(context.Background(), f.batch.BatchID, f.source, f.scanner, sampleStoreHooks{sample: fileSampleHooks{afterRead: func(int) { reads++ }}})
			if !errors.Is(err, ErrSampleOneShot) || reads != 0 || result.UsageKnown || first.Budget.TotalReservedBytes != 3 {
				t.Fatal("terminal attempt replayed or refunded", result, reads, first, err)
			}
		})
	}
}

func TestSampleStoreReservationDayAndClockHighWater(t *testing.T) {
	for _, kind := range []string{"same", "forward", "backward", "invalid", "cross_midnight", "rollback_after_read"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			initial := f.batch.Budget.MaxNow
			h := sampleStoreHooks{}
			switch kind {
			case "forward":
				f.store.now = func() time.Time { return initial.Add(24 * time.Hour) }
			case "backward":
				f.store.now = func() time.Time { return initial.Add(-time.Nanosecond) }
			case "invalid":
				f.store.now = func() time.Time { return time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC) }
			case "cross_midnight", "rollback_after_read":
				h.sample.afterRead = func(int) {
					f.store.now = func() time.Time {
						if kind == "cross_midnight" {
							return initial.Add(24 * time.Hour)
						}
						return initial.Add(-time.Second)
					}
				}
			}
			result, err := f.store.runSamples(context.Background(), f.batch.BatchID, f.source, f.scanner, h)
			if kind == "backward" || kind == "invalid" {
				if !errors.Is(err, ErrSampleClockRollback) || result.UsageKnown || sampleSaved(t, f.store).Attempt != nil {
					t.Fatal(result, err)
				}
				return
			}
			if kind == "rollback_after_read" {
				if !errors.Is(err, ErrSampleClockRollback) || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
					t.Fatal(result, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			requireSampleRunUsage(t, result, 98304, 98304, true)
			saved := sampleSaved(t, f.store)
			if saved.Budget.TotalReservedBytes != 98304 || saved.Budget.TotalRequestedBytes != 98304 || saved.Budget.MaxNow.Before(initial) {
				t.Fatal(saved)
			}
			if kind == "cross_midnight" && (saved.Budget.ReservedBytes != 0 || saved.Budget.RequestedBytes != 0 || saved.Attempt.ReservationDay != initial.Format(time.DateOnly)) {
				t.Fatal("settlement reassigned original day", saved)
			}
			if kind == "rollback_after_read" && !saved.Budget.MaxNow.Equal(initial) {
				t.Fatal("clock high-water changed on rollback", saved)
			}
		})
	}
}

func TestSampleStoreInventoryAndPrivateIdentityRefusals(t *testing.T) {
	for _, kind := range []string{"before_read", "after_read", "private_lock_replaced"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, []byte("abc"))
			mutate := func() {
				db, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err = db.Exec("UPDATE entries SET generation=generation+1 WHERE id=?", f.expected[0].ID); err != nil {
					t.Fatal(err)
				}
			}
			reads := 0
			h := sampleStoreHooks{sample: fileSampleHooks{afterRead: func(int) { reads++ }}}
			if kind == "before_read" {
				mutate()
			} else if kind == "after_read" {
				h.sample.afterRead = func(int) { reads++; mutate() }
			} else {
				path := filepath.Join(f.base, "samples", "writer.lock")
				if err := os.Rename(path, path+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.store.runSamples(context.Background(), f.batch.BatchID, f.source, f.scanner, h)
			if err == nil || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
				t.Fatal("changed evidence published", result, err)
			}
			if kind == "after_read" {
				if !errors.Is(err, ErrSampleInventoryChanged) || reads != 1 {
					t.Fatal(reads, err)
				}
				requireSampleRunUsage(t, result, 3, 3, true)
			} else if reads != 0 || result.ReservedBytes != 0 || result.UsageKnown {
				t.Fatal("refused input caused read/charge", result, reads)
			}
		})
	}
}

func TestSampleStorePrivateLeafAndLockAdmission(t *testing.T) {
	for _, kind := range []string{"db_symlink", "db_hardlink", "db_fifo", "lock_symlink", "shared_directory"} {
		t.Run(kind, func(t *testing.T) {
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(base, "samples")
			if err = os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			other := filepath.Join(base, "other")
			if err = os.WriteFile(other, nil, 0600); err != nil {
				t.Fatal(err)
			}
			db := filepath.Join(dir, sampleStoreFilename)
			switch kind {
			case "db_symlink":
				err = os.Symlink(other, db)
			case "db_hardlink":
				err = os.Link(other, db)
			case "db_fifo":
				err = unix.Mkfifo(db, 0600)
			case "lock_symlink":
				err = os.Symlink(other, filepath.Join(dir, "writer.lock"))
			case "shared_directory":
				err = os.Chmod(dir, 0755)
			}
			if err != nil {
				t.Fatal(err)
			}
			if s, e := OpenSampleWriter(context.Background(), base); e == nil {
				_ = s.Close()
				t.Fatal("unsupported private leaf admitted", kind)
			}
			info, e := os.Lstat(dir)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "shared_directory" && info.Mode().Perm() != 0755 {
				t.Fatal("opener changed another directory's permissions")
			}
		})
	}
	f := sampleStoreFixture(t, []byte("abc"))
	info, err := os.Stat(filepath.Join(f.base, "samples", "writer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if second, err := OpenSampleWriter(context.Background(), f.base); err == nil {
		_ = second.Close()
		t.Fatal("second writer admitted")
	}
	var private unix.Stat_t
	if err = unix.Lstat(filepath.Join(f.base, "samples", sampleStoreFilename), &private); err != nil {
		t.Fatal(err)
	}
	wanted := append([]state.SameSizeFile(nil), f.expected...)
	wanted[0].Device = fmt.Sprint(private.Dev)
	wanted[0].Inode = fmt.Sprint(private.Ino)
	sourceDB, err := sql.Open("sqlite", filepath.Join(f.stateDir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sourceDB.Exec("UPDATE entries SET device=?,inode=? WHERE id=?", wanted[0].Device, wanted[0].Inode, wanted[0].ID); err != nil {
		t.Fatal(err)
	}
	if err = sourceDB.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.CreateBatch(context.Background(), f.source, f.inventoryID, wanted, f.batch.Policy); err == nil {
		t.Fatal("saved file identity alias to private ledger admitted")
	}
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(f.base, "samples", "writer.lock"))
	if err != nil || !os.SameFile(info, after) {
		t.Fatal("stable lock was removed/replaced", err)
	}
	if err = f.store.Close(); err != nil {
		t.Fatal("Close not idempotent", err)
	}
}

func TestSampleStoreTypedCorruptionAndConservation(t *testing.T) {
	for _, kind := range []string{"text_counter", "extra_row", "negative_counter", "coherent_bad_payload", "grant_mismatch", "unknown_mismatch"} {
		t.Run(kind, func(t *testing.T) {
			f := sampleStoreFixture(t, []byte("abc"))
			if kind == "grant_mismatch" || kind == "unknown_mismatch" {
				sampleReserved(t, f)
			}
			var err error
			switch kind {
			case "text_counter":
				_, err = f.store.db.Exec("UPDATE sample_budget SET requested_bytes='bad'")
			case "negative_counter":
				_, err = f.store.db.Exec("UPDATE sample_budget SET read_bytes=-1")
			case "extra_row":
				_, err = f.store.db.Exec("PRAGMA ignore_check_constraints=ON; INSERT INTO sample_budget SELECT 2,day,max_now_ns,reserved_bytes,requested_bytes,read_bytes,unknown_bytes,outstanding_bytes,total_reserved_bytes,total_requested_bytes,total_read_bytes,total_unknown_bytes,total_outstanding_bytes FROM sample_budget")
			case "coherent_bad_payload":
				body := []byte(`{"version":1,"unexpected":true}`)
				h := sha256.Sum256(body)
				_, err = f.store.db.Exec("DROP TRIGGER sample_batch_no_update; UPDATE sample_batch SET payload=?,batch_id=?", body, hex.EncodeToString(h[:]))
			case "grant_mismatch":
				_, err = f.store.db.Exec("DROP TRIGGER sample_attempt_transition; UPDATE sample_attempt SET grant_bytes=4")
			case "unknown_mismatch":
				_, err = f.store.db.Exec("UPDATE sample_budget SET total_unknown_bytes=1")
			}
			if err != nil {
				t.Fatal("corruption fixture was not established", err)
			}
			reads := 0
			result, err := f.store.runSamples(context.Background(), f.batch.BatchID, f.source, f.scanner, sampleStoreHooks{sample: fileSampleHooks{afterRead: func(int) { reads++ }}})
			if !errors.Is(err, ErrSampleStoreCorrupt) || reads != 0 || !reflect.DeepEqual(result.Report, FileSampleReport{}) {
				t.Fatal("corrupt saved state used for reads", result, reads, err)
			}
		})
	}

	// The bounded outcome projection must preserve empty TEXT without admitting
	// malformed stored types. This disposable unconstrained table tests the
	// defensive reader; it does not weaken the production schema.
	for _, tc := range []struct {
		name, status      string
		outcome           any
		knownUsage, valid bool
	}{
		{"outcome_reserved_empty_text", "reserved", "", false, true},
		{"outcome_settled_observed_text", "settled", "observed", true, true},
		{"outcome_interrupted_text", "interrupted_unknown", "interrupted_unknown", false, true},
		{"outcome_reserved_sql_null", "reserved", nil, false, false},
		{"outcome_reserved_blob", "reserved", []byte{}, false, false},
		{"outcome_reserved_nonempty", "reserved", "observed", false, false},
		{"outcome_settled_empty_text", "settled", "", true, false},
		{"outcome_settled_unknown_code", "settled", "not_an_outcome", true, false},
		{"outcome_settled_overlong_text", "settled", strings.Repeat("x", 66), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "attempt.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error("close generated outcome database", err)
				}
			}()
			db.SetMaxOpenConns(1)
			if _, err = db.ExecContext(ctx, `CREATE TABLE sample_attempt (
 id, store_id, batch_id, nonce, reservation_day, reserved_at_ns,
 grant_bytes, status, outcome, requested_bytes, read_bytes, elapsed_ns)`); err != nil {
				t.Fatal("create generated outcome table", err)
			}
			storeID, batchID, nonce := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)
			at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			var requested, read, elapsed any
			if tc.knownUsage {
				requested, read, elapsed = int64(0), int64(0), int64(0)
			}
			if _, err = db.ExecContext(ctx, "INSERT INTO sample_attempt VALUES(1,?,?,?,?,?,?,?,CASE WHEN ?='outcome_reserved_blob' THEN CAST('' AS BLOB) ELSE ? END,?,?,?)", storeID, batchID, nonce, at.Format(time.DateOnly), at.UnixNano(), int64(3), tc.status, tc.name, tc.outcome, requested, read, elapsed); err != nil {
				t.Fatal("insert generated outcome row", err)
			}
			wantType := "text"
			switch tc.outcome.(type) {
			case nil:
				wantType = "null"
			case []byte:
				wantType = "blob"
			}
			var storedType string
			var storedByteLength sql.NullInt64
			if err = db.QueryRowContext(ctx, "SELECT typeof(outcome),length(CAST(outcome AS BLOB)) FROM sample_attempt").Scan(&storedType, &storedByteLength); err != nil || storedType != wantType {
				t.Fatal("outcome fixture stored the wrong type", storedType, wantType, err)
			}
			if tc.name == "outcome_reserved_blob" && (!storedByteLength.Valid || storedByteLength.Int64 != 0) {
				t.Fatal("empty BLOB fixture was not established", storedByteLength)
			}
			attempt, err := readSampleAttempt(ctx, db, storeID, batchID, 3)
			if !tc.valid {
				if !errors.Is(err, ErrSampleStoreCorrupt) || attempt != nil {
					t.Fatal("invalid outcome projection was accepted", attempt, err)
				}
				return
			}
			if err != nil || attempt == nil || attempt.ID != nonce || attempt.ReservationDay != at.Format(time.DateOnly) || !attempt.ReservedAt.Equal(at) || attempt.ReservedBytes != 3 || attempt.Status != tc.status || attempt.Code != tc.outcome.(string) {
				t.Fatal("valid outcome projection changed saved fields", attempt, err)
			}
			if tc.knownUsage {
				if attempt.RequestedBytes == nil || attempt.ReadBytes == nil || attempt.ElapsedNS == nil || *attempt.RequestedBytes != 0 || *attempt.ReadBytes != 0 || *attempt.ElapsedNS != 0 {
					t.Fatal("known zero usage lost its non-nil representation", attempt)
				}
			} else if attempt.RequestedBytes != nil || attempt.ReadBytes != nil || attempt.ElapsedNS != nil {
				t.Fatal("unknown usage became known zero", attempt)
			}
		})
	}
}

type sampleSnapshotInterleave struct {
	tx    *sql.Tx
	once  sync.Once
	after func()
}

func (q *sampleSnapshotInterleave) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row := q.tx.QueryRowContext(ctx, query, args...)
	if strings.Contains(query, "FROM sample_meta") {
		q.once.Do(q.after)
	}
	return row
}
func (q *sampleSnapshotInterleave) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.tx.QueryContext(ctx, query, args...)
}

func TestSampleStoreSavedWALSnapshotIsolation(t *testing.T) {
	f := sampleStoreFixture(t, []byte("abc"))
	reader, err := OpenSampleReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.now = func() time.Time { t.Fatal("reader sampled wall clock"); return time.Time{} }
	tx, err := reader.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	q := &sampleSnapshotInterleave{tx: tx, after: func() {
		result, e := f.store.RunSamples(context.Background(), f.batch.BatchID, f.source, f.scanner)
		if e != nil || !result.UsageDurable {
			t.Fatal(result, e)
		}
	}}
	old, _, err := reader.readSampleSnapshot(context.Background(), q)
	if err != nil || old.Attempt != nil || old.Budget.TotalReservedBytes != 0 {
		t.Fatal("reader mixed concurrent WAL states", old, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	current := sampleSaved(t, reader)
	if current.Attempt.Status != "settled" || current.Budget.TotalReadBytes != 3 {
		t.Fatal("new snapshot did not see whole publication", current)
	}
	if _, err = reader.Recover(context.Background()); err == nil {
		t.Fatal("reader recovered")
	}
	if _, err = reader.RunSamples(context.Background(), f.batch.BatchID, f.source, f.scanner); err == nil {
		t.Fatal("reader dispatched")
	}
	if _, err = reader.CreateBatch(context.Background(), f.source, f.inventoryID, f.expected, f.batch.Policy); err == nil {
		t.Fatal("reader mutated batch")
	}
}

func TestSampleStoreWorstShapeBounds(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Keep all 32 ancestors while allowing the fixture on native systems with
	// a smaller absolute-path limit. The original component cap remains 40.
	const pathByteBudget = 1000
	const ancestorCount = 32
	componentBytes := min(40, (pathByteBudget-len(root)-ancestorCount-len("/file-19"))/ancestorCount)
	if componentBytes < len("00-")+1 {
		t.Fatal("temporary root leaves too little portable fixture path budget", len(root), componentBytes)
	}
	parent := root
	for i := 0; i < ancestorCount; i++ {
		parent = filepath.Join(parent, fmt.Sprintf("%02d-%s", i, strings.Repeat("a", componentBytes-len("00-"))))
		if err = os.Mkdir(parent, 0700); err != nil {
			t.Fatal(err)
		}
	}
	paths := make([]string, 20)
	data := bytes.Repeat([]byte{'x'}, 98304)
	for i := range paths {
		paths[i] = filepath.Join(parent, fmt.Sprintf("file-%02d", i))
		if len(paths[i]) > pathByteBudget {
			t.Fatal("portable fixture path budget exceeded", len(paths[i]))
		}
		if err = os.WriteFile(paths[i], data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	scanner, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	f := sampleStoreFixtureFromFiles(t, scanner, captureSampleTargets(t, scanner, root, paths))
	// The fixture helper's deliberately smaller default budget must not be
	// replaced. Select an independent explicit full-cap ledger for this batch.
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenSampleWriter(context.Background(), filepath.Join(base, "full-cap"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.now = f.store.now
	batch, err := s.CreateBatch(context.Background(), f.source, f.inventoryID, f.expected, SampleReservationPolicy{SampleReservationContract, 1966080, 1966080})
	if err != nil {
		t.Fatal(err)
	}
	var length int
	if err = s.db.QueryRow("SELECT length(payload) FROM sample_batch").Scan(&length); err != nil || length < 512*1024 || length > 1048576 {
		t.Fatal("fixture did not exercise bounded large evidence", length, err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := s.RunSamples(context.Background(), batch.BatchID, f.source, scanner)
	if err != nil || len(result.Report.Targets) != 20 || result.ReservedBytes != 1966080 {
		t.Fatal(result, err)
	}
	requireSampleRunUsage(t, result, 1966080, 1966080, true)
	var count int
	if err = s.db.QueryRow("SELECT (SELECT count(*) FROM sample_meta)+(SELECT count(*) FROM sample_batch)+(SELECT count(*) FROM sample_attempt)+(SELECT count(*) FROM sample_budget)").Scan(&count); err != nil || count != 4 {
		t.Fatal("row bound changed", count, err)
	}
	for _, item := range result.Report.Targets {
		if item.SHA256 == "" || item.RequestedBytes != 98304 {
			t.Fatal(item)
		}
	}
	result.Report = FileSampleReport{}
	snapshot := sampleSaved(t, s)
	if len(snapshot.Targets) != 20 || snapshot.PlannedBytes != 1966080 || snapshot.Budget.TotalOutstandingReservedBytes != 0 {
		t.Fatal(snapshot)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	// This additional retained envelope belongs to the <=1MiB sample ledger,
	// not the unrelated 128KiB candidate receipt. Cumulative allocation is a
	// different measurement and is reported separately, never substituted for it.
	if after.HeapAlloc > before.HeapAlloc && after.HeapAlloc-before.HeapAlloc > 8*1024*1024 {
		t.Fatal("sample retained-state envelope exceeded", after.HeapAlloc-before.HeapAlloc)
	}
	t.Logf("sample additional retained=%d cumulative allocations=%d canonical evidence=%d", int64(after.HeapAlloc)-int64(before.HeapAlloc), after.TotalAlloc-before.TotalAlloc, length)
	runtime.KeepAlive(f)
	runtime.KeepAlive(batch)
	runtime.KeepAlive(snapshot)
	runtime.KeepAlive(result)
	runtime.KeepAlive(paths)
}
