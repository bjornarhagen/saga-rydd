package state

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func adaptiveFixture(t *testing.T) (*Store, AdaptiveRevisitScope, time.Time) {
	t.Helper()
	s, _ := queueStore(t)
	ctx := context.Background()
	roots, err := s.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := AdaptiveRevisitScopeDigest([]string{"/fixture/a"}, nil, []string{"/fixture/private"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	scope, err := s.ConfigureAdaptiveRevisits(ctx, roots, true, digest, now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.FinalizeAdaptiveRevisit(ctx, scope, 1, now)
	if err != nil || !result.Scheduled || !result.Due.Equal(now) {
		t.Fatal(result, err)
	}
	return s, scope, now
}

func adaptiveEntry(path, kind string, size int64) Entry {
	return Entry{Path: []byte(path), Kind: kind, Size: size, Allocated: size, MtimeNS: 100, CtimeNS: 200, Device: "11", Inode: strconv.Itoa(100 + len(path))}
}

func adaptiveClaimJob(t *testing.T, s *Store, now time.Time) Job {
	t.Helper()
	j, err := s.ClaimJob(context.Background(), []string{ScanKind}, now, time.Hour)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	return *j
}

func adaptiveCommit(t *testing.T, s *Store, scope AdaptiveRevisitScope, j Job, entries []Entry, gen int64, complete bool, now time.Time) {
	t.Helper()
	b := ScanBatch{Identity: "fixture-volume", Generation: gen, Directory: adaptiveEntry(string(j.Path), "directory", 4), Entries: entries, Complete: complete}
	if !complete {
		b.Cursor = []byte("fixture continuation")
	}
	if err := s.CommitAdaptiveScan(context.Background(), scope, j, b, now); err != nil {
		t.Fatal(err)
	}
}

func adaptiveDrain(t *testing.T, s *Store, root int64) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		step, err := s.RetireInventoryForRoot(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		if !step.Remaining {
			return
		}
		if !step.Worked {
			t.Fatal("maintenance failed to advance", step)
		}
	}
	t.Fatal("bounded generated maintenance did not drain")
}

func adaptivePass(t *testing.T, s *Store, scope AdaptiveRevisitScope, now time.Time, gen int64, file Entry) AdaptiveRevisitResult {
	t.Helper()
	j := adaptiveClaimJob(t, s, now)
	adaptiveCommit(t, s, scope, j, []Entry{adaptiveEntry("deep", "directory", 4)}, gen, true, now)
	j = adaptiveClaimJob(t, s, now)
	adaptiveCommit(t, s, scope, j, []Entry{file}, gen+1, true, now)
	adaptiveDrain(t, s, 1)
	r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now)
	if err != nil || !r.Scheduled {
		t.Fatal(r, err)
	}
	return r
}

func TestAdaptiveRevisitLearnsWholeEpochAndDeepMetadataChanges(t *testing.T) {
	s, scope, now := adaptiveFixture(t)
	file := adaptiveEntry("deep/file", "file", 9)
	r := adaptivePass(t, s, scope, now, 1, file)
	if !r.Changed || !r.Unknown || r.UnchangedStreak != 0 || r.Interval != InventoryRevisitInterval {
		t.Fatal(r)
	}
	first := r
	if again, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now.Add(time.Second)); err != nil || again.Scheduled || !again.Due.Equal(first.Due) || again.Epoch != first.Epoch || again.Changed != first.Changed || again.Unknown != first.Unknown || again.UnchangedStreak != first.UnchangedStreak {
		t.Fatal("retry changed first learning/result", again, err)
	}
	now = r.Due
	r = adaptivePass(t, s, scope, now, 10, file)
	if r.Changed || r.Unknown || r.UnchangedStreak != 1 || r.Interval != InventoryRevisitInterval {
		t.Fatal(r)
	}
	now = r.Due
	r = adaptivePass(t, s, scope, now, 20, file)
	if r.Changed || r.Unknown || r.UnchangedStreak != 2 || r.Interval != AdaptiveStableRevisitInterval {
		t.Fatal(r)
	}
	if found, err := s.AdaptiveRevisitPending(context.Background(), scope, r.Due); err != nil || !found {
		t.Fatal(found, err)
	}
	status, err := s.AdaptiveRevisitStatus(context.Background(), scope)
	if err != nil || status.TrackedRoots != 1 || status.StableRoots != 1 || status.WeeklyRoots != 1 || !status.HistoricalMetadataOnly || status.CurrentContentVerified || status.CleanupApproved {
		t.Fatal(status, err)
	}
	now = r.Due
	file.Size++
	file.Allocated++
	file.CtimeNS++
	r = adaptivePass(t, s, scope, now, 30, file)
	if !r.Changed || r.Unknown || r.UnchangedStreak != 0 || r.Interval != InventoryRevisitInterval {
		t.Fatal("deep edit did not reset learning", r)
	}
}

func TestAdaptiveRevisitSuccessfulPartialChunksAndCheckedRemoval(t *testing.T) {
	s, scope, now := adaptiveFixture(t)
	entries := []Entry{adaptiveEntry("keep", "file", 4), adaptiveEntry("removed", "file", 5)}
	for pass := int64(1); pass <= 3; pass++ {
		j := adaptiveClaimJob(t, s, now)
		adaptiveCommit(t, s, scope, j, entries[:1], pass, false, now)
		if r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now); err != nil || r.Scheduled {
			t.Fatal("partial epoch finalized", r, err)
		}
		j = adaptiveClaimJob(t, s, now)
		adaptiveCommit(t, s, scope, j, entries[1:], pass, true, now)
		adaptiveDrain(t, s, 1)
		r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now)
		if err != nil || r.Unknown != (pass == 1) || !r.Scheduled {
			t.Fatal(r, err)
		}
		if pass == 3 && r.Interval != AdaptiveStableRevisitInterval {
			t.Fatal("normal partial chunks prevented learning", r)
		}
		now = r.Due
	}
	j := adaptiveClaimJob(t, s, now)
	adaptiveCommit(t, s, scope, j, entries[:1], 4, true, now)
	if r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now); err != nil || r.Scheduled {
		t.Fatal("reconciliation bypassed", r, err)
	}
	adaptiveDrain(t, s, 1)
	r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now)
	if err != nil || !r.Changed || r.Unknown || r.Interval != InventoryRevisitInterval || countRows(t, s, "SELECT count(*) FROM entries WHERE path=X'72656d6f766564'") != 0 {
		t.Fatal(r, err)
	}
}

func TestAdaptiveRevisitUncertaintyFailuresRecoveryAndUntrackedWrites(t *testing.T) {
	for _, failure := range []string{"fault", "cancel", "recover", "untracked", "skip", "identity", "generation", "stale_proof"} {
		t.Run(failure, func(t *testing.T) {
			s, scope, now := adaptiveFixture(t)
			r := adaptivePass(t, s, scope, now, 1, adaptiveEntry("deep/file", "file", 5))
			now = r.Due
			j := adaptiveClaimJob(t, s, now)
			entry := adaptiveEntry("deep", "directory", 4)
			switch failure {
			case "fault":
				if err := s.CommitAdaptiveScan(context.Background(), scope, j, ScanBatch{Fault: "generated unavailable"}, now); err != nil {
					t.Fatal(err)
				}
				now = now.Add(2 * time.Hour)
				j = adaptiveClaimJob(t, s, now)
			case "cancel":
				if err := s.FinishJob(context.Background(), j, false, nil, now, ""); err != nil {
					t.Fatal(err)
				}
				j = adaptiveClaimJob(t, s, now)
			case "recover":
				if n, err := s.RecoverJobs(context.Background(), now); err != nil || n != 1 {
					t.Fatal(n, err)
				}
				j = adaptiveClaimJob(t, s, now)
			case "skip":
				entry.SkipReason = "directory unavailable"
			case "identity":
				entry.Device = ""
			case "generation":
				adaptiveCommit(t, s, scope, j, []Entry{entry}, 10, false, now)
				j = adaptiveClaimJob(t, s, now)
			}
			if failure == "untracked" {
				if err := s.commitScan(context.Background(), j, ScanBatch{Identity: "fixture-volume", Generation: 20, Directory: adaptiveEntry(".", "directory", 4), Entries: []Entry{entry}, Complete: true}, nil, now); err != nil {
					t.Fatal(err)
				}
			} else {
				adaptiveCommit(t, s, scope, j, []Entry{entry}, 20, true, now)
			}
			if entry.SkipReason == "" {
				j = adaptiveClaimJob(t, s, now)
				adaptiveCommit(t, s, scope, j, []Entry{adaptiveEntry("deep/file", "file", 5)}, 21, true, now)
			}
			if failure == "stale_proof" {
				if _, err := s.db.Exec("UPDATE subtree_reconcile SET generation=generation+100 WHERE root_id=1"); err != nil {
					t.Fatal(err)
				}
			}
			adaptiveDrain(t, s, 1)
			r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now)
			if err != nil || !r.Unknown || r.UnchangedStreak != 0 || r.Interval != InventoryRevisitInterval {
				t.Fatal("uncertainty extended wait", r, err)
			}
		})
	}
}

func adaptiveWeekly(t *testing.T, s *Store, scope AdaptiveRevisitScope, now time.Time) AdaptiveRevisitResult {
	t.Helper()
	var r AdaptiveRevisitResult
	for gen := int64(1); gen <= 3; gen++ {
		r = adaptivePass(t, s, scope, now, gen*10, adaptiveEntry("deep/file", "file", 5))
		now = r.Due
	}
	if r.Interval != AdaptiveStableRevisitInterval {
		t.Fatal(r)
	}
	return r
}

func TestAdaptiveRevisitPolicyChangeShortensOnlyNeverClaimedJob(t *testing.T) {
	for _, changed := range []string{"disable", "scope", "claimed", "reset_attempts", "error"} {
		t.Run(changed, func(t *testing.T) {
			s, scope, now := adaptiveFixture(t)
			r := adaptiveWeekly(t, s, scope, now)
			at := r.Due.Add(-6 * 24 * time.Hour)
			if changed == "claimed" || changed == "reset_attempts" {
				j := adaptiveClaimJob(t, s, r.Due)
				at = r.Due
				if changed == "reset_attempts" {
					if err := s.FinishJob(context.Background(), j, false, nil, r.Due, ""); err != nil {
						t.Fatal(err)
					}
				}
			}
			if changed == "error" {
				if _, err := s.db.Exec("UPDATE jobs SET last_error='offline',attempts=3 WHERE root_id=1"); err != nil {
					t.Fatal(err)
				}
			}
			digest := scope.digest
			if changed == "scope" {
				digest = strings.Repeat("b", 64)
			}
			newScope, err := s.ConfigureAdaptiveRevisits(context.Background(), scope.roots, false, digest, at)
			if err != nil {
				t.Fatal(err)
			}
			var due int64
			if err = s.db.QueryRow("SELECT due_at_ns FROM jobs WHERE root_id=1").Scan(&due); err != nil {
				t.Fatal(err)
			}
			want := r.Due
			if changed == "disable" || changed == "scope" {
				want = r.Due.Add(-6 * 24 * time.Hour)
			}
			if due != want.UnixNano() {
				t.Fatal("policy rewrote started/retry or retained untouched weekly work", time.Unix(0, due), want)
			}
			if _, err = s.AdaptiveRevisitStatus(context.Background(), scope); !errors.Is(err, ErrAdaptiveRevisitInput) {
				t.Fatal("old scope accepted", err)
			}
			status, err := s.AdaptiveRevisitStatus(context.Background(), newScope)
			if err != nil || status.StableRoots != 0 {
				t.Fatal(status, err)
			}
			if _, err = s.FinalizeAdaptiveRevisit(context.Background(), newScope, 1, at); !errors.Is(err, ErrAdaptiveRevisitInput) {
				t.Fatal("disabled tracking finalized", err)
			}
		})
	}
}

func TestAdaptiveRevisitPretrackingScopeRawPathsAndClockAtomicity(t *testing.T) {
	s, _ := queueStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	paths := []string{"/fixture/a"}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("deep"), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	roots, err := s.ResolveFairInventoryRoots(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := AdaptiveRevisitScopeDigest(paths, []string{"/fixture/a/excluded"}, []string{"/fixture/private"})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := s.ConfigureAdaptiveRevisits(ctx, roots, true, digest, now)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := s.FinalizeAdaptiveRevisit(ctx, scope, 1, now); err != nil || r.Scheduled || !r.Unknown {
		t.Fatal("future/pretracking bypassed", r, err)
	}
	if _, err = s.FinalizeAdaptiveRevisit(ctx, scope, 1, now.Add(-time.Nanosecond)); !errors.Is(err, ErrAdaptiveRevisitClock) {
		t.Fatal(err)
	}
	j := adaptiveClaimJob(t, s, now.Add(time.Hour))
	adaptiveCommit(t, s, scope, j, nil, 1, true, now.Add(time.Hour))
	adaptiveDrain(t, s, 1)
	canceled, cancel := context.WithCancel(ctx)
	if _, err = s.finalizeAdaptiveRevisit(canceled, scope, 1, now.Add(time.Hour), inventoryRevisitHooks{beforeCommit: cancel}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
		t.Fatal("canceled publication left job")
	}
	r, err := s.FinalizeAdaptiveRevisit(ctx, scope, 1, now.Add(time.Hour))
	if err != nil || !r.Unknown || r.Interval != InventoryRevisitInterval {
		t.Fatal(r, err)
	}
	if _, err = s.FinalizeAdaptiveRevisit(ctx, scope, 1, time.Unix(0, math.MaxInt64)); err != nil {
		t.Fatal("retained job should not overflow/change", err)
	}
	ids := scope.RootIDs()
	ids[0] = 999
	if !reflect.DeepEqual(scope.RootIDs(), []int64{1}) {
		t.Fatal("caller mutated scope")
	}
	first, err := AdaptiveRevisitScopeDigest(paths, []string{"/fixture/a/x", "/fixture/a/y", "/fixture/a/x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdaptiveRevisitScopeDigest(paths, []string{"/fixture/a/y", "/fixture/a/x"}, nil)
	if err != nil || first != second {
		t.Fatal(first, second, err)
	}
	bytePath := "/fixture/" + string([]byte{0xff})
	if _, err = AdaptiveRevisitScopeDigest([]string{bytePath}, nil, nil); err != nil {
		t.Fatal("raw byte scope refused", err)
	}
	if _, err = AdaptiveRevisitScopeDigest([]string{bytePath + "a"}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveRevisitBoundsCorruptionAndDefaultIsolation(t *testing.T) {
	s, scope, now := adaptiveFixture(t)
	if _, err := s.ConfigureAdaptiveRevisits(context.Background(), scope.roots, true, "bad", now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if _, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 2, now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if _, err := s.ConfigureAdaptiveRevisits(context.Background(), FairInventoryRoots{}, true, scope.digest, now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(context.Background(), 2, ScanKind, nil, now); err != nil {
		t.Fatal(err)
	}
	if found, err := s.AdaptiveRevisitPending(context.Background(), scope, now); err != nil || found {
		t.Fatal("initial/nonadaptive job qualified", found, err)
	}
	before := countRows(t, s, "SELECT count(*) FROM adaptive_inventory_revisits")
	if _, err := s.ScheduleInventoryRevisit(context.Background(), 2, now, InventoryRevisitInterval); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM adaptive_inventory_revisits") != before {
		t.Fatal("fixed API created tracker")
	}
	for _, damage := range []string{"unchanged_streak=3", "max_now_ns=0", "interval_ns=7", "scope_digest='bad'", "root_generation=-1", "started=2"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec("UPDATE adaptive_inventory_revisits SET " + damage + " WHERE root_id=1"); err != nil {
				t.Fatal(err)
			}
			if _, err = readAdaptiveRevisit(ctx, tx, 1); !errors.Is(err, ErrAdaptiveRevisitCorrupt) {
				t.Fatal("corruption accepted", err)
			}
		})
	}
	if !bytes.Equal(scope.roots.roots[0].path, []byte("/fixture/a")) {
		t.Fatal("scope path changed")
	}
}

func TestAdaptiveRevisitChildDirectoryReplacementAndBoundedOldText(t *testing.T) {
	for _, change := range []string{"child_directory", "large_old_identity"} {
		t.Run(change, func(t *testing.T) {
			s, scope, now := adaptiveFixture(t)
			r := adaptiveWeekly(t, s, scope, now)
			now = r.Due
			j := adaptiveClaimJob(t, s, now)
			adaptiveCommit(t, s, scope, j, []Entry{adaptiveEntry("deep", "directory", 4)}, 40, true, now)
			j = adaptiveClaimJob(t, s, now)
			b := ScanBatch{Identity: "fixture-volume", Generation: 41, Directory: adaptiveEntry("deep", "directory", 4), Entries: []Entry{adaptiveEntry("deep/file", "file", 5)}, Complete: true}
			if change == "child_directory" {
				b.Directory.Inode = "999"
			} else {
				if _, err := s.db.Exec("UPDATE entries SET device=? WHERE root_id=1 AND path=X'646565702f66696c65'", strings.Repeat("7", 1<<20)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.CommitAdaptiveScan(context.Background(), scope, j, b, now); err != nil {
				t.Fatal(err)
			}
			adaptiveDrain(t, s, 1)
			r, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now)
			if err != nil || !r.Changed || r.Interval != InventoryRevisitInterval || (change == "large_old_identity" && !r.Unknown) {
				t.Fatal("missing changed/unknown evidence", r, err)
			}
		})
	}
}

func TestAdaptiveRevisitReaderMigrationAndRestartBinding(t *testing.T) {
	s, dir := queueStore(t)
	ctx := context.Background()
	var incarnation string
	if err := s.db.QueryRow("SELECT token FROM inventory_identity").Scan(&incarnation); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("DROP TABLE adaptive_inventory_revisits; DELETE FROM schema_migrations WHERE version=13; PRAGMA user_version=12"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil || r.schema != 12 {
		t.Fatal(r, err)
	}
	if _, err = r.AdaptiveRevisitStatus(ctx, AdaptiveRevisitScope{}); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if countRows(t, r, "SELECT count(*) FROM sqlite_master WHERE name='adaptive_inventory_revisits'") != 0 {
		t.Fatal("reader migrated")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var preserved string
	if err = w.db.QueryRow("SELECT token FROM inventory_identity").Scan(&preserved); err != nil || preserved != incarnation || w.schema != 13 {
		t.Fatal(preserved, err)
	}
	roots, err := w.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := strings.Repeat("c", 64)
	scope, err := w.ConfigureAdaptiveRevisits(ctx, roots, true, digest, now)
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.FinalizeAdaptiveRevisit(ctx, scope, 1, now)
	if err != nil || !first.Initialized {
		t.Fatal(first, err)
	}
	j := adaptiveClaimJob(t, w, now)
	adaptiveCommit(t, w, scope, j, []Entry{adaptiveEntry("a", "file", 1)}, 1, false, now)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.FinalizeAdaptiveRevisit(ctx, scope, 1, now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal("old-store scope accepted", err)
	}
	roots, err = reopened.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	scope, err = reopened.ConfigureAdaptiveRevisits(ctx, roots, true, digest, now)
	if err != nil {
		t.Fatal(err)
	}
	status, err := reopened.AdaptiveRevisitStatus(ctx, scope)
	if err != nil || status.UnknownEpochs != 1 || status.StableRoots != 0 {
		t.Fatal(status, err)
	}
}

func TestAdaptiveRevisitOversizedBatchAndFaultRetryOverflowRefuseBeforeMutation(t *testing.T) {
	s, scope, now := adaptiveFixture(t)
	j := adaptiveClaimJob(t, s, now)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := readAdaptiveRevisit(ctx, tx, 1)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	tx.Rollback()
	if err = s.CommitAdaptiveScan(ctx, scope, j, ScanBatch{Identity: "fixture-volume", Generation: 1, Directory: adaptiveEntry(".", "directory", 4), Entries: make([]Entry, MaxBatchEntries+1)}, now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if err = s.CommitAdaptiveScan(ctx, scope, j, ScanBatch{Fault: "generated failure"}, time.Unix(0, math.MaxInt64)); !errors.Is(err, ErrAdaptiveRevisitClock) {
		t.Fatal(err)
	}
	if err = s.CommitAdaptiveScan(ctx, scope, j, ScanBatch{Identity: string([]byte{0xff}), Generation: 1, Directory: adaptiveEntry(".", "directory", 4), Complete: true}, now); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal("invalid identity accepted", err)
	}
	var token, status string
	var due int64
	if err = s.db.QueryRow("SELECT lease_token,status,due_at_ns FROM jobs WHERE id=?", j.ID).Scan(&token, &status, &due); err != nil || token != j.Token || status != "running" || due != now.UnixNano() {
		t.Fatal(token, status, due, err)
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	after, err := readAdaptiveRevisit(ctx, tx, 1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("refusal mutated adaptive evidence", before, after, err)
	}
}

func TestAdaptiveRevisitRetainedDetailedScopeRefusesAfterModeChange(t *testing.T) {
	s, scope, now := adaptiveFixture(t)
	j := adaptiveClaimJob(t, s, now)
	adaptiveCommit(t, s, scope, j, []Entry{adaptiveEntry("file", "file", 5)}, 1, true, now)
	adaptiveDrain(t, s, 1)
	compact := true
	if enabled, err := s.ConfigureCompact(context.Background(), &compact); err != nil || !enabled {
		t.Fatal(enabled, err)
	}
	if _, err := s.FinalizeAdaptiveRevisit(context.Background(), scope, 1, now); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal("retained detailed scope crossed storage profile", err)
	}
	if countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
		t.Fatal("mode refusal scheduled source work")
	}
}
