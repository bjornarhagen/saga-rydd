package state

import (
	"bytes"
	"context"
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

func backgroundFixture(t *testing.T, compact bool) (*Store, BackgroundInventoryScope, time.Time) {
	t.Helper()
	s, _ := queueStore(t)
	roots, err := s.ResolveFairInventoryRoots(context.Background(), []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	scope, err := s.ConfigureBackgroundInventoryMode(context.Background(), roots, compact, now)
	if err != nil {
		t.Fatal(err)
	}
	return s, scope, now
}
func backgroundJob(t *testing.T, s *Store) (id, due, claimed int64) {
	t.Helper()
	if err := s.db.QueryRow("SELECT id,due_at_ns,inventory_claimed FROM jobs WHERE root_id=1 AND kind=?", ScanKind).Scan(&id, &due, &claimed); err != nil {
		t.Fatal(err)
	}
	return
}
func backgroundMode(t *testing.T, s *Store) bool {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	value, err := readBackgroundInventoryMode(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func backgroundChange(t *testing.T, s *Store, want bool, now time.Time) (BackgroundInventoryScope, error) {
	t.Helper()
	roots, err := s.ResolveFairInventoryRoots(context.Background(), []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	return s.ConfigureBackgroundInventoryMode(context.Background(), roots, want, now)
}

func TestBackgroundInventoryFixedSchedulingAndModeTransitionPreserveJobs(t *testing.T) {
	ctx := context.Background()
	s, scope, now := backgroundFixture(t, false)
	if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=? WHERE id=1", now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if page, err := s.SeedBackgroundInventoryRevisitPage(ctx, scope, 0, now, InventoryRevisitInterval); err != nil || page.Enqueued != 1 || page.More {
		t.Fatal(page, err)
	}
	id, due, claimed := backgroundJob(t, s)
	if due != now.Add(InventoryRevisitInterval).UnixNano() || claimed != 0 {
		t.Fatal(id, due, claimed)
	}
	compact, err := backgroundChange(t, s, true, now.Add(time.Second))
	if err != nil || !compact.Compact() {
		t.Fatal(compact, err)
	}
	if gotId, gotDue, gotClaim := backgroundJob(t, s); gotId != id || gotDue != due || gotClaim != 0 {
		t.Fatal("mode change replaced future work", gotId, gotDue, gotClaim)
	}
	if _, err = s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryInput) {
		t.Fatal("old scope survived", err)
	}
	if yes, err := s.BackgroundInventoryRevisitPending(ctx, compact, time.Unix(0, due), InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
	if _, err = s.SeedInventoryRevisitPage(ctx, 0, now, InventoryRevisitInterval); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal("old detailed API changed", err)
	}
	if step, err := s.RetireBackgroundInventoryForRoot(ctx, compact, 1); err != nil || step.Eligible || step.Worked {
		t.Fatal(step, err)
	}
	detailed, err := backgroundChange(t, s, false, now.Add(2*time.Second))
	if err != nil || detailed.Compact() {
		t.Fatal(detailed, err)
	}
	if _, err = s.BackgroundInventoryRevisitPending(ctx, compact, time.Unix(0, due), InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryInput) {
		t.Fatal("two transitions restored old scope", err)
	}
	if gotId, gotDue, gotClaim := backgroundJob(t, s); gotId != id || gotDue != due || gotClaim != 0 {
		t.Fatal(gotId, gotDue, gotClaim)
	}
	if again, err := s.ConfigureBackgroundInventoryMode(ctx, detailed.roots, false, now.Add(3*time.Second)); err != nil || again.Compact() {
		t.Fatal(again, err)
	}
	ids := detailed.RootIDs()
	ids[0] = 999
	if reflect.DeepEqual(detailed.RootIDs(), ids) {
		t.Fatal("scope aliases returned root IDs")
	}
}

func TestBackgroundInventoryClaimProvenanceSurvivesReleaseAndRecovery(t *testing.T) {
	for _, fair := range []bool{false, true} {
		t.Run(fmt.Sprint(fair), func(t *testing.T) {
			ctx := context.Background()
			s, scope, now := backgroundFixture(t, false)
			if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), now); err != nil {
				t.Fatal(err)
			}
			var j *Job
			var err error
			if fair {
				if _, err = s.ReserveScanChunk(ctx, now, time.Second, 100); err != nil {
					t.Fatal(err)
				}
				turn, e := s.ClaimFairInventoryTurn(ctx, scope.roots, now, time.Hour, true, now)
				if e != nil || turn == nil {
					t.Fatal(turn, e)
				}
				j = turn.Job
			} else {
				j, err = s.ClaimJob(ctx, []string{ScanKind}, now, time.Hour)
				if err != nil || j == nil {
					t.Fatal(j, err)
				}
			}
			if _, _, claimed := backgroundJob(t, s); claimed != 1 {
				t.Fatal(claimed)
			}
			if err = s.FinishJob(ctx, *j, false, nil, now.Add(time.Hour), ""); err != nil {
				t.Fatal(err)
			}
			var attempts int
			if err = s.db.QueryRow("SELECT attempts FROM jobs WHERE id=?", j.ID).Scan(&attempts); err != nil || attempts != 0 {
				t.Fatal(attempts, err)
			}
			if _, err = backgroundChange(t, s, true, now.Add(time.Second)); !errors.Is(err, ErrBackgroundInventoryModePending) {
				t.Fatal("released zero-attempt job looked untouched", err)
			}
			same, err := backgroundChange(t, s, false, now.Add(time.Second))
			if err != nil || same.Compact() {
				t.Fatal(same, err)
			}
			j, err = s.ClaimJob(ctx, []string{ScanKind}, now.Add(time.Hour), time.Hour)
			if err != nil || j == nil {
				t.Fatal(j, err)
			}
			if n, err := s.RecoverJobs(ctx, now.Add(time.Hour)); err != nil || n != 1 {
				t.Fatal(n, err)
			}
			if _, _, claimed := backgroundJob(t, s); claimed != 1 {
				t.Fatal("recovery cleared start proof", claimed)
			}
			if _, err = backgroundChange(t, s, true, now.Add(2*time.Hour)); !errors.Is(err, ErrBackgroundInventoryModePending) {
				t.Fatal(err)
			}
		})
	}
}

func TestBackgroundInventoryModeChangeRejectsUncertainScopeAndMaintenance(t *testing.T) {
	cases := map[string]string{
		"child":               "INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed) VALUES(1,'inventory',X'6368696c64',1,0)",
		"outside":             "INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed) VALUES(2,'inventory',X'2e',1,0)",
		"unknown":             "INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'inventory',X'2e',1)",
		"error":               "INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed,last_error) VALUES(1,'inventory',X'2e',1,0,'offline')",
		"cursor":              "INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed,cursor) VALUES(1,'inventory',X'2e',1,0,X'01')",
		"lease":               "INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed,lease_token) VALUES(1,'inventory',X'2e',1,0,'held')",
		"compact maintenance": "INSERT INTO compact_retirement VALUES(1,X'2e',1)",
		"reconciliation":      "INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(1,X'2e',1)",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			s, scope, now := backgroundFixture(t, false)
			if _, err := s.db.Exec(q); err != nil {
				t.Fatal(err)
			}
			before := countRows(t, s, "SELECT count(*) FROM jobs")
			if _, err := backgroundChange(t, s, true, now); !errors.Is(err, ErrBackgroundInventoryModePending) {
				t.Fatal(err)
			}
			if backgroundMode(t, s) || countRows(t, s, "SELECT count(*) FROM jobs") != before {
				t.Fatal("refusal mutated work")
			}
			if _, err := s.ConfigureBackgroundInventoryMode(context.Background(), scope.roots, false, now); err != nil {
				t.Fatal("same-mode restart refused its saved queue", err)
			}
		})
	}
}

func TestBackgroundInventoryRawScopeBoundsTypesAndStaleBindings(t *testing.T) {
	ctx := context.Background()
	s, scope, now := backgroundFixture(t, true)
	for _, root := range []int64{0, 2, 999} {
		if _, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, root, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryInput) && !errors.Is(err, ErrInventoryRevisitInput) {
			t.Fatal(root, err)
		}
	}
	if _, err := s.db.Exec("UPDATE roots SET enabled=0 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireBackgroundInventoryForRoot(ctx, scope, 1); !errors.Is(err, ErrBackgroundInventoryInput) {
		t.Fatal(err)
	}
	for _, q := range []string{"INSERT INTO settings VALUES('inventory.compact',X'3031') ON CONFLICT(key) DO UPDATE SET value=excluded.value", "INSERT INTO settings VALUES('inventory.compact','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value", "UPDATE settings SET value=? WHERE key='inventory.background.mode_epoch_v1'"} {
		s, scope, now = backgroundFixture(t, false)
		var err error
		if strings.Contains(q, "?") {
			_, err = s.db.Exec(q, []byte(strconvFormatMax()))
		} else {
			_, err = s.db.Exec(q)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = backgroundChange(t, s, true, now); !errors.Is(err, ErrBackgroundInventoryCorrupt) {
			t.Fatal(q, err)
		}
	}
	for _, q := range []string{
		"UPDATE jobs SET inventory_claimed=X'30'",
		"UPDATE jobs SET path=zeroblob(2097152)",
		"UPDATE jobs SET last_error=CAST(zeroblob(2097152) AS TEXT)",
		"UPDATE jobs SET cursor=zeroblob(2097152)",
	} {
		s, _, now = backgroundFixture(t, false)
		if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), now); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON; " + q); err != nil {
			t.Fatal(err)
		}
		if _, err := backgroundChange(t, s, true, now); !errors.Is(err, ErrBackgroundInventoryCorrupt) && !errors.Is(err, ErrBackgroundInventoryModePending) {
			t.Fatal(q, err)
		}
	}
}
func strconvFormatMax() string { return fmt.Sprint(int64(math.MaxInt64)) }

func TestBackgroundInventoryManualSwitchInvalidatesSavedScopes(t *testing.T) {
	s, scope, now := backgroundFixture(t, false)
	ctx := context.Background()
	for _, want := range []bool{true, false} {
		if got, err := s.ConfigureCompact(ctx, &want); err != nil || got != want {
			t.Fatal(got, err)
		}
	}
	if _, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryInput) {
		t.Fatal("manual switch revived old epoch", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := backgroundChangeWithContext(s, canceled, scope.roots, true, now); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func backgroundChangeWithContext(s *Store, ctx context.Context, roots FairInventoryRoots, compact bool, now time.Time) (BackgroundInventoryScope, error) {
	return s.ConfigureBackgroundInventoryMode(ctx, roots, compact, now)
}

func TestBackgroundInventoryCompactMaintenanceMatchesDetailedScope(t *testing.T) {
	s, scope, now := backgroundFixture(t, true)
	ctx := context.Background()
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), now); err != nil {
		t.Fatal(err)
	}
	root := adaptiveClaimJob(t, s, now)
	tree := adaptiveEntry("node_modules", "directory", 4)
	if err := s.CommitScan(ctx, root, ScanBatch{Identity: "fixture-volume", Generation: 1, Directory: adaptiveEntry(".", "directory", 4), Entries: []Entry{tree, adaptiveEntry("plain", "file", 5)}, Complete: true}); err != nil {
		t.Fatal(err)
	}
	child := adaptiveClaimJob(t, s, now.Add(time.Second))
	files := []Entry{adaptiveEntry("node_modules/a", "file", 11), adaptiveEntry("node_modules/b", "file", 13)}
	files[0].Inode = "201"
	files[1].Inode = "202"
	if err := s.CommitScan(ctx, child, ScanBatch{Identity: "fixture-volume", Generation: 2, Directory: tree, Entries: files, Complete: true}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		step, err := s.RetireBackgroundInventoryForRoot(ctx, scope, 1)
		if err != nil {
			t.Fatal(step, err)
		}
		if !step.Remaining {
			break
		}
		if !step.Worked || i == 999 {
			t.Fatal("compact maintenance stopped", step)
		}
	}
	report, err := s.MeasureDirectory(ctx, "/fixture/a")
	if err != nil || report.LogicalBytes == nil || *report.LogicalBytes != 29 || report.AllocatedBytes == nil || *report.AllocatedBytes != 29 {
		t.Fatal(report, err)
	}
	if countRows(t, s, "SELECT count(*) FROM entries WHERE kind='file'") != 1 || countRows(t, s, "SELECT count(*) FROM compact_inodes") != 2 {
		t.Fatal("compact files leaked into detailed inventory")
	}
	if yes, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now.Add(2*time.Second), InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
}

func TestBackgroundInventoryMigrationLegacyReadersAndRollback(t *testing.T) {
	for _, version := range []int{12, 13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, filepath.Join(dir, Filename), false)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := s.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < version; i++ {
				if _, err = tx.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec("INSERT INTO schema_migrations VALUES(?,?,1)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = tx.Exec(fmt.Sprintf("PRAGMA application_id=0x52594444; PRAGMA user_version=%d; INSERT INTO roots(path) VALUES(X'2f666978747572652f61'); INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'inventory',X'2e',1); INSERT INTO settings VALUES('preserve',X'00ff')", version)); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil || r.schema != version {
				t.Fatal(r, err)
			}
			if countRows(t, r, "SELECT count(*) FROM pragma_table_info('jobs') WHERE name='inventory_claimed'") != 0 {
				t.Fatal("reader migrated")
			}
			r.Close()
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var preserved []byte
			if err = w.db.QueryRow("SELECT value FROM settings WHERE key='preserve'").Scan(&preserved); err != nil || !bytes.Equal(preserved, []byte{0, 255}) {
				t.Fatal(preserved, err)
			}
			if _, _, claimed := backgroundJob(t, w); claimed != 1 {
				t.Fatal("legacy work invented untouched provenance", claimed)
			}
			if _, err = backgroundChange(t, w, true, time.Now()); !errors.Is(err, ErrBackgroundInventoryModePending) {
				t.Fatal(err)
			}
		})
	}
	// DDL failure leaves the original schema/ledger and private context intact.
	dir := privateDir(t)
	if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := connect(context.Background(), filepath.Join(dir, Filename), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 13; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,1)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=13; ALTER TABLE jobs ADD COLUMN inventory_claimed INTEGER;"); err != nil {
		t.Fatal(err)
	}
	if err = s.migrate(context.Background()); err == nil {
		t.Fatal("conflicting migration accepted")
	}
	if countRows(t, s, "PRAGMA user_version") != 13 || countRows(t, s, "SELECT count(*) FROM schema_migrations") != 13 {
		t.Fatal("failed migration partially committed")
	}
}

func TestBackgroundInventoryModeBoundCountsAllRawSourceJobs(t *testing.T) {
	s, _ := queueStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	paths := make([]string, 33)
	for i := range paths {
		paths[i] = fmt.Sprintf("/fixture/root-%02d", i)
	}
	if err := s.SyncRoots(ctx, paths); err != nil {
		t.Fatal(err)
	}
	roots, err := s.ResolveFairInventoryRoots(ctx, paths[:32])
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		var id int64
		if err = s.db.QueryRow("SELECT id FROM roots WHERE path=?", []byte(path)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if err = s.EnqueueJob(ctx, id, ScanKind, []byte("."), now.Add(24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.ConfigureBackgroundInventoryMode(ctx, roots, true, now); !errors.Is(err, ErrBackgroundInventoryModePending) {
		t.Fatal("out-of-scope lookahead was omitted/refilled", err)
	}
	if countRows(t, s, "SELECT count(*) FROM jobs") != 33 || backgroundMode(t, s) {
		t.Fatal("raw bound refusal changed mode/work")
	}
	// Disabled out-of-scope jobs also consume the raw bound and remain saved.
	if _, err = s.db.Exec("UPDATE roots SET enabled=0 WHERE path=?", []byte(paths[32])); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ConfigureBackgroundInventoryMode(ctx, roots, true, now); !errors.Is(err, ErrBackgroundInventoryModePending) {
		t.Fatal(err)
	}
}

func TestBackgroundInventoryCompactResetsDetailedLearningWithoutTakingScheduleAuthority(t *testing.T) {
	ctx := context.Background()
	s, adaptive, now := adaptiveFixture(t)
	file := adaptiveEntry("deep/file", "file", 9)
	result := adaptivePass(t, s, adaptive, now, 1, file)
	result = adaptivePass(t, s, adaptive, result.Due, 10, file)
	result = adaptivePass(t, s, adaptive, result.Due, 20, file)
	if result.Interval != AdaptiveStableRevisitInterval || result.UnchangedStreak != 2 {
		t.Fatal(result)
	}
	id, due, claimed := backgroundJob(t, s)
	if claimed != 0 {
		t.Fatal("fresh weekly job already claimed", claimed)
	}
	compact, err := backgroundChange(t, s, true, result.Due.Add(-AdaptiveStableRevisitInterval).Add(time.Second))
	if err != nil || !compact.Compact() {
		t.Fatal(compact, err)
	}
	if gotId, gotDue, _ := backgroundJob(t, s); gotId != id || gotDue != due {
		t.Fatal("storage transition changed scheduling", gotId, gotDue)
	}
	if _, err = s.FinalizeAdaptiveRevisit(ctx, adaptive, 1, result.Due); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal("compact earned detailed adaptive evidence", err)
	}
	disabled, err := s.ConfigureAdaptiveRevisits(ctx, compact.roots, false, strings.Repeat("d", 64), result.Due.Add(-AdaptiveStableRevisitInterval).Add(2*time.Second))
	if err != nil {
		t.Fatal("disabled policy could not clean up exact prior weekly scheduling", err)
	}
	want := result.Due.Add(-AdaptiveStableRevisitInterval + InventoryRevisitInterval).UnixNano()
	if gotId, gotDue, _ := backgroundJob(t, s); gotId != id || gotDue != want {
		t.Fatal("disabled cleanup did not retain exact job/daily baseline", gotId, gotDue, want)
	}
	if _, err = s.FinalizeAdaptiveRevisit(ctx, disabled, 1, result.Due); !errors.Is(err, ErrAdaptiveRevisitInput) {
		t.Fatal(err)
	}
	if _, err = s.ConfigureAdaptiveRevisits(ctx, compact.roots, true, strings.Repeat("e", 64), result.Due); !errors.Is(err, ErrInventoryRevisitMode) {
		t.Fatal("compact admitted detailed learning", err)
	}
	var unknown bool
	var streak int
	if err = s.db.QueryRow("SELECT unknown,unchanged_streak FROM adaptive_inventory_revisits WHERE root_id=1").Scan(&unknown, &streak); err != nil || !unknown || streak != 0 {
		t.Fatal(unknown, streak, err)
	}
}

func TestBackgroundInventoryReopenPreservesModeProvenanceAndOtherSavedState(t *testing.T) {
	ctx := context.Background()
	s, dir := queueStore(t)
	now := time.Now().UTC()
	roots, err := s.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := s.ConfigureBackgroundInventoryMode(ctx, roots, true, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("INSERT INTO settings VALUES('fixture-preserve',X'00ff'); INSERT INTO daily_budgets VALUES('2026-10-09',71,19)"); err != nil {
		t.Fatal(err)
	}
	var incarnation string
	if err = s.db.QueryRow("SELECT token FROM inventory_identity").Scan(&incarnation); err != nil {
		t.Fatal(err)
	}
	if yes, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
	id, due, claimed := backgroundJob(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.schema != 14 {
		t.Fatal(r.schema)
	}
	if n := countRows(t, r, "SELECT count(*) FROM settings WHERE key='fixture-preserve' AND value=X'00ff'"); n != 1 {
		t.Fatal(n)
	}
	r.Close()
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = w.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryInput) {
		t.Fatal("foreign open-store scope accepted", err)
	}
	roots, err = w.ResolveFairInventoryRoots(ctx, []string{"/fixture/a"})
	if err != nil {
		t.Fatal(err)
	}
	same, err := w.ConfigureBackgroundInventoryMode(ctx, roots, true, now.Add(time.Second))
	if err != nil || !same.Compact() {
		t.Fatal(same, err)
	}
	if gotId, gotDue, gotClaim := backgroundJob(t, w); gotId != id || gotDue != due || gotClaim != claimed {
		t.Fatal(gotId, gotDue, gotClaim)
	}
	var gotIncarnation string
	var content, metadata int
	if err = w.db.QueryRow("SELECT token FROM inventory_identity").Scan(&gotIncarnation); err != nil || gotIncarnation != incarnation {
		t.Fatal(gotIncarnation, err)
	}
	if err = w.db.QueryRow("SELECT content_bytes,metadata_ops FROM daily_budgets WHERE day='2026-10-09'").Scan(&content, &metadata); err != nil || content != 71 || metadata != 19 {
		t.Fatal(content, metadata, err)
	}
	if countRows(t, w, "SELECT count(*) FROM settings WHERE key='fixture-preserve' AND value=X'00ff'") != 1 {
		t.Fatal("unrelated saved state changed")
	}
}

func TestBackgroundInventoryWaitMatcherRequiresNeverClaimedProvenance(t *testing.T) {
	s, scope, now := backgroundFixture(t, false)
	ctx := context.Background()
	if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=? WHERE id=1", now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if yes, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
	due := now.Add(InventoryRevisitInterval)
	if yes, err := s.BackgroundInventoryRevisitPending(ctx, scope, due, InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
	j, err := s.ClaimJob(ctx, []string{ScanKind}, due, time.Hour)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if err = s.FinishJob(ctx, *j, false, nil, due, ""); err != nil {
		t.Fatal(err)
	}
	if yes, err := s.BackgroundInventoryRevisitPending(ctx, scope, due, InventoryRevisitInterval); err != nil || yes {
		t.Fatal("released root was presented as a never-started revisit", yes, err)
	}
	// The prior detailed API keeps its established historical shape matcher.
	if yes, err := s.InventoryRevisitPending(ctx, due, InventoryRevisitInterval); err != nil || !yes {
		t.Fatal(yes, err)
	}
}

func TestBackgroundInventoryScopedScalarCorruptionIsBoundedAndRefused(t *testing.T) {
	ctx := context.Background()
	for _, mutation := range []string{
		"UPDATE roots SET last_scan_ns=CAST(last_scan_ns AS BLOB) WHERE id=1",
		"UPDATE roots SET last_scan_ns=zeroblob(2097152) WHERE id=1",
		"UPDATE roots SET last_scan_ns=CAST(zeroblob(2097152) AS TEXT) WHERE id=1",
	} {
		t.Run(mutation, func(t *testing.T) {
			s, scope, now := backgroundFixture(t, true)
			if _, err := s.db.Exec("UPDATE roots SET last_scan_ns=? WHERE id=1", now.UnixNano()); err != nil {
				t.Fatal(err)
			}
			if yes, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); err != nil || !yes {
				t.Fatal(yes, err)
			}
			if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON; " + mutation); err != nil {
				t.Fatal(err)
			}
			if yes, err := s.BackgroundInventoryRevisitPending(ctx, scope, now.Add(InventoryRevisitInterval), InventoryRevisitInterval); yes || !errors.Is(err, ErrBackgroundInventoryCorrupt) {
				t.Fatal("invalid saved listing qualified a revisit", yes, err)
			}
			if _, err := s.ScheduleBackgroundInventoryRevisit(ctx, scope, 1, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryCorrupt) {
				t.Fatal(err)
			}
			if _, err := s.SeedBackgroundInventoryRevisitPage(ctx, scope, 0, now, InventoryRevisitInterval); !errors.Is(err, ErrBackgroundInventoryCorrupt) {
				t.Fatal(err)
			}
		})
	}
	for _, mutation := range []string{
		"UPDATE jobs SET attempts=zeroblob(2097152)",
		"UPDATE jobs SET root_id=zeroblob(2097152)",
		"UPDATE roots SET enabled=zeroblob(2097152) WHERE id=1",
	} {
		t.Run(mutation, func(t *testing.T) {
			s, scope, now := backgroundFixture(t, false)
			if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), now); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON; " + mutation); err != nil {
				t.Fatal(err)
			}
			if yes, err := s.BackgroundInventoryRevisitPending(ctx, scope, now, InventoryRevisitInterval); yes || (!errors.Is(err, ErrBackgroundInventoryCorrupt) && !errors.Is(err, ErrBackgroundInventoryInput)) {
				t.Fatal(yes, err)
			}
		})
	}
}

func TestBackgroundInventoryResolverRejectsCorruptEnabledScalar(t *testing.T) {
	for _, value := range []string{"X'31'", "zeroblob(2097152)", "CAST(zeroblob(2097152) AS TEXT)"} {
		t.Run(value, func(t *testing.T) {
			s, _, _ := backgroundFixture(t, false)
			if _, err := s.db.Exec("PRAGMA ignore_check_constraints=ON; UPDATE roots SET enabled=" + value + " WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ResolveFairInventoryRoots(context.Background(), []string{"/fixture/a"}); !errors.Is(err, ErrFairInventoryCorrupt) {
				t.Fatal("invalid enabled scalar admitted configured roots", err)
			}
		})
	}
}
