package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fairInventoryFixture(t *testing.T) (*Store, string, FairInventoryRoots) {
	t.Helper()
	s, dir := queueStore(t)
	paths := []string{"/fixture/a", "/fixture/b", "/fixture/c"}
	if err := s.SyncRoots(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	scope, err := s.ResolveFairInventoryRoots(context.Background(), paths)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir, scope
}

func fairInventoryClaim(t *testing.T, s *Store, scope FairInventoryRoots, now time.Time, allowSource bool) *FairInventoryTurn {
	t.Helper()
	if _, err := s.ReserveScanChunk(context.Background(), now, time.Millisecond, 100000); err != nil {
		t.Fatal(err)
	}
	turn, err := s.ClaimFairInventoryTurn(context.Background(), scope, now, time.Minute, allowSource, now)
	if err != nil || turn == nil {
		t.Fatal(turn, err)
	}
	return turn
}

func fairInventoryMaintenanceFixture(t *testing.T, s *Store, root int64) {
	t.Helper()
	if _, err := s.db.Exec("INSERT INTO allocation_revisions(root_id,revision,scan_revision) VALUES(?,1,1); INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(?,X'2e',1)", root, root); err != nil {
		t.Fatal(err)
	}
}

func TestFairInventoryRootTurnsSourceAndMaintenance(t *testing.T) {
	ctx := context.Background()
	s, _, scope := fairInventoryFixture(t)
	now := time.Unix(1700000000, 0)
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("."), time.Unix(0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 1, ScanKind, []byte("child"), now); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 2, ScanKind, []byte("."), now); err != nil {
		t.Fatal(err)
	}
	fairInventoryMaintenanceFixture(t, s, 3)
	for i, want := range []int64{1, 2, 3, 1, 2, 3} {
		at := now.Add(time.Duration(i) * time.Millisecond)
		turn := fairInventoryClaim(t, s, scope, at, true)
		if turn.RootID != want {
			t.Fatal("root did not rotate", i, turn)
		}
		if want == 3 {
			if turn.Kind != FairInventoryMaintenance || turn.Job != nil {
				t.Fatal(turn)
			}
			continue
		}
		if turn.Kind != FairInventorySource || turn.Job == nil || string(turn.Job.Path) != "." {
			t.Fatal("child displaced open parent", turn)
		}
		if err := s.FinishJob(ctx, *turn.Job, false, []byte("committed"), time.Unix(0, 1), ""); err != nil {
			t.Fatal(err)
		}
	}
	peek, err := s.NextFairInventoryTurn(ctx, scope, now.Add(time.Second), false)
	if err != nil || peek.Turn == nil || peek.Turn.RootID != 3 || peek.Turn.Kind != FairInventoryMaintenance || !peek.SourceBlocked || peek.NextSourceDue.UnixNano() != 1 {
		t.Fatal("quota-blocked source starved saved-only root", peek, err)
	}
	turn := fairInventoryClaim(t, s, scope, now.Add(time.Second), false)
	if turn.RootID != 3 {
		t.Fatal(turn)
	}
	// Delayed/future source work remains a maintenance blocker for its root.
	if err = s.EnqueueJob(ctx, 3, ScanKind, []byte("."), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	peek, err = s.NextFairInventoryTurn(ctx, scope, now.Add(2*time.Second), false)
	if err != nil || peek.Turn != nil || !peek.SourceBlocked {
		t.Fatal("future job removed to manufacture maintenance eligibility", peek, err)
	}
}

func TestFairInventoryCursorRestartRemovalAndFences(t *testing.T) {
	ctx := context.Background()
	s, dir, scope := fairInventoryFixture(t)
	now := time.Unix(1700000000, 0)
	for _, root := range []int64{1, 2, 3} {
		if err := s.EnqueueJob(ctx, root, ScanKind, []byte("."), time.Unix(0, 1)); err != nil {
			t.Fatal(err)
		}
	}
	old := fairInventoryClaim(t, s, scope, now, true)
	if old.RootID != 1 {
		t.Fatal(old)
	}
	s.Close()
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.RecoverJobs(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.NextFairInventoryTurn(ctx, scope, now, true); !errors.Is(err, ErrFairInventoryInput) {
		t.Fatal("old store scope reused", err)
	}
	scope, err = s.ResolveFairInventoryRoots(ctx, []string{"/fixture/a", "/fixture/b", "/fixture/c"})
	if err != nil {
		t.Fatal(err)
	}
	fresh := fairInventoryClaim(t, s, scope, now.Add(time.Millisecond), true)
	if fresh.RootID != 2 {
		t.Fatal("restart reset fair rotation", fresh)
	}
	if err = s.FinishJob(ctx, *old.Job, true, nil, now, ""); !errors.Is(err, ErrStaleLease) {
		t.Fatal("old attempt survived recovery", err)
	}
	if err = s.FinishJob(ctx, *fresh.Job, false, fresh.Job.Cursor, time.Unix(0, 1), ""); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(ctx, []string{"/fixture/a", "/fixture/c"}); err != nil {
		t.Fatal(err)
	}
	peek, err := s.NextFairInventoryTurn(ctx, scope, now.Add(2*time.Millisecond), true)
	if err != nil || peek.Turn.RootID != 3 || len(peek.DormantRoots) != 1 || string(peek.DormantRoots[0]) != "/fixture/b" {
		t.Fatal("disabled root retained a turn/stream", peek, err)
	}
	if _, err = s.db.Exec("UPDATE settings SET value=? WHERE key=?", []byte("1:9223372036854775807:"+fmt.Sprint(now.UnixNano())), fairInventoryCursorKey); err != nil {
		t.Fatal(err)
	}
	peek, err = s.NextFairInventoryTurn(ctx, scope, now.Add(2*time.Millisecond), true)
	if err != nil || peek.Turn.RootID != 1 {
		t.Fatal("removed cursor root prevented wrap", peek, err)
	}
}

func TestFairInventoryReceiptSingleUseExpiryRollbackAndNoReadMutation(t *testing.T) {
	ctx := context.Background()
	s, _, scope := fairInventoryFixture(t)
	now := time.Unix(1700000000, 0)
	fairInventoryMaintenanceFixture(t, s, 1)
	if _, err := s.ClaimFairInventoryTurn(ctx, scope, now, time.Minute, true, now); !errors.Is(err, ErrFairInventoryReceipt) {
		t.Fatal("missing dispatch accepted", err)
	}
	if _, err := s.ReserveScanChunk(ctx, now, time.Second, 100); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{now.Add(FairInventoryClaimWindow), now.Add(time.Hour)} {
		if _, err := s.ClaimFairInventoryTurn(ctx, scope, at, time.Minute, true, now); !errors.Is(err, ErrFairInventoryReceipt) {
			t.Fatal("ancient unconsumed receipt accepted", at, err)
		}
	}
	if _, err := s.ClaimFairInventoryTurn(ctx, scope, now.Add(-time.Nanosecond), time.Minute, true, now); !errors.Is(err, ErrFairInventoryInput) {
		t.Fatal("rollback accepted", err)
	}
	if _, err := s.ClaimFairInventoryTurn(ctx, scope, now.Add(10*time.Millisecond), time.Millisecond, true, now); !errors.Is(err, ErrFairInventoryReceipt) {
		t.Fatal("expired lease window accepted", err)
	}
	turn, err := s.ClaimFairInventoryTurn(ctx, scope, now, time.Minute, true, now)
	if err != nil || turn == nil || turn.Kind != FairInventoryMaintenance {
		t.Fatal(turn, err)
	}
	if _, err = s.ClaimFairInventoryTurn(ctx, scope, now, time.Minute, true, now); !errors.Is(err, ErrFairInventoryReceipt) {
		t.Fatal("dispatch reused", err)
	}
	var before []byte
	if err = s.db.QueryRow("SELECT value FROM settings WHERE key=?", fairInventoryCursorKey).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = s.NextFairInventoryTurn(ctx, scope, now.Add(time.Second), false); err != nil {
			t.Fatal(err)
		}
	}
	var after []byte
	if err = s.db.QueryRow("SELECT value FROM settings WHERE key=?", fairInventoryCursorKey).Scan(&after); err != nil || !bytes.Equal(before, after) {
		t.Fatal("peek advanced durable rotation", err)
	}
	for _, raw := range [][]byte{[]byte(""), []byte("1:01:1"), []byte("1:-1:1"), []byte("1:1:0"), []byte("2:1:1"), []byte("1:1:9223372036854775808")} {
		if _, err = s.db.Exec("UPDATE settings SET value=? WHERE key=?", raw, fairInventoryCursorKey); err != nil {
			t.Fatal(err)
		}
		if _, err = s.NextFairInventoryTurn(ctx, scope, now, true); !errors.Is(err, ErrFairInventoryCorrupt) {
			t.Fatal("corrupt cursor accepted", string(raw), err)
		}
	}
	if _, err = s.db.Exec("UPDATE settings SET value='1:1:1' WHERE key=?", fairInventoryCursorKey); err != nil {
		t.Fatal(err)
	}
	if _, err = s.NextFairInventoryTurn(ctx, scope, now, true); !errors.Is(err, ErrFairInventoryCorrupt) {
		t.Fatal("text cursor accepted", err)
	}
}

func TestFairInventoryAdmissionAndCacheEligibilitySeeks(t *testing.T) {
	s, _, scope := fairInventoryFixture(t)
	ctx := context.Background()
	paths := make([]string, 33)
	for i := range paths {
		paths[i] = fmt.Sprintf("/fixture/%d", i)
	}
	for _, bad := range [][]string{nil, paths, {"relative"}, {"/fixture", "/fixture"}, {"/fixture/../x"}, {strings.Repeat("/x", 2049)}} {
		if ValidateFairInventoryPaths(bad) == nil {
			t.Fatal("bad admission", bad)
		}
	}
	if _, err := s.ResolveFairInventoryRoots(ctx, []string{"/not-configured"}); err == nil {
		t.Fatal("unselected root admitted")
	}
	if _, err := s.db.Exec(`INSERT INTO allocation_revisions(root_id,revision,scan_revision) VALUES(1,4,4);
 WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000)
 INSERT INTO allocation_cache(root_id,path,revision,phase) SELECT 1,CAST(x AS BLOB),4,'done' FROM n`); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"", "UPDATE allocation_cache SET phase='cleanup' WHERE path=X'30'", "UPDATE allocation_cache SET phase='done',revision=3 WHERE path=X'30'", "UPDATE allocation_cache SET revision=5 WHERE path=X'30'", "DELETE FROM allocation_revisions WHERE root_id=1"} {
		if change != "" {
			if _, err := s.db.Exec(change); err != nil {
				t.Fatal(err)
			}
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		var expected bool
		e1 := inventoryMaintenanceRemaining(ctx, tx, 1, &expected)
		actual, e2 := fairInventoryMaintenanceRemaining(ctx, tx, 1)
		tx.Rollback()
		if (e1 == nil) != (e2 == nil) || (e1 == nil && expected != actual) {
			t.Fatal("indexed predicate changed maintenance eligibility", change, expected, actual, e1, e2)
		}
	}
	queries := []struct{ query, index string }{
		{`SELECT id FROM jobs INDEXED BY jobs_inventory_root_due WHERE root_id=1 AND kind='inventory' AND status='pending' ORDER BY due_at_ns,id LIMIT 1`, "jobs_inventory_root_due"},
		{`SELECT 1 FROM allocation_cache INDEXED BY allocation_cache_pending_root WHERE root_id=1 AND phase!='done' LIMIT 1`, "allocation_cache_pending_root"},
		{`SELECT 1 FROM allocation_cache INDEXED BY allocation_cache_root_revision WHERE root_id=1 AND revision<(SELECT revision FROM allocation_revisions WHERE root_id=1) LIMIT 1`, "allocation_cache_root_revision"},
	}
	for _, check := range queries {
		rows, err := s.db.Query("EXPLAIN QUERY PLAN " + check.query)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(detail, "SEARCH") && strings.Contains(detail, check.index) {
				found = true
			}
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !found {
			t.Fatal("eligibility query did not use bounded root seek", check.index)
		}
	}
	if _, err := s.NextFairInventoryTurn(ctx, scope, time.Now(), true); !errors.Is(err, ErrInventoryRetirementCorrupt) {
		t.Fatal("orphan revision was silently ignored", err)
	}
}

func TestFairInventorySchema10SavedReaderAndMigration(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := connect(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=10; INSERT INTO roots(path) VALUES(X'2f66697874757265'); INSERT INTO settings VALUES('preserve',X'00ff')"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.MetadataBudget(ctx, time.Now(), 100); err != nil {
		t.Fatal(err)
	}
	r.Close()
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("schema10 reader mutated storage", err)
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var kept []byte
	if err = w.db.QueryRow("SELECT value FROM settings WHERE key='preserve'").Scan(&kept); err != nil || !bytes.Equal(kept, []byte{0, 255}) || w.schema != schemaVersion {
		t.Fatal("additive migration lost history", kept, w.schema, err)
	}
	if _, err = w.ResolveFairInventoryRoots(ctx, []string{"/fixture"}); err != nil {
		t.Fatal(err)
	}
}
