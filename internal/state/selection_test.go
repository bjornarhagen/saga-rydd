package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func savedSelectionFixture(t *testing.T) (*Store, string) {
	t.Helper()
	s := directoryFixture(t)
	for _, kind := range []string{"directory", "file"} {
		path := "a/node_modules"
		if kind == "file" {
			path = "a/package.json"
		}
		if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,X'61',?,0,0,1,2,'device',?,2,10)`, []byte(path), kind, path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,CAST('a/node_modules/file' AS BLOB),CAST('a/node_modules' AS BLOB),'file',3,4096,1,2,'device','file',2,10)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO directories(root_id,path,generation,complete,checked_at_ns)
 VALUES(1,CAST('a/node_modules' AS BLOB),2,0,10)`); err != nil {
		t.Fatal(err)
	}
	r, err := s.NodeModulesFindings(context.Background(), "", 90)
	if err != nil || len(r.Findings) != 1 {
		t.Fatal(r, err)
	}
	return s, r.Findings[0].ID
}

func TestSelectionSnapshotConcurrentWriter(t *testing.T) {
	w, id := savedSelectionFixture(t)
	ctx := context.Background()
	r, err := OpenReader(ctx, filepath.Dir(w.path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	before, err := r.SnapshotSelection(ctx, []string{id}, 90)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.InventoryID) != 64 || len(before.Roots) != 1 || len(before.Targets) != 1 || before.Targets[0].Manifest.Inode != "a/package.json" || before.Targets[0].Target.ChangedNS != 2 || before.Evidence.Findings[0].Measurement.Status != "partial" {
		t.Fatal(before)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var token string
	if err = tx.QueryRow("SELECT token FROM inventory_identity").Scan(&token); err != nil {
		t.Fatal(err)
	}
	// The active inventory writer can commit while plan capture owns its snapshot.
	if _, err = w.db.Exec("UPDATE entries SET inode='replacement',ctime_ns=999 WHERE path IN (X'612f6e6f64655f6d6f64756c6573',X'612f7061636b6167652e6a736f6e'); UPDATE entries SET size=99999 WHERE kind='file'; UPDATE roots SET volume_id='changed-root'"); err != nil {
		t.Fatal(err)
	}
	frozen, err := r.snapshotSelection(ctx, []string{id}, 90, tx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Roots, frozen.Roots) || !reflect.DeepEqual(before.Targets, frozen.Targets) || frozen.Evidence.Findings[0].Inode != before.Evidence.Findings[0].Inode {
		t.Fatal("mixed evidence snapshots", frozen)
	}
	if before.Evidence.Findings[0].Measurement.LogicalBytes == nil || *before.Evidence.Findings[0].Measurement.LogicalBytes != 3 || !reflect.DeepEqual(before.Evidence.Findings[0].Measurement.LogicalBytes, frozen.Evidence.Findings[0].Measurement.LogicalBytes) {
		t.Fatal("measurement crossed snapshots", frozen)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := r.SnapshotSelection(ctx, []string{id}, 90)
	if err != nil || after.Targets[0].Target.Inode != "replacement" || after.Roots[0].Fingerprint != "changed-root" {
		t.Fatal(after, err)
	}
	if _, err = r.SnapshotSelection(ctx, []string{id, id}, 90); !errors.Is(err, ErrFindingSelection) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = r.SnapshotSelection(canceled, []string{id}, 90); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = w.db.Exec("DELETE FROM inventory_identity"); err == nil {
		t.Fatal("identity deleted")
	}
	if _, err = w.db.Exec("UPDATE inventory_identity SET token=lower(hex(randomblob(32)))"); err == nil {
		t.Fatal("identity changed")
	}
}

func TestInventoryIdentityMigration(t *testing.T) {
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
	for i := 0; i < 8; i++ {
		if _, err = s.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=8"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal("schema 8 reader", err)
	}
	if _, err = r.SnapshotSelection(ctx, []string{"node-modules-v1:1:2"}, 90); !errors.Is(err, ErrPlanSchema) {
		t.Fatal(err)
	}
	r.Close()
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	var first string
	if err = w.db.QueryRow("SELECT token FROM inventory_identity").Scan(&first); err != nil || len(first) != 64 {
		t.Fatal(first, err)
	}
	w.Close()
	w, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	var second string
	if err = w.db.QueryRow("SELECT token FROM inventory_identity").Scan(&second); err != nil || first != second {
		t.Fatal(second, err)
	}
	w.Close()
	fresh, err := OpenWriter(ctx, privateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err = fresh.db.QueryRow("SELECT token FROM inventory_identity").Scan(&second); err != nil || first == second {
		t.Fatal("recreated inventory reused identity", second, err)
	}
	// An additive migration failure must leave both version and ledger at 8.
	dir2 := privateDir(t)
	path2 := filepath.Join(dir2, Filename)
	if err = os.WriteFile(path2, nil, 0600); err != nil {
		t.Fatal(err)
	}
	bad, err := connect(ctx, path2, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err = bad.db.Exec(migrations[i].sql); err != nil {
			t.Fatal(err)
		}
		if _, err = bad.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = bad.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=8; CREATE TABLE inventory_identity(conflict TEXT)"); err != nil {
		t.Fatal(err)
	}
	bad.Close()
	if migrated, err := OpenWriter(ctx, dir2); err == nil {
		migrated.Close()
		t.Fatal("expected migration failure")
	}
	bad, err = connect(ctx, path2, true)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	var version, n int
	if err = bad.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = bad.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&n); err != nil || version != 8 || n != 8 {
		t.Fatal(fmt.Sprint(version, n), err)
	}
}
