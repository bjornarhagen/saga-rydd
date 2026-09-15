package state

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestLargestFilesPaginationAndSavedFreshness(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(ctx, []string{"/unavailable-fixture"}); err != nil {
		t.Fatal(err)
	}
	var root int64
	if err = s.db.QueryRow("SELECT id FROM roots").Scan(&root); err != nil {
		t.Fatal(err)
	}
	paths := [][]byte{[]byte("a"), []byte("b\n\x1b[31m"), {'c', 0xff}}
	for i, path := range paths {
		_, err = s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(?,?,X'2e','file',100,4096,1,1,'dev','ino',?,2)`, root, path, i+1)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.Exec("INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(?,X'2e',3,0,2)", root); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE roots SET last_error='unavailable'"); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	first, err := r.LargestFiles(ctx, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Files) != 2 || first.NextCursor == "" || first.CurrentStateVerified || first.Files[0].ParentPass != "partial" || first.Files[1].ParentPass != "unconfirmed" || first.Roots[0].LastError != "unavailable" {
		t.Fatal(first)
	}
	if !bytes.HasSuffix(first.Files[0].PathBytes, paths[2]) {
		t.Fatal(first.Files[0])
	}
	second, err := r.LargestFiles(ctx, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Files) != 1 || second.NextCursor != "" || second.Files[0].ID >= first.Files[1].ID {
		t.Fatal(second)
	}
	// A finished direct-parent pass does not assert current ancestry or availability.
	if _, err = s.db.Exec("UPDATE directories SET complete=1"); err != nil {
		t.Fatal(err)
	}
	updated, err := r.LargestFiles(ctx, 1, "")
	if err != nil || updated.Files[0].ParentPass != "observed_in_completed_parent_pass" || updated.CurrentStateVerified {
		t.Fatal(updated, err)
	}
	if err = s.SyncRoots(ctx, []string{"/different-fixture"}); err != nil {
		t.Fatal(err)
	}
	empty, err := r.LargestFiles(ctx, 20, "")
	if err != nil || len(empty.Files) != 0 || len(empty.Roots) != 1 {
		t.Fatal(empty, err)
	}
	if _, err = r.LargestFiles(ctx, 20, "invalid"); !errors.Is(err, ErrReportCursor) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = r.LargestFiles(canceled, 20, ""); err == nil {
		t.Fatal("cancellation ignored")
	}
}
