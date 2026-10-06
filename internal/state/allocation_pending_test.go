package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
)

var allocationPendingPaths = [][]byte{[]byte("z-pending-00"), []byte("z-pending-01"), []byte("z-pending-\xff")}

func allocationPendingFixture(t *testing.T) (*Store, string) {
	t.Helper()
	s, dir := compactFixture(t, true)
	for _, query := range []string{
		"DELETE FROM jobs",
		"INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1)",
		"INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',1,'inodes')",
	} {
		if _, err := s.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	// A primary-key scope walk must inspect this large completed/non-directory
	// prefix before reaching the few pending directories. Commit fixture inserts
	// in the same bounded batches as production maintenance.
	const prefixRows = 8192
	for start := 0; start < prefixRows; start += MaxBatchEntries {
		func() {
			tx, err := s.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			stmt, err := tx.Prepare(`INSERT INTO allocation_members(root_id,scope,path,excluded,kind,generation,done)
 VALUES(1,X'2e',?,0,?,1,?)`)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			for i := start; i < min(start+MaxBatchEntries, prefixRows); i++ {
				kind, done := "directory", 1
				if i >= prefixRows/2 {
					kind, done = "file", 0
				}
				if _, err := stmt.Exec([]byte(fmt.Sprintf("a-prefix-%05d", i)), kind, done); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
	}
	// The prefix seek must also preserve exact root/scope filtering and the
	// partial-index exclusions, rather than selecting one of these earlier names.
	for _, member := range []struct {
		root       int64
		scope      string
		path       string
		excluded   int
		generation int
	}{
		{2, ".", "0-other-root", 0, 7},
		{1, "other", "0-other-scope", 0, 7},
		{1, ".", "0-excluded", 1, 7},
		{1, ".", "0-noncompact", 0, 0},
	} {
		if _, err := s.db.Exec(`INSERT INTO allocation_members(root_id,scope,path,excluded,kind,generation)
 VALUES(?,?,?,?,'directory',?)`, member.root, []byte(member.scope), []byte(member.path), member.excluded, member.generation); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range allocationPendingPaths {
		if _, err := s.db.Exec(`INSERT INTO allocation_members(root_id,scope,path,excluded,kind,generation)
 VALUES(1,X'2e',?,0,'directory',7)`, path); err != nil {
			t.Fatal(err)
		}
	}
	return s, dir
}

func requireAllocationPending(t *testing.T, s *Store, want []byte) {
	t.Helper()
	var path []byte
	var generation int64
	err := s.db.QueryRow(allocationPendingMemberQuery, 1, []byte(".")).Scan(&path, &generation)
	if want == nil {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("unexpected pending member", path, generation, err)
		}
		return
	}
	if err != nil || !bytes.Equal(path, want) || generation != 7 {
		t.Fatal("wrong next pending member", path, generation, err)
	}
}

func TestAllocationPendingMemberIndexedSeek(t *testing.T) {
	s, _ := allocationPendingFixture(t)
	var indexPage int
	if err := s.db.QueryRow("SELECT rootpage FROM sqlite_master WHERE name='allocation_members_pending'").Scan(&indexPage); err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+allocationPendingMemberQuery, 1, []byte("."))
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "allocation_members_pending (root_id=? AND scope=? AND done=?)") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatal("pending selection did not seek its complete index prefix in path order", plan)
	}
	// Check the bundled modernc driver's VM, not a host SQLite plan or the
	// presence of LIMIT. The exact pending-index cursor must seek all three
	// equality keys (root, scope, done), skipping the completed prefix directly.
	rows, err = s.db.Query("EXPLAIN "+allocationPendingMemberQuery, 1, []byte("."))
	if err != nil {
		t.Fatal(err)
	}
	indexCursor, threeKeySeek := -1, false
	for rows.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode string
		var p4, comment sql.NullString
		if err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if opcode == "OpenRead" && p2 == indexPage {
			indexCursor = p1
		}
		if opcode == "SeekGE" && p1 == indexCursor && p4.String == "3" {
			threeKeySeek = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if indexCursor < 0 || !threeKeySeek {
		t.Fatal("pending index did not perform a root/scope/done prefix seek")
	}
	requireAllocationPending(t, s, allocationPendingPaths[0])
}

func TestAllocationPendingMemberProgressAfterRestart(t *testing.T) {
	s, dir := allocationPendingFixture(t)
	counts := []int{MaxBatchEntries + 1, 2, 1}
	for member, count := range counts {
		for start := 0; start < count; start += MaxBatchEntries {
			func() {
				tx, err := s.db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				stmt, err := tx.Prepare(`INSERT INTO compact_inodes(root_id,path,generation,device,inode,allocated,logical,paths,conflicting)
 VALUES(1,?,7,'d',?,4096,1,1,0)`)
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
				for i := start; i < min(start+MaxBatchEntries, count); i++ {
					if _, err := stmt.Exec(allocationPendingPaths[member], fmt.Sprintf("%d-%05d", member, i)); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}()
		}
	}
	ctx := context.Background()
	step := func() {
		t.Helper()
		if worked, err := s.ReduceAllocations(ctx); err != nil || !worked {
			t.Fatal("pending reduction made no progress", worked, err)
		}
	}
	checkProgress := func(wantCount int, wantCursor string) {
		t.Helper()
		var count int
		var allocated int64
		var device, inode string
		if err := s.db.QueryRow("SELECT count(*) FROM allocation_identities WHERE root_id=1 AND scope=X'2e'").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRow("SELECT allocated,inode_device,inode_number FROM allocation_cache WHERE root_id=1 AND path=X'2e'").Scan(&allocated, &device, &inode); err != nil {
			t.Fatal(err)
		}
		wantDevice := ""
		if wantCursor != "" {
			wantDevice = "d"
		}
		if count != wantCount || allocated != int64(wantCount)*4096 || inode != wantCursor || device != wantDevice {
			t.Fatal("allocation progress repeated or skipped contributions", count, allocated, device, inode)
		}
	}
	requireAllocationPending(t, s, allocationPendingPaths[0])
	step()
	checkProgress(MaxBatchEntries, fmt.Sprintf("0-%05d", MaxBatchEntries-1))
	// A full batch leaves this member pending with its exact inode cursor.
	// Reopening must resume it before advancing to the next indexed member.
	requireAllocationPending(t, s, allocationPendingPaths[0])
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	step()
	checkProgress(MaxBatchEntries+1, "")
	requireAllocationPending(t, s, allocationPendingPaths[1])
	step()
	checkProgress(MaxBatchEntries+3, "")
	requireAllocationPending(t, s, allocationPendingPaths[2])
	step()
	checkProgress(MaxBatchEntries+4, "")
	requireAllocationPending(t, s, nil)
	step()
	var phase string
	var ready bool
	if err := s.db.QueryRow("SELECT phase,ready FROM allocation_cache WHERE root_id=1 AND path=X'2e'").Scan(&phase, &ready); err != nil || phase != "cleanup" || !ready {
		t.Fatal("pending exhaustion did not publish completion", phase, ready, err)
	}
}
