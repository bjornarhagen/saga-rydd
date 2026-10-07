package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func hashGroupsReport(t *testing.T, store *HashStore) HashGroupsReport {
	t.Helper()
	report, err := store.Groups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "saved_hash_observations" || report.Contract != HashGroupsContract || report.HashContract != FileHashContract || report.Scope != "whole_saved_selection" || report.BudgetScope != "whole_saved_selection" || report.Groups == nil || report.ProvenanceVerified || report.ContentVerified || report.CurrentStateVerified || report.DuplicatesVerified || report.Executable || report.EstimatedReclaimableBytes != nil {
		t.Fatal("stronger historical grouping claim", report)
	}
	return report
}

func TestHashGroupsCompletedObservationsAndDeterministicOrder(t *testing.T) {
	large, smallA, smallB := fullHashContents(129), bytes.Repeat([]byte{'a'}, 65), bytes.Repeat([]byte{'b'}, 65)
	f := hashStoreFixture(t, smallB, large, []byte("unmatched"), smallA, large, smallA, smallB)
	for range f.data {
		hashStoreRun(t, f, FileHashStepByteLimit, 4096)
	}
	snapshot := hashStoreSnapshot(t, f.store)
	report := hashGroupsReport(t, f.store)
	if report.StoreID != snapshot.StoreID || report.SelectionID != snapshot.SelectionID || report.InventoryID != snapshot.InventoryID || report.SelectedWork != 7 || report.CompletedObservations != 7 || report.UnfinishedWork != 0 || report.UnmatchedCompletedObservations != 1 || len(report.Groups) != 3 || !reflect.DeepEqual(report.Budget, snapshot.Budget) {
		t.Fatal("wrong selection coverage or budget", report)
	}
	want := []struct {
		size int64
		data []byte
		ids  []int
	}{{129, large, []int{2, 5}}, {65, smallA, []int{4, 6}}, {65, smallB, []int{1, 7}}}
	sort.Slice(want[1:], func(i, j int) bool {
		return fmt.Sprintf("%x", sha256.Sum256(want[i+1].data)) < fmt.Sprintf("%x", sha256.Sum256(want[j+1].data))
	})
	for i, group := range report.Groups {
		if group.LogicalBytes != want[i].size || group.SHA256 != fmt.Sprintf("%x", sha256.Sum256(want[i].data)) || len(group.Members) != 2 || group.SavedIdentities != 2 || group.RepeatedSavedPaths != 0 || group.ConflictingSavedIdentities != 0 {
			t.Fatal("wrong grouping or identity count", group, want[i])
		}
		for j, member := range group.Members {
			ordinal := want[i].ids[j]
			file := f.expected[ordinal-1]
			work := snapshot.Work[ordinal-1]
			if member.WorkID != fmt.Sprint(ordinal) || member.FileID != file.ID || member.RootID != file.RootID || !bytes.Equal(member.PathBytes, file.PathBytes) || !member.CheckedAt.Equal(work.CheckedAt) || member.CheckedAt.IsZero() || member.SavedDevice != file.Device || member.SavedInode != file.Inode || member.SavedChangedNS != file.ChangedNS || !member.SavedModifiedAt.Equal(file.ModifiedAt) || member.SavedAllocatedBytes != file.Allocated || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
				t.Fatal("member lost exact saved evidence", member, file, work)
			}
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil || !bytes.Contains(encoded, []byte(`"estimated_reclaimable_bytes":null`)) || !bytes.Contains(encoded, []byte(`"duplicates_verified":false`)) {
		t.Fatal("missing explicit unknown savings or false claim", string(encoded), err)
	}
	report.Groups[0].Members[0].PathBytes[0] = 'x'
	report.Budget.TotalReadBytes = -1
	if again := hashGroupsReport(t, f.store); !bytes.Equal(again.Groups[0].Members[0].PathBytes, f.expected[1].PathBytes) || !reflect.DeepEqual(again.Budget, snapshot.Budget) {
		t.Fatal("caller mutated saved report", again)
	}
}

func TestHashGroupsRawPathsAndSavedHardlinkAliases(t *testing.T) {
	for _, name := range []string{"line\nquote\"雪", string([]byte{'r', 0xff, 'w'})} {
		t.Run(fmt.Sprintf("%x", name), func(t *testing.T) {
			scanner, targets := sampleFixture(t, []byte("same bytes"))
			first := string(targets[0].File.PathBytes)
			second := filepath.Join(filepath.Dir(first), name)
			requireSampleFixtureFilename(t, name, os.Link(first, second))
			targets = captureSampleTargets(t, scanner, string(targets[0].Root.PathBytes), []string{first, second})
			f := hashStoreFixtureFromFiles(t, scanner, targets, [][]byte{[]byte("same bytes"), []byte("same bytes")})
			hashStoreRun(t, f, 64, 4096)
			hashStoreRun(t, f, 64, 4096)
			f.reopen(t)
			report := hashGroupsReport(t, f.store)
			if len(report.Groups) != 1 || report.Groups[0].SavedIdentities != 1 || report.Groups[0].RepeatedSavedPaths != 1 || report.Groups[0].ConflictingSavedIdentities != 0 || !bytes.Equal(report.Groups[0].Members[1].PathBytes, []byte(second)) {
				t.Fatal("saved hardlink/path evidence lost", report)
			}
			for _, member := range report.Groups[0].Members {
				if !member.RepeatedSavedIdentity || member.SavedIdentityConflict {
					t.Fatal("historical alias classification changed", member)
				}
			}
			encoded, err := json.Marshal(report)
			var decoded HashGroupsReport
			if err != nil || json.Unmarshal(encoded, &decoded) != nil || !bytes.Equal(decoded.Groups[0].Members[1].PathBytes, []byte(second)) {
				t.Fatal("raw path changed in JSON", string(encoded), err)
			}
		})
	}
}

func TestHashGroupsIdentityConflictsUseWholeSelection(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"), []byte("same"))
	for range f.data {
		hashStoreRun(t, f, 64, 4096)
	}
	snapshot, record, err := f.store.readHashSnapshot(context.Background(), f.store.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, cause := range []string{"size", "allocation", "ctime", "mtime", "completed_digest", "generation_only"} {
		t.Run(cause, func(t *testing.T) {
			// Feed only the private classifier with a cloned synthetic saved
			// comparison; no public API accepts alternate selection evidence.
			copyRecord := *record
			copyRecord.Targets = append([]SavedFileTarget(nil), record.Targets...)
			copySnapshot := snapshot
			copySnapshot.Work = append([]SavedHashWork(nil), snapshot.Work...)
			other := &copyRecord.Targets[2].File
			other.Device, other.Inode = record.Targets[0].File.Device, record.Targets[0].File.Inode
			other.Allocated, other.ChangedNS, other.ModifiedAt = record.Targets[0].File.Allocated, record.Targets[0].File.ChangedNS, record.Targets[0].File.ModifiedAt
			// The alias is outside the reported matching group, either
			// unfinished or completed with another digest/size.
			copySnapshot.Work[2].Status = "pending"
			copySnapshot.Work[2].SHA256 = ""
			switch cause {
			case "size":
				other.Size++
				copySnapshot.Work[2].LogicalBytes++
			case "allocation":
				other.Allocated++
			case "ctime":
				other.ChangedNS++
			case "mtime":
				other.ModifiedAt = other.ModifiedAt.Add(time.Nanosecond)
			case "completed_digest":
				copySnapshot.Work[2].Status = "complete"
				copySnapshot.Work[2].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte("else")))
			case "generation_only":
				other.Generation++
			}
			report := groupSavedHashes(copySnapshot, &copyRecord)
			conflict := cause != "generation_only"
			if len(report.Groups) != 1 || len(report.Groups[0].Members) != 2 || report.Groups[0].SavedIdentities != 2 || report.Groups[0].RepeatedSavedPaths != 0 || report.Groups[0].Members[0].SavedIdentityConflict != conflict || !report.Groups[0].Members[0].RepeatedSavedIdentity || report.Groups[0].Members[1].SavedIdentityConflict || report.Groups[0].Members[1].RepeatedSavedIdentity {
				t.Fatal("alias outside hash group was ignored or overclaimed", report)
			}
			wantConflicts := 1
			if !conflict {
				wantConflicts = 0
			}
			if report.Groups[0].ConflictingSavedIdentities != wantConflicts {
				t.Fatal("wrong global conflict summary", report)
			}
		})
	}
	// Equal digest alone does not join different logical sizes.
	copySnapshot := snapshot
	copySnapshot.Work = append([]SavedHashWork(nil), snapshot.Work...)
	copySnapshot.Work[2].LogicalBytes++
	if report := groupSavedHashes(copySnapshot, record); len(report.Groups) != 1 || len(report.Groups[0].Members) != 2 || report.UnmatchedCompletedObservations != 1 {
		t.Fatal("size omitted from comparison key", report)
	}
}

func TestHashGroupsOfflineReadDoesNotEvaluatePermission(t *testing.T) {
	f, req := hashReadFixture(t, []byte("same"), []byte("same"))
	consent := hashReadApprove(t, f, req)
	for range f.data {
		if _, err := f.store.RunConsented(context.Background(), consent.ID, f.source, f.scanner); err != nil {
			t.Fatal(err)
		}
	}
	before := hashStoreSnapshot(t, f.store)
	if err := f.source.Close(); err != nil {
		t.Fatal(err)
	}
	f.scanner.Close()
	for _, original := range []string{f.root, f.stateDir} {
		offline := original + ".offline"
		if err := os.Rename(original, offline); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(offline) })
	}
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.now = func() time.Time { t.Fatal("saved grouping evaluated wall time"); return time.Time{} }
	f.store.now = func() time.Time { return consent.Approval.ExpiresAt.Add(time.Hour) }
	report := hashGroupsReport(t, reader)
	if len(report.Groups) != 1 || !reflect.DeepEqual(report.Budget, before.Budget) || !reflect.DeepEqual(report.ReadConsent, before.ReadConsent) || report.ReadConsent.CurrentReadPermissionEvaluated || report.ReadConsent.Status != "recorded" || report.ReadConsent.ExpiredObserved {
		t.Fatal("saved report changed approval or omitted charges", report)
	}
	if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("group report changed saved observations", after)
	}
}

func TestHashGroupsRunningPartialAndInvalidatedAreUnfinished(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"), fullHashContents(256), fullHashContents(256), fullHashContents(256))
	hashStoreRun(t, f, 64, 4096)
	hashStoreRun(t, f, 64, 4096)
	hashStoreRun(t, f, 64, 4096) // partial checked prefix, never a full digest
	if err := os.WriteFile(string(f.expected[3].PathBytes), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096); err == nil {
		t.Fatal("changed fixture did not invalidate work")
	}
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	reserved, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, e := f.store.runNext(ctx, f.source, f.scanner, 64, 4096, hashStoreHooks{afterReserve: func() { close(reserved); <-release }})
		done <- e
	}()
	defer func() { cancel(); close(release); <-done }()
	select {
	case <-reserved:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not reserve")
	}
	before := hashStoreSnapshot(t, reader)
	report := hashGroupsReport(t, reader)
	after := hashStoreSnapshot(t, reader)
	if report.SelectedWork != 5 || report.CompletedObservations != 2 || report.UnfinishedWork != 3 || report.UnmatchedCompletedObservations != 0 || len(report.Groups) != 1 || !reflect.DeepEqual(before, after) || before.Work[2].DurableOffset != 64 || before.Work[3].Status != "invalidated" || before.Work[4].Status != "running" || before.Work[4].LatestAttempt.Status != "reserved" {
		t.Fatal("report recovered or grouped unfinished work", report, before, after)
	}
}

func TestHashGroupsUseOnePublicationSnapshot(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(65), fullHashContents(65))
	hashStoreRun(t, f, 65, 4096)
	hashStoreRun(t, f, 64, 4096)
	reader, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	tx, err := reader.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := &hashInterleaveQuery{Tx: tx, interleave: func() { hashStoreRun(t, f, 64, 4096) }}
	report, err := reader.readHashGroups(context.Background(), query)
	if err != nil || !query.once || report.CompletedObservations != 1 || report.UnfinishedWork != 1 || report.UnmatchedCompletedObservations != 1 || len(report.Groups) != 0 || report.Budget.TotalReadBytes != 129 {
		t.Fatal("mixed observation/charge publication", report, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if fresh := hashGroupsReport(t, reader); len(fresh.Groups) != 1 || fresh.CompletedObservations != 2 || fresh.Budget.TotalReadBytes != 130 {
		t.Fatal("interleave did not complete peer", fresh)
	}
}

func TestHashGroupsEmptyAndBoundedSelection(t *testing.T) {
	t.Run("no_selection", func(t *testing.T) {
		base, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		store, err := OpenHashWriter(context.Background(), filepath.Join(base, "state"))
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		report := hashGroupsReport(t, store)
		if report.SelectionID != "" || report.InventoryID != "" || report.SelectedWork != 0 || report.CompletedObservations != 0 || report.UnfinishedWork != 0 || report.UnmatchedCompletedObservations != 0 || len(report.Groups) != 0 || report.Budget != nil || report.ReadConsent != nil {
			t.Fatal(report)
		}
	})
	for _, count := range []int{2, FileSampleTargetLimit} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			contents := make([][]byte, count)
			for i := range contents {
				contents[i] = []byte{0}
			}
			f := hashStoreFixture(t, contents...)
			for range contents {
				hashStoreRun(t, f, 64, 4096)
			}
			report := hashGroupsReport(t, f.store)
			if report.SelectedWork != count || report.CompletedObservations != count || len(report.Groups) != 1 || len(report.Groups[0].Members) != count || report.Groups[0].LogicalBytes != 1 || report.Groups[0].SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte{0})) || report.Budget.TotalReservedBytes != int64(count) || report.Budget.TotalReadBytes != int64(count) {
				t.Fatal("finite selection or exact byte counts lost", report)
			}
		})
	}
}

func TestHashGroupsClassifierDoesNotTreatKnownZeroAsUnknown(t *testing.T) {
	// Current saved-row capture requires positive sizes. The private historical
	// classifier still treats a known zero as a size, never an unknown total.
	f := hashStoreFixture(t, []byte{0}, []byte{0})
	snapshot, record, err := f.store.readHashSnapshot(context.Background(), f.store.db)
	if err != nil {
		t.Fatal(err)
	}
	for i := range record.Targets {
		record.Targets[i].File.Size = 0
		snapshot.Work[i].LogicalBytes = 0
		snapshot.Work[i].Status = "complete"
		snapshot.Work[i].SHA256 = fmt.Sprintf("%x", sha256.Sum256(nil))
	}
	report := groupSavedHashes(snapshot, record)
	if len(report.Groups) != 1 || report.Groups[0].LogicalBytes != 0 || report.Groups[0].SHA256 != fmt.Sprintf("%x", sha256.Sum256(nil)) || report.CompletedObservations != 2 || report.UnmatchedCompletedObservations != 0 || report.EstimatedReclaimableBytes != nil {
		t.Fatal("known zero confused with savings or unknown evidence", report)
	}
}

func TestHashGroupsCorruptOrCanceledReportPublishesNothing(t *testing.T) {
	for _, cause := range []string{"checkpoint", "oversized_checkpoint", "extra_work", "bad_identity"} {
		t.Run(cause, func(t *testing.T) {
			f := hashStoreFixture(t, []byte("same"), []byte("same"))
			hashStoreRun(t, f, 64, 4096)
			hashStoreRun(t, f, 64, 4096)
			var err error
			switch cause {
			case "checkpoint":
				_, err = f.store.db.Exec("UPDATE hash_work SET checkpoint=? WHERE id=2", []byte("corrupt"))
			case "oversized_checkpoint":
				_, err = f.store.db.Exec("PRAGMA ignore_check_constraints=1; UPDATE hash_work SET checkpoint=? WHERE id=2", bytes.Repeat([]byte{'x'}, 8193))
			case "extra_work":
				_, err = f.store.db.Exec("PRAGMA ignore_check_constraints=1; INSERT INTO hash_work SELECT 21,status,sequence,ready_order,checked_offset,checkpoint,error_code FROM hash_work WHERE id=1")
			case "bad_identity":
				// A malformed selection must fail as a whole rather than turn
				// unknown inode evidence into an independent saved identity.
				_, record, e := f.store.readHashSnapshot(context.Background(), f.store.db)
				if e != nil {
					t.Fatal(e)
				}
				record.Targets[1].File.Inode = "unknown"
				payload, e := json.Marshal(record)
				if e != nil {
					t.Fatal(e)
				}
				_, err = f.store.db.Exec("DROP TRIGGER hash_selection_no_update; UPDATE hash_selection SET payload=?,selection_id=?", payload, fmt.Sprintf("%x", sha256.Sum256(payload)))
			}
			if err != nil {
				t.Fatal(err)
			}
			report, err := f.store.Groups(context.Background())
			if !errors.Is(err, ErrHashStoreCorrupt) || !reflect.DeepEqual(report, HashGroupsReport{}) {
				t.Fatal("partial trusted output from corrupt selection", report, err)
			}
		})
	}
	f := hashStoreFixture(t, []byte("same"), []byte("same"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if report, err := f.store.Groups(ctx); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(report, HashGroupsReport{}) {
		t.Fatal("canceled report published", report, err)
	}
	f.store.mu.Lock()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	report, err := f.store.Groups(ctx)
	f.store.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(report, HashGroupsReport{}) {
		t.Fatal("gate wait ignored cancellation", report, err)
	}
}
