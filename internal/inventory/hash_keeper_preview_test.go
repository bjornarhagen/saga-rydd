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
	"strings"
	"testing"
	"time"
)

func requireHashKeeperPreviewClaims(t *testing.T, preview HashKeeperPreview) {
	t.Helper()
	if preview.Source != "saved_hash_observations" || preview.Contract != HashKeeperPreviewContract || preview.HashContract != FileHashContract || preview.Scope != "explicit_saved_subset" || preview.BudgetScope != "whole_saved_selection" || preview.Copies == nil || preview.ApprovalAvailable || preview.ProvenanceVerified || preview.ContentVerified || preview.CurrentStateVerified || preview.DuplicatesVerified || preview.Executable || preview.EstimatedReclaimableBytes != nil {
		t.Fatal("preview claimed approval, current contents or cleanup authority", preview)
	}
	if preview.ReadConsent != nil {
		requireHashReadConsentClaims(t, *preview.ReadConsent)
	}
}

func hashKeeperPreview(t *testing.T, store *HashStore, selectionID, keeper string, copies ...string) HashKeeperPreview {
	t.Helper()
	preview, err := store.PreviewKeeper(context.Background(), selectionID, keeper, copies)
	if err != nil {
		t.Fatal(err)
	}
	requireHashKeeperPreviewClaims(t, preview)
	return preview
}

func requireHashKeeperRefusal(t *testing.T, store *HashStore, selectionID, keeper string, copies []string, want error) {
	t.Helper()
	preview, err := store.PreviewKeeper(context.Background(), selectionID, keeper, copies)
	if !errors.Is(err, want) || !reflect.DeepEqual(preview, HashKeeperPreview{}) {
		t.Fatal("refused preview returned partial evidence or wrong error", preview, err, want)
	}
}

func TestHashKeeperPreviewExactOwnerRolesAndWholeSelectionCoverage(t *testing.T) {
	same := fullHashContents(65)
	f := hashStoreFixture(t, same, same, same, []byte("other"), fullHashContents(256))
	for range f.data {
		hashStoreRun(t, f, 65, 4096)
	}
	before := hashStoreSnapshot(t, f.store)
	// The nonterminal 256-byte file receives one aligned 64-byte grant;
	// the three complete 65-byte observations retain their exact final tails.
	if before.Work[4].DurableOffset != 64 || before.Budget.TotalReservedBytes != 264 || before.Budget.TotalReadBytes != 264 {
		t.Fatal("fixture grant or known usage changed", before)
	}
	preview := hashKeeperPreview(t, f.store, before.SelectionID, "3", "1")
	if preview.StoreID != before.StoreID || preview.SelectionID != before.SelectionID || preview.InventoryID != before.InventoryID || preview.SelectedWork != 5 || preview.CompletedObservations != 4 || preview.UnfinishedWork != 1 || preview.UnmatchedCompletedObservations != 1 || preview.LogicalBytes != 65 || preview.SHA256 != fmt.Sprintf("%x", sha256.Sum256(same)) || preview.Keeper.WorkID != "3" || len(preview.Copies) != 1 || preview.Copies[0].WorkID != "1" || !reflect.DeepEqual(preview.Budget, before.Budget) || preview.Budget.TotalReadBytes != 264 {
		t.Fatal("preview chose a keeper, expanded copies or changed coverage", preview)
	}
	for _, member := range []SavedHashPreviewMember{preview.Keeper, preview.Copies[0]} {
		ordinal := 0
		if member.WorkID == "3" {
			ordinal = 2
		}
		file, observation := f.expected[ordinal], before.Work[ordinal]
		if member.FileID != file.ID || member.RootID != file.RootID || !bytes.Equal(member.PathBytes, file.PathBytes) || !member.CheckedAt.Equal(observation.CheckedAt) || member.CheckedAt.IsZero() || member.Sequence != observation.Sequence || member.SavedDevice != file.Device || member.SavedInode != file.Inode || member.SavedChangedNS != file.ChangedNS || !member.SavedModifiedAt.Equal(file.ModifiedAt) || member.SavedAllocatedBytes != file.Allocated || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			t.Fatal("preview lost exact completed observation", member, file, observation)
		}
	}
	ordered := hashKeeperPreview(t, f.store, before.SelectionID, "3", "2", "1")
	if len(ordered.Copies) != 2 || ordered.Copies[0].WorkID != "2" || ordered.Copies[1].WorkID != "1" {
		t.Fatal("explicit copy order changed", ordered)
	}
	encoded, err := json.Marshal(preview)
	if err != nil || !bytes.Contains(encoded, []byte(`"observation_sequence":1`)) || !bytes.Contains(encoded, []byte(`"approval_available":false`)) || !bytes.Contains(encoded, []byte(`"estimated_reclaimable_bytes":null`)) {
		t.Fatal("JSON omitted sequence or preview-only qualification", string(encoded), err)
	}
	preview.Keeper.PathBytes[0] = 'x'
	preview.Copies[0].PathBytes[0] = 'y'
	preview.Budget.TotalReadBytes = -1
	if again := hashKeeperPreview(t, f.store, before.SelectionID, "3", "1"); !bytes.Equal(again.Keeper.PathBytes, f.expected[2].PathBytes) || !bytes.Equal(again.Copies[0].PathBytes, f.expected[0].PathBytes) || !reflect.DeepEqual(again.Budget, before.Budget) {
		t.Fatal("caller mutation changed later previews", again)
	}
	if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("preview persisted roles or changed work", after)
	}
}

func TestHashKeeperPreviewCanonicalRequestsFailBeforeStorageRead(t *testing.T) {
	var unavailable *HashStore
	validID := strings.Repeat("a", 64)
	for _, selectionID := range []string{"", "a", strings.Repeat("A", 64), strings.Repeat("z", 64), strings.Repeat("a", 65)} {
		requireHashKeeperRefusal(t, unavailable, selectionID, "1", []string{"2"}, ErrHashSelectionID)
	}
	for _, id := range []string{"", "0", "21", "01", "+1", "-1", " 1", "1\n", strings.Repeat("1", 4096)} {
		requireHashKeeperRefusal(t, unavailable, validID, id, []string{"2"}, ErrHashKeeperRequest)
		requireHashKeeperRefusal(t, unavailable, validID, "1", []string{id}, ErrHashKeeperRequest)
	}
	for _, copies := range [][]string{nil, {}, {"1"}, {"2", "2"}, make([]string, FileSampleTargetLimit)} {
		requireHashKeeperRefusal(t, unavailable, validID, "1", copies, ErrHashKeeperRequest)
	}
}

func TestHashKeeperPreviewExactSelectionRefusesReusedOrdinalsAndMissingWork(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"))
	other := hashStoreFixture(t, []byte("same"), []byte("same"))
	for range f.data {
		hashStoreRun(t, f, 64, 4096)
		hashStoreRun(t, other, 64, 4096)
	}
	selectionID := hashStoreSnapshot(t, f.store).SelectionID
	requireHashKeeperRefusal(t, other.store, selectionID, "1", []string{"2"}, ErrHashKeeperSelection)
	requireHashKeeperRefusal(t, f.store, strings.Repeat("0", 64), "1", []string{"2"}, ErrHashKeeperSelection)
	requireHashKeeperRefusal(t, f.store, selectionID, "3", []string{"1"}, ErrHashKeeperSelection)
	requireHashKeeperRefusal(t, f.store, selectionID, "1", []string{"3"}, ErrHashKeeperSelection)
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	empty, err := OpenHashWriter(context.Background(), filepath.Join(base, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	requireHashKeeperRefusal(t, empty, selectionID, "1", []string{"2"}, ErrHashKeeperSelection)
}

func TestHashKeeperPreviewRequiresMatchingCompletedObservations(t *testing.T) {
	for _, phase := range []string{"pending_keeper", "pending_copy", "partial_keeper", "partial_copy", "invalidated_copy", "different_digest", "different_size"} {
		t.Run(phase, func(t *testing.T) {
			data := fullHashContents(129)
			second := bytes.Clone(data)
			if phase == "different_digest" {
				second[64]++
			} else if phase == "different_size" {
				second = second[:128]
			}
			f := hashStoreFixture(t, data, second)
			switch phase {
			case "pending_keeper":
			case "pending_copy":
				hashStoreRun(t, f, 129, 4096)
			case "partial_keeper":
				hashStoreRun(t, f, 64, 4096)
				hashStoreRun(t, f, 129, 4096)
			case "partial_copy":
				hashStoreRun(t, f, 129, 4096)
				hashStoreRun(t, f, 64, 4096)
			case "invalidated_copy":
				hashStoreRun(t, f, 129, 4096)
				if err := os.WriteFile(string(f.expected[1].PathBytes), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096); err == nil {
					t.Fatal("fixture did not invalidate selected copy")
				}
			default:
				hashStoreRun(t, f, 129, 4096)
				hashStoreRun(t, f, 129, 4096)
			}
			before := hashStoreSnapshot(t, f.store)
			requireHashKeeperRefusal(t, f.store, before.SelectionID, "1", []string{"2"}, ErrHashKeeperSelection)
			if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("refusal changed saved work", before, after)
			}
		})
	}
}

func TestHashKeeperPreviewRunningCopyIsNotRecovered(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"))
	hashStoreRun(t, f, 64, 4096)
	selectionID := hashStoreSnapshot(t, f.store).SelectionID
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
		t.Fatal("fixture did not reserve copy")
	}
	before := hashStoreSnapshot(t, reader)
	requireHashKeeperRefusal(t, reader, selectionID, "1", []string{"2"}, ErrHashKeeperSelection)
	if after := hashStoreSnapshot(t, reader); !reflect.DeepEqual(before, after) || after.Work[1].Status != "running" || after.Work[1].LatestAttempt.Status != "reserved" {
		t.Fatal("preview recovered a reserved observation", before, after)
	}
}

func TestHashKeeperPreviewSavedAliasesOutsideRequestedGroupRefuse(t *testing.T) {
	scanner, targets := sampleFixture(t, []byte("same"), []byte("same"))
	first, second := string(targets[0].File.PathBytes), string(targets[1].File.PathBytes)
	alias := first + ".alias"
	if err := os.Link(first, alias); err != nil {
		t.Fatal(err)
	}
	targets = captureSampleTargets(t, scanner, string(targets[0].Root.PathBytes), []string{first, second, alias})
	f := hashStoreFixtureFromFiles(t, scanner, targets, [][]byte{[]byte("same"), []byte("same"), []byte("same")})
	hashStoreRun(t, f, 64, 4096)
	hashStoreRun(t, f, 64, 4096)
	before := hashStoreSnapshot(t, f.store)
	if before.Work[2].Status != "pending" {
		t.Fatal("alias must remain outside the completed hash group", before)
	}
	requireHashKeeperRefusal(t, f.store, before.SelectionID, "1", []string{"2"}, ErrHashKeeperIdentity)
	requireHashKeeperRefusal(t, f.store, before.SelectionID, "2", []string{"1"}, ErrHashKeeperIdentity)
	hashStoreRun(t, f, 64, 4096)
	requireHashKeeperRefusal(t, f.store, before.SelectionID, "1", []string{"3"}, ErrHashKeeperIdentity)
}

func TestHashKeeperPreviewWholeSelectionConflictAndUnrelatedAmbiguity(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"), []byte("other"), []byte("other"))
	for range f.data {
		hashStoreRun(t, f, 64, 4096)
	}
	snapshot, record, err := f.store.readHashSnapshot(context.Background(), f.store.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, cause := range []string{"alias", "size", "allocation", "ctime", "mtime", "completed_digest", "unrelated"} {
		t.Run(cause, func(t *testing.T) {
			copyRecord := *record
			copyRecord.Targets = append([]SavedFileTarget(nil), record.Targets...)
			copySnapshot := snapshot
			copySnapshot.Work = append([]SavedHashWork(nil), snapshot.Work...)
			if cause == "unrelated" {
				copyRecord.Targets[3].File.Device = copyRecord.Targets[2].File.Device
				copyRecord.Targets[3].File.Inode = copyRecord.Targets[2].File.Inode
				copyRecord.Targets[3].File.ChangedNS = copyRecord.Targets[2].File.ChangedNS + 1
			} else {
				other, original := &copyRecord.Targets[2].File, copyRecord.Targets[0].File
				other.Device, other.Inode = original.Device, original.Inode
				other.Size, other.Allocated, other.ChangedNS, other.ModifiedAt = original.Size, original.Allocated, original.ChangedNS, original.ModifiedAt
				copySnapshot.Work[2].Status = "pending"
				copySnapshot.Work[2].SHA256 = ""
				switch cause {
				case "size":
					other.Size++
				case "allocation":
					other.Allocated++
				case "ctime":
					other.ChangedNS++
				case "mtime":
					other.ModifiedAt = other.ModifiedAt.Add(time.Nanosecond)
				case "completed_digest":
					copySnapshot.Work[2].Status = "complete"
					copySnapshot.Work[2].LogicalBytes = original.Size
					copySnapshot.Work[2].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte("else")))
				}
			}
			// Only the private classifier receives alternate synthetic saved
			// evidence; public preview accepts IDs, not supplied snapshots.
			preview, err := previewSavedHashKeeper(copySnapshot, &copyRecord, snapshot.SelectionID, "1", []string{"2"})
			if cause == "unrelated" {
				if err != nil || preview.Keeper.WorkID != "1" || len(preview.Copies) != 1 || preview.Copies[0].WorkID != "2" {
					t.Fatal("unselected unrelated ambiguity blocked exact roles", preview, err)
				}
				requireHashKeeperPreviewClaims(t, preview)
			} else if !errors.Is(err, ErrHashKeeperIdentity) || !reflect.DeepEqual(preview, HashKeeperPreview{}) {
				t.Fatal("whole-selection alias/conflict was ignored", preview, err)
			}
		})
	}
}

func TestHashKeeperPreviewRawPathsOfflineLegacyAndReturnClones(t *testing.T) {
	for _, name := range []string{"line\nquote\"雪", string([]byte{'r', 0xff, 'w'})} {
		t.Run(fmt.Sprintf("%x", name), func(t *testing.T) {
			scanner, targets := sampleFixture(t, []byte("same"), []byte("same"))
			root := string(targets[0].Root.PathBytes)
			path := filepath.Join(filepath.Dir(string(targets[0].File.PathBytes)), name)
			requireSampleFixtureFilename(t, name, os.Rename(string(targets[0].File.PathBytes), path))
			targets = captureSampleTargets(t, scanner, root, []string{path, string(targets[1].File.PathBytes)})
			f := hashStoreFixtureFromFiles(t, scanner, targets, [][]byte{[]byte("same"), []byte("same")})
			hashStoreRun(t, f, 64, 4096)
			hashStoreRun(t, f, 64, 4096)
			f.reopen(t)
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
			reader.now = func() time.Time { t.Fatal("preview evaluated wall time"); return time.Time{} }
			preview := hashKeeperPreview(t, reader, before.SelectionID, "1", "2")
			if !bytes.Equal(preview.Keeper.PathBytes, []byte(path)) || preview.ReadConsent != nil || !reflect.DeepEqual(preview.Budget, before.Budget) {
				t.Fatal("offline legacy evidence lost", preview)
			}
			encoded, err := json.Marshal(preview)
			var decoded HashKeeperPreview
			if err != nil || json.Unmarshal(encoded, &decoded) != nil || !bytes.Equal(decoded.Keeper.PathBytes, []byte(path)) {
				t.Fatal("authoritative path lost during JSON roundtrip", string(encoded), err)
			}
			preview.Keeper.PathBytes[0] = 'x'
			preview.Copies[0].PathBytes[0] = 'y'
			if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("preview wrote or aliased saved observations", after)
			}
		})
	}
}

func TestHashKeeperPreviewRetainsConsentWithoutPermissionEvaluation(t *testing.T) {
	f, req := hashReadFixture(t, []byte("same"), []byte("same"))
	consent := hashReadApprove(t, f, req)
	for range f.data {
		if _, err := f.store.RunConsented(context.Background(), consent.ID, f.source, f.scanner); err != nil {
			t.Fatal(err)
		}
	}
	before := hashStoreSnapshot(t, f.store)
	f.store.now = func() time.Time { return consent.Approval.ExpiresAt.Add(time.Hour) }
	preview := hashKeeperPreview(t, f.store, before.SelectionID, "1", "2")
	if !reflect.DeepEqual(preview.ReadConsent, before.ReadConsent) || preview.ReadConsent.Status != "recorded" || preview.ReadConsent.ExpiredObserved || preview.ReadConsent.CurrentReadPermissionEvaluated {
		t.Fatal("preview evaluated or changed saved permission", preview)
	}
	preview.ReadConsent.Approval.SourceLocator.RootPathBytes[0] = 'x'
	if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("preview consent aliases saved state", after)
	}
}

func TestHashKeeperPreviewOnePublicationSnapshot(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(65), fullHashContents(65), fullHashContents(65))
	hashStoreRun(t, f, 65, 4096)
	hashStoreRun(t, f, 65, 4096)
	selectionID := hashStoreSnapshot(t, f.store).SelectionID
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
	query := &hashInterleaveQuery{Tx: tx, interleave: func() { hashStoreRun(t, f, 65, 4096) }}
	preview, err := reader.readHashKeeperPreview(context.Background(), query, selectionID, "2", []string{"1"})
	if err != nil || !query.once || preview.SelectedWork != 3 || preview.CompletedObservations != 2 || preview.UnfinishedWork != 1 || preview.Budget.TotalReadBytes != 130 || preview.Keeper.Sequence != 1 || preview.Copies[0].Sequence != 1 {
		t.Fatal("preview mixed role/coverage/charge publications", preview, err)
	}
	requireHashKeeperPreviewClaims(t, preview)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fresh := hashKeeperPreview(t, reader, selectionID, "2", "1")
	if fresh.CompletedObservations != 3 || fresh.UnfinishedWork != 0 || fresh.Budget.TotalReadBytes != 195 || len(fresh.Copies) != 1 {
		t.Fatal("interleave failed or expanded explicit roles", fresh)
	}
}

func TestHashKeeperPreviewMaximumExactSubset(t *testing.T) {
	contents := make([][]byte, FileSampleTargetLimit)
	copies := make([]string, 0, FileSampleTargetLimit-1)
	for i := range contents {
		contents[i] = []byte{0}
		if i > 0 {
			copies = append(copies, fmt.Sprint(i+1))
		}
	}
	f := hashStoreFixture(t, contents...)
	for range contents {
		hashStoreRun(t, f, 64, 4096)
	}
	selectionID := hashStoreSnapshot(t, f.store).SelectionID
	preview := hashKeeperPreview(t, f.store, selectionID, "1", copies...)
	if len(preview.Copies) != 19 || preview.Copies[18].WorkID != "20" || preview.SelectedWork != 20 || preview.CompletedObservations != 20 || preview.UnfinishedWork != 0 || preview.UnmatchedCompletedObservations != 0 {
		t.Fatal("maximum finite subset changed", preview)
	}
}

func TestHashKeeperPreviewCanceledOrCorruptPublishesNothing(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"), []byte("other"))
	hashStoreRun(t, f, 64, 4096)
	hashStoreRun(t, f, 64, 4096)
	selectionID := hashStoreSnapshot(t, f.store).SelectionID
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if preview, err := f.store.PreviewKeeper(ctx, selectionID, "1", []string{"2"}); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(preview, HashKeeperPreview{}) {
		t.Fatal("canceled preview published roles", preview, err)
	}
	f.store.mu.Lock()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	preview, err := f.store.PreviewKeeper(ctx, selectionID, "1", []string{"2"})
	f.store.mu.Unlock()
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(preview, HashKeeperPreview{}) {
		t.Fatal("preview gate ignored cancellation", preview, err)
	}
	// Corruption in unselected work invalidates the bounded whole-selection
	// read; apparently valid requested roles must not produce partial output.
	if _, err = f.store.db.Exec("UPDATE hash_work SET checkpoint=? WHERE id=3", []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	requireHashKeeperRefusal(t, f.store, selectionID, "1", []string{"2"}, ErrHashStoreCorrupt)
}
