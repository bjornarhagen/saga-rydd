package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func copySizesFixture(t *testing.T) (*hashStoreTestFixture, SavedHashKeeperChoice) {
	t.Helper()
	body := fullHashContents(65)
	f := hashChoiceFixture(t, body, body, body, body)
	p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "3", "2", "1")
	saved := saveHashChoice(t, hashChoiceWriter(t, f), p)
	return f, saved
}

func copySizesSyntheticChoice(t *testing.T, original SavedHashKeeperChoice, logical int64, allocations ...int64) SavedHashKeeperChoice {
	t.Helper()
	if len(allocations) < 1 || len(allocations) >= FileSampleTargetLimit {
		t.Fatal("invalid generated arithmetic cardinality")
	}
	saved := original
	p := &saved.Record.Evidence
	p.Keeper = original.Record.Evidence.Keeper
	p.Keeper.WorkID = "20"
	p.Keeper.SavedDevice, p.Keeper.SavedInode = "0", "20"
	p.Keeper.SavedAllocatedBytes = math.MaxInt64 // Never contributes to copies.
	p.LogicalBytes = logical
	p.Copies = make([]SavedHashPreviewMember, len(allocations))
	p.SelectedWork, p.CompletedObservations, p.UnfinishedWork, p.UnmatchedCompletedObservations = 20, 20, 0, 0
	for i, allocated := range allocations {
		member := original.Record.Evidence.Copies[0]
		member.WorkID = strconv.Itoa(i + 1)
		member.PathBytes = []byte(fmt.Sprintf("/generated-copy-%02d", i+1))
		member.SavedDevice, member.SavedInode = "0", strconv.Itoa(i+1)
		member.SavedAllocatedBytes = allocated
		p.Copies[i] = member
	}
	payload, err := json.Marshal(saved.Record)
	if err != nil {
		t.Fatal(err)
	}
	saved.ID = hashKeeperChoiceID(payload)
	return saved
}

func requireCopySizes(t *testing.T, s HistoricalCopySizes, n int, logical, allocated *int64) {
	t.Helper()
	if s.Contract != HistoricalCopySizesContract || s.Basis != "immutable_choice_evidence" || s.Scope != "explicit_saved_copy_paths" || s.SelectedCopyPaths != n || s.KnownSavedObjects != n || s.KnownAllocationPaths != n || s.RepeatedSavedPaths != 0 || s.UnknownSavedIdentityPaths != 0 || s.ConflictingSavedObjects != 0 || s.UnknownAllocationPaths != 0 || s.ReclaimableBytes != nil || s.CurrentStateVerified || s.IndependentAllocationVerified || s.Executable {
		t.Fatal("summary changed its historical scope or authority", s)
	}
	for _, metric := range []struct {
		name   string
		got    *int64
		status string
		want   *int64
	}{{"logical", s.LogicalBytes, s.LogicalBytesStatus, logical}, {"allocated", s.SavedAllocatedBytes, s.SavedAllocatedBytesStatus, allocated}} {
		if metric.want == nil {
			if metric.got != nil || metric.status != "overflow" {
				t.Fatal("overflow invented a numeric total", metric.name, s)
			}
		} else if metric.got == nil || *metric.got != *metric.want || metric.status != "known" {
			t.Fatal("known historical total changed", metric.name, s, *metric.want)
		}
	}
	body, err := json.Marshal(s)
	if err != nil || len(body) > 1024 {
		t.Fatal("fixed additive summary exceeded1024 bytes", len(body), err)
	}
}

func copySizesStoredBytes(t *testing.T, base string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	for _, name := range []string{hashStoreFilename, hashStoreFilename + "-wal"} {
		path := filepath.Join(base, "hashes", name)
		body, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) || err == nil && name != hashStoreFilename && len(body) == 0 {
			continue
		}
		if err != nil || len(body) > 4<<20 {
			t.Fatal("generated store body unavailable or oversized", name, len(body), err)
		}
		out[name] = sha256.Sum256(body)
	}
	return out
}

func TestHashKeeperChoiceSizesOfflineSelectedPathsAndImmutableRecord(t *testing.T) {
	f, saved := copySizesFixture(t)
	before := hashStoreSnapshot(t, f.store)
	payload, err := json.Marshal(saved.Record)
	if err != nil {
		t.Fatal(err)
	}
	// Only these generated source and inventory paths are parked.
	hashChoiceOffline(t, f)
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	storedBefore := copySizesStoredBytes(t, f.base)
	r, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.now = func() time.Time { t.Fatal("size view consulted current admission clock"); return time.Time{} }
	report, err := r.KeeperChoiceSizes(context.Background(), saved.ID)
	if err != nil || !reflect.DeepEqual(report.SavedHashKeeperChoice, saved) {
		t.Fatal("offline size view changed immutable evidence", report, err)
	}
	logical := int64(2) * saved.Record.Evidence.LogicalBytes
	allocated := saved.Record.Evidence.Copies[0].SavedAllocatedBytes + saved.Record.Evidence.Copies[1].SavedAllocatedBytes
	requireCopySizes(t, report.HistoricalCopySizes, 2, &logical, &allocated)
	immutableView, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	gotPayload, err := json.Marshal(report.Record)
	if err != nil || !bytes.Equal(gotPayload, payload) || report.ID != hashKeeperChoiceID(payload) || report.Record.Evidence.Keeper.WorkID != "3" || report.Record.Evidence.Copies[0].WorkID != "2" || report.Record.Evidence.Copies[1].WorkID != "1" || report.Record.Evidence.SelectedWork != 4 {
		t.Fatal("totals changed role order, ID, record or whole-selection denominator", report, err)
	}
	if after := hashStoreSnapshot(t, r); !reflect.DeepEqual(after, before) {
		t.Fatal("size view advanced accounting, consent or work", after)
	}
	if err = r.Close(); err != nil || !reflect.DeepEqual(storedBefore, copySizesStoredBytes(t, f.base)) {
		t.Fatal("saved-only view rewrote persistent database/nonempty WAL", err)
	}
	report.Record.Evidence.Copies[0].PathBytes[0] = 'x'
	if *report.HistoricalCopySizes.LogicalBytes != logical {
		t.Fatal("derived quantity aliased selected paths")
	}
	reopened, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("reopened generated reader close failed", err)
		}
	}()
	reopened.now = func() time.Time { t.Fatal("reopened size view consulted current admission clock"); return time.Time{} }
	again, readErr := reopened.KeeperChoiceSizes(context.Background(), saved.ID)
	afterReopen := hashStoreSnapshot(t, reopened)
	closeErr := reopened.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal("reopened saved-only view or checked close failed", readErr, closeErr)
	}
	againView, err := json.Marshal(again)
	if err != nil || !bytes.Equal(immutableView, againView) || !reflect.DeepEqual(again.SavedHashKeeperChoice, saved) {
		t.Fatal("returned path mutation changed subsequently reopened saved choice or summary", again, err)
	}
	if !reflect.DeepEqual(afterReopen, before) || !reflect.DeepEqual(storedBefore, copySizesStoredBytes(t, f.base)) {
		t.Fatal("reopened saved-only view changed accounting or persistent database/nonempty WAL")
	}
}

func TestHashKeeperChoiceSizesCheckedArithmeticAndSummaryBound(t *testing.T) {
	_, saved := copySizesFixture(t)
	known := func(n int64) *int64 { return &n }
	cases := []struct {
		name        string
		logical     int64
		allocations []int64
		wantLogical *int64
		wantAlloc   *int64
	}{
		{"per_path_separate_allocation", 7, []int64{3, 10}, known(14), known(13)},
		{"saved_zero_allocation", 7, []int64{0, 0}, known(14), known(0)},
		{"zero_logical", 0, []int64{0}, known(0), known(0)},
		{"exact_logical_max", math.MaxInt64, []int64{0}, known(math.MaxInt64), known(0)},
		{"logical_overflow_only", math.MaxInt64/2 + 1, []int64{3, 5}, nil, known(8)},
		{"allocation_overflow_only", 1, []int64{math.MaxInt64, 1}, known(2), nil},
		{"exact_allocation_max", 1, []int64{math.MaxInt64 - 1, 1}, known(2), known(math.MaxInt64)},
		{"both_overflow", math.MaxInt64, []int64{math.MaxInt64, math.MaxInt64}, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			choice := copySizesSyntheticChoice(t, saved, tc.logical, tc.allocations...)
			before, _ := json.Marshal(choice)
			sizes, err := historicalCopySizes(choice)
			if err != nil {
				t.Fatal(err)
			}
			requireCopySizes(t, sizes, len(tc.allocations), tc.wantLogical, tc.wantAlloc)
			after, _ := json.Marshal(choice)
			if !bytes.Equal(before, after) {
				t.Fatal("pure derivation changed canonical role evidence")
			}
		})
	}
	for n := 1; n < FileSampleTargetLimit; n++ {
		allocations := make([]int64, n)
		for i := range allocations {
			allocations[i] = int64(i + 1)
		}
		logical, allocated := int64(n)*13, int64(n*(n+1)/2)
		sizes, err := historicalCopySizes(copySizesSyntheticChoice(t, saved, 13, allocations...))
		if err != nil {
			t.Fatal(n, err)
		}
		requireCopySizes(t, sizes, n, &logical, &allocated)
	}
}

func TestHashKeeperChoiceSizesGeneratedSparseAllocationStaysHistorical(t *testing.T) {
	body := make([]byte, 512<<10)
	// Extend a fresh empty work 1 with Truncate instead of writing its body.
	// Genuine sparse allocation remains mandatory in the assertion below.
	scanner, targets := sampleFixture(t, nil, body)
	path := string(targets[0].File.PathBytes)
	file, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Truncate(int64(len(body)))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatal("generated sparse file preparation failed", err, closeErr)
	}
	paths := []string{path, string(targets[1].File.PathBytes)}
	targets = captureSampleTargets(t, scanner, string(targets[0].Root.PathBytes), paths)
	f := hashStoreFixtureFromFiles(t, scanner, targets, [][]byte{body, body})
	for range 2 {
		hashStoreRun(t, f, FileHashStepByteLimit, 1<<20)
	}
	p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "2", "1")
	if p.Copies[0].SavedAllocatedBytes >= p.LogicalBytes {
		t.Fatal("generated fixture filesystem did not expose sparse allocation; genuine sparse evidence is unavailable")
	}
	saved := saveHashChoice(t, hashChoiceWriter(t, f), p)
	report, err := f.store.KeeperChoiceSizes(context.Background(), saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	requireCopySizes(t, report.HistoricalCopySizes, 1, &p.LogicalBytes, &p.Copies[0].SavedAllocatedBytes)
	if report.Record.Evidence.Keeper.WorkID != "2" || report.Record.Evidence.Copies[0].WorkID != "1" {
		t.Fatal("sparse size report chose new roles", report)
	}
}

func TestHashKeeperChoiceSizesInvalidOrRepeatedEvidenceRefuses(t *testing.T) {
	_, original := copySizesFixture(t)
	for _, kind := range []string{"missing_device", "noncanonical_device", "zero_inode", "negative_allocation", "alias_keeper", "alias_copy", "repeated_flag", "conflict_flag", "authority"} {
		t.Run(kind, func(t *testing.T) {
			saved := copySizesSyntheticChoice(t, original, 4, 3, 5)
			p := &saved.Record.Evidence
			switch kind {
			case "missing_device":
				p.Copies[0].SavedDevice = ""
			case "noncanonical_device":
				p.Copies[0].SavedDevice = "00"
			case "zero_inode":
				p.Copies[0].SavedInode = "0"
			case "negative_allocation":
				p.Copies[0].SavedAllocatedBytes = -1
			case "alias_keeper":
				p.Copies[0].SavedInode = p.Keeper.SavedInode
			case "alias_copy":
				p.Copies[1].SavedInode = p.Copies[0].SavedInode
			case "repeated_flag":
				p.Copies[0].RepeatedSavedIdentity = true
			case "conflict_flag":
				p.Copies[0].SavedIdentityConflict = true
			case "authority":
				p.Executable = true
			}
			got, err := historicalCopySizes(saved)
			if !errors.Is(err, ErrHashKeeperChoiceCorrupt) || !reflect.DeepEqual(got, HistoricalCopySizes{}) {
				t.Fatal("invalid evidence produced partial numeric summary", got, err)
			}
		})
	}
}

func TestHashKeeperChoiceSizesCorruptStorageKeepsIDAndNoPartialResult(t *testing.T) {
	for _, kind := range []string{"missing_allocation", "negative_allocation", "missing_identity", "conflict", "wrong_allocation_type"} {
		t.Run(kind, func(t *testing.T) {
			f, saved := copySizesFixture(t)
			if _, err := f.store.db.Exec("DROP TRIGGER hash_keeper_choice_no_update"); err != nil {
				t.Fatal(err)
			}
			record := saved.Record
			switch kind {
			case "negative_allocation":
				record.Evidence.Copies[0].SavedAllocatedBytes = -1
			case "missing_identity":
				record.Evidence.Copies[0].SavedInode = ""
			case "conflict":
				record.Evidence.Copies[0].SavedIdentityConflict = true
			}
			payload, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "missing_allocation" || kind == "wrong_allocation_type" {
				old := []byte(fmt.Sprintf(`"saved_allocated_bytes":%d`, record.Evidence.Keeper.SavedAllocatedBytes))
				replacement := []byte(`"saved_allocated_bytes":"unknown"`)
				if kind == "missing_allocation" {
					old = append(old, ',')
					replacement = nil
				}
				changed := bytes.Replace(payload, old, replacement, 1)
				if bytes.Equal(changed, payload) {
					t.Fatal("generated corruption did not change selected field")
				}
				payload = changed
			}
			id := hashKeeperChoiceID(payload)
			key, err := hashKeeperChoiceRequestKey(record.Evidence)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.store.db.Exec("UPDATE hash_keeper_choice SET id=?,request_key=?,payload=? WHERE id=?", id, key, payload, saved.ID); err != nil {
				t.Fatal(err)
			}
			got, err := f.store.KeeperChoiceSizes(context.Background(), id)
			if !errors.Is(err, ErrHashKeeperChoiceCorrupt) || !strings.Contains(err.Error(), id) || !reflect.DeepEqual(got, HashKeeperChoiceSizesReport{}) {
				t.Fatal("corrupt size view lost requested ID or exposed partial record", got, err)
			}
		})
	}
}

func TestHashKeeperChoiceSizesCancellationMissingAndClosedKeepRequestedID(t *testing.T) {
	f, saved := copySizesFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := f.store.KeeperChoiceSizes(ctx, saved.ID)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), saved.ID) || !reflect.DeepEqual(got, HashKeeperChoiceSizesReport{}) {
		t.Fatal("canceled read lost requested ID or returned partial evidence", got, err)
	}
	missing := "hash-choice-v1-" + strings.Repeat("f", 64)
	got, err = f.store.KeeperChoiceSizes(context.Background(), missing)
	if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), missing) || !reflect.DeepEqual(got, HashKeeperChoiceSizesReport{}) {
		t.Fatal("missing read lost requested ID", got, err)
	}
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = f.store.KeeperChoiceSizes(context.Background(), saved.ID)
	if err == nil || !strings.Contains(err.Error(), saved.ID) || !reflect.DeepEqual(got, HashKeeperChoiceSizesReport{}) {
		t.Fatal("closed read lost requested ID", got, err)
	}
	got, err = f.store.KeeperChoiceSizes(context.Background(), "not-a-choice")
	if !errors.Is(err, ErrHashKeeperChoiceID) || strings.Contains(err.Error(), "not-a-choice") || !reflect.DeepEqual(got, HashKeeperChoiceSizesReport{}) {
		t.Fatal("invalid input was reflected as a requested canonical ID", got, err)
	}
}
