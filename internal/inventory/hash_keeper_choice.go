package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const HashKeeperChoiceContract = "historical_hash_choice_v1"
const HashKeeperChoiceLimit = 128
const HashKeeperChoiceMaxRecordBytes = 256 << 10
const hashKeeperChoicePrefix = "hash-choice-v1-"

var ErrHashKeeperChoiceID = errors.New("use the full historical hash choice ID returned when saving a choice")
var ErrHashKeeperChoiceEvidence = errors.New("saved hash choice requires unchanged exact selected historical evidence; no choice was published")
var ErrHashKeeperChoiceCorrupt = errors.New("saved historical hash choice is invalid or does not match its storage and selection; no action is authorized")
var ErrHashKeeperChoiceCapacity = errors.New("this saved selection already has 128 distinct historical choices; no earlier choice was removed")

// HashKeeperChoiceRecord preserves historical owner-specified roles and their
// save-time context. It creates no keep policy, read consent or cleanup power.
type HashKeeperChoiceRecord struct {
	Version   int               `json:"version"`
	Contract  string            `json:"contract"`
	CreatedAt time.Time         `json:"created_at"`
	Status    string            `json:"status"`
	Evidence  HashKeeperPreview `json:"evidence"`
}

type SavedHashKeeperChoice struct {
	ID     string                 `json:"id"`
	Record HashKeeperChoiceRecord `json:"record"`
}

const hashKeeperChoiceSchema = `
CREATE TABLE hash_keeper_choice (
 id TEXT PRIMARY KEY CHECK(typeof(id)='text' AND length(id)=79),
 request_key TEXT UNIQUE NOT NULL CHECK(typeof(request_key)='text' AND length(request_key)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=262144)
);
CREATE TRIGGER hash_keeper_choice_capacity BEFORE INSERT ON hash_keeper_choice
 WHEN (SELECT count(*) FROM hash_keeper_choice)>=128
 BEGIN SELECT RAISE(ABORT,'historical hash choice capacity reached'); END;
CREATE TRIGGER hash_keeper_choice_no_update BEFORE UPDATE ON hash_keeper_choice BEGIN SELECT RAISE(ABORT,'historical hash choices are immutable'); END;
CREATE TRIGGER hash_keeper_choice_no_delete BEFORE DELETE ON hash_keeper_choice BEGIN SELECT RAISE(ABORT,'historical hash choices are immutable'); END;
PRAGMA user_version=3;
`

// Private hooks model cancellation and uncertain commit replies without
// changing the production publication order.
type hashKeeperChoiceHooks struct {
	beforeCommit func()
	afterCommit  func()
	commit       func(*sql.Tx) error
}

func ValidHashKeeperChoiceID(id string) bool {
	return len(id) == len(hashKeeperChoicePrefix)+64 && strings.HasPrefix(id, hashKeeperChoicePrefix) && hashStoreDigest(id[len(hashKeeperChoicePrefix):])
}

func hashKeeperChoiceID(payload []byte) string {
	return fmt.Sprintf("%s%x", hashKeeperChoicePrefix, sha256.Sum256(payload))
}

// The retry key excludes save time and context that may advance while an owner
// reads a preview. Copy order remains part of the exact explicit request.
func hashKeeperChoiceRequestKey(preview HashKeeperPreview) (string, error) {
	request := struct {
		Contract     string                   `json:"contract"`
		StoreID      string                   `json:"store_id"`
		SelectionID  string                   `json:"selection_id"`
		InventoryID  string                   `json:"inventory_id"`
		HashContract string                   `json:"hash_contract"`
		LogicalBytes int64                    `json:"logical_bytes"`
		SHA256       string                   `json:"sha256"`
		Keeper       SavedHashPreviewMember   `json:"keeper"`
		Copies       []SavedHashPreviewMember `json:"copies"`
	}{HashKeeperChoiceContract, preview.StoreID, preview.SelectionID, preview.InventoryID, preview.HashContract, preview.LogicalBytes, preview.SHA256, preview.Keeper, preview.Copies}
	payload, err := json.Marshal(request)
	if err != nil || len(payload) > HashKeeperChoiceMaxRecordBytes {
		return "", ErrHashKeeperChoiceEvidence
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

// SaveKeeperChoice publishes one immutable historical choice. The caller must
// not mutate expected while this call runs. Sources, inventory, configuration,
// interrupted attempts and source-read permission are never consulted.
func (s *HashStore) SaveKeeperChoice(ctx context.Context, expected HashKeeperPreview) (SavedHashKeeperChoice, error) {
	return s.saveKeeperChoice(ctx, expected, hashKeeperChoiceHooks{})
}

func (s *HashStore) saveKeeperChoice(ctx context.Context, expected HashKeeperPreview, hooks hashKeeperChoiceHooks) (SavedHashKeeperChoice, error) {
	if validateHashKeeperChoicePreview(expected) != nil {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceEvidence
	}
	// Clone all caller-owned slices and context before taking the writer lock.
	frozen, err := json.Marshal(expected)
	var cloned HashKeeperPreview
	if err != nil || len(frozen) > HashKeeperChoiceMaxRecordBytes || json.Unmarshal(frozen, &cloned) != nil {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceEvidence
	}
	expected = cloned
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = s.acquire(ctx, true); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	defer s.mu.Unlock()
	if err = s.checkHashStorage(nil); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	defer tx.Rollback()
	copyIDs := make([]string, len(expected.Copies))
	for i, member := range expected.Copies {
		copyIDs[i] = member.WorkID
	}
	current, err := s.readHashKeeperPreview(ctx, tx, expected.SelectionID, expected.Keeper.WorkID, copyIDs)
	if err != nil {
		if errors.Is(err, ErrHashKeeperSelection) || errors.Is(err, ErrHashKeeperIdentity) {
			return SavedHashKeeperChoice{}, fmt.Errorf("%w: %v", ErrHashKeeperChoiceEvidence, err)
		}
		return SavedHashKeeperChoice{}, err
	}
	if !equalHashKeeperChoiceSelection(expected, current) {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceEvidence
	}
	key, err := hashKeeperChoiceRequestKey(current)
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version < 1 || version > 3 {
		return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, ErrHashKeeperChoiceCorrupt)
	}
	if version == 3 {
		var id string
		err = tx.QueryRowContext(ctx, "SELECT substr(CAST(id AS BLOB),1,80) FROM hash_keeper_choice WHERE request_key=?", key).Scan(&id)
		if err == nil {
			saved, loadErr := s.readHashKeeperChoice(ctx, tx, id)
			if loadErr != nil {
				return SavedHashKeeperChoice{}, loadErr
			}
			if !equalHashKeeperChoiceSelection(expected, saved.Record.Evidence) {
				return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
			}
			if err = ctx.Err(); err != nil {
				return saved, fmt.Errorf("choice %s was already saved; inspect this ID or repeat the exact request: %w", saved.ID, err)
			}
			return saved, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
		}
		count, countErr := hashKeeperChoiceCount(ctx, tx)
		if countErr != nil {
			return SavedHashKeeperChoice{}, countErr
		}
		if count == HashKeeperChoiceLimit {
			return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCapacity
		}
	}
	record := HashKeeperChoiceRecord{Version: 1, Contract: HashKeeperChoiceContract, CreatedAt: s.now().UTC(), Status: "historical_unapproved", Evidence: current}
	if validateHashKeeperChoiceRecord(record) != nil {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceEvidence
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > HashKeeperChoiceMaxRecordBytes {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceEvidence
	}
	if version == 1 {
		if _, err = tx.ExecContext(ctx, hashReadSchema); err != nil {
			return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
		}
	}
	if version < 3 {
		if _, err = tx.ExecContext(ctx, hashKeeperChoiceSchema); err != nil {
			return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
		}
	}
	id := hashKeeperChoiceID(payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_keeper_choice(id,request_key,payload) VALUES(?,?,?)", id, key, payload); err != nil {
		return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
	}
	saved, err := s.readHashKeeperChoice(ctx, tx, id)
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	if err = s.checkHashStorage(nil); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	if hooks.commit != nil {
		err = hooks.commit(tx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		s.poisoned = true
		return saved, fmt.Errorf("choice %s publication is uncertain; close storage and inspect this ID or repeat the exact request: %w", id, ErrHashRecoveryRequired)
	}
	s.schemaVersion = 3
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	if err = ctx.Err(); err != nil {
		return saved, fmt.Errorf("choice %s was saved before cancellation; inspect this ID or repeat the exact request: %w", id, err)
	}
	return saved, nil
}

func equalHashKeeperChoiceSelection(a, b HashKeeperPreview) bool {
	return a.StoreID == b.StoreID && a.SelectionID == b.SelectionID && a.InventoryID == b.InventoryID && a.HashContract == b.HashContract && a.LogicalBytes == b.LogicalBytes && a.SHA256 == b.SHA256 && reflect.DeepEqual(a.Keeper, b.Keeper) && reflect.DeepEqual(a.Copies, b.Copies)
}

// KeeperChoice reopens the exact immutable record. Historical coverage, budget
// and consent are not refreshed or treated as current permission.
func (s *HashStore) KeeperChoice(ctx context.Context, id string) (SavedHashKeeperChoice, error) {
	if !ValidHashKeeperChoiceID(id) {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	defer tx.Rollback()
	saved, err := s.readHashKeeperChoice(ctx, tx, id)
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	if err = tx.Commit(); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	if err = ctx.Err(); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	return saved, nil
}

func hashKeeperChoiceCount(ctx context.Context, db hashQuery) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT id FROM hash_keeper_choice LIMIT 129)").Scan(&count); err != nil {
		return 0, hashKeeperChoiceFailure(ctx, err)
	}
	if count > HashKeeperChoiceLimit {
		return 0, ErrHashKeeperChoiceCorrupt
	}
	return count, nil
}

func hashKeeperChoiceFailure(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrHashKeeperChoiceCorrupt
}

func (s *HashStore) readHashKeeperChoice(ctx context.Context, db hashQuery, id string) (SavedHashKeeperChoice, error) {
	if !ValidHashKeeperChoiceID(id) {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	snapshot, selection, err := s.readHashSnapshot(ctx, db)
	if err != nil {
		return SavedHashKeeperChoice{}, err
	}
	var version int
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
	}
	if version < 3 {
		return SavedHashKeeperChoice{}, fmt.Errorf("saved historical choice %s is unavailable: %w", id, os.ErrNotExist)
	}
	if version != 3 {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	if _, err = hashKeeperChoiceCount(ctx, db); err != nil {
		return SavedHashKeeperChoice{}, err
	}
	var key string
	var payload []byte
	var valid bool
	err = db.QueryRowContext(ctx, "SELECT substr(CAST(request_key AS BLOB),1,65),substr(CAST(payload AS BLOB),1,262145),typeof(id)='text' AND typeof(request_key)='text' AND typeof(payload)='blob' FROM hash_keeper_choice WHERE id=?", id).Scan(&key, &payload, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return SavedHashKeeperChoice{}, fmt.Errorf("saved historical choice %s is unavailable: %w", id, os.ErrNotExist)
	}
	if err != nil {
		return SavedHashKeeperChoice{}, hashKeeperChoiceFailure(ctx, err)
	}
	if !valid || len(payload) > HashKeeperChoiceMaxRecordBytes || hashKeeperChoiceID(payload) != id || !hashStoreDigest(key) {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	var record HashKeeperChoiceRecord
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&record) != nil || d.Decode(new(any)) != io.EOF || validateHashKeeperChoiceRecord(record) != nil {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, payload) || validateHashKeeperChoiceBinding(record, snapshot, selection) != nil {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	expectedKey, err := hashKeeperChoiceRequestKey(record.Evidence)
	if err != nil || key != expectedKey {
		return SavedHashKeeperChoice{}, ErrHashKeeperChoiceCorrupt
	}
	return SavedHashKeeperChoice{ID: id, Record: record}, nil
}

func validateHashKeeperChoiceRecord(record HashKeeperChoiceRecord) error {
	if record.Version != 1 || record.Contract != HashKeeperChoiceContract || record.Status != "historical_unapproved" || !validHashReadClock(record.CreatedAt) {
		return ErrHashKeeperChoiceCorrupt
	}
	return validateHashKeeperChoicePreview(record.Evidence)
}

func validateHashKeeperChoicePreview(p HashKeeperPreview) error {
	if p.Source != "saved_hash_observations" || p.Contract != HashKeeperPreviewContract || p.HashContract != FileHashContract || p.Scope != "explicit_saved_subset" || p.BudgetScope != "whole_saved_selection" || !hashStoreDigest(p.StoreID) || !hashStoreDigest(p.InventoryID) || p.LogicalBytes < 0 || !hashStoreDigest(p.SHA256) || p.ApprovalAvailable || p.ProvenanceVerified || p.ContentVerified || p.CurrentStateVerified || p.DuplicatesVerified || p.Executable || p.EstimatedReclaimableBytes != nil || len(p.Copies) < 1 || len(p.Copies) >= FileSampleTargetLimit {
		return ErrHashKeeperChoiceEvidence
	}
	copyIDs := make([]string, len(p.Copies))
	for i, member := range p.Copies {
		copyIDs[i] = member.WorkID
	}
	if validateHashKeeperRequest(p.SelectionID, p.Keeper.WorkID, copyIDs) != nil || p.SelectedWork < len(p.Copies)+1 || p.SelectedWork > FileSampleTargetLimit || p.CompletedObservations < len(p.Copies)+1 || p.CompletedObservations > p.SelectedWork || p.UnfinishedWork != p.SelectedWork-p.CompletedObservations || p.UnmatchedCompletedObservations < 0 || p.UnmatchedCompletedObservations > p.CompletedObservations-len(p.Copies)-1 {
		return ErrHashKeeperChoiceEvidence
	}
	for _, member := range append([]SavedHashPreviewMember{p.Keeper}, p.Copies...) {
		if member.FileID <= 0 || member.RootID <= 0 || len(member.PathBytes) <= 1 || !validHashManualRoot(member.PathBytes) || member.Sequence <= 0 || !validHashReadClock(member.CheckedAt) || !sampleIdentityNumber(member.SavedDevice, false) || !sampleIdentityNumber(member.SavedInode, true) || member.SavedChangedNS <= 0 || member.SavedModifiedAt.IsZero() || member.SavedAllocatedBytes < 0 || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			return ErrHashKeeperChoiceEvidence
		}
	}
	if p.Budget == nil || !validHashKeeperChoiceBudget(p.Budget) || !validHashKeeperChoiceConsent(p) {
		return ErrHashKeeperChoiceEvidence
	}
	return nil
}

func validateHashKeeperChoiceBinding(record HashKeeperChoiceRecord, snapshot HashSnapshot, selection *hashSelectionRecord) error {
	p := record.Evidence
	if selection == nil || p.StoreID != snapshot.StoreID || p.SelectionID != snapshot.SelectionID || p.InventoryID != snapshot.InventoryID || p.SelectedWork != len(selection.Targets) || len(snapshot.Work) != len(selection.Targets) {
		return ErrHashKeeperChoiceCorrupt
	}
	for _, member := range append([]SavedHashPreviewMember{p.Keeper}, p.Copies...) {
		ordinal, _ := strconv.Atoi(member.WorkID)
		if ordinal < 1 || ordinal > len(selection.Targets) {
			return ErrHashKeeperChoiceCorrupt
		}
		file := selection.Targets[ordinal-1].File
		if member.FileID != file.ID || member.RootID != file.RootID || !bytes.Equal(member.PathBytes, file.PathBytes) || p.LogicalBytes != file.Size || member.SavedDevice != file.Device || member.SavedInode != file.Inode || member.SavedChangedNS != file.ChangedNS || !member.SavedModifiedAt.Equal(file.ModifiedAt) || member.SavedAllocatedBytes != file.Allocated {
			return ErrHashKeeperChoiceCorrupt
		}
		// Published complete heads cannot advance through dispatch or recovery.
		// Cross-check their immutable observation binding, without refreshing the
		// archived whole-selection coverage, budget or consent in this choice.
		observation := snapshot.Work[ordinal-1]
		if observation.ID != member.WorkID || observation.Status != "complete" || observation.LogicalBytes != p.LogicalBytes || observation.SHA256 != p.SHA256 || observation.Sequence != member.Sequence || !observation.CheckedAt.Equal(member.CheckedAt) {
			return ErrHashKeeperChoiceCorrupt
		}
		for i, other := range selection.Targets {
			if i != ordinal-1 && other.File.Device == file.Device && other.File.Inode == file.Inode {
				return ErrHashKeeperChoiceCorrupt
			}
		}
	}
	if p.ReadConsent != nil && (selection.SourceLocator == nil || !equalHashReadLocator(p.ReadConsent.Approval.SourceLocator, *selection.SourceLocator)) {
		return ErrHashKeeperChoiceCorrupt
	}
	return nil
}

func validHashKeeperChoiceBudget(b *HashBudget) bool {
	if b == nil {
		return true
	}
	return hashDay(b.Day) && validHashReadClock(b.MaxNow) && b.MaxNow.Format(time.DateOnly) == b.Day && b.ReservedBytes >= 0 && b.RequestedBytes >= 0 && b.ReadBytes >= 0 && b.ReadBytes <= b.RequestedBytes && b.UnknownReservedBytes >= 0 && b.UnknownReservedBytes <= b.ReservedBytes && b.RequestedBytes <= b.ReservedBytes-b.UnknownReservedBytes && b.TotalReservedBytes >= b.ReservedBytes && b.TotalRequestedBytes >= b.RequestedBytes && b.TotalReadBytes >= b.ReadBytes && b.TotalReadBytes <= b.TotalRequestedBytes && b.TotalUnknownReservedBytes >= b.UnknownReservedBytes && b.TotalUnknownReservedBytes <= b.TotalReservedBytes && b.TotalRequestedBytes <= b.TotalReservedBytes-b.TotalUnknownReservedBytes
}

func validHashKeeperChoiceConsent(p HashKeeperPreview) bool {
	c := p.ReadConsent
	if c == nil {
		return true
	}
	a := c.Approval
	if !hashStoreDigest(c.ID) || a.Version != 1 || a.StoreID != p.StoreID || a.SelectionID != p.SelectionID || a.InventoryID != p.InventoryID || a.Contract != HashReadApprovalContract || !validHashReadLocator(a.SourceLocator) || !validHashReadClock(a.CreatedAt) || !validHashReadClock(a.ExpiresAt) || !a.ExpiresAt.Equal(a.CreatedAt.Add(HashReadApprovalLifetime)) || a.StepByteLimit != FileHashStepByteLimit || !validHashReadLimits(a.DailyReservedByteLimit, a.LifetimeReservedByteLimit) || !a.ConfirmFullFileRead || a.InitialTotalReservedBytes < 0 || a.InitialTotalReservedBytes > a.LifetimeReservedByteLimit || !validHashReadClock(c.ClockHighWater) || c.ClockHighWater.Before(a.CreatedAt) || c.ExpiredObserved == c.ClockHighWater.Before(a.ExpiresAt) || c.CurrentReadPermissionEvaluated || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.Executable || c.EstimatedReclaimableBytes != nil {
		return false
	}
	payload, err := json.Marshal(a)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(payload)) != c.ID || p.Budget == nil && a.InitialTotalReservedBytes != 0 || p.Budget != nil && (p.Budget.TotalReservedBytes < a.InitialTotalReservedBytes || p.Budget.TotalReservedBytes > a.LifetimeReservedByteLimit) {
		return false
	}
	status := "recorded"
	if c.ExpiredObserved {
		status = "expired_observed"
	}
	if c.Revocation != nil {
		r := *c.Revocation
		id := r.ID
		r.ID = ""
		payload, err = json.Marshal(r)
		if err != nil || !hashStoreDigest(id) || fmt.Sprintf("%x", sha256.Sum256(payload)) != id || r.Version != 1 || r.StoreID != p.StoreID || r.SelectionID != p.SelectionID || r.ApprovalID != c.ID || !validHashReadClock(r.RecordedAt) || r.RecordedAt.Before(a.CreatedAt) || r.RecordedAt.After(c.ClockHighWater) {
			return false
		}
		status = "revoked"
	}
	return c.Status == status
}
