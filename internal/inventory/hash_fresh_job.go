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
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const HashFreshJobContract = "choice_bound_fresh_full_hash_job_v1"
const HashFreshJobLimit = 128
const HashFreshJobMaxRecordBytes = 2 << 20
const hashFreshJobPrefix = "hash-choice-job-v1-"
const hashFreshJobKeyPrefix = "hash-job-key-v1-"

var ErrHashFreshJobKey = errors.New("use one full canonical hash-job-key-v1 key; keys are never generated implicitly during publication")
var ErrHashFreshJobID = errors.New("use one full canonical hash-choice-job-v1 ID")
var ErrHashFreshJobEvidence = errors.New("fresh job publication requires the exact opaque request and its original private storage and frozen evidence")
var ErrHashFreshJobConflict = errors.New("this fresh-job key already belongs to a different exact request")
var ErrHashFreshJobCapacity = errors.New("this hash store already has 128 fresh jobs; no earlier job was removed")
var ErrHashFreshJobCorrupt = errors.New("saved fresh job or its exact initial pending work is invalid; no read or execution is authorized")

// HashFreshJobOriginContext is original whole-selection history observed at
// first publication. It is never refreshed by retries and supplies no fresh
// permission, charge or progress. Archived choice context remains in Request.
type HashFreshJobOriginContext struct {
	Source                string           `json:"source"`
	Contract              string           `json:"contract"`
	StoreID               string           `json:"store_id"`
	SelectionID           string           `json:"selection_id"`
	InventoryID           string           `json:"inventory_id"`
	BudgetScope           string           `json:"budget_scope"`
	SelectedWork          int              `json:"selected_work"`
	CompletedObservations int              `json:"completed_observations"`
	UnfinishedWork        int              `json:"unfinished_work"`
	Budget                *HashBudget      `json:"budget"`
	ReadConsent           *HashReadConsent `json:"read_consent,omitempty"`
}

type HashFreshJobRecord struct {
	Version         int                                `json:"version"`
	Contract        string                             `json:"contract"`
	CreatedAt       time.Time                          `json:"created_at"`
	JobKey          string                             `json:"job_key"`
	Status          string                             `json:"status"`
	Request         HashKeeperChoiceFreshRequestReport `json:"request"`
	OriginalContext HashFreshJobOriginContext          `json:"original_context"`
}

// HashFreshJobWork is a separate zero-initialized seed. This leaf accepts only
// genuine pending rows: no checkpoint, attempt, approval or dispatched work.
// Fresh ordinals are separate from original historical work IDs.
type HashFreshJobWork struct {
	Ordinal          int    `json:"ordinal"`
	HistoricalWorkID string `json:"historical_work_id"`
	Role             string `json:"role"`
	TargetDigest     string `json:"target_digest"`
	Status           string `json:"status"`
	Sequence         int64  `json:"sequence"`
	CheckedOffset    int64  `json:"checked_offset"`
}

type SavedFreshJob struct {
	ID                        string             `json:"id"`
	Record                    HashFreshJobRecord `json:"record"`
	Work                      []HashFreshJobWork `json:"work"`
	FreshReservedBytes        int64              `json:"fresh_reserved_bytes"`
	FreshRequestedBytes       int64              `json:"fresh_requested_bytes"`
	FreshReadBytes            int64              `json:"fresh_read_bytes"`
	ApprovalAvailable         bool               `json:"approval_available"`
	ProvenanceVerified        bool               `json:"provenance_verified"`
	ContentVerified           bool               `json:"content_verified"`
	CurrentStateVerified      bool               `json:"current_state_verified"`
	DuplicatesVerified        bool               `json:"duplicates_verified"`
	Executable                bool               `json:"executable"`
	EstimatedReclaimableBytes *int64             `json:"estimated_reclaimable_bytes"`
}

const hashFreshJobSchema = `
CREATE TABLE hash_fresh_job (
 id TEXT PRIMARY KEY CHECK(typeof(id)='text' AND length(id)=83),
 job_key TEXT UNIQUE NOT NULL CHECK(typeof(job_key)='text' AND length(job_key)=80),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=2097152)
);
CREATE TRIGGER hash_fresh_job_capacity BEFORE INSERT ON hash_fresh_job
 WHEN (SELECT count(*) FROM hash_fresh_job)>=128
 BEGIN SELECT RAISE(ABORT,'fresh job capacity reached'); END;
CREATE TRIGGER hash_fresh_job_no_update BEFORE UPDATE ON hash_fresh_job BEGIN SELECT RAISE(ABORT,'fresh jobs are immutable'); END;
CREATE TRIGGER hash_fresh_job_no_delete BEFORE DELETE ON hash_fresh_job BEGIN SELECT RAISE(ABORT,'fresh jobs are immutable'); END;
CREATE TABLE hash_fresh_work (
 job_id TEXT NOT NULL REFERENCES hash_fresh_job(id),
 ordinal INTEGER NOT NULL CHECK(typeof(ordinal)='integer' AND ordinal BETWEEN 1 AND 20),
 historical_work_id TEXT NOT NULL CHECK(typeof(historical_work_id)='text' AND length(historical_work_id) BETWEEN 1 AND 2),
 role TEXT NOT NULL CHECK(typeof(role)='text' AND role IN ('keeper','copy')),
 target_digest TEXT NOT NULL CHECK(typeof(target_digest)='text' AND length(target_digest)=64),
 status TEXT NOT NULL CHECK(typeof(status)='text' AND status='pending'),
 sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence=0),
 checked_offset INTEGER NOT NULL CHECK(typeof(checked_offset)='integer' AND checked_offset=0),
 PRIMARY KEY(job_id,ordinal)
);
CREATE TRIGGER hash_fresh_work_no_update BEFORE UPDATE ON hash_fresh_work BEGIN SELECT RAISE(ABORT,'fresh initial work is immutable'); END;
CREATE TRIGGER hash_fresh_work_no_delete BEFORE DELETE ON hash_fresh_work BEGIN SELECT RAISE(ABORT,'fresh initial work is immutable'); END;
PRAGMA user_version=4;
`

type hashFreshJobHooks struct {
	beforeCommit func()
	afterCommit  func()
	commit       func(*sql.Tx) error
}

func ValidHashFreshJobKey(key string) bool {
	return len(key) == len(hashFreshJobKeyPrefix)+64 && strings.HasPrefix(key, hashFreshJobKeyPrefix) && hashStoreDigest(key[len(hashFreshJobKeyPrefix):])
}

func ValidHashFreshJobID(id string) bool {
	return len(id) == len(hashFreshJobPrefix)+64 && strings.HasPrefix(id, hashFreshJobPrefix) && hashStoreDigest(id[len(hashFreshJobPrefix):])
}

func NewHashFreshJobKey() (string, error) {
	token, err := hashStoreToken()
	if err != nil {
		return "", err
	}
	return hashFreshJobKeyPrefix + token, nil
}

// OpenHashFreshJobWriter checks the captured original private names and all
// proposal aliases BEFORE SQLite or the lock file is opened. It opens only
// existing storage, migrates/reconciles nothing and stays bound to this opaque
// request. Its selection-only mode cannot dispatch original hashing work.
func OpenHashFreshJobWriter(ctx context.Context, request *KeeperChoiceFreshRequest) (*HashStore, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if request == nil || request.core == nil || validateHashFreshJobRequest(request.core.report) != nil {
		return nil, ErrHashFreshJobEvidence
	}
	lockID, err := checkHashFreshJobStorage(ctx, request.core, "")
	if err != nil {
		return nil, err
	}
	s, err := openHashStoreMigrationMode(ctx, request.core.base, false, true, true, false)
	if err != nil {
		return nil, err
	}
	s.freshJobRequest = request.core
	s.freshJobLockID, err = checkHashFreshJobStorage(ctx, request.core, lockID)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	_, _, err = s.rebuildHashFreshJobRequest(ctx, tx, request.core.report)
	if err == nil {
		_, err = checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID)
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err == nil {
		_, err = checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID)
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func checkHashFreshJobStorage(ctx context.Context, core *keeperChoiceFreshRequestCore, expectedLock string) (string, error) {
	if err := core.checkStorage(ctx); err != nil {
		return "", err
	}
	path := filepath.Join(core.base, "hashes", "writer.lock")
	if err := hashPrivateFile(path); err != nil {
		return "", err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return "", err
	}
	id := objectID(st)
	if core.protectedIDs[id] || expectedLock != "" && id != expectedLock {
		return "", ErrHashFreshJobEvidence
	}
	return id, ctx.Err()
}

func (s *HashStore) SaveFreshJob(ctx context.Context, request *KeeperChoiceFreshRequest, key string) (SavedFreshJob, error) {
	return s.saveFreshJob(ctx, request, key, hashFreshJobHooks{})
}

func (s *HashStore) saveFreshJob(ctx context.Context, request *KeeperChoiceFreshRequest, key string, hooks hashFreshJobHooks) (SavedFreshJob, error) {
	if !ValidHashFreshJobKey(key) {
		return SavedFreshJob{}, ErrHashFreshJobKey
	}
	if s == nil || request == nil || request.core == nil || s.freshJobRequest != request.core || validateHashFreshJobRequest(request.core.report) != nil {
		return SavedFreshJob{}, ErrHashFreshJobEvidence
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return SavedFreshJob{}, err
	}
	defer s.mu.Unlock()
	if _, err := checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID); err != nil {
		return SavedFreshJob{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SavedFreshJob{}, err
	}
	defer tx.Rollback()
	rebuilt, snapshot, err := s.rebuildHashFreshJobRequest(ctx, tx, request.core.report)
	if err != nil {
		return SavedFreshJob{}, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version < 3 || version > 4 {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, ErrHashFreshJobCorrupt)
	}
	if version == 4 {
		var id string
		err = tx.QueryRowContext(ctx, "SELECT substr(CAST(id AS BLOB),1,84) FROM hash_fresh_job WHERE job_key=?", key).Scan(&id)
		if err == nil {
			saved, loadErr := s.readFreshJob(ctx, tx, id)
			if loadErr != nil {
				return SavedFreshJob{}, loadErr
			}
			if !equalHashFreshJobRequests(saved.Record.Request, rebuilt) {
				return SavedFreshJob{}, ErrHashFreshJobConflict
			}
			if _, err := checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID); err != nil {
				return SavedFreshJob{}, err
			}
			if err := ctx.Err(); err != nil {
				return saved, fmt.Errorf("fresh job %s with key %s was already saved; inspect it or repeat the exact key/request: %w", saved.ID, key, err)
			}
			return saved, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
		}
		count, countErr := hashFreshJobCount(ctx, tx)
		if countErr != nil {
			return SavedFreshJob{}, countErr
		}
		if count == HashFreshJobLimit {
			return SavedFreshJob{}, ErrHashFreshJobCapacity
		}
	}
	record := HashFreshJobRecord{Version: 1, Contract: HashFreshJobContract, CreatedAt: s.now().UTC(), JobKey: key, Status: "unapproved", Request: rebuilt, OriginalContext: hashFreshJobContext(snapshot)}
	if validateHashFreshJobRecord(record) != nil {
		return SavedFreshJob{}, ErrHashFreshJobEvidence
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > HashFreshJobMaxRecordBytes {
		return SavedFreshJob{}, ErrHashFreshJobEvidence
	}
	if version == 3 {
		if _, err = tx.ExecContext(ctx, hashFreshJobSchema); err != nil {
			return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
		}
	}
	id := hashFreshJobID(payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_job(id,job_key,payload) VALUES(?,?,?)", id, key, payload); err != nil {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	for i, target := range rebuilt.Targets {
		digest, digestErr := hashTargetDigest(target.Target)
		if digestErr != nil {
			return SavedFreshJob{}, ErrHashFreshJobEvidence
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_work VALUES(?,?,?,?,?,'pending',0,0)", id, i+1, target.Observation.WorkID, target.Role, digest); err != nil {
			return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
		}
	}
	saved, err := s.readFreshJob(ctx, tx, id)
	if err != nil {
		return SavedFreshJob{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if _, err = checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID); err != nil {
		return SavedFreshJob{}, err
	}
	if hooks.commit != nil {
		err = hooks.commit(tx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		s.poisoned = true
		return saved, fmt.Errorf("fresh job %s with key %s publication is uncertain; close storage, inspect this ID or repeat the exact key/request: %w", id, key, ErrHashRecoveryRequired)
	}
	s.schemaVersion = 4
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	if _, err = checkHashFreshJobStorage(ctx, request.core, s.freshJobLockID); err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return saved, fmt.Errorf("fresh job %s with key %s was saved before cancellation; inspect it or repeat the exact key/request: %w", id, key, canceled)
		}
		s.poisoned = true
		return saved, fmt.Errorf("fresh job %s with key %s was saved but storage became uncertain; close and inspect it: %w", id, key, ErrHashRecoveryRequired)
	}
	return saved, nil
}

func hashFreshJobID(payload []byte) string {
	return fmt.Sprintf("%s%x", hashFreshJobPrefix, sha256.Sum256(payload))
}

func hashFreshJobContext(snapshot HashSnapshot) HashFreshJobOriginContext {
	context := HashFreshJobOriginContext{Source: snapshot.Source, Contract: snapshot.Contract, StoreID: snapshot.StoreID,
		SelectionID: snapshot.SelectionID, InventoryID: snapshot.InventoryID, BudgetScope: "whole_original_selection",
		SelectedWork: len(snapshot.Work), Budget: snapshot.Budget, ReadConsent: cloneHashKeeperChoiceFreshConsent(snapshot.ReadConsent)}
	for _, work := range snapshot.Work {
		if work.Status == "complete" {
			context.CompletedObservations++
		}
	}
	context.UnfinishedWork = context.SelectedWork - context.CompletedObservations
	return context
}

// Rebuild exactly the captured request inside the caller's saved transaction.
// Mutable original context is returned separately for first publication only.
func (s *HashStore) rebuildHashFreshJobRequest(ctx context.Context, db hashQuery, expected HashKeeperChoiceFreshRequestReport) (HashKeeperChoiceFreshRequestReport, HashSnapshot, error) {
	choice, err := s.readHashKeeperChoice(ctx, db, expected.ChoiceID)
	if err != nil {
		return HashKeeperChoiceFreshRequestReport{}, HashSnapshot{}, err
	}
	snapshot, selection, err := s.readHashSnapshot(ctx, db)
	if err != nil {
		return HashKeeperChoiceFreshRequestReport{}, HashSnapshot{}, err
	}
	if selection == nil || selection.SourceLocator == nil || !validHashReadLocator(*selection.SourceLocator) {
		return HashKeeperChoiceFreshRequestReport{}, HashSnapshot{}, ErrHashFreshJobEvidence
	}
	p := choice.Record.Evidence
	current := HashKeeperChoiceFreshRequestReport{Version: 1, Contract: HashKeeperChoiceFreshRequestContract, HashContract: FileHashContract,
		Source: "saved_hash_choice", Status: "unapproved", ChoiceID: choice.ID, StoreID: p.StoreID, SelectionID: p.SelectionID,
		InventoryID: p.InventoryID, SourceLocator: cloneHashReadLocator(*selection.SourceLocator), HistoricalChoice: choice,
		Targets: make([]HashKeeperChoiceFreshRequestTarget, 0, len(p.Copies)+1)}
	for i, member := range append([]SavedHashPreviewMember{p.Keeper}, p.Copies...) {
		ordinal, parseErr := strconv.Atoi(member.WorkID)
		if parseErr != nil || ordinal < 1 || ordinal > len(selection.Targets) {
			return HashKeeperChoiceFreshRequestReport{}, HashSnapshot{}, ErrHashFreshJobEvidence
		}
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		current.Targets = append(current.Targets, HashKeeperChoiceFreshRequestTarget{Role: role, Observation: member, Target: cloneHashTarget(selection.Targets[ordinal-1])})
	}
	current.RequestID, err = hashKeeperChoiceFreshRequestID(current)
	if err != nil || validateHashFreshJobRequest(current) != nil || !equalHashFreshJobRequests(current, expected) {
		return HashKeeperChoiceFreshRequestReport{}, HashSnapshot{}, ErrHashFreshJobEvidence
	}
	return current, snapshot, nil
}

func equalHashFreshJobRequests(a, b HashKeeperChoiceFreshRequestReport) bool {
	left, e1 := json.Marshal(a)
	right, e2 := json.Marshal(b)
	return e1 == nil && e2 == nil && bytes.Equal(left, right)
}

func validateHashFreshJobRequest(r HashKeeperChoiceFreshRequestReport) error {
	if r.Version != 1 || r.Contract != HashKeeperChoiceFreshRequestContract || r.HashContract != FileHashContract || r.Source != "saved_hash_choice" || r.Status != "unapproved" || !ValidHashKeeperChoiceFreshRequestID(r.RequestID) || !ValidHashKeeperChoiceID(r.ChoiceID) || !hashStoreDigest(r.StoreID) || !hashStoreDigest(r.SelectionID) || !hashStoreDigest(r.InventoryID) || !validHashReadLocator(r.SourceLocator) || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil || r.HistoricalChoice.ID != r.ChoiceID || len(r.Targets) < 2 || len(r.Targets) > FileSampleTargetLimit {
		return ErrHashFreshJobEvidence
	}
	archived := r.HistoricalChoice.Record
	p := archived.Evidence
	payload, err := json.Marshal(archived)
	if err != nil || len(payload) > HashKeeperChoiceMaxRecordBytes || validateHashKeeperChoiceRecord(archived) != nil || hashKeeperChoiceID(payload) != r.ChoiceID || p.StoreID != r.StoreID || p.SelectionID != r.SelectionID || p.InventoryID != r.InventoryID || len(r.Targets) != len(p.Copies)+1 {
		return ErrHashFreshJobEvidence
	}
	targets := make([]SavedFileTarget, len(r.Targets))
	members := append([]SavedHashPreviewMember{p.Keeper}, p.Copies...)
	for i, target := range r.Targets {
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		file := target.Target.File
		if target.Role != role || !reflect.DeepEqual(target.Observation, members[i]) || !bytes.Equal(target.Target.Root.PathBytes, r.SourceLocator.RootPathBytes) || target.Target.InventoryID != r.InventoryID || file.ID != members[i].FileID || file.RootID != members[i].RootID || !bytes.Equal(file.PathBytes, members[i].PathBytes) || file.Size != p.LogicalBytes || file.Device != members[i].SavedDevice || file.Inode != members[i].SavedInode || file.ChangedNS != members[i].SavedChangedNS || !file.ModifiedAt.Equal(members[i].SavedModifiedAt) || file.Allocated != members[i].SavedAllocatedBytes {
			return ErrHashFreshJobEvidence
		}
		if _, err := hashTargetDigest(target.Target); err != nil {
			return ErrHashFreshJobEvidence
		}
		targets[i] = target.Target
	}
	if boundedSampleRequest(targets) != nil {
		return ErrHashFreshJobEvidence
	}
	id, err := hashKeeperChoiceFreshRequestID(r)
	if err != nil || id != r.RequestID {
		return ErrHashFreshJobEvidence
	}
	return nil
}

func validateHashFreshJobRecord(record HashFreshJobRecord) error {
	if record.Version != 1 || record.Contract != HashFreshJobContract || !validHashReadClock(record.CreatedAt) || !ValidHashFreshJobKey(record.JobKey) || record.Status != "unapproved" || validateHashFreshJobRequest(record.Request) != nil {
		return ErrHashFreshJobCorrupt
	}
	r, c := record.Request, record.OriginalContext
	p := r.HistoricalChoice.Record.Evidence
	if c.Source != "saved_hash_observations" || c.Contract != FileHashContract || c.StoreID != r.StoreID || c.SelectionID != r.SelectionID || c.InventoryID != r.InventoryID || c.BudgetScope != "whole_original_selection" || c.SelectedWork != p.SelectedWork || c.CompletedObservations < p.CompletedObservations || c.CompletedObservations > c.SelectedWork || c.UnfinishedWork != c.SelectedWork-c.CompletedObservations || c.Budget == nil || !validHashKeeperChoiceBudget(c.Budget) {
		return ErrHashFreshJobCorrupt
	}
	// Validate monotone first-publication context against the archived choice,
	// never against mutable present-day context. Daily counters may reset, but
	// lifetime charges, completed heads and consent lifecycle cannot regress.
	b := p.Budget
	if c.Budget.MaxNow.Before(b.MaxNow) || c.Budget.TotalReservedBytes < b.TotalReservedBytes || c.Budget.TotalRequestedBytes < b.TotalRequestedBytes || c.Budget.TotalReadBytes < b.TotalReadBytes || c.Budget.TotalUnknownReservedBytes < b.TotalUnknownReservedBytes {
		return ErrHashFreshJobCorrupt
	}
	if c.Budget.Day == b.Day && (c.Budget.ReservedBytes < b.ReservedBytes || c.Budget.RequestedBytes < b.RequestedBytes || c.Budget.ReadBytes < b.ReadBytes || c.Budget.UnknownReservedBytes < b.UnknownReservedBytes) {
		return ErrHashFreshJobCorrupt
	}
	if archived := p.ReadConsent; archived != nil {
		current := c.ReadConsent
		if current == nil || current.ID != archived.ID || !reflect.DeepEqual(current.Approval, archived.Approval) || current.ClockHighWater.Before(archived.ClockHighWater) || archived.ExpiredObserved && !current.ExpiredObserved || archived.Revocation != nil && !reflect.DeepEqual(current.Revocation, archived.Revocation) {
			return ErrHashFreshJobCorrupt
		}
	}
	p.Budget, p.ReadConsent = c.Budget, c.ReadConsent
	if !validHashKeeperChoiceConsent(p) || c.ReadConsent != nil && !equalHashReadLocator(c.ReadConsent.Approval.SourceLocator, r.SourceLocator) {
		return ErrHashFreshJobCorrupt
	}
	return nil
}

// FreshJob reads only saved SQLite evidence. It never initializes, migrates,
// recovers, loads configuration/inventory, evaluates consent or opens sources.
func (s *HashStore) FreshJob(ctx context.Context, id string) (SavedFreshJob, error) {
	if !ValidHashFreshJobID(id) {
		return SavedFreshJob{}, ErrHashFreshJobID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return SavedFreshJob{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkHashStorage(nil); err != nil {
		return SavedFreshJob{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SavedFreshJob{}, err
	}
	defer tx.Rollback()
	saved, err := s.readFreshJob(ctx, tx, id)
	if err != nil {
		return SavedFreshJob{}, err
	}
	if err = tx.Commit(); err != nil {
		return SavedFreshJob{}, err
	}
	if err = s.checkHashStorage(nil); err != nil {
		return SavedFreshJob{}, err
	}
	if err = ctx.Err(); err != nil {
		return SavedFreshJob{}, err
	}
	return saved, nil
}

func (s *HashStore) readFreshJob(ctx context.Context, db hashQuery, id string) (SavedFreshJob, error) {
	if !ValidHashFreshJobID(id) {
		return SavedFreshJob{}, ErrHashFreshJobCorrupt
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	if version >= 1 && version <= 3 {
		return SavedFreshJob{}, fmt.Errorf("saved fresh job %s is unavailable: %w", id, os.ErrNotExist)
	}
	if version != 4 {
		return SavedFreshJob{}, ErrHashFreshJobCorrupt
	}
	if _, err := hashFreshJobCount(ctx, db); err != nil {
		return SavedFreshJob{}, err
	}
	var key string
	var payload []byte
	var valid bool
	err := db.QueryRowContext(ctx, "SELECT substr(CAST(job_key AS BLOB),1,81),substr(CAST(payload AS BLOB),1,2097153),typeof(id)='text' AND typeof(job_key)='text' AND typeof(payload)='blob' FROM hash_fresh_job WHERE id=?", id).Scan(&key, &payload, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return SavedFreshJob{}, fmt.Errorf("saved fresh job %s is unavailable: %w", id, os.ErrNotExist)
	}
	if err != nil {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	if !valid || !ValidHashFreshJobKey(key) || len(payload) > HashFreshJobMaxRecordBytes || hashFreshJobID(payload) != id || validateHashFreshJobJSON(payload) != nil {
		return SavedFreshJob{}, ErrHashFreshJobCorrupt
	}
	var record HashFreshJobRecord
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&record) != nil || d.Decode(new(any)) != io.EOF || validateHashFreshJobRecord(record) != nil || record.JobKey != key {
		return SavedFreshJob{}, ErrHashFreshJobCorrupt
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, payload) {
		return SavedFreshJob{}, ErrHashFreshJobCorrupt
	}
	if _, _, err = s.rebuildHashFreshJobRequest(ctx, db, record.Request); err != nil {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	rows, err := db.QueryContext(ctx, `SELECT CASE WHEN typeof(ordinal)='integer' THEN ordinal ELSE -1 END,substr(CAST(historical_work_id AS BLOB),1,3),substr(CAST(role AS BLOB),1,7),substr(CAST(target_digest AS BLOB),1,65),substr(CAST(status AS BLOB),1,8),CASE WHEN typeof(sequence)='integer' THEN sequence ELSE -1 END,CASE WHEN typeof(checked_offset)='integer' THEN checked_offset ELSE -1 END,typeof(historical_work_id)='text' AND typeof(role)='text' AND typeof(target_digest)='text' AND typeof(status)='text' FROM hash_fresh_work WHERE job_id=? ORDER BY ordinal LIMIT 21`, id)
	if err != nil {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	work := make([]HashFreshJobWork, 0, len(record.Request.Targets))
	for rows.Next() {
		var row HashFreshJobWork
		var valid bool
		if err = rows.Scan(&row.Ordinal, &row.HistoricalWorkID, &row.Role, &row.TargetDigest, &row.Status, &row.Sequence, &row.CheckedOffset, &valid); err != nil {
			break
		}
		i := len(work)
		if !valid || i >= len(record.Request.Targets) || row.Ordinal != i+1 || row.Status != "pending" || row.Sequence != 0 || row.CheckedOffset != 0 {
			err = ErrHashFreshJobCorrupt
			break
		}
		target := record.Request.Targets[i]
		digest, digestErr := hashTargetDigest(target.Target)
		if digestErr != nil || row.HistoricalWorkID != target.Observation.WorkID || row.Role != target.Role || row.TargetDigest != digest {
			err = ErrHashFreshJobCorrupt
			break
		}
		work = append(work, row)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil || len(work) != len(record.Request.Targets) {
		return SavedFreshJob{}, hashFreshJobFailure(ctx, err)
	}
	// The canonical JSON above defines immutable stored bytes. API display
	// strings are reconstructed from authoritative raw path bytes afterward.
	for i := range record.Request.Targets {
		record.Request.Targets[i].Target = cloneHashTarget(record.Request.Targets[i].Target)
	}
	return SavedFreshJob{ID: id, Record: record, Work: work}, nil
}

func hashFreshJobCount(ctx context.Context, db hashQuery) (int, error) {
	var jobs, work, orphans int
	err := db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM (SELECT id FROM hash_fresh_job LIMIT 129)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_work LIMIT 2561))").Scan(&jobs, &work)
	if err != nil || jobs > HashFreshJobLimit || work > HashFreshJobLimit*FileSampleTargetLimit {
		return 0, hashFreshJobFailure(ctx, err)
	}
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT w.job_id FROM hash_fresh_work w LEFT JOIN hash_fresh_job j ON w.job_id=j.id WHERE j.id IS NULL LIMIT 1)").Scan(&orphans)
	if err != nil || orphans != 0 {
		return 0, hashFreshJobFailure(ctx, err)
	}
	return jobs, nil
}

func hashFreshJobFailure(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrHashFreshJobCorrupt
}

// Token preflight bounds nested arrays before decoding into large structs.
// Canonical round-trip and DisallowUnknownFields still define the exact schema.
func validateHashFreshJobJSON(payload []byte) error {
	d := json.NewDecoder(bytes.NewReader(payload))
	d.UseNumber()
	var visit func(int) error
	visit = func(depth int) error {
		if depth > 32 {
			return ErrHashFreshJobCorrupt
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				name, ok := key.(string)
				if err != nil || !ok || len(name) > 128 || seen[name] || len(seen) >= 64 {
					return ErrHashFreshJobCorrupt
				}
				seen[name] = true
				if err := visit(depth + 1); err != nil {
					return err
				}
			}
			close, err := d.Token()
			if err != nil || close != json.Delim('}') {
				return ErrHashFreshJobCorrupt
			}
		case '[':
			count := 0
			for d.More() {
				count++
				if count > state.LivePathDepthLimit+1 {
					return ErrHashFreshJobCorrupt
				}
				if err := visit(depth + 1); err != nil {
					return err
				}
			}
			close, err := d.Token()
			if err != nil || close != json.Delim(']') {
				return ErrHashFreshJobCorrupt
			}
		default:
			return ErrHashFreshJobCorrupt
		}
		return nil
	}
	if err := visit(0); err != nil {
		return ErrHashFreshJobCorrupt
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrHashFreshJobCorrupt
	}
	return nil
}
