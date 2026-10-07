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
	"strings"
	"time"
)

const HashFreshReadApprovalContract = "explicit_choice_bound_fresh_full_file_hash_read_v1"
const HashFreshReadApprovalMaxRecordBytes = 16 << 10
const HashFreshReadRevocationMaxRecordBytes = 4 << 10
const hashFreshReadApprovalPrefix = "hash-job-read-v1-"

var ErrHashFreshReadConfirmation = errors.New("fresh full-file reads require explicit confirmation and both reservation limits")
var ErrHashFreshReadLimits = errors.New("fresh daily and lifetime reservation limits must each be 1–1125899906842624 bytes")
var ErrHashFreshReadBinding = errors.New("fresh read consent requires the exact saved job, generation key, request and request-bound private writer")
var ErrHashFreshReadApprovalConflict = errors.New("an immutable fresh read approval already exists; its expiry and reservation limits cannot be replaced")
var ErrHashFreshReadApprovalMissing = errors.New("the exact saved fresh-job read approval is unavailable")
var ErrHashFreshReadCorrupt = errors.New("saved fresh-job consent or its exact job binding is invalid; no read is authorized")

type HashFreshReadApprovalRequest struct {
	JobID                     string `json:"job_id"`
	JobKey                    string `json:"job_key"`
	RequestID                 string `json:"request_id"`
	ConfirmFullFileRead       bool   `json:"confirm_full_file_read"`
	DailyReservedByteLimit    int64  `json:"daily_reserved_byte_limit"`
	LifetimeReservedByteLimit int64  `json:"lifetime_reserved_byte_limit"`
}

// HashFreshReadApproval is immutable consent for one independent fresh job.
// Original history supplies scope only, never fresh allowance or SHA state.
type HashFreshReadApproval struct {
	Version                   int               `json:"version"`
	Contract                  string            `json:"contract"`
	HashContract              string            `json:"hash_contract"`
	JobID                     string            `json:"job_id"`
	JobKey                    string            `json:"job_key"`
	RequestID                 string            `json:"request_id"`
	StoreID                   string            `json:"store_id"`
	SelectionID               string            `json:"selection_id"`
	InventoryID               string            `json:"inventory_id"`
	SourceLocator             HashSourceLocator `json:"source_locator"`
	RoleTargetScopeDigest     string            `json:"role_target_scope_digest"`
	CreatedAt                 time.Time         `json:"created_at"`
	ExpiresAt                 time.Time         `json:"expires_at"`
	StepByteLimit             int64             `json:"step_byte_limit"`
	DailyReservedByteLimit    int64             `json:"daily_reserved_byte_limit"`
	LifetimeReservedByteLimit int64             `json:"lifetime_reserved_byte_limit"`
	InitialTotalReservedBytes int64             `json:"initial_total_reserved_bytes"`
	ConfirmFullFileRead       bool              `json:"confirm_full_file_read"`
}

type HashFreshReadRevocation struct {
	ID         string    `json:"id,omitempty"`
	Version    int       `json:"version"`
	JobID      string    `json:"job_id"`
	JobKey     string    `json:"job_key"`
	RequestID  string    `json:"request_id"`
	ApprovalID string    `json:"approval_id"`
	RecordedAt time.Time `json:"recorded_at"`
}

// HashFreshReadConsent exposes saved lifecycle observations. Even a recorded
// approval leaves current read permission unevaluated and grants no execution
// or cleanup capability. Readers never advance this job's clock.
type HashFreshReadConsent struct {
	ID                             string                   `json:"id"`
	Status                         string                   `json:"status"`
	Approval                       HashFreshReadApproval    `json:"approval"`
	Revocation                     *HashFreshReadRevocation `json:"revocation,omitempty"`
	ClockHighWater                 time.Time                `json:"clock_high_water"`
	ExpiredObserved                bool                     `json:"expired_observed"`
	CurrentReadPermissionEvaluated bool                     `json:"current_read_permission_evaluated"`
	ProvenanceVerified             bool                     `json:"provenance_verified"`
	ContentVerified                bool                     `json:"content_verified"`
	CurrentStateVerified           bool                     `json:"current_state_verified"`
	DuplicatesVerified             bool                     `json:"duplicates_verified"`
	Executable                     bool                     `json:"executable"`
	EstimatedReclaimableBytes      *int64                   `json:"estimated_reclaimable_bytes"`
}

type hashFreshReadHooks struct {
	beforeApprovalCommit   func()
	afterApprovalCommit    func()
	beforeRevocationCommit func()
	afterRevocationCommit  func()
	commit                 func(*sql.Tx) error
}

func ValidHashFreshReadApprovalID(id string) bool {
	return len(id) == len(hashFreshReadApprovalPrefix)+64 && strings.HasPrefix(id, hashFreshReadApprovalPrefix) && hashStoreDigest(id[len(hashFreshReadApprovalPrefix):])
}

func hashFreshReadApprovalID(payload []byte) string {
	return fmt.Sprintf("%s%x", hashFreshReadApprovalPrefix, sha256.Sum256(payload))
}

// Scope digest preserves the exact ordered fresh ordinals, historical roles
// and complete frozen target digests. RequestID separately binds observations.
func hashFreshReadScopeDigest(job SavedFreshJob) (string, error) {
	type scopeTarget struct {
		Ordinal          int    `json:"ordinal"`
		HistoricalWorkID string `json:"historical_work_id"`
		Role             string `json:"role"`
		TargetDigest     string `json:"target_digest"`
	}
	scope := make([]scopeTarget, len(job.Record.Request.Targets))
	for i, target := range job.Record.Request.Targets {
		digest, err := hashTargetDigest(target.Target)
		if err != nil {
			return "", ErrHashFreshReadCorrupt
		}
		scope[i] = scopeTarget{i + 1, target.Observation.WorkID, target.Role, digest}
	}
	payload, err := json.Marshal(scope)
	if err != nil {
		return "", ErrHashFreshReadCorrupt
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

func decodeHashFreshReadRecord(payload []byte, out any) error {
	if validateHashFreshJobJSON(payload) != nil {
		return ErrHashFreshReadCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrHashFreshReadCorrupt
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrHashFreshReadCorrupt
	}
	return nil
}

func hashFreshReadFailure(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrHashFreshReadCorrupt
}

func hashFreshReadCount(ctx context.Context, db hashQuery) error {
	var approvals, revocations, observations, orphan int
	err := db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM (SELECT job_id FROM hash_fresh_read_approval LIMIT 129)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_read_revocation LIMIT 129)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_read_observation LIMIT 129))`).Scan(&approvals, &revocations, &observations)
	if err != nil || approvals > HashFreshJobLimit || revocations > approvals || observations != approvals {
		return hashFreshReadFailure(ctx, err)
	}
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM (
 SELECT a.job_id FROM hash_fresh_read_approval a LEFT JOIN hash_fresh_job j ON a.job_id=j.id WHERE j.id IS NULL
 UNION ALL SELECT r.job_id FROM hash_fresh_read_revocation r LEFT JOIN hash_fresh_read_approval a ON r.job_id=a.job_id WHERE a.job_id IS NULL
 UNION ALL SELECT o.job_id FROM hash_fresh_read_observation o LEFT JOIN hash_fresh_read_approval a ON o.job_id=a.job_id AND o.approval_id=a.approval_id WHERE a.job_id IS NULL
 LIMIT 1)`).Scan(&orphan)
	if err != nil || orphan != 0 {
		return hashFreshReadFailure(ctx, err)
	}
	return nil
}

// This helper does not call readFreshJob: the validated immutable job is its
// caller's input, avoiding recursion when saved jobs include consent evidence.
func (s *HashStore) readHashFreshReadConsent(ctx context.Context, db hashQuery, job SavedFreshJob) (*HashFreshReadConsent, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, hashFreshReadFailure(ctx, err)
	}
	if version == 4 {
		return nil, nil
	}
	if version != 5 || hashFreshReadCount(ctx, db) != nil {
		return nil, hashFreshReadFailure(ctx, nil)
	}
	var id string
	var payload []byte
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT substr(CAST(approval_id AS BLOB),1,82),substr(CAST(payload AS BLOB),1,16385),typeof(job_id)='text' AND typeof(approval_id)='text' AND typeof(payload)='blob' FROM hash_fresh_read_approval WHERE job_id=?`, job.ID).Scan(&id, &payload, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, hashFreshReadFailure(ctx, err)
	}
	if !valid || !ValidHashFreshReadApprovalID(id) || len(payload) > HashFreshReadApprovalMaxRecordBytes || hashFreshReadApprovalID(payload) != id {
		return nil, ErrHashFreshReadCorrupt
	}
	c := &HashFreshReadConsent{ID: id, Status: "recorded"}
	a := &c.Approval
	r := job.Record.Request
	scopeDigest, err := hashFreshReadScopeDigest(job)
	if err != nil || decodeHashFreshReadRecord(payload, a) != nil || a.Version != 1 || a.Contract != HashFreshReadApprovalContract || a.HashContract != FileHashContract || a.JobID != job.ID || a.JobKey != job.Record.JobKey || a.RequestID != r.RequestID || a.StoreID != r.StoreID || a.SelectionID != r.SelectionID || a.InventoryID != r.InventoryID || !equalHashReadLocator(a.SourceLocator, r.SourceLocator) || a.RoleTargetScopeDigest != scopeDigest || !validHashReadClock(a.CreatedAt) || a.CreatedAt.Before(job.Record.CreatedAt) || !validHashReadClock(a.ExpiresAt) || !a.ExpiresAt.Equal(a.CreatedAt.Add(HashReadApprovalLifetime)) || a.StepByteLimit != FileHashStepByteLimit || !validHashReadLimits(a.DailyReservedByteLimit, a.LifetimeReservedByteLimit) || a.InitialTotalReservedBytes != 0 || !a.ConfirmFullFileRead {
		return nil, ErrHashFreshReadCorrupt
	}
	var observedID string
	var high, expired int64
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(approval_id AS BLOB),1,82),CASE WHEN typeof(max_now_ns)='integer' THEN max_now_ns ELSE -1 END,CASE WHEN typeof(expired)='integer' THEN expired ELSE -1 END,typeof(job_id)='text' AND typeof(approval_id)='text' AND typeof(max_now_ns)='integer' AND typeof(expired)='integer' FROM hash_fresh_read_observation WHERE job_id=?`, job.ID).Scan(&observedID, &high, &expired, &valid)
	if err != nil {
		return nil, hashFreshReadFailure(ctx, err)
	}
	c.ClockHighWater, c.ExpiredObserved = time.Unix(0, high).UTC(), expired == 1
	if !valid || observedID != id || !validHashReadClock(c.ClockHighWater) || c.ClockHighWater.Before(a.CreatedAt) || expired != 0 && expired != 1 || c.ExpiredObserved == c.ClockHighWater.Before(a.ExpiresAt) {
		return nil, ErrHashFreshReadCorrupt
	}
	if c.ExpiredObserved {
		c.Status = "expired_observed"
	}
	var revocationID string
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(revocation_id AS BLOB),1,65),substr(CAST(payload AS BLOB),1,4097),typeof(job_id)='text' AND typeof(revocation_id)='text' AND typeof(payload)='blob' FROM hash_fresh_read_revocation WHERE job_id=?`, job.ID).Scan(&revocationID, &payload, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ctx.Err()
	}
	if err != nil {
		return nil, hashFreshReadFailure(ctx, err)
	}
	if !valid || !hashStoreDigest(revocationID) || len(payload) > HashFreshReadRevocationMaxRecordBytes || fmt.Sprintf("%x", sha256.Sum256(payload)) != revocationID {
		return nil, ErrHashFreshReadCorrupt
	}
	revocation := new(HashFreshReadRevocation)
	if decodeHashFreshReadRecord(payload, revocation) != nil || revocation.ID != "" || revocation.Version != 1 || revocation.JobID != job.ID || revocation.JobKey != job.Record.JobKey || revocation.RequestID != r.RequestID || revocation.ApprovalID != id || !validHashReadClock(revocation.RecordedAt) || revocation.RecordedAt.Before(a.CreatedAt) || revocation.RecordedAt.After(c.ClockHighWater) {
		return nil, ErrHashFreshReadCorrupt
	}
	revocation.ID = revocationID
	c.Revocation, c.Status = revocation, "revoked"
	return c, ctx.Err()
}

func (s *HashStore) freshReadBoundJob(ctx context.Context, db hashQuery, jobID, key, requestID string) (SavedFreshJob, error) {
	job, err := s.readFreshJob(ctx, db, jobID)
	if err != nil {
		return SavedFreshJob{}, err
	}
	if s.freshJobRequest == nil || job.Record.JobKey != key || job.Record.Request.RequestID != requestID || !equalHashFreshJobRequests(job.Record.Request, s.freshJobRequest.report) {
		return SavedFreshJob{}, ErrHashFreshReadBinding
	}
	return job, nil
}

func (s *HashStore) freshReadStorage(ctx context.Context) error {
	if s == nil || s.freshJobRequest == nil {
		return ErrHashFreshReadBinding
	}
	_, err := checkHashFreshJobStorage(ctx, s.freshJobRequest, s.freshJobLockID)
	return err
}

// FreshReadApproval reopens saved consent without evaluating the current wall
// clock, configuration, inventory or source. Original IDs are rejected first.
func (s *HashStore) FreshReadApproval(ctx context.Context, id string) (HashFreshReadConsent, error) {
	if !ValidHashFreshReadApprovalID(id) {
		return HashFreshReadConsent{}, ErrHashFreshReadApprovalMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashFreshReadConsent{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkHashStorage(nil); err != nil {
		return HashFreshReadConsent{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	defer tx.Rollback()
	jobID, err := hashFreshReadApprovalJob(ctx, tx, id)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	job, err := s.readFreshJob(ctx, tx, jobID)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	if job.ReadConsent == nil || job.ReadConsent.ID != id {
		return HashFreshReadConsent{}, ErrHashFreshReadCorrupt
	}
	if err = tx.Commit(); err == nil {
		err = s.checkHashStorage(nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	return *job.ReadConsent, nil
}

func hashFreshReadApprovalJob(ctx context.Context, db hashQuery, id string) (string, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return "", hashFreshReadFailure(ctx, err)
	}
	if version >= 1 && version <= 4 {
		return "", ErrHashFreshReadApprovalMissing
	}
	if version != 5 {
		return "", ErrHashFreshReadCorrupt
	}
	var jobID string
	var valid bool
	err := db.QueryRowContext(ctx, "SELECT substr(CAST(job_id AS BLOB),1,84),typeof(job_id)='text' AND typeof(approval_id)='text' FROM hash_fresh_read_approval WHERE approval_id=?", id).Scan(&jobID, &valid)
	if canceled := ctx.Err(); canceled != nil {
		return "", canceled
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrHashFreshReadApprovalMissing
	}
	if err != nil {
		return "", hashFreshReadFailure(ctx, err)
	}
	if !valid || !ValidHashFreshJobID(jobID) {
		return "", ErrHashFreshReadCorrupt
	}
	return jobID, nil
}

func (s *HashStore) ApproveFreshRead(ctx context.Context, req HashFreshReadApprovalRequest) (HashFreshReadConsent, error) {
	return s.approveFreshRead(ctx, req, hashFreshReadHooks{})
}

func (s *HashStore) approveFreshRead(ctx context.Context, req HashFreshReadApprovalRequest, hooks hashFreshReadHooks) (HashFreshReadConsent, error) {
	if !req.ConfirmFullFileRead {
		return HashFreshReadConsent{}, ErrHashFreshReadConfirmation
	}
	if !validHashReadLimits(req.DailyReservedByteLimit, req.LifetimeReservedByteLimit) {
		return HashFreshReadConsent{}, ErrHashFreshReadLimits
	}
	if !ValidHashFreshJobID(req.JobID) || !ValidHashFreshJobKey(req.JobKey) || !ValidHashKeeperChoiceFreshRequestID(req.RequestID) {
		return HashFreshReadConsent{}, ErrHashFreshReadBinding
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return HashFreshReadConsent{}, err
	}
	defer s.mu.Unlock()
	if err := s.freshReadStorage(ctx); err != nil {
		return HashFreshReadConsent{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	defer tx.Rollback()
	job, err := s.freshReadBoundJob(ctx, tx, req.JobID, req.JobKey, req.RequestID)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	if c := job.ReadConsent; c != nil {
		if c.Approval.DailyReservedByteLimit != req.DailyReservedByteLimit || c.Approval.LifetimeReservedByteLimit != req.LifetimeReservedByteLimit {
			return HashFreshReadConsent{}, ErrHashFreshReadApprovalConflict
		}
		guardErr := s.observeHashFreshReadConsent(ctx, tx, c, s.now().UTC())
		if guardErr != nil && !errors.Is(guardErr, ErrHashReadExpired) && !errors.Is(guardErr, ErrHashReadRevoked) && !errors.Is(guardErr, ErrHashReadClockRollback) {
			return HashFreshReadConsent{}, guardErr
		}
		return s.commitFreshRead(ctx, tx, *c, hooks.beforeApprovalCommit, hooks.afterApprovalCommit, hooks.commit, "clock observation", guardErr)
	}
	now := s.now().UTC()
	if !validHashReadClock(now) || !validHashReadClock(now.Add(HashReadApprovalLifetime)) || now.Before(job.Record.CreatedAt) {
		return HashFreshReadConsent{}, ErrHashReadClockRollback
	}
	scope, err := hashFreshReadScopeDigest(job)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	r := job.Record.Request
	a := HashFreshReadApproval{Version: 1, Contract: HashFreshReadApprovalContract, HashContract: FileHashContract, JobID: job.ID, JobKey: job.Record.JobKey,
		RequestID: r.RequestID, StoreID: r.StoreID, SelectionID: r.SelectionID, InventoryID: r.InventoryID, SourceLocator: cloneHashReadLocator(r.SourceLocator), RoleTargetScopeDigest: scope,
		CreatedAt: now, ExpiresAt: now.Add(HashReadApprovalLifetime), StepByteLimit: FileHashStepByteLimit, DailyReservedByteLimit: req.DailyReservedByteLimit,
		LifetimeReservedByteLimit: req.LifetimeReservedByteLimit, ConfirmFullFileRead: true}
	payload, err := json.Marshal(a)
	if err != nil || len(payload) > HashFreshReadApprovalMaxRecordBytes {
		return HashFreshReadConsent{}, ErrHashFreshReadBinding
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
	}
	if version == 4 {
		if _, err = tx.ExecContext(ctx, hashFreshReadSchema); err != nil {
			return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
		}
	} else if version != 5 {
		return HashFreshReadConsent{}, ErrHashFreshReadCorrupt
	}
	id := hashFreshReadApprovalID(payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_read_approval VALUES(?,?,?)", job.ID, id, payload); err != nil {
		return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_read_observation VALUES(?,?,?,0)", job.ID, id, now.UnixNano()); err != nil {
		return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
	}
	c, err := s.readHashFreshReadConsent(ctx, tx, job)
	if err != nil || c == nil {
		return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
	}
	return s.commitFreshRead(ctx, tx, *c, hooks.beforeApprovalCommit, hooks.afterApprovalCommit, hooks.commit, "approval", nil)
}

// Only this fresh job's lifecycle is observed. Refused retries still persist a
// monotone high-water and permanent expiry; original clock/charges are unused.
func (s *HashStore) observeHashFreshReadConsent(ctx context.Context, tx *sql.Tx, c *HashFreshReadConsent, now time.Time) error {
	high := c.ClockHighWater
	clockOK := validHashReadClock(now)
	if clockOK && now.After(high) {
		high = now
	}
	expired := c.ExpiredObserved || !high.Before(c.Approval.ExpiresAt)
	result, err := tx.ExecContext(ctx, "UPDATE hash_fresh_read_observation SET max_now_ns=?,expired=? WHERE job_id=? AND approval_id=? AND max_now_ns=? AND expired=?", high.UnixNano(), expired, c.Approval.JobID, c.ID, c.ClockHighWater.UnixNano(), c.ExpiredObserved)
	if err != nil {
		return hashFreshReadFailure(ctx, err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return hashFreshReadFailure(ctx, err)
	}
	c.ClockHighWater, c.ExpiredObserved = high, expired
	if c.Revocation != nil {
		return ErrHashReadRevoked
	}
	if expired {
		c.Status = "expired_observed"
		return ErrHashReadExpired
	}
	if !clockOK || now.Before(high) {
		return ErrHashReadClockRollback
	}
	return nil
}

// RevokeFreshRead is offline and idempotent. Expiry and clock rollback never
// obstruct revocation. It cannot preempt an operation holding the writer lock.
func (s *HashStore) RevokeFreshRead(ctx context.Context, id string) (HashFreshReadConsent, error) {
	return s.revokeFreshRead(ctx, id, hashFreshReadHooks{})
}

func (s *HashStore) revokeFreshRead(ctx context.Context, id string, hooks hashFreshReadHooks) (HashFreshReadConsent, error) {
	if !ValidHashFreshReadApprovalID(id) {
		return HashFreshReadConsent{}, ErrHashFreshReadApprovalMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return HashFreshReadConsent{}, err
	}
	defer s.mu.Unlock()
	if err := s.freshReadStorage(ctx); err != nil {
		return HashFreshReadConsent{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	defer tx.Rollback()
	jobID, err := hashFreshReadApprovalJob(ctx, tx, id)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	job, err := s.readFreshJob(ctx, tx, jobID)
	if err != nil {
		return HashFreshReadConsent{}, err
	}
	if !equalHashFreshJobRequests(job.Record.Request, s.freshJobRequest.report) {
		return HashFreshReadConsent{}, ErrHashFreshReadBinding
	}
	c := job.ReadConsent
	if c == nil || c.ID != id {
		return HashFreshReadConsent{}, ErrHashFreshReadCorrupt
	}
	if c.Revocation != nil {
		if err := s.freshReadStorage(ctx); err != nil {
			return HashFreshReadConsent{}, err
		}
		return *c, nil
	}
	high := c.ClockHighWater
	if now := s.now().UTC(); validHashReadClock(now) && now.After(high) {
		high = now
	}
	if err = s.observeHashFreshReadConsent(ctx, tx, c, high); err != nil && !errors.Is(err, ErrHashReadExpired) {
		return HashFreshReadConsent{}, err
	}
	r := HashFreshReadRevocation{Version: 1, JobID: job.ID, JobKey: job.Record.JobKey, RequestID: job.Record.Request.RequestID, ApprovalID: id, RecordedAt: high}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > HashFreshReadRevocationMaxRecordBytes {
		return HashFreshReadConsent{}, ErrHashFreshReadCorrupt
	}
	r.ID = fmt.Sprintf("%x", sha256.Sum256(payload))
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_read_revocation VALUES(?,?,?)", job.ID, r.ID, payload); err != nil {
		return HashFreshReadConsent{}, hashFreshReadFailure(ctx, err)
	}
	c.Revocation, c.Status = &r, "revoked"
	return s.commitFreshRead(ctx, tx, *c, hooks.beforeRevocationCommit, hooks.afterRevocationCommit, hooks.commit, "revocation", nil)
}

func (s *HashStore) commitFreshRead(ctx context.Context, tx *sql.Tx, candidate HashFreshReadConsent, before, after func(), commit func(*sql.Tx) error, kind string, guardErr error) (HashFreshReadConsent, error) {
	if before != nil {
		before()
	}
	if err := s.freshReadStorage(ctx); err != nil {
		return HashFreshReadConsent{}, err
	}
	var err error
	if commit != nil {
		err = commit(tx)
	} else {
		err = tx.Commit()
	}
	recordID := candidate.ID
	if kind == "revocation" && candidate.Revocation != nil {
		recordID = candidate.Revocation.ID
	}
	if err != nil {
		s.poisoned = true
		return candidate, fmt.Errorf("fresh read %s %s (approval %s) for job %s with key %s is uncertain; close storage and inspect the exact approval or retry: %w", kind, recordID, candidate.ID, candidate.Approval.JobID, candidate.Approval.JobKey, ErrHashRecoveryRequired)
	}
	s.schemaVersion = 5
	if after != nil {
		after()
	}
	if err = s.freshReadStorage(ctx); err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return candidate, fmt.Errorf("fresh read %s %s (approval %s) for job %s with key %s was saved before cancellation; inspect the exact approval: %w", kind, recordID, candidate.ID, candidate.Approval.JobID, candidate.Approval.JobKey, canceled)
		}
		s.poisoned = true
		return candidate, fmt.Errorf("fresh read %s %s (approval %s) for job %s with key %s was saved but storage became uncertain; close and inspect: %w", kind, recordID, candidate.ID, candidate.Approval.JobID, candidate.Approval.JobKey, ErrHashRecoveryRequired)
	}
	return candidate, guardErr
}
