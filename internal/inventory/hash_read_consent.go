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
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const HashReadApprovalContract = "explicit_full_file_hash_read_v1"
const HashReadApprovalLifetime = 24 * time.Hour
const hashReadByteLimit int64 = 1 << 50

var ErrHashReadConfirmation = errors.New("full-file reads require explicit confirmation and both reservation limits")
var ErrHashReadBinding = errors.New("read approval requires the exact saved store, selection, inventory and manual source locator")
var ErrHashReadLimits = errors.New("approved daily and lifetime reservation limits must be 1–1125899906842624 bytes and include prior lifetime charges")
var ErrHashReadApprovalConflict = errors.New("an immutable read approval already exists; its binding, expiry and limits cannot be replaced")
var ErrHashReadApprovalMissing = errors.New("the exact saved read approval is unavailable")
var ErrHashReadExpired = errors.New("read approval expired; this selection cannot be renewed")
var ErrHashReadRevoked = errors.New("read approval was revoked; this selection cannot be renewed")
var ErrHashReadClockRollback = errors.New("read approval is unavailable because the clock precedes its recorded high-water")

// HashReadApprovalRequest is an explicit owner statement for one exact saved
// proposal. It submits no source data, checkpoint, digest or resume offset.
type HashReadApprovalRequest struct {
	StoreID                   string            `json:"store_id"`
	SelectionID               string            `json:"selection_id"`
	InventoryID               string            `json:"inventory_id"`
	SourceLocator             HashSourceLocator `json:"source_locator"`
	DailyReservedByteLimit    int64             `json:"daily_reserved_byte_limit"`
	LifetimeReservedByteLimit int64             `json:"lifetime_reserved_byte_limit"`
	ConfirmFullFileRead       bool              `json:"confirm_full_file_read"`
}

// HashReadApproval is immutable recorded consent. It never proves current
// source availability or grants cleanup permission. Every dispatcher call must
// separately recheck its lifecycle, charges, current inventory and live input.
type HashReadApproval struct {
	Version                   int               `json:"version"`
	StoreID                   string            `json:"store_id"`
	SelectionID               string            `json:"selection_id"`
	InventoryID               string            `json:"inventory_id"`
	Contract                  string            `json:"contract"`
	SourceLocator             HashSourceLocator `json:"source_locator"`
	CreatedAt                 time.Time         `json:"created_at"`
	ExpiresAt                 time.Time         `json:"expires_at"`
	StepByteLimit             int64             `json:"step_byte_limit"`
	DailyReservedByteLimit    int64             `json:"daily_reserved_byte_limit"`
	LifetimeReservedByteLimit int64             `json:"lifetime_reserved_byte_limit"`
	InitialTotalReservedBytes int64             `json:"initial_total_reserved_bytes"`
	ConfirmFullFileRead       bool              `json:"confirm_full_file_read"`
}

type HashReadRevocation struct {
	ID          string    `json:"id,omitempty"`
	Version     int       `json:"version"`
	StoreID     string    `json:"store_id"`
	SelectionID string    `json:"selection_id"`
	ApprovalID  string    `json:"approval_id"`
	RecordedAt  time.Time `json:"recorded_at"`
}

// HashReadConsent reports saved records only. Status is recorded, revoked or
// expired_observed; even recorded consent leaves current permission unevaluated.
// ExpiresAt and ClockHighWater are evidence, not a current authorization check.
type HashReadConsent struct {
	ID                             string              `json:"id"`
	Status                         string              `json:"status"`
	Approval                       HashReadApproval    `json:"approval"`
	Revocation                     *HashReadRevocation `json:"revocation,omitempty"`
	ClockHighWater                 time.Time           `json:"clock_high_water"`
	ExpiredObserved                bool                `json:"expired_observed"`
	CurrentReadPermissionEvaluated bool                `json:"current_read_permission_evaluated"`
	ProvenanceVerified             bool                `json:"provenance_verified"`
	ContentVerified                bool                `json:"content_verified"`
	CurrentStateVerified           bool                `json:"current_state_verified"`
	DuplicatesVerified             bool                `json:"duplicates_verified"`
	Executable                     bool                `json:"executable"`
	EstimatedReclaimableBytes      *int64              `json:"estimated_reclaimable_bytes"`
}

type hashReadHooks struct {
	beforeApprovalCommit   func()
	afterApprovalCommit    func()
	beforeRevocationCommit func()
	afterRevocationCommit  func()
	commit                 func(*sql.Tx) error // test-only acknowledgment seam for immutable records
}

func commitHashReadRecord(tx *sql.Tx, hooks hashReadHooks) error {
	if hooks.commit != nil {
		return hooks.commit(tx)
	}
	return tx.Commit()
}

func validHashReadClock(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Year() >= 1970 && t.Year() <= 2261 && t.UnixNano() >= 0
}
func validHashReadLimits(day, total int64) bool {
	return day >= 1 && day <= hashReadByteLimit && total >= 1 && total <= hashReadByteLimit
}
func validHashReadLocator(l HashSourceLocator) bool {
	return l.Kind == "manual_inventory_v1" && validHashManualRoot(l.RootPathBytes) && l.InventoryKey == hashManualInventoryKey(l.RootPathBytes)
}
func equalHashReadLocator(a, b HashSourceLocator) bool {
	return a.Kind == b.Kind && a.InventoryKey == b.InventoryKey && bytes.Equal(a.RootPathBytes, b.RootPathBytes)
}
func cloneHashReadLocator(l HashSourceLocator) HashSourceLocator {
	l.RootPathBytes = bytes.Clone(l.RootPathBytes)
	return l
}
func hashReadMaxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func decodeHashReadRecord(payload []byte, id string, out any) error {
	if !hashStoreDigest(id) || id != fmt.Sprintf("%x", sha256.Sum256(payload)) {
		return ErrHashStoreCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrHashStoreCorrupt
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrHashStoreCorrupt
	}
	return nil
}

func hashReadSQLFailure(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrHashStoreCorrupt
}

func (s *HashStore) readHashReadConsent(ctx context.Context, db hashQuery, record *hashSelectionRecord, selectionID string, budget *HashBudget) (*HashReadConsent, error) {
	// A reader may remain open while an exclusive writer upgrades the store.
	// Read the version from this same snapshot instead of the open-time cache.
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, hashReadSQLFailure(ctx, err)
	}
	if version == 1 {
		return nil, nil
	}
	if version != 2 && version != 3 && version != 4 && version != 5 {
		return nil, ErrHashStoreCorrupt
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM (SELECT id FROM hash_read_approval LIMIT 2))+(SELECT count(*) FROM (SELECT id FROM hash_read_revocation LIMIT 2))+(SELECT count(*) FROM (SELECT id FROM hash_read_observation LIMIT 2))`).Scan(&count); err != nil {
		return nil, hashReadSQLFailure(ctx, err)
	}
	var id string
	var payload []byte
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT substr(CAST(approval_id AS BLOB),1,65),substr(CAST(payload AS BLOB),1,8193),typeof(approval_id)='text' AND typeof(payload)='blob' FROM hash_read_approval WHERE id=1`).Scan(&id, &payload, &valid)
	if canceled := ctx.Err(); canceled != nil {
		return nil, canceled
	}
	if errors.Is(err, sql.ErrNoRows) {
		if count != 0 {
			return nil, ErrHashStoreCorrupt
		}
		return nil, nil
	}
	if err != nil {
		return nil, hashReadSQLFailure(ctx, err)
	}
	if !valid || len(payload) > 8192 || record == nil || record.SourceLocator == nil {
		return nil, ErrHashStoreCorrupt
	}
	consent := &HashReadConsent{ID: id, Status: "recorded"}
	a := &consent.Approval
	if decodeHashReadRecord(payload, id, a) != nil || a.Version != 1 || a.StoreID != record.StoreID || a.SelectionID != selectionID || a.InventoryID != record.InventoryID || a.Contract != HashReadApprovalContract || !a.ConfirmFullFileRead || a.StepByteLimit != FileHashStepByteLimit || !validHashReadLocator(a.SourceLocator) || !equalHashReadLocator(a.SourceLocator, *record.SourceLocator) || !validHashReadClock(a.CreatedAt) || !validHashReadClock(a.ExpiresAt) || !a.ExpiresAt.Equal(a.CreatedAt.Add(HashReadApprovalLifetime)) || !validHashReadLimits(a.DailyReservedByteLimit, a.LifetimeReservedByteLimit) || a.InitialTotalReservedBytes < 0 || a.InitialTotalReservedBytes > a.LifetimeReservedByteLimit {
		return nil, ErrHashStoreCorrupt
	}
	if (budget == nil && a.InitialTotalReservedBytes != 0) || (budget != nil && (budget.TotalReservedBytes < a.InitialTotalReservedBytes || budget.TotalReservedBytes > a.LifetimeReservedByteLimit)) {
		return nil, ErrHashStoreCorrupt
	}
	var observationID string
	var maxNow, expired int64
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(approval_id AS BLOB),1,65),CASE WHEN typeof(max_now_ns)='integer' THEN max_now_ns ELSE -1 END,CASE WHEN typeof(expired)='integer' THEN expired ELSE -1 END,typeof(approval_id)='text' AND typeof(max_now_ns)='integer' AND typeof(expired)='integer' FROM hash_read_observation WHERE id=1`).Scan(&observationID, &maxNow, &expired, &valid)
	if err != nil {
		return nil, hashReadSQLFailure(ctx, err)
	}
	consent.ClockHighWater = time.Unix(0, maxNow).UTC()
	consent.ExpiredObserved = expired == 1
	if !valid || observationID != id || maxNow < 0 || !validHashReadClock(consent.ClockHighWater) || consent.ClockHighWater.Before(a.CreatedAt) || (expired != 0 && expired != 1) || consent.ExpiredObserved == consent.ClockHighWater.Before(a.ExpiresAt) {
		return nil, ErrHashStoreCorrupt
	}
	if consent.ExpiredObserved {
		consent.Status = "expired_observed"
	}
	var revokedID string
	var revokedPayload []byte
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(revocation_id AS BLOB),1,65),substr(CAST(payload AS BLOB),1,2049),typeof(revocation_id)='text' AND typeof(payload)='blob' FROM hash_read_revocation WHERE id=1`).Scan(&revokedID, &revokedPayload, &valid)
	if canceled := ctx.Err(); canceled != nil {
		return nil, canceled
	}
	if errors.Is(err, sql.ErrNoRows) {
		if count != 2 {
			return nil, ErrHashStoreCorrupt
		}
		return consent, nil
	}
	if err != nil {
		return nil, hashReadSQLFailure(ctx, err)
	}
	if !valid || len(revokedPayload) > 2048 || count != 3 {
		return nil, ErrHashStoreCorrupt
	}
	r := &HashReadRevocation{}
	if decodeHashReadRecord(revokedPayload, revokedID, r) != nil || r.ID != "" || r.Version != 1 || r.StoreID != a.StoreID || r.SelectionID != a.SelectionID || r.ApprovalID != id || !validHashReadClock(r.RecordedAt) || r.RecordedAt.Before(a.CreatedAt) || r.RecordedAt.After(consent.ClockHighWater) {
		return nil, ErrHashStoreCorrupt
	}
	r.ID = revokedID
	consent.Revocation = r
	consent.Status = "revoked"
	return consent, nil
}

// Approval reads only existing local saved records. It neither advances clock
// observations nor evaluates current permission, and works with offline inputs.
func (s *HashStore) Approval(ctx context.Context, id string) (HashReadConsent, error) {
	if !hashStoreDigest(id) {
		return HashReadConsent{}, ErrHashReadApprovalMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashReadConsent{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashReadConsent{}, err
	}
	defer tx.Rollback()
	snapshot, _, err := s.readHashSnapshot(ctx, tx)
	if err != nil {
		return HashReadConsent{}, err
	}
	if snapshot.ReadConsent == nil || snapshot.ReadConsent.ID != id {
		return HashReadConsent{}, ErrHashReadApprovalMissing
	}
	return *snapshot.ReadConsent, tx.Commit()
}

// ApproveRead records one explicitly confirmed, exact saved proposal with fixed
// expiry/step size. It performs no source reads, inventory checks or recovery.
func (s *HashStore) ApproveRead(ctx context.Context, req HashReadApprovalRequest) (HashReadConsent, error) {
	return s.approveRead(ctx, req, hashReadHooks{})
}
func (s *HashStore) approveRead(ctx context.Context, req HashReadApprovalRequest, hooks hashReadHooks) (HashReadConsent, error) {
	if !req.ConfirmFullFileRead {
		return HashReadConsent{}, ErrHashReadConfirmation
	}
	if !validHashReadLimits(req.DailyReservedByteLimit, req.LifetimeReservedByteLimit) {
		return HashReadConsent{}, ErrHashReadLimits
	}
	if !hashStoreDigest(req.StoreID) || !hashStoreDigest(req.SelectionID) || !hashStoreDigest(req.InventoryID) || !validHashReadLocator(req.SourceLocator) {
		return HashReadConsent{}, ErrHashReadBinding
	}
	req.SourceLocator = cloneHashReadLocator(req.SourceLocator)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return HashReadConsent{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkHashStorage(nil); err != nil {
		return HashReadConsent{}, err
	}
	snapshot, record, err := s.readHashSnapshot(ctx, s.db)
	if err != nil {
		return HashReadConsent{}, err
	}
	if record == nil || record.SourceLocator == nil || req.StoreID != record.StoreID || req.SelectionID != snapshot.SelectionID || req.InventoryID != record.InventoryID || !equalHashReadLocator(req.SourceLocator, *record.SourceLocator) {
		return HashReadConsent{}, ErrHashReadBinding
	}
	if previous := snapshot.ReadConsent; previous != nil {
		a := previous.Approval
		if a.DailyReservedByteLimit != req.DailyReservedByteLimit || a.LifetimeReservedByteLimit != req.LifetimeReservedByteLimit {
			return HashReadConsent{}, ErrHashReadApprovalConflict
		}
		return s.observeHashReadApproval(ctx, record, snapshot.SelectionID, snapshot.Budget, previous.ID, s.now().UTC(), hooks.beforeApprovalCommit, hooks.afterApprovalCommit)
	}
	now := s.now().UTC()
	if !validHashReadClock(now) || !validHashReadClock(now.Add(HashReadApprovalLifetime)) || (snapshot.Budget != nil && now.Before(snapshot.Budget.MaxNow)) {
		return HashReadConsent{}, ErrHashReadClockRollback
	}
	initial := int64(0)
	if snapshot.Budget != nil {
		initial = snapshot.Budget.TotalReservedBytes
	}
	if initial > req.LifetimeReservedByteLimit {
		return HashReadConsent{}, ErrHashReadLimits
	}
	a := HashReadApproval{Version: 1, StoreID: req.StoreID, SelectionID: req.SelectionID, InventoryID: req.InventoryID, Contract: HashReadApprovalContract, SourceLocator: req.SourceLocator, CreatedAt: now, ExpiresAt: now.Add(HashReadApprovalLifetime), StepByteLimit: FileHashStepByteLimit, DailyReservedByteLimit: req.DailyReservedByteLimit, LifetimeReservedByteLimit: req.LifetimeReservedByteLimit, InitialTotalReservedBytes: initial, ConfirmFullFileRead: true}
	payload, err := json.Marshal(a)
	if err != nil || len(payload) > 8192 {
		return HashReadConsent{}, ErrHashReadBinding
	}
	id := fmt.Sprintf("%x", sha256.Sum256(payload))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashReadConsent{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_read_approval VALUES(1,?,?)", id, payload); err != nil {
		return HashReadConsent{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_read_observation VALUES(1,?,?,0)", id, now.UnixNano()); err != nil {
		return HashReadConsent{}, err
	}
	if hooks.beforeApprovalCommit != nil {
		hooks.beforeApprovalCommit()
	}
	if err = ctx.Err(); err != nil {
		return HashReadConsent{}, err
	}
	if err = commitHashReadRecord(tx, hooks); err != nil {
		s.poisoned = true
		return HashReadConsent{}, ErrHashRecoveryRequired
	}
	if hooks.afterApprovalCommit != nil {
		hooks.afterApprovalCommit()
	}
	return HashReadConsent{ID: id, Status: "recorded", Approval: a, ClockHighWater: now}, nil
}

// RevokeRead is offline and idempotent. It waits for the writer lock; it cannot
// preempt a dispatcher already holding that lock or a blocked kernel operation.
func (s *HashStore) RevokeRead(ctx context.Context, id string) (HashReadConsent, error) {
	return s.revokeRead(ctx, id, hashReadHooks{})
}
func (s *HashStore) revokeRead(ctx context.Context, id string, hooks hashReadHooks) (HashReadConsent, error) {
	if !hashStoreDigest(id) {
		return HashReadConsent{}, ErrHashReadApprovalMissing
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return HashReadConsent{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkHashStorage(nil); err != nil {
		return HashReadConsent{}, err
	}
	snapshot, record, err := s.readHashSnapshot(ctx, s.db)
	if err != nil {
		return HashReadConsent{}, err
	}
	if snapshot.ReadConsent == nil || snapshot.ReadConsent.ID != id {
		return HashReadConsent{}, ErrHashReadApprovalMissing
	}
	if snapshot.ReadConsent.Revocation != nil {
		return *snapshot.ReadConsent, nil
	}
	now := s.now().UTC()
	high := snapshot.ReadConsent.ClockHighWater
	if validHashReadClock(now) && now.After(high) {
		high = now
	}
	if snapshot.Budget != nil && snapshot.Budget.MaxNow.After(high) {
		high = snapshot.Budget.MaxNow
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashReadConsent{}, err
	}
	defer tx.Rollback()
	consent, observeErr := s.observeHashReadConsent(ctx, tx, record, snapshot.SelectionID, snapshot.Budget, id, high)
	if consent == nil {
		return HashReadConsent{}, observeErr
	}
	// Expiry or a backward wall clock must never obstruct revocation.
	if observeErr != nil && !errors.Is(observeErr, ErrHashReadExpired) && !errors.Is(observeErr, ErrHashReadClockRollback) {
		return HashReadConsent{}, observeErr
	}
	r := HashReadRevocation{Version: 1, StoreID: consent.Approval.StoreID, SelectionID: consent.Approval.SelectionID, ApprovalID: id, RecordedAt: high}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > 2048 {
		return HashReadConsent{}, ErrHashStoreCorrupt
	}
	revokedID := fmt.Sprintf("%x", sha256.Sum256(payload))
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_read_revocation VALUES(1,?,?)", revokedID, payload); err != nil {
		return HashReadConsent{}, err
	}
	if hooks.beforeRevocationCommit != nil {
		hooks.beforeRevocationCommit()
	}
	if err = ctx.Err(); err != nil {
		return HashReadConsent{}, err
	}
	if err = commitHashReadRecord(tx, hooks); err != nil {
		s.poisoned = true
		return HashReadConsent{}, ErrHashRecoveryRequired
	}
	if hooks.afterRevocationCommit != nil {
		hooks.afterRevocationCommit()
	}
	r.ID = revokedID
	consent.Revocation = &r
	consent.Status = "revoked"
	return *consent, nil
}

// observeHashReadConsent must run inside a writer transaction. Callers commit
// lifecycle observations even when the returned guard error refuses a read.
func (s *HashStore) observeHashReadConsent(ctx context.Context, tx *sql.Tx, record *hashSelectionRecord, selectionID string, budget *HashBudget, id string, now time.Time) (*HashReadConsent, error) {
	c, err := s.readHashReadConsent(ctx, tx, record, selectionID, budget)
	if err != nil {
		return nil, err
	}
	if c == nil || c.ID != id {
		return nil, ErrHashReadApprovalMissing
	}
	high := c.ClockHighWater
	if budget != nil && budget.MaxNow.After(high) {
		high = budget.MaxNow
	}
	clockOK := validHashReadClock(now)
	if clockOK && now.After(high) {
		high = now
	}
	expired := c.ExpiredObserved || !high.Before(c.Approval.ExpiresAt)
	r, err := tx.ExecContext(ctx, "UPDATE hash_read_observation SET max_now_ns=?,expired=? WHERE id=1 AND approval_id=? AND max_now_ns=? AND expired=?", high.UnixNano(), expired, c.ID, c.ClockHighWater.UnixNano(), c.ExpiredObserved)
	if err != nil {
		return nil, err
	}
	n, err := r.RowsAffected()
	if err != nil || n != 1 {
		return nil, ErrHashStoreCorrupt
	}
	c.ClockHighWater, c.ExpiredObserved = high, expired
	if c.Revocation != nil {
		return c, ErrHashReadRevoked
	}
	if expired {
		c.Status = "expired_observed"
		return c, ErrHashReadExpired
	}
	if !clockOK || now.Before(high) {
		return c, ErrHashReadClockRollback
	}
	return c, nil
}

func (s *HashStore) observeHashReadApproval(ctx context.Context, record *hashSelectionRecord, selectionID string, budget *HashBudget, id string, now time.Time, before, after func()) (HashReadConsent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashReadConsent{}, err
	}
	defer tx.Rollback()
	c, guardErr := s.observeHashReadConsent(ctx, tx, record, selectionID, budget, id, now)
	if c == nil {
		return HashReadConsent{}, guardErr
	}
	if before != nil {
		before()
	}
	if err = ctx.Err(); err != nil {
		return HashReadConsent{}, err
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		return HashReadConsent{}, ErrHashRecoveryRequired
	}
	if after != nil {
		after()
	}
	return *c, guardErr
}

// RunConsented performs one bounded step through the store-owned consent gate.
// It never expands selection or accepts caller limits/state. This fixture-built
// core provides no CLI, worker rollout or authority for new original-file reads.
func (s *HashStore) RunConsented(ctx context.Context, id string, source *state.Store, scanner *Scanner) (HashRunResult, error) {
	return s.runConsented(ctx, id, source, scanner, hashStoreHooks{})
}
func (s *HashStore) runConsented(ctx context.Context, id string, source *state.Store, scanner *Scanner, hooks hashStoreHooks) (HashRunResult, error) {
	if !hashStoreDigest(id) {
		return HashRunResult{}, ErrHashReadApprovalMissing
	}
	return s.runHashStep(ctx, source, scanner, FileHashStepByteLimit, hashReadByteLimit, hooks, id)
}

func hashReadErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrHashReadExpired):
		return "read_consent_expired"
	case errors.Is(err, ErrHashReadRevoked):
		return "read_consent_revoked"
	case errors.Is(err, ErrHashReadClockRollback):
		return "clock_rollback"
	case errors.Is(err, ErrHashReadApprovalMissing):
		return "read_consent_missing"
	case errors.Is(err, ErrHashReadBinding):
		return "read_consent_required"
	default:
		return "read_consent_unavailable"
	}
}
