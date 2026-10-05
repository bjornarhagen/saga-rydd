package plans

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
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const ReviewContract = "same_filesystem_quarantine_review_v1"
const ReviewLifetime = 24 * time.Hour
const maxReviewBytes = 16384

var ErrConfirmation = errors.New("approval requires --confirm-project-review and --confirm-quarantine: confirm owner review of activity, local dependency edits and reinstall requirements, and acceptance of same-filesystem quarantine without purge; review consent expires after 24 hours and cannot execute cleanup")
var ErrReviewEvidence = errors.New("saved evidence changed or is incomplete; review and save a new selection before approval")
var ErrReviewTerminal = errors.New("review approval is revoked, expired or not yet valid; review and save a new selection instead of renewing this plan")
var ErrReviewCorrupt = errors.New("review record is invalid or does not match this plan, store or supported action contract; no action is authorized")

type Confirmations struct {
	ProjectReview bool `json:"project_review"`
	Quarantine    bool `json:"same_filesystem_quarantine_without_purge"`
}

type Approval struct {
	Version       int           `json:"version"`
	PlanID        string        `json:"plan_id"`
	StoreID       string        `json:"store_id"`
	InventoryID   string        `json:"inventory_id"`
	Contract      string        `json:"action_contract"`
	CreatedAt     time.Time     `json:"created_at"`
	ExpiresAt     time.Time     `json:"expires_at"`
	Confirmations Confirmations `json:"owner_confirmations"`
}

type Revocation struct {
	Version    int       `json:"version"`
	PlanID     string    `json:"plan_id"`
	StoreID    string    `json:"store_id"`
	ApprovalID string    `json:"approval_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type Review struct {
	Status               string      `json:"status"`
	ID                   string      `json:"id"`
	Approval             Approval    `json:"approval"`
	Revocation           *Revocation `json:"revocation,omitempty"`
	Executable           bool        `json:"executable"`
	CurrentStateVerified bool        `json:"current_state_verified"`
}

const reviewSchema = `
CREATE TABLE plan_store_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), token TEXT NOT NULL CHECK(length(token)=64));
INSERT INTO plan_store_identity VALUES(1,lower(hex(randomblob(32))));
CREATE TRIGGER plan_store_identity_no_update BEFORE UPDATE ON plan_store_identity BEGIN SELECT RAISE(ABORT,'store identity is immutable'); END;
CREATE TRIGGER plan_store_identity_no_delete BEFORE DELETE ON plan_store_identity BEGIN SELECT RAISE(ABORT,'store identity is immutable'); END;
CREATE TABLE review_approvals (plan_id TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384));
CREATE TRIGGER review_approvals_no_update BEFORE UPDATE ON review_approvals BEGIN SELECT RAISE(ABORT,'approvals are immutable'); END;
CREATE TRIGGER review_approvals_no_delete BEFORE DELETE ON review_approvals BEGIN SELECT RAISE(ABORT,'approvals are immutable'); END;
CREATE TABLE review_revocations (plan_id TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384));
CREATE TRIGGER review_revocations_no_update BEFORE UPDATE ON review_revocations BEGIN SELECT RAISE(ABORT,'revocations are immutable'); END;
CREATE TRIGGER review_revocations_no_delete BEFORE DELETE ON review_revocations BEGIN SELECT RAISE(ABORT,'revocations are immutable'); END;
PRAGMA user_version=2;`

func migrateReviews(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, reviewSchema); err != nil {
		return err
	}
	return tx.Commit()
}

// Show reads frozen evidence and review status in one plan-store snapshot.
// It never rechecks source inventory or presents review consent as execution.
func Show(ctx context.Context, base, id string) (Saved, error) {
	if !ValidID(id) {
		return Saved{}, ErrID
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Saved{}, err
	}
	defer tx.Rollback()
	saved, err := load(ctx, tx, id)
	if err != nil {
		return Saved{}, err
	}
	saved.Review, err = readReview(ctx, tx, saved, time.Now().UTC())
	if err != nil {
		return Saved{}, err
	}
	return saved, tx.Commit()
}

// Approve records explicit review consent only after checking the exact saved
// selection. The inventory can change later; review-v1 can never execute files.
func Approve(ctx context.Context, base, id string, inventory *state.Store, confirmations Confirmations) (Saved, error) {
	if !confirmations.ProjectReview || !confirmations.Quarantine {
		return Saved{}, ErrConfirmation
	}
	saved, err := Load(ctx, base, id)
	if err != nil {
		return Saved{}, err
	}
	if inventory == nil {
		return Saved{}, ErrReviewEvidence
	}
	check, err := inventory.CheckSelection(ctx, saved.Record.Selection)
	if err != nil {
		return Saved{}, err
	}
	if check.Status != "matches_saved_inventory" || len(check.Issues) != 0 {
		return Saved{}, fmt.Errorf("%w (%s)", ErrReviewEvidence, check.Status)
	}
	return approve(ctx, base, id, time.Now().UTC())
}

// approve owns the plan writer lock through load, status check and commit. The
// caller must have obtained owner confirmations and checked saved evidence.
func approve(ctx context.Context, base, id string, now time.Time) (Saved, error) {
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	saved, err := load(ctx, db, id)
	if err != nil {
		return Saved{}, err
	}
	r, err := readReview(ctx, db, saved, now)
	if err != nil {
		return Saved{}, err
	}
	if r != nil {
		if r.Status != "review_approved" {
			return Saved{}, ErrReviewTerminal
		}
		saved.Review = r
		return saved, nil // Retry never extends expiry or creates another approval.
	}
	storeID, err := storeIdentity(ctx, db)
	if err != nil {
		return Saved{}, err
	}
	a := Approval{Version: 1, PlanID: id, StoreID: storeID, InventoryID: saved.Record.Selection.InventoryID,
		Contract: ReviewContract, CreatedAt: now, ExpiresAt: now.Add(ReviewLifetime), Confirmations: Confirmations{true, true}}
	payload, err := json.Marshal(a)
	if err != nil {
		return Saved{}, err
	}
	approvalID := digest("approval-v1-", payload)
	if _, err = db.ExecContext(ctx, "INSERT INTO review_approvals(plan_id,id,payload) VALUES(?,?,?)", id, approvalID, payload); err != nil {
		return Saved{}, err
	}
	saved.Review = &Review{Status: "review_approved", ID: approvalID, Approval: a}
	return saved, nil
}

func Revoke(ctx context.Context, base, id string) (Saved, error) {
	// Do not create or migrate missing/unapproved storage on a failed request.
	before, err := Show(ctx, base, id)
	if err != nil {
		return Saved{}, err
	}
	if before.Review == nil {
		return Saved{}, fmt.Errorf("no review approval for this plan: %w", os.ErrNotExist)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	saved, err := load(ctx, db, id)
	if err != nil {
		return Saved{}, err
	}
	r, err := readReview(ctx, db, saved, time.Now().UTC())
	if err != nil {
		return Saved{}, err
	}
	if r == nil {
		return Saved{}, ErrReviewCorrupt
	}
	if r.Revocation == nil {
		revoked := Revocation{Version: 1, PlanID: id, StoreID: r.Approval.StoreID, ApprovalID: r.ID, CreatedAt: time.Now().UTC()}
		payload, err := json.Marshal(revoked)
		if err != nil {
			return Saved{}, err
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO review_revocations(plan_id,id,payload) VALUES(?,?,?)", id, digest("revocation-v1-", payload), payload); err != nil {
			return Saved{}, err
		}
		r.Revocation = &revoked
	}
	r.Status = "revoked"
	saved.Review = r
	return saved, nil
}

func storeIdentity(ctx context.Context, db queryer) (string, error) {
	var token string
	err := db.QueryRowContext(ctx, "SELECT substr(token,1,65) FROM plan_store_identity WHERE singleton=1").Scan(&token)
	if err != nil {
		return "", err
	}
	if !validDigest(token) {
		return "", ErrReviewCorrupt
	}
	return token, nil
}

func readReview(ctx context.Context, db queryer, saved Saved, now time.Time) (*Review, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version == 1 {
		return nil, nil
	}
	storeID, err := storeIdentity(ctx, db)
	if err != nil {
		return nil, err
	}
	var id, revocationID string
	var payload, revokedPayload []byte
	err = db.QueryRowContext(ctx, "SELECT substr(id,1,100),substr(payload,1,?) FROM review_approvals WHERE plan_id=?", maxReviewBytes+1, saved.ID).Scan(&id, &payload)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	missing := errors.Is(err, sql.ErrNoRows)
	err = db.QueryRowContext(ctx, "SELECT substr(id,1,100),substr(payload,1,?) FROM review_revocations WHERE plan_id=?", maxReviewBytes+1, saved.ID).Scan(&revocationID, &revokedPayload)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if missing {
		if err == nil { // An orphan revocation must never allow new consent.
			return nil, ErrReviewCorrupt
		}
		return nil, nil
	}
	var a Approval
	if decodeReview(payload, id, "approval-v1-", &a) != nil || a.Version != 1 || a.PlanID != saved.ID || a.StoreID != storeID || a.InventoryID != saved.Record.Selection.InventoryID || a.Contract != ReviewContract || saved.Record.Action != "same_filesystem_quarantine" || !a.Confirmations.ProjectReview || !a.Confirmations.Quarantine || a.CreatedAt.IsZero() || !a.ExpiresAt.Equal(a.CreatedAt.Add(ReviewLifetime)) {
		return nil, ErrReviewCorrupt
	}
	r := &Review{Status: "review_approved", ID: id, Approval: a}
	if now.Before(a.CreatedAt) {
		r.Status = "not_yet_valid"
	} else if !now.Before(a.ExpiresAt) {
		r.Status = "expired"
	}
	if err == nil {
		var revoked Revocation
		if decodeReview(revokedPayload, revocationID, "revocation-v1-", &revoked) != nil || revoked.Version != 1 || revoked.PlanID != saved.ID || revoked.StoreID != storeID || revoked.ApprovalID != id || revoked.CreatedAt.IsZero() {
			return nil, ErrReviewCorrupt
		}
		r.Revocation, r.Status = &revoked, "revoked"
	}
	return r, nil
}

func digest(prefix string, payload []byte) string {
	return fmt.Sprintf("%s%x", prefix, sha256.Sum256(payload))
}

func decodeReview(payload []byte, id, prefix string, result any) error {
	if len(payload) > maxReviewBytes || id != digest(prefix, payload) {
		return ErrReviewCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(result); err != nil {
		return ErrReviewCorrupt
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrReviewCorrupt
	}
	return nil
}
