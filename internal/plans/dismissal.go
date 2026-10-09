package plans

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const DismissalContract = "saved_finding_dismissal_v1"
const DismissalRequestContract = "saved_finding_dismissal_request_v1"
const DismissalUndoContract = "saved_finding_dismissal_undo_v1"
const DismissalLimit = 128
const DismissalMaxRecordBytes = 256 << 10
const DismissalUndoMaxRecordBytes = 4 << 10
const dismissalPrefix = "dismissal-v1-"
const dismissalRequestPrefix = "dismissal-request-v1-"
const dismissalUndoPrefix = "dismissal-undo-v1-"

var ErrDismissalRequest = errors.New("dismissal requires one exact canonical saved finding request; preview the current saved finding first")
var ErrDismissalID = errors.New("use the full dismissal ID returned by ignore --save")
var ErrDismissalCorrupt = errors.New("saved dismissal storage or evidence is invalid; no finding is hidden")
var ErrDismissalCapacity = errors.New("saved dismissal capacity reached; existing review records are preserved")
var ErrDismissalPublication = errors.New("dismissal publication outcome is uncertain; inspect its ID or repeat the exact request")

type DismissalRequest struct {
	Version                   int                     `json:"version"`
	Contract                  string                  `json:"contract"`
	ID                        string                  `json:"id"`
	ManualRootBytes           []byte                  `json:"manual_root_bytes"`
	Selection                 state.SelectionSnapshot `json:"selection"`
	CurrentStateVerified      bool                    `json:"current_state_verified"`
	ApprovalAvailable         bool                    `json:"approval_available"`
	Executable                bool                    `json:"executable"`
	EstimatedReclaimableBytes *int64                  `json:"estimated_reclaimable_bytes"`
}

type DismissalRecord struct {
	Version   int              `json:"version"`
	Contract  string           `json:"contract"`
	StoreID   string           `json:"store_id"`
	CreatedAt time.Time        `json:"created_at"`
	Status    string           `json:"status"`
	Request   DismissalRequest `json:"request"`
}

type DismissalUndoRecord struct {
	Version     int       `json:"version"`
	Contract    string    `json:"contract"`
	StoreID     string    `json:"store_id"`
	DismissalID string    `json:"dismissal_id"`
	RequestID   string    `json:"request_id"`
	CreatedAt   time.Time `json:"created_at"`
}

type DismissalUndo struct {
	ID     string              `json:"id"`
	Record DismissalUndoRecord `json:"record"`
}

type SavedDismissal struct {
	ID                            string          `json:"id"`
	Status                        string          `json:"status"`
	Record                        DismissalRecord `json:"record"`
	Undo                          *DismissalUndo  `json:"undo,omitempty"`
	CurrentEvidenceMatchEvaluated bool            `json:"current_evidence_match_evaluated"`
	CurrentStateVerified          bool            `json:"current_state_verified"`
	ApprovalAvailable             bool            `json:"approval_available"`
	Executable                    bool            `json:"executable"`
	EstimatedReclaimableBytes     *int64          `json:"estimated_reclaimable_bytes"`
}

func ValidDismissalID(id string) bool        { return validPrefixedID(id, dismissalPrefix) }
func ValidDismissalRequestID(id string) bool { return validPrefixedID(id, dismissalRequestPrefix) }

// NewDismissalRequest freezes one saved finding. It opens no source or storage.
func NewDismissalRequest(root []byte, selection state.SelectionSnapshot) (DismissalRequest, error) {
	if !dismissalRoot(root) {
		return DismissalRequest{}, ErrDismissalRequest
	}
	canonical, err := state.CanonicalDismissalSelection(selection)
	if err != nil || len(canonical.Roots) != 1 || !bytes.Equal(root, canonical.Roots[0].PathBytes) {
		return DismissalRequest{}, ErrDismissalRequest
	}
	r := DismissalRequest{Version: 1, Contract: DismissalRequestContract, ManualRootBytes: bytes.Clone(root), Selection: canonical}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > DismissalMaxRecordBytes-1024 {
		return DismissalRequest{}, ErrDismissalRequest
	}
	r.ID = digest(dismissalRequestPrefix, payload)
	return r, nil
}

func ValidateDismissalRequest(r DismissalRequest) error {
	canonical, err := NewDismissalRequest(r.ManualRootBytes, r.Selection)
	if err != nil || !reflect.DeepEqual(r, canonical) {
		return ErrDismissalRequest
	}
	return nil
}

func dismissalRoot(root []byte) bool {
	p := string(root)
	return len(root) > 0 && len(root) <= 4096 && !strings.ContainsRune(p, 0) && filepath.IsAbs(p) && filepath.Clean(p) == p
}

const dismissalSchema = `
CREATE TABLE dismissal_store_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), token TEXT NOT NULL CHECK(length(token)=64));
INSERT INTO dismissal_store_identity VALUES(1,lower(hex(randomblob(32))));
CREATE TRIGGER dismissal_store_identity_no_update BEFORE UPDATE ON dismissal_store_identity BEGIN SELECT RAISE(ABORT,'dismissal identity is immutable'); END;
CREATE TRIGGER dismissal_store_identity_no_delete BEFORE DELETE ON dismissal_store_identity BEGIN SELECT RAISE(ABORT,'dismissal identity is immutable'); END;
CREATE TABLE finding_dismissals (id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=262144));
CREATE TRIGGER finding_dismissals_no_update BEFORE UPDATE ON finding_dismissals BEGIN SELECT RAISE(ABORT,'dismissals are immutable'); END;
CREATE TRIGGER finding_dismissals_no_delete BEFORE DELETE ON finding_dismissals BEGIN SELECT RAISE(ABORT,'dismissals are immutable'); END;
CREATE TRIGGER finding_dismissals_capacity BEFORE INSERT ON finding_dismissals WHEN (SELECT count(*) FROM finding_dismissals)>=128 BEGIN SELECT RAISE(ABORT,'dismissal capacity reached'); END;
CREATE TABLE finding_dismissal_undos (dismissal_id TEXT PRIMARY KEY REFERENCES finding_dismissals(id), id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=4096));
CREATE TRIGGER finding_dismissal_undos_no_update BEFORE UPDATE ON finding_dismissal_undos BEGIN SELECT RAISE(ABORT,'dismissal undo records are immutable'); END;
CREATE TRIGGER finding_dismissal_undos_no_delete BEFORE DELETE ON finding_dismissal_undos BEGIN SELECT RAISE(ABORT,'dismissal undo records are immutable'); END;
PRAGMA user_version=5;`

type dismissalHooks struct {
	beforeCommit func()
	afterCommit  func()
	commit       func(*sql.Tx) error
}

// SaveDismissal publishes review data only. The caller must compare the exact
// request with its saved inventory first; no cross-database atomicity is claimed.
// FindDismissal permits offline recovery of an already committed exact request.
func SaveDismissal(ctx context.Context, base string, request DismissalRequest) (SavedDismissal, error) {
	return saveDismissal(ctx, base, request, dismissalHooks{})
}

func saveDismissal(ctx context.Context, base string, request DismissalRequest, hooks dismissalHooks) (SavedDismissal, error) {
	if err := ctx.Err(); err != nil {
		return SavedDismissal{}, err
	}
	if err := ValidateDismissalRequest(request); err != nil {
		return SavedDismissal{}, err
	}
	// Re-freeze caller-owned slices before the first filesystem operation.
	request, _ = NewDismissalRequest(request.ManualRootBytes, request.Selection)
	if err := CheckDismissalStorage(ctx, base, []state.SelectionSnapshot{request.Selection}); err != nil {
		return SavedDismissal{}, err
	}
	db, closeDB, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return SavedDismissal{}, err
	}
	if version < 5 {
		if err = migrateJournalTx(ctx, tx, version); err != nil {
			return SavedDismissal{}, err
		}
		if _, err = tx.ExecContext(ctx, dismissalSchema); err != nil {
			return SavedDismissal{}, err
		}
	}
	storeID, err := dismissalStoreIdentity(ctx, tx)
	if err != nil {
		return SavedDismissal{}, err
	}
	if err = dismissalBounds(ctx, tx); err != nil {
		return SavedDismissal{}, err
	}
	existing, err := findDismissal(ctx, tx, storeID, request.ID)
	if err == nil {
		if !reflect.DeepEqual(existing.Record.Request, request) {
			return SavedDismissal{}, ErrDismissalCorrupt
		}
		if err = ctx.Err(); err != nil {
			return SavedDismissal{}, err
		}
		if err = tx.Commit(); err != nil {
			return SavedDismissal{}, err
		}
		return existing, ctx.Err()
	}
	if !errors.Is(err, os.ErrNotExist) {
		return SavedDismissal{}, err
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM finding_dismissals").Scan(&count); err != nil {
		return SavedDismissal{}, err
	}
	if count >= DismissalLimit {
		return SavedDismissal{}, ErrDismissalCapacity
	}
	record := DismissalRecord{Version: 1, Contract: DismissalContract, StoreID: storeID, CreatedAt: time.Now().UTC(), Status: "historical_dismissed", Request: request}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > DismissalMaxRecordBytes {
		return SavedDismissal{}, ErrDismissalRequest
	}
	id := digest(dismissalPrefix, payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO finding_dismissals(id,request_id,payload) VALUES(?,?,?)", id, request.ID, payload); err != nil {
		return SavedDismissal{}, err
	}
	return commitDismissal(ctx, tx, SavedDismissal{ID: id, Status: "dismissed", Record: record}, hooks)
}

func commitDismissal(ctx context.Context, tx *sql.Tx, candidate SavedDismissal, hooks dismissalHooks) (SavedDismissal, error) {
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err := ctx.Err(); err != nil {
		return SavedDismissal{}, err
	}
	commit := tx.Commit
	if hooks.commit != nil {
		commit = func() error { return hooks.commit(tx) }
	}
	if err := commit(); err != nil {
		return candidate, fmt.Errorf("%w: %w", ErrDismissalPublication, err)
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return candidate, ctx.Err()
}

func FindDismissal(ctx context.Context, base, requestID string) (SavedDismissal, error) {
	if !ValidDismissalRequestID(requestID) {
		return SavedDismissal{}, ErrDismissalRequest
	}
	return readDismissal(ctx, base, requestID, true)
}

func ShowDismissal(ctx context.Context, base, id string) (SavedDismissal, error) {
	if !ValidDismissalID(id) {
		return SavedDismissal{}, ErrDismissalID
	}
	return readDismissal(ctx, base, id, false)
}

func readDismissal(ctx context.Context, base, id string, byRequest bool) (SavedDismissal, error) {
	if err := ctx.Err(); err != nil {
		return SavedDismissal{}, err
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer tx.Rollback()
	storeID, err := dismissalReaderIdentity(ctx, tx)
	if err != nil {
		return SavedDismissal{}, err
	}
	var saved SavedDismissal
	if byRequest {
		saved, err = findDismissal(ctx, tx, storeID, id)
	} else {
		saved, err = loadDismissal(ctx, tx, storeID, id)
	}
	if err != nil {
		return SavedDismissal{}, err
	}
	if err = ctx.Err(); err != nil {
		return SavedDismissal{}, err
	}
	if err = tx.Commit(); err != nil {
		return SavedDismissal{}, err
	}
	if err = ctx.Err(); err != nil {
		return SavedDismissal{}, err
	}
	return saved, nil
}

func UndoDismissal(ctx context.Context, base, id string) (SavedDismissal, error) {
	return undoDismissal(ctx, base, id, dismissalHooks{})
}

func undoDismissal(ctx context.Context, base, id string, hooks dismissalHooks) (SavedDismissal, error) {
	if !ValidDismissalID(id) {
		return SavedDismissal{}, ErrDismissalID
	}
	prior, err := ShowDismissal(ctx, base, id)
	if err != nil {
		return SavedDismissal{}, err
	}
	if err = CheckDismissalStorage(ctx, base, []state.SelectionSnapshot{prior.Record.Request.Selection}); err != nil {
		return SavedDismissal{}, err
	}
	db, closeDB, err := openExistingDismissalWriter(ctx, base)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SavedDismissal{}, err
	}
	defer tx.Rollback()
	storeID, err := dismissalReaderIdentity(ctx, tx)
	if err != nil {
		return SavedDismissal{}, err
	}
	saved, err := loadDismissal(ctx, tx, storeID, id)
	if err != nil {
		return SavedDismissal{}, err
	}
	if saved.Undo != nil {
		if err = ctx.Err(); err != nil {
			return SavedDismissal{}, err
		}
		if err = tx.Commit(); err != nil {
			return SavedDismissal{}, err
		}
		return saved, ctx.Err()
	}
	undo := DismissalUndoRecord{Version: 1, Contract: DismissalUndoContract, StoreID: storeID, DismissalID: id, RequestID: saved.Record.Request.ID, CreatedAt: time.Now().UTC()}
	payload, err := json.Marshal(undo)
	if err != nil || len(payload) > DismissalUndoMaxRecordBytes {
		return SavedDismissal{}, ErrDismissalCorrupt
	}
	undoID := digest(dismissalUndoPrefix, payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO finding_dismissal_undos(dismissal_id,id,payload) VALUES(?,?,?)", id, undoID, payload); err != nil {
		return SavedDismissal{}, err
	}
	saved.Undo, saved.Status = &DismissalUndo{ID: undoID, Record: undo}, "undone"
	return commitDismissal(ctx, tx, saved, hooks)
}

// DismissedFindings checks only this existing bounded page. It cannot extend
// the cursor or refill filtered results. Unusable identities remain visible.
func DismissedFindings(ctx context.Context, base string, root []byte, selections []state.SelectionSnapshot) ([]bool, error) {
	if !dismissalRoot(root) || len(selections) > state.PreviewTargetLimit {
		return nil, ErrDismissalRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := CheckDismissalStorage(ctx, base, selections); err != nil {
		return nil, err
	}
	result := make([]bool, len(selections))
	db, closeDB, err := open(ctx, base, false)
	if errors.Is(err, os.ErrNotExist) {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	storeID, err := dismissalReaderIdentity(ctx, tx)
	if errors.Is(err, os.ErrNotExist) {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	for i, selection := range selections {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		request, e := NewDismissalRequest(root, selection)
		if errors.Is(e, ErrDismissalRequest) {
			continue
		}
		if e != nil {
			return nil, e
		}
		saved, e := findDismissal(ctx, tx, storeID, request.ID)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if !reflect.DeepEqual(saved.Record.Request, request) {
			return nil, ErrDismissalCorrupt
		}
		result[i] = saved.Undo == nil
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func dismissalReaderIdentity(ctx context.Context, tx *sql.Tx) (string, error) {
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return "", err
	}
	if version < 5 {
		return "", fmt.Errorf("saved dismissal not found: %w", os.ErrNotExist)
	}
	id, err := dismissalStoreIdentity(ctx, tx)
	if err != nil {
		return "", err
	}
	if err = dismissalBounds(ctx, tx); err != nil {
		return "", err
	}
	return id, nil
}

func dismissalStoreIdentity(ctx context.Context, db queryer) (string, error) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM dismissal_store_identity LIMIT 2)").Scan(&count); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrDismissalCorrupt
	}
	if count != 1 {
		return "", ErrDismissalCorrupt
	}
	var id string
	if err := db.QueryRowContext(ctx, "SELECT substr(token,1,65) FROM dismissal_store_identity WHERE singleton=1").Scan(&id); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrDismissalCorrupt
	}
	if !validDigest(id) {
		return "", ErrDismissalCorrupt
	}
	return id, nil
}

func dismissalBounds(ctx context.Context, tx *sql.Tx) error {
	var records, undos int
	for i, query := range []string{"SELECT count(*) FROM (SELECT 1 FROM finding_dismissals LIMIT 129)", "SELECT count(*) FROM (SELECT 1 FROM finding_dismissal_undos LIMIT 129)"} {
		count := &records
		if i == 1 {
			count = &undos
		}
		if err := tx.QueryRowContext(ctx, query).Scan(count); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrDismissalCorrupt
		}
	}
	if records > DismissalLimit || undos > records {
		return ErrDismissalCorrupt
	}
	return nil
}

func findDismissal(ctx context.Context, tx *sql.Tx, storeID, requestID string) (SavedDismissal, error) {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT substr(id,1,100) FROM finding_dismissals WHERE request_id=?", requestID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return SavedDismissal{}, fmt.Errorf("saved dismissal not found: %w", os.ErrNotExist)
	}
	if err != nil {
		return SavedDismissal{}, err
	}
	saved, err := loadDismissal(ctx, tx, storeID, id)
	if err == nil && saved.Record.Request.ID != requestID {
		return SavedDismissal{}, ErrDismissalCorrupt
	}
	return saved, err
}

func loadDismissal(ctx context.Context, tx *sql.Tx, storeID, id string) (SavedDismissal, error) {
	if !ValidDismissalID(id) {
		return SavedDismissal{}, ErrDismissalCorrupt
	}
	var requestID string
	var payload []byte
	err := tx.QueryRowContext(ctx, "SELECT substr(request_id,1,100),substr(payload,1,?) FROM finding_dismissals WHERE id=?", DismissalMaxRecordBytes+1, id).Scan(&requestID, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return SavedDismissal{}, fmt.Errorf("saved dismissal not found: %w", os.ErrNotExist)
	}
	if err != nil {
		return SavedDismissal{}, err
	}
	var record DismissalRecord
	if len(payload) > DismissalMaxRecordBytes || id != digest(dismissalPrefix, payload) || dismissalDecode(payload, &record) != nil || record.Version != 1 || record.Contract != DismissalContract || record.StoreID != storeID || record.CreatedAt.IsZero() || record.Status != "historical_dismissed" || requestID != record.Request.ID || ValidateDismissalRequest(record.Request) != nil {
		return SavedDismissal{}, ErrDismissalCorrupt
	}
	saved := SavedDismissal{ID: id, Status: "dismissed", Record: record}
	var undoID string
	var undoPayload []byte
	err = tx.QueryRowContext(ctx, "SELECT substr(id,1,100),substr(payload,1,?) FROM finding_dismissal_undos WHERE dismissal_id=?", DismissalUndoMaxRecordBytes+1, id).Scan(&undoID, &undoPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return saved, nil
	}
	if err != nil {
		return SavedDismissal{}, err
	}
	var undo DismissalUndoRecord
	if len(undoPayload) > DismissalUndoMaxRecordBytes || !validPrefixedID(undoID, dismissalUndoPrefix) || undoID != digest(dismissalUndoPrefix, undoPayload) || dismissalDecode(undoPayload, &undo) != nil || undo.Version != 1 || undo.Contract != DismissalUndoContract || undo.StoreID != storeID || undo.DismissalID != id || undo.RequestID != requestID || undo.CreatedAt.IsZero() {
		return SavedDismissal{}, ErrDismissalCorrupt
	}
	saved.Status, saved.Undo = "undone", &DismissalUndo{ID: undoID, Record: undo}
	return saved, nil
}

func dismissalDecode(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrDismissalCorrupt
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrDismissalCorrupt
	}
	return nil
}
