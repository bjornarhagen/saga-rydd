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
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const JournalContract = "preparation_only_v1"
const JournalEventLimit = 16
const MaxJournalBytes = 32 << 10
const JournalPathBytesLimit = 4096
const journalPrefix = "journal-v1-"
const journalEventPrefix = "journal-event-v1-"

var ErrJournalID = errors.New("use the full saved journal intent ID")
var ErrJournalCorrupt = errors.New("journal history is invalid or incomplete; outcome is unknown and no action or retry is authorized")
var ErrJournalConflict = errors.New("journal request key or target already has different immutable preparation history")
var ErrJournalTransition = errors.New("journal event does not follow the exact recorded sequence or supported preparation transition; no action or retry is authorized")
var ErrJournalPreparation = errors.New("journal preparation needs exact saved plan and observation bindings, known identities and distinct bounded paths; no action is authorized")

// JournalIdentity is supplied evidence, not a live filesystem check. Generation
// is the saved inventory generation; it is not a platform inode-generation API.
type JournalIdentity struct {
	Device     string `json:"device"`
	Inode      string `json:"inode"`
	ChangedNS  int64  `json:"ctime_ns"`
	Generation int64  `json:"generation"`
}

type PreparationRequest struct {
	RequestKey              string          `json:"request_key"`
	Kind                    string          `json:"kind"`
	PlanID                  string          `json:"plan_id"`
	ObservationID           string          `json:"observation_id"`
	FindingID               string          `json:"finding_id"`
	OriginalIntentID        string          `json:"original_intent_id"`
	SourcePathBytes         []byte          `json:"source_path_bytes"`
	DestinationPathBytes    []byte          `json:"destination_path_bytes"`
	SourceParent            JournalIdentity `json:"source_parent"`
	SourceObject            JournalIdentity `json:"source_object"`
	DestinationParent       JournalIdentity `json:"destination_parent"`
	DestinationMustBeAbsent bool            `json:"destination_must_be_absent"`
}

type JournalIntentRecord struct {
	Version              int                `json:"version"`
	Contract             string             `json:"preparation_contract"`
	CreatedAt            time.Time          `json:"created_at"`
	StoreID              string             `json:"store_id"`
	InventoryID          string             `json:"inventory_id"`
	Request              PreparationRequest `json:"request"`
	Status               string             `json:"status"`
	Executable           bool               `json:"executable"`
	CurrentStateVerified bool               `json:"current_state_verified"`
}

type JournalIntent struct {
	ID     string              `json:"id"`
	Record JournalIntentRecord `json:"record"`
}

type EventRequest struct {
	RequestKey       string           `json:"request_key"`
	ExpectedSequence int              `json:"expected_sequence"`
	PredecessorID    string           `json:"predecessor_id"`
	Kind             string           `json:"kind"`
	Outcome          string           `json:"outcome"`
	SourceState      string           `json:"source_state"`
	DestinationState string           `json:"destination_state"`
	ObjectIdentity   *JournalIdentity `json:"object_identity"`
}

type JournalEventRecord struct {
	Version              int          `json:"version"`
	IntentID             string       `json:"intent_id"`
	CreatedAt            time.Time    `json:"created_at"`
	Request              EventRequest `json:"request"`
	EvidenceSource       string       `json:"evidence_source"`
	Executable           bool         `json:"executable"`
	CurrentStateVerified bool         `json:"current_state_verified"`
}

type JournalEvent struct {
	ID     string             `json:"id"`
	Record JournalEventRecord `json:"record"`
}

type JournalSnapshot struct {
	Intent               JournalIntent  `json:"intent"`
	Events               []JournalEvent `json:"events"`
	State                string         `json:"state"`
	Executable           bool           `json:"executable"`
	CurrentStateVerified bool           `json:"current_state_verified"`
}

const journalSchema = `
CREATE TABLE journal_intents (id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, plan_id TEXT NOT NULL, observation_id TEXT NOT NULL, finding_id TEXT NOT NULL, original_intent_id TEXT NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=32768), UNIQUE(observation_id,finding_id,kind));
CREATE UNIQUE INDEX journal_restore_once ON journal_intents(original_intent_id) WHERE kind='restore_preparation';
CREATE TRIGGER journal_intents_no_update BEFORE UPDATE ON journal_intents BEGIN SELECT RAISE(ABORT,'journal intents are immutable'); END;
CREATE TRIGGER journal_intents_no_delete BEFORE DELETE ON journal_intents BEGIN SELECT RAISE(ABORT,'journal intents are immutable'); END;
CREATE TABLE journal_events (intent_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 16), id TEXT UNIQUE NOT NULL, request_key TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=32768), PRIMARY KEY(intent_id,sequence));
CREATE TRIGGER journal_events_no_update BEFORE UPDATE ON journal_events BEGIN SELECT RAISE(ABORT,'journal events are immutable'); END;
CREATE TRIGGER journal_events_no_delete BEFORE DELETE ON journal_events BEGIN SELECT RAISE(ABORT,'journal events are immutable'); END;
CREATE TABLE journal_heads (intent_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence BETWEEN 0 AND 16), event_id TEXT NOT NULL, PRIMARY KEY(intent_id,sequence));
CREATE TRIGGER journal_heads_no_update BEFORE UPDATE ON journal_heads BEGIN SELECT RAISE(ABORT,'journal heads are immutable'); END;
CREATE TRIGGER journal_heads_no_delete BEFORE DELETE ON journal_heads BEGIN SELECT RAISE(ABORT,'journal heads are immutable'); END;
PRAGMA user_version=4;`

func migrateJournalTx(ctx context.Context, tx *sql.Tx, version int) error {
	if err := migrateObservationTx(ctx, tx, version); err != nil {
		return err
	}
	if version < 4 {
		_, err := tx.ExecContext(ctx, journalSchema)
		return err
	}
	return nil
}

func ValidJournalID(id string) bool { return validPrefixedID(id, journalPrefix) }
func validPrefixedID(id, prefix string) bool {
	return strings.HasPrefix(id, prefix) && validDigest(strings.TrimPrefix(id, prefix))
}

// PrepareJournal publishes a preparation-only record. It never touches source
// paths, checks live identities, or interprets review-v1 as permission to move.
// A future executable contract needs a distinct intent and consent version.
func PrepareJournal(ctx context.Context, base string, request PreparationRequest) (JournalSnapshot, error) {
	return prepareJournal(ctx, base, request, nil)
}

func prepareJournal(ctx context.Context, base string, request PreparationRequest, beforeCommit func(*sql.Tx) error) (JournalSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return JournalSnapshot{}, err
	}
	if validatePreparation(request) != nil {
		return JournalSnapshot{}, ErrJournalPreparation
	}
	// A failed binding request must not migrate existing private storage.
	if _, _, err := LoadObservation(ctx, base, request.ObservationID); err != nil {
		return JournalSnapshot{}, err
	}
	db, closeDB, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer tx.Rollback()
	observation, saved, err := loadObservation(ctx, tx, request.ObservationID, time.Now().UTC())
	if err != nil {
		return JournalSnapshot{}, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return JournalSnapshot{}, err
	}
	if err = migrateJournalTx(ctx, tx, version); err != nil {
		return JournalSnapshot{}, err
	}
	record := JournalIntentRecord{Version: 1, Contract: JournalContract, CreatedAt: time.Now().UTC(), StoreID: observation.Record.StoreID, InventoryID: observation.Record.InventoryID, Request: request, Status: "prepared"}
	if err = validateJournalBinding(ctx, tx, record, observation, saved); err != nil {
		return JournalSnapshot{}, err
	}
	var id string
	err = tx.QueryRowContext(ctx, "SELECT substr(id,1,100) FROM journal_intents WHERE request_key=?", request.RequestKey).Scan(&id)
	if err == nil {
		existing, e := loadJournal(ctx, tx, id, time.Now().UTC())
		if e != nil {
			return JournalSnapshot{}, e
		}
		if !reflect.DeepEqual(existing.Intent.Record.Request, request) {
			return JournalSnapshot{}, ErrJournalConflict
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return JournalSnapshot{}, err
	}
	var collision int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM journal_intents WHERE (observation_id=? AND finding_id=? AND kind=?) OR (kind='restore_preparation' AND original_intent_id=?)", request.ObservationID, request.FindingID, request.Kind, request.OriginalIntentID).Scan(&collision); err != nil {
		return JournalSnapshot{}, err
	}
	if collision != 0 {
		return JournalSnapshot{}, ErrJournalConflict
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > MaxJournalBytes {
		return JournalSnapshot{}, ErrJournalPreparation
	}
	id = digest(journalPrefix, payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO journal_intents VALUES(?,?,?,?,?,?,?,?)", id, request.RequestKey, request.Kind, request.PlanID, request.ObservationID, request.FindingID, request.OriginalIntentID, payload); err != nil {
		return JournalSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO journal_heads VALUES(?,0,?)", id, id); err != nil {
		return JournalSnapshot{}, err
	}
	snapshot, err := loadJournal(ctx, tx, id, time.Now().UTC())
	if err != nil {
		return JournalSnapshot{}, err
	}
	if beforeCommit != nil {
		if err = beforeCommit(tx); err != nil {
			return JournalSnapshot{}, err
		}
	}
	return snapshot, tx.Commit()
}

// AppendJournalEvent records supplied observations, never verified movement.
// Missing results produce outcome_unknown. A second attempt is never accepted.
func AppendJournalEvent(ctx context.Context, base, intentID string, request EventRequest) (JournalSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return JournalSnapshot{}, err
	}
	if !ValidJournalID(intentID) {
		return JournalSnapshot{}, ErrJournalID
	}
	if validateEventRequest(request) != nil {
		return JournalSnapshot{}, ErrJournalTransition
	}
	// Refuse unsupported/missing history without creating storage or migrating it.
	if _, err := LoadJournal(ctx, base, intentID); err != nil {
		return JournalSnapshot{}, err
	}
	db, closeDB, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := loadJournal(ctx, tx, intentID, time.Now().UTC())
	if err != nil {
		return JournalSnapshot{}, err
	}
	var existingIntent, existingID string
	err = tx.QueryRowContext(ctx, "SELECT substr(intent_id,1,100),substr(id,1,100) FROM journal_events WHERE request_key=?", request.RequestKey).Scan(&existingIntent, &existingID)
	if err == nil {
		if existingIntent != intentID {
			return JournalSnapshot{}, ErrJournalConflict
		}
		for _, event := range snapshot.Events {
			if event.ID == existingID {
				if !reflect.DeepEqual(event.Record.Request, request) {
					return JournalSnapshot{}, ErrJournalConflict
				}
				return snapshot, tx.Commit()
			}
		}
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return JournalSnapshot{}, err
	}
	if err = validateNextEvent(snapshot, request); err != nil {
		return JournalSnapshot{}, err
	}
	record := JournalEventRecord{Version: 1, IntentID: intentID, CreatedAt: time.Now().UTC(), Request: request, EvidenceSource: "caller_supplied_record"}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > MaxJournalBytes {
		return JournalSnapshot{}, ErrJournalTransition
	}
	id := digest(journalEventPrefix, payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO journal_events VALUES(?,?,?,?,?)", intentID, request.ExpectedSequence, id, request.RequestKey, payload); err != nil {
		return JournalSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO journal_heads VALUES(?,?,?)", intentID, request.ExpectedSequence, id); err != nil {
		return JournalSnapshot{}, err
	}
	snapshot, err = loadJournal(ctx, tx, intentID, time.Now().UTC())
	if err != nil {
		return JournalSnapshot{}, err
	}
	return snapshot, tx.Commit()
}

// LoadJournal reads bindings and the entire bounded chain in one SQLite
// snapshot. It never observes original or destination filesystem paths.
func LoadJournal(ctx context.Context, base, intentID string) (JournalSnapshot, error) {
	if !ValidJournalID(intentID) {
		return JournalSnapshot{}, ErrJournalID
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return JournalSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := loadJournal(ctx, tx, intentID, time.Now().UTC())
	if err != nil {
		return JournalSnapshot{}, err
	}
	return snapshot, tx.Commit()
}

type journalQueryer interface {
	queryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadJournal(ctx context.Context, db journalQueryer, id string, now time.Time) (JournalSnapshot, error) {
	return loadJournalAtDepth(ctx, db, id, now, 0)
}

func loadJournalAtDepth(ctx context.Context, db journalQueryer, id string, now time.Time, depth int) (JournalSnapshot, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return JournalSnapshot{}, err
	}
	if version < 4 {
		return JournalSnapshot{}, fmt.Errorf("journal intent not found: %w", os.ErrNotExist)
	}
	var key, kind, planID, observationID, findingID, originalID string
	var payload []byte
	err := db.QueryRowContext(ctx, "SELECT substr(request_key,1,65),substr(kind,1,100),substr(plan_id,1,100),substr(observation_id,1,100),substr(finding_id,1,101),substr(original_intent_id,1,100),substr(payload,1,?) FROM journal_intents WHERE id=?", MaxJournalBytes+1, id).Scan(&key, &kind, &planID, &observationID, &findingID, &originalID, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return JournalSnapshot{}, fmt.Errorf("journal intent not found: %w", os.ErrNotExist)
	}
	if err != nil {
		return JournalSnapshot{}, err
	}
	var r JournalIntentRecord
	if strictJournalDecode(payload, id, journalPrefix, &r) != nil || validateIntent(r, now) != nil {
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	if depth > 0 && r.Request.Kind != "quarantine_preparation" {
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	q := r.Request
	if key != q.RequestKey || kind != q.Kind || planID != q.PlanID || observationID != q.ObservationID || findingID != q.FindingID || originalID != q.OriginalIntentID {
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	observation, saved, err := loadObservation(ctx, db, q.ObservationID, now)
	if err != nil {
		return JournalSnapshot{}, journalReadError(ctx, err)
	}
	if err = validateJournalBinding(ctx, db, r, observation, saved); err != nil {
		return JournalSnapshot{}, journalReadError(ctx, err)
	}
	snapshot := JournalSnapshot{Intent: JournalIntent{ID: id, Record: r}, Events: make([]JournalEvent, 0), State: "prepared"}
	rows, err := db.QueryContext(ctx, "SELECT sequence,substr(event_id,1,100) FROM journal_heads WHERE intent_id=? ORDER BY sequence LIMIT ?", id, JournalEventLimit+2)
	if err != nil {
		return JournalSnapshot{}, err
	}
	heads := make([]string, 0, JournalEventLimit+1)
	for rows.Next() {
		var sequence int
		var eventID string
		if err = rows.Scan(&sequence, &eventID); err != nil {
			rows.Close()
			return JournalSnapshot{}, err
		}
		if sequence != len(heads) || len(heads) > JournalEventLimit {
			rows.Close()
			return JournalSnapshot{}, ErrJournalCorrupt
		}
		heads = append(heads, eventID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return JournalSnapshot{}, err
	}
	if len(heads) == 0 || heads[0] != id {
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	rows, err = db.QueryContext(ctx, "SELECT sequence,substr(id,1,100),substr(request_key,1,65),substr(payload,1,?) FROM journal_events WHERE intent_id=? ORDER BY sequence LIMIT ?", MaxJournalBytes+1, id, JournalEventLimit+1)
	if err != nil {
		return JournalSnapshot{}, err
	}
	lastTime := r.CreatedAt
	for rows.Next() {
		var sequence int
		var eventID, eventKey string
		var eventPayload []byte
		if err = rows.Scan(&sequence, &eventID, &eventKey, &eventPayload); err != nil {
			rows.Close()
			return JournalSnapshot{}, err
		}
		var event JournalEventRecord
		if sequence != len(snapshot.Events)+1 || sequence > JournalEventLimit || sequence >= len(heads) || heads[sequence] != eventID || strictJournalDecode(eventPayload, eventID, journalEventPrefix, &event) != nil || event.Version != 1 || event.IntentID != id || event.Request.RequestKey != eventKey || event.Request.ExpectedSequence != sequence || event.EvidenceSource != "caller_supplied_record" || event.Executable || event.CurrentStateVerified || event.CreatedAt.IsZero() || event.CreatedAt.Before(lastTime) || event.CreatedAt.After(now) || validateNextEvent(snapshot, event.Request) != nil {
			rows.Close()
			return JournalSnapshot{}, ErrJournalCorrupt
		}
		snapshot.Events = append(snapshot.Events, JournalEvent{ID: eventID, Record: event})
		lastTime = event.CreatedAt
		snapshot.State = event.Request.Outcome
		if event.Request.Kind == "attempt_recorded" {
			snapshot.State = "outcome_unknown"
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return JournalSnapshot{}, err
	}
	if len(snapshot.Events)+1 != len(heads) {
		return JournalSnapshot{}, ErrJournalCorrupt
	}
	return snapshot, nil
}

func journalReadError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrJournalCorrupt
}

func validateJournalBinding(ctx context.Context, db journalQueryer, r JournalIntentRecord, observation CapturedObservation, saved Saved) error {
	q := r.Request
	if q.PlanID != saved.ID || r.StoreID != observation.Record.StoreID || r.InventoryID != observation.Record.InventoryID || q.ObservationID != observation.ID || r.CreatedAt.Before(observation.Record.ObservedAt) {
		return ErrJournalPreparation
	}
	index := -1
	for i, target := range saved.Record.Selection.Targets {
		if target.FindingID == q.FindingID {
			index = i
			break
		}
	}
	if index < 0 || observation.Record.Targets[index].FindingID != q.FindingID {
		return ErrJournalPreparation
	}
	finding := saved.Record.Selection.Evidence.Findings[index]
	target := saved.Record.Selection.Targets[index].Target
	rootFound := false
	for _, root := range saved.Record.Selection.Roots {
		if root.ID == finding.RootID {
			path := string(root.PathBytes)
			if (path != "/" && !validJournalPath(root.PathBytes)) || !pathWithin(path, string(finding.PathBytes)) || path == string(finding.PathBytes) {
				return ErrJournalPreparation
			}
			rootFound = true
			break
		}
	}
	if !rootFound {
		return ErrJournalPreparation
	}
	if q.Kind == "quarantine_preparation" {
		if !bytes.Equal(q.SourcePathBytes, finding.PathBytes) || q.SourceObject != journalIdentity(target) {
			return ErrJournalPreparation
		}
		return nil
	}
	// Restoration remains linked to the preserved original intent, independent
	// of whether historical review consent later expired or was revoked.
	original, err := loadJournalAtDepth(ctx, db, q.OriginalIntentID, time.Now().UTC(), 1)
	if err != nil {
		return err
	}
	o := original.Intent.Record
	if o.Request.Kind != "quarantine_preparation" || q.PlanID != o.Request.PlanID || q.ObservationID != o.Request.ObservationID || q.FindingID != o.Request.FindingID || r.StoreID != o.StoreID || r.InventoryID != o.InventoryID || original.State != "recorded_at_destination" || len(original.Events) == 0 || !bytes.Equal(q.SourcePathBytes, o.Request.DestinationPathBytes) || !bytes.Equal(q.DestinationPathBytes, o.Request.SourcePathBytes) {
		return ErrJournalPreparation
	}
	last := original.Events[len(original.Events)-1].Record.Request.ObjectIdentity
	if last == nil || q.SourceObject != *last || q.SourceObject.Device != o.Request.SourceObject.Device || q.SourceObject.Inode != o.Request.SourceObject.Inode {
		return ErrJournalPreparation
	}
	return nil
}

func journalIdentity(identity state.EntryBinding) JournalIdentity {
	return JournalIdentity{identity.Device, identity.Inode, identity.ChangedNS, identity.Generation}
}

func validateIntent(r JournalIntentRecord, now time.Time) error {
	if r.Version != 1 || r.Contract != JournalContract || r.CreatedAt.IsZero() || r.CreatedAt.After(now) || !validDigest(r.StoreID) || !validDigest(r.InventoryID) || r.Status != "prepared" || r.Executable || r.CurrentStateVerified {
		return ErrJournalCorrupt
	}
	return validatePreparation(r.Request)
}

func validatePreparation(q PreparationRequest) error {
	if !validDigest(q.RequestKey) || !ValidID(q.PlanID) || !ValidObservationID(q.ObservationID) || !validObservationFindingID(q.FindingID) || !q.DestinationMustBeAbsent || !validJournalPath(q.SourcePathBytes) || !validJournalPath(q.DestinationPathBytes) || bytes.Equal(q.SourcePathBytes, q.DestinationPathBytes) || !knownJournalIdentity(q.SourceParent) || !knownJournalIdentity(q.SourceObject) || !knownJournalIdentity(q.DestinationParent) || q.SourceParent.Device != q.SourceObject.Device || q.DestinationParent.Device != q.SourceObject.Device {
		return ErrJournalPreparation
	}
	source, destination := string(q.SourcePathBytes), string(q.DestinationPathBytes)
	if q.SourceParent.Device == q.SourceObject.Device && q.SourceParent.Inode == q.SourceObject.Inode || q.DestinationParent.Device == q.SourceObject.Device && q.DestinationParent.Inode == q.SourceObject.Inode {
		return ErrJournalPreparation
	}
	if filepath.Dir(source) == filepath.Dir(destination) && q.SourceParent != q.DestinationParent {
		return ErrJournalPreparation
	}
	if pathWithin(source, destination) || pathWithin(source, filepath.Dir(destination)) || pathWithin(destination, source) {
		return ErrJournalPreparation
	}
	if q.Kind == "quarantine_preparation" {
		if q.OriginalIntentID != "" || filepath.Base(source) != "node_modules" {
			return ErrJournalPreparation
		}
	} else if q.Kind == "restore_preparation" {
		if !ValidJournalID(q.OriginalIntentID) || filepath.Base(destination) != "node_modules" {
			return ErrJournalPreparation
		}
	} else {
		return ErrJournalPreparation
	}
	return nil
}

func pathWithin(parent, child string) bool {
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return child == parent || strings.HasPrefix(child, parent+"/")
}
func validJournalPath(path []byte) bool {
	return len(path) > 1 && len(path) <= JournalPathBytesLimit && bytes.IndexByte(path, 0) < 0 && filepath.IsAbs(string(path)) && filepath.Clean(string(path)) == string(path) && strings.Count(string(path), "/") <= state.LivePathDepthLimit
}
func knownJournalIdentity(s JournalIdentity) bool {
	return canonicalUint(s.Device, false) && canonicalUint(s.Inode, true) && s.ChangedNS > 0 && s.Generation >= 0
}
func canonicalUint(value string, nonzero bool) bool {
	if len(value) > 20 {
		return false
	}
	number, err := strconv.ParseUint(value, 10, 64)
	return err == nil && (!nonzero || number != 0) && strconv.FormatUint(number, 10) == value
}

func validateEventRequest(q EventRequest) error {
	if !validDigest(q.RequestKey) || q.ExpectedSequence < 1 || q.ExpectedSequence > JournalEventLimit || (!ValidJournalID(q.PredecessorID) && !validPrefixedID(q.PredecessorID, journalEventPrefix)) {
		return ErrJournalTransition
	}
	if q.Kind == "attempt_recorded" {
		if q.Outcome != "not_observed" || q.SourceState != "unknown" || q.DestinationState != "unknown" || q.ObjectIdentity != nil {
			return ErrJournalTransition
		}
		return nil
	}
	if q.Kind != "result_recorded" && q.Kind != "reconciliation_recorded" {
		return ErrJournalTransition
	}
	switch q.Outcome {
	case "recorded_at_source":
		if q.SourceState != "present" || q.DestinationState != "absent" || q.ObjectIdentity == nil || !knownJournalIdentity(*q.ObjectIdentity) {
			return ErrJournalTransition
		}
	case "recorded_at_destination":
		if q.SourceState != "absent" || q.DestinationState != "present" || q.ObjectIdentity == nil || !knownJournalIdentity(*q.ObjectIdentity) {
			return ErrJournalTransition
		}
	case "recorded_conflict":
		if q.SourceState != "present" || q.DestinationState != "present" || q.ObjectIdentity != nil {
			return ErrJournalTransition
		}
	case "outcome_unknown":
		if q.SourceState != "unknown" || q.DestinationState != "unknown" || q.ObjectIdentity != nil {
			return ErrJournalTransition
		}
	default:
		return ErrJournalTransition
	}
	return nil
}

func validateNextEvent(snapshot JournalSnapshot, q EventRequest) error {
	if validateEventRequest(q) != nil || q.ExpectedSequence != len(snapshot.Events)+1 {
		return ErrJournalTransition
	}
	predecessor := snapshot.Intent.ID
	if len(snapshot.Events) > 0 {
		predecessor = snapshot.Events[len(snapshot.Events)-1].ID
	}
	if q.PredecessorID != predecessor {
		return ErrJournalTransition
	}
	if q.Kind == "attempt_recorded" {
		if len(snapshot.Events) != 0 || snapshot.State != "prepared" {
			return ErrJournalTransition
		}
	} else {
		if len(snapshot.Events) == 0 {
			return ErrJournalTransition
		}
		last := snapshot.Events[len(snapshot.Events)-1].Record.Request
		if q.Kind == "result_recorded" && last.Kind != "attempt_recorded" {
			return ErrJournalTransition
		}
		if q.Kind == "reconciliation_recorded" && snapshot.State != "outcome_unknown" && snapshot.State != "recorded_conflict" {
			return ErrJournalTransition
		}
		if q.ObjectIdentity != nil && (q.ObjectIdentity.Device != snapshot.Intent.Record.Request.SourceObject.Device || q.ObjectIdentity.Inode != snapshot.Intent.Record.Request.SourceObject.Inode || q.ObjectIdentity.Generation != snapshot.Intent.Record.Request.SourceObject.Generation) {
			return ErrJournalTransition
		}
	}
	return nil
}

func strictJournalDecode(payload []byte, id, prefix string, result any) error {
	if len(payload) > MaxJournalBytes || !validPrefixedID(id, prefix) || id != digest(prefix, payload) {
		return ErrJournalCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return ErrJournalCorrupt
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrJournalCorrupt
	}
	canonical, err := json.Marshal(result)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrJournalCorrupt
	}
	return nil
}
