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
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/regeneration"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const ObservationContract = "npm_inputs_tree_metadata_v1"
const MaxObservationBytes = 64 << 10
const observationPrefix = "observation-v1-"

var ErrObservationID = errors.New("use the full observation ID returned by plan --capture")
var ErrObservationCorrupt = errors.New("saved observation is invalid or does not match its plan, store or inspection contract; no action is authorized")
var ErrObservationEvidence = errors.New("capture requires successful input and tree observations for every exact saved target; no observation was saved")
var ErrObservationConflict = errors.New("this plan already has an observation; compare it or save a new selection before capturing changed evidence")

// ObservationRecord stores summaries only. Ordinary dependency bodies and
// descendant names are never stored, and this record cannot authorize cleanup.
type ObservationRecord struct {
	Version                   int                 `json:"version"`
	PlanID                    string              `json:"plan_id"`
	StoreID                   string              `json:"store_id"`
	InventoryID               string              `json:"inventory_id"`
	Contract                  string              `json:"inspection_contract"`
	ObservedAt                time.Time           `json:"observed_at"`
	Status                    string              `json:"status"`
	Source                    string              `json:"source"`
	CurrentStateVerified      bool                `json:"current_state_verified"`
	Executable                bool                `json:"executable"`
	RegenerationVerified      bool                `json:"regeneration_verified"`
	DependencyContentsChecked bool                `json:"dependency_contents_checked"`
	LocalDependencyEdits      string              `json:"local_dependency_edits"`
	Targets                   []ObservationTarget `json:"targets"`
}

type ObservationTarget struct {
	FindingID string                  `json:"finding_id"`
	Status    string                  `json:"status"`
	Inputs    inventory.InputEvidence `json:"inputs"`
	Tree      inventory.TreeEvidence  `json:"tree"`
}

type CapturedObservation struct {
	ID     string            `json:"id"`
	Record ObservationRecord `json:"record"`
}

type ObservationComparison struct {
	ObservationID             string                        `json:"observation_id"`
	PlanID                    string                        `json:"plan_id"`
	Status                    string                        `json:"status"`
	Source                    string                        `json:"source"`
	ObservedAt                time.Time                     `json:"observed_at"`
	CheckedAt                 time.Time                     `json:"checked_at"`
	CurrentStateVerified      bool                          `json:"current_state_verified"`
	Executable                bool                          `json:"executable"`
	RegenerationVerified      bool                          `json:"regeneration_verified"`
	DependencyContentsChecked bool                          `json:"dependency_contents_checked"`
	LocalDependencyEdits      string                        `json:"local_dependency_edits"`
	Targets                   []ObservationComparisonTarget `json:"targets"`
}

type ObservationComparisonTarget struct {
	FindingID string                   `json:"finding_id"`
	Status    string                   `json:"status"`
	Code      string                   `json:"code,omitempty"`
	Message   string                   `json:"message"`
	Changes   []string                 `json:"changes,omitempty"`
	Inputs    *inventory.InputEvidence `json:"inputs,omitempty"`
	Tree      *inventory.TreeEvidence  `json:"tree,omitempty"`
}

const observationSchema = `
CREATE TABLE observations (id TEXT PRIMARY KEY, plan_id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=65536));
CREATE TRIGGER observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END;
CREATE TRIGGER observations_no_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END;
PRAGMA user_version=3;`

func migrateObservationStore(ctx context.Context, db *sql.DB, version int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = migrateObservationTx(ctx, tx, version); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateObservationTx(ctx context.Context, tx *sql.Tx, version int) error {
	if version == 1 {
		if _, err := tx.ExecContext(ctx, reviewSchema); err != nil {
			return err
		}
	}
	if version < 3 {
		_, err := tx.ExecContext(ctx, observationSchema)
		return err
	}
	return nil
}

func ValidObservationID(id string) bool {
	return len(id) == len(observationPrefix)+64 && id[:len(observationPrefix)] == observationPrefix && validDigest(id[len(observationPrefix):])
}

// Capture publishes one separate immutable baseline per plan. A matching retry
// returns the original ID and time; changed evidence requires a new selection.
func Capture(ctx context.Context, base, planID string, report inventory.InspectionReport) (CapturedObservation, error) {
	if err := ctx.Err(); err != nil {
		return CapturedObservation{}, err
	}
	if !ValidID(planID) {
		return CapturedObservation{}, ErrID
	}
	// Validate before taking a writer lock or migrating an older store.
	saved, initialStoreID, err := capturePreflight(ctx, base, planID)
	if err != nil {
		return CapturedObservation{}, err
	}
	if err = validateCaptureReport(saved, report, time.Now().UTC()); err != nil {
		return CapturedObservation{}, err
	}
	db, closeDB, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		return CapturedObservation{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return CapturedObservation{}, err
	}
	defer tx.Rollback()
	saved, err = load(ctx, tx, planID)
	if err != nil {
		return CapturedObservation{}, err
	}
	if err = validateCaptureReport(saved, report, time.Now().UTC()); err != nil {
		return CapturedObservation{}, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return CapturedObservation{}, err
	}
	if err = migrateObservationTx(ctx, tx, version); err != nil {
		return CapturedObservation{}, err
	}
	storeID, err := storeIdentity(ctx, tx)
	if err != nil || (initialStoreID != "" && initialStoreID != storeID) {
		if ctx.Err() != nil {
			return CapturedObservation{}, ctx.Err()
		}
		return CapturedObservation{}, ErrObservationCorrupt
	}
	existing, err := readObservationForPlan(ctx, tx, planID, time.Now().UTC())
	if err != nil {
		return CapturedObservation{}, err
	}
	if existing != nil {
		comparison, e := CompareObservation(*existing, report)
		if e != nil {
			return CapturedObservation{}, e
		}
		if comparison.Status != "matches_observation" {
			return CapturedObservation{}, ErrObservationConflict
		}
		return *existing, tx.Commit()
	}
	r := ObservationRecord{Version: 1, PlanID: saved.ID, StoreID: storeID, InventoryID: saved.Record.Selection.InventoryID,
		Contract: ObservationContract, ObservedAt: report.CheckedAt.UTC(), Status: report.Status, Source: report.Source,
		LocalDependencyEdits: "unknown", Targets: make([]ObservationTarget, len(report.Targets))}
	for i, target := range report.Targets {
		r.Targets[i] = ObservationTarget{FindingID: target.FindingID, Status: target.Status, Inputs: cloneInputs(*target.Inputs), Tree: *target.Tree}
	}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > MaxObservationBytes || validateObservation(r) != nil {
		return CapturedObservation{}, ErrObservationEvidence
	}
	id := digest(observationPrefix, payload)
	if _, err = tx.ExecContext(ctx, "INSERT INTO observations(id,plan_id,payload) VALUES(?,?,?)", id, planID, payload); err != nil {
		return CapturedObservation{}, err
	}
	// Verify the exact published bytes and all bindings before committing.
	stored, _, err := loadObservation(ctx, tx, id, time.Now().UTC())
	if err != nil {
		return CapturedObservation{}, err
	}
	if err = tx.Commit(); err != nil {
		return CapturedObservation{}, err
	}
	return stored, nil
}

func capturePreflight(ctx context.Context, base, planID string) (Saved, string, error) {
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return Saved{}, "", err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Saved{}, "", err
	}
	defer tx.Rollback()
	saved, err := load(ctx, tx, planID)
	if err != nil {
		return Saved{}, "", err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return Saved{}, "", err
	}
	storeID := ""
	if version > 1 {
		storeID, err = storeIdentity(ctx, tx)
		if err != nil {
			if ctx.Err() != nil {
				return Saved{}, "", ctx.Err()
			}
			return Saved{}, "", ErrObservationCorrupt
		}
	}
	if err = tx.Commit(); err != nil {
		return Saved{}, "", err
	}
	return saved, storeID, nil
}

func readObservationForPlan(ctx context.Context, db queryer, planID string, now time.Time) (*CapturedObservation, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version < 3 {
		return nil, nil
	}
	var id string
	err := db.QueryRowContext(ctx, "SELECT substr(id,1,100) FROM observations WHERE plan_id=?", planID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !ValidObservationID(id) {
		return nil, ErrObservationCorrupt
	}
	observation, _, err := loadObservation(ctx, db, id, now)
	if err != nil {
		return nil, err
	}
	return &observation, nil
}

// LoadObservation reads the baseline, its exact plan and store binding in one
// SQLite snapshot. Older stores stay read-only and are never migrated here.
func LoadObservation(ctx context.Context, base, id string) (CapturedObservation, Saved, error) {
	if !ValidObservationID(id) {
		return CapturedObservation{}, Saved{}, ErrObservationID
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	defer tx.Rollback()
	observation, saved, err := loadObservation(ctx, tx, id, time.Now().UTC())
	if err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	if err = tx.Commit(); err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	return observation, saved, nil
}

func loadObservation(ctx context.Context, db queryer, id string, now time.Time) (CapturedObservation, Saved, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	if version < 3 {
		return CapturedObservation{}, Saved{}, fmt.Errorf("saved observation not found: %w", os.ErrNotExist)
	}
	var planID string
	var payload []byte
	err := db.QueryRowContext(ctx, "SELECT substr(plan_id,1,100),substr(payload,1,?) FROM observations WHERE id=?", MaxObservationBytes+1, id).Scan(&planID, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return CapturedObservation{}, Saved{}, fmt.Errorf("saved observation not found: %w", os.ErrNotExist)
	}
	if err != nil {
		return CapturedObservation{}, Saved{}, err
	}
	var r ObservationRecord
	if decodeObservation(payload, id, &r) != nil || planID != r.PlanID || r.ObservedAt.After(now) {
		return CapturedObservation{}, Saved{}, ErrObservationCorrupt
	}
	saved, err := load(ctx, db, planID)
	if err != nil {
		if ctx.Err() != nil {
			return CapturedObservation{}, Saved{}, ctx.Err()
		}
		return CapturedObservation{}, Saved{}, ErrObservationCorrupt
	}
	storeID, err := storeIdentity(ctx, db)
	if err != nil || storeID != r.StoreID || saved.Record.Selection.InventoryID != r.InventoryID || r.ObservedAt.Before(saved.Record.CreatedAt) || len(r.Targets) != len(saved.Record.Selection.Targets) {
		if ctx.Err() != nil {
			return CapturedObservation{}, Saved{}, ctx.Err()
		}
		return CapturedObservation{}, Saved{}, ErrObservationCorrupt
	}
	for i, target := range r.Targets {
		if target.FindingID != saved.Record.Selection.Targets[i].FindingID {
			return CapturedObservation{}, Saved{}, ErrObservationCorrupt
		}
	}
	return CapturedObservation{ID: id, Record: r}, saved, nil
}

func validateCaptureReport(saved Saved, report inventory.InspectionReport, now time.Time) error {
	if report.Status != "inputs_and_tree_observed" || report.Source != "live_project_inputs_and_tree_metadata" || report.CheckedAt.IsZero() || report.CheckedAt.After(now) || report.CheckedAt.Before(saved.Record.CreatedAt) || !safeReport(report) || len(report.Targets) != len(saved.Record.Selection.Targets) {
		return ErrObservationEvidence
	}
	for i, target := range report.Targets {
		if target.FindingID != saved.Record.Selection.Targets[i].FindingID || target.Status != "inputs_and_tree_observed" || target.Code != "" || target.Inputs == nil || target.Tree == nil || validateEvidence(*target.Inputs, *target.Tree) != nil {
			return ErrObservationEvidence
		}
	}
	return nil
}

func safeReport(r inventory.InspectionReport) bool {
	return !r.CurrentStateVerified && !r.Executable && !r.RegenerationVerified && !r.DependencyContentsChecked && r.LocalDependencyEdits == "unknown"
}

func validateObservation(r ObservationRecord) error {
	if r.Version != 1 || !ValidID(r.PlanID) || !validDigest(r.StoreID) || !validDigest(r.InventoryID) || r.Contract != ObservationContract || r.ObservedAt.IsZero() || r.Status != "inputs_and_tree_observed" || r.Source != "live_project_inputs_and_tree_metadata" || r.CurrentStateVerified || r.Executable || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) < 1 || len(r.Targets) > state.PreviewTargetLimit {
		return ErrObservationCorrupt
	}
	seen := make(map[string]bool, len(r.Targets))
	for _, target := range r.Targets {
		if !validObservationFindingID(target.FindingID) || seen[target.FindingID] || target.Status != "inputs_and_tree_observed" || validateEvidence(target.Inputs, target.Tree) != nil {
			return ErrObservationCorrupt
		}
		seen[target.FindingID] = true
	}
	return nil
}

func validObservationFindingID(id string) bool {
	if len(id) > 100 {
		return false
	}
	parts := strings.Split(id, ":")
	if len(parts) != 3 || parts[0] != "node-modules-v1" {
		return false
	}
	root, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || root <= 0 {
		return false
	}
	entry, err := strconv.ParseInt(parts[2], 10, 64)
	return err == nil && entry > 0 && id == fmt.Sprintf("node-modules-v1:%d:%d", root, entry)
}

func validateEvidence(inputs inventory.InputEvidence, tree inventory.TreeEvidence) error {
	if (inputs.LockfileVersion != 2 && inputs.LockfileVersion != 3) || inputs.LockedPackages < 0 || inputs.LockedPackages > regeneration.JSONValueLimit || len(inputs.Files) != 2 {
		return ErrObservationCorrupt
	}
	for i, file := range inputs.Files {
		name, limit := "package.json", inventory.ManifestInputLimit
		if i == 1 {
			name, limit = "package-lock.json", inventory.LockInputLimit
		}
		if file.Name != name || file.Bytes < 1 || file.Bytes > limit || !validDigest(file.SHA256) {
			return ErrObservationCorrupt
		}
	}
	if tree.Status != "metadata_observed" || tree.Entries < 0 || tree.Entries > inventory.TreeEntryLimit || tree.Directories < 0 || tree.Directories > tree.Entries || tree.RegularFiles < 0 || tree.RegularFiles > tree.Entries || tree.InternalBinLinks < 0 || tree.InternalBinLinks > tree.Entries || tree.Directories+tree.RegularFiles+tree.InternalBinLinks != tree.Entries || !validDigest(tree.MetadataSHA256) {
		return ErrObservationCorrupt
	}
	if tree.InternalBinLinks > 0 && (tree.Directories < 2 || tree.RegularFiles < 1 || inputs.LockedPackages < 1) {
		return ErrObservationCorrupt
	}
	return nil
}

func decodeObservation(payload []byte, id string, result *ObservationRecord) error {
	if len(payload) > MaxObservationBytes || !ValidObservationID(id) || id != digest(observationPrefix, payload) {
		return ErrObservationCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(result); err != nil || validateObservation(*result) != nil {
		return ErrObservationCorrupt
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrObservationCorrupt
	}
	// Capture has one canonical encoding. Requiring it rejects duplicate keys,
	// omitted safety fields, null slices and alternate ambiguous encodings.
	canonical, err := json.Marshal(result)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ErrObservationCorrupt
	}
	return nil
}

func cloneInputs(inputs inventory.InputEvidence) inventory.InputEvidence {
	inputs.Files = append([]inventory.InputFileEvidence(nil), inputs.Files...)
	return inputs
}

// CompareObservation is a pure comparison of bounded evidence. Timestamps and
// explanatory messages do not affect equality; no record or consent is written.
func CompareObservation(captured CapturedObservation, live inventory.InspectionReport) (ObservationComparison, error) {
	payload, err := json.Marshal(captured.Record)
	if err != nil || decodeObservation(payload, captured.ID, new(ObservationRecord)) != nil {
		return ObservationComparison{}, ErrObservationCorrupt
	}
	r := captured.Record
	if live.Source != r.Source || !safeReport(live) || live.CheckedAt.IsZero() || live.CheckedAt.Before(r.ObservedAt) || live.CheckedAt.After(time.Now().UTC()) || len(live.Targets) != len(r.Targets) || (live.Status != "inputs_and_tree_observed" && live.Status != "blocked") {
		return ObservationComparison{}, ErrObservationEvidence
	}
	result := ObservationComparison{ObservationID: captured.ID, PlanID: r.PlanID, Status: "matches_observation", Source: live.Source,
		ObservedAt: r.ObservedAt, CheckedAt: live.CheckedAt, LocalDependencyEdits: "unknown", Targets: make([]ObservationComparisonTarget, len(r.Targets))}
	blocked := false
	for i, baseline := range r.Targets {
		current := live.Targets[i]
		if current.FindingID != baseline.FindingID {
			return ObservationComparison{}, ErrObservationEvidence
		}
		target := ObservationComparisonTarget{FindingID: baseline.FindingID, Status: "matches_observation", Message: "The observed inputs and tree metadata match this baseline. Contents, local edits and reinstall remain unverified."}
		if current.Status == "blocked" {
			message, known := observationBlockMessage(current.Code)
			if current.Inputs != nil || current.Tree != nil || !known {
				return ObservationComparison{}, ErrObservationEvidence
			}
			target.Status, target.Code, target.Message = "blocked", current.Code, message
			blocked = true
		} else if current.Status != "inputs_and_tree_observed" || current.Code != "" || current.Inputs == nil || current.Tree == nil || validateEvidence(*current.Inputs, *current.Tree) != nil {
			return ObservationComparison{}, ErrObservationEvidence
		} else {
			inputs := cloneInputs(*current.Inputs)
			tree := *current.Tree
			target.Inputs, target.Tree = &inputs, &tree
			for j, name := range []string{"manifest_bytes_changed", "lock_bytes_changed"} {
				if !reflect.DeepEqual(baseline.Inputs.Files[j], inputs.Files[j]) {
					target.Changes = append(target.Changes, name)
				}
			}
			if baseline.Inputs.LockfileVersion != inputs.LockfileVersion || baseline.Inputs.LockedPackages != inputs.LockedPackages {
				target.Changes = append(target.Changes, "input_summary_changed")
			}
			if baseline.Tree.MetadataSHA256 != tree.MetadataSHA256 {
				target.Changes = append(target.Changes, "tree_metadata_changed")
			}
			if baseline.Tree.Entries != tree.Entries || baseline.Tree.Directories != tree.Directories || baseline.Tree.RegularFiles != tree.RegularFiles || baseline.Tree.InternalBinLinks != tree.InternalBinLinks {
				target.Changes = append(target.Changes, "tree_counts_changed")
			}
			if len(target.Changes) != 0 {
				target.Status, target.Message = "changed", "The observed inputs or tree metadata differ from this baseline. Review the current evidence."
				result.Status = "changed"
			}
		}
		result.Targets[i] = target
	}
	if (live.Status == "blocked") != blocked {
		return ObservationComparison{}, ErrObservationEvidence
	}
	if blocked {
		result.Status = "blocked"
	}
	return result, nil
}

// Keep actionable scanner codes while excluding arbitrary supplied messages.
func observationBlockMessage(code string) (string, bool) {
	switch code {
	case "tree_limit", "path_limit", "input_limit":
		return "The current observation exceeds a supported inspection limit. Review the folder separately.", true
	case "tree_layout_unknown":
		return "The dependency layout contains an unsupported boundary entry. Review the folder.", true
	case "tree_link_unsupported":
		return "The dependency tree contains an unsupported or unsafe link. Review the folder.", true
	case "tree_unsupported":
		return "The dependency tree contains an unsupported object or hardlink. Review the folder.", true
	case "tree_changed_during_check", "input_changed_during_check", "path_changed_during_check":
		return "The observed path, input or dependency metadata changed during the check. Review it and retry.", true
	case "scope_invalid", "scope_excluded":
		return "The selected path is outside the allowed scope or is excluded. Review the current configuration.", true
	case "mount_boundary", "filesystem_unsupported":
		return "A selected object crosses a mount boundary or lacks supported filesystem evidence.", true
	case "identity_changed", "manifest_changed", "root_changed", "evidence_unknown":
		return "The current path identity or saved evidence no longer matches. Scan and review a new selection.", true
	case "input_missing":
		return "The project needs package.json and package-lock.json for the supported input check.", true
	case "input_invalid", "input_unsupported":
		return "The project inputs or install configuration do not fit the supported npm contract. Review the project.", true
	case "path_unavailable":
		return "A selected path or input is missing, inaccessible or unsupported. Review the folder.", true
	default:
		return "", false
	}
}
