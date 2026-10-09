package plans

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const PlanAdmissionContract = "saved_plan_root_admission_v1"
const PlanLimit = 128

// These structural bounds include the plan and its supported derived history.
// They exclude identities, indexes, SQLite pages/WAL and independent dismissals;
// they neither allocate space nor bound the physical size of a legacy store.
const MaxPlanHistoryRows = 1364
const MaxPlanHistoryPayloadBytes = 23429120

var ErrPlanCapacity = errors.New("saved plan capacity reached; existing history remains available")
var ErrPlanPublication = errors.New("saved plan publication outcome is uncertain; reopen the exact plan ID or retry its frozen record")

const admissionTrigger = `CREATE TRIGGER plans_capacity BEFORE INSERT ON plans
WHEN (SELECT count(*) FROM (SELECT 1 FROM plans LIMIT 128))>=128
BEGIN SELECT RAISE(ABORT,'saved plan capacity reached'); END`

const admissionSchema = admissionTrigger + "; PRAGMA user_version=6;"

// Only surrounding whitespace may differ from the canonical trigger, and at
// most 128 additional bytes are accepted. This bounds the Go query projection;
// SQLite's internal schema parsing is separate from this returned-scalar bound.
const maxAdmissionTriggerBytes = len(admissionTrigger) + 128

type admissionHooks struct {
	beforeCommit func()
	afterCommit  func()
	commit       func(*sql.Tx) error
}

// SaveRecord publishes an exact frozen unapproved record. An exact retry returns
// its original ID/time before admission checks, including in over-limit legacy
// stores. Save instead constructs a new timestamp and therefore a new plan.
// A nonempty result with ErrPlanPublication is a candidate, not proof of commit.
// Retry that Record or Load its ID; do not construct a fresh Save to recover it.
func SaveRecord(ctx context.Context, base string, record Record) (Saved, error) {
	return saveRecord(ctx, base, record, admissionHooks{})
}

func saveRecord(ctx context.Context, base string, record Record, hooks admissionHooks) (Saved, error) {
	if err := ctx.Err(); err != nil {
		return Saved{}, err
	}
	if err := validate(record); err != nil {
		return Saved{}, err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return Saved{}, err
	}
	if len(payload) > MaxRecordBytes {
		return Saved{}, errors.New("selected evidence exceeds the 1 MiB saved-plan limit; select fewer targets")
	}
	// Detach every caller-owned slice and reflect the exact stored JSON, including
	// lossy display strings. Authoritative byte paths remain unchanged.
	var frozen Record
	if err = json.Unmarshal(payload, &frozen); err != nil || validate(frozen) != nil {
		return Saved{}, ErrCorrupt
	}
	record = frozen
	id := fmt.Sprintf("plan-v1-%x", sha256.Sum256(payload))
	candidate := Saved{ID: id, Record: record}
	// Defer every additive schema migration until new-root publication.
	db, closeDB, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Saved{}, err
	}
	defer tx.Rollback()
	exists, err := exactPlanRecord(ctx, tx, id, payload)
	if err != nil {
		return Saved{}, err
	}
	if exists {
		if err = ctx.Err(); err != nil {
			return Saved{}, err
		}
		// A read-only transaction publishes nothing and needs no commit retry.
		return candidate, nil
	}
	if err = admitPlan(ctx, tx); err != nil {
		return Saved{}, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return Saved{}, err
	}
	if version < 6 {
		if err = migrateJournalTx(ctx, tx, version); err != nil {
			return Saved{}, err
		}
		if version < 5 {
			if _, err = tx.ExecContext(ctx, dismissalSchema); err != nil {
				return Saved{}, err
			}
		}
		if _, err = tx.ExecContext(ctx, admissionSchema); err != nil {
			return Saved{}, err
		}
	} else {
		if err = checkAdmissionTrigger(ctx, tx); err != nil {
			return Saved{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO plans(id,payload) VALUES(?,?)", id, payload); err != nil {
		return Saved{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return Saved{}, err
	}
	commit := tx.Commit
	if hooks.commit != nil {
		commit = func() error { return hooks.commit(tx) }
	}
	if err = commit(); err != nil {
		return candidate, fmt.Errorf("%w: %w", ErrPlanPublication, err)
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return candidate, ctx.Err()
}

func checkAdmissionTrigger(ctx context.Context, tx *sql.Tx) error {
	var kind string
	var length sql.NullInt64
	var trigger []byte
	err := tx.QueryRowContext(ctx, `SELECT typeof(sql),length(CAST(sql AS BLOB)),substr(CAST(sql AS BLOB),1,?) FROM sqlite_master WHERE type='trigger' AND name='plans_capacity'`, maxAdmissionTriggerBytes+1).Scan(&kind, &length, &trigger)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrCorrupt
	}
	if kind != "text" || !length.Valid || length.Int64 < int64(len(admissionTrigger)) || length.Int64 > int64(maxAdmissionTriggerBytes) || int64(len(trigger)) != length.Int64 || strings.TrimSpace(string(trigger)) != admissionTrigger {
		return ErrCorrupt
	}
	return nil
}

// A malformed exact row never becomes a retry or an unused admission slot.
func exactPlanRecord(ctx context.Context, tx *sql.Tx, id string, payload []byte) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT typeof(id),typeof(payload),length(CAST(id AS BLOB)),length(payload),substr(CAST(id AS BLOB),1,73),substr(payload,1,?) FROM plans WHERE id=? LIMIT 2`, MaxRecordBytes+1, id)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var idType, payloadType string
		var idLength, payloadLength int64
		var savedID, savedPayload []byte
		if err = rows.Scan(&idType, &payloadType, &idLength, &payloadLength, &savedID, &savedPayload); err != nil {
			return false, admissionReadError(ctx, err)
		}
		count++
		if count > 1 || idType != "text" || payloadType != "blob" || idLength != 72 || payloadLength < 1 || payloadLength > MaxRecordBytes || string(savedID) != id || !bytes.Equal(savedPayload, payload) {
			return false, ErrCorrupt
		}
	}
	if err = rows.Err(); err != nil {
		return false, admissionReadError(ctx, err)
	}
	// Candidate validation and equality establish the exact digest/record too.
	return count == 1, nil
}

func admitPlan(ctx context.Context, tx *sql.Tx) error {
	var count int64
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM plans LIMIT 129)").Scan(&count); err != nil {
		return admissionReadError(ctx, err)
	}
	if count < 0 || count > PlanLimit+1 {
		return ErrCorrupt
	}
	if count >= PlanLimit {
		return ErrPlanCapacity
	}
	// Inspect bounded scalar shapes, not every unrelated historical payload.
	// Every raw row was counted, including malformed rows, without filtering.
	rows, err := tx.QueryContext(ctx, `SELECT typeof(id),typeof(payload),length(CAST(id AS BLOB)),length(payload),substr(CAST(id AS BLOB),1,73) FROM plans ORDER BY id LIMIT 129`)
	if err != nil {
		return admissionReadError(ctx, err)
	}
	defer rows.Close()
	var checked int64
	for rows.Next() {
		var idType, payloadType string
		var idLength, payloadLength int64
		var id []byte
		if err = rows.Scan(&idType, &payloadType, &idLength, &payloadLength, &id); err != nil {
			return admissionReadError(ctx, err)
		}
		checked++
		if checked > count || idType != "text" || payloadType != "blob" || idLength != 72 || !ValidID(string(id)) || payloadLength < 1 || payloadLength > MaxRecordBytes {
			return ErrCorrupt
		}
	}
	if err = rows.Err(); err != nil {
		return admissionReadError(ctx, err)
	}
	if checked != count {
		return ErrCorrupt
	}
	return nil
}

func admissionReadError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w", ErrCorrupt, err)
}
