package inventory

import (
	"context"
	"database/sql"
)

// These bounded records are separate from the immutable selection and current
// work heads. Observed expiry and clock high-water can only move forward.
const hashReadSchema = `
CREATE TABLE hash_read_approval (
 id INTEGER PRIMARY KEY CHECK(id=1), approval_id TEXT NOT NULL CHECK(length(approval_id)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=8192)
);
CREATE TRIGGER hash_read_approval_no_update BEFORE UPDATE ON hash_read_approval BEGIN SELECT RAISE(ABORT,'hash read approval is immutable'); END;
CREATE TRIGGER hash_read_approval_no_delete BEFORE DELETE ON hash_read_approval BEGIN SELECT RAISE(ABORT,'hash read approval is immutable'); END;
CREATE TABLE hash_read_revocation (
 id INTEGER PRIMARY KEY CHECK(id=1), revocation_id TEXT NOT NULL CHECK(length(revocation_id)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=2048)
);
CREATE TRIGGER hash_read_revocation_no_update BEFORE UPDATE ON hash_read_revocation BEGIN SELECT RAISE(ABORT,'hash read revocation is immutable'); END;
CREATE TRIGGER hash_read_revocation_no_delete BEFORE DELETE ON hash_read_revocation BEGIN SELECT RAISE(ABORT,'hash read revocation is immutable'); END;
CREATE TABLE hash_read_observation (
 id INTEGER PRIMARY KEY CHECK(id=1), approval_id TEXT NOT NULL CHECK(length(approval_id)=64),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=0),
 expired INTEGER NOT NULL CHECK(typeof(expired)='integer' AND expired IN (0,1))
);
CREATE TRIGGER hash_read_observation_monotonic BEFORE UPDATE ON hash_read_observation
 WHEN NEW.approval_id!=OLD.approval_id OR NEW.max_now_ns<OLD.max_now_ns OR NEW.expired<OLD.expired
 BEGIN SELECT RAISE(ABORT,'hash read clock observations cannot be reversed'); END;
CREATE TRIGGER hash_read_observation_no_delete BEFORE DELETE ON hash_read_observation BEGIN SELECT RAISE(ABORT,'hash read observations cannot be removed'); END;
PRAGMA user_version=2;
`

func (s *HashStore) migrateHashReadSchema(ctx context.Context) error {
	// Validate the old schema and every bounded record before publishing a new
	// version. This includes valid reserved work, without recovering it.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	_, _, err = s.readHashSnapshot(ctx, tx)
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		return err
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, hashReadSchema); err != nil {
		return ErrHashStoreCorrupt
	}
	if err = tx.Commit(); err != nil {
		return ErrHashRecoveryRequired
	}
	s.schemaVersion = 2
	return nil
}
