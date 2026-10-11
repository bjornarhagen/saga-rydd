package inventory

// This separate schema owns one immutable batch and at most one attempt. Its
// charges do not participate in a HashStore budget or grant source-read consent.
const sampleStoreSchema = `
CREATE TABLE sample_meta (
 id INTEGER PRIMARY KEY CHECK(id=1),
 store_id TEXT NOT NULL CHECK(typeof(store_id)='text' AND length(store_id)=64)
);
CREATE TRIGGER sample_meta_no_update BEFORE UPDATE ON sample_meta BEGIN SELECT RAISE(ABORT,'sample identity is immutable'); END;
CREATE TRIGGER sample_meta_no_delete BEFORE DELETE ON sample_meta BEGIN SELECT RAISE(ABORT,'sample identity is immutable'); END;
CREATE TABLE sample_batch (
 id INTEGER PRIMARY KEY CHECK(id=1),
 batch_id TEXT NOT NULL CHECK(typeof(batch_id)='text' AND length(batch_id)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=1048576)
);
CREATE TRIGGER sample_batch_no_update BEFORE UPDATE ON sample_batch BEGIN SELECT RAISE(ABORT,'sample batch is immutable'); END;
CREATE TRIGGER sample_batch_no_delete BEFORE DELETE ON sample_batch BEGIN SELECT RAISE(ABORT,'sample batch is immutable'); END;
CREATE TABLE sample_attempt (
 id INTEGER PRIMARY KEY CHECK(id=1),
 store_id TEXT NOT NULL, batch_id TEXT NOT NULL, nonce TEXT NOT NULL,
 reservation_day TEXT NOT NULL, reserved_at_ns INTEGER NOT NULL,
 grant_bytes INTEGER NOT NULL CHECK(typeof(grant_bytes)='integer' AND grant_bytes BETWEEN 1 AND 1966080),
 status TEXT NOT NULL CHECK(status IN ('reserved','settled','interrupted_unknown')),
 outcome TEXT NOT NULL CHECK(typeof(outcome)='text' AND length(outcome)<=64),
 requested_bytes INTEGER, read_bytes INTEGER, elapsed_ns INTEGER,
 CHECK(status='settled' AND typeof(requested_bytes)='integer' AND typeof(read_bytes)='integer' AND typeof(elapsed_ns)='integer'
   AND requested_bytes BETWEEN 0 AND grant_bytes AND read_bytes BETWEEN 0 AND requested_bytes AND elapsed_ns>=0
   OR status!='settled' AND requested_bytes IS NULL AND read_bytes IS NULL AND elapsed_ns IS NULL)
);
CREATE TRIGGER sample_attempt_transition BEFORE UPDATE ON sample_attempt
 WHEN NEW.id!=OLD.id OR NEW.store_id!=OLD.store_id OR NEW.batch_id!=OLD.batch_id OR NEW.nonce!=OLD.nonce
 OR NEW.reservation_day!=OLD.reservation_day OR NEW.reserved_at_ns!=OLD.reserved_at_ns OR NEW.grant_bytes!=OLD.grant_bytes
 OR OLD.status!='reserved' OR NEW.status NOT IN ('settled','interrupted_unknown')
 BEGIN SELECT RAISE(ABORT,'sample attempt cannot be replaced or replayed'); END;
CREATE TRIGGER sample_attempt_no_delete BEFORE DELETE ON sample_attempt BEGIN SELECT RAISE(ABORT,'sample attempt cannot be removed'); END;
CREATE TABLE sample_budget (
 id INTEGER PRIMARY KEY CHECK(id=1), day TEXT NOT NULL, max_now_ns INTEGER NOT NULL,
 reserved_bytes INTEGER NOT NULL, requested_bytes INTEGER NOT NULL, read_bytes INTEGER NOT NULL,
 unknown_bytes INTEGER NOT NULL, outstanding_bytes INTEGER NOT NULL,
 total_reserved_bytes INTEGER NOT NULL, total_requested_bytes INTEGER NOT NULL, total_read_bytes INTEGER NOT NULL,
 total_unknown_bytes INTEGER NOT NULL, total_outstanding_bytes INTEGER NOT NULL
);
CREATE TRIGGER sample_budget_monotonic BEFORE UPDATE ON sample_budget
 WHEN NEW.id!=OLD.id OR NEW.day<OLD.day OR NEW.max_now_ns<OLD.max_now_ns OR NEW.total_reserved_bytes<OLD.total_reserved_bytes
 BEGIN SELECT RAISE(ABORT,'sample charge and clock cannot be reversed'); END;
CREATE TRIGGER sample_budget_no_delete BEFORE DELETE ON sample_budget BEGIN SELECT RAISE(ABORT,'sample charge cannot be removed'); END;
PRAGMA application_id=0x52595350;
PRAGMA user_version=1;
`
