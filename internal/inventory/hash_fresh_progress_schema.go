package inventory

// Immutable job scope and initial work remain in schema 4. These independent
// rows are initialized only through an explicit exact-job run/recovery writer.
const hashFreshProgressSchema = `
CREATE TABLE hash_fresh_run_state (
 job_id TEXT NOT NULL PRIMARY KEY REFERENCES hash_fresh_job(id),
 next_order INTEGER NOT NULL CHECK(typeof(next_order)='integer' AND next_order>=1)
);
CREATE TRIGGER hash_fresh_run_state_monotonic BEFORE UPDATE ON hash_fresh_run_state
 WHEN NEW.job_id!=OLD.job_id OR NEW.next_order<OLD.next_order
 BEGIN SELECT RAISE(ABORT,'fresh queue state cannot be reversed'); END;
CREATE TRIGGER hash_fresh_run_state_no_delete BEFORE DELETE ON hash_fresh_run_state BEGIN SELECT RAISE(ABORT,'fresh queue state cannot be removed'); END;
CREATE TABLE hash_fresh_progress (
 job_id TEXT NOT NULL REFERENCES hash_fresh_run_state(job_id),
 ordinal INTEGER NOT NULL CHECK(typeof(ordinal)='integer' AND ordinal BETWEEN 1 AND 20),
 status TEXT NOT NULL CHECK(typeof(status)='text' AND status IN ('pending','running','complete','invalidated')),
 sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>=0),
 ready_order INTEGER NOT NULL CHECK(typeof(ready_order)='integer' AND ready_order>=1),
 checked_offset INTEGER NOT NULL CHECK(typeof(checked_offset)='integer' AND checked_offset>=0),
 checkpoint BLOB NOT NULL CHECK(typeof(checkpoint)='blob' AND length(checkpoint)<=16384),
 error_code TEXT NOT NULL CHECK(typeof(error_code)='text' AND length(error_code)<=64),
 PRIMARY KEY(job_id,ordinal), UNIQUE(job_id,ready_order),
 FOREIGN KEY(job_id,ordinal) REFERENCES hash_fresh_work(job_id,ordinal)
);
CREATE INDEX hash_fresh_progress_pending ON hash_fresh_progress(job_id,ready_order,ordinal) WHERE status='pending';
CREATE TRIGGER hash_fresh_progress_monotonic BEFORE UPDATE ON hash_fresh_progress
 WHEN NEW.job_id!=OLD.job_id OR NEW.ordinal!=OLD.ordinal OR NEW.sequence<OLD.sequence OR NEW.ready_order<OLD.ready_order OR NEW.checked_offset<OLD.checked_offset OR OLD.status IN ('complete','invalidated')
 BEGIN SELECT RAISE(ABORT,'fresh progress cannot be reversed or reopened'); END;
CREATE TRIGGER hash_fresh_progress_no_delete BEFORE DELETE ON hash_fresh_progress BEGIN SELECT RAISE(ABORT,'fresh progress cannot be removed'); END;
CREATE TABLE hash_fresh_attempt (
 job_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL CHECK(typeof(ordinal)='integer' AND ordinal BETWEEN 1 AND 20),
 nonce TEXT NOT NULL CHECK(typeof(nonce)='text' AND length(nonce)=64),
 base_sequence INTEGER NOT NULL CHECK(typeof(base_sequence)='integer' AND base_sequence>=0),
 from_offset INTEGER NOT NULL CHECK(typeof(from_offset)='integer' AND from_offset>=0),
 grant_bytes INTEGER NOT NULL CHECK(typeof(grant_bytes)='integer' AND grant_bytes BETWEEN 0 AND 1048576),
 reservation_day TEXT NOT NULL CHECK(typeof(reservation_day)='text' AND length(reservation_day)=10),
 status TEXT NOT NULL CHECK(typeof(status)='text' AND status IN ('reserved','settled','interrupted_unknown')),
 requested_bytes INTEGER CHECK(typeof(requested_bytes) IN ('integer','null')),
 read_bytes INTEGER CHECK(typeof(read_bytes) IN ('integer','null')),
 elapsed_ns INTEGER CHECK(typeof(elapsed_ns) IN ('integer','null')),
 PRIMARY KEY(job_id,ordinal), FOREIGN KEY(job_id,ordinal) REFERENCES hash_fresh_progress(job_id,ordinal)
);
CREATE TABLE hash_fresh_budget (
 job_id TEXT NOT NULL PRIMARY KEY REFERENCES hash_fresh_run_state(job_id),
 day TEXT NOT NULL CHECK(typeof(day)='text' AND length(day)=10),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=0),
 reserved_bytes INTEGER NOT NULL CHECK(typeof(reserved_bytes)='integer' AND reserved_bytes>=0),
 requested_bytes INTEGER NOT NULL CHECK(typeof(requested_bytes)='integer' AND requested_bytes>=0),
 read_bytes INTEGER NOT NULL CHECK(typeof(read_bytes)='integer' AND read_bytes>=0),
 unknown_reserved_bytes INTEGER NOT NULL CHECK(typeof(unknown_reserved_bytes)='integer' AND unknown_reserved_bytes>=0),
 total_reserved_bytes INTEGER NOT NULL CHECK(typeof(total_reserved_bytes)='integer' AND total_reserved_bytes>=0),
 total_requested_bytes INTEGER NOT NULL CHECK(typeof(total_requested_bytes)='integer' AND total_requested_bytes>=0),
 total_read_bytes INTEGER NOT NULL CHECK(typeof(total_read_bytes)='integer' AND total_read_bytes>=0),
 total_unknown_reserved_bytes INTEGER NOT NULL CHECK(typeof(total_unknown_reserved_bytes)='integer' AND total_unknown_reserved_bytes>=0)
);
CREATE TRIGGER hash_fresh_budget_monotonic BEFORE UPDATE ON hash_fresh_budget
 WHEN NEW.job_id!=OLD.job_id OR NEW.day<OLD.day OR NEW.max_now_ns<OLD.max_now_ns OR NEW.total_reserved_bytes<OLD.total_reserved_bytes OR NEW.total_requested_bytes<OLD.total_requested_bytes OR NEW.total_read_bytes<OLD.total_read_bytes OR NEW.total_unknown_reserved_bytes<OLD.total_unknown_reserved_bytes
 BEGIN SELECT RAISE(ABORT,'fresh lifetime charges and clocks cannot be reversed'); END;
CREATE TRIGGER hash_fresh_budget_no_delete BEFORE DELETE ON hash_fresh_budget BEGIN SELECT RAISE(ABORT,'fresh budget cannot be removed'); END;
PRAGMA user_version=6;
`
