package inventory

// Hash work is separate from rebuildable inventory. The finite selection,
// current heads, latest attempts and aggregate counters have bounded row counts.
const hashStoreSchema = `
CREATE TABLE hash_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), store_id TEXT NOT NULL CHECK(length(store_id)=64),
 next_order INTEGER NOT NULL CHECK(typeof(next_order)='integer' AND next_order>=0)
);
CREATE TABLE hash_selection (
 id INTEGER PRIMARY KEY CHECK(id=1), selection_id TEXT NOT NULL CHECK(length(selection_id)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=1048576)
);
CREATE TRIGGER hash_selection_no_update BEFORE UPDATE ON hash_selection BEGIN SELECT RAISE(ABORT,'hash selection is immutable'); END;
CREATE TRIGGER hash_selection_no_delete BEFORE DELETE ON hash_selection BEGIN SELECT RAISE(ABORT,'hash selection is immutable'); END;
CREATE TABLE hash_work (
 id INTEGER PRIMARY KEY CHECK(id BETWEEN 1 AND 20),
 status TEXT NOT NULL CHECK(status IN ('pending','running','complete','invalidated')),
 sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>=0),
 ready_order INTEGER NOT NULL CHECK(typeof(ready_order)='integer' AND ready_order>0),
 checked_offset INTEGER NOT NULL CHECK(typeof(checked_offset)='integer' AND checked_offset>=0),
 checkpoint BLOB NOT NULL CHECK(typeof(checkpoint)='blob' AND length(checkpoint)<=8192),
 error_code TEXT NOT NULL DEFAULT '' CHECK(length(error_code)<=64)
);
CREATE INDEX hash_work_pending ON hash_work(ready_order,id) WHERE status='pending';
CREATE UNIQUE INDEX hash_work_one_running ON hash_work(status) WHERE status='running';
CREATE TABLE hash_attempt (
 work_id INTEGER PRIMARY KEY REFERENCES hash_work(id), nonce TEXT NOT NULL CHECK(length(nonce)=64),
 base_sequence INTEGER NOT NULL CHECK(typeof(base_sequence)='integer' AND base_sequence>=0),
 from_offset INTEGER NOT NULL CHECK(typeof(from_offset)='integer' AND from_offset>=0),
 grant_bytes INTEGER NOT NULL CHECK(typeof(grant_bytes)='integer' AND grant_bytes BETWEEN 0 AND 1048576),
 reservation_day TEXT NOT NULL CHECK(length(reservation_day)=10),
 status TEXT NOT NULL CHECK(status IN ('reserved','settled','interrupted_unknown')),
 requested_bytes INTEGER CHECK(requested_bytes IS NULL OR (typeof(requested_bytes)='integer' AND requested_bytes>=0 AND requested_bytes<=grant_bytes)),
 read_bytes INTEGER CHECK(read_bytes IS NULL OR (typeof(read_bytes)='integer' AND read_bytes>=0 AND read_bytes<=requested_bytes)),
 elapsed_ns INTEGER CHECK(elapsed_ns IS NULL OR (typeof(elapsed_ns)='integer' AND elapsed_ns>=0))
);
CREATE UNIQUE INDEX hash_attempt_one_reserved ON hash_attempt(status) WHERE status='reserved';
CREATE TABLE hash_budget (
 id INTEGER PRIMARY KEY CHECK(id=1), day TEXT NOT NULL CHECK(length(day)=10),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=0),
 reserved_bytes INTEGER NOT NULL CHECK(typeof(reserved_bytes)='integer' AND reserved_bytes>=0),
 requested_bytes INTEGER NOT NULL CHECK(typeof(requested_bytes)='integer' AND requested_bytes>=0),
 read_bytes INTEGER NOT NULL CHECK(typeof(read_bytes)='integer' AND read_bytes>=0 AND read_bytes<=requested_bytes),
 unknown_reserved_bytes INTEGER NOT NULL CHECK(typeof(unknown_reserved_bytes)='integer' AND unknown_reserved_bytes>=0 AND unknown_reserved_bytes<=reserved_bytes),
 total_reserved_bytes INTEGER NOT NULL CHECK(typeof(total_reserved_bytes)='integer' AND total_reserved_bytes>=reserved_bytes),
 total_requested_bytes INTEGER NOT NULL CHECK(typeof(total_requested_bytes)='integer' AND total_requested_bytes>=requested_bytes),
 total_read_bytes INTEGER NOT NULL CHECK(typeof(total_read_bytes)='integer' AND total_read_bytes>=read_bytes AND total_read_bytes<=total_requested_bytes),
 total_unknown_reserved_bytes INTEGER NOT NULL CHECK(typeof(total_unknown_reserved_bytes)='integer' AND total_unknown_reserved_bytes>=unknown_reserved_bytes AND total_unknown_reserved_bytes<=total_reserved_bytes)
);
PRAGMA application_id=0x52594853;
PRAGMA user_version=1;
`
