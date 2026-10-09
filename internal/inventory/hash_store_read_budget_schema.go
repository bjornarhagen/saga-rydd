package inventory

// This singleton tracks permanent reservation charges across the original and
// independent fresh namespaces. Namespace ledgers remain the usage evidence.
const hashStoreReadBudgetSchema = `
CREATE TABLE hash_store_read_budget (
 id INTEGER PRIMARY KEY CHECK(typeof(id)='integer' AND id=1),
 store_id TEXT NOT NULL CHECK(typeof(store_id)='text' AND length(store_id)=64),
 day TEXT NOT NULL CHECK(typeof(day)='text' AND length(day)=10),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=0),
 reserved_bytes INTEGER NOT NULL CHECK(typeof(reserved_bytes)='integer' AND reserved_bytes>=0),
 total_reserved_bytes INTEGER NOT NULL CHECK(typeof(total_reserved_bytes)='integer' AND total_reserved_bytes>=reserved_bytes)
);
CREATE TRIGGER hash_store_read_budget_monotonic BEFORE UPDATE ON hash_store_read_budget
 WHEN NEW.id!=OLD.id OR NEW.store_id!=OLD.store_id OR NEW.day<OLD.day OR NEW.max_now_ns<OLD.max_now_ns OR NEW.total_reserved_bytes<OLD.total_reserved_bytes
 BEGIN SELECT RAISE(ABORT,'shared hash charges and clocks cannot be reversed'); END;
CREATE TRIGGER hash_store_read_budget_no_delete BEFORE DELETE ON hash_store_read_budget BEGIN SELECT RAISE(ABORT,'shared hash charges cannot be removed'); END;
PRAGMA user_version=7;
`
