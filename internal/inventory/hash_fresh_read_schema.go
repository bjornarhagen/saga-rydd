package inventory

// Fresh consent is separate from immutable job scope, initial work, original
// approvals and original byte ledgers. Only explicit approval publication
// creates these tables; ordinary opens and readers never migrate them.
const hashFreshReadSchema = `
CREATE TABLE hash_fresh_read_approval (
 job_id TEXT PRIMARY KEY REFERENCES hash_fresh_job(id),
 approval_id TEXT UNIQUE NOT NULL CHECK(typeof(approval_id)='text' AND length(approval_id)=81),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384)
);
CREATE TRIGGER hash_fresh_read_approval_no_update BEFORE UPDATE ON hash_fresh_read_approval BEGIN SELECT RAISE(ABORT,'fresh read approval is immutable'); END;
CREATE TRIGGER hash_fresh_read_approval_no_delete BEFORE DELETE ON hash_fresh_read_approval BEGIN SELECT RAISE(ABORT,'fresh read approval is immutable'); END;
CREATE TABLE hash_fresh_read_revocation (
 job_id TEXT PRIMARY KEY REFERENCES hash_fresh_read_approval(job_id),
 revocation_id TEXT UNIQUE NOT NULL CHECK(typeof(revocation_id)='text' AND length(revocation_id)=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=4096)
);
CREATE TRIGGER hash_fresh_read_revocation_no_update BEFORE UPDATE ON hash_fresh_read_revocation BEGIN SELECT RAISE(ABORT,'fresh read revocation is immutable'); END;
CREATE TRIGGER hash_fresh_read_revocation_no_delete BEFORE DELETE ON hash_fresh_read_revocation BEGIN SELECT RAISE(ABORT,'fresh read revocation is immutable'); END;
CREATE TABLE hash_fresh_read_observation (
 job_id TEXT PRIMARY KEY REFERENCES hash_fresh_read_approval(job_id),
 approval_id TEXT UNIQUE NOT NULL CHECK(typeof(approval_id)='text' AND length(approval_id)=81),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=0),
 expired INTEGER NOT NULL CHECK(typeof(expired)='integer' AND expired IN (0,1))
);
CREATE TRIGGER hash_fresh_read_observation_monotonic BEFORE UPDATE ON hash_fresh_read_observation
 WHEN NEW.job_id!=OLD.job_id OR NEW.approval_id!=OLD.approval_id OR NEW.max_now_ns<OLD.max_now_ns OR NEW.expired<OLD.expired
 BEGIN SELECT RAISE(ABORT,'fresh read clock observations cannot be reversed'); END;
CREATE TRIGGER hash_fresh_read_observation_no_delete BEFORE DELETE ON hash_fresh_read_observation BEGIN SELECT RAISE(ABORT,'fresh read observations cannot be removed'); END;
PRAGMA user_version=5;
`
