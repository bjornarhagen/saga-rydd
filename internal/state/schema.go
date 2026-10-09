package state

// Migrations are append-only. Inventory tables are rebuildable, but the database
// must never be deleted/recreated as a migration strategy: future action/restore
// records must stay separate from rebuildable inventory. Saved selections use
// their own database; this inventory has a durable incarnation identity.
const schemaVersion = 14
const applicationID = 0x52594444 // RYDD

const migration1 = `
CREATE TABLE schema_migrations (
 version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_ns INTEGER NOT NULL
);
CREATE TABLE roots (
 id INTEGER PRIMARY KEY, path BLOB NOT NULL UNIQUE,
 enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
 volume_id TEXT NOT NULL DEFAULT '', last_scan_ns INTEGER,
 last_error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE entries (
 id INTEGER PRIMARY KEY, root_id INTEGER NOT NULL REFERENCES roots(id),
 path BLOB NOT NULL, parent BLOB NOT NULL, kind TEXT NOT NULL
 CHECK(kind IN ('file','directory','symlink','other')),
 size INTEGER NOT NULL CHECK(size >= 0), allocated INTEGER NOT NULL CHECK(allocated >= 0),
 mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL,
 device TEXT NOT NULL, inode TEXT NOT NULL,
 generation INTEGER NOT NULL, observed_at_ns INTEGER NOT NULL,
 UNIQUE(root_id,path)
);
CREATE INDEX entries_parent ON entries(root_id,parent);
CREATE INDEX entries_size ON entries(size) WHERE kind='file';
CREATE TABLE jobs (
 id INTEGER PRIMARY KEY, root_id INTEGER NOT NULL REFERENCES roots(id),
 kind TEXT NOT NULL, path BLOB NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running')),
 due_at_ns INTEGER NOT NULL, cursor BLOB, attempts INTEGER NOT NULL DEFAULT 0,
 last_error TEXT NOT NULL DEFAULT '', UNIQUE(root_id,kind,path)
);
CREATE INDEX jobs_due ON jobs(status,due_at_ns);
CREATE TABLE settings (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE daily_budgets (
 day TEXT PRIMARY KEY, content_bytes INTEGER NOT NULL DEFAULT 0 CHECK(content_bytes >= 0),
 metadata_ops INTEGER NOT NULL DEFAULT 0 CHECK(metadata_ops >= 0)
);
`

const migration2 = `
ALTER TABLE jobs ADD COLUMN lease_token TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN lease_until_ns INTEGER NOT NULL DEFAULT 0;
INSERT INTO settings(key,value) VALUES('worker.paused',X'30') ON CONFLICT(key) DO NOTHING;
`

var migrations = []struct{ name, sql string }{
	{"inventory-foundation", migration1},
	{"worker-queue-leases", migration2},
	{"streaming-inventory", migration3},
	{"durable-scan-dispatch", migration4},
	{"compact-directory-inventory", migration5},
	{"scoped-allocated-reductions", migration6},
	{"absent-subtree-retirement", migration7},
	{"complete-scope-coverage", migration8},
	{"inventory-incarnation", migration9},
	{"scanner-metadata-reservations", migration10},
	{"fair-inventory-root-turns", migration11},
	{"experimental-worker-cpu-feedback", migration12},
	{"adaptive-historical-metadata-revisits", migration13},
	{"background-compact-mode", migration14},
	{"conservative-self-cpu-charges", migration15},
}

const migration12 = `
CREATE TABLE worker_cpu_feedback (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 tracking_started_ns INTEGER NOT NULL CHECK(typeof(tracking_started_ns)='integer' AND tracking_started_ns>0),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=tracking_started_ns),
 last_begun_dispatch_ns INTEGER NOT NULL CHECK(typeof(last_begun_dispatch_ns)='integer' AND last_begun_dispatch_ns>0),
 completed_unknown INTEGER NOT NULL CHECK(typeof(completed_unknown)='integer' AND completed_unknown>=0),
 recovered_unknown INTEGER NOT NULL CHECK(typeof(recovered_unknown)='integer' AND recovered_unknown>=0),
 count_saturated INTEGER NOT NULL CHECK(count_saturated IN (0,1)),
 token TEXT NOT NULL CHECK(length(token)=64 AND token NOT GLOB '*[^0-9a-f]*'),
 instance TEXT NOT NULL CHECK(length(instance)=32 AND instance NOT GLOB '*[^0-9a-f]*'),
 kind TEXT NOT NULL CHECK(kind IN ('source','maintenance')),
 root_id INTEGER NOT NULL CHECK(typeof(root_id)='integer' AND root_id>0),
 job_id INTEGER NOT NULL CHECK(typeof(job_id)='integer' AND job_id>=0), job_token TEXT NOT NULL,
 job_lease_until_ns INTEGER CHECK(job_lease_until_ns IS NULL OR (typeof(job_lease_until_ns)='integer' AND job_lease_until_ns>0)),
 dispatch_reserved_ns INTEGER NOT NULL CHECK(typeof(dispatch_reserved_ns)='integer' AND dispatch_reserved_ns>0),
 window_started_ns INTEGER NOT NULL CHECK(typeof(window_started_ns)='integer' AND window_started_ns>0),
 recorded_ns INTEGER NOT NULL CHECK(typeof(recorded_ns)='integer' AND recorded_ns>0),
 status TEXT NOT NULL CHECK(status IN ('pending','observed','completed_unknown','recovered_unknown')),
 settled_ns INTEGER CHECK(settled_ns IS NULL OR (typeof(settled_ns)='integer' AND settled_ns>0)),
 cpu_time_ns INTEGER CHECK(cpu_time_ns IS NULL OR (typeof(cpu_time_ns)='integer' AND cpu_time_ns>=0)),
 elapsed_ns INTEGER CHECK(elapsed_ns IS NULL OR (typeof(elapsed_ns)='integer' AND elapsed_ns>0)),
 reason TEXT NOT NULL CHECK(length(reason)<=64),
 backoff_ns INTEGER NOT NULL CHECK(typeof(backoff_ns)='integer' AND backoff_ns BETWEEN 0 AND 3600000000000),
 backoff_capped INTEGER NOT NULL CHECK(backoff_capped IN (0,1)),
 next_allowed_ns INTEGER CHECK(next_allowed_ns IS NULL OR (typeof(next_allowed_ns)='integer' AND next_allowed_ns>0)),
 CHECK(last_begun_dispatch_ns=dispatch_reserved_ns AND dispatch_reserved_ns<=recorded_ns AND window_started_ns<=recorded_ns),
 CHECK((kind='source' AND job_id>0 AND length(job_token)=32 AND job_token NOT GLOB '*[^0-9a-f]*' AND job_lease_until_ns>recorded_ns) OR
       (kind='maintenance' AND job_id=0 AND job_token='' AND job_lease_until_ns IS NULL))
);
`

const migration11 = `
CREATE INDEX jobs_inventory_root_due ON jobs(root_id,due_at_ns,id)
 WHERE status='pending' AND kind='inventory';
CREATE INDEX allocation_cache_pending_root ON allocation_cache(root_id)
 WHERE phase!='done';
CREATE INDEX allocation_cache_root_revision ON allocation_cache(root_id,revision);
`

const migration4 = `
CREATE TABLE scan_dispatch (
 id INTEGER PRIMARY KEY CHECK(id=1), day TEXT NOT NULL,
 chunks INTEGER NOT NULL CHECK(chunks>=0), last_start_ns INTEGER NOT NULL,
 next_start_ns INTEGER NOT NULL
);
`

const migration3 = `
ALTER TABLE entries ADD COLUMN skip_reason TEXT NOT NULL DEFAULT '';
CREATE TABLE directories (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
 checked_at_ns INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(root_id,path)
);
`

const migration5 = `
CREATE TABLE compact_dirs (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 generation INTEGER NOT NULL, logical INTEGER NOT NULL, files INTEGER NOT NULL,
 unknown_inodes INTEGER NOT NULL, skipped_files INTEGER NOT NULL,
 PRIMARY KEY(root_id,path)
);
CREATE TABLE compact_inodes (
 root_id INTEGER NOT NULL, path BLOB NOT NULL, generation INTEGER NOT NULL,
 device TEXT NOT NULL, inode TEXT NOT NULL, allocated INTEGER NOT NULL,
 logical INTEGER NOT NULL, paths INTEGER NOT NULL, conflicting INTEGER NOT NULL,
 PRIMARY KEY(root_id,path,generation,device,inode)
) WITHOUT ROWID;
CREATE TABLE compact_retirement (
 root_id INTEGER NOT NULL, path BLOB NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY(root_id,path,generation)
);
CREATE INDEX entries_compact_retire ON entries(root_id,parent,id) WHERE kind='file';
`

const migration6 = `
CREATE TABLE allocation_revisions (
 root_id INTEGER PRIMARY KEY REFERENCES roots(id),
 revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>=0)
);
CREATE TABLE allocation_cache (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 revision INTEGER NOT NULL DEFAULT -1,
 phase TEXT NOT NULL DEFAULT 'reset' CHECK(phase IN ('reset','entries','inodes','cleanup','done')),
 entry_cursor BLOB NOT NULL DEFAULT X'', inode_device TEXT NOT NULL DEFAULT '', inode_number TEXT NOT NULL DEFAULT '',
 examined INTEGER NOT NULL DEFAULT 0, allocated INTEGER NOT NULL DEFAULT 0,
 repeated INTEGER NOT NULL DEFAULT 0, unknown INTEGER NOT NULL DEFAULT 0, conflicting INTEGER NOT NULL DEFAULT 0,
 ready INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(root_id,path)
);
CREATE TABLE allocation_members (
 root_id INTEGER NOT NULL, scope BLOB NOT NULL, path BLOB NOT NULL,
 excluded INTEGER NOT NULL, kind TEXT NOT NULL, generation INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(root_id,scope,path)
) WITHOUT ROWID;
CREATE INDEX allocation_members_pending ON allocation_members(root_id,scope,done,path)
 WHERE excluded=0 AND kind='directory' AND generation>0;
CREATE TABLE allocation_identities (
 root_id INTEGER NOT NULL, scope BLOB NOT NULL, device TEXT NOT NULL, inode TEXT NOT NULL,
 allocated INTEGER NOT NULL, logical INTEGER NOT NULL,
 PRIMARY KEY(root_id,scope,device,inode)
) WITHOUT ROWID;
`

const migration7 = `
ALTER TABLE allocation_revisions ADD COLUMN scan_revision INTEGER NOT NULL DEFAULT 0
 CHECK(typeof(scan_revision)='integer' AND scan_revision>=0);
CREATE TABLE subtree_reconcile (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 generation INTEGER NOT NULL, cursor BLOB NOT NULL DEFAULT X'', PRIMARY KEY(root_id,path)
);
CREATE TABLE subtree_retirement (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 scan_revision INTEGER NOT NULL, preserve_entry INTEGER NOT NULL CHECK(preserve_entry IN (0,1)),
 phase INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(root_id,path)
);
CREATE INDEX entries_membership ON entries(root_id,parent,path);
`

const migration8 = `
ALTER TABLE allocation_cache ADD COLUMN coverage BLOB NOT NULL DEFAULT X'';
ALTER TABLE allocation_members ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0;
UPDATE allocation_cache SET revision=-1,ready=0;
`

const migration9 = `
CREATE TABLE inventory_identity (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 token TEXT NOT NULL CHECK(length(token)=64)
);
INSERT INTO inventory_identity VALUES(1,lower(hex(randomblob(32))));
CREATE TRIGGER inventory_identity_no_update BEFORE UPDATE ON inventory_identity
 BEGIN SELECT RAISE(ABORT,'inventory identity is immutable'); END;
CREATE TRIGGER inventory_identity_no_delete BEFORE DELETE ON inventory_identity
 BEGIN SELECT RAISE(ABORT,'inventory identity is immutable'); END;
`

const migration10 = `
CREATE TABLE scan_metadata_budget (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 tracking_started_ns INTEGER NOT NULL CHECK(typeof(tracking_started_ns)='integer' AND tracking_started_ns>0),
 day TEXT NOT NULL CHECK(length(day)=10),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>=tracking_started_ns),
 reserved INTEGER NOT NULL CHECK(typeof(reserved)='integer' AND reserved>=0),
 observed INTEGER NOT NULL CHECK(typeof(observed)='integer' AND observed>=0),
 unknown_reserved INTEGER NOT NULL CHECK(typeof(unknown_reserved)='integer' AND unknown_reserved>=0),
 total_reserved INTEGER NOT NULL CHECK(typeof(total_reserved)='integer' AND total_reserved>=reserved),
 total_observed INTEGER NOT NULL CHECK(typeof(total_observed)='integer' AND total_observed>=observed),
 total_unknown_reserved INTEGER NOT NULL CHECK(typeof(total_unknown_reserved)='integer' AND total_unknown_reserved>=unknown_reserved),
 CHECK(observed<=reserved AND unknown_reserved<=reserved-observed),
 CHECK(total_observed<=total_reserved AND total_unknown_reserved<=total_reserved-total_observed)
);
CREATE TABLE scan_metadata_reservations (
 scope TEXT PRIMARY KEY CHECK(scope IN ('startup','next')),
 token TEXT NOT NULL UNIQUE CHECK(length(token)=64 AND token NOT GLOB '*[^0-9a-f]*'),
 job_id INTEGER NOT NULL CHECK(typeof(job_id)='integer' AND job_id>=0),
 job_token TEXT NOT NULL,
 day TEXT NOT NULL CHECK(length(day)=10),
 started_ns INTEGER NOT NULL CHECK(typeof(started_ns)='integer' AND started_ns>0),
 expires_ns INTEGER NOT NULL CHECK(typeof(expires_ns)='integer' AND expires_ns>started_ns),
 high_water_ns INTEGER NOT NULL CHECK(typeof(high_water_ns)='integer' AND high_water_ns=started_ns),
 allowance INTEGER NOT NULL CHECK(typeof(allowance)='integer' AND allowance BETWEEN 1 AND 65536),
 status TEXT NOT NULL CHECK(status IN ('reserved','settled','unknown')),
 observed INTEGER CHECK(observed IS NULL OR (typeof(observed)='integer' AND observed BETWEEN 0 AND allowance)),
 CHECK((scope='startup' AND job_id=0 AND job_token='') OR
       (scope='next' AND job_id>0 AND length(job_token)=32 AND job_token NOT GLOB '*[^0-9a-f]*')),
 CHECK((status='settled' AND observed IS NOT NULL) OR (status!='settled' AND observed IS NULL))
);
`
