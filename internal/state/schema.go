package state

// Migrations are append-only. Inventory tables are rebuildable, but the database
// must never be deleted/recreated as a migration strategy: future action/restore
// records will live in their own durable tables here.
const schemaVersion = 3
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
}

const migration3 = `
ALTER TABLE entries ADD COLUMN skip_reason TEXT NOT NULL DEFAULT '';
CREATE TABLE directories (
 root_id INTEGER NOT NULL REFERENCES roots(id), path BLOB NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
 checked_at_ns INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(root_id,path)
);
`
