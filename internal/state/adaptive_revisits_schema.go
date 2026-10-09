package state

// One bounded scalar record per historically tracked root. Disabled roots are
// retained like the inventory; this is not a lifetime root-count bound.
const migration13 = `
CREATE TABLE adaptive_inventory_revisits (
 root_id INTEGER PRIMARY KEY REFERENCES roots(id), root_path BLOB NOT NULL CHECK(length(root_path) BETWEEN 1 AND 4096),
 scope_digest TEXT NOT NULL CHECK(length(scope_digest)=64 AND scope_digest NOT GLOB '*[^0-9a-f]*'),
 enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 instance TEXT NOT NULL CHECK(length(instance)=32 AND instance NOT GLOB '*[^0-9a-f]*'),
 root_identity TEXT NOT NULL CHECK(length(root_identity)<=4096),
 epoch INTEGER NOT NULL CHECK(typeof(epoch)='integer' AND epoch>=0),
 root_job_id INTEGER NOT NULL CHECK(typeof(root_job_id)='integer' AND root_job_id>=0),
 root_generation INTEGER NOT NULL CHECK(typeof(root_generation)='integer' AND root_generation>=0),
 claimed INTEGER NOT NULL CHECK(claimed IN (0,1)),
 started INTEGER NOT NULL CHECK(started IN (0,1)), root_complete INTEGER NOT NULL CHECK(root_complete IN (0,1)),
 changed INTEGER NOT NULL CHECK(changed IN (0,1)), unknown INTEGER NOT NULL CHECK(unknown IN (0,1)),
 unchanged_streak INTEGER NOT NULL CHECK(typeof(unchanged_streak)='integer' AND unchanged_streak BETWEEN 0 AND 2),
 max_now_ns INTEGER NOT NULL CHECK(typeof(max_now_ns)='integer' AND max_now_ns>0),
 finalized_epoch INTEGER NOT NULL CHECK(typeof(finalized_epoch)='integer' AND finalized_epoch>=0 AND finalized_epoch<=epoch),
 finished_ns INTEGER NOT NULL CHECK(typeof(finished_ns)='integer' AND finished_ns>=0 AND finished_ns<=max_now_ns),
 scheduled_due_ns INTEGER NOT NULL CHECK(typeof(scheduled_due_ns)='integer' AND scheduled_due_ns>=0),
 interval_ns INTEGER NOT NULL CHECK(interval_ns IN (86400000000000,604800000000000)),
 last_initialized INTEGER NOT NULL CHECK(last_initialized IN (0,1)),
 last_changed INTEGER NOT NULL CHECK(last_changed IN (0,1)), last_unknown INTEGER NOT NULL CHECK(last_unknown IN (0,1)),
 last_streak INTEGER NOT NULL CHECK(typeof(last_streak)='integer' AND last_streak BETWEEN 0 AND 2),
 last_outcome TEXT NOT NULL CHECK(last_outcome IN ('not_recorded','changed','unknown','unchanged')),
 CHECK(root_complete=0 OR (started=1 AND root_generation>0)),
 CHECK(epoch!=0 OR (root_job_id=0 AND root_generation=0 AND started=0 AND root_complete=0 AND finalized_epoch=0)),
 CHECK(started=1 OR root_generation=0)
);
`
